// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! IndexSigMap v1: mapping from index artifact hash to SegmentSig hash.
//!
//! Purpose
//! -------
//! Query-time gating needs a fast way to locate the SegmentSigV1 artifact that
//! describes a given index artifact (IndexSegmentV1 or IndexPackV1) referenced by an IndexSnapshotV1 entry. We keep the IndexSnapshot manifest
//! stable and add this mapping as a sidecar artifact.
//!
//! Design
//! ------
//! - Content-addressed (stored as an artifact).
//! - Canonical bytes (encode/decode is deterministic).
//! - Sorted by index_seg ascending.
//! - Exactly one signature hash per index_seg (duplicates rejected).

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::frame::{Id64, SourceId};
use crate::hash::Hash32;

const INDEX_SIG_MAP_MAGIC: [u8; 8] = *b"FSALMISM";
const INDEX_SIG_MAP_VERSION: u16 = 1;

/// One mapping entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexSigMapEntryV1 {
    /// Hash of the index artifact (IndexSegmentV1 or IndexPackV1).
    pub index_seg: Hash32,
    /// Hash of the SegmentSig artifact.
    pub sig: Hash32,
}

/// IndexSigMap v1.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexSigMapV1 {
    /// Source id this mapping corresponds to.
    pub source_id: SourceId,
    /// Entries sorted by index_seg.
    pub entries: Vec<IndexSigMapEntryV1>,
}

impl IndexSigMapV1 {
    /// Create an empty map.
    pub fn new(source_id: SourceId) -> Self {
        Self {
            source_id,
            entries: Vec::new(),
        }
    }

    /// Insert a mapping entry.
    ///
    /// Note: encode sorts and validates. Callers may push unsorted entries.
    pub fn push(&mut self, index_seg: Hash32, sig: Hash32) {
        self.entries.push(IndexSigMapEntryV1 { index_seg, sig });
    }

    /// Lookup the signature hash for an index segment hash.
    pub fn lookup_sig(&self, index_seg: &Hash32) -> Option<Hash32> {
        self.entries
            .binary_search_by(|e| e.index_seg.cmp(index_seg))
            .ok()
            .map(|i| self.entries[i].sig)
    }

    /// Encode into canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        if self.entries.len() > (u32::MAX as usize) {
            return Err(EncodeError::new("too many entries"));
        }

        // Canonicalize: sort by (index_seg, sig).
        let mut entries = self.entries.clone();
        entries.sort_by(|a, b| {
            a.index_seg
                .cmp(&b.index_seg)
                .then_with(|| a.sig.cmp(&b.sig))
        });

        // Validate: no duplicate index_seg.
        for i in 1..entries.len() {
            if entries[i - 1].index_seg == entries[i].index_seg {
                return Err(EncodeError::new("duplicate index_seg"));
            }
        }

        let mut w = ByteWriter::with_capacity(32 + entries.len() * 64);
        w.write_raw(&INDEX_SIG_MAP_MAGIC);
        w.write_u16(INDEX_SIG_MAP_VERSION);
        w.write_u16(0);
        w.write_u64(self.source_id.0 .0);
        w.write_u32(entries.len() as u32);

        for e in entries.iter() {
            w.write_raw(&e.index_seg);
            w.write_raw(&e.sig);
        }

        Ok(w.into_bytes())
    }

    /// Decode from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<IndexSigMapV1, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let magic = r.read_fixed(8)?;
        if magic != INDEX_SIG_MAP_MAGIC {
            return Err(DecodeError::new("bad index sig map magic"));
        }
        let ver = r.read_u16()?;
        if ver != INDEX_SIG_MAP_VERSION {
            return Err(DecodeError::new("unsupported index sig map version"));
        }
        let _reserved = r.read_u16()?;

        let sid = r.read_u64()?;
        let source_id = SourceId(Id64(sid));

        let n = r.read_u32()? as usize;
        // Defensive bound: avoid huge allocations on malformed input.
        if n > 50_000_000 {
            return Err(DecodeError::new("too many entries"));
        }

        let mut entries: Vec<IndexSigMapEntryV1> = Vec::with_capacity(n);
        for _ in 0..n {
            let a = r.read_fixed(32)?;
            let b = r.read_fixed(32)?;
            let mut index_seg = [0u8; 32];
            let mut sig = [0u8; 32];
            index_seg.copy_from_slice(a);
            sig.copy_from_slice(b);
            entries.push(IndexSigMapEntryV1 { index_seg, sig });
        }

        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        // Validate canonical order and uniqueness.
        for i in 1..entries.len() {
            if entries[i - 1].index_seg >= entries[i].index_seg {
                return Err(DecodeError::new("entries not strictly sorted"));
            }
        }

        Ok(IndexSigMapV1 { source_id, entries })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(b: u8) -> Hash32 {
        let mut x = [0u8; 32];
        x[0] = b;
        x
    }

    #[test]
    fn index_sig_map_round_trip_and_lookup() {
        let mut m = IndexSigMapV1::new(SourceId(Id64(7)));
        m.push(h(2), h(20));
        m.push(h(1), h(10));
        m.push(h(3), h(30));

        let bytes = m.encode().unwrap();
        let got = IndexSigMapV1::decode(&bytes).unwrap();
        assert_eq!(got.source_id, SourceId(Id64(7)));
        assert_eq!(got.entries.len(), 3);
        assert_eq!(got.lookup_sig(&h(1)), Some(h(10)));
        assert_eq!(got.lookup_sig(&h(2)), Some(h(20)));
        assert_eq!(got.lookup_sig(&h(9)), None);
    }

    #[test]
    fn index_sig_map_decode_rejects_unsorted() {
        // Build a byte stream with out-of-order entries.
        let mut w = ByteWriter::with_capacity(8 + 2 + 2 + 8 + 4 + 2 * 64);
        w.write_raw(&INDEX_SIG_MAP_MAGIC);
        w.write_u16(INDEX_SIG_MAP_VERSION);
        w.write_u16(0);
        w.write_u64(1);
        w.write_u32(2);
        w.write_raw(&h(2));
        w.write_raw(&h(20));
        w.write_raw(&h(1));
        w.write_raw(&h(10));
        let bytes = w.into_bytes();

        let err = IndexSigMapV1::decode(&bytes).unwrap_err();
        assert_eq!(err.to_string(), "entries not strictly sorted");
    }

    #[test]
    fn index_sig_map_encode_rejects_duplicate_index_seg() {
        let mut m = IndexSigMapV1::new(SourceId(Id64(1)));
        m.push(h(2), h(20));
        m.push(h(2), h(21));
        let err = m.encode().unwrap_err();
        assert_eq!(err.to_string(), "duplicate index_seg");
    }
}
