// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! IndexSnapshot v1: manifest linking FrameSegment hashes to IndexSegment hashes.
//!
//! IndexSegment is built per FrameSegment. IndexSnapshot provides a
//! deterministic "inventory" of which segments are indexed, and the hashes of
//! those index artifacts.
//!
//! This is not a search engine feature. It is an internal acceleration manifest
//! used to support Novel's reasoning pipeline.
//!
//! Design goals:
//! - Canonical bytes (bitwise deterministic).
//! - Integer-only metadata (no floats).
//! - Defensive decode bounds to avoid pathological allocations.

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::frame::SourceId;
use crate::hash::Hash32;

/// A single mapping entry in IndexSnapshot v1.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexSnapshotEntryV1 {
    /// Hash of the FrameSegment bytes.
    pub frame_seg: Hash32,
    /// Hash of the IndexSegment bytes.
    pub index_seg: Hash32,
    /// Row count from the indexed segment.
    pub row_count: u32,
    /// Term dictionary count in the index segment.
    pub term_count: u32,
    /// Total postings blob length in bytes.
    pub postings_bytes: u32,
}

/// IndexSnapshot manifest v1.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexSnapshotV1 {
    /// Manifest version (currently 1).
    pub version: u16,
    /// Source id for all indexed segments in this snapshot.
    pub source_id: SourceId,
    /// Entries sorted by frame_seg hash ascending.
    pub entries: Vec<IndexSnapshotEntryV1>,
}

/// IndexSnapshot encode/decode errors are propagated as codec errors.
impl IndexSnapshotV1 {
    /// Create an empty snapshot for a source id.
    pub fn new(source_id: SourceId) -> Self {
        IndexSnapshotV1 {
            version: 1,
            source_id,
            entries: Vec::new(),
        }
    }

    /// Canonicalize the snapshot in-place for deterministic encoding.
    ///
    /// Rules:
    /// - entries sorted by frame_seg asc; tie-break by index_seg asc.
    pub fn canonicalize_in_place(&mut self) {
        self.entries
            .sort_by(|a, b| match a.frame_seg.cmp(&b.frame_seg) {
                core::cmp::Ordering::Equal => a.index_seg.cmp(&b.index_seg),
                other => other,
            });
    }

    /// Encode snapshot into canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        let mut tmp = self.clone();
        tmp.canonicalize_in_place();

        let mut w = ByteWriter::with_capacity(32 + (tmp.entries.len() * 80));
        w.write_u16(tmp.version);
        w.write_u64(tmp.source_id.0 .0);
        if tmp.entries.len() > (u32::MAX as usize) {
            return Err(EncodeError::new("too many entries"));
        }
        w.write_u32(tmp.entries.len() as u32);

        for e in &tmp.entries {
            w.write_raw(&e.frame_seg);
            w.write_raw(&e.index_seg);
            w.write_u32(e.row_count);
            w.write_u32(e.term_count);
            w.write_u32(e.postings_bytes);
        }

        Ok(w.into_bytes())
    }

    /// Decode snapshot from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<IndexSnapshotV1, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let version = r.read_u16()?;
        if version != 1 {
            return Err(DecodeError::new("unsupported index snapshot version"));
        }
        let src = r.read_u64()?;
        let n = r.read_u32()? as usize;

        // Defensive bound: prevent huge allocations on corrupt inputs.
        if n > 10_000_000 {
            return Err(DecodeError::new("entries length too large"));
        }

        let mut entries: Vec<IndexSnapshotEntryV1> = Vec::with_capacity(n);
        let mut last_pair: Option<(Hash32, Hash32)> = None;

        for _ in 0..n {
            let seg_b = r.read_fixed(32)?;
            let idx_b = r.read_fixed(32)?;
            let mut seg = [0u8; 32];
            let mut idx = [0u8; 32];
            seg.copy_from_slice(seg_b);
            idx.copy_from_slice(idx_b);

            // Enforce canonical sort order at decode time.
            if let Some(prev) = last_pair {
                if (seg, idx) < prev {
                    return Err(DecodeError::new(
                        "entries not sorted by (frame_seg, index_seg)",
                    ));
                }
            }
            last_pair = Some((seg, idx));

            let row_count = r.read_u32()?;
            let term_count = r.read_u32()?;
            let postings_bytes = r.read_u32()?;
            entries.push(IndexSnapshotEntryV1 {
                frame_seg: seg,
                index_seg: idx,
                row_count,
                term_count,
                postings_bytes,
            });
        }

        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        Ok(IndexSnapshotV1 {
            version,
            source_id: SourceId(crate::frame::Id64(src)),
            entries,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{Id64, SourceId};

    fn h(b: u8) -> Hash32 {
        let mut x = [0u8; 32];
        x[0] = b;
        x
    }

    #[test]
    fn snapshot_round_trip() {
        let src = SourceId(Id64(9));
        let mut s = IndexSnapshotV1::new(src);
        s.entries.push(IndexSnapshotEntryV1 {
            frame_seg: h(3),
            index_seg: h(4),
            row_count: 10,
            term_count: 20,
            postings_bytes: 30,
        });
        let bytes = s.encode().unwrap();
        let s2 = IndexSnapshotV1::decode(&bytes).unwrap();
        assert_eq!(s2.version, 1);
        assert_eq!(s2.source_id, src);
        assert_eq!(s2.entries.len(), 1);
        assert_eq!(s2.entries[0].frame_seg, h(3));
    }

    #[test]
    fn snapshot_is_deterministic_under_entry_order() {
        let src = SourceId(Id64(9));
        let mut a = IndexSnapshotV1::new(src);
        a.entries.push(IndexSnapshotEntryV1 {
            frame_seg: h(7),
            index_seg: h(1),
            row_count: 1,
            term_count: 2,
            postings_bytes: 3,
        });
        a.entries.push(IndexSnapshotEntryV1 {
            frame_seg: h(2),
            index_seg: h(9),
            row_count: 4,
            term_count: 5,
            postings_bytes: 6,
        });

        let mut b = IndexSnapshotV1::new(src);
        b.entries.push(a.entries[1].clone());
        b.entries.push(a.entries[0].clone());

        assert_eq!(a.encode().unwrap(), b.encode().unwrap());
    }

    #[test]
    fn snapshot_decode_rejects_unsorted() {
        let src = SourceId(Id64(9));
        let mut s = IndexSnapshotV1::new(src);
        s.entries.push(IndexSnapshotEntryV1 {
            frame_seg: h(7),
            index_seg: h(1),
            row_count: 1,
            term_count: 2,
            postings_bytes: 3,
        });
        s.entries.push(IndexSnapshotEntryV1 {
            frame_seg: h(2),
            index_seg: h(9),
            row_count: 4,
            term_count: 5,
            postings_bytes: 6,
        });

        // Encode without canonicalize by manually writing out-of-order bytes.
        let mut w = ByteWriter::with_capacity(256);
        w.write_u16(1);
        w.write_u64(src.0 .0);
        w.write_u32(2);
        // write in existing order (unsorted)
        for e in &s.entries {
            w.write_raw(&e.frame_seg);
            w.write_raw(&e.index_seg);
            w.write_u32(e.row_count);
            w.write_u32(e.term_count);
            w.write_u32(e.postings_bytes);
        }
        let bytes = w.into_bytes();
        assert!(IndexSnapshotV1::decode(&bytes).is_err());
    }
}
