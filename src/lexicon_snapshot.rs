// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! LexiconSnapshot v1: manifest linking LexiconSegment hashes and basic counts.
//!
//! LexiconSnapshot is a small deterministic inventory that allows query-time
//! systems to discover which LexiconSegment artifacts are available without
//! scanning or decoding the full set of lexicon artifacts.
//!
//! This stage only defines the snapshot schema and canonical encoding.
//! Higher-level indexes (meta-code postings, adjacency, text tables) are
//! introduced in later stages.

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::hash::Hash32;

/// A single LexiconSegment entry in LexiconSnapshot v1.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LexiconSnapshotEntryV1 {
    /// Hash of the LexiconSegment bytes.
    pub lex_seg: Hash32,
    /// Lemma row count from the segment.
    pub lemma_count: u32,
    /// Sense row count from the segment.
    pub sense_count: u32,
    /// Relation edge row count from the segment.
    pub rel_count: u32,
    /// Pronunciation row count from the segment.
    pub pron_count: u32,
}

/// LexiconSnapshot manifest v1.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LexiconSnapshotV1 {
    /// Manifest version (currently 1).
    pub version: u16,
    /// Entries sorted by lex_seg hash ascending.
    pub entries: Vec<LexiconSnapshotEntryV1>,
}

impl LexiconSnapshotV1 {
    /// Create an empty snapshot.
    pub fn new() -> Self {
        LexiconSnapshotV1 {
            version: 1,
            entries: Vec::new(),
        }
    }

    /// Canonicalize the snapshot in-place for deterministic encoding.
    ///
    /// Rules:
    /// - entries sorted by lex_seg ascending.
    pub fn canonicalize_in_place(&mut self) {
        self.entries.sort_by(|a, b| a.lex_seg.cmp(&b.lex_seg));
    }

    /// Encode snapshot into canonical bytes.
    ///
    /// Encoding canonicalizes entries by sorting and rejects duplicate hashes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        let mut tmp = self.clone();
        tmp.canonicalize_in_place();

        // Reject duplicates after canonicalize.
        for i in 1..tmp.entries.len() {
            if tmp.entries[i - 1].lex_seg == tmp.entries[i].lex_seg {
                return Err(EncodeError::new("duplicate lexicon segment hash"));
            }
        }

        let mut w = ByteWriter::with_capacity(16 + (tmp.entries.len() * 48));
        w.write_u16(tmp.version);
        if tmp.entries.len() > (u32::MAX as usize) {
            return Err(EncodeError::new("too many entries"));
        }
        w.write_u32(tmp.entries.len() as u32);

        for e in &tmp.entries {
            w.write_raw(&e.lex_seg);
            w.write_u32(e.lemma_count);
            w.write_u32(e.sense_count);
            w.write_u32(e.rel_count);
            w.write_u32(e.pron_count);
        }

        Ok(w.into_bytes())
    }

    /// Decode snapshot from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<LexiconSnapshotV1, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let version = r.read_u16()?;
        if version != 1 {
            return Err(DecodeError::new("unsupported lexicon snapshot version"));
        }

        let n = r.read_u32()? as usize;
        // Defensive bound: prevent huge allocations on corrupt inputs.
        if n > 10_000_000 {
            return Err(DecodeError::new("entries length too large"));
        }

        let mut entries: Vec<LexiconSnapshotEntryV1> = Vec::with_capacity(n);
        let mut last_seg: Option<Hash32> = None;

        for _ in 0..n {
            let seg_b = r.read_fixed(32)?;
            let mut seg = [0u8; 32];
            seg.copy_from_slice(seg_b);

            // Enforce canonical sort order and uniqueness at decode time.
            if let Some(prev) = last_seg {
                if seg <= prev {
                    if seg == prev {
                        return Err(DecodeError::new("duplicate lex_seg hash"));
                    }
                    return Err(DecodeError::new("entries not sorted by lex_seg"));
                }
            }
            last_seg = Some(seg);

            let lemma_count = r.read_u32()?;
            let sense_count = r.read_u32()?;
            let rel_count = r.read_u32()?;
            let pron_count = r.read_u32()?;
            entries.push(LexiconSnapshotEntryV1 {
                lex_seg: seg,
                lemma_count,
                sense_count,
                rel_count,
                pron_count,
            });
        }

        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        Ok(LexiconSnapshotV1 { version, entries })
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
    fn snapshot_round_trip() {
        let mut s = LexiconSnapshotV1::new();
        s.entries.push(LexiconSnapshotEntryV1 {
            lex_seg: h(3),
            lemma_count: 10,
            sense_count: 20,
            rel_count: 30,
            pron_count: 40,
        });
        let bytes = s.encode().unwrap();
        let s2 = LexiconSnapshotV1::decode(&bytes).unwrap();
        assert_eq!(s2.version, 1);
        assert_eq!(s2.entries.len(), 1);
        assert_eq!(s2.entries[0].lex_seg, h(3));
    }

    #[test]
    fn snapshot_is_deterministic_under_entry_order() {
        let mut a = LexiconSnapshotV1::new();
        a.entries.push(LexiconSnapshotEntryV1 {
            lex_seg: h(7),
            lemma_count: 1,
            sense_count: 2,
            rel_count: 3,
            pron_count: 4,
        });
        a.entries.push(LexiconSnapshotEntryV1 {
            lex_seg: h(2),
            lemma_count: 5,
            sense_count: 6,
            rel_count: 7,
            pron_count: 8,
        });

        let mut b = LexiconSnapshotV1::new();
        b.entries.push(a.entries[1].clone());
        b.entries.push(a.entries[0].clone());

        assert_eq!(a.encode().unwrap(), b.encode().unwrap());
    }

    #[test]
    fn snapshot_decode_rejects_unsorted() {
        let mut s = LexiconSnapshotV1::new();
        s.entries.push(LexiconSnapshotEntryV1 {
            lex_seg: h(7),
            lemma_count: 1,
            sense_count: 2,
            rel_count: 3,
            pron_count: 4,
        });
        s.entries.push(LexiconSnapshotEntryV1 {
            lex_seg: h(2),
            lemma_count: 5,
            sense_count: 6,
            rel_count: 7,
            pron_count: 8,
        });

        // Encode without canonicalize by manually writing out-of-order bytes.
        let mut w = ByteWriter::with_capacity(128);
        w.write_u16(1);
        w.write_u32(2);
        for e in &s.entries {
            w.write_raw(&e.lex_seg);
            w.write_u32(e.lemma_count);
            w.write_u32(e.sense_count);
            w.write_u32(e.rel_count);
            w.write_u32(e.pron_count);
        }
        let bytes = w.into_bytes();
        assert!(LexiconSnapshotV1::decode(&bytes).is_err());
    }

    #[test]
    fn snapshot_decode_rejects_duplicate_hash() {
        let mut w = ByteWriter::with_capacity(128);
        w.write_u16(1);
        w.write_u32(2);
        w.write_raw(&h(9));
        w.write_u32(1);
        w.write_u32(2);
        w.write_u32(3);
        w.write_u32(4);
        w.write_raw(&h(9));
        w.write_u32(5);
        w.write_u32(6);
        w.write_u32(7);
        w.write_u32(8);
        let bytes = w.into_bytes();
        assert!(LexiconSnapshotV1::decode(&bytes).is_err());
    }

    #[test]
    fn snapshot_decode_rejects_trailing_bytes() {
        let mut s = LexiconSnapshotV1::new();
        s.entries.push(LexiconSnapshotEntryV1 {
            lex_seg: h(1),
            lemma_count: 0,
            sense_count: 0,
            rel_count: 0,
            pron_count: 0,
        });
        let mut bytes = s.encode().unwrap();
        bytes.push(0);
        assert!(LexiconSnapshotV1::decode(&bytes).is_err());
    }
}
