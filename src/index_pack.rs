// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! IndexPackV1: bundle multiple IndexSegmentV1 artifacts into a single artifact.
//!
//! Why this exists:
//! - IndexSnapshotV1 references index artifacts by hash.
//! - IndexSegmentV1 is per FrameSegment (seg_hash + postings with row_ix local to that segment).
//! - For compaction we want fewer index artifacts to load from disk.
//!
//! IndexPackV1 preserves exact query semantics by storing the canonical
//! IndexSegmentV1 bytes for each FrameSegment, keyed by the FrameSegment hash.
//!
//! On-disk layout (all little-endian):
//! - magic[8] = b"FSALMIPK"
//! - version u16 = 1
//! - reserved u16 = 0
//! - source_id u64
//! - n_entries u32
//! - repeated n_entries:
//! - frame_seg_hash[32]
//! - index_bytes_len u32
//! - index_bytes[index_bytes_len]
//!
//! Canonicalization rules:
//! - entries are sorted by frame_seg_hash ascending
//! - frame_seg_hash values are unique
//! - index_bytes MUST decode as IndexSegmentV1 and MUST have seg_hash == frame_seg_hash

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::frame::SourceId;
use crate::hash::Hash32;
use crate::index_segment::IndexSegmentV1;

/// Magic bytes for IndexPack v1.
pub const INDEX_PACK_MAGIC: [u8; 8] = *b"FSALMIPK";

/// Current IndexPack encoding version.
pub const INDEX_PACK_VERSION: u16 = 1;

/// One entry in an IndexPack.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexPackEntryV1 {
    /// FrameSegment hash this inner index applies to.
    pub frame_seg: Hash32,
    /// Canonical IndexSegmentV1 bytes for the frame segment.
    pub index_bytes: Vec<u8>,
}

/// Bundle of multiple IndexSegmentV1 artifacts for the same source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexPackV1 {
    /// Source/corpus identifier.
    pub source_id: SourceId,
    /// Entries in this pack.
    pub entries: Vec<IndexPackEntryV1>,
}

impl IndexPackV1 {
    /// Canonicalize the pack in-place.
    pub fn canonicalize_in_place(&mut self) {
        self.entries.sort_by(|a, b| a.frame_seg.cmp(&b.frame_seg));
    }

    /// Encode into canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        let mut tmp = self.clone();
        tmp.canonicalize_in_place();

        if tmp.entries.len() > (u32::MAX as usize) {
            return Err(EncodeError::new("too many pack entries"));
        }

        // Validate uniqueness and inner segment invariants.
        let mut last: Option<Hash32> = None;
        for e in &tmp.entries {
            if let Some(prev) = last {
                if e.frame_seg <= prev {
                    return Err(EncodeError::new("pack entries not sorted unique"));
                }
            }
            last = Some(e.frame_seg);

            let idx = IndexSegmentV1::decode(&e.index_bytes)
                .map_err(|_| EncodeError::new("pack contains invalid index segment bytes"))?;
            if idx.seg_hash != e.frame_seg {
                return Err(EncodeError::new(
                    "pack entry frame_seg does not match index seg_hash",
                ));
            }
            if idx.source_id != tmp.source_id {
                return Err(EncodeError::new("pack entry source_id mismatch"));
            }
        }

        let mut cap: usize = 32;
        for e in &tmp.entries {
            cap = cap.saturating_add(32 + 4 + e.index_bytes.len());
        }

        let mut w = ByteWriter::with_capacity(cap);
        w.write_raw(&INDEX_PACK_MAGIC);
        w.write_u16(INDEX_PACK_VERSION);
        w.write_u16(0);
        w.write_u64(tmp.source_id.0 .0);
        w.write_u32(tmp.entries.len() as u32);

        for e in &tmp.entries {
            w.write_raw(&e.frame_seg);
            if e.index_bytes.len() > (u32::MAX as usize) {
                return Err(EncodeError::new("index_bytes too large"));
            }
            w.write_u32(e.index_bytes.len() as u32);
            w.write_raw(&e.index_bytes);
        }

        Ok(w.into_bytes())
    }

    /// Decode from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<IndexPackV1, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let magic = r.read_fixed(8)?;
        if magic != INDEX_PACK_MAGIC {
            return Err(DecodeError::new("bad index pack magic"));
        }

        let ver = r.read_u16()?;
        if ver != INDEX_PACK_VERSION {
            return Err(DecodeError::new("unsupported index pack version"));
        }
        let _reserved = r.read_u16()?;

        let src_u64 = r.read_u64()?;
        let source_id = SourceId(crate::frame::Id64(src_u64));
        let n = r.read_u32()? as usize;
        if n > 10_000_000 {
            return Err(DecodeError::new("pack entries length too large"));
        }

        let mut entries: Vec<IndexPackEntryV1> = Vec::with_capacity(n);
        let mut last: Option<Hash32> = None;

        for _ in 0..n {
            let seg_b = r.read_fixed(32)?;
            let mut frame_seg = [0u8; 32];
            frame_seg.copy_from_slice(seg_b);

            if let Some(prev) = last {
                if frame_seg <= prev {
                    return Err(DecodeError::new("pack entries not sorted unique"));
                }
            }
            last = Some(frame_seg);

            let len = r.read_u32()? as usize;
            // Defensive bound: avoid allocating absurd buffers.
            if len > (512 * 1024 * 1024) {
                return Err(DecodeError::new("index_bytes_len too large"));
            }
            let idx_bytes = r.read_fixed(len)?.to_vec();

            let idx = IndexSegmentV1::decode(&idx_bytes)?;
            if idx.seg_hash != frame_seg {
                return Err(DecodeError::new(
                    "pack entry frame_seg does not match index seg_hash",
                ));
            }
            if idx.source_id != source_id {
                return Err(DecodeError::new("pack entry source_id mismatch"));
            }

            entries.push(IndexPackEntryV1 {
                frame_seg,
                index_bytes: idx_bytes,
            });
        }

        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        Ok(IndexPackV1 { source_id, entries })
    }

    /// Return the canonical IndexSegmentV1 bytes for a given FrameSegment hash.
    pub fn index_bytes_for(&self, frame_seg: &Hash32) -> Option<&[u8]> {
        // entries are sorted by frame_seg; use binary search.
        let mut lo: usize = 0;
        let mut hi: usize = self.entries.len();
        while lo < hi {
            let mid = lo + ((hi - lo) >> 1);
            let k = &self.entries[mid].frame_seg;
            if k == frame_seg {
                return Some(&self.entries[mid].index_bytes);
            }
            if k < frame_seg {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        None
    }

    /// Return the canonical IndexSegmentV1 bytes for a given FrameSegment hash.
    ///
    /// This is an alias for [`index_bytes_for`].
    pub fn get_index_bytes(&self, frame_seg: &Hash32) -> Option<&[u8]> {
        self.index_bytes_for(frame_seg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{DocId, Id64};
    use crate::frame_segment::FrameSegmentV1;
    use crate::hash::blake3_hash;

    fn mk_small_index(source_id: SourceId, term_u64: u64) -> (Hash32, IndexSegmentV1) {
        // Build a tiny FrameSegment with one row and one term, then build index.
        // IndexSegmentV1::build_from_segment requires seg_hash to be the canonical content hash
        // of the encoded FrameSegment bytes.
        let mut row = crate::frame::FrameRowV1::new(DocId(Id64(1)), source_id);
        row.terms.push(crate::frame::TermFreq {
            term: crate::frame::TermId(Id64(term_u64)),
            tf: 1,
        });
        row.recompute_doc_len();
        let seg = FrameSegmentV1::from_rows(&[row], 16).unwrap();
        let seg_bytes = seg.encode().unwrap();
        let seg_hash = blake3_hash(&seg_bytes);
        let idx = IndexSegmentV1::build_from_segment(seg_hash, &seg).unwrap();
        (seg_hash, idx)
    }

    #[test]
    fn encode_decode_roundtrip_and_lookup() {
        let sid = SourceId(Id64(7));

        let (seg_a, idx_a) = mk_small_index(sid, 101);
        let (seg_b, idx_b) = mk_small_index(sid, 202);

        let p = IndexPackV1 {
            source_id: sid,
            entries: vec![
                IndexPackEntryV1 {
                    frame_seg: seg_b,
                    index_bytes: idx_b.encode().unwrap(),
                },
                IndexPackEntryV1 {
                    frame_seg: seg_a,
                    index_bytes: idx_a.encode().unwrap(),
                },
            ],
        };

        let bytes = p.encode().unwrap();
        let dec = IndexPackV1::decode(&bytes).unwrap();
        assert_eq!(dec.source_id, sid);
        assert_eq!(dec.entries.len(), 2);

        // canonical order
        assert_eq!(dec.entries[0].frame_seg, seg_a);
        assert_eq!(dec.entries[1].frame_seg, seg_b);

        let b = dec.index_bytes_for(&seg_b).unwrap();
        let idx_b2 = IndexSegmentV1::decode(b).unwrap();
        assert_eq!(idx_b2.seg_hash, seg_b);
        assert_eq!(idx_b2.source_id, sid);
    }

    #[test]
    fn reject_non_unique_or_unsorted_entries() {
        let sid = SourceId(Id64(7));
        let (seg_a, idx_a) = mk_small_index(sid, 101);
        let idx_bytes = idx_a.encode().unwrap();

        let p = IndexPackV1 {
            source_id: sid,
            entries: vec![
                IndexPackEntryV1 {
                    frame_seg: seg_a,
                    index_bytes: idx_bytes.clone(),
                },
                IndexPackEntryV1 {
                    frame_seg: seg_a,
                    index_bytes: idx_bytes,
                },
            ],
        };

        assert!(p.encode().is_err());
    }
}
