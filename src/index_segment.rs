// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! IndexSegment v1: per-segment postings index for FrameSegment rows.
//!
//! This module builds a deterministic inverted index over a single FrameSegment.
//! It is designed for CPU-only operation with minimal dependencies.
//!
//! Goals:
//! - Deterministic, canonical bytes (bitwise stable within a build).
//! - Integer-only scoring primitives (no floats).
//! - Streaming-friendly decode for query-time lookups (postings are not expanded unless requested).
//!
//! High-level layout (canonical bytes):
//! - MAGIC[8] + version(u16) + reserved(u16)
//! - seg_hash[32]
//! - source_id(u64)
//! - row_count(u32)
//! - term_count(u32)
//! - term dictionary entries (sorted by term id asc):
//! term_id(u64), postings_off(u32), postings_len(u32), df(u32), tf_sum(u32)
//! - postings_total_len(u32)
//! - postings blob bytes (concatenated per term in dictionary order)
//!
//! Postings encoding per term:
//! - a sequence of (row_delta_varint_u32, tf_varint_u32) pairs
//! - row indices are absolute row positions within the FrameSegment (0..row_count)
//! - row_delta is delta from previous row index for this term (first delta is row_ix + 1)
//!
//! Notes:
//! - v1 requires all rows in the segment to share the same source_id for simpler routing.
//! - term ids are the u64 payload of TermId(Id64(u64)).

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::frame::{SourceId, TermId};
use crate::frame_segment::FrameSegmentV1;
use crate::hash::Hash32;

/// Domain separator for IndexSegment content addressing.
pub const DOMAIN_INDEX_SEGMENT: &[u8] = b"idx\0";

const INDEX_SEGMENT_MAGIC: [u8; 8] = *b"FSALMIDX";
const INDEX_SEGMENT_VERSION: u16 = 1;

/// A single term dictionary entry for IndexSegment v1.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexTermEntryV1 {
    /// Term identifier.
    pub term: TermId,
    /// Offset into the postings blob (bytes).
    pub postings_off: u32,
    /// Length of this term's postings payload (bytes).
    pub postings_len: u32,
    /// Document frequency (number of rows that contain the term).
    pub df: u32,
    /// Total term frequency summed across rows.
    pub tf_sum: u32,
}

/// A postings item decoded from the postings blob.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PostingV1 {
    /// Absolute row index within the segment.
    pub row_ix: u32,
    /// Term frequency in that row.
    pub tf: u32,
}

/// IndexSegment v1: deterministic postings index for a FrameSegment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexSegmentV1 {
    /// Hash of the source FrameSegment bytes this index was built from.
    pub seg_hash: Hash32,
    /// Source/corpus identifier for all rows in the segment.
    pub source_id: SourceId,
    /// Total number of rows in the segment.
    pub row_count: u32,
    /// Dictionary entries sorted by term id ascending.
    pub terms: Vec<IndexTermEntryV1>,
    /// Concatenated postings blob.
    pub postings: Vec<u8>,
}

/// Errors during IndexSegment build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexBuildError {
    /// The FrameSegment contains no rows.
    EmptySegment,
    /// The FrameSegment mixes multiple source_id values (not allowed in v1).
    MixedSourceId,
    /// The FrameSegment has too many rows for v1 (u32 overflow).
    TooManyRows,
    /// The postings blob would exceed u32 offsets/lengths.
    PostingsTooLarge,
}

impl core::fmt::Display for IndexBuildError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            IndexBuildError::EmptySegment => f.write_str("empty segment"),
            IndexBuildError::MixedSourceId => f.write_str("mixed source_id in segment"),
            IndexBuildError::TooManyRows => f.write_str("too many rows for v1"),
            IndexBuildError::PostingsTooLarge => f.write_str("postings blob too large for v1"),
        }
    }
}

impl IndexSegmentV1 {
    /// Build an IndexSegmentV1 from a decoded FrameSegmentV1.
    ///
    /// `seg_hash` should be the content hash of the canonical FrameSegment bytes.
    pub fn build_from_segment(
        seg_hash: Hash32,
        seg: &FrameSegmentV1,
    ) -> Result<IndexSegmentV1, IndexBuildError> {
        let mut row_count_u64: u64 = 0;
        for c in &seg.chunks {
            row_count_u64 = row_count_u64.saturating_add(c.rows as u64);
        }
        if row_count_u64 == 0 {
            return Err(IndexBuildError::EmptySegment);
        }
        if row_count_u64 > (u32::MAX as u64) {
            return Err(IndexBuildError::TooManyRows);
        }
        let row_count = row_count_u64 as u32;

        // Determine and validate source_id (v1 requires uniform source_id).
        let mut source_id_u64: Option<u64> = None;
        for c in &seg.chunks {
            for &sid in &c.source_id {
                match source_id_u64 {
                    None => source_id_u64 = Some(sid),
                    Some(x) => {
                        if x != sid {
                            return Err(IndexBuildError::MixedSourceId);
                        }
                    }
                }
            }
        }
        let source_id_u64 = source_id_u64.ok_or(IndexBuildError::EmptySegment)?;
        let source_id = SourceId(crate::frame::Id64(source_id_u64));

        // Collect postings as a flat list: (term_u64, row_ix_u32, tf_u32).
        // We sort once and then scan to build the dictionary and postings blob.
        let mut est: usize = 0;
        for c in &seg.chunks {
            est = est.saturating_add(c.term_id_pool.len());
        }
        let mut flat: Vec<(u64, u32, u32)> = Vec::with_capacity(est);

        let mut base_row_ix: u32 = 0;
        for c in &seg.chunks {
            let rows = c.rows as usize;
            for i in 0..rows {
                let row_ix = base_row_ix + (i as u32);
                let off = c.term_offs[i] as usize;
                let len = c.term_lens[i] as usize;
                // term lists are canonicalized by FrameSegment builder.
                for j in 0..len {
                    let term_u64 = c.term_id_pool[off + j];
                    let tf = c.term_tf_pool[off + j];
                    if tf == 0 {
                        continue;
                    }
                    flat.push((term_u64, row_ix, tf));
                }
            }
            base_row_ix = base_row_ix.saturating_add(c.rows);
        }

        // Sort by (term, row_ix) for deterministic encoding.
        flat.sort_by(|a, b| {
            let (ta, ra, _tfa) = *a;
            let (tb, rb, _tfb) = *b;
            match ta.cmp(&tb) {
                core::cmp::Ordering::Equal => ra.cmp(&rb),
                other => other,
            }
        });

        // Build postings blob and dictionary by scanning sorted flat postings.
        let mut postings: Vec<u8> = Vec::with_capacity(flat.len().saturating_mul(3));
        let mut terms: Vec<IndexTermEntryV1> = Vec::new();

        let mut i = 0usize;
        while i < flat.len() {
            let term_u64 = flat[i].0;
            let postings_off_usize = postings.len();
            let mut prev_row: u32 = 0;
            let mut df: u32 = 0;
            let mut tf_sum_u64: u64 = 0;

            while i < flat.len() && flat[i].0 == term_u64 {
                let row_ix = flat[i].1;
                let tf = flat[i].2;

                let delta = if df == 0 {
                    row_ix.saturating_add(1)
                } else {
                    match row_ix.checked_sub(prev_row) {
                        Some(d) => d.saturating_add(1),
                        None => return Err(IndexBuildError::PostingsTooLarge),
                    }
                };
                write_var_u32_vec(&mut postings, delta);
                write_var_u32_vec(&mut postings, tf);

                prev_row = row_ix;
                df = df.saturating_add(1);
                tf_sum_u64 = tf_sum_u64.saturating_add(tf as u64);
                i += 1;
            }

            let postings_len_usize = postings.len().saturating_sub(postings_off_usize);
            if postings_off_usize > (u32::MAX as usize) || postings_len_usize > (u32::MAX as usize)
            {
                return Err(IndexBuildError::PostingsTooLarge);
            }

            let tf_sum = if tf_sum_u64 > (u32::MAX as u64) {
                u32::MAX
            } else {
                tf_sum_u64 as u32
            };

            terms.push(IndexTermEntryV1 {
                term: TermId(crate::frame::Id64(term_u64)),
                postings_off: postings_off_usize as u32,
                postings_len: postings_len_usize as u32,
                df,
                tf_sum,
            });
        }

        Ok(IndexSegmentV1 {
            seg_hash,
            source_id,
            row_count,
            terms,
            postings,
        })
    }

    /// Encode this IndexSegmentV1 into canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        let mut w = ByteWriter::with_capacity(128 + self.terms.len() * 24 + self.postings.len());
        w.write_raw(&INDEX_SEGMENT_MAGIC);
        w.write_u16(INDEX_SEGMENT_VERSION);
        w.write_u16(0);

        w.write_raw(&self.seg_hash);
        w.write_u64((self.source_id.0).0);
        w.write_u32(self.row_count);
        if self.terms.len() > (u32::MAX as usize) {
            return Err(EncodeError::new("term dict too large"));
        }
        w.write_u32(self.terms.len() as u32);

        for e in &self.terms {
            w.write_u64((e.term.0).0);
            w.write_u32(e.postings_off);
            w.write_u32(e.postings_len);
            w.write_u32(e.df);
            w.write_u32(e.tf_sum);
        }

        if self.postings.len() > (u32::MAX as usize) {
            return Err(EncodeError::new("postings too large"));
        }
        w.write_u32(self.postings.len() as u32);
        w.write_raw(&self.postings);

        Ok(w.into_bytes())
    }

    /// Decode IndexSegmentV1 from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<IndexSegmentV1, DecodeError> {
        let mut r = ByteReader::new(bytes);

        let magic = r.read_fixed(8)?;
        if magic != INDEX_SEGMENT_MAGIC {
            return Err(DecodeError::new("bad index segment magic"));
        }

        let ver = r.read_u16()?;
        if ver != INDEX_SEGMENT_VERSION {
            return Err(DecodeError::new("unsupported index segment version"));
        }
        let _reserved = r.read_u16()?;

        let seg_hash_b = r.read_fixed(32)?;
        let mut seg_hash = [0u8; 32];
        seg_hash.copy_from_slice(seg_hash_b);

        let source_id_u64 = r.read_u64()?;
        let source_id = SourceId(crate::frame::Id64(source_id_u64));

        let row_count = r.read_u32()?;
        let term_count = r.read_u32()? as usize;

        let mut terms: Vec<IndexTermEntryV1> = Vec::with_capacity(term_count);
        let mut last_term: Option<u64> = None;
        for _ in 0..term_count {
            let term_u64 = r.read_u64()?;
            if let Some(prev) = last_term {
                if term_u64 < prev {
                    return Err(DecodeError::new("term dict not sorted"));
                }
            }
            last_term = Some(term_u64);

            let postings_off = r.read_u32()?;
            let postings_len = r.read_u32()?;
            let df = r.read_u32()?;
            let tf_sum = r.read_u32()?;
            terms.push(IndexTermEntryV1 {
                term: TermId(crate::frame::Id64(term_u64)),
                postings_off,
                postings_len,
                df,
                tf_sum,
            });
        }

        let postings_total = r.read_u32()? as usize;
        let postings = r.read_fixed(postings_total)?.to_vec();
        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes after postings"));
        }

        // Basic bounds validation: dictionary ranges must fit the postings blob.
        for e in &terms {
            let off = e.postings_off as usize;
            let len = e.postings_len as usize;
            if off > postings.len() || off + len > postings.len() {
                return Err(DecodeError::new("postings range out of bounds"));
            }
        }

        Ok(IndexSegmentV1 {
            seg_hash,
            source_id,
            row_count,
            terms,
            postings,
        })
    }

    /// Create a postings iterator for a dictionary entry index.
    pub fn postings_iter(&self, term_ix: usize) -> Result<PostingsIterV1<'_>, DecodeError> {
        if term_ix >= self.terms.len() {
            return Err(DecodeError::new("term index out of bounds"));
        }
        let e = &self.terms[term_ix];
        let off = e.postings_off as usize;
        let len = e.postings_len as usize;
        Ok(PostingsIterV1::new(&self.postings[off..off + len]))
    }
}

/// Iterator over postings payload bytes for a single term.
#[derive(Clone, Debug)]
pub struct PostingsIterV1<'a> {
    buf: &'a [u8],
    pos: usize,
    prev_row: u32,
}

impl<'a> PostingsIterV1<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self {
            buf,
            pos: 0,
            prev_row: 0,
        }
    }

    fn read_var_u32(&mut self) -> Result<u32, DecodeError> {
        let mut shift: u32 = 0;
        let mut out: u32 = 0;
        loop {
            if self.pos >= self.buf.len() {
                return Err(DecodeError::new("unexpected EOF in varint"));
            }
            let b = self.buf[self.pos];
            self.pos += 1;
            out |= ((b & 0x7F) as u32) << shift;
            if (b & 0x80) == 0 {
                return Ok(out);
            }
            shift = shift.saturating_add(7);
            if shift >= 32 {
                return Err(DecodeError::new("varint too large"));
            }
        }
    }
}

impl<'a> Iterator for PostingsIterV1<'a> {
    type Item = Result<PostingV1, DecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.buf.len() {
            return None;
        }
        let delta = match self.read_var_u32() {
            Ok(v) => v,
            Err(e) => return Some(Err(e)),
        };
        let tf = match self.read_var_u32() {
            Ok(v) => v,
            Err(e) => return Some(Err(e)),
        };

        // First delta is encoded as row_ix + 1 so that 0 can be rejected and a
        // first row index of 0 is representable.
        if delta == 0 {
            return Some(Err(DecodeError::new("row delta must be >= 1")));
        }
        let row_ix = self.prev_row.saturating_add(delta - 1);
        self.prev_row = row_ix;

        Some(Ok(PostingV1 { row_ix, tf }))
    }
}

/// Write varint u32 into a Vec<u8>.
fn write_var_u32_vec(out: &mut Vec<u8>, mut v: u32) {
    while v >= 0x80 {
        out.push(((v as u8) & 0x7F) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{DocId, Id64, TermFreq};

    fn tid(x: u64) -> TermId {
        TermId(Id64(x))
    }

    #[test]
    fn index_segment_round_trip_encode_decode() {
        // Build a tiny FrameSegment with a few terms.
        let src = SourceId(Id64(7));
        let mut r1 = crate::frame::FrameRowV1::new(DocId(Id64(1)), src);
        r1.terms.push(TermFreq {
            term: tid(10),
            tf: 2,
        });
        r1.terms.push(TermFreq {
            term: tid(11),
            tf: 1,
        });
        r1.doc_len = 3;

        let mut r2 = crate::frame::FrameRowV1::new(DocId(Id64(2)), src);
        r2.terms.push(TermFreq {
            term: tid(10),
            tf: 1,
        });
        r2.terms.push(TermFreq {
            term: tid(12),
            tf: 5,
        });
        r2.doc_len = 6;

        let seg = FrameSegmentV1::from_rows(&[r1, r2], 8).unwrap();
        let seg_bytes = seg.encode().unwrap();
        let seg_hash = crate::hash::blake3_hash(&seg_bytes);

        let idx = IndexSegmentV1::build_from_segment(seg_hash, &seg).unwrap();
        let bytes = idx.encode().unwrap();
        let idx2 = IndexSegmentV1::decode(&bytes).unwrap();
        assert_eq!(idx, idx2);
    }

    #[test]
    fn index_segment_is_deterministic() {
        let src = SourceId(Id64(7));
        let mut r1 = crate::frame::FrameRowV1::new(DocId(Id64(1)), src);
        r1.terms.push(TermFreq {
            term: tid(10),
            tf: 2,
        });
        r1.terms.push(TermFreq {
            term: tid(11),
            tf: 1,
        });
        r1.doc_len = 3;

        let mut r2 = crate::frame::FrameRowV1::new(DocId(Id64(2)), src);
        r2.terms.push(TermFreq {
            term: tid(10),
            tf: 1,
        });
        r2.terms.push(TermFreq {
            term: tid(12),
            tf: 5,
        });
        r2.doc_len = 6;

        let seg = FrameSegmentV1::from_rows(&[r1, r2], 8).unwrap();
        let seg_hash = crate::hash::blake3_hash(&seg.encode().unwrap());

        let idx_a = IndexSegmentV1::build_from_segment(seg_hash, &seg).unwrap();
        let idx_b = IndexSegmentV1::build_from_segment(seg_hash, &seg).unwrap();
        assert_eq!(idx_a.encode().unwrap(), idx_b.encode().unwrap());
    }

    #[test]
    fn postings_iter_decodes_expected_rows() {
        let src = SourceId(Id64(7));
        let mut r1 = crate::frame::FrameRowV1::new(DocId(Id64(1)), src);
        r1.terms.push(TermFreq {
            term: tid(10),
            tf: 2,
        });
        r1.terms.push(TermFreq {
            term: tid(11),
            tf: 1,
        });
        r1.doc_len = 3;

        let mut r2 = crate::frame::FrameRowV1::new(DocId(Id64(2)), src);
        r2.terms.push(TermFreq {
            term: tid(10),
            tf: 1,
        });
        r2.terms.push(TermFreq {
            term: tid(12),
            tf: 5,
        });
        r2.doc_len = 6;

        let seg = FrameSegmentV1::from_rows(&[r1, r2], 8).unwrap();
        let seg_hash = crate::hash::blake3_hash(&seg.encode().unwrap());
        let idx = IndexSegmentV1::build_from_segment(seg_hash, &seg).unwrap();

        // term 10 should appear in row 0 and row 1.
        let pos = idx.terms.iter().position(|e| (e.term.0).0 == 10).unwrap();
        let posts: Vec<PostingV1> = idx
            .postings_iter(pos)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(
            posts,
            vec![
                PostingV1 { row_ix: 0, tf: 2 },
                PostingV1 { row_ix: 1, tf: 1 }
            ]
        );
    }
}
