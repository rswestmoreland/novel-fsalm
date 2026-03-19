// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! FrameSegment on-disk format.
//!
//! A FrameSegment stores knowledge frames in a disk-first, columnar layout.
//! This stage focuses on correctness, determinism, and a layout that can scale
//! beyond RAM by allowing segment-level and chunk-level processing.
//!
//! Notes:
//! - This v1 format is intentionally simple: fixed-width arrays for fast access.
//! - Compression (delta/varint, dictionary coding) can be added in later stages
//! without changing the logical schema.
//! - All numeric fields are encoded in little-endian, with explicit length
//! prefixes for each column block.
//!
//! Design goals for v1:
//! - Bitwise deterministic encoding.
//! - Chunked layout to support bounded-memory ingestion and future partial reads.
//! - No external crates beyond those already in the project (hashing crate is OK;
//! we do not use it here).

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::frame::{
    ConfidenceQ16, DocId, EntityId, FrameRowV1, Id64, Polarity, SectionId, SourceId, TermFreq,
    TermId, VerbId, WhereId,
};

/// Magic bytes for FrameSegment v1.
pub const FRAME_SEGMENT_MAGIC: [u8; 8] = *b"FSALMFRS";

/// Current FrameSegment encoding version.
pub const FRAME_SEGMENT_VERSION: u16 = 1;

/// A columnar segment storing knowledge frame rows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrameSegmentV1 {
    /// Maximum rows per chunk. Encoding preserves this value.
    pub chunk_rows: u32,
    /// Chunks in this segment.
    pub chunks: Vec<FrameChunkV1>,
}

impl FrameSegmentV1 {
    /// Build a segment from row-oriented data.
    ///
    /// This constructor performs light canonicalization for determinism:
    /// - entity_ids are sorted ascending by id
    /// - terms are sorted ascending by term id
    /// - doc_len is recomputed as sum(tf) (clamped to u32::MAX)
    pub fn from_rows(
        rows: &[FrameRowV1],
        chunk_rows: u32,
    ) -> Result<FrameSegmentV1, FrameSegmentError> {
        let cr = if chunk_rows == 0 { 1024 } else { chunk_rows };
        let mut chunks: Vec<FrameChunkV1> = Vec::new();

        let mut i = 0usize;
        while i < rows.len() {
            let end = core::cmp::min(rows.len(), i + (cr as usize));
            let chunk = FrameChunkV1::from_rows(&rows[i..end])?;
            chunks.push(chunk);
            i = end;
        }

        Ok(FrameSegmentV1 {
            chunk_rows: cr,
            chunks,
        })
    }

    /// Encode the segment to canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        // Conservative reservation: header + per-chunk overhead.
        let mut w = ByteWriter::with_capacity(64);
        w.write_raw(&FRAME_SEGMENT_MAGIC);
        w.write_u16(FRAME_SEGMENT_VERSION);
        w.write_u16(0);
        w.write_u32(self.chunk_rows);
        if self.chunks.len() > (u32::MAX as usize) {
            return Err(EncodeError::new("too many chunks"));
        }
        w.write_u32(self.chunks.len() as u32);

        for c in &self.chunks {
            c.encode_into(&mut w)?;
        }

        Ok(w.into_bytes())
    }

    /// Decode a segment from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<FrameSegmentV1, DecodeError> {
        let mut r = ByteReader::new(bytes);

        let magic = r.read_fixed(8)?;
        if magic != FRAME_SEGMENT_MAGIC {
            return Err(DecodeError::new("bad frame segment magic"));
        }

        let ver = r.read_u16()?;
        if ver != FRAME_SEGMENT_VERSION {
            return Err(DecodeError::new("unsupported frame segment version"));
        }

        let _reserved = r.read_u16()?;
        let chunk_rows = r.read_u32()?;
        let n_chunks = r.read_u32()? as usize;

        let mut chunks: Vec<FrameChunkV1> = Vec::with_capacity(n_chunks);
        for _ in 0..n_chunks {
            let c = FrameChunkV1::decode_from(&mut r)?;
            chunks.push(c);
        }

        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        Ok(FrameSegmentV1 { chunk_rows, chunks })
    }

    /// Expand the segment into row-oriented data.
    ///
    /// This is primarily intended for tests and tooling.
    pub fn to_rows(&self) -> Vec<FrameRowV1> {
        let mut out: Vec<FrameRowV1> = Vec::new();
        for c in &self.chunks {
            c.push_rows(&mut out);
        }
        out
    }

    /// Return the total number of rows in this segment (sum of chunk row counts).
    pub fn row_count(&self) -> u64 {
        let mut n: u64 = 0;
        for c in &self.chunks {
            n = n.saturating_add(c.rows as u64);
        }
        n
    }

    /// Fetch a single row by absolute row index without expanding the whole segment.
    ///
    /// Returns None if the index is out of range or if the underlying packed
    /// columns are inconsistent. This is intended for retrieval/evidence paths
    /// that must remain bounded-memory.
    pub fn get_row(&self, row_ix: u32) -> Option<FrameRowV1> {
        let mut idx = row_ix as u64;
        for c in &self.chunks {
            let cr = c.rows as u64;
            if idx < cr {
                return c.row_at(idx as usize);
            }
            idx -= cr;
        }
        None
    }
}

/// A chunk of rows within a FrameSegment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrameChunkV1 {
    /// Number of rows in this chunk.
    pub rows: u32,

    /// doc_id per row (u64 payload of Id64).
    pub doc_id: Vec<u64>,
    /// source_id per row.
    pub source_id: Vec<u64>,
    /// when_ns per row.
    pub when_ns: Vec<i128>,

    /// Optional section_id bitmap and values.
    pub section_id_bitmap: Vec<u8>,
    /// Values for present section_id entries (row order).
    pub section_id_values: Vec<u64>,

    /// Optional where_id bitmap and values.
    pub where_id_bitmap: Vec<u8>,
    /// Values for present where_id entries (row order).
    pub where_id_values: Vec<u64>,

    /// Optional who bitmap and values.
    pub who_bitmap: Vec<u8>,
    /// Values for present who entries (row order).
    pub who_values: Vec<u64>,

    /// Optional what bitmap and values.
    pub what_bitmap: Vec<u8>,
    /// Values for present what entries (row order).
    pub what_values: Vec<u64>,

    /// Optional verb bitmap and values.
    pub verb_bitmap: Vec<u8>,
    /// Values for present verb entries (row order).
    pub verb_values: Vec<u64>,

    /// polarity per row (i8 stored as u8).
    pub polarity: Vec<u8>,
    /// confidence per row (Q16.16 stored as u32).
    pub confidence: Vec<u32>,
    /// doc_len per row.
    pub doc_len: Vec<u32>,

    /// entity_ids list index per row (offset, len) and pool of ids.
    pub entity_offs: Vec<u32>,
    /// Entity list length per row (elements).
    pub entity_lens: Vec<u32>,
    /// Contiguous pool of entity ids for all rows (u64).
    pub entity_pool: Vec<u64>,

    /// terms list index per row (offset, len) and pools of term id and tf.
    pub term_offs: Vec<u32>,
    /// Term list length per row (elements).
    pub term_lens: Vec<u32>,
    /// Contiguous pool of term ids for all rows (u64).
    pub term_id_pool: Vec<u64>,
    /// Contiguous pool of term frequencies for all rows (u32).
    pub term_tf_pool: Vec<u32>,
}

impl FrameChunkV1 {
    fn from_rows(rows: &[FrameRowV1]) -> Result<FrameChunkV1, FrameSegmentError> {
        if rows.len() > (u32::MAX as usize) {
            return Err(FrameSegmentError::TooManyRows);
        }
        let n = rows.len();
        let rows_u32 = n as u32;

        let mut doc_id = Vec::with_capacity(n);
        let mut source_id = Vec::with_capacity(n);
        let mut when_ns = Vec::with_capacity(n);

        let mut polarity = Vec::with_capacity(n);
        let mut confidence = Vec::with_capacity(n);
        let mut doc_len = Vec::with_capacity(n);

        let mut section_bits = BitsetBuilder::new(n);
        let mut section_vals: Vec<u64> = Vec::new();

        let mut where_bits = BitsetBuilder::new(n);
        let mut where_vals: Vec<u64> = Vec::new();

        let mut who_bits = BitsetBuilder::new(n);
        let mut who_vals: Vec<u64> = Vec::new();

        let mut what_bits = BitsetBuilder::new(n);
        let mut what_vals: Vec<u64> = Vec::new();

        let mut verb_bits = BitsetBuilder::new(n);
        let mut verb_vals: Vec<u64> = Vec::new();

        let mut entity_offs: Vec<u32> = Vec::with_capacity(n);
        let mut entity_lens: Vec<u32> = Vec::with_capacity(n);
        let mut entity_pool: Vec<u64> = Vec::new();

        let mut term_offs: Vec<u32> = Vec::with_capacity(n);
        let mut term_lens: Vec<u32> = Vec::with_capacity(n);
        let mut term_id_pool: Vec<u64> = Vec::new();
        let mut term_tf_pool: Vec<u32> = Vec::new();

        for (idx, row) in rows.iter().enumerate() {
            doc_id.push(row.doc_id.0 .0);
            source_id.push(row.source_id.0 .0);
            when_ns.push(row.when_ns);

            section_bits.push_opt_u64(idx, row.section_id.map(|v| v.0 .0), &mut section_vals);
            where_bits.push_opt_u64(idx, row.where_id.map(|v| v.0 .0), &mut where_vals);
            who_bits.push_opt_u64(idx, row.who.map(|v| v.0 .0), &mut who_vals);
            what_bits.push_opt_u64(idx, row.what.map(|v| v.0 .0), &mut what_vals);
            verb_bits.push_opt_u64(idx, row.verb.map(|v| v.0 .0), &mut verb_vals);

            polarity.push(row.polarity.as_i8() as u8);
            confidence.push(row.confidence.0);

            // Canonicalize entity_ids: stable sort by id.
            let mut eids: Vec<u64> = Vec::with_capacity(row.entity_ids.len());
            for e in &row.entity_ids {
                eids.push(e.0 .0);
            }
            eids.sort_unstable();

            let e_off = entity_pool.len();
            if e_off > (u32::MAX as usize) {
                return Err(FrameSegmentError::PoolTooLarge);
            }
            entity_offs.push(e_off as u32);

            if eids.len() > (u32::MAX as usize) {
                return Err(FrameSegmentError::PoolTooLarge);
            }
            entity_lens.push(eids.len() as u32);
            entity_pool.extend_from_slice(&eids);

            // Canonicalize terms: stable sort by term id.
            let mut tids: Vec<(u64, u32)> = Vec::with_capacity(row.terms.len());
            let mut sum_tf: u64 = 0;
            for t in &row.terms {
                let id = t.term.0 .0;
                tids.push((id, t.tf));
                sum_tf += t.tf as u64;
            }
            tids.sort_unstable_by(|a, b| a.0.cmp(&b.0));

            let t_off = term_id_pool.len();
            if t_off > (u32::MAX as usize) {
                return Err(FrameSegmentError::PoolTooLarge);
            }
            term_offs.push(t_off as u32);

            if tids.len() > (u32::MAX as usize) {
                return Err(FrameSegmentError::PoolTooLarge);
            }
            term_lens.push(tids.len() as u32);
            for (id, tf) in tids {
                term_id_pool.push(id);
                term_tf_pool.push(tf);
            }

            // Recompute doc_len from terms (clamp).
            let dl = if sum_tf > (u32::MAX as u64) {
                u32::MAX
            } else {
                sum_tf as u32
            };
            doc_len.push(dl);
        }

        Ok(FrameChunkV1 {
            rows: rows_u32,
            doc_id,
            source_id,
            when_ns,
            section_id_bitmap: section_bits.into_bytes(),
            section_id_values: section_vals,
            where_id_bitmap: where_bits.into_bytes(),
            where_id_values: where_vals,
            who_bitmap: who_bits.into_bytes(),
            who_values: who_vals,
            what_bitmap: what_bits.into_bytes(),
            what_values: what_vals,
            verb_bitmap: verb_bits.into_bytes(),
            verb_values: verb_vals,
            polarity,
            confidence,
            doc_len,
            entity_offs,
            entity_lens,
            entity_pool,
            term_offs,
            term_lens,
            term_id_pool,
            term_tf_pool,
        })
    }

    fn encode_into(&self, w: &mut ByteWriter) -> Result<(), EncodeError> {
        w.write_u32(self.rows);

        // Column blobs (length-prefixed).
        w.write_bytes(&encode_u64_raw(&self.doc_id)?)?;
        w.write_bytes(&encode_u64_raw(&self.source_id)?)?;
        w.write_bytes(&encode_i128_raw(&self.when_ns)?)?;

        w.write_bytes(&self.section_id_bitmap)?;
        w.write_bytes(&encode_u64_raw(&self.section_id_values)?)?;

        w.write_bytes(&self.where_id_bitmap)?;
        w.write_bytes(&encode_u64_raw(&self.where_id_values)?)?;

        w.write_bytes(&self.who_bitmap)?;
        w.write_bytes(&encode_u64_raw(&self.who_values)?)?;

        w.write_bytes(&self.what_bitmap)?;
        w.write_bytes(&encode_u64_raw(&self.what_values)?)?;

        w.write_bytes(&self.verb_bitmap)?;
        w.write_bytes(&encode_u64_raw(&self.verb_values)?)?;

        w.write_bytes(&self.polarity)?;
        w.write_bytes(&encode_u32_raw(&self.confidence)?)?;
        w.write_bytes(&encode_u32_raw(&self.doc_len)?)?;

        // entity index + pool
        w.write_bytes(&encode_u32_raw(&self.entity_offs)?)?;
        w.write_bytes(&encode_u32_raw(&self.entity_lens)?)?;
        w.write_bytes(&encode_u64_raw(&self.entity_pool)?)?;

        // term index + pools
        w.write_bytes(&encode_u32_raw(&self.term_offs)?)?;
        w.write_bytes(&encode_u32_raw(&self.term_lens)?)?;
        w.write_bytes(&encode_u64_raw(&self.term_id_pool)?)?;
        w.write_bytes(&encode_u32_raw(&self.term_tf_pool)?)?;

        Ok(())
    }

    fn decode_from(r: &mut ByteReader<'_>) -> Result<FrameChunkV1, DecodeError> {
        let rows = r.read_u32()? as usize;

        let doc_id = decode_u64_raw(r.read_bytes_view()?)?;
        let source_id = decode_u64_raw(r.read_bytes_view()?)?;
        let when_ns = decode_i128_raw(r.read_bytes_view()?)?;

        if doc_id.len() != rows || source_id.len() != rows || when_ns.len() != rows {
            return Err(DecodeError::new("column length mismatch"));
        }

        let section_id_bitmap = r.read_bytes_view()?.to_vec();
        let section_id_values = decode_u64_raw(r.read_bytes_view()?)?;

        let where_id_bitmap = r.read_bytes_view()?.to_vec();
        let where_id_values = decode_u64_raw(r.read_bytes_view()?)?;

        let who_bitmap = r.read_bytes_view()?.to_vec();
        let who_values = decode_u64_raw(r.read_bytes_view()?)?;

        let what_bitmap = r.read_bytes_view()?.to_vec();
        let what_values = decode_u64_raw(r.read_bytes_view()?)?;

        let verb_bitmap = r.read_bytes_view()?.to_vec();
        let verb_values = decode_u64_raw(r.read_bytes_view()?)?;

        let polarity = r.read_bytes_view()?.to_vec();
        let confidence = decode_u32_raw(r.read_bytes_view()?)?;
        let doc_len = decode_u32_raw(r.read_bytes_view()?)?;

        if polarity.len() != rows || confidence.len() != rows || doc_len.len() != rows {
            return Err(DecodeError::new("column length mismatch"));
        }

        let entity_offs = decode_u32_raw(r.read_bytes_view()?)?;
        let entity_lens = decode_u32_raw(r.read_bytes_view()?)?;
        let entity_pool = decode_u64_raw(r.read_bytes_view()?)?;

        if entity_offs.len() != rows || entity_lens.len() != rows {
            return Err(DecodeError::new("column length mismatch"));
        }

        let term_offs = decode_u32_raw(r.read_bytes_view()?)?;
        let term_lens = decode_u32_raw(r.read_bytes_view()?)?;
        let term_id_pool = decode_u64_raw(r.read_bytes_view()?)?;
        let term_tf_pool = decode_u32_raw(r.read_bytes_view()?)?;

        if term_offs.len() != rows || term_lens.len() != rows {
            return Err(DecodeError::new("column length mismatch"));
        }
        if term_id_pool.len() != term_tf_pool.len() {
            return Err(DecodeError::new("term pool length mismatch"));
        }

        // Validate option bitmaps are plausible.
        validate_bitmap_len(rows, &section_id_bitmap)?;
        validate_bitmap_len(rows, &where_id_bitmap)?;
        validate_bitmap_len(rows, &who_bitmap)?;
        validate_bitmap_len(rows, &what_bitmap)?;
        validate_bitmap_len(rows, &verb_bitmap)?;

        // Validate optional counts match bitmap popcount.
        if popcount(&section_id_bitmap) != section_id_values.len() {
            return Err(DecodeError::new("section_id bitmap/value mismatch"));
        }
        if popcount(&where_id_bitmap) != where_id_values.len() {
            return Err(DecodeError::new("where_id bitmap/value mismatch"));
        }
        if popcount(&who_bitmap) != who_values.len() {
            return Err(DecodeError::new("who bitmap/value mismatch"));
        }
        if popcount(&what_bitmap) != what_values.len() {
            return Err(DecodeError::new("what bitmap/value mismatch"));
        }
        if popcount(&verb_bitmap) != verb_values.len() {
            return Err(DecodeError::new("verb bitmap/value mismatch"));
        }

        // Validate variable list indexes do not exceed pools.
        validate_index_bounds(&entity_offs, &entity_lens, entity_pool.len())?;
        validate_index_bounds(&term_offs, &term_lens, term_id_pool.len())?;

        Ok(FrameChunkV1 {
            rows: rows as u32,
            doc_id,
            source_id,
            when_ns,
            section_id_bitmap,
            section_id_values,
            where_id_bitmap,
            where_id_values,
            who_bitmap,
            who_values,
            what_bitmap,
            what_values,
            verb_bitmap,
            verb_values,
            polarity,
            confidence,
            doc_len,
            entity_offs,
            entity_lens,
            entity_pool,
            term_offs,
            term_lens,
            term_id_pool,
            term_tf_pool,
        })
    }

    // Fetch a single row by index without expanding the whole chunk.
    //
    // Returns None if the index is out of range or if internal packed columns
    // are inconsistent (malformed data).
    fn row_at(&self, idx: usize) -> Option<FrameRowV1> {
        let rows = self.rows as usize;
        if idx >= rows {
            return None;
        }
        if idx >= self.doc_id.len()
            || idx >= self.source_id.len()
            || idx >= self.when_ns.len()
            || idx >= self.polarity.len()
            || idx >= self.confidence.len()
            || idx >= self.doc_len.len()
            || idx >= self.entity_offs.len()
            || idx >= self.entity_lens.len()
            || idx >= self.term_offs.len()
            || idx >= self.term_lens.len()
        {
            return None;
        }

        let doc_id = DocId(Id64(self.doc_id[idx]));
        let source_id = SourceId(Id64(self.source_id[idx]));
        let mut row = FrameRowV1::new(doc_id, source_id);

        row.when_ns = self.when_ns[idx];

        row.section_id = if bitmap_has(&self.section_id_bitmap, idx) {
            let vi = bitmap_rank(&self.section_id_bitmap, idx);
            if vi >= self.section_id_values.len() {
                return None;
            }
            Some(SectionId(Id64(self.section_id_values[vi])))
        } else {
            None
        };

        row.where_id = if bitmap_has(&self.where_id_bitmap, idx) {
            let vi = bitmap_rank(&self.where_id_bitmap, idx);
            if vi >= self.where_id_values.len() {
                return None;
            }
            Some(WhereId(Id64(self.where_id_values[vi])))
        } else {
            None
        };

        row.who = if bitmap_has(&self.who_bitmap, idx) {
            let vi = bitmap_rank(&self.who_bitmap, idx);
            if vi >= self.who_values.len() {
                return None;
            }
            Some(EntityId(Id64(self.who_values[vi])))
        } else {
            None
        };

        row.what = if bitmap_has(&self.what_bitmap, idx) {
            let vi = bitmap_rank(&self.what_bitmap, idx);
            if vi >= self.what_values.len() {
                return None;
            }
            Some(EntityId(Id64(self.what_values[vi])))
        } else {
            None
        };

        row.verb = if bitmap_has(&self.verb_bitmap, idx) {
            let vi = bitmap_rank(&self.verb_bitmap, idx);
            if vi >= self.verb_values.len() {
                return None;
            }
            Some(VerbId(Id64(self.verb_values[vi])))
        } else {
            None
        };

        let p = self.polarity[idx] as i8;
        row.polarity = Polarity::from_i8(p).unwrap_or(Polarity::Neutral);
        row.confidence = ConfidenceQ16(self.confidence[idx]);
        row.doc_len = self.doc_len[idx];

        // entities
        let eo = self.entity_offs[idx] as usize;
        let el = self.entity_lens[idx] as usize;
        if eo
            .checked_add(el)
            .map(|end| end <= self.entity_pool.len())
            .unwrap_or(false)
        {
            row.entity_ids = self.entity_pool[eo..eo + el]
                .iter()
                .map(|v| EntityId(Id64(*v)))
                .collect();
        } else {
            return None;
        }

        // terms
        let to = self.term_offs[idx] as usize;
        let tl = self.term_lens[idx] as usize;
        let end = match to.checked_add(tl) {
            Some(v) => v,
            None => return None,
        };
        if end > self.term_id_pool.len() || end > self.term_tf_pool.len() {
            return None;
        }

        let mut terms: Vec<TermFreq> = Vec::with_capacity(tl);
        for j in 0..tl {
            let id = self.term_id_pool[to + j];
            let tf = self.term_tf_pool[to + j];
            terms.push(TermFreq {
                term: TermId(Id64(id)),
                tf,
            });
        }
        row.terms = terms;

        Some(row)
    }

    fn push_rows(&self, out: &mut Vec<FrameRowV1>) {
        let rows = self.rows as usize;

        let mut section_i = 0usize;
        let mut where_i = 0usize;
        let mut who_i = 0usize;
        let mut what_i = 0usize;
        let mut verb_i = 0usize;

        for idx in 0..rows {
            let doc_id = DocId(Id64(self.doc_id[idx]));
            let source_id = SourceId(Id64(self.source_id[idx]));
            let mut row = FrameRowV1::new(doc_id, source_id);

            row.when_ns = self.when_ns[idx];

            row.section_id = if bitmap_has(&self.section_id_bitmap, idx) {
                let v = self.section_id_values[section_i];
                section_i += 1;
                Some(SectionId(Id64(v)))
            } else {
                None
            };

            row.where_id = if bitmap_has(&self.where_id_bitmap, idx) {
                let v = self.where_id_values[where_i];
                where_i += 1;
                Some(WhereId(Id64(v)))
            } else {
                None
            };

            row.who = if bitmap_has(&self.who_bitmap, idx) {
                let v = self.who_values[who_i];
                who_i += 1;
                Some(EntityId(Id64(v)))
            } else {
                None
            };

            row.what = if bitmap_has(&self.what_bitmap, idx) {
                let v = self.what_values[what_i];
                what_i += 1;
                Some(EntityId(Id64(v)))
            } else {
                None
            };

            row.verb = if bitmap_has(&self.verb_bitmap, idx) {
                let v = self.verb_values[verb_i];
                verb_i += 1;
                Some(VerbId(Id64(v)))
            } else {
                None
            };

            let p = self.polarity[idx] as i8;
            row.polarity = Polarity::from_i8(p).unwrap_or(Polarity::Neutral);
            row.confidence = ConfidenceQ16(self.confidence[idx]);
            row.doc_len = self.doc_len[idx];

            // entities
            let eo = self.entity_offs[idx] as usize;
            let el = self.entity_lens[idx] as usize;
            row.entity_ids = self.entity_pool[eo..eo + el]
                .iter()
                .map(|v| EntityId(Id64(*v)))
                .collect();

            // terms
            let to = self.term_offs[idx] as usize;
            let tl = self.term_lens[idx] as usize;
            let mut terms: Vec<TermFreq> = Vec::with_capacity(tl);
            for j in 0..tl {
                let id = self.term_id_pool[to + j];
                let tf = self.term_tf_pool[to + j];
                terms.push(TermFreq {
                    term: TermId(Id64(id)),
                    tf,
                });
            }
            row.terms = terms;

            out.push(row);
        }
    }
}

/// Errors for FrameSegment construction helpers.
#[derive(Debug)]
pub enum FrameSegmentError {
    /// Too many rows for v1 encoding.
    TooManyRows,
    /// A pool (entities/terms) exceeded u32 element counts.
    PoolTooLarge,
    /// Encoding error.
    Encode(EncodeError),
}

impl core::fmt::Display for FrameSegmentError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            FrameSegmentError::TooManyRows => write!(f, "too many rows"),
            FrameSegmentError::PoolTooLarge => write!(f, "pool too large"),
            FrameSegmentError::Encode(e) => write!(f, "encode: {}", e),
        }
    }
}

impl std::error::Error for FrameSegmentError {}

impl From<EncodeError> for FrameSegmentError {
    fn from(e: EncodeError) -> Self {
        FrameSegmentError::Encode(e)
    }
}

// ---------- helpers ----------

fn validate_index_bounds(offs: &[u32], lens: &[u32], pool_len: usize) -> Result<(), DecodeError> {
    for i in 0..offs.len() {
        let o = offs[i] as usize;
        let l = lens[i] as usize;
        if o > pool_len || o + l > pool_len {
            return Err(DecodeError::new("index out of bounds"));
        }
    }
    Ok(())
}

fn validate_bitmap_len(rows: usize, bits: &[u8]) -> Result<(), DecodeError> {
    let need = (rows + 7) / 8;
    if bits.len() != need {
        return Err(DecodeError::new("bad bitmap length"));
    }
    Ok(())
}

fn popcount(bits: &[u8]) -> usize {
    let mut c = 0usize;
    for b in bits {
        c += b.count_ones() as usize;
    }
    c
}

fn bitmap_has(bits: &[u8], idx: usize) -> bool {
    let byte = idx / 8;
    let bit = idx % 8;
    ((bits[byte] >> bit) & 1) == 1
}

// Count the number of set bits in [0, idx).
//
// This is used to map a row index to its packed "values" index when a column
// is stored as (bitmap, values[]). It is deterministic and bounded by the
// bitmap length.
fn bitmap_rank(bits: &[u8], idx: usize) -> usize {
    if idx == 0 || bits.is_empty() {
        return 0;
    }

    let full_bytes = idx / 8;
    let rem_bits = idx % 8;

    let mut count = 0usize;
    let max_full = core::cmp::min(full_bytes, bits.len());
    for i in 0..max_full {
        count += bits[i].count_ones() as usize;
    }

    if rem_bits != 0 && full_bytes < bits.len() {
        let mask = (1u8 << rem_bits) - 1;
        count += (bits[full_bytes] & mask).count_ones() as usize;
    }

    count
}

struct BitsetBuilder {
    bits: Vec<u8>,
}

impl BitsetBuilder {
    fn new(rows: usize) -> BitsetBuilder {
        let n = (rows + 7) / 8;
        BitsetBuilder { bits: vec![0u8; n] }
    }

    fn set(&mut self, idx: usize) {
        let byte = idx / 8;
        let bit = idx % 8;
        self.bits[byte] |= 1u8 << bit;
    }

    fn push_opt_u64(&mut self, idx: usize, v: Option<u64>, out: &mut Vec<u64>) {
        if let Some(x) = v {
            self.set(idx);
            out.push(x);
        }
    }

    fn into_bytes(self) -> Vec<u8> {
        self.bits
    }
}

fn encode_u64_raw(v: &[u64]) -> Result<Vec<u8>, EncodeError> {
    let len = v.len();
    if len > (u32::MAX as usize) {
        return Err(EncodeError::new("vector too large"));
    }
    let mut out: Vec<u8> = Vec::with_capacity(len * 8);
    for x in v {
        out.extend_from_slice(&x.to_le_bytes());
    }
    Ok(out)
}

fn decode_u64_raw(b: &[u8]) -> Result<Vec<u64>, DecodeError> {
    if b.len() % 8 != 0 {
        return Err(DecodeError::new("bad u64 column length"));
    }
    let n = b.len() / 8;
    let mut out: Vec<u64> = Vec::with_capacity(n);
    let mut i = 0usize;
    while i < b.len() {
        let mut tmp = [0u8; 8];
        tmp.copy_from_slice(&b[i..i + 8]);
        out.push(u64::from_le_bytes(tmp));
        i += 8;
    }
    Ok(out)
}

fn encode_u32_raw(v: &[u32]) -> Result<Vec<u8>, EncodeError> {
    let len = v.len();
    if len > (u32::MAX as usize) {
        return Err(EncodeError::new("vector too large"));
    }
    let mut out: Vec<u8> = Vec::with_capacity(len * 4);
    for x in v {
        out.extend_from_slice(&x.to_le_bytes());
    }
    Ok(out)
}

fn decode_u32_raw(b: &[u8]) -> Result<Vec<u32>, DecodeError> {
    if b.len() % 4 != 0 {
        return Err(DecodeError::new("bad u32 column length"));
    }
    let n = b.len() / 4;
    let mut out: Vec<u32> = Vec::with_capacity(n);
    let mut i = 0usize;
    while i < b.len() {
        let mut tmp = [0u8; 4];
        tmp.copy_from_slice(&b[i..i + 4]);
        out.push(u32::from_le_bytes(tmp));
        i += 4;
    }
    Ok(out)
}

fn encode_i128_raw(v: &[i128]) -> Result<Vec<u8>, EncodeError> {
    let len = v.len();
    if len > (u32::MAX as usize) {
        return Err(EncodeError::new("vector too large"));
    }
    let mut out: Vec<u8> = Vec::with_capacity(len * 16);
    for x in v {
        out.extend_from_slice(&x.to_le_bytes());
    }
    Ok(out)
}

fn decode_i128_raw(b: &[u8]) -> Result<Vec<i128>, DecodeError> {
    if b.len() % 16 != 0 {
        return Err(DecodeError::new("bad i128 column length"));
    }
    let n = b.len() / 16;
    let mut out: Vec<i128> = Vec::with_capacity(n);
    let mut i = 0usize;
    while i < b.len() {
        let mut tmp = [0u8; 16];
        tmp.copy_from_slice(&b[i..i + 16]);
        out.push(i128::from_le_bytes(tmp));
        i += 16;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{derive_id64, TermId};

    fn mk_row(doc: u64, src: u64) -> FrameRowV1 {
        let doc_id = DocId(Id64(doc));
        let source_id = SourceId(Id64(src));
        let mut r = FrameRowV1::new(doc_id, source_id);
        r.when_ns = 123;
        r.polarity = Polarity::Positive;
        r.confidence = ConfidenceQ16::from_ratio(1, 2);
        r
    }

    #[test]
    fn frame_segment_round_trip_rows() {
        let mut rows: Vec<FrameRowV1> = Vec::new();

        let mut a = mk_row(1, 9);
        a.section_id = Some(SectionId(Id64(77)));
        a.where_id = Some(WhereId(Id64(88)));
        a.who = Some(EntityId(Id64(5)));
        a.what = Some(EntityId(Id64(6)));
        a.verb = Some(VerbId(Id64(7)));
        // Intentionally unsorted entity ids; segment builder should sort.
        a.entity_ids.push(EntityId(Id64(33)));
        a.entity_ids.push(EntityId(Id64(22)));
        // Intentionally unsorted terms; segment builder should sort.
        a.terms.push(TermFreq {
            term: TermId(derive_id64(b"t", b"b")),
            tf: 2,
        });
        a.terms.push(TermFreq {
            term: TermId(derive_id64(b"t", b"a")),
            tf: 1,
        });
        rows.push(a);

        let mut b = mk_row(2, 9);
        b.when_ns = -999;
        rows.push(b);

        let seg = FrameSegmentV1::from_rows(&rows, 1).unwrap();
        let bytes = seg.encode().unwrap();
        let seg2 = FrameSegmentV1::decode(&bytes).unwrap();

        assert_eq!(seg.chunk_rows, seg2.chunk_rows);
        assert_eq!(seg.chunks.len(), seg2.chunks.len());

        let r2 = seg2.to_rows();
        assert_eq!(r2.len(), 2);

        // Row 0 should have sorted entity ids.
        assert_eq!(r2[0].entity_ids[0].0 .0, 22);
        assert_eq!(r2[0].entity_ids[1].0 .0, 33);

        // Row 0 terms should be sorted by term id.
        assert!(r2[0].terms[0].term.0 .0 <= r2[0].terms[1].term.0 .0);

        // Optional fields preserved.
        assert_eq!(r2[0].section_id.unwrap().0 .0, 77);
        assert_eq!(r2[0].where_id.unwrap().0 .0, 88);

        // Row 1 optional fields are None.
        assert!(r2[1].section_id.is_none());
        assert!(r2[1].where_id.is_none());
    }

    #[test]
    fn frame_segment_get_row_matches_to_rows() {
        let mut rows: Vec<FrameRowV1> = Vec::new();

        let mut a = mk_row(10, 1);
        a.section_id = Some(SectionId(Id64(123)));
        a.entity_ids.push(EntityId(Id64(9)));
        a.terms.push(TermFreq {
            term: TermId(derive_id64(b"t", b"alpha")),
            tf: 3,
        });
        rows.push(a);

        let mut b = mk_row(11, 1);
        b.when_ns = 999;
        rows.push(b);

        let mut c = mk_row(12, 2);
        c.where_id = Some(WhereId(Id64(555)));
        c.who = Some(EntityId(Id64(77)));
        c.terms.push(TermFreq {
            term: TermId(derive_id64(b"t", b"beta")),
            tf: 1,
        });
        c.terms.push(TermFreq {
            term: TermId(derive_id64(b"t", b"gamma")),
            tf: 2,
        });
        rows.push(c);

        // Force multiple chunks.
        let seg = FrameSegmentV1::from_rows(&rows, 2).unwrap();
        let bytes = seg.encode().unwrap();
        let seg2 = FrameSegmentV1::decode(&bytes).unwrap();

        let all = seg2.to_rows();
        assert_eq!(all.len(), 3);

        for i in 0..all.len() {
            let r = seg2.get_row(i as u32).unwrap();
            assert_eq!(r, all[i]);
        }

        assert!(seg2.get_row(3).is_none());
    }

    #[test]
    fn frame_segment_is_deterministic() {
        let mut r = mk_row(1, 2);
        r.entity_ids.push(EntityId(Id64(2)));
        r.entity_ids.push(EntityId(Id64(1)));

        let seg1 = FrameSegmentV1::from_rows(&[r.clone()], 1024).unwrap();
        let seg2 = FrameSegmentV1::from_rows(&[r], 1024).unwrap();

        let b1 = seg1.encode().unwrap();
        let b2 = seg2.encode().unwrap();
        assert_eq!(b1, b2);
    }

    #[test]
    fn frame_chunk_validates_bounds_on_decode() {
        // Build a valid segment, then corrupt entity_lens to exceed pool.
        let mut r = mk_row(1, 2);
        r.entity_ids.push(EntityId(Id64(1)));
        let seg = FrameSegmentV1::from_rows(&[r], 1024).unwrap();
        let mut bytes = seg.encode().unwrap();

        // Locate first chunk and then entity_lens column blob.
        // Format:
        // header: 8 + 2 + 2 + 4 + 4 = 20 bytes
        // chunk: rows(u32) + 14 blobs (bytes+len)
        // We will parse with ByteReader and then rewrite the entity_lens blob payload.
        let mut rr = ByteReader::new(&bytes);
        rr.read_fixed(8).unwrap();
        rr.read_u16().unwrap();
        rr.read_u16().unwrap();
        rr.read_u32().unwrap();
        rr.read_u32().unwrap();

        // rows
        rr.read_u32().unwrap();

        // Skip blobs until entity_lens (we need offsets):
        // doc_id, source_id, when_ns,
        // section_bits, section_vals,
        // where_bits, where_vals,
        // who_bits, who_vals,
        // what_bits, what_vals,
        // verb_bits, verb_vals,
        // polarity, confidence, doc_len,
        // entity_offs, entity_lens, entity_pool,
        // term_offs, term_lens, term_id_pool, term_tf_pool
        // entity_lens is after entity_offs.

        fn skip_blob(rr: &mut ByteReader<'_>) {
            let ln = rr.read_u32().unwrap() as usize;
            rr.read_fixed(ln).unwrap();
        }

        // Skip until polarity/confidence/doc_len.
        // doc_id, source_id, when_ns
        for _ in 0..3 {
            skip_blob(&mut rr);
        }
        // section bits/vals
        for _ in 0..2 {
            skip_blob(&mut rr);
        }
        // where bits/vals
        for _ in 0..2 {
            skip_blob(&mut rr);
        }
        // who bits/vals
        for _ in 0..2 {
            skip_blob(&mut rr);
        }
        // what bits/vals
        for _ in 0..2 {
            skip_blob(&mut rr);
        }
        // verb bits/vals
        for _ in 0..2 {
            skip_blob(&mut rr);
        }
        // polarity/confidence/doc_len
        for _ in 0..3 {
            skip_blob(&mut rr);
        }

        // entity_offs
        skip_blob(&mut rr);

        // entity_lens: record payload position, then overwrite first u32.
        let ln = rr.read_u32().unwrap() as usize;
        let payload_pos = rr.position();
        rr.read_fixed(ln).unwrap();

        // entity_lens is u32 raw; set it to a huge value so index validation fails.
        let big: u32 = 10_000;
        let b = big.to_le_bytes();
        bytes[payload_pos..payload_pos + 4].copy_from_slice(&b);

        // decode should fail due to index out of bounds
        assert!(FrameSegmentV1::decode(&bytes).is_err());
    }
}
