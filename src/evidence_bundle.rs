// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! EvidenceBundleV1 schema and canonical codec.
//!
//! EvidenceBundleV1 is the canonical output of retrieval.
//! It is designed to be:
//! - deterministic (canonical ordering, stable bytes)
//! - compact (integer-only; optional sketches)
//! - easy to replay (content-addressed artifact)
//!
//! Canonical ordering rules are enforced in decode and in
//! encode_assuming_canonical. The convenience encode method
//! canonicalizes a bundle before encoding.

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::hash::Hash32;
use core::cmp::Ordering;
use rustc_hash::FxHashSet;

const EVIDENCE_BUNDLE_V1_VERSION: u16 = 1;

/// Hard limits carried alongside a bundle.
///
/// These are intended to make evidence production bounded and replayable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvidenceLimitsV1 {
    /// Number of segments touched during retrieval.
    pub segments_touched: u32,
    /// Maximum number of evidence items permitted.
    pub max_items: u32,
    /// Maximum number of bytes permitted for the encoded bundle.
    pub max_bytes: u32,
}

/// A canonical evidence bundle produced by retrieval.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceBundleV1 {
    /// Stable id for the query (typically derived from the PromptPack or query plan).
    pub query_id: Hash32,
    /// Stable id for the index snapshot used during retrieval.
    pub snapshot_id: Hash32,
    /// Retrieval caps applied while constructing this bundle.
    pub limits: EvidenceLimitsV1,
    /// Id of the scoring model or retrieval scoring configuration.
    pub score_model_id: u32,
    /// Evidence items in canonical order.
    pub items: Vec<EvidenceItemV1>,
}

/// A single evidence item (scored).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceItemV1 {
    /// Integer score (higher is better).
    pub score: i64,
    /// Item payload.
    pub data: EvidenceItemDataV1,
}

/// Evidence item payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvidenceItemDataV1 {
    /// Reference to a frame row.
    Frame(FrameRowRefV1),
    /// Reference to a lexicon row (reserved for later stages).
    Lexicon(LexiconRowRefV1),
    /// Reference to a proof or verifier output (reserved for later stages).
    Proof(ProofRefV1),
}

/// Reference to a specific FrameRow within a FrameSegment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameRowRefV1 {
    /// FrameSegment artifact hash.
    pub segment_id: Hash32,
    /// Row index within the segment.
    pub row_ix: u32,
    /// Optional compact sketch of the row for gating / synthesis.
    pub sketch: Option<FrameRowSketchV1>,
}

/// Compact sketch of a frame row.
///
/// Canonical rules:
/// - entity_ids sorted ascending and unique
/// - meta_codes sorted ascending and unique
/// - terms sorted by (tf desc, term_id asc)
/// - term_id values are unique
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameRowSketchV1 {
    /// Term sketch entries.
    pub terms: Vec<TermTfV1>,
    /// Entity ids present in the row (ascending, unique).
    pub entity_ids: Vec<u32>,
    /// Meta codes present in the row (ascending, unique).
    pub meta_codes: Vec<u32>,
}

/// Term id + term frequency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TermTfV1 {
    /// Tokenizer term id.
    pub term_id: u32,
    /// Term frequency for this row.
    pub tf: u32,
}

/// Reference to a lexicon row (reserved).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LexiconRowRefV1 {
    /// Lexicon segment hash (future).
    pub segment_id: Hash32,
    /// Row index.
    pub row_ix: u32,
}

/// Reference to a proof/verifier output (reserved).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProofRefV1 {
    /// Proof artifact hash.
    pub proof_id: Hash32,
}

impl EvidenceBundleV1 {
    /// Create a new empty evidence bundle.
    pub fn new(query_id: Hash32, snapshot_id: Hash32, limits: EvidenceLimitsV1, score_model_id: u32) -> Self {
        Self {
            query_id,
            snapshot_id,
            limits,
            score_model_id,
            items: Vec::new(),
        }
    }

    /// Encode as canonical bytes.
    ///
    /// This method canonicalizes a cloned copy of the bundle first.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        let mut tmp = self.clone();
        tmp.canonicalize_in_place()?;
        tmp.encode_assuming_canonical()
    }

    /// Encode as canonical bytes, assuming the bundle is already canonical.
    ///
    /// Returns an error if the bundle violates canonical ordering rules.
    pub fn encode_assuming_canonical(&self) -> Result<Vec<u8>, EncodeError> {
        self.validate_canonical().map_err(|_| EncodeError::new("bundle not canonical"))?;

        // Rough capacity estimate: fixed header + per-item payload.
        let mut cap = 2 + 32 + 32 + 12 + 4 + 4;
        for it in self.items.iter() {
            cap += 8 + 1;
            cap += match &it.data {
                EvidenceItemDataV1::Frame(r) => {
                    let mut n = 32 + 4 + 1;
                    if let Some(sk) = &r.sketch {
                        n += 4 + (sk.entity_ids.len() * 4);
                        n += 4 + (sk.meta_codes.len() * 4);
                        n += 4 + (sk.terms.len() * 8);
                    }
                    n
                }
                EvidenceItemDataV1::Lexicon(_) => 32 + 4,
                EvidenceItemDataV1::Proof(_) => 32,
            };
        }

        let mut w = ByteWriter::with_capacity(cap);
        w.write_u16(EVIDENCE_BUNDLE_V1_VERSION);
        w.write_raw(&self.query_id);
        w.write_raw(&self.snapshot_id);
        w.write_u32(self.limits.segments_touched);
        w.write_u32(self.limits.max_items);
        w.write_u32(self.limits.max_bytes);
        w.write_u32(self.score_model_id);
        if self.items.len() > (u32::MAX as usize) {
            return Err(EncodeError::new("too many items"));
        }
        w.write_u32(self.items.len() as u32);

        for it in self.items.iter() {
            w.write_i64(it.score);
            w.write_u8(it.data.kind());
            match &it.data {
                EvidenceItemDataV1::Frame(r) => {
                    w.write_raw(&r.segment_id);
                    w.write_u32(r.row_ix);
                    match &r.sketch {
                        None => {
                            w.write_u8(0);
                        }
                        Some(sk) => {
                            w.write_u8(1);
                            write_u32_vec(&mut w, &sk.entity_ids)?;
                            write_u32_vec(&mut w, &sk.meta_codes)?;

                            if sk.terms.len() > (u32::MAX as usize) {
                                return Err(EncodeError::new("too many term sketch entries"));
                            }
                            w.write_u32(sk.terms.len() as u32);
                            for t in sk.terms.iter() {
                                w.write_u32(t.term_id);
                                w.write_u32(t.tf);
                            }
                        }
                    }
                }
                EvidenceItemDataV1::Lexicon(r) => {
                    w.write_raw(&r.segment_id);
                    w.write_u32(r.row_ix);
                }
                EvidenceItemDataV1::Proof(p) => {
                    w.write_raw(&p.proof_id);
                }
            }
        }

        let out = w.into_bytes();
        if self.limits.max_bytes != 0 && out.len() > (self.limits.max_bytes as usize) {
            return Err(EncodeError::new("encoded bundle exceeds max_bytes"));
        }
        Ok(out)
    }

    /// Decode a bundle and validate canonical ordering.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let ver = r.read_u16()?;
        if ver != EVIDENCE_BUNDLE_V1_VERSION {
            return Err(DecodeError::new("unsupported EvidenceBundle version"));
        }

        let query_id = read_hash32(&mut r)?;
        let snapshot_id = read_hash32(&mut r)?;
        let limits = EvidenceLimitsV1 {
            segments_touched: r.read_u32()?,
            max_items: r.read_u32()?,
            max_bytes: r.read_u32()?,
        };
        let score_model_id = r.read_u32()?;
        let items_len = r.read_u32()? as usize;

        let mut items = Vec::with_capacity(items_len);
        for _ in 0..items_len {
            let score = r.read_i64()?;
            let kind = r.read_u8()?;
            let data = match kind {
                0 => {
                    let segment_id = read_hash32(&mut r)?;
                    let row_ix = r.read_u32()?;
                    let has_sketch = r.read_u8()?;
                    let sketch = if has_sketch == 0 {
                        None
                    } else if has_sketch == 1 {
                        let entity_ids = read_u32_vec(&mut r)?;
                        let meta_codes = read_u32_vec(&mut r)?;
                        let terms_len = r.read_u32()? as usize;
                        let mut terms = Vec::with_capacity(terms_len);
                        for _ in 0..terms_len {
                            let term_id = r.read_u32()?;
                            let tf = r.read_u32()?;
                            terms.push(TermTfV1 { term_id, tf });
                        }
                        Some(FrameRowSketchV1 {
                            terms,
                            entity_ids,
                            meta_codes,
                        })
                    } else {
                        return Err(DecodeError::new("invalid sketch flag"));
                    };
                    EvidenceItemDataV1::Frame(FrameRowRefV1 {
                        segment_id,
                        row_ix,
                        sketch,
                    })
                }
                1 => {
                    let segment_id = read_hash32(&mut r)?;
                    let row_ix = r.read_u32()?;
                    EvidenceItemDataV1::Lexicon(LexiconRowRefV1 { segment_id, row_ix })
                }
                2 => {
                    let proof_id = read_hash32(&mut r)?;
                    EvidenceItemDataV1::Proof(ProofRefV1 { proof_id })
                }
                _ => return Err(DecodeError::new("unknown evidence item kind")),
            };
            items.push(EvidenceItemV1 { score, data });
        }

        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        let out = Self {
            query_id,
            snapshot_id,
            limits,
            score_model_id,
            items,
        };

        out.validate_canonical()?;
        if out.limits.max_bytes != 0 && bytes.len() > (out.limits.max_bytes as usize) {
            return Err(DecodeError::new("bundle exceeds max_bytes"));
        }
        Ok(out)
    }

    /// Canonicalize this bundle in place.
    ///
    /// This sorts items, normalizes sketches where safe (entity/meta sorted unique),
    /// and then validates the final canonical form.
    pub fn canonicalize_in_place(&mut self) -> Result<(), EncodeError> {
        // Normalize sketches first so ordering checks are deterministic.
        for it in self.items.iter_mut() {
            if let EvidenceItemDataV1::Frame(r) = &mut it.data {
                if let Some(sk) = &mut r.sketch {
                    sk.entity_ids.sort_unstable();
                    sk.entity_ids.dedup();
                    sk.meta_codes.sort_unstable();
                    sk.meta_codes.dedup();

                    // Terms must already be canonical; we do not reorder them here.
                    validate_term_sketch(&sk.terms)
                        .map_err(|_| EncodeError::new("term sketch not canonical"))?;
                }
            }
        }

        self.items.sort_unstable_by(item_order);

        // Validate final form.
        self.validate_canonical().map_err(|_| EncodeError::new("bundle not canonical"))?;
        Ok(())
    }

    fn validate_canonical(&self) -> Result<(), DecodeError> {
        if self.items.len() > (u32::MAX as usize) {
            return Err(DecodeError::new("too many items"));
        }
        if self.limits.max_items != 0 && self.items.len() > (self.limits.max_items as usize) {
            return Err(DecodeError::new("items exceed max_items"));
        }

        // Validate sketches.
        for it in self.items.iter() {
            if let EvidenceItemDataV1::Frame(r) = &it.data {
                if let Some(sk) = &r.sketch {
                    validate_sorted_unique_u32(&sk.entity_ids, "entity_ids")?;
                    validate_sorted_unique_u32(&sk.meta_codes, "meta_codes")?;
                    validate_term_sketch(&sk.terms)?;
                }
            }
        }

        // Validate canonical item order.
        for i in 1..self.items.len() {
            let prev = &self.items[i - 1];
            let cur = &self.items[i];
            if item_order(prev, cur) == Ordering::Greater {
                return Err(DecodeError::new("items not in canonical order"));
            }
        }

        Ok(())
    }
}

impl EvidenceItemDataV1 {
    fn kind(&self) -> u8 {
        match self {
            EvidenceItemDataV1::Frame(_) => 0,
            EvidenceItemDataV1::Lexicon(_) => 1,
            EvidenceItemDataV1::Proof(_) => 2,
        }
    }
}

fn item_order(a: &EvidenceItemV1, b: &EvidenceItemV1) -> Ordering {
    match b.score.cmp(&a.score) {
        Ordering::Equal => {
            let ka = a.data.kind();
            let kb = b.data.kind();
            match ka.cmp(&kb) {
                Ordering::Equal => stable_id_cmp(&a.data, &b.data),
                other => other,
            }
        }
        other => other,
    }
}

fn stable_id_cmp(a: &EvidenceItemDataV1, b: &EvidenceItemDataV1) -> Ordering {
    match (a, b) {
        (EvidenceItemDataV1::Frame(ra), EvidenceItemDataV1::Frame(rb)) => {
            match ra.segment_id.cmp(&rb.segment_id) {
                Ordering::Equal => ra.row_ix.cmp(&rb.row_ix),
                other => other,
            }
        }
        (EvidenceItemDataV1::Lexicon(ra), EvidenceItemDataV1::Lexicon(rb)) => {
            match ra.segment_id.cmp(&rb.segment_id) {
                Ordering::Equal => ra.row_ix.cmp(&rb.row_ix),
                other => other,
            }
        }
        (EvidenceItemDataV1::Proof(pa), EvidenceItemDataV1::Proof(pb)) => pa.proof_id.cmp(&pb.proof_id),
        // Kinds differ: should be handled by kind compare.
        _ => a.kind().cmp(&b.kind()),
    }
}

fn read_hash32(r: &mut ByteReader<'_>) -> Result<Hash32, DecodeError> {
    let b = r.read_fixed(32)?;
    let mut out = [0u8; 32];
    out.copy_from_slice(b);
    Ok(out)
}

fn write_u32_vec(w: &mut ByteWriter, v: &[u32]) -> Result<(), EncodeError> {
    if v.len() > (u32::MAX as usize) {
        return Err(EncodeError::new("vector too large"));
    }
    w.write_u32(v.len() as u32);
    for &x in v.iter() {
        w.write_u32(x);
    }
    Ok(())
}

fn read_u32_vec(r: &mut ByteReader<'_>) -> Result<Vec<u32>, DecodeError> {
    let n = r.read_u32()? as usize;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(r.read_u32()?);
    }
    Ok(out)
}

fn validate_sorted_unique_u32(v: &[u32], _name: &'static str) -> Result<(), DecodeError> {
    if v.is_empty() {
        return Ok(());
    }
    for i in 1..v.len() {
        if v[i - 1] >= v[i] {
            return Err(DecodeError::new("u32 list must be sorted unique"));
        }
    }
    Ok(())
}

fn validate_term_sketch(terms: &[TermTfV1]) -> Result<(), DecodeError> {
    if terms.is_empty() {
        return Ok(());
    }

    let mut seen: FxHashSet<u32> = FxHashSet::with_capacity_and_hasher(terms.len(), Default::default());
    for t in terms.iter() {
        if t.tf == 0 {
            return Err(DecodeError::new("tf must be nonzero"));
        }
        if !seen.insert(t.term_id) {
            return Err(DecodeError::new("duplicate term_id"));
        }
    }

    for i in 1..terms.len() {
        let a = terms[i - 1];
        let b = terms[i];
        let ok = if a.tf > b.tf {
            true
        } else if a.tf == b.tf {
            a.term_id < b.term_id
        } else {
            false
        };
        if !ok {
            return Err(DecodeError::new("term sketch not in canonical order"));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(b: u8) -> Hash32 {
        [b; 32]
    }

    #[test]
    fn evidence_bundle_round_trip() {
        let limits = EvidenceLimitsV1 {
            segments_touched: 2,
            max_items: 16,
            max_bytes: 0,
        };

        let mut b = EvidenceBundleV1::new(h(1), h(2), limits, 7);

        b.items.push(EvidenceItemV1 {
            score: 10,
            data: EvidenceItemDataV1::Frame(FrameRowRefV1 {
                segment_id: h(9),
                row_ix: 1,
                sketch: None,
            }),
        });
        b.items.push(EvidenceItemV1 {
            score: 5,
            data: EvidenceItemDataV1::Frame(FrameRowRefV1 {
                segment_id: h(8),
                row_ix: 2,
                sketch: None,
            }),
        });

        let bytes = b.encode().unwrap();
        let got = EvidenceBundleV1::decode(&bytes).unwrap();
        assert_eq!(got, b);
    }

    #[test]
    fn encode_canonicalizes_item_order() {
        let limits = EvidenceLimitsV1 {
            segments_touched: 1,
            max_items: 16,
            max_bytes: 0,
        };

        let mut b = EvidenceBundleV1::new(h(1), h(2), limits, 7);

        // Intentionally out of order.
        b.items.push(EvidenceItemV1 {
            score: 1,
            data: EvidenceItemDataV1::Frame(FrameRowRefV1 {
                segment_id: h(9),
                row_ix: 2,
                sketch: None,
            }),
        });
        b.items.push(EvidenceItemV1 {
            score: 2,
            data: EvidenceItemDataV1::Frame(FrameRowRefV1 {
                segment_id: h(9),
                row_ix: 1,
                sketch: None,
            }),
        });

        let bytes = b.encode().unwrap();
        let got = EvidenceBundleV1::decode(&bytes).unwrap();
        assert_eq!(got.items.len(), 2);
        assert_eq!(got.items[0].score, 2);
        assert_eq!(got.items[1].score, 1);
    }

    #[test]
    fn encode_assuming_canonical_rejects_noncanonical() {
        let limits = EvidenceLimitsV1 {
            segments_touched: 1,
            max_items: 16,
            max_bytes: 0,
        };

        let mut b = EvidenceBundleV1::new(h(1), h(2), limits, 7);

        // Non-canonical: score order is increasing.
        b.items.push(EvidenceItemV1 {
            score: 1,
            data: EvidenceItemDataV1::Frame(FrameRowRefV1 {
                segment_id: h(9),
                row_ix: 1,
                sketch: None,
            }),
        });
        b.items.push(EvidenceItemV1 {
            score: 2,
            data: EvidenceItemDataV1::Frame(FrameRowRefV1 {
                segment_id: h(9),
                row_ix: 2,
                sketch: None,
            }),
        });

        assert!(b.encode_assuming_canonical().is_err());
    }

    #[test]
    fn sketch_requires_sorted_unique_and_term_order() {
        let limits = EvidenceLimitsV1 {
            segments_touched: 1,
            max_items: 16,
            max_bytes: 0,
        };

        let mut b = EvidenceBundleV1::new(h(1), h(2), limits, 7);

        let sk = FrameRowSketchV1 {
            terms: vec![
                TermTfV1 { term_id: 3, tf: 2 },
                TermTfV1 { term_id: 2, tf: 2 },
            ],
            entity_ids: vec![5, 5],
            meta_codes: vec![9, 1],
        };

        b.items.push(EvidenceItemV1 {
            score: 1,
            data: EvidenceItemDataV1::Frame(FrameRowRefV1 {
                segment_id: h(9),
                row_ix: 1,
                sketch: Some(sk),
            }),
        });

        // encode will attempt to canonicalize entity/meta ordering, but term order
        // violations should fail.
        assert!(b.encode().is_err());
    }
}
