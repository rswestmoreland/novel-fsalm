// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! EvidenceSetV1 schema and canonical codec.
//!
//! EvidenceSetV1 is a deterministic mapping from claims (or output spans) to
//! evidence row references. It is intended to make answers auditable and to
//! support future evaluation / training stages.
//!
//! This initial schema is intentionally small:
//! - references the EvidenceBundleV1 that was used for the answer
//! - stores a list of claim items
//! - each claim item stores a claim id, claim text, and a list of evidence row refs
//!
//! Canonical ordering rules:
//! - items are sorted by claim_id ascending, and claim_id values are unique
//! - within each item, evidence_refs are sorted by (segment_id, row_ix) ascending
//! and the (segment_id, row_ix) pairs are unique

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::hash::Hash32;
use core::cmp::Ordering;

const EVIDENCE_SET_V1_VERSION: u16 = 1;

/// A canonical evidence set produced alongside an answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceSetV1 {
    /// Schema version (must equal 1).
    pub version: u16,
    /// Evidence bundle hash used to construct this evidence set.
    pub evidence_bundle_id: Hash32,
    /// Claim items in canonical order.
    pub items: Vec<EvidenceSetItemV1>,
}

/// A single claim (or output span) and its supporting evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceSetItemV1 {
    /// Stable claim id (ascending, unique within the set).
    pub claim_id: u32,
    /// Claim text for this item (UTF-8).
    pub claim_text: String,
    /// Evidence row references supporting this claim.
    pub evidence_refs: Vec<EvidenceRowRefV1>,
}

/// Reference to a specific frame row used as evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvidenceRowRefV1 {
    /// FrameSegment artifact hash.
    pub segment_id: Hash32,
    /// Row index within the segment.
    pub row_ix: u32,
    /// Optional score carried forward from retrieval (higher is better).
    ///
    /// For v1, this is always present to avoid per-entry tag bytes.
    pub score: i64,
}

impl EvidenceSetV1 {
    /// Encode as canonical bytes.
    ///
    /// This method canonicalizes a cloned copy of the set first.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        let mut tmp = self.clone();
        tmp.canonicalize_in_place()?;
        tmp.encode_assuming_canonical()
    }

    /// Encode as canonical bytes, assuming the set is already canonical.
    pub fn encode_assuming_canonical(&self) -> Result<Vec<u8>, EncodeError> {
        self.validate_canonical()
            .map_err(|_| EncodeError::new("evidence set not canonical"))?;

        // Rough capacity: header + per-item strings and refs.
        let mut cap = 2 + 32 + 4;
        for it in self.items.iter() {
            cap += 4;
            cap += 4 + it.claim_text.len();
            cap += 4;
            cap += it.evidence_refs.len() * (32 + 4 + 8);
        }

        let mut w = ByteWriter::with_capacity(cap);
        w.write_u16(EVIDENCE_SET_V1_VERSION);
        w.write_raw(&self.evidence_bundle_id);

        if self.items.len() > (u32::MAX as usize) {
            return Err(EncodeError::new("too many evidence set items"));
        }
        w.write_u32(self.items.len() as u32);

        for it in self.items.iter() {
            w.write_u32(it.claim_id);
            w.write_str(&it.claim_text)?;

            if it.evidence_refs.len() > (u32::MAX as usize) {
                return Err(EncodeError::new("too many evidence refs"));
            }
            w.write_u32(it.evidence_refs.len() as u32);
            for r in it.evidence_refs.iter() {
                w.write_raw(&r.segment_id);
                w.write_u32(r.row_ix);
                w.write_i64(r.score);
            }
        }

        Ok(w.into_bytes())
    }

    /// Decode canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let ver = r.read_u16()?;
        if ver != EVIDENCE_SET_V1_VERSION {
            return Err(DecodeError::new("unsupported evidence set version"));
        }

        let bundle_id = {
            let b = r.read_fixed(32)?;
            let mut out = [0u8; 32];
            out.copy_from_slice(b);
            out
        };

        let n_items = r.read_u32()? as usize;
        let mut items = Vec::with_capacity(n_items);
        for _ in 0..n_items {
            let claim_id = r.read_u32()?;
            let claim_text = r.read_str_view()?.to_owned();

            let n_refs = r.read_u32()? as usize;
            let mut refs = Vec::with_capacity(n_refs);
            for _ in 0..n_refs {
                let seg = {
                    let b = r.read_fixed(32)?;
                    let mut out = [0u8; 32];
                    out.copy_from_slice(b);
                    out
                };
                let row_ix = r.read_u32()?;
                let score = r.read_i64()?;
                refs.push(EvidenceRowRefV1 {
                    segment_id: seg,
                    row_ix,
                    score,
                });
            }

            items.push(EvidenceSetItemV1 {
                claim_id,
                claim_text,
                evidence_refs: refs,
            });
        }

        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        let out = EvidenceSetV1 {
            version: ver,
            evidence_bundle_id: bundle_id,
            items,
        };
        out.validate_canonical()?;
        Ok(out)
    }

    /// Canonicalize in-place.
    pub fn canonicalize_in_place(&mut self) -> Result<(), EncodeError> {
        self.items.sort_by(|a, b| a.claim_id.cmp(&b.claim_id));
        for it in self.items.iter_mut() {
            it.evidence_refs.sort_by(cmp_row_ref);
        }
        // Validate now to catch duplicates / non-canonical structure.
        self.validate_canonical()
            .map_err(|_| EncodeError::new("evidence set not canonical"))?;
        Ok(())
    }

    /// Validate canonical invariants.
    pub fn validate_canonical(&self) -> Result<(), DecodeError> {
        if self.version != EVIDENCE_SET_V1_VERSION {
            return Err(DecodeError::new("unsupported evidence set version"));
        }

        // claim_id strict ascending and unique.
        let mut prev_claim: Option<u32> = None;
        for it in self.items.iter() {
            if let Some(p) = prev_claim {
                if it.claim_id <= p {
                    return Err(DecodeError::new(
                        "items must be sorted by claim_id and unique",
                    ));
                }
            }
            prev_claim = Some(it.claim_id);

            // evidence refs strict ascending by (segment_id,row_ix) and unique.
            let mut prev_ref: Option<EvidenceRowRefV1> = None;
            for r in it.evidence_refs.iter() {
                if let Some(p) = prev_ref {
                    let ord = cmp_row_ref(&p, r);
                    if ord != Ordering::Less {
                        return Err(DecodeError::new(
                            "evidence_refs must be sorted by (segment_id,row_ix) and unique",
                        ));
                    }
                }
                prev_ref = Some(*r);
            }
        }

        Ok(())
    }
}

fn cmp_row_ref(a: &EvidenceRowRefV1, b: &EvidenceRowRefV1) -> Ordering {
    match a.segment_id.cmp(&b.segment_id) {
        Ordering::Equal => a.row_ix.cmp(&b.row_ix),
        o => o,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h32(x: u8) -> Hash32 {
        let mut h = [0u8; 32];
        h[0] = x;
        h
    }

    #[test]
    fn evidence_set_encode_decode_round_trip() {
        let es = EvidenceSetV1 {
            version: EVIDENCE_SET_V1_VERSION,
            evidence_bundle_id: h32(9),
            items: vec![
                EvidenceSetItemV1 {
                    claim_id: 2,
                    claim_text: "b".to_string(),
                    evidence_refs: vec![
                        EvidenceRowRefV1 {
                            segment_id: h32(2),
                            row_ix: 10,
                            score: 7,
                        },
                        EvidenceRowRefV1 {
                            segment_id: h32(1),
                            row_ix: 3,
                            score: 9,
                        },
                    ],
                },
                EvidenceSetItemV1 {
                    claim_id: 1,
                    claim_text: "a".to_string(),
                    evidence_refs: vec![EvidenceRowRefV1 {
                        segment_id: h32(1),
                        row_ix: 1,
                        score: 5,
                    }],
                },
            ],
        };

        let bytes1 = es.encode().unwrap();
        let dec = EvidenceSetV1::decode(&bytes1).unwrap();

        // Encode should canonicalize ordering.
        assert_eq!(dec.items[0].claim_id, 1);
        assert_eq!(dec.items[1].claim_id, 2);
        assert_eq!(dec.items[1].evidence_refs[0].segment_id, h32(1));
        assert_eq!(dec.items[1].evidence_refs[1].segment_id, h32(2));

        // Deterministic re-encode.
        let bytes2 = dec.encode().unwrap();
        assert_eq!(bytes1, bytes2);

        // Decoded structure should be stable.
        let dec2 = EvidenceSetV1::decode(&bytes2).unwrap();
        assert_eq!(dec, dec2);
    }

    #[test]
    fn evidence_set_decode_rejects_noncanonical_duplicate_claim_id() {
        let es = EvidenceSetV1 {
            version: EVIDENCE_SET_V1_VERSION,
            evidence_bundle_id: h32(1),
            items: vec![
                EvidenceSetItemV1 {
                    claim_id: 1,
                    claim_text: "a".to_string(),
                    evidence_refs: vec![],
                },
                EvidenceSetItemV1 {
                    claim_id: 1,
                    claim_text: "b".to_string(),
                    evidence_refs: vec![],
                },
            ],
        };
        let bytes = es.encode().unwrap_err();
        let _ = bytes;

        // Build a non-canonical byte stream directly (claim_id not unique).
        let mut w = ByteWriter::with_capacity(128);
        w.write_u16(EVIDENCE_SET_V1_VERSION);
        w.write_raw(&h32(1));
        w.write_u32(2);
        w.write_u32(1);
        w.write_str("a").unwrap();
        w.write_u32(0);
        w.write_u32(1);
        w.write_str("b").unwrap();
        w.write_u32(0);
        let bad = w.into_bytes();

        let got = EvidenceSetV1::decode(&bad);
        assert!(got.is_err());
    }
}
