// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Lexicon segmenting helpers.
//!
//! This module partitions raw lexicon rows into `N` independent segments.
//! The partition key is the owner `LemmaId`:
//! - `LemmaRowV1` owner: `lemma_id`
//! - `SenseRowV1` owner: `lemma_id`
//! - `PronunciationRowV1` owner: `lemma_id`
//! - `RelationEdgeRowV1` owner:
//! - `RelFromId::Lemma(lemma_id)` -> `lemma_id`
//! - `RelFromId::Sense(sense_id)` -> the parent lemma of `sense_id`
//!
//! The segment index is computed as:
//!
//! `seg_ix = mix64(owner_lemma_id_u64) % segment_count`
//!
//! The mixer is a SplitMix64-like function copied from other deterministic
//! components in this repo. Do not change the mixer or constants lightly,
//! because it would change segment assignment and thus artifact ids.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use crate::lexicon::{
    LemmaId, LemmaRowV1, PronunciationRowV1, RelFromId, RelationEdgeRowV1, SenseId, SenseRowV1,
};

/// Unsegmented or segmented row bundles (in row form).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LexiconRowsV1 {
    /// Lemma rows for this bundle.
    pub lemmas: Vec<LemmaRowV1>,
    /// Sense rows for this bundle.
    pub senses: Vec<SenseRowV1>,
    /// Relation edge rows for this bundle.
    pub rels: Vec<RelationEdgeRowV1>,
    /// Pronunciation rows for this bundle.
    pub prons: Vec<PronunciationRowV1>,
}

impl LexiconRowsV1 {
    /// Returns an empty row bundle.
    pub fn empty() -> Self {
        Self {
            lemmas: Vec::new(),
            senses: Vec::new(),
            rels: Vec::new(),
            prons: Vec::new(),
        }
    }
}

/// Errors produced by lexicon segmenting.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LexiconSegmentationError {
    /// Segment count is zero.
    InvalidSegmentCount,
    /// A relation references a `SenseId` that was not provided in `senses`.
    UnknownSense {
        /// The missing `SenseId` referenced by a relation.
        sense_id: SenseId,
    },
    /// The same `SenseId` was observed with different owning lemma ids.
    SenseOwnerMismatch {
        /// The duplicated `SenseId`.
        sense_id: SenseId,
        /// First observed owning lemma id.
        a: LemmaId,
        /// Second observed owning lemma id.
        b: LemmaId,
    },
}

impl fmt::Display for LexiconSegmentationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LexiconSegmentationError::InvalidSegmentCount => write!(f, "segment_count must be > 0"),
            LexiconSegmentationError::UnknownSense { sense_id } => {
                write!(f, "unknown sense_id: {}", (sense_id.0).0)
            }
            LexiconSegmentationError::SenseOwnerMismatch { sense_id, a, b } => write!(
                f,
                "sense_id maps to two lemmas: sense_id={}, a={}, b={}",
                (sense_id.0).0,
                (a.0).0,
                (b.0).0
            ),
        }
    }
}

impl Error for LexiconSegmentationError {}

fn build_sense_owner_map(
    senses: &[SenseRowV1],
) -> Result<BTreeMap<SenseId, LemmaId>, LexiconSegmentationError> {
    let mut m: BTreeMap<SenseId, LemmaId> = BTreeMap::new();
    for s in senses {
        match m.get(&s.sense_id) {
            None => {
                m.insert(s.sense_id, s.lemma_id);
            }
            Some(&existing) => {
                if existing != s.lemma_id {
                    return Err(LexiconSegmentationError::SenseOwnerMismatch {
                        sense_id: s.sense_id,
                        a: existing,
                        b: s.lemma_id,
                    });
                }
            }
        }
    }
    Ok(m)
}

fn owner_lemma_for_rel(
    r: &RelationEdgeRowV1,
    sense_owner: &BTreeMap<SenseId, LemmaId>,
) -> Result<LemmaId, LexiconSegmentationError> {
    match r.from {
        RelFromId::Lemma(lid) => Ok(lid),
        RelFromId::Sense(sid) => sense_owner
            .get(&sid)
            .copied()
            .ok_or(LexiconSegmentationError::UnknownSense { sense_id: sid }),
    }
}

fn lemma_u64(lemma_id: LemmaId) -> u64 {
    (lemma_id.0).0
}

fn seg_ix_for_lemma(lemma_id: LemmaId, segment_count: usize) -> usize {
    debug_assert!(segment_count > 0);
    let x = lemma_u64(lemma_id);
    let h = mix64(x);
    (h % (segment_count as u64)) as usize
}

/// Partition lexicon rows into `segment_count` buckets based on owner `LemmaId`.
///
/// The returned vector has length `segment_count`. Each element contains the rows
/// that belong to that segment. Ordering within each segment is the input order;
/// downstream builders (e.g., `LexiconSegmentV1::build_from_rows`) are expected
/// to canonicalize ordering.
pub fn segment_lexicon_rows_v1(
    rows: LexiconRowsV1,
    segment_count: usize,
) -> Result<Vec<LexiconRowsV1>, LexiconSegmentationError> {
    if segment_count == 0 {
        return Err(LexiconSegmentationError::InvalidSegmentCount);
    }

    let sense_owner = build_sense_owner_map(&rows.senses)?;

    let mut out: Vec<LexiconRowsV1> = (0..segment_count).map(|_| LexiconRowsV1::empty()).collect();

    for l in rows.lemmas {
        let ix = seg_ix_for_lemma(l.lemma_id, segment_count);
        out[ix].lemmas.push(l);
    }
    for s in rows.senses {
        let ix = seg_ix_for_lemma(s.lemma_id, segment_count);
        out[ix].senses.push(s);
    }
    for p in rows.prons {
        let ix = seg_ix_for_lemma(p.lemma_id, segment_count);
        out[ix].prons.push(p);
    }
    for r in rows.rels {
        let owner = owner_lemma_for_rel(&r, &sense_owner)?;
        let ix = seg_ix_for_lemma(owner, segment_count);
        out[ix].rels.push(r);
    }

    Ok(out)
}

/// Compute the segment index for an owner `LemmaId`.
///
/// This is the same mixer and modulus used by `segment_lexicon_rows_v1`.
/// Callers can use this to assign rows incrementally without buffering the full
/// lexicon in memory.
pub fn segment_index_for_lemma_id_v1(
    lemma_id: LemmaId,
    segment_count: usize,
) -> Result<usize, LexiconSegmentationError> {
    if segment_count == 0 {
        return Err(LexiconSegmentationError::InvalidSegmentCount);
    }
    Ok(seg_ix_for_lemma(lemma_id, segment_count))
}

// Deterministic 64-bit mixer (SplitMix64-like).
fn mix64(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E3779B97F4A7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
     use super::*;

    use crate::frame::Id64;
    use crate::lexicon::{derive_lemma_id, POS_NOUN, REL_SYNONYM};
    use crate::lexicon_segment::LexiconSegmentV1;

    fn mk_rows() -> LexiconRowsV1 {
        let l1 = LemmaRowV1::new("Night", POS_NOUN, 0);
        let l2 = LemmaRowV1::new("day", POS_NOUN, 0);
        let l3 = LemmaRowV1::new("Evening", POS_NOUN, 0);

        let s1 = SenseRowV1::new(l1.lemma_id, 0, "The time of darkness.", 0);
        let s2 = SenseRowV1::new(l2.lemma_id, 0, "The period of light.", 0);
        let s3 = SenseRowV1::new(l3.lemma_id, 0, "Late afternoon.", 0);

        let r1 = RelationEdgeRowV1::new(
            RelFromId::Sense(s1.sense_id),
            REL_SYNONYM,
            derive_lemma_id("evening"),
        );
        let r2 = RelationEdgeRowV1::new(RelFromId::Lemma(l2.lemma_id), REL_SYNONYM, l1.lemma_id);

        let p1 = PronunciationRowV1::new(l1.lemma_id, "nait", vec![], 0);

        LexiconRowsV1 {
            lemmas: vec![l1, l2, l3],
            senses: vec![s1, s2, s3],
            rels: vec![r1, r2],
            prons: vec![p1],
        }
    }

    #[test]
    fn segmenting_rejects_zero_segments() {
        let rows = mk_rows();
        let err = segment_lexicon_rows_v1(rows, 0).unwrap_err();
        assert_eq!(err, LexiconSegmentationError::InvalidSegmentCount);
    }

    #[test]
    fn segmenting_assigns_by_owner_lemma() {
        let rows = mk_rows();
        let segs = segment_lexicon_rows_v1(rows.clone(), 4).unwrap();
        assert_eq!(segs.len(), 4);

        let mut seen: BTreeMap<LemmaId, usize> = BTreeMap::new();
        for (ix, s) in segs.iter().enumerate() {
            for l in &s.lemmas {
                assert!(seen.insert(l.lemma_id, ix).is_none());
            }
        }
        assert_eq!(seen.len(), rows.lemmas.len());

        for (ix, s) in segs.iter().enumerate() {
            for r in &s.senses {
                assert_eq!(seen.get(&r.lemma_id).copied().unwrap(), ix);
            }
            for p in &s.prons {
                assert_eq!(seen.get(&p.lemma_id).copied().unwrap(), ix);
            }
        }

        let sense_owner = build_sense_owner_map(&rows.senses).unwrap();
        for (ix, s) in segs.iter().enumerate() {
            for r in &s.rels {
                let owner = owner_lemma_for_rel(r, &sense_owner).unwrap();
                assert_eq!(seen.get(&owner).copied().unwrap(), ix);
            }
        }
    }

    #[test]
    fn segmenting_is_deterministic_under_input_order_after_build() {
        let rows_a = mk_rows();

        let mut rows_b = mk_rows();
        rows_b.lemmas.reverse();
        rows_b.senses.reverse();
        rows_b.rels.reverse();
        rows_b.prons.reverse();

        let a = segment_lexicon_rows_v1(rows_a, 8).unwrap();
        let b = segment_lexicon_rows_v1(rows_b, 8).unwrap();

        for i in 0..8usize {
            let sa = LexiconSegmentV1::build_from_rows(&a[i].lemmas, &a[i].senses, &a[i].rels, &a[i].prons).unwrap();
            let sb = LexiconSegmentV1::build_from_rows(&b[i].lemmas, &b[i].senses, &b[i].rels, &b[i].prons).unwrap();
            assert_eq!(sa.encode().unwrap(), sb.encode().unwrap());
        }
    }

    #[test]
    fn segmenting_rejects_unknown_sense_in_relation_owner() {
        let mut rows = mk_rows();
        rows.rels.push(RelationEdgeRowV1::new(
            RelFromId::Sense(SenseId(Id64(999))),
            REL_SYNONYM,
            derive_lemma_id("x"),
        ));
        let err = segment_lexicon_rows_v1(rows, 4).unwrap_err();
        assert_eq!(err, LexiconSegmentationError::UnknownSense { sense_id: SenseId(Id64(999)) });
    }

    #[test]
    fn segmenting_rejects_sense_owner_mismatch() {
        let mut rows = mk_rows();
        let s = rows.senses[0].clone();
        let b = rows.lemmas[1].lemma_id;
        let mut s2 = s.clone();
        s2.lemma_id = b;
        rows.senses.push(s2);

        let err = segment_lexicon_rows_v1(rows, 4).unwrap_err();
        assert_eq!(
            err,
            LexiconSegmentationError::SenseOwnerMismatch {
                sense_id: s.sense_id,
                a: s.lemma_id,
                b
            }
        );
    }
}
