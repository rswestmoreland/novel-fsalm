// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! AnswerPlanV1 schema.
//!
//! AnswerPlanV1 is a deterministic intermediate representation between
//! retrieval (EvidenceBundleV1) and answer rendering.
//!
//! Goals:
//! - Evidence-first: every plan item references evidence items.
//! - Deterministic: stable ordering rules and bounded sizes.
//! - Replay-friendly: references content-addressed artifacts.
//!
//! v1 is intentionally minimal and bounded.

use crate::hash::Hash32;

/// AnswerPlanV1 schema version.
pub const ANSWER_PLAN_V1_VERSION: u16 = 1;

/// Errors returned by [`AnswerPlanV1::validate`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AnswerPlanValidateError {
    /// The version field is not supported.
    BadVersion,
    /// The plan contains too many items.
    TooManyItems,
    /// The plan claims an unsupported evidence item count.
    BadEvidenceItemCount,
    /// A plan item contains too many evidence references.
    TooManyEvidenceRefs,
    /// An evidence item index is out of range.
    EvidenceIxOutOfRange,
    /// Evidence indices must be sorted ascending within a plan item.
    EvidenceIxNotSorted,
    /// Evidence indices must not contain duplicates within a plan item.
    EvidenceIxDuplicate,
    /// Strength must be within the allowed range.
    BadStrength,
}

impl core::fmt::Display for AnswerPlanValidateError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            AnswerPlanValidateError::BadVersion => f.write_str("bad version"),
            AnswerPlanValidateError::TooManyItems => f.write_str("too many plan items"),
            AnswerPlanValidateError::BadEvidenceItemCount => f.write_str("bad evidence item count"),
            AnswerPlanValidateError::TooManyEvidenceRefs => f.write_str("too many evidence refs"),
            AnswerPlanValidateError::EvidenceIxOutOfRange => {
                f.write_str("evidence item index out of range")
            }
            AnswerPlanValidateError::EvidenceIxNotSorted => {
                f.write_str("evidence item indices not sorted")
            }
            AnswerPlanValidateError::EvidenceIxDuplicate => {
                f.write_str("duplicate evidence item index")
            }
            AnswerPlanValidateError::BadStrength => f.write_str("bad strength"),
        }
    }
}

/// A deterministic answer plan derived from an evidence bundle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnswerPlanV1 {
    /// Schema version.
    pub version: u16,
    /// Stable id for the query (typically derived from the PromptPack or query plan).
    pub query_id: Hash32,
    /// Stable id for the index snapshot used during retrieval.
    pub snapshot_id: Hash32,
    /// EvidenceBundle artifact hash this plan references.
    pub evidence_bundle_id: Hash32,
    /// Number of evidence items in the referenced EvidenceBundle.
    ///
    /// This allows validating evidence item indices without loading the bundle.
    pub evidence_item_count: u32,
    /// Plan items in the chosen order.
    pub items: Vec<AnswerPlanItemV1>,
}

impl AnswerPlanV1 {
    /// Create an empty plan.
    pub fn new(
        query_id: Hash32,
        snapshot_id: Hash32,
        evidence_bundle_id: Hash32,
        evidence_item_count: u32,
    ) -> Self {
        Self {
            version: ANSWER_PLAN_V1_VERSION,
            query_id,
            snapshot_id,
            evidence_bundle_id,
            evidence_item_count,
            items: Vec::new(),
        }
    }

    /// Validate canonical invariants.
    pub fn validate(&self) -> Result<(), AnswerPlanValidateError> {
        if self.version != ANSWER_PLAN_V1_VERSION {
            return Err(AnswerPlanValidateError::BadVersion);
        }

        // Keep plans bounded and replay-friendly.
        if self.items.len() > 16_384 {
            return Err(AnswerPlanValidateError::TooManyItems);
        }

        // Evidence item count is a u32 but keep an explicit cap in case
        // a future codec tries to encode something larger.
        if self.evidence_item_count > 4_000_000_000 {
            return Err(AnswerPlanValidateError::BadEvidenceItemCount);
        }

        for it in self.items.iter() {
            it.validate(self.evidence_item_count)?;
        }

        Ok(())
    }
}

/// Plan item kind (v1).
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AnswerPlanItemKindV1 {
    /// A short summary statement.
    Summary = 1,
    /// A bullet claim supported by evidence.
    Bullet = 2,
    /// A step in a procedure.
    Step = 3,
    /// A caveat or uncertainty note.
    Caveat = 4,
}

/// A single plan item.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnswerPlanItemV1 {
    /// Item kind.
    pub kind: AnswerPlanItemKindV1,
    /// Optional strength score in the range 0..=1000.
    ///
    /// v1 uses this to rank bullets without floats.
    pub strength: u16,
    /// References into the EvidenceBundle items vector.
    ///
    /// Canonical rules (v1):
    /// - sorted ascending
    /// - unique
    pub evidence_item_ix: Vec<u32>,
}

impl AnswerPlanItemV1 {
    /// Create a plan item with no evidence references.
    pub fn new(kind: AnswerPlanItemKindV1) -> Self {
        Self {
            kind,
            strength: 0,
            evidence_item_ix: Vec::new(),
        }
    }

    /// Validate item invariants.
    fn validate(&self, evidence_item_count: u32) -> Result<(), AnswerPlanValidateError> {
        if self.strength > 1000 {
            return Err(AnswerPlanValidateError::BadStrength);
        }

        if self.evidence_item_ix.len() > 256 {
            return Err(AnswerPlanValidateError::TooManyEvidenceRefs);
        }

        let mut prev: Option<u32> = None;
        for &ix in self.evidence_item_ix.iter() {
            if ix >= evidence_item_count {
                return Err(AnswerPlanValidateError::EvidenceIxOutOfRange);
            }
            if let Some(p) = prev {
                if ix < p {
                    return Err(AnswerPlanValidateError::EvidenceIxNotSorted);
                }
                if ix == p {
                    return Err(AnswerPlanValidateError::EvidenceIxDuplicate);
                }
            }
            prev = Some(ix);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::blake3_hash;

    fn sample_ok() -> AnswerPlanV1 {
        let q = blake3_hash(b"query");
        let s = blake3_hash(b"snapshot");
        let b = blake3_hash(b"bundle");
        let mut p = AnswerPlanV1::new(q, s, b, 10);

        let mut it = AnswerPlanItemV1::new(AnswerPlanItemKindV1::Bullet);
        it.strength = 500;
        it.evidence_item_ix.push(1);
        it.evidence_item_ix.push(3);
        it.evidence_item_ix.push(7);
        p.items.push(it);

        p
    }

    #[test]
    fn plan_validate_ok() {
        let p = sample_ok();
        assert!(p.validate().is_ok());
    }

    #[test]
    fn plan_validate_bad_version() {
        let mut p = sample_ok();
        p.version = 2;
        assert_eq!(p.validate(), Err(AnswerPlanValidateError::BadVersion));
    }

    #[test]
    fn plan_validate_out_of_range_evidence_ix() {
        let mut p = sample_ok();
        p.items[0].evidence_item_ix.push(10);
        assert_eq!(
            p.validate(),
            Err(AnswerPlanValidateError::EvidenceIxOutOfRange)
        );
    }

    #[test]
    fn plan_validate_unsorted_evidence_ix() {
        let mut p = sample_ok();
        p.items[0].evidence_item_ix.clear();
        p.items[0].evidence_item_ix.push(4);
        p.items[0].evidence_item_ix.push(1);
        assert_eq!(
            p.validate(),
            Err(AnswerPlanValidateError::EvidenceIxNotSorted)
        );
    }

    #[test]
    fn plan_validate_duplicate_evidence_ix() {
        let mut p = sample_ok();
        p.items[0].evidence_item_ix.clear();
        p.items[0].evidence_item_ix.push(2);
        p.items[0].evidence_item_ix.push(2);
        assert_eq!(
            p.validate(),
            Err(AnswerPlanValidateError::EvidenceIxDuplicate)
        );
    }
}
