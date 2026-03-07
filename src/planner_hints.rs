// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Planner hints schema.
//!
//! PlannerHintsV1 is a replayable, deterministic advisory record that can be
//! emitted by a rules-first hint generator.
//!
//! Scope:
//! - schema + canonical codec + validation helpers
//! - unit tests for determinism and canonical decoding
//!
//! Out of scope:
//! - building hints from evidence/pragmatics
//! - wiring into planner/answer CLI

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::frame::Id64;
use crate::hash::Hash32;

/// PlannerHintsV1 schema version.
pub const PLANNER_HINTS_V1_VERSION: u32 = 1;

/// Maximum number of hint items allowed in v1.
pub const PLANNER_HINTS_V1_MAX_HINTS: usize = 64;

/// Maximum number of followups allowed in v1.
pub const PLANNER_HINTS_V1_MAX_FOLLOWUPS: usize = 32;

/// Maximum length in bytes for any followup text.
pub const PLANNER_HINTS_V1_MAX_TEXT_BYTES: usize = 512;

/// Planner hints flags (v1).
///
/// These flags are advisory and do not change the evidence-first contracts.
///
/// Canonical encoding requires that unknown bits are not set.
pub type PlannerHintsFlagsV1 = u32;

/// Prefer asking a single clarifying question before producing a full answer.
pub const PH_FLAG_PREFER_CLARIFY: PlannerHintsFlagsV1 = 1u32 << 0;

/// Prefer a short, direct answer (minimize verbosity).
pub const PH_FLAG_PREFER_DIRECT: PlannerHintsFlagsV1 = 1u32 << 1;

/// Prefer a structured response with explicit steps.
pub const PH_FLAG_PREFER_STEPS: PlannerHintsFlagsV1 = 1u32 << 2;

/// Prefer including explicit caveats/uncertainty notes.
pub const PH_FLAG_PREFER_CAVEATS: PlannerHintsFlagsV1 = 1u32 << 3;

/// Mask of all known v1 flags.
pub const PH_FLAGS_V1_ALL: PlannerHintsFlagsV1 =
    PH_FLAG_PREFER_CLARIFY | PH_FLAG_PREFER_DIRECT | PH_FLAG_PREFER_STEPS | PH_FLAG_PREFER_CAVEATS;

/// High-level hint item kinds (v1).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
#[repr(u8)]
pub enum PlannerHintKindV1 {
    /// Ask a clarifying question (planner may emit a question-like plan).
    Clarify = 1,
    /// Proceed with bounded assumptions when evidence supports an answer.
    AssumeAndAnswer = 2,
    /// Prefer a checklist/steps style plan.
    Steps = 3,
    /// Prefer a concise summary-first plan.
    SummaryFirst = 4,
    /// Prefer an explicit comparison structure.
    Compare = 5,
}

impl PlannerHintKindV1 {
    fn from_u8(v: u8) -> Result<Self, DecodeError> {
        match v {
            1 => Ok(PlannerHintKindV1::Clarify),
            2 => Ok(PlannerHintKindV1::AssumeAndAnswer),
            3 => Ok(PlannerHintKindV1::Steps),
            4 => Ok(PlannerHintKindV1::SummaryFirst),
            5 => Ok(PlannerHintKindV1::Compare),
            _ => Err(DecodeError::new("bad PlannerHintKindV1")),
        }
    }
}

fn cmp_hint_canon(a: &PlannerHintItemV1, b: &PlannerHintItemV1) -> core::cmp::Ordering {
    // Canonical order:
    // - score desc
    // - kind asc
    // - hint_id asc
    match b.score.cmp(&a.score) {
        core::cmp::Ordering::Equal => {}
        o => return o,
    }
    match (a.kind as u8).cmp(&(b.kind as u8)) {
        core::cmp::Ordering::Equal => {}
        o => return o,
    }
    a.hint_id.0.cmp(&b.hint_id.0)
}

fn cmp_followup_canon(a: &PlannerFollowupV1, b: &PlannerFollowupV1) -> core::cmp::Ordering {
    // Canonical order:
    // - score desc
    // - followup_id asc
    match b.score.cmp(&a.score) {
        core::cmp::Ordering::Equal => {}
        o => return o,
    }
    a.followup_id.0.cmp(&b.followup_id.0)
}

fn seen_insert_u72(seen: &mut Vec<(u8, u64)>, key: (u8, u64)) -> bool {
    match seen.binary_search(&key) {
        Ok(_) => false,
        Err(pos) => {
            seen.insert(pos, key);
            true
        }
    }
}

fn seen_insert_u64(seen: &mut Vec<u64>, key: u64) -> bool {
    match seen.binary_search(&key) {
        Ok(_) => false,
        Err(pos) => {
            seen.insert(pos, key);
            true
        }
    }
}

/// One planner hint item (v1).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlannerHintItemV1 {
    /// Hint kind.
    pub kind: PlannerHintKindV1,
    /// Stable hint id used for deterministic tie-breaking.
    pub hint_id: Id64,
    /// Signed score used for ranking.
    pub score: i64,
    /// Rationale code (rules-first id; 0 may be used for "unspecified").
    pub rationale_code: u16,
}

impl PlannerHintItemV1 {
    /// Construct a hint item.
    pub fn new(kind: PlannerHintKindV1, hint_id: Id64, score: i64, rationale_code: u16) -> Self {
        PlannerHintItemV1 {
            kind,
            hint_id,
            score,
            rationale_code,
        }
    }
}

/// One suggested followup (v1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannerFollowupV1 {
    /// Stable followup id used for deterministic tie-breaking.
    pub followup_id: Id64,
    /// Signed score used for ranking.
    pub score: i64,
    /// Followup text (UTF-8).
    pub text: String,
    /// Rationale code (rules-first id; 0 may be used for "unspecified").
    pub rationale_code: u16,
}

impl PlannerFollowupV1 {
    /// Construct a followup.
    pub fn new(followup_id: Id64, score: i64, text: String, rationale_code: u16) -> Self {
        PlannerFollowupV1 {
            followup_id,
            score,
            text,
            rationale_code,
        }
    }
}

/// Planner hints record (v1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannerHintsV1 {
    /// Schema version (must be PLANNER_HINTS_V1_VERSION).
    pub version: u32,
    /// Query id (hash of the user query bytes).
    pub query_id: Hash32,
    /// Flags controlling high-level planner preferences.
    pub flags: PlannerHintsFlagsV1,
    /// Ranked hint items.
    pub hints: Vec<PlannerHintItemV1>,
    /// Ranked followup suggestions.
    pub followups: Vec<PlannerFollowupV1>,
}

/// PlannerHintsV1 validation errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlannerHintsError {
    /// version is not supported.
    BadVersion,
    /// flags contain unknown bits.
    BadFlags,
    /// Too many hints.
    TooManyHints,
    /// Too many followups.
    TooManyFollowups,
    /// Hints are not in canonical order or contain duplicates.
    HintsNotCanonical,
    /// Followups are not in canonical order or contain duplicates.
    FollowupsNotCanonical,
    /// Followup text length exceeds the cap.
    FollowupTextTooLong,
}

impl core::fmt::Display for PlannerHintsError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            PlannerHintsError::BadVersion => f.write_str("bad planner hints version"),
            PlannerHintsError::BadFlags => f.write_str("bad planner hints flags"),
            PlannerHintsError::TooManyHints => f.write_str("too many planner hints"),
            PlannerHintsError::TooManyFollowups => f.write_str("too many planner followups"),
            PlannerHintsError::HintsNotCanonical => f.write_str("planner hints not canonical"),
            PlannerHintsError::FollowupsNotCanonical => {
                f.write_str("planner followups not canonical")
            }
            PlannerHintsError::FollowupTextTooLong => f.write_str("planner followup text too long"),
        }
    }
}

impl std::error::Error for PlannerHintsError {}

impl PlannerHintsV1 {
    /// Return true if hints and followups are strictly canonical.
    pub fn is_canonical(&self) -> bool {
        for i in 1..self.hints.len() {
            if cmp_hint_canon(&self.hints[i - 1], &self.hints[i]) != core::cmp::Ordering::Less {
                return false;
            }
        }
        for i in 1..self.followups.len() {
            if cmp_followup_canon(&self.followups[i - 1], &self.followups[i])
                != core::cmp::Ordering::Less
            {
                return false;
            }
        }
        true
    }

    /// Validate invariants.
    pub fn validate(&self) -> Result<(), PlannerHintsError> {
        if self.version != PLANNER_HINTS_V1_VERSION {
            return Err(PlannerHintsError::BadVersion);
        }
        if (self.flags & !PH_FLAGS_V1_ALL) != 0 {
            return Err(PlannerHintsError::BadFlags);
        }
        if self.hints.len() > PLANNER_HINTS_V1_MAX_HINTS {
            return Err(PlannerHintsError::TooManyHints);
        }
        if self.followups.len() > PLANNER_HINTS_V1_MAX_FOLLOWUPS {
            return Err(PlannerHintsError::TooManyFollowups);
        }
        for fu in &self.followups {
            if fu.text.as_bytes().len() > PLANNER_HINTS_V1_MAX_TEXT_BYTES {
                return Err(PlannerHintsError::FollowupTextTooLong);
            }
        }
        if !self.is_canonical() {
            // Pinpoint the failing list.
            for i in 1..self.hints.len() {
                if cmp_hint_canon(&self.hints[i - 1], &self.hints[i]) != core::cmp::Ordering::Less {
                    return Err(PlannerHintsError::HintsNotCanonical);
                }
            }
            return Err(PlannerHintsError::FollowupsNotCanonical);
        }

        // Uniqueness: (kind, hint_id) and followup_id.
        let mut seen_h: Vec<(u8, u64)> = Vec::with_capacity(self.hints.len());
        for h in &self.hints {
            let key = (h.kind as u8, h.hint_id.0);
            if !seen_insert_u72(&mut seen_h, key) {
                return Err(PlannerHintsError::HintsNotCanonical);
            }
        }
        let mut seen_fu: Vec<u64> = Vec::with_capacity(self.followups.len());
        for fu in &self.followups {
            if !seen_insert_u64(&mut seen_fu, fu.followup_id.0) {
                return Err(PlannerHintsError::FollowupsNotCanonical);
            }
        }
        Ok(())
    }

    /// Encode to canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        if self.version != PLANNER_HINTS_V1_VERSION {
            return Err(EncodeError::new("bad planner hints version"));
        }
        if (self.flags & !PH_FLAGS_V1_ALL) != 0 {
            return Err(EncodeError::new("bad planner hints flags"));
        }
        if self.hints.len() > PLANNER_HINTS_V1_MAX_HINTS {
            return Err(EncodeError::new("too many planner hints"));
        }
        if self.followups.len() > PLANNER_HINTS_V1_MAX_FOLLOWUPS {
            return Err(EncodeError::new("too many planner followups"));
        }
        for fu in &self.followups {
            if fu.text.as_bytes().len() > PLANNER_HINTS_V1_MAX_TEXT_BYTES {
                return Err(EncodeError::new("planner followup text too long"));
            }
        }
        if !self.is_canonical() {
            return Err(EncodeError::new("non-canonical planner hints"));
        }

        // Uniqueness enforcement.
        let mut seen_h: Vec<(u8, u64)> = Vec::with_capacity(self.hints.len());
        for h in &self.hints {
            let key = (h.kind as u8, h.hint_id.0);
            if !seen_insert_u72(&mut seen_h, key) {
                return Err(EncodeError::new("duplicate planner hint"));
            }
        }
        let mut seen_fu: Vec<u64> = Vec::with_capacity(self.followups.len());
        for fu in &self.followups {
            if !seen_insert_u64(&mut seen_fu, fu.followup_id.0) {
                return Err(EncodeError::new("duplicate planner followup"));
            }
        }

        let mut cap = 64 + self.hints.len() * 32 + self.followups.len() * 48;
        for fu in &self.followups {
            cap = cap.saturating_add(fu.text.len());
        }
        let mut w = ByteWriter::with_capacity(cap);
        w.write_u32(self.version);
        w.write_raw(&self.query_id);
        w.write_u32(self.flags);
        w.write_u8(self.hints.len() as u8);
        w.write_u8(self.followups.len() as u8);

        for h in &self.hints {
            w.write_u8(h.kind as u8);
            w.write_u64(h.hint_id.0);
            w.write_i64(h.score);
            w.write_u16(h.rationale_code);
        }
        for fu in &self.followups {
            w.write_u64(fu.followup_id.0);
            w.write_i64(fu.score);
            w.write_str(&fu.text)?;
            w.write_u16(fu.rationale_code);
        }
        Ok(w.into_bytes())
    }

    /// Decode from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let version = r.read_u32()?;
        if version != PLANNER_HINTS_V1_VERSION {
            return Err(DecodeError::new("bad planner hints version"));
        }
        let qid_b = r.read_fixed(32)?;
        let mut query_id = [0u8; 32];
        query_id.copy_from_slice(qid_b);

        let flags = r.read_u32()?;
        if (flags & !PH_FLAGS_V1_ALL) != 0 {
            return Err(DecodeError::new("bad planner hints flags"));
        }

        let hints_n = r.read_u8()? as usize;
        let followups_n = r.read_u8()? as usize;
        if hints_n > PLANNER_HINTS_V1_MAX_HINTS {
            return Err(DecodeError::new("too many planner hints"));
        }
        if followups_n > PLANNER_HINTS_V1_MAX_FOLLOWUPS {
            return Err(DecodeError::new("too many planner followups"));
        }

        let mut hints: Vec<PlannerHintItemV1> = Vec::with_capacity(hints_n);
        let mut followups: Vec<PlannerFollowupV1> = Vec::with_capacity(followups_n);

        for _ in 0..hints_n {
            let kind = PlannerHintKindV1::from_u8(r.read_u8()?)?;
            let hint_id = Id64(r.read_u64()?);
            let score = r.read_i64()?;
            let rationale_code = r.read_u16()?;
            hints.push(PlannerHintItemV1::new(kind, hint_id, score, rationale_code));
        }
        for _ in 0..followups_n {
            let followup_id = Id64(r.read_u64()?);
            let score = r.read_i64()?;
            let text = r.read_str_view()?.to_string();
            if text.as_bytes().len() > PLANNER_HINTS_V1_MAX_TEXT_BYTES {
                return Err(DecodeError::new("planner followup text too long"));
            }
            let rationale_code = r.read_u16()?;
            followups.push(PlannerFollowupV1::new(
                followup_id,
                score,
                text,
                rationale_code,
            ));
        }
        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        // Enforce canonical order and uniqueness.
        for i in 1..hints.len() {
            if cmp_hint_canon(&hints[i - 1], &hints[i]) != core::cmp::Ordering::Less {
                return Err(DecodeError::new("planner hints not canonical"));
            }
        }
        for i in 1..followups.len() {
            if cmp_followup_canon(&followups[i - 1], &followups[i]) != core::cmp::Ordering::Less {
                return Err(DecodeError::new("planner followups not canonical"));
            }
        }

        let mut seen_h: Vec<(u8, u64)> = Vec::with_capacity(hints.len());
        for h in &hints {
            let key = (h.kind as u8, h.hint_id.0);
            if !seen_insert_u72(&mut seen_h, key) {
                return Err(DecodeError::new("duplicate planner hint"));
            }
        }
        let mut seen_fu: Vec<u64> = Vec::with_capacity(followups.len());
        for fu in &followups {
            if !seen_insert_u64(&mut seen_fu, fu.followup_id.0) {
                return Err(DecodeError::new("duplicate planner followup"));
            }
        }

        Ok(PlannerHintsV1 {
            version,
            query_id,
            flags,
            hints,
            followups,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::blake3_hash;

    fn mk() -> PlannerHintsV1 {
        PlannerHintsV1 {
            version: PLANNER_HINTS_V1_VERSION,
            query_id: blake3_hash(b"q"),
            flags: PH_FLAG_PREFER_DIRECT,
            hints: vec![
                PlannerHintItemV1::new(PlannerHintKindV1::Steps, Id64(10), 100, 1),
                PlannerHintItemV1::new(PlannerHintKindV1::SummaryFirst, Id64(11), 90, 2),
            ],
            followups: vec![PlannerFollowupV1::new(
                Id64(1),
                50,
                "Do you want A or B?".to_string(),
                3,
            )],
        }
    }

    #[test]
    fn planner_hints_round_trip() {
        let h = mk();
        assert!(h.is_canonical());
        h.validate().unwrap();
        let bytes = h.encode().unwrap();
        let dec = PlannerHintsV1::decode(&bytes).unwrap();
        assert_eq!(dec, h);
    }

    #[test]
    fn planner_hints_encode_rejects_unknown_flags() {
        let mut h = mk();
        h.flags |= 1u32 << 31;
        assert!(h.encode().is_err());
        assert!(h.validate().is_err());
    }

    #[test]
    fn planner_hints_decode_rejects_noncanonical_hints() {
        let mut h = mk();
        h.hints.swap(0, 1);
        assert!(!h.is_canonical());
        let mut w = ByteWriter::with_capacity(256);
        w.write_u32(PLANNER_HINTS_V1_VERSION);
        w.write_raw(&h.query_id);
        w.write_u32(h.flags);
        w.write_u8(h.hints.len() as u8);
        w.write_u8(h.followups.len() as u8);
        for it in &h.hints {
            w.write_u8(it.kind as u8);
            w.write_u64(it.hint_id.0);
            w.write_i64(it.score);
            w.write_u16(it.rationale_code);
        }
        for fu in &h.followups {
            w.write_u64(fu.followup_id.0);
            w.write_i64(fu.score);
            w.write_str(&fu.text).unwrap();
            w.write_u16(fu.rationale_code);
        }
        let bytes = w.into_bytes();
        assert!(PlannerHintsV1::decode(&bytes).is_err());
    }

    #[test]
    fn planner_hints_decode_rejects_trailing_bytes() {
        let mut bytes = mk().encode().unwrap();
        bytes.push(0);
        assert!(PlannerHintsV1::decode(&bytes).is_err());
    }
}
