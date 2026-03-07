// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Forecast schema.
//!
//! ForecastV1 is a replayable, deterministic prediction record used to
//! represent top-k guesses about what the user may ask next.
//!
//! Scope:
//! - schema + canonical codec + validation helpers
//! - unit tests for determinism and canonical decoding
//!
//! Out of scope:
//! - forecast builder implementation
//! - wiring into planner/answer CLI

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::frame::Id64;
use crate::hash::Hash32;

/// ForecastV1 schema version.
pub const FORECAST_V1_VERSION: u32 = 1;

/// Maximum number of intent predictions allowed in v1.
pub const FORECAST_V1_MAX_INTENTS: usize = 32;

/// Maximum number of predicted questions allowed in v1.
pub const FORECAST_V1_MAX_QUESTIONS: usize = 16;

/// Maximum length in bytes for any predicted question text.
pub const FORECAST_V1_MAX_TEXT_BYTES: usize = 512;

/// Forecast flags (v1).
///
/// These flags are advisory and do not change evidence-first contracts.
///
/// Canonical encoding requires that unknown bits are not set.
pub type ForecastFlagsV1 = u32;

/// Forecast used a pragmatics frame as an input.
pub const FC_FLAG_HAS_PRAGMATICS: ForecastFlagsV1 = 1u32 << 0;

/// Forecast used prior conversation history (ReplayLog context).
pub const FC_FLAG_HAS_HISTORY: ForecastFlagsV1 = 1u32 << 1;

/// Forecast used Markov/PPM style hints.
pub const FC_FLAG_USED_MARKOV: ForecastFlagsV1 = 1u32 << 2;

/// Forecast used lexicon expansion context.
pub const FC_FLAG_USED_LEXICON: ForecastFlagsV1 = 1u32 << 3;

/// Mask of all known v1 flags.
pub const FC_FLAGS_V1_ALL: ForecastFlagsV1 =
    FC_FLAG_HAS_PRAGMATICS | FC_FLAG_HAS_HISTORY | FC_FLAG_USED_MARKOV | FC_FLAG_USED_LEXICON;

/// High-level forecast intent kinds (v1).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
#[repr(u8)]
pub enum ForecastIntentKindV1 {
    /// The user is likely to ask for clarification.
    Clarify = 1,
    /// The user is likely to ask for an example.
    Example = 2,
    /// The user is likely to ask for more detail.
    MoreDetail = 3,
    /// The user is likely to ask for a comparison.
    Compare = 4,
    /// The user is likely to ask for next steps.
    NextSteps = 5,
    /// The user is likely to ask about risks/caveats.
    Risks = 6,
    /// The user is likely to ask about implementation.
    Implementation = 7,
    /// The user is likely to ask how to verify or troubleshoot.
    VerifyOrTroubleshoot = 8,
}

impl ForecastIntentKindV1 {
    fn from_u8(v: u8) -> Result<Self, DecodeError> {
        match v {
            1 => Ok(ForecastIntentKindV1::Clarify),
            2 => Ok(ForecastIntentKindV1::Example),
            3 => Ok(ForecastIntentKindV1::MoreDetail),
            4 => Ok(ForecastIntentKindV1::Compare),
            5 => Ok(ForecastIntentKindV1::NextSteps),
            6 => Ok(ForecastIntentKindV1::Risks),
            7 => Ok(ForecastIntentKindV1::Implementation),
            8 => Ok(ForecastIntentKindV1::VerifyOrTroubleshoot),
            _ => Err(DecodeError::new("bad ForecastIntentKindV1")),
        }
    }
}

fn cmp_intent_canon(a: &ForecastIntentV1, b: &ForecastIntentV1) -> core::cmp::Ordering {
    // Canonical order:
    // - score desc
    // - kind asc
    // - intent_id asc
    match b.score.cmp(&a.score) {
        core::cmp::Ordering::Equal => {}
        o => return o,
    }
    match (a.kind as u8).cmp(&(b.kind as u8)) {
        core::cmp::Ordering::Equal => {}
        o => return o,
    }
    a.intent_id.0.cmp(&b.intent_id.0)
}

fn cmp_question_canon(a: &ForecastQuestionV1, b: &ForecastQuestionV1) -> core::cmp::Ordering {
    // Canonical order:
    // - score desc
    // - question_id asc
    match b.score.cmp(&a.score) {
        core::cmp::Ordering::Equal => {}
        o => return o,
    }
    a.question_id.0.cmp(&b.question_id.0)
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

fn intents_are_canon(intents: &[ForecastIntentV1]) -> bool {
    if intents.is_empty() {
        return true;
    }
    let mut prev = &intents[0];
    let mut seen: Vec<(u8, u64)> = Vec::new();
    if !seen_insert_u72(&mut seen, (prev.kind as u8, prev.intent_id.0)) {
        return false;
    }
    for it in intents.iter().skip(1) {
        if cmp_intent_canon(prev, it) == core::cmp::Ordering::Greater {
            return false;
        }
        if !seen_insert_u72(&mut seen, (it.kind as u8, it.intent_id.0)) {
            return false;
        }
        prev = it;
    }
    true
}

fn questions_are_canon(questions: &[ForecastQuestionV1]) -> bool {
    if questions.is_empty() {
        return true;
    }
    let mut prev = &questions[0];
    let mut seen: Vec<u64> = Vec::new();
    if !seen_insert_u64(&mut seen, prev.question_id.0) {
        return false;
    }
    for q in questions.iter().skip(1) {
        if cmp_question_canon(prev, q) == core::cmp::Ordering::Greater {
            return false;
        }
        if !seen_insert_u64(&mut seen, q.question_id.0) {
            return false;
        }
        prev = q;
    }
    true
}

/// One forecast intent prediction (v1).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ForecastIntentV1 {
    /// Intent kind.
    pub kind: ForecastIntentKindV1,
    /// Stable intent id used for deterministic tie-breaking.
    pub intent_id: Id64,
    /// Signed score used for ranking.
    pub score: i64,
    /// Rationale code (rules-first id; 0 may be used for "unspecified").
    pub rationale_code: u16,
}

impl ForecastIntentV1 {
    /// Construct an intent prediction.
    pub fn new(kind: ForecastIntentKindV1, intent_id: Id64, score: i64, rationale_code: u16) -> Self {
        ForecastIntentV1 {
            kind,
            intent_id,
            score,
            rationale_code,
        }
    }
}

/// One forecast question prediction (v1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForecastQuestionV1 {
    /// Stable question id used for deterministic tie-breaking.
    pub question_id: Id64,
    /// Signed score used for ranking.
    pub score: i64,
    /// Question text (UTF-8).
    pub text: String,
    /// Rationale code (rules-first id; 0 may be used for "unspecified").
    pub rationale_code: u16,
}

impl ForecastQuestionV1 {
    /// Construct a question prediction.
    pub fn new(question_id: Id64, score: i64, text: String, rationale_code: u16) -> Self {
        ForecastQuestionV1 {
            question_id,
            score,
            text,
            rationale_code,
        }
    }
}

/// Forecast record (v1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForecastV1 {
    /// Schema version (must be FORECAST_V1_VERSION).
    pub version: u32,
    /// Query id (hash of the user query bytes).
    pub query_id: Hash32,
    /// Flags describing inputs and sources used to produce this forecast.
    pub flags: ForecastFlagsV1,
    /// Horizon in turns (v1 expects 1; upper bound is small).
    pub horizon_turns: u8,
    /// Ranked intent predictions.
    pub intents: Vec<ForecastIntentV1>,
    /// Ranked question predictions.
    pub questions: Vec<ForecastQuestionV1>,
}

/// ForecastV1 validation errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForecastError {
    /// version is not supported.
    BadVersion,
    /// flags contain unknown bits.
    BadFlags,
    /// horizon is invalid.
    BadHorizon,
    /// Too many intents.
    TooManyIntents,
    /// Too many questions.
    TooManyQuestions,
    /// Intents are not in canonical order or contain duplicates.
    IntentsNotCanonical,
    /// Questions are not in canonical order or contain duplicates.
    QuestionsNotCanonical,
    /// Question text length exceeds the cap.
    QuestionTextTooLong,
}

impl core::fmt::Display for ForecastError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ForecastError::BadVersion => f.write_str("bad forecast version"),
            ForecastError::BadFlags => f.write_str("bad forecast flags"),
            ForecastError::BadHorizon => f.write_str("bad forecast horizon"),
            ForecastError::TooManyIntents => f.write_str("too many forecast intents"),
            ForecastError::TooManyQuestions => f.write_str("too many forecast questions"),
            ForecastError::IntentsNotCanonical => f.write_str("forecast intents not canonical"),
            ForecastError::QuestionsNotCanonical => f.write_str("forecast questions not canonical"),
            ForecastError::QuestionTextTooLong => f.write_str("forecast question text too long"),
        }
    }
}

impl ForecastV1 {
    /// Validate invariants.
    pub fn validate(&self) -> Result<(), ForecastError> {
        if self.version != FORECAST_V1_VERSION {
            return Err(ForecastError::BadVersion);
        }
        let unknown = self.flags & !FC_FLAGS_V1_ALL;
        if unknown != 0 {
            return Err(ForecastError::BadFlags);
        }
        // Keep horizon small and deterministic. v1 expects 1, but accept 1..=4.
        if self.horizon_turns == 0 || self.horizon_turns > 4 {
            return Err(ForecastError::BadHorizon);
        }
        if self.intents.len() > FORECAST_V1_MAX_INTENTS {
            return Err(ForecastError::TooManyIntents);
        }
        if self.questions.len() > FORECAST_V1_MAX_QUESTIONS {
            return Err(ForecastError::TooManyQuestions);
        }
        for q in &self.questions {
            if q.text.as_bytes().len() > FORECAST_V1_MAX_TEXT_BYTES {
                return Err(ForecastError::QuestionTextTooLong);
            }
        }
        if !intents_are_canon(&self.intents) {
            return Err(ForecastError::IntentsNotCanonical);
        }
        if !questions_are_canon(&self.questions) {
            return Err(ForecastError::QuestionsNotCanonical);
        }
        Ok(())
    }

    /// Return true if the record is canonical (order + uniqueness).
    pub fn is_canonical(&self) -> bool {
        intents_are_canon(&self.intents) && questions_are_canon(&self.questions)
    }

    /// Encode to canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        self.validate().map_err(|e| {
            EncodeError::new(match e {
                ForecastError::BadVersion => "bad forecast version",
                ForecastError::BadFlags => "bad forecast flags",
                ForecastError::BadHorizon => "bad forecast horizon",
                ForecastError::TooManyIntents => "too many forecast intents",
                ForecastError::TooManyQuestions => "too many forecast questions",
                ForecastError::IntentsNotCanonical => "forecast intents not canonical",
                ForecastError::QuestionsNotCanonical => "forecast questions not canonical",
                ForecastError::QuestionTextTooLong => "forecast question text too long",
            })
        })?;

        let mut cap = 48 + self.intents.len() * 24;
        for q in &self.questions {
            cap += 24 + q.text.len();
        }
        let mut w = ByteWriter::with_capacity(cap);
        w.write_u32(self.version);
        w.write_raw(&self.query_id);
        w.write_u32(self.flags);
        w.write_u8(self.horizon_turns);
        w.write_u8(self.intents.len() as u8);
        w.write_u8(self.questions.len() as u8);
        w.write_u8(0); // reserved

        for it in &self.intents {
            w.write_u8(it.kind as u8);
            w.write_u64(it.intent_id.0);
            w.write_i64(it.score);
            w.write_u16(it.rationale_code);
        }
        for q in &self.questions {
            w.write_u64(q.question_id.0);
            w.write_i64(q.score);
            w.write_str(&q.text)?;
            w.write_u16(q.rationale_code);
        }

        Ok(w.into_bytes())
    }

    /// Decode from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let version = r.read_u32()?;
        if version != FORECAST_V1_VERSION {
            return Err(DecodeError::new("bad forecast version"));
        }

        let qid_b = r.read_fixed(32)?;
        let mut query_id: Hash32 = [0u8; 32];
        query_id.copy_from_slice(qid_b);

        let flags = r.read_u32()?;
        let unknown = flags & !FC_FLAGS_V1_ALL;
        if unknown != 0 {
            return Err(DecodeError::new("bad forecast flags"));
        }

        let horizon_turns = r.read_u8()?;
        if horizon_turns == 0 || horizon_turns > 4 {
            return Err(DecodeError::new("bad forecast horizon"));
        }

        let n_intents = r.read_u8()? as usize;
        let n_questions = r.read_u8()? as usize;
        let _reserved = r.read_u8()?;

        if n_intents > FORECAST_V1_MAX_INTENTS {
            return Err(DecodeError::new("too many forecast intents"));
        }
        if n_questions > FORECAST_V1_MAX_QUESTIONS {
            return Err(DecodeError::new("too many forecast questions"));
        }

        let mut intents: Vec<ForecastIntentV1> = Vec::with_capacity(n_intents);
        for _ in 0..n_intents {
            let kind = ForecastIntentKindV1::from_u8(r.read_u8()?)?;
            let intent_id = Id64(r.read_u64()?);
            let score = r.read_i64()?;
            let rationale_code = r.read_u16()?;
            intents.push(ForecastIntentV1 {
                kind,
                intent_id,
                score,
                rationale_code,
            });
        }

        let mut questions: Vec<ForecastQuestionV1> = Vec::with_capacity(n_questions);
        for _ in 0..n_questions {
            let question_id = Id64(r.read_u64()?);
            let score = r.read_i64()?;
            let text = r.read_str_view()?.to_string();
            if text.as_bytes().len() > FORECAST_V1_MAX_TEXT_BYTES {
                return Err(DecodeError::new("forecast question text too long"));
            }
            let rationale_code = r.read_u16()?;
            questions.push(ForecastQuestionV1 {
                question_id,
                score,
                text,
                rationale_code,
            });
        }

        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }
        if !intents_are_canon(&intents) {
            return Err(DecodeError::new("forecast intents not canonical"));
        }
        if !questions_are_canon(&questions) {
            return Err(DecodeError::new("forecast questions not canonical"));
        }

        Ok(ForecastV1 {
            version,
            query_id,
            flags,
            horizon_turns,
            intents,
            questions,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::blake3_hash;

    fn mk() -> ForecastV1 {
        ForecastV1 {
            version: FORECAST_V1_VERSION,
            query_id: blake3_hash(b"q"),
            flags: FC_FLAG_HAS_PRAGMATICS,
            horizon_turns: 1,
            intents: vec![
                ForecastIntentV1::new(ForecastIntentKindV1::Clarify, Id64(10), 100, 1),
                ForecastIntentV1::new(ForecastIntentKindV1::NextSteps, Id64(11), 90, 2),
            ],
            questions: vec![ForecastQuestionV1::new(
                Id64(1),
                50,
                "What environment are you targeting?".to_string(),
                3,
            )],
        }
    }

    #[test]
    fn forecast_round_trip() {
        let f = mk();
        assert!(f.is_canonical());
        f.validate().unwrap();
        let bytes = f.encode().unwrap();
        let dec = ForecastV1::decode(&bytes).unwrap();
        assert_eq!(dec, f);
    }

    #[test]
    fn forecast_encode_rejects_unknown_flags() {
        let mut f = mk();
        f.flags |= 1u32 << 31;
        assert!(f.encode().is_err());
        assert!(f.validate().is_err());
    }

    #[test]
    fn forecast_decode_rejects_noncanonical_intents() {
        let mut f = mk();
        f.intents.swap(0, 1);
        assert!(!f.is_canonical());

        let mut w = ByteWriter::with_capacity(256);
        w.write_u32(FORECAST_V1_VERSION);
        w.write_raw(&f.query_id);
        w.write_u32(f.flags);
        w.write_u8(f.horizon_turns);
        w.write_u8(f.intents.len() as u8);
        w.write_u8(f.questions.len() as u8);
        w.write_u8(0);
        for it in &f.intents {
            w.write_u8(it.kind as u8);
            w.write_u64(it.intent_id.0);
            w.write_i64(it.score);
            w.write_u16(it.rationale_code);
        }
        for q in &f.questions {
            w.write_u64(q.question_id.0);
            w.write_i64(q.score);
            w.write_str(&q.text).unwrap();
            w.write_u16(q.rationale_code);
        }

        let bytes = w.into_bytes();
        assert!(ForecastV1::decode(&bytes).is_err());
    }

    #[test]
    fn forecast_decode_rejects_trailing_bytes() {
        let mut bytes = mk().encode().unwrap();
        bytes.push(0);
        assert!(ForecastV1::decode(&bytes).is_err());
    }
}
