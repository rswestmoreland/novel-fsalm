// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Markov/PPM hints schema.
//!
//! MarkovHintsV1 is a replayable, deterministic advisory record that can be
//! emitted by an offline hint generator trained on prior realized token streams.
//!
//! Scope:
//! - schema + canonical codec + validation helpers
//! - unit tests for determinism and canonical decoding
//!
//! Out of scope:
//! - training/building the Markov model
//! - wiring into the realizer

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::frame::Id64;
use crate::hash::Hash32;

/// MarkovHintsV1 schema version.
pub const MARKOV_HINTS_V1_VERSION: u32 = 1;

/// Maximum Markov order supported by v1.
pub const MARKOV_HINTS_V1_MAX_ORDER_N: u8 = 6;

/// Maximum number of choices allowed in v1.
pub const MARKOV_HINTS_V1_MAX_CHOICES: usize = 32;

/// Markov hints flags (v1).
///
/// Canonical encoding requires that unknown bits are not set.
pub type MarkovHintsFlagsV1 = u32;

/// Hints used prior conversation history.
pub const MH_FLAG_HAS_HISTORY: MarkovHintsFlagsV1 = 1u32 << 0;

/// Hints used a pragmatics frame (tone/intent signals).
pub const MH_FLAG_HAS_PRAGMATICS: MarkovHintsFlagsV1 = 1u32 << 1;

/// Hints used a PPM-style predictor.
pub const MH_FLAG_USED_PPM: MarkovHintsFlagsV1 = 1u32 << 2;

/// Hints used lexicon context.
pub const MH_FLAG_USED_LEXICON: MarkovHintsFlagsV1 = 1u32 << 3;

/// Mask of all known v1 flags.
pub const MH_FLAGS_V1_ALL: MarkovHintsFlagsV1 =
    MH_FLAG_HAS_HISTORY | MH_FLAG_HAS_PRAGMATICS | MH_FLAG_USED_PPM | MH_FLAG_USED_LEXICON;

/// Choice kinds for Markov hints (v1).
///
/// The realizer may apply different kinds at different template sites.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(u8)]
pub enum MarkovChoiceKindV1 {
    /// Opener / greeting / first sentence scaffolding.
    Opener = 1,
    /// Mid-response transition phrase scaffolding.
    Transition = 2,
    /// Closer / wrap-up scaffolding.
    Closer = 3,
    /// Other surface form choice (reserved for future use).
    Other = 4,
}

impl MarkovChoiceKindV1 {
    fn from_u8(v: u8) -> Result<Self, DecodeError> {
        match v {
            1 => Ok(MarkovChoiceKindV1::Opener),
            2 => Ok(MarkovChoiceKindV1::Transition),
            3 => Ok(MarkovChoiceKindV1::Closer),
            4 => Ok(MarkovChoiceKindV1::Other),
            _ => Err(DecodeError::new("bad MarkovChoiceKindV1")),
        }
    }
}

fn cmp_choice_canon(a: &MarkovChoiceV1, b: &MarkovChoiceV1) -> core::cmp::Ordering {
    // Canonical order:
    // - score desc
    // - kind asc
    // - choice_id asc
    match b.score.cmp(&a.score) {
        core::cmp::Ordering::Equal => {}
        o => return o,
    }
    match (a.kind as u8).cmp(&(b.kind as u8)) {
        core::cmp::Ordering::Equal => {}
        o => return o,
    }
    a.choice_id.0.cmp(&b.choice_id.0)
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

fn choices_are_canon(choices: &[MarkovChoiceV1]) -> bool {
    if choices.is_empty() {
        return true;
    }
    let mut prev = &choices[0];
    let mut seen: Vec<(u8, u64)> = Vec::new();
    if !seen_insert_u72(&mut seen, (prev.kind as u8, prev.choice_id.0)) {
        return false;
    }
    for c in choices.iter().skip(1) {
        if cmp_choice_canon(prev, c) == core::cmp::Ordering::Greater {
            return false;
        }
        if !seen_insert_u72(&mut seen, (c.kind as u8, c.choice_id.0)) {
            return false;
        }
        prev = c;
    }
    true
}

/// One Markov/PPM surface-form choice (v1).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MarkovChoiceV1 {
    /// Choice kind.
    pub kind: MarkovChoiceKindV1,
    /// Stable choice id used for deterministic tie-breaking.
    pub choice_id: Id64,
    /// Signed score used for ranking.
    pub score: i64,
    /// Rationale code (rules-first id; 0 may be used for "unspecified").
    pub rationale_code: u16,
}

impl MarkovChoiceV1 {
    /// Construct a choice.
    pub fn new(kind: MarkovChoiceKindV1, choice_id: Id64, score: i64, rationale_code: u16) -> Self {
        MarkovChoiceV1 {
            kind,
            choice_id,
            score,
            rationale_code,
        }
    }
}

/// Markov hints record (v1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarkovHintsV1 {
    /// Schema version (must be MARKOV_HINTS_V1_VERSION).
    pub version: u32,
    /// Query id (hash of the user query bytes).
    pub query_id: Hash32,
    /// Flags describing inputs and sources used to produce these hints.
    pub flags: MarkovHintsFlagsV1,
    /// Markov order (n-gram length).
    pub order_n: u8,
    /// Stable state id used for deterministic tie-breaking across hint sources.
    pub state_id: Id64,
    /// Hash of the Markov model artifact used to compute these hints.
    pub model_hash: Hash32,
    /// Hash of the context token stream used to compute these hints.
    pub context_hash: Hash32,
    /// Ranked surface-form choices.
    pub choices: Vec<MarkovChoiceV1>,
}

/// MarkovHintsV1 validation errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarkovHintsError {
    /// version is not supported.
    BadVersion,
    /// flags contain unknown bits.
    BadFlags,
    /// order_n is invalid.
    BadOrder,
    /// Too many choices.
    TooManyChoices,
    /// Choices are not in canonical order or contain duplicates.
    ChoicesNotCanonical,
}

impl core::fmt::Display for MarkovHintsError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            MarkovHintsError::BadVersion => f.write_str("bad markov hints version"),
            MarkovHintsError::BadFlags => f.write_str("bad markov hints flags"),
            MarkovHintsError::BadOrder => f.write_str("bad markov order"),
            MarkovHintsError::TooManyChoices => f.write_str("too many markov choices"),
            MarkovHintsError::ChoicesNotCanonical => f.write_str("markov choices not canonical"),
        }
    }
}

impl MarkovHintsV1 {
    /// Validate invariants.
    pub fn validate(&self) -> Result<(), MarkovHintsError> {
        if self.version != MARKOV_HINTS_V1_VERSION {
            return Err(MarkovHintsError::BadVersion);
        }
        let unknown = self.flags & !MH_FLAGS_V1_ALL;
        if unknown != 0 {
            return Err(MarkovHintsError::BadFlags);
        }
        if self.order_n == 0 || self.order_n > MARKOV_HINTS_V1_MAX_ORDER_N {
            return Err(MarkovHintsError::BadOrder);
        }
        if self.choices.len() > MARKOV_HINTS_V1_MAX_CHOICES {
            return Err(MarkovHintsError::TooManyChoices);
        }
        if !choices_are_canon(&self.choices) {
            return Err(MarkovHintsError::ChoicesNotCanonical);
        }
        Ok(())
    }

    /// Return true if the record is canonical (order + uniqueness).
    pub fn is_canonical(&self) -> bool {
        choices_are_canon(&self.choices)
    }

    /// Encode to canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        self.validate().map_err(|e| {
            EncodeError::new(match e {
                MarkovHintsError::BadVersion => "bad markov hints version",
                MarkovHintsError::BadFlags => "bad markov hints flags",
                MarkovHintsError::BadOrder => "bad markov order",
                MarkovHintsError::TooManyChoices => "too many markov choices",
                MarkovHintsError::ChoicesNotCanonical => "markov choices not canonical",
            })
        })?;

        let cap = 4 + 32 + 4 + 4 + 8 + 32 + 32 + self.choices.len() * 20;
        let mut w = ByteWriter::with_capacity(cap);
        w.write_u32(self.version);
        w.write_raw(&self.query_id);
        w.write_u32(self.flags);
        w.write_u8(self.order_n);
        w.write_u8(self.choices.len() as u8);
        w.write_u8(0); // reserved
        w.write_u8(0); // reserved
        w.write_u64(self.state_id.0);
        w.write_raw(&self.model_hash);
        w.write_raw(&self.context_hash);

        for c in &self.choices {
            w.write_u8(c.kind as u8);
            w.write_u64(c.choice_id.0);
            w.write_i64(c.score);
            w.write_u16(c.rationale_code);
        }
        Ok(w.into_bytes())
    }

    /// Decode from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let version = r.read_u32()?;
        if version != MARKOV_HINTS_V1_VERSION {
            return Err(DecodeError::new("bad markov hints version"));
        }

        let qid_b = r.read_fixed(32)?;
        let mut query_id: Hash32 = [0u8; 32];
        query_id.copy_from_slice(qid_b);

        let flags = r.read_u32()?;
        let unknown = flags & !MH_FLAGS_V1_ALL;
        if unknown != 0 {
            return Err(DecodeError::new("bad markov hints flags"));
        }

        let order_n = r.read_u8()?;
        if order_n == 0 || order_n > MARKOV_HINTS_V1_MAX_ORDER_N {
            return Err(DecodeError::new("bad markov order"));
        }
        let n_choices = r.read_u8()? as usize;
        let _reserved0 = r.read_u8()?;
        let _reserved1 = r.read_u8()?;

        if n_choices > MARKOV_HINTS_V1_MAX_CHOICES {
            return Err(DecodeError::new("too many markov choices"));
        }

        let state_id = Id64(r.read_u64()?);

        let mh_b = r.read_fixed(32)?;
        let mut model_hash: Hash32 = [0u8; 32];
        model_hash.copy_from_slice(mh_b);

        let ch_b = r.read_fixed(32)?;
        let mut context_hash: Hash32 = [0u8; 32];
        context_hash.copy_from_slice(ch_b);

        let mut choices: Vec<MarkovChoiceV1> = Vec::with_capacity(n_choices);
        for _ in 0..n_choices {
            let kind = MarkovChoiceKindV1::from_u8(r.read_u8()?)?;
            let choice_id = Id64(r.read_u64()?);
            let score = r.read_i64()?;
            let rationale_code = r.read_u16()?;
            choices.push(MarkovChoiceV1 {
                kind,
                choice_id,
                score,
                rationale_code,
            });
        }

        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }
        if !choices_are_canon(&choices) {
            return Err(DecodeError::new("markov choices not canonical"));
        }

        Ok(MarkovHintsV1 {
            version,
            query_id,
            flags,
            order_n,
            state_id,
            model_hash,
            context_hash,
            choices,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::blake3_hash;

    fn mk() -> MarkovHintsV1 {
        MarkovHintsV1 {
            version: MARKOV_HINTS_V1_VERSION,
            query_id: blake3_hash(b"q"),
            flags: MH_FLAG_HAS_HISTORY | MH_FLAG_USED_PPM,
            order_n: 3,
            state_id: Id64(123),
            model_hash: blake3_hash(b"model"),
            context_hash: blake3_hash(b"ctx"),
            choices: vec![
                MarkovChoiceV1::new(MarkovChoiceKindV1::Opener, Id64(10), 100, 1),
                MarkovChoiceV1::new(MarkovChoiceKindV1::Transition, Id64(11), 90, 2),
            ],
        }
    }

    #[test]
    fn markov_hints_round_trip() {
        let h = mk();
        assert!(h.is_canonical());
        h.validate().unwrap();
        let bytes = h.encode().unwrap();
        let dec = MarkovHintsV1::decode(&bytes).unwrap();
        assert_eq!(dec, h);
    }

    #[test]
    fn markov_hints_encode_rejects_unknown_flags() {
        let mut h = mk();
        h.flags |= 1u32 << 31;
        assert!(h.encode().is_err());
        assert!(h.validate().is_err());
    }

    #[test]
    fn markov_hints_decode_rejects_noncanonical_choices() {
        let mut h = mk();
        h.choices.swap(0, 1);
        assert!(!h.is_canonical());

        // Build noncanonical bytes manually to ensure decode rejects.
        let mut w = ByteWriter::with_capacity(256);
        w.write_u32(MARKOV_HINTS_V1_VERSION);
        w.write_raw(&h.query_id);
        w.write_u32(h.flags);
        w.write_u8(h.order_n);
        w.write_u8(h.choices.len() as u8);
        w.write_u8(0);
        w.write_u8(0);
        w.write_u64(h.state_id.0);
        w.write_raw(&h.model_hash);
        w.write_raw(&h.context_hash);
        for c in &h.choices {
            w.write_u8(c.kind as u8);
            w.write_u64(c.choice_id.0);
            w.write_i64(c.score);
            w.write_u16(c.rationale_code);
        }
        let bytes2 = w.into_bytes();
        assert!(MarkovHintsV1::decode(&bytes2).is_err());
    }
}
