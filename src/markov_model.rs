// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Markov/PPM model schema.
//!
//! MarkovModelV1 is a replayable, deterministic artifact representing bounded
//! n-gram transition counts over surface-form tokens (choice kind + choice id).
//!
//! Scope:
//! - schema + canonical codec + validation helpers
//! - unit tests for determinism and canonical decoding
//!
//! Out of scope:
//! - training/building the model
//! - sampling and integration into the realizer

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::frame::Id64;
use crate::hash::Hash32;
use crate::markov_hints::MarkovChoiceKindV1;

/// MarkovModelV1 schema version.
pub const MARKOV_MODEL_V1_VERSION: u32 = 1;

/// Maximum supported order (n-gram length) in v1.
pub const MARKOV_MODEL_V1_MAX_ORDER_N: u8 = 6;

/// Hard cap on number of states encoded in a model.
pub const MARKOV_MODEL_V1_MAX_STATES: usize = 200_000;

/// Hard cap on number of next choices per state.
pub const MARKOV_MODEL_V1_MAX_NEXT_PER_STATE: usize = 64;

fn kind_from_u8(v: u8) -> Result<MarkovChoiceKindV1, DecodeError> {
    match v {
        1 => Ok(MarkovChoiceKindV1::Opener),
        2 => Ok(MarkovChoiceKindV1::Transition),
        3 => Ok(MarkovChoiceKindV1::Closer),
        4 => Ok(MarkovChoiceKindV1::Other),
        _ => Err(DecodeError::new("bad MarkovChoiceKindV1")),
    }
}

fn cmp_next_canon(a: &MarkovNextV1, b: &MarkovNextV1) -> core::cmp::Ordering {
    // Canonical order:
    // - count desc
    // - token asc
    match b.count.cmp(&a.count) {
        core::cmp::Ordering::Equal => {}
        o => return o,
    }
    a.token.cmp(&b.token)
}

fn cmp_state_canon(a: &MarkovStateV1, b: &MarkovStateV1) -> core::cmp::Ordering {
    // Canonical order:
    // - context length desc (higher-order contexts first)
    // - context tokens lexicographic asc
    match b.context.len().cmp(&a.context.len()) {
        core::cmp::Ordering::Equal => {}
        o => return o,
    }
    a.context.cmp(&b.context)
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

fn next_is_canon(next: &[MarkovNextV1]) -> bool {
    if next.is_empty() {
        return true;
    }
    let mut prev = &next[0];
    let mut seen: Vec<(u8, u64)> = Vec::new();
    if !seen_insert_u72(&mut seen, (prev.token.kind as u8, prev.token.choice_id.0)) {
        return false;
    }
    for n in next.iter().skip(1) {
        if cmp_next_canon(prev, n) == core::cmp::Ordering::Greater {
            return false;
        }
        if !seen_insert_u72(&mut seen, (n.token.kind as u8, n.token.choice_id.0)) {
            return false;
        }
        prev = n;
    }
    true
}

fn states_are_canon(states: &[MarkovStateV1]) -> bool {
    if states.is_empty() {
        return true;
    }
    let mut prev = &states[0];
    if !next_is_canon(&prev.next) {
        return false;
    }
    for s in states.iter().skip(1) {
        if !next_is_canon(&s.next) {
            return false;
        }
        if cmp_state_canon(prev, s) == core::cmp::Ordering::Greater {
            return false;
        }
        // Reject duplicate contexts.
        if prev.context == s.context {
            return false;
        }
        prev = s;
    }
    true
}

/// A surface-form token for the Markov model (v1).
///
/// Token = (kind, choice_id).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct MarkovTokenV1 {
    /// Choice kind for this token.
    pub kind: MarkovChoiceKindV1,
    /// Stable choice identifier within the kind domain.
    pub choice_id: Id64,
}

impl MarkovTokenV1 {
    /// Construct a token from kind and choice id.
    pub fn new(kind: MarkovChoiceKindV1, choice_id: Id64) -> Self {
        MarkovTokenV1 { kind, choice_id }
    }
}

/// A next-token count record (v1).
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct MarkovNextV1 {
    /// Next token.
    pub token: MarkovTokenV1,
    /// Observed transition count.
    pub count: u32,
}

/// One model state: a context key and bounded next-token counts (v1).
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct MarkovStateV1 {
    /// Context tokens. Length may be 0 for the unconditional distribution.
    pub context: Vec<MarkovTokenV1>,
    /// Escape count used by PPM-style estimators.
    pub escape_count: u32,
    /// Next-token counts for this context.
    pub next: Vec<MarkovNextV1>,
}

/// MarkovModelV1 validation errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarkovModelError {
    /// Unsupported or mismatched schema version.
    BadVersion,
    /// Invalid order_n_max value.
    BadOrder,
    /// Model contains too many states for v1 caps.
    TooManyStates,
    /// State contains too many next entries or max_next_per_state is invalid.
    TooManyNext,
    /// Context length is outside allowed range for this model order.
    BadContextLen,
    /// Encoded form is not canonical (order/dupes).
    NotCanonical,
}

impl core::fmt::Display for MarkovModelError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            MarkovModelError::BadVersion => f.write_str("bad markov model version"),
            MarkovModelError::BadOrder => f.write_str("bad markov model order"),
            MarkovModelError::TooManyStates => f.write_str("too many markov states"),
            MarkovModelError::TooManyNext => f.write_str("too many markov next entries"),
            MarkovModelError::BadContextLen => f.write_str("bad markov context length"),
            MarkovModelError::NotCanonical => f.write_str("markov model not canonical"),
        }
    }
}

/// Markov model artifact (v1).
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct MarkovModelV1 {
    /// Schema version (must be MARKOV_MODEL_V1_VERSION).
    pub version: u32,
    /// Maximum order (n-gram length) used by the model.
    pub order_n_max: u8,
    /// Maximum next entries allowed per state in this model.
    pub max_next_per_state: u8,
    /// Total number of observed transitions used to build the model.
    pub total_transitions: u64,
    /// Hash of the training corpus (e.g., a ReplayLog set hash).
    pub corpus_hash: Hash32,
    /// State table.
    pub states: Vec<MarkovStateV1>,
}

impl MarkovModelV1 {
    /// Validate invariants.
    pub fn validate(&self) -> Result<(), MarkovModelError> {
        if self.version != MARKOV_MODEL_V1_VERSION {
            return Err(MarkovModelError::BadVersion);
        }
        if self.order_n_max == 0 || self.order_n_max > MARKOV_MODEL_V1_MAX_ORDER_N {
            return Err(MarkovModelError::BadOrder);
        }
        if self.states.len() > MARKOV_MODEL_V1_MAX_STATES {
            return Err(MarkovModelError::TooManyStates);
        }
        let max_next = self.max_next_per_state as usize;
        if max_next == 0 || max_next > MARKOV_MODEL_V1_MAX_NEXT_PER_STATE {
            return Err(MarkovModelError::TooManyNext);
        }
        for s in &self.states {
            if s.context.len() >= (self.order_n_max as usize) {
                return Err(MarkovModelError::BadContextLen);
            }
            if s.next.len() > max_next {
                return Err(MarkovModelError::TooManyNext);
            }
        }
        if !states_are_canon(&self.states) {
            return Err(MarkovModelError::NotCanonical);
        }
        Ok(())
    }

    /// Return true if the state table and next lists are in canonical order.
    pub fn is_canonical(&self) -> bool {
        states_are_canon(&self.states)
    }

    /// Encode to canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        self.validate().map_err(|e| {
            EncodeError::new(match e {
                MarkovModelError::BadVersion => "bad markov model version",
                MarkovModelError::BadOrder => "bad markov model order",
                MarkovModelError::TooManyStates => "too many markov states",
                MarkovModelError::TooManyNext => "too many markov next entries",
                MarkovModelError::BadContextLen => "bad markov context length",
                MarkovModelError::NotCanonical => "markov model not canonical",
            })
        })?;

        // Rough capacity estimate.
        let mut cap: usize = 4 + 1 + 1 + 1 + 1 + 4 + 8 + 32;
        for s in &self.states {
            cap += 1 + 1 + 2;
            cap += s.context.len() * (1 + 8);
            cap += 4;
            cap += s.next.len() * (1 + 8 + 4);
        }

        let mut w = ByteWriter::with_capacity(cap);
        w.write_u32(self.version);
        w.write_u8(self.order_n_max);
        w.write_u8(self.max_next_per_state);
        w.write_u8(0);
        w.write_u8(0);
        w.write_u32(self.states.len() as u32);
        w.write_u64(self.total_transitions);
        w.write_raw(&self.corpus_hash);

        for s in &self.states {
            w.write_u8(s.context.len() as u8);
            w.write_u8(s.next.len() as u8);
            w.write_u16(0);
            for t in &s.context {
                w.write_u8(t.kind as u8);
                w.write_u64(t.choice_id.0);
            }
            w.write_u32(s.escape_count);
            for n in &s.next {
                w.write_u8(n.token.kind as u8);
                w.write_u64(n.token.choice_id.0);
                w.write_u32(n.count);
            }
        }
        Ok(w.into_bytes())
    }

    /// Decode from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let version = r.read_u32()?;
        if version != MARKOV_MODEL_V1_VERSION {
            return Err(DecodeError::new("bad markov model version"));
        }

        let order_n_max = r.read_u8()?;
        if order_n_max == 0 || order_n_max > MARKOV_MODEL_V1_MAX_ORDER_N {
            return Err(DecodeError::new("bad markov model order"));
        }

        let max_next_per_state = r.read_u8()?;
        let max_next = max_next_per_state as usize;
        if max_next == 0 || max_next > MARKOV_MODEL_V1_MAX_NEXT_PER_STATE {
            return Err(DecodeError::new("too many markov next entries"));
        }

        let _reserved0 = r.read_u8()?;
        let _reserved1 = r.read_u8()?;

        let n_states = r.read_u32()? as usize;
        if n_states > MARKOV_MODEL_V1_MAX_STATES {
            return Err(DecodeError::new("too many markov states"));
        }

        let total_transitions = r.read_u64()?;

        let ch_b = r.read_fixed(32)?;
        let mut corpus_hash: Hash32 = [0u8; 32];
        corpus_hash.copy_from_slice(ch_b);

        let mut states: Vec<MarkovStateV1> = Vec::with_capacity(n_states);
        for _ in 0..n_states {
            let ctx_n = r.read_u8()? as usize;
            if ctx_n >= (order_n_max as usize) {
                return Err(DecodeError::new("bad markov context length"));
            }
            let next_n = r.read_u8()? as usize;
            let _reserved = r.read_u16()?;
            if next_n > max_next {
                return Err(DecodeError::new("too many markov next entries"));
            }
            let mut context: Vec<MarkovTokenV1> = Vec::with_capacity(ctx_n);
            for _ in 0..ctx_n {
                let kind = kind_from_u8(r.read_u8()?)?;
                let choice_id = Id64(r.read_u64()?);
                context.push(MarkovTokenV1 { kind, choice_id });
            }
            let escape_count = r.read_u32()?;
            let mut next: Vec<MarkovNextV1> = Vec::with_capacity(next_n);
            for _ in 0..next_n {
                let kind = kind_from_u8(r.read_u8()?)?;
                let choice_id = Id64(r.read_u64()?);
                let count = r.read_u32()?;
                next.push(MarkovNextV1 {
                    token: MarkovTokenV1 { kind, choice_id },
                    count,
                });
            }
            states.push(MarkovStateV1 {
                context,
                escape_count,
                next,
            });
        }

        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        if !states_are_canon(&states) {
            return Err(DecodeError::new("markov model not canonical"));
        }

        Ok(MarkovModelV1 {
            version,
            order_n_max,
            max_next_per_state,
            total_transitions,
            corpus_hash,
            states,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::blake3_hash;

    fn mk() -> MarkovModelV1 {
        MarkovModelV1 {
            version: MARKOV_MODEL_V1_VERSION,
            order_n_max: 3,
            max_next_per_state: 4,
            total_transitions: 7,
            corpus_hash: blake3_hash(b"corpus"),
            // NOTE: canonical state order requires higher-order contexts first.
            states: vec![
                MarkovStateV1 {
                    context: vec![
                        MarkovTokenV1::new(MarkovChoiceKindV1::Opener, Id64(1)),
                        MarkovTokenV1::new(MarkovChoiceKindV1::Transition, Id64(2)),
                    ],
                    escape_count: 1,
                    next: vec![
                        MarkovNextV1 {
                            token: MarkovTokenV1::new(MarkovChoiceKindV1::Closer, Id64(9)),
                            count: 10,
                        },
                        MarkovNextV1 {
                            token: MarkovTokenV1::new(MarkovChoiceKindV1::Other, Id64(8)),
                            count: 2,
                        },
                    ],
                },
                MarkovStateV1 {
                    context: vec![],
                    escape_count: 0,
                    next: vec![MarkovNextV1 {
                        token: MarkovTokenV1::new(MarkovChoiceKindV1::Opener, Id64(1)),
                        count: 3,
                    }],
                },
            ],
        }
    }

    #[test]
    fn markov_model_round_trip() {
        let m = mk();
        assert!(m.is_canonical());
        m.validate().unwrap();
        let bytes = m.encode().unwrap();
        let dec = MarkovModelV1::decode(&bytes).unwrap();
        assert_eq!(dec, m);
    }

    #[test]
    fn markov_model_decode_rejects_noncanonical_state_order() {
        let mut m = mk();
        // Force non-canonical state order by swapping the two states.
        m.states.swap(0, 1);
        assert!(!m.is_canonical());

        // Build noncanonical bytes manually to ensure decode rejects.
        let mut w = ByteWriter::with_capacity(512);
        w.write_u32(MARKOV_MODEL_V1_VERSION);
        w.write_u8(m.order_n_max);
        w.write_u8(m.max_next_per_state);
        w.write_u8(0);
        w.write_u8(0);
        w.write_u32(m.states.len() as u32);
        w.write_u64(m.total_transitions);
        w.write_raw(&m.corpus_hash);
        for s in &m.states {
            w.write_u8(s.context.len() as u8);
            w.write_u8(s.next.len() as u8);
            w.write_u16(0);
            for t in &s.context {
                w.write_u8(t.kind as u8);
                w.write_u64(t.choice_id.0);
            }
            w.write_u32(s.escape_count);
            for n in &s.next {
                w.write_u8(n.token.kind as u8);
                w.write_u64(n.token.choice_id.0);
                w.write_u32(n.count);
            }
        }
        let bytes = w.into_bytes();
        assert!(MarkovModelV1::decode(&bytes).is_err());
    }

    #[test]
    fn markov_model_decode_rejects_noncanonical_next_order() {
        let mut m = mk();
        // Make next list non-canonical by swapping counts order.
        m.states[0].next.swap(0, 1);
        assert!(!m.is_canonical());

        let mut w = ByteWriter::with_capacity(512);
        w.write_u32(MARKOV_MODEL_V1_VERSION);
        w.write_u8(m.order_n_max);
        w.write_u8(m.max_next_per_state);
        w.write_u8(0);
        w.write_u8(0);
        w.write_u32(m.states.len() as u32);
        w.write_u64(m.total_transitions);
        w.write_raw(&m.corpus_hash);
        for s in &m.states {
            w.write_u8(s.context.len() as u8);
            w.write_u8(s.next.len() as u8);
            w.write_u16(0);
            for t in &s.context {
                w.write_u8(t.kind as u8);
                w.write_u64(t.choice_id.0);
            }
            w.write_u32(s.escape_count);
            for n in &s.next {
                w.write_u8(n.token.kind as u8);
                w.write_u64(n.token.choice_id.0);
                w.write_u32(n.count);
            }
        }
        let bytes = w.into_bytes();
        assert!(MarkovModelV1::decode(&bytes).is_err());
    }
}
