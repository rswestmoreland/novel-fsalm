// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Markov choice trace schema.
//!
//! MarkovTraceV1 is a replayable, deterministic per-turn record of the
//! surface-form choice stream used by the realizer.
//!
//! This trace is the primary input to offline Markov training.
//! It intentionally stores only (kind, choice_id) tokens, not free-form text.

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::hash::Hash32;
use crate::markov_model::MarkovTokenV1;

/// MarkovTraceV1 schema version.
pub const MARKOV_TRACE_V1_VERSION: u32 = 1;

/// Hard cap on tokens in a single trace.
///
/// A per-turn trace should be small (openers/transitions/closers), but the cap
/// is defensive for replay artifacts.
pub const MARKOV_TRACE_V1_MAX_TOKENS: usize = 2048;

/// MarkovTraceV1 validation errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarkovTraceError {
    /// Unsupported or mismatched schema version.
    BadVersion,
    /// Trace contains too many tokens.
    TooManyTokens,
    /// Trace contains an invalid token kind encoding.
    BadKind,
}

impl core::fmt::Display for MarkovTraceError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            MarkovTraceError::BadVersion => f.write_str("bad markov trace version"),
            MarkovTraceError::TooManyTokens => f.write_str("too many markov trace tokens"),
            MarkovTraceError::BadKind => f.write_str("bad markov trace token kind"),
        }
    }
}

/// One per-turn Markov choice trace (v1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarkovTraceV1 {
    /// Schema version (must be MARKOV_TRACE_V1_VERSION).
    pub version: u32,
    /// Query id (hash of the user query bytes).
    pub query_id: Hash32,
    /// Observed token stream in order.
    pub tokens: Vec<MarkovTokenV1>,
}

impl MarkovTraceV1 {
    /// Validate invariants.
    pub fn validate(&self) -> Result<(), MarkovTraceError> {
        if self.version != MARKOV_TRACE_V1_VERSION {
            return Err(MarkovTraceError::BadVersion);
        }
        if self.tokens.len() > MARKOV_TRACE_V1_MAX_TOKENS {
            return Err(MarkovTraceError::TooManyTokens);
        }
        // MarkovTokenV1.kind is an enum with explicit encodings; reject any
        // values outside the v1 set.
        for t in &self.tokens {
            let k = t.kind as u8;
            if k < 1 || k > 4 {
                return Err(MarkovTraceError::BadKind);
            }
        }
        Ok(())
    }

    /// Encode to canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        self.validate().map_err(|e| {
            EncodeError::new(match e {
                MarkovTraceError::BadVersion => "bad markov trace version",
                MarkovTraceError::TooManyTokens => "too many markov trace tokens",
                MarkovTraceError::BadKind => "bad markov trace token kind",
            })
        })?;

        let cap = 4 + 32 + 2 + (self.tokens.len() * (1 + 8));
        let mut w = ByteWriter::with_capacity(cap);
        w.write_u32(self.version);
        w.write_raw(&self.query_id);
        w.write_u16(self.tokens.len() as u16);
        for t in &self.tokens {
            w.write_u8(t.kind as u8);
            w.write_u64(t.choice_id.0);
        }
        Ok(w.into_bytes())
    }

    /// Decode from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let version = r.read_u32()?;
        if version != MARKOV_TRACE_V1_VERSION {
            return Err(DecodeError::new("bad markov trace version"));
        }
        let qid_b = r.read_fixed(32)?;
        let mut query_id = [0u8; 32];
        query_id.copy_from_slice(qid_b);

        let n = r.read_u16()? as usize;
        if n > MARKOV_TRACE_V1_MAX_TOKENS {
            return Err(DecodeError::new("too many markov trace tokens"));
        }

        let mut tokens: Vec<MarkovTokenV1> = Vec::with_capacity(n);
        for _ in 0..n {
            let kind_u8 = r.read_u8()?;
            let kind = match kind_u8 {
                1 => crate::markov_hints::MarkovChoiceKindV1::Opener,
                2 => crate::markov_hints::MarkovChoiceKindV1::Transition,
                3 => crate::markov_hints::MarkovChoiceKindV1::Closer,
                4 => crate::markov_hints::MarkovChoiceKindV1::Other,
                _ => return Err(DecodeError::new("bad markov trace token kind")),
            };
            let choice_id = crate::frame::Id64(r.read_u64()?);
            tokens.push(MarkovTokenV1::new(kind, choice_id));
        }
        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        Ok(MarkovTraceV1 {
            version,
            query_id,
            tokens,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::blake3_hash;
    use crate::markov_hints::MarkovChoiceKindV1;
    use crate::markov_model::MarkovTokenV1;

    #[test]
    fn trace_roundtrip() {
        let t = MarkovTraceV1 {
            version: MARKOV_TRACE_V1_VERSION,
            query_id: blake3_hash(b"q"),
            tokens: vec![
                MarkovTokenV1::new(MarkovChoiceKindV1::Opener, crate::frame::Id64(1)),
                MarkovTokenV1::new(MarkovChoiceKindV1::Transition, crate::frame::Id64(2)),
                MarkovTokenV1::new(MarkovChoiceKindV1::Closer, crate::frame::Id64(3)),
            ],
        };
        let b = t.encode().expect("encode");
        let d = MarkovTraceV1::decode(&b).expect("decode");
        assert_eq!(t, d);
        assert!(d.validate().is_ok());
    }

    #[test]
    fn trace_decode_rejects_trailing() {
        let t = MarkovTraceV1 {
            version: MARKOV_TRACE_V1_VERSION,
            query_id: blake3_hash(b"q"),
            tokens: Vec::new(),
        };
        let mut b = t.encode().unwrap();
        b.push(0);
        assert!(MarkovTraceV1::decode(&b).is_err());
    }
}
