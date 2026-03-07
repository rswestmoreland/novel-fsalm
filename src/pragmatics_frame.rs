// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Pragmatics control-signal schema.
//!
//! Pragmatics is a deterministic coprocessor that extracts compact signals
//! about tone, tact, emphasis, and intent from input text. These signals do
//! not change what evidence is retrieved. They shape how the planner and
//! realizer should respond.
//!
//! This module defines the v1 schema and its canonical byte codec.

use crate::frame::Id64;

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};

use std::error::Error;
use std::fmt;

/// Current schema version for [`PragmaticsFrameV1`].
pub const PRAGMATICS_FRAME_V1_VERSION: u16 = 1;

/// PragmaticsFrame artifact magic.
pub const PRAGMATICS_FRAME_MAGIC: [u8; 8] = *b"FSALMPRG";

/// PragmaticsFrame artifact version (v1).
pub const PRAGMATICS_FRAME_VERSION: u16 = 1;

/// Fixed encoded length for PragmaticsFrame v1.
pub const PRAGMATICS_FRAME_V1_ENCODED_LEN: usize = 84;

/// Intent flags bitset for [`PragmaticsFrameV1`].
///
/// This is a `u32` mask with stable bit assignments. Bits not defined in v1
/// must be zero.
pub type IntentFlagsV1 = u32;

/// The message contains at least one question cue (e.g., '?' or WH-word patterns).
pub const INTENT_FLAG_HAS_QUESTION: IntentFlagsV1 = 1u32 << 0;

/// The message contains a direct request cue (e.g., 'please', 'can you', imperative forms).
pub const INTENT_FLAG_HAS_REQUEST: IntentFlagsV1 = 1u32 << 1;

/// The message specifies constraints (e.g., must/should/avoid/requirements lists).
pub const INTENT_FLAG_HAS_CONSTRAINTS: IntentFlagsV1 = 1u32 << 2;

/// The message contains math-related cues (symbols, keywords, numeric-heavy forms).
pub const INTENT_FLAG_HAS_MATH: IntentFlagsV1 = 1u32 << 3;

/// The message contains code-related cues (fenced blocks, keywords, stack traces).
pub const INTENT_FLAG_HAS_CODE: IntentFlagsV1 = 1u32 << 4;

/// The message is a meta prompt about the system or model behavior.
pub const INTENT_FLAG_IS_META_PROMPT: IntentFlagsV1 = 1u32 << 5;

/// The message is likely a short follow-up referencing an earlier turn.
pub const INTENT_FLAG_IS_FOLLOW_UP: IntentFlagsV1 = 1u32 << 6;

/// The message contains conservative safety-sensitive cues (rules-only).
pub const INTENT_FLAG_SAFETY_SENSITIVE: IntentFlagsV1 = 1u32 << 7;

/// The message is requesting problem solving (troubleshooting, debugging, reverse engineering, retrospection).
pub const INTENT_FLAG_IS_PROBLEM_SOLVE: IntentFlagsV1 = 1u32 << 8;

/// The message is likely a logic puzzle / constraint satisfaction prompt.
pub const INTENT_FLAG_IS_LOGIC_PUZZLE: IntentFlagsV1 = 1u32 << 9;

/// Mask of all defined v1 intent flags.
pub const INTENT_FLAGS_V1_ALL: IntentFlagsV1 =
    INTENT_FLAG_HAS_QUESTION
        | INTENT_FLAG_HAS_REQUEST
        | INTENT_FLAG_HAS_CONSTRAINTS
        | INTENT_FLAG_HAS_MATH
        | INTENT_FLAG_HAS_CODE
        | INTENT_FLAG_IS_META_PROMPT
        | INTENT_FLAG_IS_FOLLOW_UP
        | INTENT_FLAG_SAFETY_SENSITIVE
        | INTENT_FLAG_IS_PROBLEM_SOLVE
        | INTENT_FLAG_IS_LOGIC_PUZZLE;

/// Errors produced by [`PragmaticsFrameV1::validate`].
#[derive(Debug)]
pub enum PragmaticsFrameV1ValidateError {
    /// Schema version mismatch.
    VersionMismatch {
        /// Observed version.
        got: u16,
        /// Expected version.
        expected: u16,
    },

    /// `ascii_only` must be 0 or 1.
    AsciiOnlyInvalid {
        /// Observed value.
        got: u8,
    },

    /// A `u16` score is out of its allowed range.
    RangeU16 {
        /// Field name.
        field: &'static str,
        /// Minimum inclusive.
        min: u16,
        /// Maximum inclusive.
        max: u16,
        /// Observed value.
        got: u16,
    },

    /// An `i16` score is out of its allowed range.
    RangeI16 {
        /// Field name.
        field: &'static str,
        /// Minimum inclusive.
        min: i16,
        /// Maximum inclusive.
        max: i16,
        /// Observed value.
        got: i16,
    },

    /// Unknown intent-flag bits were set.
    UnknownIntentFlags {
        /// Full flags value.
        flags: IntentFlagsV1,
        /// Mask of unknown bits.
        unknown: IntentFlagsV1,
    },

    /// A count is inconsistent with `byte_len`.
    CountExceedsByteLen {
        /// Field name.
        field: &'static str,
        /// Observed count.
        count: u16,
        /// Message byte length.
        byte_len: u32,
        /// Minimum bytes required per counted unit.
        min_bytes_per: u32,
    },
}

impl fmt::Display for PragmaticsFrameV1ValidateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PragmaticsFrameV1ValidateError::VersionMismatch { got, expected } => {
                write!(f, "version mismatch: got={got} expected={expected}")
            }
            PragmaticsFrameV1ValidateError::AsciiOnlyInvalid { got } => {
                write!(f, "ascii_only must be 0 or 1: got={got}")
            }
            PragmaticsFrameV1ValidateError::RangeU16 {
                field,
                min,
                max,
                got,
            } => {
                write!(
                    f,
                    "{field} out of range: got={got} expected={min}..={max}"
                )
            }
            PragmaticsFrameV1ValidateError::RangeI16 {
                field,
                min,
                max,
                got,
            } => {
                write!(
                    f,
                    "{field} out of range: got={got} expected={min}..={max}"
                )
            }
            PragmaticsFrameV1ValidateError::UnknownIntentFlags { flags, unknown } => {
                write!(
                    f,
                    "unknown intent flags bits: flags=0x{flags:08x} unknown=0x{unknown:08x}"
                )
            }
            PragmaticsFrameV1ValidateError::CountExceedsByteLen {
                field,
                count,
                byte_len,
                min_bytes_per,
            } => {
                write!(
                    f,
                    "{field} exceeds byte_len: count={count} byte_len={byte_len} min_bytes_per={min_bytes_per}"
                )
            }
        }
    }
}

impl Error for PragmaticsFrameV1ValidateError {}

/// Coarse rhetorical mode inferred from a message.
///
/// This is a small enum used as a planning hint.
#[repr(u16)]
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum RhetoricModeV1 {
    /// Unknown or not confidently inferred.
    Unknown = 0,
    /// The message is primarily a question.
    Ask = 1,
    /// The message is primarily a request or command.
    Command = 2,
    /// The message is primarily venting or complaining.
    Vent = 3,
    /// The message is primarily argumentative or debating.
    Debate = 4,
    /// The message is brainstorming or exploring options.
    Brainstorm = 5,
    /// The message is telling a story or narrative.
    Story = 6,
    /// The message is negotiating or bargaining.
    Negotiation = 7,
}

/// Pragmatics control signals for a single message.
///
/// All scores are integer-only and intended to be clamped to fixed ranges.
/// The v1 schema is compact and portable so it can be stored as an artifact
/// and referenced by later stages.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct PragmaticsFrameV1 {
    /// Schema version (must be [`PRAGMATICS_FRAME_V1_VERSION`]).
    pub version: u16,

    /// Deterministic identifier for the input source (e.g., request id).
    pub source_id: Id64,

    /// Message index within the source container (e.g., PromptPack).
    pub msg_ix: u32,

    /// Length of the message in bytes.
    pub byte_len: u32,

    /// 1 if the message bytes are all ASCII; 0 otherwise.
    pub ascii_only: u8,

    /// Temperature in [0..=1000]. Higher is more heated.
    pub temperature: u16,

    /// Valence in [-1000..=1000]. Negative to positive affect.
    pub valence: i16,

    /// Arousal in [0..=1000]. Higher is higher energy or urgency.
    pub arousal: u16,

    /// Politeness in [0..=1000]. Higher is more polite.
    pub politeness: u16,

    /// Formality in [0..=1000]. Higher is more formal.
    pub formality: u16,

    /// Directness in [0..=1000]. Higher is more direct.
    pub directness: u16,

    /// Empathy need in [0..=1000]. Higher suggests supportive framing.
    pub empathy_need: u16,

    /// Coarse rhetorical mode.
    pub mode: RhetoricModeV1,

    /// Intent flags bitset (v1).
    pub flags: IntentFlagsV1,

    /// Count of '!' characters.
    pub exclamations: u16,

    /// Count of '?' characters.
    pub questions: u16,

    /// Count of "..." occurrences.
    pub ellipses: u16,

    /// Count of all-caps ASCII words (length >= 2).
    pub caps_words: u16,

    /// Count of repeated punctuation runs like "!!!" or "??".
    pub repeat_punct_runs: u16,

    /// Count of ASCII quote characters ('"' and '\'').
    pub quotes: u16,

    /// Emphasis score in [0..=1000].
    pub emphasis_score: u16,

    /// Count of matched hedge cues.
    pub hedge_count: u16,

    /// Count of matched intensifier cues.
    pub intensifier_count: u16,

    /// Count of matched profanity cues.
    pub profanity_count: u16,

    /// Count of matched apology cues.
    pub apology_count: u16,

    /// Count of matched gratitude cues.
    pub gratitude_count: u16,

    /// Count of matched insult cues.
    pub insult_count: u16,
}

impl PragmaticsFrameV1 {
    /// Validate core v1 invariants.
    ///
    /// This is intended to be used by codecs, stores, and extractors to
    /// enforce deterministic schema bounds.
    pub fn validate(&self) -> Result<(), PragmaticsFrameV1ValidateError> {
        if self.version != PRAGMATICS_FRAME_V1_VERSION {
            return Err(PragmaticsFrameV1ValidateError::VersionMismatch {
                got: self.version,
                expected: PRAGMATICS_FRAME_V1_VERSION,
            });
        }

        if self.ascii_only > 1 {
            return Err(PragmaticsFrameV1ValidateError::AsciiOnlyInvalid {
                got: self.ascii_only,
            });
        }

        check_u16_range("temperature", self.temperature, 0, 1000)?;
        check_i16_range("valence", self.valence, -1000, 1000)?;
        check_u16_range("arousal", self.arousal, 0, 1000)?;
        check_u16_range("politeness", self.politeness, 0, 1000)?;
        check_u16_range("formality", self.formality, 0, 1000)?;
        check_u16_range("directness", self.directness, 0, 1000)?;
        check_u16_range("empathy_need", self.empathy_need, 0, 1000)?;
        check_u16_range("emphasis_score", self.emphasis_score, 0, 1000)?;

        let unknown = self.flags & !INTENT_FLAGS_V1_ALL;
        if unknown != 0 {
            return Err(PragmaticsFrameV1ValidateError::UnknownIntentFlags {
                flags: self.flags,
                unknown,
            });
        }

        let bl = self.byte_len;
        check_count_vs_byte_len("exclamations", self.exclamations, bl, 1)?;
        check_count_vs_byte_len("questions", self.questions, bl, 1)?;
        check_count_vs_byte_len("quotes", self.quotes, bl, 1)?;

        // "..." occurrences are at least 3 ASCII bytes each.
        check_count_vs_byte_len("ellipses", self.ellipses, bl, 3)?;

        // These counters represent at least 2 ASCII bytes per unit.
        check_count_vs_byte_len("caps_words", self.caps_words, bl, 2)?;
        check_count_vs_byte_len("repeat_punct_runs", self.repeat_punct_runs, bl, 2)?;

        // Cue-match counters are conservative; each match implies at least 1 byte.
        check_count_vs_byte_len("hedge_count", self.hedge_count, bl, 1)?;
        check_count_vs_byte_len("intensifier_count", self.intensifier_count, bl, 1)?;
        check_count_vs_byte_len("profanity_count", self.profanity_count, bl, 1)?;
        check_count_vs_byte_len("apology_count", self.apology_count, bl, 1)?;
        check_count_vs_byte_len("gratitude_count", self.gratitude_count, bl, 1)?;
        check_count_vs_byte_len("insult_count", self.insult_count, bl, 1)?;

        Ok(())
    }

    /// Encode the frame to canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        if self.validate().is_err() {
            return Err(EncodeError::new("invalid pragmatics frame"));
        }

        let mut w = ByteWriter::with_capacity(PRAGMATICS_FRAME_V1_ENCODED_LEN);
        w.write_raw(&PRAGMATICS_FRAME_MAGIC);
        w.write_u16(PRAGMATICS_FRAME_VERSION);
        w.write_u16(0);

        // Schema version (v1) plus reserved.
        w.write_u16(self.version);
        w.write_u16(0);

        w.write_u64(self.source_id.0);
        w.write_u32(self.msg_ix);
        w.write_u32(self.byte_len);

        w.write_u8(self.ascii_only);
        w.write_u8(0);
        w.write_u16(0);

        w.write_u16(self.temperature);
        w.write_raw(&self.valence.to_le_bytes());
        w.write_u16(self.arousal);
        w.write_u16(self.politeness);
        w.write_u16(self.formality);
        w.write_u16(self.directness);
        w.write_u16(self.empathy_need);

        w.write_u16(self.mode as u16);
        w.write_u32(self.flags);

        w.write_u16(self.exclamations);
        w.write_u16(self.questions);
        w.write_u16(self.ellipses);
        w.write_u16(self.caps_words);
        w.write_u16(self.repeat_punct_runs);
        w.write_u16(self.quotes);
        w.write_u16(self.emphasis_score);
        w.write_u16(self.hedge_count);
        w.write_u16(self.intensifier_count);
        w.write_u16(self.profanity_count);
        w.write_u16(self.apology_count);
        w.write_u16(self.gratitude_count);
        w.write_u16(self.insult_count);

        // Reserved for future v1 extensions.
        w.write_u16(0);

        let bytes = w.into_bytes();
        if bytes.len() != PRAGMATICS_FRAME_V1_ENCODED_LEN {
            return Err(EncodeError::new("pragmatics frame size mismatch"));
        }
        Ok(bytes)
    }

    /// Decode a frame from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<PragmaticsFrameV1, DecodeError> {
        let mut r = ByteReader::new(bytes);

        let magic = r.read_fixed(8)?;
        if magic != PRAGMATICS_FRAME_MAGIC {
            return Err(DecodeError::new("bad pragmatics frame magic"));
        }

        let ver = r.read_u16()?;
        if ver != PRAGMATICS_FRAME_VERSION {
            return Err(DecodeError::new("unsupported pragmatics frame version"));
        }

        let _reserved = r.read_u16()?;

        let schema_ver = r.read_u16()?;
        if schema_ver != PRAGMATICS_FRAME_V1_VERSION {
            return Err(DecodeError::new("unsupported pragmatics schema version"));
        }

        let _reserved2 = r.read_u16()?;

        let source_id = Id64(r.read_u64()?);
        let msg_ix = r.read_u32()?;
        let byte_len = r.read_u32()?;

        let ascii_only = r.read_u8()?;
        let _reserved3 = r.read_u8()?;
        let _reserved4 = r.read_u16()?;

        let temperature = r.read_u16()?;
        let vb = r.read_fixed(2)?;
        let valence = i16::from_le_bytes([vb[0], vb[1]]);
        let arousal = r.read_u16()?;
        let politeness = r.read_u16()?;
        let formality = r.read_u16()?;
        let directness = r.read_u16()?;
        let empathy_need = r.read_u16()?;

        let mode_u16 = r.read_u16()?;
        let mode = rhetoric_mode_from_u16(mode_u16);
        let flags = r.read_u32()?;

        let exclamations = r.read_u16()?;
        let questions = r.read_u16()?;
        let ellipses = r.read_u16()?;
        let caps_words = r.read_u16()?;
        let repeat_punct_runs = r.read_u16()?;
        let quotes = r.read_u16()?;
        let emphasis_score = r.read_u16()?;
        let hedge_count = r.read_u16()?;
        let intensifier_count = r.read_u16()?;
        let profanity_count = r.read_u16()?;
        let apology_count = r.read_u16()?;
        let gratitude_count = r.read_u16()?;
        let insult_count = r.read_u16()?;

        let _reserved_tail = r.read_u16()?;

        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        let out = PragmaticsFrameV1 {
            version: schema_ver,
            source_id,
            msg_ix,
            byte_len,
            ascii_only,
            temperature,
            valence,
            arousal,
            politeness,
            formality,
            directness,
            empathy_need,
            mode,
            flags,
            exclamations,
            questions,
            ellipses,
            caps_words,
            repeat_punct_runs,
            quotes,
            emphasis_score,
            hedge_count,
            intensifier_count,
            profanity_count,
            apology_count,
            gratitude_count,
            insult_count,
        };

        if out.validate().is_err() {
            return Err(DecodeError::new("invalid pragmatics frame"));
        }
        Ok(out)
    }
}

fn rhetoric_mode_from_u16(v: u16) -> RhetoricModeV1 {
    match v {
        1 => RhetoricModeV1::Ask,
        2 => RhetoricModeV1::Command,
        3 => RhetoricModeV1::Vent,
        4 => RhetoricModeV1::Debate,
        5 => RhetoricModeV1::Brainstorm,
        6 => RhetoricModeV1::Story,
        7 => RhetoricModeV1::Negotiation,
        _ => RhetoricModeV1::Unknown,
    }
}

fn check_u16_range(
    field: &'static str,
    got: u16,
    min: u16,
    max: u16,
) -> Result<(), PragmaticsFrameV1ValidateError> {
    if got < min || got > max {
        return Err(PragmaticsFrameV1ValidateError::RangeU16 {
            field,
            min,
            max,
            got,
        });
    }
    Ok(())
}

fn check_i16_range(
    field: &'static str,
    got: i16,
    min: i16,
    max: i16,
) -> Result<(), PragmaticsFrameV1ValidateError> {
    if got < min || got > max {
        return Err(PragmaticsFrameV1ValidateError::RangeI16 {
            field,
            min,
            max,
            got,
        });
    }
    Ok(())
}

fn check_count_vs_byte_len(
    field: &'static str,
    count: u16,
    byte_len: u32,
    min_bytes_per: u32,
) -> Result<(), PragmaticsFrameV1ValidateError> {
    let need = u32::from(count).saturating_mul(min_bytes_per);
    if need > byte_len {
        return Err(PragmaticsFrameV1ValidateError::CountExceedsByteLen {
            field,
            count,
            byte_len,
            min_bytes_per,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_ok() -> PragmaticsFrameV1 {
        PragmaticsFrameV1 {
            version: PRAGMATICS_FRAME_V1_VERSION,
            source_id: Id64(1),
            msg_ix: 0,
            byte_len: 16,
            ascii_only: 1,
            temperature: 0,
            valence: 0,
            arousal: 0,
            politeness: 0,
            formality: 0,
            directness: 0,
            empathy_need: 0,
            mode: RhetoricModeV1::Unknown,
            flags: 0,
            exclamations: 0,
            questions: 0,
            ellipses: 0,
            caps_words: 0,
            repeat_punct_runs: 0,
            quotes: 0,
            emphasis_score: 0,
            hedge_count: 0,
            intensifier_count: 0,
            profanity_count: 0,
            apology_count: 0,
            gratitude_count: 0,
            insult_count: 0,
        }
    }

    #[test]
    fn pragmatics_frame_v1_validate_ok() {
        let f = sample_ok();
        assert!(f.validate().is_ok());
    }

    #[test]
    fn pragmatics_frame_v1_validate_version_mismatch() {
        let mut f = sample_ok();
        f.version = 0;
        let err = f.validate().unwrap_err();
        match err {
            PragmaticsFrameV1ValidateError::VersionMismatch { got, expected } => {
                assert_eq!(got, 0);
                assert_eq!(expected, PRAGMATICS_FRAME_V1_VERSION);
            }
            other => panic!("unexpected error: {other:?}"),
        }
        assert_eq!(
            err.to_string(),
            "version mismatch: got=0 expected=1"
        );
    }

    #[test]
    fn pragmatics_frame_v1_validate_temperature_range() {
        let mut f = sample_ok();
        f.temperature = 1001;
        let err = f.validate().unwrap_err();
        match err {
            PragmaticsFrameV1ValidateError::RangeU16 { field, got, .. } => {
                assert_eq!(field, "temperature");
                assert_eq!(got, 1001);
            }
            other => panic!("unexpected error: {other:?}"),
        }
        assert_eq!(
            err.to_string(),
            "temperature out of range: got=1001 expected=0..=1000"
        );
    }

    #[test]
    fn pragmatics_frame_v1_validate_unknown_intent_flags() {
        let mut f = sample_ok();
        f.flags = INTENT_FLAG_HAS_QUESTION | (1u32 << 31);
        let err = f.validate().unwrap_err();
        match err {
            PragmaticsFrameV1ValidateError::UnknownIntentFlags { flags, unknown } => {
                assert_eq!(flags, 0x8000_0001);
                assert_eq!(unknown, 0x8000_0000);
            }
            other => panic!("unexpected error: {other:?}"),
        }
        assert_eq!(
            err.to_string(),
            "unknown intent flags bits: flags=0x80000001 unknown=0x80000000"
        );
    }

    #[test]
    fn pragmatics_frame_v1_validate_count_exceeds_byte_len() {
        let mut f = sample_ok();
        f.byte_len = 2;
        f.ellipses = 1;
        let err = f.validate().unwrap_err();
        match err {
            PragmaticsFrameV1ValidateError::CountExceedsByteLen {
                field,
                count,
                byte_len,
                min_bytes_per,
            } => {
                assert_eq!(field, "ellipses");
                assert_eq!(count, 1);
                assert_eq!(byte_len, 2);
                assert_eq!(min_bytes_per, 3);
            }
            other => panic!("unexpected error: {other:?}"),
        }
        assert_eq!(
            err.to_string(),
            "ellipses exceeds byte_len: count=1 byte_len=2 min_bytes_per=3"
        );
    }

    #[test]
    fn pragmatics_frame_v1_codec_round_trip() {
        let mut f = sample_ok();
        f.byte_len = 64;
        f.temperature = 123;
        f.valence = -10;
        f.arousal = 55;
        f.politeness = 900;
        f.directness = 800;
        f.mode = RhetoricModeV1::Ask;
        f.flags = INTENT_FLAG_HAS_QUESTION | INTENT_FLAG_HAS_CONSTRAINTS;
        f.exclamations = 1;
        f.questions = 2;
        f.ellipses = 1;
        f.caps_words = 1;
        f.repeat_punct_runs = 1;
        f.quotes = 2;
        f.emphasis_score = 250;

        let bytes = f.encode().unwrap();
        assert_eq!(bytes.len(), PRAGMATICS_FRAME_V1_ENCODED_LEN);

        let got = PragmaticsFrameV1::decode(&bytes).unwrap();
        assert_eq!(got, f);
    }

    #[test]
    fn pragmatics_frame_v1_decode_unsupported_version() {
        let f = sample_ok();
        let mut bytes = f.encode().unwrap();
        // Version is immediately after the 8-byte magic.
        bytes[8] = 2;
        bytes[9] = 0;
        let err = PragmaticsFrameV1::decode(&bytes).unwrap_err();
        assert_eq!(err.to_string(), "unsupported pragmatics frame version");
    }

    #[test]
    fn pragmatics_frame_v1_decode_truncated() {
        let f = sample_ok();
        let bytes = f.encode().unwrap();
        let err = PragmaticsFrameV1::decode(&bytes[..10]).unwrap_err();
        assert_eq!(err.to_string(), "unexpected EOF");
    }

    #[test]
    fn pragmatics_frame_v1_decode_trailing_bytes() {
        let f = sample_ok();
        let mut bytes = f.encode().unwrap();
        bytes.push(0);
        let err = PragmaticsFrameV1::decode(&bytes).unwrap_err();
        assert_eq!(err.to_string(), "trailing bytes");
    }
}
