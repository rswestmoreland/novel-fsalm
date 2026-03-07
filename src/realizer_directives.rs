// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Realizer directives schema.
//!
//! Novel's answering loop is evidence-first:
//! PromptPack -> retrieval -> EvidenceBundle -> AnswerPlan -> realized text.
//!
//! PragmaticsFrameV1 captures tone/tact/emphasis signals from input text.
//! RealizerDirectivesV1 is the downstream, realization-focused control plane:
//! it constrains how the Realizer renders output without changing evidence.
//!
//! is contract-only:
//! - defines enums, flags, and limits
//! - defines canonical byte codec + validation
//! - integration is performed in quality_gate_v1

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};

use crate::pragmatics_frame::{
    PragmaticsFrameV1,
    RhetoricModeV1,
    INTENT_FLAG_HAS_CODE,
    INTENT_FLAG_HAS_CONSTRAINTS,
    INTENT_FLAG_HAS_MATH,
    INTENT_FLAG_HAS_QUESTION,
    INTENT_FLAG_HAS_REQUEST,
    INTENT_FLAG_IS_LOGIC_PUZZLE,
    INTENT_FLAG_IS_PROBLEM_SOLVE,
    INTENT_FLAG_SAFETY_SENSITIVE,
};

/// RealizerDirectivesV1 schema version.
pub const REALIZER_DIRECTIVES_V1_VERSION: u32 = 1;

/// Maximum number of rationale codes allowed in v1.
pub const REALIZER_DIRECTIVES_V1_MAX_RATIONALE_CODES: usize = 64;

/// Output tone selection (v1).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
#[repr(u8)]
pub enum ToneV1 {
    /// Neutral default tone.
    Neutral = 0,
    /// Supportive and encouraging tone.
    Supportive = 1,
    /// Direct and action-oriented tone.
    Direct = 2,
    /// Cautious tone emphasizing uncertainty and risk.
    Cautious = 3,
}

impl ToneV1 {
    fn from_u8(v: u8) -> Result<Self, DecodeError> {
        match v {
            0 => Ok(ToneV1::Neutral),
            1 => Ok(ToneV1::Supportive),
            2 => Ok(ToneV1::Direct),
            3 => Ok(ToneV1::Cautious),
            _ => Err(DecodeError::new("bad ToneV1")),
        }
    }
}

/// Output style selection (v1).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
#[repr(u8)]
pub enum StyleV1 {
    /// Default style (balanced; uses Realizer defaults).
    Default = 0,
    /// Concise style (shorter sentences; fewer asides).
    Concise = 1,
    /// Step-by-step explanatory style.
    StepByStep = 2,
    /// Checklist style (compact bullets or enumerated steps).
    Checklist = 3,
    /// Debug style (more explicit constraints and mechanics).
    Debug = 4,
}

impl StyleV1 {
    fn from_u8(v: u8) -> Result<Self, DecodeError> {
        match v {
            0 => Ok(StyleV1::Default),
            1 => Ok(StyleV1::Concise),
            2 => Ok(StyleV1::StepByStep),
            3 => Ok(StyleV1::Checklist),
            4 => Ok(StyleV1::Debug),
            _ => Err(DecodeError::new("bad StyleV1")),
        }
    }
}

/// Formatting flags bitset for [`RealizerDirectivesV1`].
pub type FormatFlagsV1 = u32;

/// Prefer bullet formatting for lists.
pub const FORMAT_FLAG_BULLETS: FormatFlagsV1 = 1u32 << 0;
/// Prefer numbered formatting for procedures.
pub const FORMAT_FLAG_NUMBERED: FormatFlagsV1 = 1u32 << 1;
/// Include a short summary section (when content supports it).
pub const FORMAT_FLAG_INCLUDE_SUMMARY: FormatFlagsV1 = 1u32 << 2;
/// Include a short next-steps section (when content supports it).
pub const FORMAT_FLAG_INCLUDE_NEXT_STEPS: FormatFlagsV1 = 1u32 << 3;
/// Include a short risks/caveats section (when content supports it).
pub const FORMAT_FLAG_INCLUDE_RISKS: FormatFlagsV1 = 1u32 << 4;
/// Include an assumptions section (when content supports it).
pub const FORMAT_FLAG_INCLUDE_ASSUMPTIONS: FormatFlagsV1 = 1u32 << 5;

/// Mask of all defined v1 formatting flags.
pub const FORMAT_FLAGS_V1_ALL: FormatFlagsV1 = FORMAT_FLAG_BULLETS
    | FORMAT_FLAG_NUMBERED
    | FORMAT_FLAG_INCLUDE_SUMMARY
    | FORMAT_FLAG_INCLUDE_NEXT_STEPS
    | FORMAT_FLAG_INCLUDE_RISKS
    | FORMAT_FLAG_INCLUDE_ASSUMPTIONS;

/// Realizer directives derived from control signals (v1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RealizerDirectivesV1 {
    /// Schema version (must be [`REALIZER_DIRECTIVES_V1_VERSION`]).
    pub version: u32,
    /// Output tone selection.
    pub tone: ToneV1,
    /// Output style selection.
    pub style: StyleV1,
    /// Formatting flags bitset.
    pub format_flags: FormatFlagsV1,
    /// Maximum number of "softener" phrases (e.g., "maybe", "it might help") to emit.
    pub max_softeners: u8,
    /// Maximum number of preface sentences (before the main answer).
    pub max_preface_sentences: u8,
    /// Maximum number of hedge phrases (e.g., "likely", "roughly") to emit.
    pub max_hedges: u8,
    /// Maximum number of explicit questions to ask in the output.
    pub max_questions: u8,
    /// Rationale codes explaining why the directives were selected.
    ///
    /// Canonical form: strictly increasing (sorted) with no duplicates.
    pub rationale_codes: Vec<u16>,
}

/// Rationale code assignments for derivation from PragmaticsFrameV1.
///
/// These codes are stable within the repository once introduced. Values are
/// intentionally sparse to allow future additions without renumbering.
/// Pragmatics indicates safety sensitive content; prefer cautious phrasing.
pub const RD_RATIONALE_SAFETY_SENSITIVE: u16 = 1;
/// Pragmatics indicates high empathy need; prefer supportive framing.
pub const RD_RATIONALE_EMPATHY_HIGH: u16 = 2;
/// Pragmatics indicates venting; prefer supportive, low-friction phrasing.
pub const RD_RATIONALE_VENT_MODE: u16 = 3;
/// Pragmatics indicates high directness; prefer direct tone and structure.
pub const RD_RATIONALE_DIRECTNESS_HIGH: u16 = 4;
/// Input contains code or logs; prefer debug-oriented formatting.
pub const RD_RATIONALE_HAS_CODE: u16 = 10;
/// Input contains math; prefer careful, step-wise presentation.
pub const RD_RATIONALE_HAS_MATH: u16 = 11;
/// Input includes explicit constraints; prefer checklist/next-steps structure.
pub const RD_RATIONALE_HAS_CONSTRAINTS: u16 = 12;
/// Input includes an explicit request/imperative; prefer action-oriented framing.
pub const RD_RATIONALE_HAS_REQUEST: u16 = 13;
/// Input includes a question; prefer clear answer-first structure.
pub const RD_RATIONALE_HAS_QUESTION: u16 = 14;
/// Input is long; prefer more structure and less preface.
pub const RD_RATIONALE_LONG_INPUT: u16 = 15;
/// Pragmatics indicates low politeness; avoid extra softeners.
pub const RD_RATIONALE_LOW_POLITENESS: u16 = 16;
/// Pragmatics indicates high arousal/urgency; prefer concise structure.
pub const RD_RATIONALE_HIGH_AROUSAL: u16 = 17;

/// Validation errors for [`RealizerDirectivesV1`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RealizerDirectivesError {
    /// Version is not supported.
    BadVersion,
    /// Formatting flags contain unknown bits.
    UnknownFormatFlags,
    /// Too many rationale codes.
    TooManyRationaleCodes,
    /// Rationale codes are not canonical (not sorted strictly increasing or duplicates).
    RationaleNotCanonical,
}

impl core::fmt::Display for RealizerDirectivesError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            RealizerDirectivesError::BadVersion => f.write_str("bad realizer directives version"),
            RealizerDirectivesError::UnknownFormatFlags => f.write_str("unknown realizer directives format flags"),
            RealizerDirectivesError::TooManyRationaleCodes => {
                f.write_str("too many realizer directives rationale codes")
            }
            RealizerDirectivesError::RationaleNotCanonical => f.write_str("rationale codes not canonical"),
        }
    }
}

impl std::error::Error for RealizerDirectivesError {}

fn rationale_is_canon(xs: &[u16]) -> bool {
    for i in 1..xs.len() {
        if xs[i - 1] >= xs[i] {
            return false;
        }
    }
    true
}

impl RealizerDirectivesV1 {
    /// Validate invariants.
    pub fn validate(&self) -> Result<(), RealizerDirectivesError> {
        if self.version != REALIZER_DIRECTIVES_V1_VERSION {
            return Err(RealizerDirectivesError::BadVersion);
        }
        let unknown = self.format_flags & !FORMAT_FLAGS_V1_ALL;
        if unknown != 0 {
            return Err(RealizerDirectivesError::UnknownFormatFlags);
        }
        if self.rationale_codes.len() > REALIZER_DIRECTIVES_V1_MAX_RATIONALE_CODES {
            return Err(RealizerDirectivesError::TooManyRationaleCodes);
        }
        if !rationale_is_canon(&self.rationale_codes) {
            return Err(RealizerDirectivesError::RationaleNotCanonical);
        }
        Ok(())
    }

    /// Encode to canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        self.validate().map_err(|e| {
            EncodeError::new(match e {
                RealizerDirectivesError::BadVersion => "bad realizer directives version",
                RealizerDirectivesError::UnknownFormatFlags => "unknown format flags",
                RealizerDirectivesError::TooManyRationaleCodes => "too many rationale codes",
                RealizerDirectivesError::RationaleNotCanonical => "non-canonical rationale codes",
            })
        })?;

        let cap = 32 + self.rationale_codes.len() * 2;
        let mut w = ByteWriter::with_capacity(cap);
        w.write_u32(self.version);
        w.write_u8(self.tone as u8);
        w.write_u8(self.style as u8);
        w.write_u16(0); // reserved for v2
        w.write_u32(self.format_flags);
        w.write_u8(self.max_softeners);
        w.write_u8(self.max_preface_sentences);
        w.write_u8(self.max_hedges);
        w.write_u8(self.max_questions);
        w.write_u16(self.rationale_codes.len() as u16);
        for &c in &self.rationale_codes {
            w.write_u16(c);
        }
        Ok(w.into_bytes())
    }

    /// Decode from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let version = r.read_u32()?;
        if version != REALIZER_DIRECTIVES_V1_VERSION {
            return Err(DecodeError::new("bad realizer directives version"));
        }
        let tone = ToneV1::from_u8(r.read_u8()?)?;
        let style = StyleV1::from_u8(r.read_u8()?)?;
        let _reserved = r.read_u16()?;
        let format_flags = r.read_u32()?;
        let unknown = format_flags & !FORMAT_FLAGS_V1_ALL;
        if unknown != 0 {
            return Err(DecodeError::new("unknown format flags"));
        }
        let max_softeners = r.read_u8()?;
        let max_preface_sentences = r.read_u8()?;
        let max_hedges = r.read_u8()?;
        let max_questions = r.read_u8()?;

        let n = r.read_u16()? as usize;
        if n > REALIZER_DIRECTIVES_V1_MAX_RATIONALE_CODES {
            return Err(DecodeError::new("too many rationale codes"));
        }
        let mut rationale_codes: Vec<u16> = Vec::with_capacity(n);
        for _ in 0..n {
            rationale_codes.push(r.read_u16()?);
        }
        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }
        if !rationale_is_canon(&rationale_codes) {
            return Err(DecodeError::new("rationale codes not canonical"));
        }

        Ok(RealizerDirectivesV1 {
            version,
            tone,
            style,
            format_flags,
            max_softeners,
            max_preface_sentences,
            max_hedges,
            max_questions,
            rationale_codes,
        })
    }
}

fn push_rationale(out: &mut Vec<u16>, code: u16) {
    out.push(code);
}

fn finalize_rationale(mut xs: Vec<u16>) -> Vec<u16> {
    xs.sort_unstable();
    xs.dedup();
    xs
}

/// Deterministically derive RealizerDirectivesV1 from PragmaticsFrameV1.
///
/// This is a rules-first mapping intended to be stable and replay-friendly.
/// It must not change evidence selection.
pub fn derive_realizer_directives_v1(p: &PragmaticsFrameV1) -> RealizerDirectivesV1 {
    // Derivation assumes `p` is validated upstream, but stays total even if not.
    // Clamp-like behavior is implemented by conservative thresholds.

    let has_code = (p.flags & INTENT_FLAG_HAS_CODE) != 0;
    let has_math = (p.flags & INTENT_FLAG_HAS_MATH) != 0;
    let has_constraints = (p.flags & INTENT_FLAG_HAS_CONSTRAINTS) != 0;
    let has_request = (p.flags & INTENT_FLAG_HAS_REQUEST) != 0;
    let has_question = (p.flags & INTENT_FLAG_HAS_QUESTION) != 0 || p.questions != 0;
    let is_problem_solve = (p.flags & INTENT_FLAG_IS_PROBLEM_SOLVE) != 0;
    let is_logic_puzzle = (p.flags & INTENT_FLAG_IS_LOGIC_PUZZLE) != 0;
    let safety_sensitive = (p.flags & INTENT_FLAG_SAFETY_SENSITIVE) != 0;

    let mut rationale: Vec<u16> = Vec::new();

    if safety_sensitive {
        push_rationale(&mut rationale, RD_RATIONALE_SAFETY_SENSITIVE);
    }
    if has_code {
        push_rationale(&mut rationale, RD_RATIONALE_HAS_CODE);
    }
    if has_math {
        push_rationale(&mut rationale, RD_RATIONALE_HAS_MATH);
    }
    if has_constraints {
        push_rationale(&mut rationale, RD_RATIONALE_HAS_CONSTRAINTS);
    }
    if has_request {
        push_rationale(&mut rationale, RD_RATIONALE_HAS_REQUEST);
    }
    if has_question {
        push_rationale(&mut rationale, RD_RATIONALE_HAS_QUESTION);
    }
    if p.byte_len >= 300 {
        push_rationale(&mut rationale, RD_RATIONALE_LONG_INPUT);
    }
    if p.politeness <= 350 {
        push_rationale(&mut rationale, RD_RATIONALE_LOW_POLITENESS);
    }
    if p.arousal >= 650 {
        push_rationale(&mut rationale, RD_RATIONALE_HIGH_AROUSAL);
    }

    // Tone selection (priority order).
    let tone = if safety_sensitive {
        ToneV1::Cautious
    } else if p.empathy_need >= 650 {
        push_rationale(&mut rationale, RD_RATIONALE_EMPATHY_HIGH);
        ToneV1::Supportive
    } else if p.mode == RhetoricModeV1::Vent {
        push_rationale(&mut rationale, RD_RATIONALE_VENT_MODE);
        ToneV1::Supportive
    } else if p.directness >= 700 && p.politeness <= 350 {
        push_rationale(&mut rationale, RD_RATIONALE_DIRECTNESS_HIGH);
        ToneV1::Direct
    } else if p.arousal >= 650 && p.directness >= 600 {
        ToneV1::Direct
    } else {
        ToneV1::Neutral
    };

    // Style selection (priority order).
    let style = if has_code {
        StyleV1::Debug
    } else if has_math {
        StyleV1::StepByStep
    } else if has_constraints || p.mode == RhetoricModeV1::Command {
        StyleV1::Checklist
    } else if p.mode == RhetoricModeV1::Brainstorm {
        StyleV1::StepByStep
    } else {
        StyleV1::Default
    };

    // Format flags are advisory; they guide structure but do not force it.
    let mut format_flags: FormatFlagsV1 = 0;
    if p.byte_len >= 300 {
        format_flags |= FORMAT_FLAG_INCLUDE_SUMMARY;
    }
    if has_request || p.mode == RhetoricModeV1::Command || is_problem_solve || is_logic_puzzle {
        format_flags |= FORMAT_FLAG_INCLUDE_NEXT_STEPS;
    }
    if safety_sensitive {
        format_flags |= FORMAT_FLAG_INCLUDE_RISKS;
    }
    if has_question && !has_constraints {
        format_flags |= FORMAT_FLAG_INCLUDE_ASSUMPTIONS;
    }

    match style {
        StyleV1::Checklist => {
            if p.mode == RhetoricModeV1::Command || has_request {
                format_flags |= FORMAT_FLAG_NUMBERED;
            } else {
                format_flags |= FORMAT_FLAG_BULLETS;
            }
        }
        StyleV1::StepByStep => {
            format_flags |= FORMAT_FLAG_NUMBERED;
        }
        StyleV1::Debug => {
            format_flags |= FORMAT_FLAG_BULLETS;
        }
        _ => {}
    }

    // Limits.
    let allow_questions = (has_question && !has_constraints) || is_problem_solve || is_logic_puzzle;

    let (max_softeners, max_preface_sentences, max_hedges, max_questions) = match tone {
        ToneV1::Neutral => (1u8, 0u8, 2u8, if allow_questions { 1u8 } else { 0u8 }),
        ToneV1::Supportive => (2u8, 1u8, 2u8, if allow_questions { 1u8 } else { 0u8 }),
        ToneV1::Direct => (0u8, 0u8, 1u8, if allow_questions { 1u8 } else { 0u8 }),
        ToneV1::Cautious => (1u8, 1u8, 3u8, if allow_questions { 1u8 } else { 0u8 }),
    };

    RealizerDirectivesV1 {
        version: REALIZER_DIRECTIVES_V1_VERSION,
        tone,
        style,
        format_flags,
        max_softeners,
        max_preface_sentences,
        max_hedges,
        max_questions,
        rationale_codes: finalize_rationale(rationale),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::frame::Id64;

    fn mk_ok() -> RealizerDirectivesV1 {
        RealizerDirectivesV1 {
            version: REALIZER_DIRECTIVES_V1_VERSION,
            tone: ToneV1::Supportive,
            style: StyleV1::StepByStep,
            format_flags: FORMAT_FLAG_BULLETS | FORMAT_FLAG_INCLUDE_NEXT_STEPS,
            max_softeners: 2,
            max_preface_sentences: 1,
            max_hedges: 2,
            max_questions: 1,
            rationale_codes: vec![1, 3, 10],
        }
    }

    #[test]
    fn directives_round_trip_ok() {
        let d = mk_ok();
        let b = d.encode().unwrap();
        let d2 = RealizerDirectivesV1::decode(&b).unwrap();
        assert_eq!(d, d2);
    }

    #[test]
    fn directives_decode_rejects_trailing_bytes() {
        let d = mk_ok();
        let mut b = d.encode().unwrap();
        b.push(0);
        let e = RealizerDirectivesV1::decode(&b).unwrap_err();
        assert!(format!("{e}").contains("trailing"));
    }

    #[test]
    fn directives_encode_rejects_non_canonical_rationale() {
        let mut d = mk_ok();
        d.rationale_codes = vec![3, 1];
        assert!(d.encode().is_err());
    }

    #[test]
    fn directives_decode_rejects_unknown_flags() {
        let d = mk_ok();
        let mut b = d.encode().unwrap();
        // format_flags is after: u32 version + u8 tone + u8 style + u16 reserved.
        let off = 4 + 1 + 1 + 2;
        let mut f = u32::from_le_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]]);
        f |= 1u32 << 31;
        let fb = f.to_le_bytes();
        b[off..off + 4].copy_from_slice(&fb);
        let e = RealizerDirectivesV1::decode(&b).unwrap_err();
        assert!(format!("{e}").contains("unknown format flags"));
    }

    fn mk_prag_base() -> PragmaticsFrameV1 {
        PragmaticsFrameV1 {
            version: crate::pragmatics_frame::PRAGMATICS_FRAME_V1_VERSION,
            source_id: Id64(1),
            msg_ix: 0,
            byte_len: 1000,
            ascii_only: 1,
            temperature: 0,
            valence: 0,
            arousal: 0,
            politeness: 500,
            formality: 500,
            directness: 500,
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

    fn is_strictly_increasing(xs: &[u16]) -> bool {
        for i in 1..xs.len() {
            if xs[i - 1] >= xs[i] {
                return false;
            }
        }
        true
    }

    #[test]
    fn derive_safety_sensitive_is_cautious_and_includes_risks() {
        let mut p = mk_prag_base();
        p.flags = INTENT_FLAG_SAFETY_SENSITIVE;
        p.questions = 1;
        p.byte_len = 400;
        p.validate().unwrap();

        let d = derive_realizer_directives_v1(&p);
        assert_eq!(d.tone, ToneV1::Cautious);
        assert!(d.format_flags & FORMAT_FLAG_INCLUDE_RISKS != 0);
        assert!(d.rationale_codes.contains(&RD_RATIONALE_SAFETY_SENSITIVE));
        assert!(is_strictly_increasing(&d.rationale_codes));
    }

    #[test]
    fn derive_code_prefers_debug_style() {
        let mut p = mk_prag_base();
        p.flags = INTENT_FLAG_HAS_CODE;
        p.validate().unwrap();

        let d = derive_realizer_directives_v1(&p);
        assert_eq!(d.style, StyleV1::Debug);
        assert!(d.rationale_codes.contains(&RD_RATIONALE_HAS_CODE));
        assert!(is_strictly_increasing(&d.rationale_codes));
    }

    #[test]
    fn derive_command_with_constraints_is_checklist_numbered() {
        let mut p = mk_prag_base();
        p.mode = RhetoricModeV1::Command;
        p.flags = INTENT_FLAG_HAS_REQUEST | INTENT_FLAG_HAS_CONSTRAINTS;
        p.validate().unwrap();

        let d = derive_realizer_directives_v1(&p);
        assert_eq!(d.style, StyleV1::Checklist);
        assert!(d.format_flags & FORMAT_FLAG_NUMBERED != 0);
        assert!(d.format_flags & FORMAT_FLAG_INCLUDE_NEXT_STEPS != 0);
        assert!(d.rationale_codes.contains(&RD_RATIONALE_HAS_CONSTRAINTS));
        assert!(d.rationale_codes.contains(&RD_RATIONALE_HAS_REQUEST));
        assert!(is_strictly_increasing(&d.rationale_codes));
    }

    #[test]
    fn derive_empathy_high_is_supportive_with_preface_budget() {
        let mut p = mk_prag_base();
        p.empathy_need = 800;
        p.validate().unwrap();

        let d = derive_realizer_directives_v1(&p);
        assert_eq!(d.tone, ToneV1::Supportive);
        assert_eq!(d.max_preface_sentences, 1);
        assert!(d.rationale_codes.contains(&RD_RATIONALE_EMPATHY_HIGH));
        assert!(is_strictly_increasing(&d.rationale_codes));
    }
}
