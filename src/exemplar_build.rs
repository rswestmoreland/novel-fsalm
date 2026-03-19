// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Offline exemplar builder helpers.
//!
//! This module keeps exemplar work offline and advisory-only. The builder:
//! - prepares a deterministic input plan over supported source families
//! - mines bounded structure rows from selected decoded artifacts
//! - finalizes a canonical ExemplarMemoryV1 artifact
//!
//! Mining remains intentionally conservative in this step:
//! - PromptPack and ConversationPack contribute request-shape signals from the
//!   last user message
//! - MarkovTrace contributes bounded surface-shape signals from emitted choice ids
//! - ReplayLog, GoldenPack, and GoldenPackConversation are currently accepted as
//!   supported inputs but do not emit rows yet because their current report forms
//!   do not expose enough structure detail for safe v1 mining

use crate::conversation_pack::{ConversationPackV1, ConversationRole};
use crate::exemplar_memory::{
    ExemplarMemoryFlagsV1, ExemplarMemoryV1, ExemplarResponseModeV1, ExemplarRowFlagsV1,
    ExemplarRowV1, ExemplarStructureKindV1, ExemplarSupportRefV1, ExemplarSupportSourceKindV1,
    ExemplarToneKindV1, EXEMPLAR_MEMORY_V1_MAX_ROWS, EXEMPLAR_MEMORY_V1_MAX_SUPPORT_REFS,
    EXEMPLAR_MEMORY_V1_VERSION, EXMEM_FLAGS_V1_ALL, EXMEM_FLAG_HAS_CONVERSATION_PACK,
    EXMEM_FLAG_HAS_GOLDEN_PACK, EXMEM_FLAG_HAS_GOLDEN_PACK_CONVERSATION,
    EXMEM_FLAG_HAS_MARKOV_TRACE, EXMEM_FLAG_HAS_PROMPT_PACK, EXMEM_FLAG_HAS_REPLAY_LOG,
    EXROW_FLAG_HAS_CLARIFIER, EXROW_FLAG_HAS_COMPARISON, EXROW_FLAG_HAS_STEPS,
    EXROW_FLAG_HAS_SUMMARY,
};
use crate::frame::{derive_id64, Id64};
use crate::golden_pack::GoldenPackReportV1;
use crate::golden_pack_conversation::GoldenPackConversationReportV1;
use crate::hash::Hash32;
use crate::markov_hints::MarkovChoiceKindV1;
use crate::markov_trace::MarkovTraceV1;
use crate::pragmatics_extract::{extract_pragmatics_frame_v1, PragmaticsExtractCfg};
use crate::pragmatics_frame::{
    INTENT_FLAG_HAS_FOCUS_STEPS, INTENT_FLAG_HAS_FOCUS_SUMMARY, INTENT_FLAG_IS_COMPARE_REQUEST,
    INTENT_FLAG_IS_EXPLAIN_REQUEST, INTENT_FLAG_IS_FOLLOW_UP, INTENT_FLAG_IS_LOGIC_PUZZLE,
    INTENT_FLAG_IS_PROBLEM_SOLVE, INTENT_FLAG_IS_RECOMMEND_REQUEST,
    INTENT_FLAG_IS_SUMMARIZE_REQUEST,
};
use crate::prompt_pack::{PromptPack, Role as PromptRole};
use crate::realizer_directives::{derive_realizer_directives_v1, ToneV1};
use crate::replay::ReplayLog;

/// Maximum number of input artifacts accepted by the v1 builder.
pub const EXEMPLAR_BUILD_V1_MAX_INPUTS: usize = 512;

/// Deterministic builder config for exemplar memory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExemplarBuildConfigV1 {
    /// Maximum number of input artifacts to consider overall.
    pub max_inputs_total: u32,
    /// Maximum number of input artifacts to keep per source kind.
    pub max_inputs_per_source_kind: u32,
    /// Maximum number of rows to keep in the finalized exemplar artifact.
    pub max_rows: u32,
    /// Maximum number of support refs to keep per row in the finalized artifact.
    pub max_support_refs_per_row: u8,
}

impl ExemplarBuildConfigV1 {
    /// Default deterministic config for v1.
    pub fn default_v1() -> Self {
        Self {
            max_inputs_total: 256,
            max_inputs_per_source_kind: 64,
            max_rows: EXEMPLAR_MEMORY_V1_MAX_ROWS as u32,
            max_support_refs_per_row: EXEMPLAR_MEMORY_V1_MAX_SUPPORT_REFS as u8,
        }
    }

    /// Validate builder caps.
    pub fn validate(&self) -> Result<(), ExemplarBuildError> {
        if self.max_inputs_total == 0
            || self.max_inputs_total as usize > EXEMPLAR_BUILD_V1_MAX_INPUTS
            || self.max_inputs_per_source_kind == 0
            || self.max_rows == 0
            || self.max_rows as usize > EXEMPLAR_MEMORY_V1_MAX_ROWS
            || self.max_support_refs_per_row == 0
            || self.max_support_refs_per_row as usize > EXEMPLAR_MEMORY_V1_MAX_SUPPORT_REFS
        {
            return Err(ExemplarBuildError::BadConfig);
        }
        Ok(())
    }
}

/// One input artifact accepted by the builder.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ExemplarBuildInputV1 {
    /// Input source family.
    pub source_kind: ExemplarSupportSourceKindV1,
    /// Content hash of the source artifact.
    pub source_hash: Hash32,
}

impl ExemplarBuildInputV1 {
    /// Construct a builder input.
    pub fn new(source_kind: ExemplarSupportSourceKindV1, source_hash: Hash32) -> Self {
        Self {
            source_kind,
            source_hash,
        }
    }
}

/// Prepared deterministic build plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExemplarBuildPlanV1 {
    /// Stable build id for the future exemplar artifact.
    pub build_id: Hash32,
    /// Source-family flags derived from kept inputs.
    pub flags: ExemplarMemoryFlagsV1,
    /// Canonical deduplicated and capped input inventory.
    pub inputs: Vec<ExemplarBuildInputV1>,
    /// Final row cap to apply when the artifact is finalized.
    pub max_rows: u32,
    /// Final per-row support-ref cap to apply when the artifact is finalized.
    pub max_support_refs_per_row: u8,
}

/// Deterministic report for build planning.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExemplarBuildReportV1 {
    /// Number of inputs provided by the caller.
    pub inputs_seen: u32,
    /// Number of inputs kept after dedup and capping.
    pub inputs_kept: u32,
    /// Number of duplicate inputs dropped.
    pub inputs_deduped: u32,
    /// Number of inputs dropped by caps.
    pub inputs_dropped_by_cap: u32,
}

/// Borrowed decoded source artifact used by the mining step.
#[derive(Clone, Copy, Debug)]
pub enum ExemplarSourceArtifactV1<'a> {
    /// ReplayLog source.
    ReplayLog {
        /// Content hash of the artifact.
        source_hash: Hash32,
        /// Decoded artifact.
        artifact: &'a ReplayLog,
    },
    /// PromptPack source.
    PromptPack {
        /// Content hash of the artifact.
        source_hash: Hash32,
        /// Decoded artifact.
        artifact: &'a PromptPack,
    },
    /// GoldenPack source.
    GoldenPack {
        /// Content hash of the artifact.
        source_hash: Hash32,
        /// Decoded artifact.
        artifact: &'a GoldenPackReportV1,
    },
    /// GoldenPackConversation source.
    GoldenPackConversation {
        /// Content hash of the artifact.
        source_hash: Hash32,
        /// Decoded artifact.
        artifact: &'a GoldenPackConversationReportV1,
    },
    /// ConversationPack source.
    ConversationPack {
        /// Content hash of the artifact.
        source_hash: Hash32,
        /// Decoded artifact.
        artifact: &'a ConversationPackV1,
    },
    /// MarkovTrace source.
    MarkovTrace {
        /// Content hash of the artifact.
        source_hash: Hash32,
        /// Decoded artifact.
        artifact: &'a MarkovTraceV1,
    },
}

impl<'a> ExemplarSourceArtifactV1<'a> {
    /// Return the canonical build input for this source artifact.
    pub fn build_input(&self) -> ExemplarBuildInputV1 {
        match self {
            ExemplarSourceArtifactV1::ReplayLog {
                source_hash,
                artifact,
            } => {
                let _ = artifact.version;
                ExemplarBuildInputV1::new(ExemplarSupportSourceKindV1::ReplayLog, *source_hash)
            }
            ExemplarSourceArtifactV1::PromptPack {
                source_hash,
                artifact,
            } => {
                let _ = artifact.version;
                ExemplarBuildInputV1::new(ExemplarSupportSourceKindV1::PromptPack, *source_hash)
            }
            ExemplarSourceArtifactV1::GoldenPack {
                source_hash,
                artifact,
            } => {
                let _ = artifact.version;
                ExemplarBuildInputV1::new(ExemplarSupportSourceKindV1::GoldenPack, *source_hash)
            }
            ExemplarSourceArtifactV1::GoldenPackConversation {
                source_hash,
                artifact,
            } => {
                let _ = artifact.version;
                ExemplarBuildInputV1::new(
                    ExemplarSupportSourceKindV1::GoldenPackConversation,
                    *source_hash,
                )
            }
            ExemplarSourceArtifactV1::ConversationPack {
                source_hash,
                artifact,
            } => {
                let _ = artifact.version;
                ExemplarBuildInputV1::new(
                    ExemplarSupportSourceKindV1::ConversationPack,
                    *source_hash,
                )
            }
            ExemplarSourceArtifactV1::MarkovTrace {
                source_hash,
                artifact,
            } => {
                let _ = artifact.version;
                ExemplarBuildInputV1::new(ExemplarSupportSourceKindV1::MarkovTrace, *source_hash)
            }
        }
    }
}

/// Errors produced by exemplar build helpers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExemplarBuildError {
    /// Builder config is invalid.
    BadConfig,
    /// Too many inputs were supplied.
    TooManyInputs,
    /// Finalized rows exceed the configured row cap.
    TooManyRows,
    /// A row exceeds the configured support-ref cap.
    TooManySupportRefs,
    /// Finalized artifact failed validation.
    InvalidOutput,
}

impl core::fmt::Display for ExemplarBuildError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ExemplarBuildError::BadConfig => f.write_str("bad exemplar build config"),
            ExemplarBuildError::TooManyInputs => f.write_str("too many exemplar build inputs"),
            ExemplarBuildError::TooManyRows => f.write_str("too many exemplar rows"),
            ExemplarBuildError::TooManySupportRefs => f.write_str("too many exemplar support refs"),
            ExemplarBuildError::InvalidOutput => f.write_str("invalid exemplar build output"),
        }
    }
}

impl std::error::Error for ExemplarBuildError {}

fn source_kind_flag(kind: ExemplarSupportSourceKindV1) -> ExemplarMemoryFlagsV1 {
    match kind {
        ExemplarSupportSourceKindV1::ReplayLog => EXMEM_FLAG_HAS_REPLAY_LOG,
        ExemplarSupportSourceKindV1::PromptPack => EXMEM_FLAG_HAS_PROMPT_PACK,
        ExemplarSupportSourceKindV1::GoldenPack => EXMEM_FLAG_HAS_GOLDEN_PACK,
        ExemplarSupportSourceKindV1::GoldenPackConversation => {
            EXMEM_FLAG_HAS_GOLDEN_PACK_CONVERSATION
        }
        ExemplarSupportSourceKindV1::ConversationPack => EXMEM_FLAG_HAS_CONVERSATION_PACK,
        ExemplarSupportSourceKindV1::MarkovTrace => EXMEM_FLAG_HAS_MARKOV_TRACE,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ExemplarRowKeyV1 {
    response_mode: ExemplarResponseModeV1,
    structure_kind: ExemplarStructureKindV1,
    tone_kind: ExemplarToneKindV1,
    flags: ExemplarRowFlagsV1,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExemplarMinedSeedV1 {
    key: ExemplarRowKeyV1,
    support_ref: ExemplarSupportRefV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct SourceSlotV1 {
    input: ExemplarBuildInputV1,
    source_ix: usize,
}

fn cmp_support_ref_canon(
    a: &ExemplarSupportRefV1,
    b: &ExemplarSupportRefV1,
) -> core::cmp::Ordering {
    match (a.source_kind as u8).cmp(&(b.source_kind as u8)) {
        core::cmp::Ordering::Equal => {}
        o => return o,
    }
    match a.source_hash.cmp(&b.source_hash) {
        core::cmp::Ordering::Equal => {}
        o => return o,
    }
    a.item_ix.cmp(&b.item_ix)
}

fn sort_rows_canonical(xs: &mut [ExemplarRowV1]) {
    xs.sort_by(|a, b| {
        match b.support_count.cmp(&a.support_count) {
            core::cmp::Ordering::Equal => {}
            o => return o,
        }
        match (a.response_mode as u8).cmp(&(b.response_mode as u8)) {
            core::cmp::Ordering::Equal => {}
            o => return o,
        }
        match (a.structure_kind as u8).cmp(&(b.structure_kind as u8)) {
            core::cmp::Ordering::Equal => {}
            o => return o,
        }
        match (a.tone_kind as u8).cmp(&(b.tone_kind as u8)) {
            core::cmp::Ordering::Equal => {}
            o => return o,
        }
        a.exemplar_id.0.cmp(&b.exemplar_id.0)
    });
}

fn make_exemplar_id_v1(key: ExemplarRowKeyV1) -> Id64 {
    let mut payload = [0u8; 7];
    payload[0] = key.response_mode as u8;
    payload[1] = key.structure_kind as u8;
    payload[2] = key.tone_kind as u8;
    payload[3..7].copy_from_slice(&key.flags.to_le_bytes());
    derive_id64(b"exemplar_row_v1", &payload)
}

fn tone_kind_from_directives(tone: ToneV1) -> ExemplarToneKindV1 {
    match tone {
        ToneV1::Neutral => ExemplarToneKindV1::Neutral,
        ToneV1::Supportive => ExemplarToneKindV1::Supportive,
        ToneV1::Direct => ExemplarToneKindV1::Direct,
        ToneV1::Cautious => ExemplarToneKindV1::Cautious,
    }
}

fn tone_kind_from_preface_choice_id(choice_id: Id64) -> Option<ExemplarToneKindV1> {
    let supportive0 = derive_id64(b"markov_choice_v1", b"preface:supportive:0");
    let supportive1 = derive_id64(b"markov_choice_v1", b"preface:supportive:1");
    let neutral0 = derive_id64(b"markov_choice_v1", b"preface:neutral:0");
    let neutral1 = derive_id64(b"markov_choice_v1", b"preface:neutral:1");
    let direct0 = derive_id64(b"markov_choice_v1", b"preface:direct:0");
    let direct1 = derive_id64(b"markov_choice_v1", b"preface:direct:1");
    let cautious0 = derive_id64(b"markov_choice_v1", b"preface:cautious:0");
    let cautious1 = derive_id64(b"markov_choice_v1", b"preface:cautious:1");
    if choice_id == supportive0 || choice_id == supportive1 {
        Some(ExemplarToneKindV1::Supportive)
    } else if choice_id == neutral0 || choice_id == neutral1 {
        Some(ExemplarToneKindV1::Neutral)
    } else if choice_id == direct0 || choice_id == direct1 {
        Some(ExemplarToneKindV1::Direct)
    } else if choice_id == cautious0 || choice_id == cautious1 {
        Some(ExemplarToneKindV1::Cautious)
    } else {
        None
    }
}

fn build_query_shape_seed_v1(
    source_kind: ExemplarSupportSourceKindV1,
    source_hash: Hash32,
    item_ix: u32,
    text: &str,
    continue_bias: bool,
) -> Option<ExemplarMinedSeedV1> {
    let cfg = PragmaticsExtractCfg::default();
    let frame = extract_pragmatics_frame_v1(Id64(0), item_ix, text, &cfg).ok()?;
    let directives = derive_realizer_directives_v1(&frame);
    let flags = frame.flags;
    let lower = text.to_ascii_lowercase();

    let fallback_compare = lower.starts_with("compare ")
        || lower.contains(" compare ")
        || lower.contains(" vs ")
        || (lower.contains("option a") && lower.contains("option b"));
    let fallback_recommend = lower.contains("recommend")
        || lower.contains("best option")
        || lower.contains("best choice");
    let fallback_summarize = lower.contains("summarize")
        || lower.contains("summary")
        || lower.contains("recap")
        || lower.contains("overview")
        || lower.contains("tldr");
    let fallback_explain = lower.contains("explain")
        || lower.contains("walk through")
        || lower.contains("walkthrough")
        || lower.contains("how ");
    let fallback_steps = lower.contains("step by step")
        || lower.contains("walk through")
        || lower.contains("walkthrough");
    let fallback_summary_focus = lower.contains("brief")
        || lower.contains("briefly")
        || lower.contains("short")
        || lower.contains("high level")
        || lower.contains("overview")
        || lower.contains("tldr");

    let response_mode = if continue_bias || (flags & INTENT_FLAG_IS_FOLLOW_UP) != 0 {
        ExemplarResponseModeV1::Continue
    } else if (flags & INTENT_FLAG_IS_COMPARE_REQUEST) != 0 || fallback_compare {
        ExemplarResponseModeV1::Compare
    } else if (flags & INTENT_FLAG_IS_RECOMMEND_REQUEST) != 0 || fallback_recommend {
        ExemplarResponseModeV1::Recommend
    } else if (flags & INTENT_FLAG_IS_SUMMARIZE_REQUEST) != 0 || fallback_summarize {
        ExemplarResponseModeV1::Summarize
    } else if (flags & INTENT_FLAG_IS_EXPLAIN_REQUEST) != 0 || fallback_explain {
        ExemplarResponseModeV1::Explain
    } else if (flags & INTENT_FLAG_IS_PROBLEM_SOLVE) != 0
        || (flags & INTENT_FLAG_IS_LOGIC_PUZZLE) != 0
    {
        ExemplarResponseModeV1::Troubleshoot
    } else {
        ExemplarResponseModeV1::Direct
    };

    let structure_kind = if response_mode == ExemplarResponseModeV1::Compare {
        ExemplarStructureKindV1::Comparison
    } else if response_mode == ExemplarResponseModeV1::Recommend {
        ExemplarStructureKindV1::Recommendation
    } else if response_mode == ExemplarResponseModeV1::Summarize
        || (flags & INTENT_FLAG_HAS_FOCUS_SUMMARY) != 0
        || fallback_summary_focus
    {
        ExemplarStructureKindV1::SummaryFirst
    } else if response_mode == ExemplarResponseModeV1::Explain
        || response_mode == ExemplarResponseModeV1::Troubleshoot
        || (flags & INTENT_FLAG_HAS_FOCUS_STEPS) != 0
        || fallback_steps
    {
        ExemplarStructureKindV1::Steps
    } else {
        ExemplarStructureKindV1::Direct
    };

    let mut row_flags: ExemplarRowFlagsV1 = 0;
    if structure_kind == ExemplarStructureKindV1::SummaryFirst {
        row_flags |= EXROW_FLAG_HAS_SUMMARY;
    }
    if structure_kind == ExemplarStructureKindV1::Steps {
        row_flags |= EXROW_FLAG_HAS_STEPS;
    }
    if structure_kind == ExemplarStructureKindV1::Comparison {
        row_flags |= EXROW_FLAG_HAS_COMPARISON;
    }

    Some(ExemplarMinedSeedV1 {
        key: ExemplarRowKeyV1 {
            response_mode,
            structure_kind,
            tone_kind: tone_kind_from_directives(directives.tone),
            flags: row_flags,
        },
        support_ref: ExemplarSupportRefV1::new(source_kind, source_hash, item_ix),
    })
}

fn mine_prompt_pack_rows_v1(source_hash: Hash32, pack: &PromptPack) -> Vec<ExemplarMinedSeedV1> {
    for (ix, msg) in pack.messages.iter().enumerate().rev() {
        if msg.role == PromptRole::User {
            let item_ix = if (ix as u64) > (u32::MAX as u64) {
                u32::MAX
            } else {
                ix as u32
            };
            if let Some(seed) = build_query_shape_seed_v1(
                ExemplarSupportSourceKindV1::PromptPack,
                source_hash,
                item_ix,
                &msg.content,
                false,
            ) {
                return vec![seed];
            }
            break;
        }
    }
    Vec::new()
}

fn mine_conversation_pack_rows_v1(
    source_hash: Hash32,
    pack: &ConversationPackV1,
) -> Vec<ExemplarMinedSeedV1> {
    let mut user_count = 0usize;
    for m in &pack.messages {
        if m.role == ConversationRole::User {
            user_count += 1;
        }
    }
    for (ix, msg) in pack.messages.iter().enumerate().rev() {
        if msg.role == ConversationRole::User {
            let item_ix = if (ix as u64) > (u32::MAX as u64) {
                u32::MAX
            } else {
                ix as u32
            };
            if let Some(seed) = build_query_shape_seed_v1(
                ExemplarSupportSourceKindV1::ConversationPack,
                source_hash,
                item_ix,
                &msg.content,
                user_count > 1,
            ) {
                return vec![seed];
            }
            break;
        }
    }
    Vec::new()
}

fn mine_markov_trace_rows_v1(
    source_hash: Hash32,
    trace: &MarkovTraceV1,
) -> Vec<ExemplarMinedSeedV1> {
    let clarifier_intro0 = derive_id64(b"markov_choice_v1", b"other:clarifier_intro:0");
    let clarifier_intro1 = derive_id64(b"markov_choice_v1", b"other:clarifier_intro:1");
    let append_clarify = derive_id64(b"markov_choice_v1", b"append:clarify_question");
    let plan_summary = derive_id64(b"markov_choice_v1", b"plan_item:summary");

    let mut tone_kind = ExemplarToneKindV1::Neutral;
    let mut found_tone = false;
    let mut flags: ExemplarRowFlagsV1 = 0;
    let mut response_mode = ExemplarResponseModeV1::Direct;
    let mut structure_kind = ExemplarStructureKindV1::Direct;
    let mut first_ix = 0u32;
    let mut saw_any = false;

    for (ix, tok) in trace.tokens.iter().enumerate() {
        if !found_tone && tok.kind == MarkovChoiceKindV1::Opener {
            if let Some(tone) = tone_kind_from_preface_choice_id(tok.choice_id) {
                tone_kind = tone;
                found_tone = true;
                if !saw_any {
                    first_ix = if (ix as u64) > (u32::MAX as u64) {
                        u32::MAX
                    } else {
                        ix as u32
                    };
                    saw_any = true;
                }
            }
        }
        if tok.choice_id == plan_summary {
            flags |= EXROW_FLAG_HAS_SUMMARY;
            if structure_kind == ExemplarStructureKindV1::Direct {
                structure_kind = ExemplarStructureKindV1::SummaryFirst;
            }
            if !saw_any {
                first_ix = if (ix as u64) > (u32::MAX as u64) {
                    u32::MAX
                } else {
                    ix as u32
                };
                saw_any = true;
            }
        }
        if tok.choice_id == clarifier_intro0
            || tok.choice_id == clarifier_intro1
            || tok.choice_id == append_clarify
        {
            flags |= EXROW_FLAG_HAS_CLARIFIER;
            response_mode = ExemplarResponseModeV1::Clarify;
            structure_kind = ExemplarStructureKindV1::Clarifier;
            if !saw_any {
                first_ix = if (ix as u64) > (u32::MAX as u64) {
                    u32::MAX
                } else {
                    ix as u32
                };
                saw_any = true;
            }
        }
    }

    if !found_tone && !saw_any {
        return Vec::new();
    }

    vec![ExemplarMinedSeedV1 {
        key: ExemplarRowKeyV1 {
            response_mode,
            structure_kind,
            tone_kind,
            flags,
        },
        support_ref: ExemplarSupportRefV1::new(
            ExemplarSupportSourceKindV1::MarkovTrace,
            source_hash,
            first_ix,
        ),
    }]
}

/// Prepare a deterministic build plan from supported input artifacts.
///
/// Inputs are canonicalized by `(source_kind, source_hash)`, deduplicated, and
/// capped in that order. No source artifact decoding happens in this step.
pub fn prepare_exemplar_build_plan_v1(
    build_id: Hash32,
    mut inputs: Vec<ExemplarBuildInputV1>,
    cfg: &ExemplarBuildConfigV1,
) -> Result<(ExemplarBuildPlanV1, ExemplarBuildReportV1), ExemplarBuildError> {
    cfg.validate()?;
    if inputs.len() > EXEMPLAR_BUILD_V1_MAX_INPUTS {
        return Err(ExemplarBuildError::TooManyInputs);
    }

    let inputs_seen = inputs.len() as u32;
    inputs.sort_unstable();

    let mut deduped: Vec<ExemplarBuildInputV1> = Vec::with_capacity(inputs.len());
    let mut deduped_count = 0u32;
    let mut prev: Option<ExemplarBuildInputV1> = None;
    for item in inputs {
        if prev == Some(item) {
            deduped_count = deduped_count.saturating_add(1);
            continue;
        }
        prev = Some(item);
        deduped.push(item);
    }

    let mut kept: Vec<ExemplarBuildInputV1> = Vec::with_capacity(deduped.len());
    let mut dropped_by_cap = 0u32;
    let mut flags = 0u32;
    let mut current_kind: Option<ExemplarSupportSourceKindV1> = None;
    let mut kept_for_kind = 0u32;
    for item in deduped {
        if kept.len() >= cfg.max_inputs_total as usize {
            dropped_by_cap = dropped_by_cap.saturating_add(1);
            continue;
        }
        if current_kind != Some(item.source_kind) {
            current_kind = Some(item.source_kind);
            kept_for_kind = 0;
        }
        if kept_for_kind >= cfg.max_inputs_per_source_kind {
            dropped_by_cap = dropped_by_cap.saturating_add(1);
            continue;
        }
        kept_for_kind = kept_for_kind.saturating_add(1);
        flags |= source_kind_flag(item.source_kind);
        kept.push(item);
    }
    flags &= EXMEM_FLAGS_V1_ALL;

    let inputs_kept = kept_for_kind_count(&kept);

    Ok((
        ExemplarBuildPlanV1 {
            build_id,
            flags,
            inputs: kept,
            max_rows: cfg.max_rows,
            max_support_refs_per_row: cfg.max_support_refs_per_row,
        },
        ExemplarBuildReportV1 {
            inputs_seen,
            inputs_kept,
            inputs_deduped: deduped_count,
            inputs_dropped_by_cap: dropped_by_cap,
        },
    ))
}

fn kept_for_kind_count(xs: &[ExemplarBuildInputV1]) -> u32 {
    if xs.len() > u32::MAX as usize {
        u32::MAX
    } else {
        xs.len() as u32
    }
}

/// Mine deterministic exemplar rows from selected decoded source artifacts.
///
/// The caller supplies decoded artifacts. This step stays offline and does not
/// load from the artifact store. Only artifacts present in `plan.inputs` are
/// considered. Duplicate source artifacts are ignored after canonical sorting.
pub fn mine_exemplar_rows_from_sources_v1(
    plan: &ExemplarBuildPlanV1,
    sources: &[ExemplarSourceArtifactV1<'_>],
) -> Result<Vec<ExemplarRowV1>, ExemplarBuildError> {
    if plan.inputs.len() > EXEMPLAR_BUILD_V1_MAX_INPUTS {
        return Err(ExemplarBuildError::BadConfig);
    }

    let mut slots: Vec<SourceSlotV1> = Vec::with_capacity(sources.len());
    for (ix, src) in sources.iter().enumerate() {
        slots.push(SourceSlotV1 {
            input: src.build_input(),
            source_ix: ix,
        });
    }
    slots.sort_unstable();

    let mut rows: Vec<ExemplarRowV1> = Vec::new();
    let mut used_inputs: Vec<ExemplarBuildInputV1> = Vec::new();
    for slot in slots {
        if used_inputs.binary_search(&slot.input).is_ok() {
            continue;
        }
        if plan.inputs.binary_search(&slot.input).is_err() {
            continue;
        }
        used_inputs.push(slot.input);
        used_inputs.sort_unstable();

        let mined = match sources[slot.source_ix] {
            ExemplarSourceArtifactV1::ReplayLog { .. } => Vec::new(),
            ExemplarSourceArtifactV1::PromptPack {
                source_hash,
                artifact,
            } => mine_prompt_pack_rows_v1(source_hash, artifact),
            ExemplarSourceArtifactV1::GoldenPack { .. } => Vec::new(),
            ExemplarSourceArtifactV1::GoldenPackConversation { .. } => Vec::new(),
            ExemplarSourceArtifactV1::ConversationPack {
                source_hash,
                artifact,
            } => mine_conversation_pack_rows_v1(source_hash, artifact),
            ExemplarSourceArtifactV1::MarkovTrace {
                source_hash,
                artifact,
            } => mine_markov_trace_rows_v1(source_hash, artifact),
        };

        for seed in mined {
            let exemplar_id = make_exemplar_id_v1(seed.key);
            match rows.iter_mut().find(|r| r.exemplar_id == exemplar_id) {
                Some(row) => {
                    let is_new = row.support_refs.iter().all(|r| {
                        cmp_support_ref_canon(r, &seed.support_ref) != core::cmp::Ordering::Equal
                    });
                    if is_new {
                        row.support_count = row.support_count.saturating_add(1);
                        if row.support_refs.len() < plan.max_support_refs_per_row as usize {
                            row.support_refs.push(seed.support_ref);
                            row.support_refs.sort_by(cmp_support_ref_canon);
                        }
                    }
                }
                None => {
                    rows.push(ExemplarRowV1 {
                        exemplar_id,
                        response_mode: seed.key.response_mode,
                        structure_kind: seed.key.structure_kind,
                        tone_kind: seed.key.tone_kind,
                        flags: seed.key.flags,
                        support_count: 1,
                        support_refs: vec![seed.support_ref],
                    });
                }
            }
        }
    }

    sort_rows_canonical(&mut rows);
    if rows.len() > plan.max_rows as usize {
        rows.truncate(plan.max_rows as usize);
    }
    for row in &mut rows {
        row.support_refs.sort_by(cmp_support_ref_canon);
        if row.support_refs.len() > plan.max_support_refs_per_row as usize {
            row.support_refs
                .truncate(plan.max_support_refs_per_row as usize);
        }
    }
    Ok(rows)
}

/// Build a canonical empty exemplar artifact from a prepared plan.
pub fn build_empty_exemplar_memory_v1(
    plan: &ExemplarBuildPlanV1,
) -> Result<ExemplarMemoryV1, ExemplarBuildError> {
    let out = ExemplarMemoryV1 {
        version: EXEMPLAR_MEMORY_V1_VERSION,
        build_id: plan.build_id,
        flags: plan.flags,
        rows: Vec::new(),
    };
    out.validate()
        .map_err(|_| ExemplarBuildError::InvalidOutput)?;
    Ok(out)
}

/// Finalize a prepared plan into ExemplarMemoryV1 using caller-supplied rows.
///
/// Mining stays separate from finalization so later steps can still inject rows
/// from additional offline passes without changing the artifact contract.
pub fn finalize_exemplar_memory_v1(
    plan: &ExemplarBuildPlanV1,
    rows: Vec<ExemplarRowV1>,
) -> Result<ExemplarMemoryV1, ExemplarBuildError> {
    if rows.len() > plan.max_rows as usize {
        return Err(ExemplarBuildError::TooManyRows);
    }
    for row in &rows {
        if row.support_refs.len() > plan.max_support_refs_per_row as usize {
            return Err(ExemplarBuildError::TooManySupportRefs);
        }
    }
    let out = ExemplarMemoryV1 {
        version: EXEMPLAR_MEMORY_V1_VERSION,
        build_id: plan.build_id,
        flags: plan.flags,
        rows,
    };
    out.validate()
        .map_err(|_| ExemplarBuildError::InvalidOutput)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exemplar_memory::{ExemplarSupportRefV1, EXROW_FLAG_HAS_STEPS};
    use crate::hash::blake3_hash;
    use crate::markov_model::MarkovTokenV1;
    use crate::prompt_pack::{ConstraintKV, Message, PromptIds};

    fn sample_cfg() -> ExemplarBuildConfigV1 {
        ExemplarBuildConfigV1 {
            max_inputs_total: 4,
            max_inputs_per_source_kind: 2,
            max_rows: 4,
            max_support_refs_per_row: 2,
        }
    }

    fn sample_prompt_pack(text: &str) -> PromptPack {
        PromptPack {
            version: crate::prompt_pack::PROMPT_PACK_VERSION,
            seed: 7,
            max_output_tokens: 64,
            ids: PromptIds {
                snapshot_id: [0u8; 32],
                weights_id: [0u8; 32],
                tokenizer_id: [0u8; 32],
            },
            messages: vec![Message {
                role: PromptRole::User,
                content: text.to_string(),
            }],
            constraints: Vec::<ConstraintKV>::new(),
        }
    }

    fn sample_conversation_pack(text: &str) -> ConversationPackV1 {
        ConversationPackV1 {
            version: crate::conversation_pack::CONVERSATION_PACK_VERSION,
            seed: 9,
            max_output_tokens: 64,
            snapshot_id: [0u8; 32],
            sig_map_id: [0u8; 32],
            lexicon_snapshot_id: None,
            markov_model_id: None,
            exemplar_memory_id: None,
            graph_relevance_id: None,
            presentation_mode: None,
            limits: crate::conversation_pack::ConversationLimits::default_v1(),
            messages: vec![
                crate::conversation_pack::ConversationMessage {
                    role: ConversationRole::User,
                    content: "Explain the baseline".to_string(),
                    replay_id: None,
                },
                crate::conversation_pack::ConversationMessage {
                    role: ConversationRole::Assistant,
                    content: "Main answer\n- item=0".to_string(),
                    replay_id: None,
                },
                crate::conversation_pack::ConversationMessage {
                    role: ConversationRole::User,
                    content: text.to_string(),
                    replay_id: None,
                },
            ],
        }
    }

    fn sample_trace_clarifier() -> MarkovTraceV1 {
        MarkovTraceV1 {
            version: crate::markov_trace::MARKOV_TRACE_V1_VERSION,
            query_id: blake3_hash(b"q"),
            tokens: vec![
                MarkovTokenV1::new(
                    MarkovChoiceKindV1::Opener,
                    derive_id64(b"markov_choice_v1", b"preface:supportive:1"),
                ),
                MarkovTokenV1::new(
                    MarkovChoiceKindV1::Other,
                    derive_id64(b"markov_choice_v1", b"other:clarifier_intro:1"),
                ),
                MarkovTokenV1::new(
                    MarkovChoiceKindV1::Other,
                    derive_id64(b"markov_choice_v1", b"append:clarify_question"),
                ),
            ],
        }
    }

    #[test]
    fn build_plan_dedups_and_caps_inputs() {
        let cfg = sample_cfg();
        let a =
            ExemplarBuildInputV1::new(ExemplarSupportSourceKindV1::ReplayLog, blake3_hash(b"a"));
        let b =
            ExemplarBuildInputV1::new(ExemplarSupportSourceKindV1::ReplayLog, blake3_hash(b"b"));
        let c =
            ExemplarBuildInputV1::new(ExemplarSupportSourceKindV1::ReplayLog, blake3_hash(b"c"));
        let d =
            ExemplarBuildInputV1::new(ExemplarSupportSourceKindV1::PromptPack, blake3_hash(b"d"));
        let (plan, report) =
            prepare_exemplar_build_plan_v1(blake3_hash(b"build"), vec![c, a, a, d, b], &cfg)
                .expect("prepare");
        assert_eq!(report.inputs_seen, 5);
        assert_eq!(report.inputs_deduped, 1);
        assert_eq!(report.inputs_dropped_by_cap, 1);
        assert_eq!(report.inputs_kept, 3);
        let mut expected = vec![a, b, d];
        expected.sort_unstable();
        assert_eq!(plan.inputs, expected);
        assert_eq!(
            plan.flags,
            EXMEM_FLAG_HAS_REPLAY_LOG | EXMEM_FLAG_HAS_PROMPT_PACK
        );
    }

    #[test]
    fn build_empty_memory_is_canonical() {
        let cfg = sample_cfg();
        let (plan, _) = prepare_exemplar_build_plan_v1(
            blake3_hash(b"build"),
            vec![ExemplarBuildInputV1::new(
                ExemplarSupportSourceKindV1::ReplayLog,
                blake3_hash(b"a"),
            )],
            &cfg,
        )
        .expect("prepare");
        let out = build_empty_exemplar_memory_v1(&plan).expect("build");
        assert!(out.is_canonical());
        assert!(out.rows.is_empty());
        assert_eq!(out.flags, EXMEM_FLAG_HAS_REPLAY_LOG);
    }

    #[test]
    fn finalize_minimal_memory_respects_caps() {
        let cfg = sample_cfg();
        let (plan, _) = prepare_exemplar_build_plan_v1(
            blake3_hash(b"build"),
            vec![ExemplarBuildInputV1::new(
                ExemplarSupportSourceKindV1::ReplayLog,
                blake3_hash(b"a"),
            )],
            &cfg,
        )
        .expect("prepare");
        let row = ExemplarRowV1 {
            exemplar_id: Id64(1),
            response_mode: ExemplarResponseModeV1::Explain,
            structure_kind: ExemplarStructureKindV1::Steps,
            tone_kind: ExemplarToneKindV1::Supportive,
            flags: EXROW_FLAG_HAS_STEPS,
            support_count: 1,
            support_refs: vec![ExemplarSupportRefV1::new(
                ExemplarSupportSourceKindV1::ReplayLog,
                blake3_hash(b"a"),
                0,
            )],
        };
        let out = finalize_exemplar_memory_v1(&plan, vec![row]).expect("finalize");
        assert_eq!(out.rows.len(), 1);
        assert!(out.is_canonical());
    }

    #[test]
    fn mine_rows_from_prompt_and_trace_is_canonical() {
        let cfg = sample_cfg();
        let prompt_hash = blake3_hash(b"prompt");
        let trace_hash = blake3_hash(b"trace");
        let prompt = sample_prompt_pack("Summarize the outage briefly.");
        let trace = sample_trace_clarifier();
        let (plan, _) = prepare_exemplar_build_plan_v1(
            blake3_hash(b"build"),
            vec![
                ExemplarBuildInputV1::new(ExemplarSupportSourceKindV1::PromptPack, prompt_hash),
                ExemplarBuildInputV1::new(ExemplarSupportSourceKindV1::MarkovTrace, trace_hash),
            ],
            &cfg,
        )
        .expect("prepare");
        let rows = mine_exemplar_rows_from_sources_v1(
            &plan,
            &[
                ExemplarSourceArtifactV1::MarkovTrace {
                    source_hash: trace_hash,
                    artifact: &trace,
                },
                ExemplarSourceArtifactV1::PromptPack {
                    source_hash: prompt_hash,
                    artifact: &prompt,
                },
            ],
        )
        .expect("mine");
        assert_eq!(rows.len(), 2);
        let clarifier = rows
            .iter()
            .find(|r| r.response_mode == ExemplarResponseModeV1::Clarify)
            .expect("clarifier row");
        assert_eq!(clarifier.structure_kind, ExemplarStructureKindV1::Clarifier);
        assert_eq!(clarifier.tone_kind, ExemplarToneKindV1::Supportive);
        assert_eq!(clarifier.flags, EXROW_FLAG_HAS_CLARIFIER);
        let summary = rows
            .iter()
            .find(|r| r.response_mode == ExemplarResponseModeV1::Summarize)
            .expect("summary row");
        assert_eq!(
            summary.structure_kind,
            ExemplarStructureKindV1::SummaryFirst
        );
        assert_eq!(summary.flags, EXROW_FLAG_HAS_SUMMARY);
        let out = finalize_exemplar_memory_v1(&plan, rows).expect("finalize");
        assert!(out.is_canonical());
    }

    #[test]
    fn mine_conversation_followup_becomes_continue_steps() {
        let cfg = sample_cfg();
        let conv_hash = blake3_hash(b"conv");
        let pack = sample_conversation_pack("Can you walk through the fix step by step?");
        let (plan, _) = prepare_exemplar_build_plan_v1(
            blake3_hash(b"build"),
            vec![ExemplarBuildInputV1::new(
                ExemplarSupportSourceKindV1::ConversationPack,
                conv_hash,
            )],
            &cfg,
        )
        .expect("prepare");
        let rows = mine_exemplar_rows_from_sources_v1(
            &plan,
            &[ExemplarSourceArtifactV1::ConversationPack {
                source_hash: conv_hash,
                artifact: &pack,
            }],
        )
        .expect("mine");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].response_mode, ExemplarResponseModeV1::Continue);
        assert_eq!(rows[0].structure_kind, ExemplarStructureKindV1::Steps);
        assert_eq!(rows[0].flags, EXROW_FLAG_HAS_STEPS);
        assert_eq!(rows[0].support_refs[0].item_ix, 2);
    }

    #[test]
    fn mine_rows_ignores_unplanned_and_opaque_sources() {
        let cfg = sample_cfg();
        let prompt_hash = blake3_hash(b"prompt");
        let skipped_hash = blake3_hash(b"skipped");
        let prompt = sample_prompt_pack("Compare option A and option B.");
        let golden = GoldenPackReportV1 {
            version: crate::golden_pack::GOLDEN_PACK_REPORT_V1_VERSION,
            pack_name: "gp".to_string(),
            scale_report_hash: [0u8; 32],
            scale_report: crate::scale_report::ScaleDemoScaleReportV1 {
                version: crate::scale_report::SCALE_DEMO_SCALE_REPORT_V1_VERSION,
                workload_hash: [0u8; 32],
                doc_count: 0,
                query_count: 0,
                tie_pair: 0,
                seed: 0,
                frame_manifest_hash: [0u8; 32],
                docs_total: 0,
                rows_total: 0,
                frame_segments_total: 0,
                has_index: 0,
                index_snapshot_hash: [0u8; 32],
                index_sig_map_hash: [0u8; 32],
                index_segments_total: 0,
                has_prompts: 0,
                prompts_max_output_tokens: 0,
                prompts: crate::scale_report::HashListSummaryV1::empty(),
                has_evidence: 0,
                evidence_k: 0,
                evidence_max_bytes: 0,
                evidence: crate::scale_report::HashListSummaryV1::empty(),
                has_answers: 0,
                planner_max_plan_items: 0,
                realizer_max_evidence_items: 0,
                realizer_max_terms_per_row: 0,
                realizer_load_frame_rows: 0,
                answers: crate::scale_report::HashListSummaryV1::empty(),
                planner_hints: crate::scale_report::HashListSummaryV1::empty(),
                forecasts: crate::scale_report::HashListSummaryV1::empty(),
                markov_traces: crate::scale_report::HashListSummaryV1::empty(),
            },
        };
        let (plan, _) = prepare_exemplar_build_plan_v1(
            blake3_hash(b"build"),
            vec![ExemplarBuildInputV1::new(
                ExemplarSupportSourceKindV1::PromptPack,
                prompt_hash,
            )],
            &cfg,
        )
        .expect("prepare");
        let rows = mine_exemplar_rows_from_sources_v1(
            &plan,
            &[
                ExemplarSourceArtifactV1::GoldenPack {
                    source_hash: skipped_hash,
                    artifact: &golden,
                },
                ExemplarSourceArtifactV1::PromptPack {
                    source_hash: prompt_hash,
                    artifact: &prompt,
                },
            ],
        )
        .expect("mine");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].response_mode, ExemplarResponseModeV1::Compare);
        assert_eq!(rows[0].structure_kind, ExemplarStructureKindV1::Comparison);
        assert_eq!(rows[0].flags, EXROW_FLAG_HAS_COMPARISON);
        assert_eq!(rows[0].support_refs.len(), 1);
        assert_eq!(rows[0].support_refs[0].source_hash, prompt_hash);
    }
}
