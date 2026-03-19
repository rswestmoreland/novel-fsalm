// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Planner v1.
//!
//! Planner v1 converts an EvidenceBundleV1 into an AnswerPlanV1.
//!
//! v1 goals:
//! - deterministic
//! - bounded
//! - evidence-first (plan items reference evidence indices)
//!
//! v1 is intentionally simple: it emits one Bullet plan item per
//! evidence item, in evidence bundle order, up to a configured max.
//!
//! ASCII-only comments.

use crate::answer_plan::{AnswerPlanItemKindV1, AnswerPlanItemV1, AnswerPlanV1, AnswerPlanValidateError};
use crate::evidence_bundle::EvidenceBundleV1;
use crate::hash::Hash32;

/// Planner config schema version.
pub const PLANNER_CFG_V1_VERSION: u16 = 1;

/// Planner configuration (v1).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlannerCfgV1 {
    /// Schema version.
    pub version: u16,
    /// Maximum number of plan items to emit.
    pub max_plan_items: u32,
    /// Strength value assigned to each bullet in v1 (0..=1000).
    pub bullet_strength: u16,
}

impl PlannerCfgV1 {
    /// Default conservative config for v1.
    pub fn default_v1() -> Self {
        Self {
            version: PLANNER_CFG_V1_VERSION,
            max_plan_items: 64,
            bullet_strength: 500,
        }
    }

    /// Validate config invariants.
    pub fn validate(&self) -> Result<(), PlannerCfgError> {
        if self.version != PLANNER_CFG_V1_VERSION {
            return Err(PlannerCfgError::BadVersion);
        }
        if self.max_plan_items == 0 {
            return Err(PlannerCfgError::BadMaxPlanItems);
        }
        if self.max_plan_items > 16_384 {
            return Err(PlannerCfgError::BadMaxPlanItems);
        }
        if self.bullet_strength > 1000 {
            return Err(PlannerCfgError::BadBulletStrength);
        }
        Ok(())
    }
}

/// Errors returned by [`PlannerCfgV1::validate`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlannerCfgError {
    /// Version is not supported.
    BadVersion,
    /// max_plan_items must be 1..=16384.
    BadMaxPlanItems,
    /// bullet_strength must be 0..=1000.
    BadBulletStrength,
}

impl core::fmt::Display for PlannerCfgError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            PlannerCfgError::BadVersion => f.write_str("bad planner cfg version"),
            PlannerCfgError::BadMaxPlanItems => f.write_str("bad planner max_plan_items"),
            PlannerCfgError::BadBulletStrength => f.write_str("bad planner bullet_strength"),
        }
    }
}

/// Errors returned by planner v1.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlannerV1Error {
    /// Invalid config.
    Cfg(PlannerCfgError),
    /// Generated plan failed validation.
    Plan(AnswerPlanValidateError),
}

impl core::fmt::Display for PlannerV1Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            PlannerV1Error::Cfg(e) => {
                f.write_str("planner cfg invalid: ")?;
                core::fmt::Display::fmt(e, f)
            }
            PlannerV1Error::Plan(e) => {
                f.write_str("answer plan invalid: ")?;
                core::fmt::Display::fmt(e, f)
            }
        }
    }
}

/// Derive an AnswerPlanV1 from a canonical evidence bundle.
///
/// This does not re-score or rewrite evidence. It only selects and
/// arranges references into the evidence bundle.
pub fn plan_from_evidence_bundle_v1(
    evidence_bundle: &EvidenceBundleV1,
    evidence_bundle_id: Hash32,
    cfg: &PlannerCfgV1,
) -> Result<AnswerPlanV1, PlannerV1Error> {
    cfg.validate().map_err(PlannerV1Error::Cfg)?;

    let evidence_item_count = evidence_bundle.items.len() as u32;
    let mut plan = AnswerPlanV1::new(
        evidence_bundle.query_id,
        evidence_bundle.snapshot_id,
        evidence_bundle_id,
        evidence_item_count,
    );

    let n = core::cmp::min(cfg.max_plan_items as usize, evidence_bundle.items.len());
    for i in 0..n {
        let mut it = AnswerPlanItemV1::new(AnswerPlanItemKindV1::Bullet);
        it.strength = cfg.bullet_strength;
        it.evidence_item_ix.push(i as u32);
        plan.items.push(it);
    }

    plan.validate().map_err(PlannerV1Error::Plan)?;
    Ok(plan)
}


use crate::forecast::{
    ForecastIntentKindV1, ForecastIntentV1, ForecastQuestionV1, ForecastV1, FC_FLAG_HAS_PRAGMATICS,
};
use crate::frame::Id64;
use crate::planner_hints::{
    PlannerHintKindV1, PlannerHintItemV1, PlannerFollowupV1, PlannerHintsV1, PlannerHintsFlagsV1,
    PH_FLAG_PREFER_CAVEATS, PH_FLAG_PREFER_CLARIFY, PH_FLAG_PREFER_DIRECT, PH_FLAG_PREFER_STEPS,
};
use crate::pragmatics_frame::{
    PragmaticsFrameV1, INTENT_FLAG_HAS_CODE, INTENT_FLAG_HAS_COMPARE_TARGETS,
    INTENT_FLAG_HAS_CONSTRAINTS, INTENT_FLAG_HAS_FOCUS_EXAMPLE, INTENT_FLAG_HAS_FOCUS_STEPS,
    INTENT_FLAG_HAS_FOCUS_SUMMARY, INTENT_FLAG_HAS_MATH, INTENT_FLAG_HAS_QUESTION,
    INTENT_FLAG_HAS_REQUEST, INTENT_FLAG_IS_COMPARE_REQUEST, INTENT_FLAG_IS_EXPLAIN_REQUEST,
    INTENT_FLAG_IS_FOLLOW_UP, INTENT_FLAG_IS_LOGIC_PUZZLE, INTENT_FLAG_IS_PROBLEM_SOLVE,
    INTENT_FLAG_IS_RECOMMEND_REQUEST, INTENT_FLAG_IS_SUMMARIZE_REQUEST,
    INTENT_FLAG_SAFETY_SENSITIVE,
};

// Internal stable ids for planner hints.
const HINT_ID_CLARIFY: u64 = 1;
const HINT_ID_ASSUME_AND_ANSWER: u64 = 2;
const HINT_ID_STEPS: u64 = 3;
const HINT_ID_SUMMARY_FIRST: u64 = 4;
const HINT_ID_COMPARE: u64 = 5;

// Internal stable ids for followups.
const FOLLOWUP_ID_SCOPE: u64 = 100;
const FOLLOWUP_ID_CONSTRAINTS: u64 = 101;
const FOLLOWUP_ID_EXAMPLE: u64 = 102;
const FOLLOWUP_ID_NEXT_STEPS: u64 = 103;

// Problem-solving followups (general troubleshooting / retrospection).
const FOLLOWUP_ID_EXPECTED_ACTUAL: u64 = 104;
const FOLLOWUP_ID_REPRO: u64 = 105;
const FOLLOWUP_ID_WHAT_CHANGED: u64 = 106;
const FOLLOWUP_ID_ENV: u64 = 107;

// Logic puzzle followups.
const FOLLOWUP_ID_PUZZLE_VARS: u64 = 108;
const FOLLOWUP_ID_PUZZLE_UNIQUE: u64 = 109;
const FOLLOWUP_ID_RECOMMEND_PRIORITIES: u64 = 110;
const FOLLOWUP_ID_SUMMARIZE_FOCUS: u64 = 111;
const FOLLOWUP_ID_EXPLAIN_FOCUS: u64 = 112;
const FOLLOWUP_ID_COMPARE_CRITERIA: u64 = 113;
const FOLLOWUP_ID_SUMMARY_DEPTH: u64 = 114;
const FOLLOWUP_ID_EXPLAIN_STYLE: u64 = 115;

// Internal stable ids for forecast intents.
const FC_INTENT_ID_CLARIFY: u64 = 1;
const FC_INTENT_ID_EXAMPLE: u64 = 2;
const FC_INTENT_ID_MORE_DETAIL: u64 = 3;
const FC_INTENT_ID_COMPARE: u64 = 4;
const FC_INTENT_ID_NEXT_STEPS: u64 = 5;
const FC_INTENT_ID_RISKS: u64 = 6;
const FC_INTENT_ID_IMPLEMENTATION: u64 = 7;
const FC_INTENT_ID_VERIFY: u64 = 8;

// Internal stable ids for forecast questions.
const FC_QUESTION_ID_STYLE: u64 = 200;
const FC_QUESTION_ID_CONSTRAINTS: u64 = 201;
const FC_QUESTION_ID_EXAMPLE: u64 = 202;
const FC_QUESTION_ID_COMPARE: u64 = 203;


// Additional forecast questions for problem-solving and logic puzzles.
const FC_QUESTION_ID_EXPECTED_ACTUAL: u64 = 204;
const FC_QUESTION_ID_REPRO: u64 = 205;
const FC_QUESTION_ID_WHAT_CHANGED: u64 = 206;
const FC_QUESTION_ID_ENV: u64 = 207;
const FC_QUESTION_ID_PUZZLE_VARS: u64 = 208;
const FC_QUESTION_ID_PUZZLE_UNIQUE: u64 = 209;
const FC_QUESTION_ID_RECOMMEND_PRIORITIES: u64 = 210;
const FC_QUESTION_ID_SUMMARIZE_FOCUS: u64 = 211;
const FC_QUESTION_ID_EXPLAIN_FOCUS: u64 = 212;
const FC_QUESTION_ID_COMPARE_CRITERIA: u64 = 213;
const FC_QUESTION_ID_SUMMARY_DEPTH: u64 = 214;
const FC_QUESTION_ID_SUMMARY_EXAMPLE: u64 = 215;
const FC_QUESTION_ID_EXPLAIN_SUMMARY: u64 = 216;
const FC_QUESTION_ID_EXPLAIN_EXAMPLE: u64 = 217;

/// Planner output bundle including guidance artifacts.
///
/// This preserves evidence-first planning. Hints and forecast are advisory,
/// deterministic, and replay-friendly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannerOutputV1 {
    /// Answer plan (evidence-indexed).
    pub plan: AnswerPlanV1,
    /// Planner hints record.
    pub hints: PlannerHintsV1,
    /// Forecast record.
    pub forecast: ForecastV1,
}

fn hints_has_kind(h: &PlannerHintsV1, k: PlannerHintKindV1) -> bool {
    for it in h.hints.iter() {
        if it.kind == k {
            return true;
        }
    }
    false
}

fn sort_and_dedupe_hints(mut items: Vec<PlannerHintItemV1>) -> Vec<PlannerHintItemV1> {
    items.sort_by(|a, b| {
        let o = b.score.cmp(&a.score);
        if o != core::cmp::Ordering::Equal {
            return o;
        }
        let o = (a.kind as u8).cmp(&(b.kind as u8));
        if o != core::cmp::Ordering::Equal {
            return o;
        }
        a.hint_id.0.cmp(&b.hint_id.0)
    });

    // Uniqueness key: (kind, hint_id). Items are already sorted so the first seen wins.
    let mut seen: Vec<(u8, u64)> = Vec::new();
    let mut out: Vec<PlannerHintItemV1> = Vec::with_capacity(items.len());
    for it in items {
        let key = (it.kind as u8, it.hint_id.0);
        if seen.iter().any(|k| *k == key) {
            continue;
        }
        seen.push(key);
        out.push(it);
    }
    out
}

fn sort_and_dedupe_followups(mut items: Vec<PlannerFollowupV1>) -> Vec<PlannerFollowupV1> {
    items.sort_by(|a, b| {
        let o = b.score.cmp(&a.score);
        if o != core::cmp::Ordering::Equal {
            return o;
        }
        a.followup_id.0.cmp(&b.followup_id.0)
    });

    let mut seen: Vec<u64> = Vec::new();
    let mut out: Vec<PlannerFollowupV1> = Vec::with_capacity(items.len());
    for it in items {
        if seen.iter().any(|id| *id == it.followup_id.0) {
            continue;
        }
        seen.push(it.followup_id.0);
        out.push(it);
    }
    out
}

fn build_planner_hints_v1(
    query_id: Hash32,
    evidence: &EvidenceBundleV1,
    prag_opt: Option<&PragmaticsFrameV1>,
) -> PlannerHintsV1 {
    let mut flags: PlannerHintsFlagsV1 = 0;

    let mut has_question = false;
    let mut has_request = false;
    let mut has_constraints = false;
    let mut has_code = false;
    let mut has_math = false;
    let mut is_follow_up = false;
    let mut is_problem_solve = false;
    let mut is_logic_puzzle = false;
    let mut is_compare_request = false;
    let mut is_recommend_request = false;
    let mut is_summarize_request = false;
    let mut is_explain_request = false;
    let mut has_compare_targets = false;
    let mut has_focus_summary = false;
    let mut has_focus_steps = false;
    let mut has_focus_example = false;
    let mut safety_sensitive = false;

    if let Some(p) = prag_opt {
        let f = p.flags;
        has_question = (f & INTENT_FLAG_HAS_QUESTION) != 0;
        has_request = (f & INTENT_FLAG_HAS_REQUEST) != 0;
        has_constraints = (f & INTENT_FLAG_HAS_CONSTRAINTS) != 0;
        has_code = (f & INTENT_FLAG_HAS_CODE) != 0;
        has_math = (f & INTENT_FLAG_HAS_MATH) != 0;
        is_follow_up = (f & INTENT_FLAG_IS_FOLLOW_UP) != 0;
        is_problem_solve = (f & INTENT_FLAG_IS_PROBLEM_SOLVE) != 0;
        is_logic_puzzle = (f & INTENT_FLAG_IS_LOGIC_PUZZLE) != 0;
        is_compare_request = (f & INTENT_FLAG_IS_COMPARE_REQUEST) != 0;
        is_recommend_request = (f & INTENT_FLAG_IS_RECOMMEND_REQUEST) != 0;
        is_summarize_request = (f & INTENT_FLAG_IS_SUMMARIZE_REQUEST) != 0;
        is_explain_request = (f & INTENT_FLAG_IS_EXPLAIN_REQUEST) != 0;
        has_compare_targets = (f & INTENT_FLAG_HAS_COMPARE_TARGETS) != 0;
        has_focus_summary = (f & INTENT_FLAG_HAS_FOCUS_SUMMARY) != 0;
        has_focus_steps = (f & INTENT_FLAG_HAS_FOCUS_STEPS) != 0;
        has_focus_example = (f & INTENT_FLAG_HAS_FOCUS_EXAMPLE) != 0;
        safety_sensitive = (f & INTENT_FLAG_SAFETY_SENSITIVE) != 0;
    }

    let evidence_n = evidence.items.len();

    // Prefer clarify when we have very little evidence or a follow-up question.
    let prefer_clarify = evidence_n == 0 || (has_question && is_follow_up) || (has_question && evidence_n < 2);
    if prefer_clarify {
        flags |= PH_FLAG_PREFER_CLARIFY;
    }

    // Prefer steps when the message contains constraints, code, or math.
    // Also prefer steps for problem-solving and logic puzzles when detected by pragmatics.
    let prefer_steps = has_constraints
        || has_code
        || has_math
        || is_problem_solve
        || is_logic_puzzle
        || is_explain_request
        || has_focus_steps;
    if prefer_steps {
        flags |= PH_FLAG_PREFER_STEPS;
    }

    // Prefer direct when the message is short and question-like.
    let prefer_direct = (has_question && evidence_n <= 8)
        || is_recommend_request
        || is_summarize_request
        || has_focus_summary;
    if prefer_direct {
        flags |= PH_FLAG_PREFER_DIRECT;
    }

    // Prefer caveats for safety-sensitive content or low evidence.
    let prefer_caveats = safety_sensitive || evidence_n == 0;
    if prefer_caveats {
        flags |= PH_FLAG_PREFER_CAVEATS;
    }

    let mut hints: Vec<PlannerHintItemV1> = Vec::new();

    if prefer_clarify {
        hints.push(PlannerHintItemV1::new(
            PlannerHintKindV1::Clarify,
            Id64(HINT_ID_CLARIFY),
            100,
            1,
        ));
    } else {
        hints.push(PlannerHintItemV1::new(
            PlannerHintKindV1::AssumeAndAnswer,
            Id64(HINT_ID_ASSUME_AND_ANSWER),
            50,
            2,
        ));
    }

    if prefer_steps {
        hints.push(PlannerHintItemV1::new(
            PlannerHintKindV1::Steps,
            Id64(HINT_ID_STEPS),
            40,
            10,
        ));
    }

    // Summary-first when there is enough evidence to summarize,
    // or when the user explicitly asks for a summary.
    if evidence_n >= 3 || (is_summarize_request && evidence_n >= 1) {
        hints.push(PlannerHintItemV1::new(
            PlannerHintKindV1::SummaryFirst,
            Id64(HINT_ID_SUMMARY_FIRST),
            if is_summarize_request { 70 } else { 30 },
            20,
        ));
    }

    // Compare hint when evidence spans multiple segments,
    // or when the user explicitly asks for a comparison.
    let mut evidence_compare_ok = false;
    if evidence_n >= 2 {
        let mut segs: Vec<Hash32> = Vec::new();
        for it in evidence.items.iter() {
            if let crate::evidence_bundle::EvidenceItemDataV1::Frame(fr) = &it.data {
                segs.push(fr.segment_id);
            }
        }
        segs.sort();
        segs.dedup();
        evidence_compare_ok = segs.len() >= 2;
    }
    if evidence_compare_ok || is_compare_request {
        hints.push(PlannerHintItemV1::new(
            PlannerHintKindV1::Compare,
            Id64(HINT_ID_COMPARE),
            if is_compare_request { 65 } else { 25 },
            21,
        ));
    }

    let hints = sort_and_dedupe_hints(hints);

    let mut followups: Vec<PlannerFollowupV1> = Vec::new();

    if prefer_clarify {
        followups.push(PlannerFollowupV1::new(
            Id64(FOLLOWUP_ID_SCOPE),
            100,
            "What should I assume about your constraints or environment?".to_string(),
            1,
        ));
    }

    if has_constraints {
        followups.push(PlannerFollowupV1::new(
            Id64(FOLLOWUP_ID_CONSTRAINTS),
            70,
            "Which constraint is most important if there is a tradeoff?".to_string(),
            10,
        ));
    }

    if has_request {
        followups.push(PlannerFollowupV1::new(
            Id64(FOLLOWUP_ID_NEXT_STEPS),
            60,
            "Do you want concrete next steps or a high-level overview?".to_string(),
            30,
        ));
    }

    if is_recommend_request {
        followups.push(PlannerFollowupV1::new(
            Id64(FOLLOWUP_ID_RECOMMEND_PRIORITIES),
            72,
            "What matters most for the recommendation (cost, speed, simplicity, or risk)?".to_string(),
            31,
        ));
    }

    if is_compare_request && has_compare_targets {
        followups.push(PlannerFollowupV1::new(
            Id64(FOLLOWUP_ID_COMPARE_CRITERIA),
            74,
            "Which comparison criteria matter most?".to_string(),
            34,
        ));
    }

    if is_summarize_request {
        let (followup_id, score, text, rationale_code) = if has_focus_steps {
            (
                FOLLOWUP_ID_SUMMARY_DEPTH,
                72,
                "Should I give the short summary first or the step-by-step breakdown?".to_string(),
                35,
            )
        } else if has_focus_example {
            (
                FOLLOWUP_ID_SUMMARIZE_FOCUS,
                70,
                "Should I summarize first or start with an example?".to_string(),
                36,
            )
        } else {
            (
                FOLLOWUP_ID_SUMMARIZE_FOCUS,
                68,
                "Which part should I summarize first?".to_string(),
                32,
            )
        };
        followups.push(PlannerFollowupV1::new(
            Id64(followup_id),
            score,
            text,
            rationale_code,
        ));
    }

    if is_explain_request {
        let (followup_id, score, text, rationale_code) = if has_focus_summary {
            (
                FOLLOWUP_ID_EXPLAIN_STYLE,
                74,
                "Should I start with the short summary or go straight into the walkthrough?".to_string(),
                37,
            )
        } else if has_focus_example {
            (
                FOLLOWUP_ID_EXPLAIN_FOCUS,
                72,
                "Should I explain it directly or start with an example?".to_string(),
                38,
            )
        } else {
            (
                FOLLOWUP_ID_EXPLAIN_FOCUS,
                70,
                "Which part should I explain first?".to_string(),
                33,
            )
        };
        followups.push(PlannerFollowupV1::new(
            Id64(followup_id),
            score,
            text,
            rationale_code,
        ));
    }

    if is_problem_solve {
        followups.push(PlannerFollowupV1::new(
            Id64(FOLLOWUP_ID_EXPECTED_ACTUAL),
            75,
            "What did you expect to happen, and what actually happened?".to_string(),
            50,
        ));
        followups.push(PlannerFollowupV1::new(
            Id64(FOLLOWUP_ID_REPRO),
            70,
            "Can you share a minimal reproduction (exact command, input, and output)?".to_string(),
            51,
        ));
        followups.push(PlannerFollowupV1::new(
            Id64(FOLLOWUP_ID_WHAT_CHANGED),
            65,
            "What changed most recently before this started happening?".to_string(),
            52,
        ));
        followups.push(PlannerFollowupV1::new(
            Id64(FOLLOWUP_ID_ENV),
            60,
            "What environment details matter (OS, versions, config, and constraints)?".to_string(),
            53,
        ));
    }

    if is_logic_puzzle {
        followups.push(PlannerFollowupV1::new(
            Id64(FOLLOWUP_ID_PUZZLE_VARS),
            80,
            "Can you list the variables and allowed values (domains) in a structured form?".to_string(),
            60,
        ));
        followups.push(PlannerFollowupV1::new(
            Id64(FOLLOWUP_ID_PUZZLE_UNIQUE),
            75,
            "Is there exactly one solution, or are multiple solutions allowed?".to_string(),
            61,
        ));
    }

    followups.push(PlannerFollowupV1::new(
        Id64(FOLLOWUP_ID_EXAMPLE),
        40,
        "Would an example help?".to_string(),
        40,
    ));

    let followups = sort_and_dedupe_followups(followups);

    let mut out = PlannerHintsV1 {
        version: crate::planner_hints::PLANNER_HINTS_V1_VERSION,
        query_id,
        flags,
        hints,
        followups,
    };

    // Enforce canonical caps deterministically.
    if out.hints.len() > crate::planner_hints::PLANNER_HINTS_V1_MAX_HINTS {
        out.hints.truncate(crate::planner_hints::PLANNER_HINTS_V1_MAX_HINTS);
    }
    if out.followups.len() > crate::planner_hints::PLANNER_HINTS_V1_MAX_FOLLOWUPS {
        out.followups
            .truncate(crate::planner_hints::PLANNER_HINTS_V1_MAX_FOLLOWUPS);
    }

    if out.validate().is_err() {
        // Fallback to an empty-but-valid record.
        out.flags = 0;
        out.hints.clear();
        out.followups.clear();
    }

    out
}

fn sort_and_dedupe_forecast_intents(mut items: Vec<ForecastIntentV1>) -> Vec<ForecastIntentV1> {
    items.sort_by(|a, b| {
        let o = b.score.cmp(&a.score);
        if o != core::cmp::Ordering::Equal {
            return o;
        }
        let o = (a.kind as u8).cmp(&(b.kind as u8));
        if o != core::cmp::Ordering::Equal {
            return o;
        }
        a.intent_id.0.cmp(&b.intent_id.0)
    });

    let mut seen: Vec<(u8, u64)> = Vec::new();
    let mut out: Vec<ForecastIntentV1> = Vec::with_capacity(items.len());
    for it in items {
        let key = (it.kind as u8, it.intent_id.0);
        if seen.iter().any(|k| *k == key) {
            continue;
        }
        seen.push(key);
        out.push(it);
    }
    out
}

fn sort_and_dedupe_forecast_questions(mut items: Vec<ForecastQuestionV1>) -> Vec<ForecastQuestionV1> {
    items.sort_by(|a, b| {
        let o = b.score.cmp(&a.score);
        if o != core::cmp::Ordering::Equal {
            return o;
        }
        a.question_id.0.cmp(&b.question_id.0)
    });

    let mut seen: Vec<u64> = Vec::new();
    let mut out: Vec<ForecastQuestionV1> = Vec::with_capacity(items.len());
    for it in items {
        if seen.iter().any(|id| *id == it.question_id.0) {
            continue;
        }
        seen.push(it.question_id.0);
        out.push(it);
    }
    out
}

fn build_forecast_v1(
    query_id: Hash32,
    prag_opt: Option<&PragmaticsFrameV1>,
    hints: &PlannerHintsV1,
) -> ForecastV1 {
    let mut flags: u32 = 0;
    if prag_opt.is_some() {
        flags |= FC_FLAG_HAS_PRAGMATICS;
    }

    let mut intents: Vec<ForecastIntentV1> = Vec::new();

    let prefer_clarify = (hints.flags & PH_FLAG_PREFER_CLARIFY) != 0;
    let prefer_steps = (hints.flags & PH_FLAG_PREFER_STEPS) != 0;
    let prefer_caveats = (hints.flags & PH_FLAG_PREFER_CAVEATS) != 0;

    let mut is_problem_solve = false;
    let mut is_logic_puzzle = false;
    let mut is_compare_request = false;
    let mut is_recommend_request = false;
    let mut is_summarize_request = false;
    let mut is_explain_request = false;
    let mut has_compare_targets = false;
    let mut has_focus_summary = false;
    let mut has_focus_steps = false;
    let mut has_focus_example = false;
    if let Some(p) = prag_opt {
        let f = p.flags;
        is_problem_solve = (f & INTENT_FLAG_IS_PROBLEM_SOLVE) != 0;
        is_logic_puzzle = (f & INTENT_FLAG_IS_LOGIC_PUZZLE) != 0;
        is_compare_request = (f & INTENT_FLAG_IS_COMPARE_REQUEST) != 0;
        is_recommend_request = (f & INTENT_FLAG_IS_RECOMMEND_REQUEST) != 0;
        is_summarize_request = (f & INTENT_FLAG_IS_SUMMARIZE_REQUEST) != 0;
        is_explain_request = (f & INTENT_FLAG_IS_EXPLAIN_REQUEST) != 0;
        has_compare_targets = (f & INTENT_FLAG_HAS_COMPARE_TARGETS) != 0;
        has_focus_summary = (f & INTENT_FLAG_HAS_FOCUS_SUMMARY) != 0;
        has_focus_steps = (f & INTENT_FLAG_HAS_FOCUS_STEPS) != 0;
        has_focus_example = (f & INTENT_FLAG_HAS_FOCUS_EXAMPLE) != 0;
    }

    if prefer_clarify {
        intents.push(ForecastIntentV1::new(
            ForecastIntentKindV1::Clarify,
            Id64(FC_INTENT_ID_CLARIFY),
            100,
            1,
        ));
    }

    intents.push(ForecastIntentV1::new(
        ForecastIntentKindV1::Example,
        Id64(FC_INTENT_ID_EXAMPLE),
        80,
        2,
    ));

    intents.push(ForecastIntentV1::new(
        ForecastIntentKindV1::MoreDetail,
        Id64(FC_INTENT_ID_MORE_DETAIL),
        60,
        3,
    ));

    if is_compare_request {
        intents.push(ForecastIntentV1::new(
            ForecastIntentKindV1::Compare,
            Id64(FC_INTENT_ID_COMPARE),
            80,
            4,
        ));
    }

    if is_recommend_request {
        intents.push(ForecastIntentV1::new(
            ForecastIntentKindV1::NextSteps,
            Id64(FC_INTENT_ID_NEXT_STEPS),
            72,
            5,
        ));
    }

    if is_summarize_request {
        intents.push(ForecastIntentV1::new(
            ForecastIntentKindV1::MoreDetail,
            Id64(FC_INTENT_ID_MORE_DETAIL),
            68,
            5,
        ));
    }

    if is_explain_request {
        intents.push(ForecastIntentV1::new(
            ForecastIntentKindV1::MoreDetail,
            Id64(FC_INTENT_ID_MORE_DETAIL),
            72,
            5,
        ));
    }

    if hints_has_kind(hints, PlannerHintKindV1::Compare) {
        intents.push(ForecastIntentV1::new(
            ForecastIntentKindV1::Compare,
            Id64(FC_INTENT_ID_COMPARE),
            55,
            4,
        ));
    }

    if let Some(p) = prag_opt {
        if (p.flags & INTENT_FLAG_HAS_REQUEST) != 0 {
            intents.push(ForecastIntentV1::new(
                ForecastIntentKindV1::NextSteps,
                Id64(FC_INTENT_ID_NEXT_STEPS),
                50,
                5,
            ));
        }
        if (p.flags & INTENT_FLAG_IS_PROBLEM_SOLVE) != 0 {
            intents.push(ForecastIntentV1::new(
                ForecastIntentKindV1::VerifyOrTroubleshoot,
                Id64(FC_INTENT_ID_VERIFY),
                55,
                6,
            ));
            intents.push(ForecastIntentV1::new(
                ForecastIntentKindV1::MoreDetail,
                Id64(FC_INTENT_ID_MORE_DETAIL),
                50,
                7,
            ));
        }
        if (p.flags & INTENT_FLAG_IS_LOGIC_PUZZLE) != 0 {
            intents.push(ForecastIntentV1::new(
                ForecastIntentKindV1::Clarify,
                Id64(FC_INTENT_ID_CLARIFY),
                60,
                6,
            ));
        }
        if (p.flags & INTENT_FLAG_HAS_CODE) != 0 {
            intents.push(ForecastIntentV1::new(
                ForecastIntentKindV1::Implementation,
                Id64(FC_INTENT_ID_IMPLEMENTATION),
                45,
                7,
            ));
            intents.push(ForecastIntentV1::new(
                ForecastIntentKindV1::VerifyOrTroubleshoot,
                Id64(FC_INTENT_ID_VERIFY),
                40,
                8,
            ));
        }
        if (p.flags & INTENT_FLAG_SAFETY_SENSITIVE) != 0 {
            intents.push(ForecastIntentV1::new(
                ForecastIntentKindV1::Risks,
                Id64(FC_INTENT_ID_RISKS),
                70,
                6,
            ));
        }
    }

    if prefer_steps {
        intents.push(ForecastIntentV1::new(
            ForecastIntentKindV1::Implementation,
            Id64(FC_INTENT_ID_IMPLEMENTATION),
            35,
            7,
        ));
    }

    if prefer_caveats {
        intents.push(ForecastIntentV1::new(
            ForecastIntentKindV1::Risks,
            Id64(FC_INTENT_ID_RISKS),
            65,
            6,
        ));
    }

    let intents = sort_and_dedupe_forecast_intents(intents);

    let mut questions: Vec<ForecastQuestionV1> = Vec::new();

    questions.push(ForecastQuestionV1::new(
        Id64(FC_QUESTION_ID_STYLE),
        50,
        "Do you want a short answer or a detailed walkthrough?".to_string(),
        1,
    ));

    if prefer_clarify {
        if is_logic_puzzle {
            questions.push(ForecastQuestionV1::new(
                Id64(FC_QUESTION_ID_PUZZLE_VARS),
                95,
                "Can you list the variables and allowed values (domains)?".to_string(),
                2,
            ));
            questions.push(ForecastQuestionV1::new(
                Id64(FC_QUESTION_ID_PUZZLE_UNIQUE),
                90,
                "Should I assume there is exactly one solution?".to_string(),
                3,
            ));
        } else if is_problem_solve {
            questions.push(ForecastQuestionV1::new(
                Id64(FC_QUESTION_ID_EXPECTED_ACTUAL),
                95,
                "What did you expect to happen, and what actually happened?".to_string(),
                2,
            ));
            questions.push(ForecastQuestionV1::new(
                Id64(FC_QUESTION_ID_REPRO),
                90,
                "Can you share a minimal reproduction (command, input, and output)?".to_string(),
                3,
            ));
            questions.push(ForecastQuestionV1::new(
                Id64(FC_QUESTION_ID_WHAT_CHANGED),
                85,
                "What changed most recently before this started happening?".to_string(),
                4,
            ));
            questions.push(ForecastQuestionV1::new(
                Id64(FC_QUESTION_ID_ENV),
                80,
                "What environment details matter (OS, versions, config, constraints)?".to_string(),
                5,
            ));
            questions.push(ForecastQuestionV1::new(
                Id64(FC_QUESTION_ID_CONSTRAINTS),
                70,
                "What constraints should I assume?".to_string(),
                6,
            ));
        } else {
            let generic_constraints_score = if is_compare_request || is_recommend_request || is_summarize_request || is_explain_request {
                78
            } else {
                90
            };
            questions.push(ForecastQuestionV1::new(
                Id64(FC_QUESTION_ID_CONSTRAINTS),
                generic_constraints_score,
                "What constraints should I assume?".to_string(),
                2,
            ));
        }
    }

    if prefer_clarify && is_recommend_request {
        questions.push(ForecastQuestionV1::new(
            Id64(FC_QUESTION_ID_RECOMMEND_PRIORITIES),
            88,
            "What matters most for the recommendation (cost, speed, simplicity, or risk)?".to_string(),
            7,
        ));
    }

    if prefer_clarify && is_summarize_request {
        let (question_id, score, text, rationale_code) = if has_focus_steps {
            (
                FC_QUESTION_ID_SUMMARY_DEPTH,
                88,
                "Should I give the short summary first or the step-by-step breakdown?".to_string(),
                8,
            )
        } else if has_focus_example {
            (
                FC_QUESTION_ID_SUMMARY_EXAMPLE,
                86,
                "Should I summarize first or start with an example?".to_string(),
                9,
            )
        } else {
            (
                FC_QUESTION_ID_SUMMARIZE_FOCUS,
                84,
                "Which part should I summarize first?".to_string(),
                8,
            )
        };
        questions.push(ForecastQuestionV1::new(
            Id64(question_id),
            score,
            text,
            rationale_code,
        ));
    }

    if prefer_clarify && is_explain_request {
        let (question_id, score, text, rationale_code) = if has_focus_summary {
            (
                FC_QUESTION_ID_EXPLAIN_SUMMARY,
                90,
                "Do you want the short summary first or the full walkthrough?".to_string(),
                10,
            )
        } else if has_focus_example {
            (
                FC_QUESTION_ID_EXPLAIN_EXAMPLE,
                88,
                "Should I explain it directly or start with an example?".to_string(),
                11,
            )
        } else {
            (
                FC_QUESTION_ID_EXPLAIN_FOCUS,
                86,
                "Which part should I explain first?".to_string(),
                9,
            )
        };
        questions.push(ForecastQuestionV1::new(
            Id64(question_id),
            score,
            text,
            rationale_code,
        ));
    }

    questions.push(ForecastQuestionV1::new(
        Id64(FC_QUESTION_ID_EXAMPLE),
        60,
        "Would you like an example?".to_string(),
        3,
    ));

    if hints_has_kind(hints, PlannerHintKindV1::Compare) {
        let (question_id, score, text, rationale_code) = if is_compare_request && has_compare_targets {
            (
                FC_QUESTION_ID_COMPARE_CRITERIA,
                94,
                "Which comparison criteria matter most?".to_string(),
                12,
            )
        } else {
            (
                FC_QUESTION_ID_COMPARE,
                if is_compare_request { 92 } else { 55 },
                "Which options should I compare?".to_string(),
                4,
            )
        };
        questions.push(ForecastQuestionV1::new(
            Id64(question_id),
            score,
            text,
            rationale_code,
        ));
    }

    let questions = sort_and_dedupe_forecast_questions(questions);

    let mut out = ForecastV1 {
        version: crate::forecast::FORECAST_V1_VERSION,
        query_id,
        flags,
        horizon_turns: 1,
        intents,
        questions,
    };

    if out.intents.len() > crate::forecast::FORECAST_V1_MAX_INTENTS {
        out.intents.truncate(crate::forecast::FORECAST_V1_MAX_INTENTS);
    }
    if out.questions.len() > crate::forecast::FORECAST_V1_MAX_QUESTIONS {
        out.questions.truncate(crate::forecast::FORECAST_V1_MAX_QUESTIONS);
    }

    if out.validate().is_err() {
        out.flags = 0;
        out.intents.clear();
        out.questions.clear();
    }

    out
}

fn plan_from_bundle_guided(
    evidence_bundle: &EvidenceBundleV1,
    evidence_bundle_id: Hash32,
    cfg: &PlannerCfgV1,
    hints: &PlannerHintsV1,
) -> Result<AnswerPlanV1, PlannerV1Error> {
    let evidence_item_count = evidence_bundle.items.len() as u32;

    let mut plan = AnswerPlanV1::new(
        evidence_bundle.query_id,
        evidence_bundle.snapshot_id,
        evidence_bundle_id,
        evidence_item_count,
    );

    let prefer_clarify = (hints.flags & PH_FLAG_PREFER_CLARIFY) != 0;
    let prefer_steps = (hints.flags & PH_FLAG_PREFER_STEPS) != 0;
    let prefer_caveats = (hints.flags & PH_FLAG_PREFER_CAVEATS) != 0;
    let summary_first = hints_has_kind(hints, PlannerHintKindV1::SummaryFirst);

    let mut remaining = cfg.max_plan_items as usize;

    if remaining == 0 {
        return Err(PlannerV1Error::Cfg(PlannerCfgError::BadMaxPlanItems));
    }

    if prefer_clarify {
        // When we prefer clarify, emit a minimal plan that still supports structured output
        // when steps are requested by hints (constraints/code/math/problem-solve/puzzle).
        let mut it = AnswerPlanItemV1::new(AnswerPlanItemKindV1::Summary);
        if !evidence_bundle.items.is_empty() {
            it.evidence_item_ix.push(0);
        }
        plan.items.push(it);

        let remaining_after_summary = remaining.saturating_sub(1);
        if prefer_steps && remaining_after_summary > 0 {
            let n = core::cmp::min(remaining_after_summary, evidence_bundle.items.len());
            for i in 0..n {
                let mut st = AnswerPlanItemV1::new(AnswerPlanItemKindV1::Step);
                st.strength = cfg.bullet_strength;
                st.evidence_item_ix.push(i as u32);
                plan.items.push(st);
            }
        }

        plan.validate().map_err(PlannerV1Error::Plan)?;
        return Ok(plan);
    }

    if summary_first && remaining > 0 {
        let mut it = AnswerPlanItemV1::new(AnswerPlanItemKindV1::Summary);
        if !evidence_bundle.items.is_empty() {
            it.evidence_item_ix.push(0);
        }
        plan.items.push(it);
        remaining = remaining.saturating_sub(1);
    }

    if remaining > 0 {
        let n = core::cmp::min(remaining, evidence_bundle.items.len());
        for i in 0..n {
            let kind = if prefer_steps {
                AnswerPlanItemKindV1::Step
            } else {
                AnswerPlanItemKindV1::Bullet
            };
            let mut it = AnswerPlanItemV1::new(kind);
            it.strength = cfg.bullet_strength;
            it.evidence_item_ix.push(i as u32);
            plan.items.push(it);
        }
        remaining = remaining.saturating_sub(n);
    }

    if prefer_caveats && remaining > 0 {
        let it = AnswerPlanItemV1::new(AnswerPlanItemKindV1::Caveat);
        plan.items.push(it);
    }

    plan.validate().map_err(PlannerV1Error::Plan)?;
    Ok(plan)
}

/// Derive an AnswerPlanV1 plus advisory guidance artifacts from a canonical evidence bundle.
///
/// adds replay-friendly guidance outputs:
/// - PlannerHintsV1 (how to structure the interaction)
/// - ForecastV1 (top-k predicted next intents/questions)
///
/// The returned plan remains evidence-first and index-based.
pub fn plan_from_evidence_bundle_v1_with_guidance(
    evidence_bundle: &EvidenceBundleV1,
    evidence_bundle_id: Hash32,
    cfg: &PlannerCfgV1,
    prag_opt: Option<&PragmaticsFrameV1>,
) -> Result<PlannerOutputV1, PlannerV1Error> {
    cfg.validate().map_err(PlannerV1Error::Cfg)?;

    let hints = build_planner_hints_v1(evidence_bundle.query_id, evidence_bundle, prag_opt);
    let forecast = build_forecast_v1(evidence_bundle.query_id, prag_opt, &hints);
    let plan = plan_from_bundle_guided(evidence_bundle, evidence_bundle_id, cfg, &hints)?;

    Ok(PlannerOutputV1 {
        plan,
        hints,
        forecast,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evidence_bundle::{
        EvidenceBundleV1, EvidenceItemDataV1, EvidenceItemV1, EvidenceLimitsV1, FrameRowRefV1,
    };
    use crate::hash::blake3_hash;

    fn sample_bundle() -> EvidenceBundleV1 {
        let q = blake3_hash(b"query");
        let s = blake3_hash(b"snapshot");
        let limits = EvidenceLimitsV1 {
            segments_touched: 1,
            max_items: 10,
            max_bytes: 0,
        };
        let mut b = EvidenceBundleV1::new(q, s, limits, 1);

        for i in 0..3u32 {
            let seg = blake3_hash(&[b's', i as u8]);
            let r = FrameRowRefV1 {
                segment_id: seg,
                row_ix: i,
                sketch: None,
            };
            b.items.push(EvidenceItemV1 {
                score: 100 - (i as i64),
                data: EvidenceItemDataV1::Frame(r),
            });
        }

        b
    }

    #[test]
    fn plan_from_bundle_basic_and_deterministic() {
        let b = sample_bundle();
        let bundle_id = blake3_hash(b"bundle_id");
        let mut cfg = PlannerCfgV1::default_v1();
        cfg.max_plan_items = 2;

        let p1 = plan_from_evidence_bundle_v1(&b, bundle_id, &cfg).unwrap();
        let p2 = plan_from_evidence_bundle_v1(&b, bundle_id, &cfg).unwrap();
        assert_eq!(p1, p2);

        assert_eq!(p1.query_id, b.query_id);
        assert_eq!(p1.snapshot_id, b.snapshot_id);
        assert_eq!(p1.evidence_bundle_id, bundle_id);
        assert_eq!(p1.evidence_item_count, 3);
        assert_eq!(p1.items.len(), 2);

        assert_eq!(p1.items[0].kind, AnswerPlanItemKindV1::Bullet);
        assert_eq!(p1.items[0].strength, 500);
        assert_eq!(p1.items[0].evidence_item_ix, vec![0]);

        assert_eq!(p1.items[1].evidence_item_ix, vec![1]);

        assert!(p1.validate().is_ok());
    }

    #[test]
    fn cfg_validate_rejects_zero_max_items() {
        let mut cfg = PlannerCfgV1::default_v1();
        cfg.max_plan_items = 0;
        assert_eq!(cfg.validate(), Err(PlannerCfgError::BadMaxPlanItems));
    }


    fn sample_prag(flags: crate::pragmatics_frame::IntentFlagsV1) -> crate::pragmatics_frame::PragmaticsFrameV1 {
        use crate::frame::Id64;
        use crate::pragmatics_frame::{PragmaticsFrameV1, PRAGMATICS_FRAME_V1_VERSION, RhetoricModeV1};

        PragmaticsFrameV1 {
            version: PRAGMATICS_FRAME_V1_VERSION,
            source_id: Id64(1),
            msg_ix: 0,
            byte_len: 12,
            ascii_only: 1,
            temperature: 0,
            valence: 0,
            arousal: 0,
            politeness: 500,
            formality: 500,
            directness: 500,
            empathy_need: 0,
            mode: RhetoricModeV1::Ask,
            flags,
            exclamations: 0,
            questions: 1,
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

    fn sample_bundle_n(n: u32) -> EvidenceBundleV1 {
        let q = blake3_hash(b"query");
        let s = blake3_hash(b"snapshot");
        let limits = EvidenceLimitsV1 {
            segments_touched: 1,
            max_items: 100,
            max_bytes: 0,
        };
        let mut b = EvidenceBundleV1::new(q, s, limits, 1);

        for i in 0..n {
            let seg = blake3_hash(&[b's', i as u8]);
            let r = FrameRowRefV1 {
                segment_id: seg,
                row_ix: i,
                sketch: None,
            };
            b.items.push(EvidenceItemV1 {
                score: 100 - (i as i64),
                data: EvidenceItemDataV1::Frame(r),
            });
        }

        b
    }

    #[test]
    fn plan_with_guidance_is_deterministic() {
        let b = sample_bundle_n(4);
        let bundle_id = blake3_hash(b"bundle_id");
        let cfg = PlannerCfgV1::default_v1();
        let prag = sample_prag(crate::pragmatics_frame::INTENT_FLAG_HAS_REQUEST);

        let o1 = plan_from_evidence_bundle_v1_with_guidance(&b, bundle_id, &cfg, Some(&prag)).unwrap();
        let o2 = plan_from_evidence_bundle_v1_with_guidance(&b, bundle_id, &cfg, Some(&prag)).unwrap();
        assert_eq!(o1, o2);
        assert!(o1.hints.validate().is_ok());
        assert!(o1.forecast.validate().is_ok());
        assert!(o1.plan.validate().is_ok());
    }

    #[test]
    fn plan_with_guidance_prefers_steps_when_constraints_present() {
        let b = sample_bundle_n(3);
        let bundle_id = blake3_hash(b"bundle_id");
        let cfg = PlannerCfgV1::default_v1();
        let prag = sample_prag(crate::pragmatics_frame::INTENT_FLAG_HAS_CONSTRAINTS);

        let o = plan_from_evidence_bundle_v1_with_guidance(&b, bundle_id, &cfg, Some(&prag)).unwrap();
        assert_ne!(o.hints.flags & crate::planner_hints::PH_FLAG_PREFER_STEPS, 0);

        // If summary-first is present, steps will begin after the summary.
        let mut saw_step = false;
        for it in o.plan.items.iter() {
            if it.kind == AnswerPlanItemKindV1::Step {
                saw_step = true;
                break;
            }
        }
        assert!(saw_step);
    }

    #[test]
    fn plan_with_guidance_prefers_steps_when_problem_solve_present() {
        let b = sample_bundle_n(3);
        let bundle_id = blake3_hash(b"bundle_id");
        let cfg = PlannerCfgV1::default_v1();
        let prag = sample_prag(crate::pragmatics_frame::INTENT_FLAG_IS_PROBLEM_SOLVE | crate::pragmatics_frame::INTENT_FLAG_HAS_REQUEST);

        let o = plan_from_evidence_bundle_v1_with_guidance(&b, bundle_id, &cfg, Some(&prag)).unwrap();
        assert_ne!(o.hints.flags & crate::planner_hints::PH_FLAG_PREFER_STEPS, 0);
        let mut saw_step = false;
        for it in o.plan.items.iter() {
            if it.kind == AnswerPlanItemKindV1::Step {
                saw_step = true;
                break;
            }
        }
        assert!(saw_step);
    }

    #[test]
    fn forecast_questions_prioritize_problem_solve_when_clarify() {
        let b = sample_bundle_n(0);
        let prag = sample_prag(crate::pragmatics_frame::INTENT_FLAG_IS_PROBLEM_SOLVE);
        let hints = build_planner_hints_v1(b.query_id, &b, Some(&prag));
        let fc = build_forecast_v1(b.query_id, Some(&prag), &hints);
        assert!(!fc.questions.is_empty());
        let top = fc.questions[0].text.to_lowercase();
        assert!(top.contains("expect") && top.contains("actual"), "top={}", fc.questions[0].text);
    }

    #[test]
    fn forecast_questions_prioritize_logic_puzzle_when_clarify() {
        let b = sample_bundle_n(0);
        let prag = sample_prag(crate::pragmatics_frame::INTENT_FLAG_IS_LOGIC_PUZZLE);
        let hints = build_planner_hints_v1(b.query_id, &b, Some(&prag));
        let fc = build_forecast_v1(b.query_id, Some(&prag), &hints);
        assert!(!fc.questions.is_empty());
        let top = fc.questions[0].text.to_lowercase();
        assert!(top.contains("variables") || top.contains("domains"), "top={}", fc.questions[0].text);
    }

    #[test]
    fn plan_with_guidance_adds_compare_hint_for_compare_request() {
        let b = sample_bundle_n(1);
        let bundle_id = blake3_hash(b"bundle_id");
        let cfg = PlannerCfgV1::default_v1();
        let prag = sample_prag(crate::pragmatics_frame::INTENT_FLAG_IS_COMPARE_REQUEST | crate::pragmatics_frame::INTENT_FLAG_HAS_REQUEST);

        let o = plan_from_evidence_bundle_v1_with_guidance(&b, bundle_id, &cfg, Some(&prag)).unwrap();
        assert!(o.hints.hints.iter().any(|h| h.kind == crate::planner_hints::PlannerHintKindV1::Compare));
        assert!(o.forecast.intents.iter().any(|it| it.kind == crate::forecast::ForecastIntentKindV1::Compare));
    }

    #[test]
    fn plan_with_guidance_adds_summary_first_for_summarize_request() {
        let b = sample_bundle_n(1);
        let bundle_id = blake3_hash(b"bundle_id");
        let cfg = PlannerCfgV1::default_v1();
        let prag = sample_prag(crate::pragmatics_frame::INTENT_FLAG_IS_SUMMARIZE_REQUEST | crate::pragmatics_frame::INTENT_FLAG_HAS_REQUEST);

        let o = plan_from_evidence_bundle_v1_with_guidance(&b, bundle_id, &cfg, Some(&prag)).unwrap();
        assert!(o.hints.hints.iter().any(|h| h.kind == crate::planner_hints::PlannerHintKindV1::SummaryFirst));
        assert_eq!(o.plan.items[0].kind, AnswerPlanItemKindV1::Summary);
    }

    #[test]
    fn forecast_questions_prioritize_recommend_request_when_clarify() {
        let b = sample_bundle_n(0);
        let prag = sample_prag(crate::pragmatics_frame::INTENT_FLAG_IS_RECOMMEND_REQUEST | crate::pragmatics_frame::INTENT_FLAG_HAS_REQUEST);
        let hints = build_planner_hints_v1(b.query_id, &b, Some(&prag));
        let fc = build_forecast_v1(b.query_id, Some(&prag), &hints);
        assert!(fc.questions.iter().any(|q| q.text.to_lowercase().contains("recommendation")));
        assert!(fc.intents.iter().any(|it| it.kind == crate::forecast::ForecastIntentKindV1::NextSteps));
    }

    #[test]
    fn plan_with_guidance_prefers_steps_for_explain_request() {
        let b = sample_bundle_n(2);
        let bundle_id = blake3_hash(b"bundle_id");
        let cfg = PlannerCfgV1::default_v1();
        let prag = sample_prag(crate::pragmatics_frame::INTENT_FLAG_IS_EXPLAIN_REQUEST | crate::pragmatics_frame::INTENT_FLAG_HAS_REQUEST);

        let o = plan_from_evidence_bundle_v1_with_guidance(&b, bundle_id, &cfg, Some(&prag)).unwrap();
        assert_ne!(o.hints.flags & crate::planner_hints::PH_FLAG_PREFER_STEPS, 0);
        assert!(o.forecast.intents.iter().any(|it| it.kind == crate::forecast::ForecastIntentKindV1::MoreDetail));
    }

    #[test]
    fn focus_steps_sets_prefer_steps_in_hints() {
        let b = sample_bundle_n(1);
        let prag = sample_prag(
            crate::pragmatics_frame::INTENT_FLAG_HAS_REQUEST
                | crate::pragmatics_frame::INTENT_FLAG_HAS_FOCUS_STEPS,
        );
        let hints = build_planner_hints_v1(b.query_id, &b, Some(&prag));
        assert_ne!(hints.flags & crate::planner_hints::PH_FLAG_PREFER_STEPS, 0);
    }

    #[test]
    fn forecast_compare_clarifier_uses_criteria_when_targets_are_present() {
        let b = sample_bundle_n(0);
        let prag = sample_prag(
            crate::pragmatics_frame::INTENT_FLAG_IS_COMPARE_REQUEST
                | crate::pragmatics_frame::INTENT_FLAG_HAS_REQUEST
                | crate::pragmatics_frame::INTENT_FLAG_HAS_COMPARE_TARGETS,
        );
        let hints = build_planner_hints_v1(b.query_id, &b, Some(&prag));
        let fc = build_forecast_v1(b.query_id, Some(&prag), &hints);
        assert!(fc.questions.iter().any(|q| q.text.to_lowercase().contains("criteria")));
    }

    #[test]
    fn forecast_explain_clarifier_uses_focus_cues() {
        let b = sample_bundle_n(0);
        let prag = sample_prag(
            crate::pragmatics_frame::INTENT_FLAG_IS_EXPLAIN_REQUEST
                | crate::pragmatics_frame::INTENT_FLAG_HAS_REQUEST
                | crate::pragmatics_frame::INTENT_FLAG_HAS_FOCUS_SUMMARY,
        );
        let hints = build_planner_hints_v1(b.query_id, &b, Some(&prag));
        let fc = build_forecast_v1(b.query_id, Some(&prag), &hints);
        let top = fc.questions[0].text.to_lowercase();
        assert!(top.contains("short summary") || top.contains("walkthrough"), "top={}", fc.questions[0].text);
    }

    #[test]
    fn forecast_recommend_clarifier_prioritizes_recommendation_specific_question() {
        let b = sample_bundle_n(0);
        let prag = sample_prag(
            crate::pragmatics_frame::INTENT_FLAG_IS_RECOMMEND_REQUEST
                | crate::pragmatics_frame::INTENT_FLAG_HAS_REQUEST,
        );
        let hints = build_planner_hints_v1(b.query_id, &b, Some(&prag));
        let fc = build_forecast_v1(b.query_id, Some(&prag), &hints);
        let top = fc.questions[0].text.to_lowercase();
        assert!(top.contains("recommendation") || top.contains("cost") || top.contains("speed") || top.contains("risk"), "top={}", fc.questions[0].text);
    }

    #[test]
    fn plan_with_guidance_prefers_clarify_when_no_evidence() {
        let b = sample_bundle_n(0);
        let bundle_id = blake3_hash(b"bundle_id");
        let cfg = PlannerCfgV1::default_v1();

        let o = plan_from_evidence_bundle_v1_with_guidance(&b, bundle_id, &cfg, None).unwrap();
        assert_ne!(o.hints.flags & crate::planner_hints::PH_FLAG_PREFER_CLARIFY, 0);
        assert_eq!(o.plan.items.len(), 1);
        assert_eq!(o.plan.items[0].kind, AnswerPlanItemKindV1::Summary);
    }
}
