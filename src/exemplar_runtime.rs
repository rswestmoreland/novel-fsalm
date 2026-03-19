// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Runtime exemplar advisory helpers.
//!
//! Exemplar runtime use is intentionally bounded and advisory-only.
//! It may shape tone and presentation, but it must not change truth,
//! retrieved evidence, or claim content.

use crate::answer_plan::{AnswerPlanItemKindV1, AnswerPlanV1};
use crate::exemplar_memory::{
    ExemplarMemoryV1, ExemplarResponseModeV1, ExemplarRowFlagsV1, ExemplarStructureKindV1,
    ExemplarToneKindV1, EXROW_FLAG_HAS_CLARIFIER, EXROW_FLAG_HAS_COMPARISON, EXROW_FLAG_HAS_STEPS,
    EXROW_FLAG_HAS_SUMMARY,
};
use crate::frame::Id64;
use crate::planner_hints::{
    PlannerHintKindV1, PlannerHintsV1, PH_FLAG_PREFER_CLARIFY, PH_FLAG_PREFER_STEPS,
};
use crate::pragmatics_frame::{
    PragmaticsFrameV1, INTENT_FLAG_IS_COMPARE_REQUEST, INTENT_FLAG_IS_EXPLAIN_REQUEST,
    INTENT_FLAG_IS_FOLLOW_UP, INTENT_FLAG_IS_LOGIC_PUZZLE, INTENT_FLAG_IS_PROBLEM_SOLVE,
    INTENT_FLAG_IS_RECOMMEND_REQUEST, INTENT_FLAG_IS_SUMMARIZE_REQUEST,
};
use crate::realizer_directives::{
    FormatFlagsV1, RealizerDirectivesV1, StyleV1, ToneV1, FORMAT_FLAG_BULLETS,
    FORMAT_FLAG_INCLUDE_NEXT_STEPS, FORMAT_FLAG_INCLUDE_SUMMARY, FORMAT_FLAG_NUMBERED,
    RD_RATIONALE_EXEMPLAR_ADVISORY, REALIZER_DIRECTIVES_V1_VERSION,
};

/// One selected runtime exemplar advisory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExemplarAdvisoryV1 {
    /// Stable exemplar id selected from ExemplarMemoryV1.
    pub exemplar_id: Id64,
    /// Selected response mode.
    pub response_mode: ExemplarResponseModeV1,
    /// Selected structure kind.
    pub structure_kind: ExemplarStructureKindV1,
    /// Selected tone kind.
    pub tone_kind: ExemplarToneKindV1,
    /// Selected row flags.
    pub flags: ExemplarRowFlagsV1,
    /// Support count kept with the selected row.
    pub support_count: u32,
    /// Deterministic match score used during selection.
    pub score: u16,
    /// Stable match-reason flags.
    pub match_flags: u8,
}

/// Advisory match includes response-mode alignment.
pub const EXAD_MATCH_RESPONSE_MODE: u8 = 1u8 << 0;
/// Advisory match includes structure-kind alignment.
pub const EXAD_MATCH_STRUCTURE: u8 = 1u8 << 1;
/// Advisory match includes tone-kind alignment.
pub const EXAD_MATCH_TONE: u8 = 1u8 << 2;
/// Advisory match includes summary-shape alignment.
pub const EXAD_MATCH_SUMMARY: u8 = 1u8 << 3;
/// Advisory match includes steps-shape alignment.
pub const EXAD_MATCH_STEPS: u8 = 1u8 << 4;
/// Advisory match includes comparison-shape alignment.
pub const EXAD_MATCH_COMPARISON: u8 = 1u8 << 5;
/// Advisory match includes clarifier-shape alignment.
pub const EXAD_MATCH_CLARIFIER: u8 = 1u8 << 6;

struct ExemplarTargetV1 {
    response_mode: ExemplarResponseModeV1,
    structure_kind: ExemplarStructureKindV1,
    tone_kind: ExemplarToneKindV1,
    wants_summary: bool,
    wants_steps: bool,
    wants_comparison: bool,
    wants_clarifier: bool,
}

fn hints_has_kind(hints: &PlannerHintsV1, kind: PlannerHintKindV1) -> bool {
    for it in hints.hints.iter() {
        if it.kind == kind {
            return true;
        }
    }
    false
}

fn target_from_runtime_inputs_v1(
    prag_opt: Option<&PragmaticsFrameV1>,
    hints: &PlannerHintsV1,
    directives_opt: Option<&RealizerDirectivesV1>,
    query_text_opt: Option<&str>,
) -> ExemplarTargetV1 {
    let mut response_mode = ExemplarResponseModeV1::Direct;

    let prefer_clarify = (hints.flags & PH_FLAG_PREFER_CLARIFY) != 0;
    let prefer_steps = (hints.flags & PH_FLAG_PREFER_STEPS) != 0;
    let summary_first = hints_has_kind(hints, PlannerHintKindV1::SummaryFirst);
    let compare_hint = hints_has_kind(hints, PlannerHintKindV1::Compare);

    let lower = query_text_opt.unwrap_or("").to_ascii_lowercase();
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

    if let Some(p) = prag_opt {
        let f = p.flags;
        if (f & INTENT_FLAG_IS_FOLLOW_UP) != 0 {
            response_mode = ExemplarResponseModeV1::Continue;
        } else if (f & INTENT_FLAG_IS_COMPARE_REQUEST) != 0 || compare_hint || fallback_compare {
            response_mode = ExemplarResponseModeV1::Compare;
        } else if (f & INTENT_FLAG_IS_RECOMMEND_REQUEST) != 0 || fallback_recommend {
            response_mode = ExemplarResponseModeV1::Recommend;
        } else if (f & INTENT_FLAG_IS_SUMMARIZE_REQUEST) != 0 || summary_first || fallback_summarize
        {
            response_mode = ExemplarResponseModeV1::Summarize;
        } else if (f & INTENT_FLAG_IS_EXPLAIN_REQUEST) != 0 || fallback_explain {
            response_mode = ExemplarResponseModeV1::Explain;
        } else if (f & INTENT_FLAG_IS_PROBLEM_SOLVE) != 0 || (f & INTENT_FLAG_IS_LOGIC_PUZZLE) != 0
        {
            response_mode = ExemplarResponseModeV1::Troubleshoot;
        } else if prefer_clarify {
            response_mode = ExemplarResponseModeV1::Clarify;
        }
    } else if compare_hint || fallback_compare {
        response_mode = ExemplarResponseModeV1::Compare;
    } else if summary_first || fallback_summarize {
        response_mode = ExemplarResponseModeV1::Summarize;
    } else if fallback_recommend {
        response_mode = ExemplarResponseModeV1::Recommend;
    } else if fallback_explain {
        response_mode = ExemplarResponseModeV1::Explain;
    } else if prefer_clarify {
        response_mode = ExemplarResponseModeV1::Clarify;
    }

    let structure_kind = if prefer_clarify {
        ExemplarStructureKindV1::Clarifier
    } else {
        match response_mode {
            ExemplarResponseModeV1::Compare => ExemplarStructureKindV1::Comparison,
            ExemplarResponseModeV1::Recommend => ExemplarStructureKindV1::Recommendation,
            _ if prefer_steps || fallback_steps => ExemplarStructureKindV1::Steps,
            _ if summary_first || fallback_summary_focus => ExemplarStructureKindV1::SummaryFirst,
            _ => ExemplarStructureKindV1::Direct,
        }
    };

    let tone_kind = match directives_opt.map(|d| d.tone).unwrap_or(ToneV1::Neutral) {
        ToneV1::Neutral => ExemplarToneKindV1::Neutral,
        ToneV1::Supportive => ExemplarToneKindV1::Supportive,
        ToneV1::Direct => ExemplarToneKindV1::Direct,
        ToneV1::Cautious => ExemplarToneKindV1::Cautious,
    };

    ExemplarTargetV1 {
        response_mode,
        structure_kind,
        tone_kind,
        wants_summary: summary_first
            || fallback_summary_focus
            || structure_kind == ExemplarStructureKindV1::SummaryFirst,
        wants_steps: prefer_steps
            || fallback_steps
            || structure_kind == ExemplarStructureKindV1::Steps,
        wants_comparison: compare_hint
            || fallback_compare
            || response_mode == ExemplarResponseModeV1::Compare,
        wants_clarifier: prefer_clarify,
    }
}

fn row_match_flags_v1(
    row: &crate::exemplar_memory::ExemplarRowV1,
    target: &ExemplarTargetV1,
) -> u8 {
    let mut flags = 0u8;
    if row.response_mode == target.response_mode {
        flags |= EXAD_MATCH_RESPONSE_MODE;
    }
    if row.structure_kind == target.structure_kind {
        flags |= EXAD_MATCH_STRUCTURE;
    }
    if row.tone_kind == target.tone_kind {
        flags |= EXAD_MATCH_TONE;
    }
    if target.wants_summary && (row.flags & EXROW_FLAG_HAS_SUMMARY) != 0 {
        flags |= EXAD_MATCH_SUMMARY;
    }
    if target.wants_steps && (row.flags & EXROW_FLAG_HAS_STEPS) != 0 {
        flags |= EXAD_MATCH_STEPS;
    }
    if target.wants_comparison && (row.flags & EXROW_FLAG_HAS_COMPARISON) != 0 {
        flags |= EXAD_MATCH_COMPARISON;
    }
    if target.wants_clarifier && (row.flags & EXROW_FLAG_HAS_CLARIFIER) != 0 {
        flags |= EXAD_MATCH_CLARIFIER;
    }
    flags
}

fn row_match_score_v1(
    row: &crate::exemplar_memory::ExemplarRowV1,
    target: &ExemplarTargetV1,
) -> (u32, u8) {
    let flags = row_match_flags_v1(row, target);
    let mut score = 0u32;
    if (flags & EXAD_MATCH_RESPONSE_MODE) != 0 {
        score = score.saturating_add(100);
    }
    if (flags & EXAD_MATCH_STRUCTURE) != 0 {
        score = score.saturating_add(50);
    }
    if (flags & EXAD_MATCH_TONE) != 0 {
        score = score.saturating_add(20);
    }
    if (flags & EXAD_MATCH_SUMMARY) != 0 {
        score = score.saturating_add(10);
    }
    if (flags & EXAD_MATCH_STEPS) != 0 {
        score = score.saturating_add(10);
    }
    if (flags & EXAD_MATCH_COMPARISON) != 0 {
        score = score.saturating_add(10);
    }
    if (flags & EXAD_MATCH_CLARIFIER) != 0 {
        score = score.saturating_add(10);
    }
    (score, flags)
}

/// Select the best bounded exemplar advisory for the current runtime request.
pub fn lookup_exemplar_advisory_v1(
    exemplar_memory: &ExemplarMemoryV1,
    prag_opt: Option<&PragmaticsFrameV1>,
    hints: &PlannerHintsV1,
    directives_opt: Option<&RealizerDirectivesV1>,
    query_text_opt: Option<&str>,
) -> Option<ExemplarAdvisoryV1> {
    if exemplar_memory.rows.is_empty() {
        return None;
    }
    let target = target_from_runtime_inputs_v1(prag_opt, hints, directives_opt, query_text_opt);
    let mut best_ix: Option<usize> = None;
    let mut best_score = 0u32;
    let mut best_match_flags = 0u8;
    for (ix, row) in exemplar_memory.rows.iter().enumerate() {
        let (score, match_flags) = row_match_score_v1(row, &target);
        if score == 0 {
            continue;
        }
        match best_ix {
            None => {
                best_ix = Some(ix);
                best_score = score;
                best_match_flags = match_flags;
            }
            Some(bix) => {
                let best = &exemplar_memory.rows[bix];
                if score > best_score
                    || (score == best_score
                        && (row.support_count > best.support_count
                            || (row.support_count == best.support_count
                                && row.exemplar_id.0 < best.exemplar_id.0)))
                {
                    best_ix = Some(ix);
                    best_score = score;
                    best_match_flags = match_flags;
                }
            }
        }
    }
    let row = exemplar_memory.rows.get(best_ix?)?;
    Some(ExemplarAdvisoryV1 {
        exemplar_id: row.exemplar_id,
        response_mode: row.response_mode,
        structure_kind: row.structure_kind,
        tone_kind: row.tone_kind,
        flags: row.flags,
        support_count: row.support_count,
        score: best_score as u16,
        match_flags: best_match_flags,
    })
}

fn tone_kind_to_tone_v1(t: ExemplarToneKindV1) -> ToneV1 {
    match t {
        ExemplarToneKindV1::Neutral => ToneV1::Neutral,
        ExemplarToneKindV1::Supportive => ToneV1::Supportive,
        ExemplarToneKindV1::Direct => ToneV1::Direct,
        ExemplarToneKindV1::Cautious => ToneV1::Cautious,
    }
}

fn structure_kind_to_style_v1(k: ExemplarStructureKindV1) -> StyleV1 {
    match k {
        ExemplarStructureKindV1::Direct => StyleV1::Default,
        ExemplarStructureKindV1::SummaryFirst => StyleV1::Concise,
        ExemplarStructureKindV1::Steps => StyleV1::StepByStep,
        ExemplarStructureKindV1::Comparison => StyleV1::Checklist,
        ExemplarStructureKindV1::Recommendation => StyleV1::Checklist,
        ExemplarStructureKindV1::Clarifier => StyleV1::Default,
    }
}

fn default_limits_for_tone_v1(tone: ToneV1) -> (u8, u8, u8, u8) {
    match tone {
        ToneV1::Neutral => (1, 0, 2, 0),
        ToneV1::Supportive => (2, 1, 2, 1),
        ToneV1::Direct => (0, 0, 1, 0),
        ToneV1::Cautious => (1, 1, 3, 1),
    }
}

fn base_format_flags_for_structure_v1(k: ExemplarStructureKindV1) -> FormatFlagsV1 {
    match k {
        ExemplarStructureKindV1::Direct => 0,
        ExemplarStructureKindV1::SummaryFirst => FORMAT_FLAG_INCLUDE_SUMMARY,
        ExemplarStructureKindV1::Steps => FORMAT_FLAG_INCLUDE_NEXT_STEPS | FORMAT_FLAG_NUMBERED,
        ExemplarStructureKindV1::Comparison => FORMAT_FLAG_BULLETS,
        ExemplarStructureKindV1::Recommendation => {
            FORMAT_FLAG_INCLUDE_NEXT_STEPS | FORMAT_FLAG_BULLETS
        }
        ExemplarStructureKindV1::Clarifier => 0,
    }
}

fn insert_rationale_code_v1(xs: &mut Vec<u16>, code: u16) {
    if xs.binary_search(&code).is_err() {
        xs.push(code);
        xs.sort_unstable();
        xs.dedup();
    }
}

fn plan_has_kind_v1(plan: &AnswerPlanV1, kind: AnswerPlanItemKindV1) -> bool {
    plan.items.iter().any(|it| it.kind == kind)
}

fn promote_first_kind_to_v1(
    plan: &mut AnswerPlanV1,
    from_kind: AnswerPlanItemKindV1,
    to_kind: AnswerPlanItemKindV1,
) -> bool {
    for it in plan.items.iter_mut() {
        if it.kind == from_kind {
            it.kind = to_kind;
            return true;
        }
    }
    false
}

fn convert_all_kind_v1(
    plan: &mut AnswerPlanV1,
    from_kind: AnswerPlanItemKindV1,
    to_kind: AnswerPlanItemKindV1,
) -> bool {
    let mut changed = false;
    for it in plan.items.iter_mut() {
        if it.kind == from_kind {
            it.kind = to_kind;
            changed = true;
        }
    }
    changed
}

fn apply_summary_first_shape_v1(plan: &mut AnswerPlanV1) -> bool {
    if plan_has_kind_v1(plan, AnswerPlanItemKindV1::Summary) {
        return false;
    }
    if promote_first_kind_to_v1(
        plan,
        AnswerPlanItemKindV1::Bullet,
        AnswerPlanItemKindV1::Summary,
    ) {
        return true;
    }
    promote_first_kind_to_v1(
        plan,
        AnswerPlanItemKindV1::Step,
        AnswerPlanItemKindV1::Summary,
    )
}

fn apply_comparison_shape_v1(plan: &mut AnswerPlanV1) -> bool {
    if plan_has_kind_v1(plan, AnswerPlanItemKindV1::Bullet) {
        return false;
    }
    convert_all_kind_v1(
        plan,
        AnswerPlanItemKindV1::Step,
        AnswerPlanItemKindV1::Bullet,
    )
}

fn apply_recommendation_shape_v1(plan: &mut AnswerPlanV1, advisory: &ExemplarAdvisoryV1) -> bool {
    let mut changed = false;
    if !plan_has_kind_v1(plan, AnswerPlanItemKindV1::Summary) {
        if promote_first_kind_to_v1(
            plan,
            AnswerPlanItemKindV1::Bullet,
            AnswerPlanItemKindV1::Summary,
        ) {
            changed = true;
        } else if promote_first_kind_to_v1(
            plan,
            AnswerPlanItemKindV1::Step,
            AnswerPlanItemKindV1::Summary,
        ) {
            changed = true;
        }
    }
    if (advisory.flags & EXROW_FLAG_HAS_STEPS) != 0
        && !plan_has_kind_v1(plan, AnswerPlanItemKindV1::Step)
    {
        let mut converted = false;
        for it in plan.items.iter_mut() {
            if it.kind == AnswerPlanItemKindV1::Bullet {
                it.kind = AnswerPlanItemKindV1::Step;
                converted = true;
            }
        }
        if converted {
            changed = true;
        }
    }
    changed
}

/// Apply a selected exemplar advisory conservatively to runtime rendering inputs.
///
/// This may shape presentation, but it must not change evidence rows or claims.
pub fn apply_exemplar_advisory_v1(
    plan: &mut AnswerPlanV1,
    directives_opt: &mut Option<RealizerDirectivesV1>,
    advisory: &ExemplarAdvisoryV1,
) -> bool {
    let advisory_tone = tone_kind_to_tone_v1(advisory.tone_kind);
    let advisory_style = structure_kind_to_style_v1(advisory.structure_kind);
    let advisory_flags = base_format_flags_for_structure_v1(advisory.structure_kind);
    let (def_softeners, def_preface, def_hedges, def_questions) =
        default_limits_for_tone_v1(advisory_tone);

    let mut changed = false;
    if directives_opt.is_none() {
        let mut format_flags = advisory_flags;
        if (advisory.flags & EXROW_FLAG_HAS_SUMMARY) != 0 {
            format_flags |= FORMAT_FLAG_INCLUDE_SUMMARY;
        }
        if (advisory.flags & EXROW_FLAG_HAS_STEPS) != 0 {
            format_flags |= FORMAT_FLAG_INCLUDE_NEXT_STEPS | FORMAT_FLAG_NUMBERED;
        }
        *directives_opt = Some(RealizerDirectivesV1 {
            version: REALIZER_DIRECTIVES_V1_VERSION,
            tone: advisory_tone,
            style: advisory_style,
            format_flags,
            max_softeners: def_softeners,
            max_preface_sentences: def_preface,
            max_hedges: def_hedges,
            max_questions: if (advisory.flags & EXROW_FLAG_HAS_CLARIFIER) != 0 {
                core::cmp::max(def_questions, 1)
            } else {
                def_questions
            },
            rationale_codes: vec![RD_RATIONALE_EXEMPLAR_ADVISORY],
        });
        changed = true;
    } else if let Some(d) = directives_opt.as_mut() {
        if d.style != StyleV1::Debug {
            if d.tone == ToneV1::Neutral && advisory_tone != ToneV1::Neutral {
                d.tone = advisory_tone;
                d.max_softeners = core::cmp::max(d.max_softeners, def_softeners);
                d.max_preface_sentences = core::cmp::max(d.max_preface_sentences, def_preface);
                d.max_hedges = core::cmp::max(d.max_hedges, def_hedges);
                changed = true;
            }
            if matches!(d.style, StyleV1::Default | StyleV1::Concise) {
                match advisory.structure_kind {
                    ExemplarStructureKindV1::SummaryFirst
                    | ExemplarStructureKindV1::Steps
                    | ExemplarStructureKindV1::Comparison
                    | ExemplarStructureKindV1::Recommendation => {
                        if d.style != advisory_style {
                            d.style = advisory_style;
                            changed = true;
                        }
                    }
                    _ => {}
                }
            }
            let new_flags = d.format_flags | advisory_flags;
            if new_flags != d.format_flags {
                d.format_flags = new_flags;
                changed = true;
            }
            if (advisory.flags & EXROW_FLAG_HAS_SUMMARY) != 0 {
                let nf = d.format_flags | FORMAT_FLAG_INCLUDE_SUMMARY;
                if nf != d.format_flags {
                    d.format_flags = nf;
                    changed = true;
                }
            }
            if (advisory.flags & EXROW_FLAG_HAS_CLARIFIER) != 0 {
                let nq = core::cmp::max(d.max_questions, 1);
                if nq != d.max_questions {
                    d.max_questions = nq;
                    changed = true;
                }
            }
            if (advisory.flags & EXROW_FLAG_HAS_STEPS) != 0 {
                let nf = d.format_flags | FORMAT_FLAG_INCLUDE_NEXT_STEPS | FORMAT_FLAG_NUMBERED;
                if nf != d.format_flags {
                    d.format_flags = nf;
                    changed = true;
                }
            }
        }
        insert_rationale_code_v1(&mut d.rationale_codes, RD_RATIONALE_EXEMPLAR_ADVISORY);
    }

    match advisory.structure_kind {
        ExemplarStructureKindV1::Steps => {
            let mut has_step = false;
            let mut has_bullet = false;
            for it in plan.items.iter() {
                if it.kind == AnswerPlanItemKindV1::Step {
                    has_step = true;
                }
                if it.kind == AnswerPlanItemKindV1::Bullet {
                    has_bullet = true;
                }
            }
            if !has_step && has_bullet {
                for it in plan.items.iter_mut() {
                    if it.kind == AnswerPlanItemKindV1::Bullet {
                        it.kind = AnswerPlanItemKindV1::Step;
                        changed = true;
                    }
                }
            }
        }
        ExemplarStructureKindV1::SummaryFirst => {
            if apply_summary_first_shape_v1(plan) {
                changed = true;
            }
        }
        ExemplarStructureKindV1::Comparison => {
            if apply_comparison_shape_v1(plan) {
                changed = true;
            }
        }
        ExemplarStructureKindV1::Recommendation => {
            if apply_recommendation_shape_v1(plan, advisory) {
                changed = true;
            }
        }
        _ => {}
    }

    changed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::answer_plan::{AnswerPlanItemV1, AnswerPlanV1};
    use crate::exemplar_memory::{ExemplarMemoryV1, ExemplarRowV1, EXEMPLAR_MEMORY_V1_VERSION};
    use crate::hash::blake3_hash;
    use crate::planner_hints::PLANNER_HINTS_V1_VERSION;
    use crate::realizer_directives::FORMAT_FLAG_INCLUDE_SUMMARY;

    fn sample_hints() -> PlannerHintsV1 {
        PlannerHintsV1 {
            version: PLANNER_HINTS_V1_VERSION,
            query_id: [0u8; 32],
            flags: 0,
            hints: Vec::new(),
            followups: Vec::new(),
        }
    }

    #[test]
    fn empty_exemplar_memory_falls_back_cleanly() {
        let mem = ExemplarMemoryV1 {
            version: EXEMPLAR_MEMORY_V1_VERSION,
            build_id: [0u8; 32],
            flags: 0,
            rows: Vec::new(),
        };
        assert_eq!(
            lookup_exemplar_advisory_v1(&mem, None, &sample_hints(), None, None),
            None
        );
    }

    #[test]
    fn lookup_prefers_direct_row_with_matching_structure() {
        let mem = ExemplarMemoryV1 {
            version: EXEMPLAR_MEMORY_V1_VERSION,
            build_id: [0u8; 32],
            flags: 0,
            rows: vec![
                ExemplarRowV1 {
                    exemplar_id: Id64(9),
                    response_mode: ExemplarResponseModeV1::Direct,
                    structure_kind: ExemplarStructureKindV1::Direct,
                    tone_kind: ExemplarToneKindV1::Supportive,
                    flags: 0,
                    support_count: 3,
                    support_refs: Vec::new(),
                },
                ExemplarRowV1 {
                    exemplar_id: Id64(10),
                    response_mode: ExemplarResponseModeV1::Compare,
                    structure_kind: ExemplarStructureKindV1::Comparison,
                    tone_kind: ExemplarToneKindV1::Neutral,
                    flags: EXROW_FLAG_HAS_COMPARISON,
                    support_count: 99,
                    support_refs: Vec::new(),
                },
            ],
        };
        let adv =
            lookup_exemplar_advisory_v1(&mem, None, &sample_hints(), None, None).expect("advisory");
        assert_eq!(adv.exemplar_id, Id64(9));
        assert_eq!(adv.tone_kind, ExemplarToneKindV1::Supportive);
        assert_ne!(adv.match_flags & EXAD_MATCH_RESPONSE_MODE, 0);
    }

    #[test]
    fn lookup_uses_query_text_fallback_for_summary_first_matching() {
        let mem = ExemplarMemoryV1 {
            version: EXEMPLAR_MEMORY_V1_VERSION,
            build_id: [0u8; 32],
            flags: 0,
            rows: vec![ExemplarRowV1 {
                exemplar_id: Id64(11),
                response_mode: ExemplarResponseModeV1::Summarize,
                structure_kind: ExemplarStructureKindV1::SummaryFirst,
                tone_kind: ExemplarToneKindV1::Neutral,
                flags: EXROW_FLAG_HAS_SUMMARY,
                support_count: 3,
                support_refs: Vec::new(),
            }],
        };
        let adv = lookup_exemplar_advisory_v1(
            &mem,
            None,
            &sample_hints(),
            None,
            Some("Summarize banana split briefly"),
        )
        .expect("advisory");
        assert_eq!(adv.exemplar_id, Id64(11));
        assert_ne!(adv.match_flags & EXAD_MATCH_RESPONSE_MODE, 0);
        assert_ne!(adv.match_flags & EXAD_MATCH_STRUCTURE, 0);
        assert_ne!(adv.match_flags & EXAD_MATCH_TONE, 0);
        assert_ne!(adv.match_flags & EXAD_MATCH_SUMMARY, 0);
    }

    #[test]
    fn apply_advisory_can_synthesize_supportive_directives_and_steps() {
        let mut plan =
            AnswerPlanV1::new(blake3_hash(b"q"), blake3_hash(b"s"), blake3_hash(b"e"), 1);
        let mut it = AnswerPlanItemV1::new(AnswerPlanItemKindV1::Bullet);
        it.evidence_item_ix.push(0);
        plan.items.push(it);

        let adv = ExemplarAdvisoryV1 {
            exemplar_id: Id64(1),
            response_mode: ExemplarResponseModeV1::Direct,
            structure_kind: ExemplarStructureKindV1::Steps,
            tone_kind: ExemplarToneKindV1::Supportive,
            flags: EXROW_FLAG_HAS_STEPS | EXROW_FLAG_HAS_SUMMARY,
            support_count: 1,
            score: 160,
            match_flags: EXAD_MATCH_RESPONSE_MODE
                | EXAD_MATCH_STRUCTURE
                | EXAD_MATCH_SUMMARY
                | EXAD_MATCH_STEPS,
        };

        let mut directives_opt: Option<RealizerDirectivesV1> = None;
        let changed = apply_exemplar_advisory_v1(&mut plan, &mut directives_opt, &adv);
        assert!(changed);
        let d = directives_opt.expect("directives");
        assert_eq!(d.tone, ToneV1::Supportive);
        assert_eq!(d.style, StyleV1::StepByStep);
        assert_ne!(d.format_flags & FORMAT_FLAG_INCLUDE_SUMMARY, 0);
        assert_eq!(plan.items[0].kind, AnswerPlanItemKindV1::Step);
    }

    #[test]
    fn apply_advisory_summary_first_promotes_first_bullet() {
        let mut plan = AnswerPlanV1::new(
            blake3_hash(b"q1"),
            blake3_hash(b"s1"),
            blake3_hash(b"e1"),
            1,
        );
        let mut it = AnswerPlanItemV1::new(AnswerPlanItemKindV1::Bullet);
        it.evidence_item_ix.push(0);
        plan.items.push(it);
        let adv = ExemplarAdvisoryV1 {
            exemplar_id: Id64(2),
            response_mode: ExemplarResponseModeV1::Summarize,
            structure_kind: ExemplarStructureKindV1::SummaryFirst,
            tone_kind: ExemplarToneKindV1::Neutral,
            flags: EXROW_FLAG_HAS_SUMMARY,
            support_count: 2,
            score: 160,
            match_flags: EXAD_MATCH_RESPONSE_MODE | EXAD_MATCH_STRUCTURE | EXAD_MATCH_SUMMARY,
        };
        let mut directives_opt = Some(RealizerDirectivesV1 {
            version: REALIZER_DIRECTIVES_V1_VERSION,
            tone: ToneV1::Neutral,
            style: StyleV1::Default,
            format_flags: 0,
            max_softeners: 0,
            max_preface_sentences: 0,
            max_hedges: 0,
            max_questions: 0,
            rationale_codes: Vec::new(),
        });
        assert!(apply_exemplar_advisory_v1(
            &mut plan,
            &mut directives_opt,
            &adv
        ));
        assert_eq!(plan.items[0].kind, AnswerPlanItemKindV1::Summary);
        assert_eq!(directives_opt.unwrap().style, StyleV1::Concise);
    }

    #[test]
    fn apply_advisory_comparison_can_convert_steps_to_bullets() {
        let mut plan = AnswerPlanV1::new(
            blake3_hash(b"q2"),
            blake3_hash(b"s2"),
            blake3_hash(b"e2"),
            1,
        );
        let mut it = AnswerPlanItemV1::new(AnswerPlanItemKindV1::Step);
        it.evidence_item_ix.push(0);
        plan.items.push(it);
        let adv = ExemplarAdvisoryV1 {
            exemplar_id: Id64(3),
            response_mode: ExemplarResponseModeV1::Compare,
            structure_kind: ExemplarStructureKindV1::Comparison,
            tone_kind: ExemplarToneKindV1::Neutral,
            flags: EXROW_FLAG_HAS_COMPARISON,
            support_count: 2,
            score: 160,
            match_flags: EXAD_MATCH_RESPONSE_MODE | EXAD_MATCH_STRUCTURE | EXAD_MATCH_COMPARISON,
        };
        let mut directives_opt = None;
        assert!(apply_exemplar_advisory_v1(
            &mut plan,
            &mut directives_opt,
            &adv
        ));
        assert_eq!(plan.items[0].kind, AnswerPlanItemKindV1::Bullet);
        assert_eq!(directives_opt.unwrap().style, StyleV1::Checklist);
    }

    #[test]
    fn apply_advisory_recommendation_can_shape_summary_and_steps() {
        let mut plan = AnswerPlanV1::new(
            blake3_hash(b"q3"),
            blake3_hash(b"s3"),
            blake3_hash(b"e3"),
            2,
        );
        let mut it0 = AnswerPlanItemV1::new(AnswerPlanItemKindV1::Bullet);
        it0.evidence_item_ix.push(0);
        let mut it1 = AnswerPlanItemV1::new(AnswerPlanItemKindV1::Bullet);
        it1.evidence_item_ix.push(1);
        plan.items.push(it0);
        plan.items.push(it1);
        let adv = ExemplarAdvisoryV1 {
            exemplar_id: Id64(4),
            response_mode: ExemplarResponseModeV1::Recommend,
            structure_kind: ExemplarStructureKindV1::Recommendation,
            tone_kind: ExemplarToneKindV1::Supportive,
            flags: EXROW_FLAG_HAS_SUMMARY | EXROW_FLAG_HAS_STEPS,
            support_count: 4,
            score: 170,
            match_flags: EXAD_MATCH_RESPONSE_MODE
                | EXAD_MATCH_STRUCTURE
                | EXAD_MATCH_SUMMARY
                | EXAD_MATCH_STEPS,
        };
        let mut directives_opt = None;
        assert!(apply_exemplar_advisory_v1(
            &mut plan,
            &mut directives_opt,
            &adv
        ));
        assert_eq!(plan.items[0].kind, AnswerPlanItemKindV1::Summary);
        assert_eq!(plan.items[1].kind, AnswerPlanItemKindV1::Step);
    }
}
