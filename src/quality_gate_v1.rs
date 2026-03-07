// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Quality gate consolidation helpers.
//!
//! The quality gate is a deterministic post-planning integration layer that
//! combines:
//! - Pragmatics -> RealizerDirectivesV1
//! - (Optional) MarkovModelV1 -> MarkovHintsV1 (surface-form selection only)
//! - PlannerHintsV1 + ForecastV1 -> bounded clarifying question append
//! - Realizer Markov events -> MarkovTraceV1 token stream
//!
//! This module is intentionally conservative:
//! - It MUST NOT introduce new claims.
//! - It MUST remain deterministic and replay-friendly.
//! - It focuses on wiring and bookkeeping, not retrieval.

use crate::answer_plan::AnswerPlanV1;
use crate::artifact::ArtifactStore;
use crate::evidence_bundle::EvidenceBundleV1;
use crate::forecast::ForecastV1;
use crate::frame::{derive_id64, Id64};
use crate::hash::Hash32;
use crate::markov_hints::{
    MarkovChoiceKindV1, MarkovHintsFlagsV1, MarkovHintsV1, MH_FLAG_HAS_HISTORY, MH_FLAG_HAS_PRAGMATICS,
};
use crate::markov_model::{MarkovModelV1, MarkovTokenV1};
use crate::markov_runtime::derive_markov_hints_opener_preface_v1;
use crate::planner_hints::PlannerHintsV1;
use crate::pragmatics_frame::PragmaticsFrameV1;
use crate::realizer_directives::{derive_realizer_directives_v1, RealizerDirectivesV1};
use crate::realizer_v1::{
    append_clarifying_question_v1, realize_answer_plan_v1_with_directives_and_markov_events,
    RealizerCfgV1, RealizerMarkovEventsV1, RealizerOutputV1, RealizerV1Error,
};

/// The realized text output plus quality-gate events.
#[derive(Debug)]
pub struct QualityGateRenderV1 {
    /// Realized answer text (after any bounded clarifying append).
    pub text: String,
    /// True if a clarifying question was appended.
    pub did_append_question: bool,
    /// Markov surface-choice events observed during realization.
    pub markov: RealizerMarkovEventsV1,
}

/// Derive RealizerDirectivesV1 from an optional PragmaticsFrameV1.
pub fn derive_directives_opt(p: Option<&PragmaticsFrameV1>) -> Option<RealizerDirectivesV1> {
    p.map(derive_realizer_directives_v1)
}

/// Derive MarkovHintsV1 for the opener/preface surface template.
///
/// This returns None when:
/// - directives is None
/// - directives.max_preface_sentences == 0
/// - markov_max_choices == 0
///
/// The derivation is deterministic and filtered to only allow:
/// - kind = Opener
/// - choice_id in the fixed preface:<tone>:{0|1} set
pub fn derive_markov_hints_opener_preface_opt(
    query_id: Hash32,
    has_pragmatics: bool,
    model_hash: Hash32,
    model: &MarkovModelV1,
    directives: Option<&RealizerDirectivesV1>,
    context_tokens: &[MarkovTokenV1],
    markov_max_choices: usize,
) -> Option<MarkovHintsV1> {
    if markov_max_choices == 0 {
        return None;
    }
    let d = directives?;
    if d.max_preface_sentences == 0 {
        return None;
    }
    let mut flags: MarkovHintsFlagsV1 = 0;
    if !context_tokens.is_empty() {
        flags |= MH_FLAG_HAS_HISTORY;
    }
    if has_pragmatics {
        flags |= MH_FLAG_HAS_PRAGMATICS;
    }
    Some(derive_markov_hints_opener_preface_v1(
        query_id,
        flags,
        model_hash,
        model,
        d.tone,
        context_tokens,
        markov_max_choices,
    ))
}

/// Run the realizer with optional directives + markov hints, then apply the
/// bounded clarifying append driven by PlannerHintsV1 + ForecastV1.
pub fn realize_with_quality_gate_v1<S: ArtifactStore>(
    store: &S,
    evidence: &EvidenceBundleV1,
    plan: &AnswerPlanV1,
    cfg: &RealizerCfgV1,
    directives: Option<&RealizerDirectivesV1>,
    markov_hints: Option<&MarkovHintsV1>,
    planner_hints: &PlannerHintsV1,
    forecast: &ForecastV1,
) -> Result<QualityGateRenderV1, RealizerV1Error> {
    let ro: RealizerOutputV1 = realize_answer_plan_v1_with_directives_and_markov_events(
        store,
        evidence,
        plan,
        cfg,
        directives,
        markov_hints,
    )?;
    let mut text = ro.text;
    let max_q = directives.map(|d| d.max_questions).unwrap_or(0);
    let did_append_q = append_clarifying_question_v1(&mut text, planner_hints, forecast, max_q);
    Ok(QualityGateRenderV1 {
        text,
        did_append_question: did_append_q,
        markov: ro.markov,
    })
}

/// Build the canonical MarkovTraceV1 token stream from the plan structure and
/// quality-gate events.
///
/// - If `opener_preface_choice` is Some, it is emitted as the first token.
/// - Each plan item kind emits a placeholder token (plan_item:*).
/// - If `did_append_q` is true, append:clarify_question is emitted.
pub fn build_markov_trace_tokens_v1(
    plan: &AnswerPlanV1,
    opener_preface_choice: Option<Id64>,
    did_append_q: bool,
) -> Vec<MarkovTokenV1> {
    let mut out: Vec<MarkovTokenV1> = Vec::with_capacity(
        plan.items.len()
            + if did_append_q { 1 } else { 0 }
            + if opener_preface_choice.is_some() { 1 } else { 0 },
    );

    if let Some(cid) = opener_preface_choice {
        out.push(MarkovTokenV1::new(MarkovChoiceKindV1::Opener, cid));
    }

    for it in plan.items.iter() {
        let (tk, label): (MarkovChoiceKindV1, &'static [u8]) = match it.kind {
            crate::answer_plan::AnswerPlanItemKindV1::Summary => {
                (MarkovChoiceKindV1::Opener, b"plan_item:summary")
            }
            crate::answer_plan::AnswerPlanItemKindV1::Bullet => {
                (MarkovChoiceKindV1::Transition, b"plan_item:bullet")
            }
            crate::answer_plan::AnswerPlanItemKindV1::Step => {
                (MarkovChoiceKindV1::Transition, b"plan_item:step")
            }
            crate::answer_plan::AnswerPlanItemKindV1::Caveat => {
                (MarkovChoiceKindV1::Closer, b"plan_item:caveat")
            }
        };
        let cid = derive_id64(b"markov_choice_v1", label);
        out.push(MarkovTokenV1::new(tk, cid));
    }

    if did_append_q {
        let cid = derive_id64(b"markov_choice_v1", b"append:clarify_question");
        out.push(MarkovTokenV1::new(MarkovChoiceKindV1::Closer, cid));
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::answer_plan::{AnswerPlanItemKindV1, AnswerPlanItemV1};
    use crate::markov_model::{MarkovNextV1, MarkovStateV1, MARKOV_MODEL_V1_VERSION};
    use crate::realizer_directives::{REALIZER_DIRECTIVES_V1_VERSION, StyleV1, ToneV1};


    #[test]
    fn build_markov_trace_tokens_includes_preface_first() {
        let z: Hash32 = [0u8; 32];
        let mut plan = AnswerPlanV1::new(z, z, z, 1);
        let mut it = AnswerPlanItemV1::new(AnswerPlanItemKindV1::Bullet);
        it.strength = 500;
        it.evidence_item_ix.push(0);
        plan.items.push(it);
        let pre = Id64(123);
        let toks = build_markov_trace_tokens_v1(&plan, Some(pre), false);
        assert_eq!(toks.len(), 2);
        assert_eq!(toks[0], MarkovTokenV1::new(MarkovChoiceKindV1::Opener, pre));
    }

    #[test]
    fn derive_markov_hints_sets_history_flag_when_context_nonempty() {
        let query_id: Hash32 = [0u8; 32];
        let model_hash: Hash32 = [1u8; 32];

        let directives = RealizerDirectivesV1 {
            version: REALIZER_DIRECTIVES_V1_VERSION,
            tone: ToneV1::Supportive,
            style: StyleV1::Default,
            format_flags: 0,
            max_softeners: 0,
            max_preface_sentences: 1,
            max_hedges: 0,
            max_questions: 0,
            rationale_codes: Vec::new(),
        };

        let ctx_tok = MarkovTokenV1::new(MarkovChoiceKindV1::Transition, Id64(777));
        let pre0 = derive_id64(b"markov_choice_v1", b"preface:supportive:0");
        let pre1 = derive_id64(b"markov_choice_v1", b"preface:supportive:1");

        // states are canonical: higher-order context first.
        let s1 = MarkovStateV1 {
            context: vec![ctx_tok],
            escape_count: 0,
            next: vec![MarkovNextV1 {
                token: MarkovTokenV1::new(MarkovChoiceKindV1::Opener, pre1),
                count: 9,
            }],
        };
        let s0 = MarkovStateV1 {
            context: Vec::new(),
            escape_count: 0,
            next: vec![
                MarkovNextV1 {
                    token: MarkovTokenV1::new(MarkovChoiceKindV1::Opener, pre0),
                    count: 10,
                },
                MarkovNextV1 {
                    token: MarkovTokenV1::new(MarkovChoiceKindV1::Opener, pre1),
                    count: 8,
                },
            ],
        };

        let model = MarkovModelV1 {
            version: MARKOV_MODEL_V1_VERSION,
            order_n_max: 3,
            max_next_per_state: 8,
            total_transitions: 27,
            corpus_hash: [2u8; 32],
            states: vec![s1, s0],
        };
        assert!(model.validate().is_ok());

        let h0 = derive_markov_hints_opener_preface_opt(
            query_id,
            false,
            model_hash,
            &model,
            Some(&directives),
            &[],
            8,
        )
        .unwrap();
        assert_eq!(h0.flags & crate::markov_hints::MH_FLAG_HAS_HISTORY, 0);

        let h1 = derive_markov_hints_opener_preface_opt(
            query_id,
            false,
            model_hash,
            &model,
            Some(&directives),
            &[ctx_tok],
            8,
        )
        .unwrap();
        assert_ne!(h1.flags & crate::markov_hints::MH_FLAG_HAS_HISTORY, 0);
        assert_ne!(h0.context_hash, h1.context_hash);
        assert_ne!(h0.state_id, h1.state_id);
    }

}
