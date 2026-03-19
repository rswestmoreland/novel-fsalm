// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use fsa_lm::answer_plan::{AnswerPlanItemKindV1, AnswerPlanItemV1, AnswerPlanV1};
use fsa_lm::forecast::{ForecastQuestionV1, ForecastV1, FORECAST_V1_VERSION};
use fsa_lm::frame::{derive_id64, Id64};
use fsa_lm::hash::Hash32;
use fsa_lm::markov_hints::{MarkovChoiceKindV1, MarkovChoiceV1, MarkovHintsV1};
use fsa_lm::markov_model::MarkovTokenV1;
use fsa_lm::planner_hints::{PlannerHintsV1, PH_FLAG_PREFER_CLARIFY, PLANNER_HINTS_V1_VERSION};
use fsa_lm::quality_gate_v1::build_markov_trace_tokens_v1;
use fsa_lm::realizer_v1::{
    append_clarifying_question_v1_with_markov_events, RealizerMarkovEventsV1,
};

#[test]
fn markov_valid_other_selects_alternate_clarifier_intro() {
    let mut out = String::from("Answer v1\n");
    let hints = PlannerHintsV1 {
        version: PLANNER_HINTS_V1_VERSION,
        query_id: [0u8; 32],
        flags: PH_FLAG_PREFER_CLARIFY,
        hints: Vec::new(),
        followups: Vec::new(),
    };
    let fc = ForecastV1 {
        version: FORECAST_V1_VERSION,
        query_id: [1u8; 32],
        flags: 0,
        horizon_turns: 1,
        intents: Vec::new(),
        questions: vec![ForecastQuestionV1::new(
            Id64(1),
            1,
            "What did you expect to happen".to_string(),
            0,
        )],
    };
    let choice_id = derive_id64(b"markov_choice_v1", b"other:clarifier_intro:1");
    let mh = MarkovHintsV1 {
        version: fsa_lm::markov_hints::MARKOV_HINTS_V1_VERSION,
        query_id: [0u8; 32],
        flags: 0,
        order_n: 1,
        state_id: Id64(0),
        model_hash: [0u8; 32],
        context_hash: [0u8; 32],
        choices: vec![MarkovChoiceV1::new(
            MarkovChoiceKindV1::Other,
            choice_id,
            10,
            0,
        )],
    };
    let mut events = RealizerMarkovEventsV1 {
        opener_preface_choice: None,
        details_heading_transition_choice: None,
        caveat_heading_closer_choice: None,
        clarifier_intro_choice: None,
    };

    let appended = append_clarifying_question_v1_with_markov_events(
        &mut out,
        &hints,
        &fc,
        1,
        Some(&mh),
        &mut events,
    );
    assert!(appended);
    assert!(out.contains("So I can answer the right thing:"));
    assert!(!out.contains("To make sure I answer the right thing:"));
    assert_eq!(events.clarifier_intro_choice, Some(choice_id));
}

#[test]
fn clarifier_intro_trace_emits_other_before_append() {
    let z: Hash32 = [0u8; 32];
    let mut plan = AnswerPlanV1::new(z, z, z, 1);
    plan.items
        .push(AnswerPlanItemV1::new(AnswerPlanItemKindV1::Summary));

    let intro = derive_id64(b"markov_choice_v1", b"other:clarifier_intro:1");
    let events = RealizerMarkovEventsV1 {
        opener_preface_choice: None,
        details_heading_transition_choice: None,
        caveat_heading_closer_choice: None,
        clarifier_intro_choice: Some(intro),
    };
    let toks: Vec<MarkovTokenV1> = build_markov_trace_tokens_v1(&plan, &events, true);
    assert_eq!(toks.len(), 3);
    assert_eq!(
        toks[0],
        MarkovTokenV1::new(
            MarkovChoiceKindV1::Opener,
            derive_id64(b"markov_choice_v1", b"plan_item:summary"),
        )
    );
    assert_eq!(
        toks[1],
        MarkovTokenV1::new(MarkovChoiceKindV1::Other, intro)
    );
    assert_eq!(
        toks[2],
        MarkovTokenV1::new(
            MarkovChoiceKindV1::Closer,
            derive_id64(b"markov_choice_v1", b"append:clarify_question"),
        )
    );
}
