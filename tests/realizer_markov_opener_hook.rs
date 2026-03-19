// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use fsa_lm::answer_plan::{AnswerPlanItemKindV1, AnswerPlanItemV1, AnswerPlanV1};
use fsa_lm::artifact::{ArtifactResult, ArtifactStore};
use fsa_lm::evidence_bundle::{
    EvidenceBundleV1, EvidenceItemDataV1, EvidenceItemV1, EvidenceLimitsV1, ProofRefV1,
};
use fsa_lm::frame::{derive_id64, Id64};
use fsa_lm::hash::{blake3_hash, Hash32};
use fsa_lm::markov_hints::{MarkovChoiceKindV1, MarkovChoiceV1, MarkovHintsV1};
use fsa_lm::realizer_directives::{
    RealizerDirectivesV1, StyleV1, ToneV1, REALIZER_DIRECTIVES_V1_VERSION,
};
use fsa_lm::realizer_v1::{
    realize_answer_plan_v1_with_directives, realize_answer_plan_v1_with_directives_and_markov,
    RealizerCfgV1,
};

struct NoopStore;

impl ArtifactStore for NoopStore {
    fn put(&self, _bytes: &[u8]) -> ArtifactResult<Hash32> {
        panic!("NoopStore::put should not be called in this test");
    }

    fn get(&self, _hash: &Hash32) -> ArtifactResult<Option<Vec<u8>>> {
        panic!("NoopStore::get should not be called in this test");
    }

    fn path_for(&self, _hash: &Hash32) -> std::path::PathBuf {
        std::path::PathBuf::new()
    }
}

fn preface_variants_for_tone(t: ToneV1) -> (&'static str, &'static str) {
    match t {
        ToneV1::Supportive => (
            "I can help with that. Based on the evidence, here is the clearest answer:",
            "Happy to help. Based on the evidence, here is the clearest answer:",
        ),
        ToneV1::Neutral => (
            "Based on the evidence, here is the clearest answer:",
            "From the available evidence, here is the best-supported answer:",
        ),
        ToneV1::Direct => (
            "The evidence points to this answer:",
            "Most directly, the evidence supports this answer:",
        ),
        ToneV1::Cautious => (
            "From the available evidence, this is the most supported answer:",
            "With the current evidence, this is the safest answer:",
        ),
    }
}

fn build_evidence_and_plan() -> (EvidenceBundleV1, AnswerPlanV1) {
    let query_id = blake3_hash(b"q");
    let snapshot_id = blake3_hash(b"s");
    let limits = EvidenceLimitsV1 {
        segments_touched: 0,
        max_items: 8,
        max_bytes: 0,
    };
    let mut evidence = EvidenceBundleV1::new(query_id, snapshot_id, limits, 1);
    evidence.items.push(EvidenceItemV1 {
        score: 10,
        data: EvidenceItemDataV1::Proof(ProofRefV1 {
            proof_id: blake3_hash(b"proof"),
        }),
    });

    let eb_bytes = evidence.encode().unwrap();
    let evidence_bundle_id = blake3_hash(&eb_bytes);

    let mut plan = AnswerPlanV1::new(
        query_id,
        snapshot_id,
        evidence_bundle_id,
        evidence.items.len() as u32,
    );
    let mut it = AnswerPlanItemV1::new(AnswerPlanItemKindV1::Summary);
    it.strength = 500;
    it.evidence_item_ix.push(0);
    plan.items.push(it);

    (evidence, plan)
}

fn build_evidence_and_plan_with_bullet() -> (EvidenceBundleV1, AnswerPlanV1) {
    let query_id = blake3_hash(b"q-bullet");
    let snapshot_id = blake3_hash(b"s-bullet");
    let limits = EvidenceLimitsV1 {
        segments_touched: 0,
        max_items: 8,
        max_bytes: 0,
    };
    let mut evidence = EvidenceBundleV1::new(query_id, snapshot_id, limits, 1);
    evidence.items.push(EvidenceItemV1 {
        score: 10,
        data: EvidenceItemDataV1::Proof(ProofRefV1 {
            proof_id: blake3_hash(b"proof-bullet-0"),
        }),
    });
    evidence.items.push(EvidenceItemV1 {
        score: 9,
        data: EvidenceItemDataV1::Proof(ProofRefV1 {
            proof_id: blake3_hash(b"proof-bullet-1"),
        }),
    });

    let eb_bytes = evidence.encode().unwrap();
    let evidence_bundle_id = blake3_hash(&eb_bytes);

    let mut plan = AnswerPlanV1::new(
        query_id,
        snapshot_id,
        evidence_bundle_id,
        evidence.items.len() as u32,
    );
    let mut summary = AnswerPlanItemV1::new(AnswerPlanItemKindV1::Summary);
    summary.strength = 500;
    summary.evidence_item_ix.push(0);
    plan.items.push(summary);

    let mut bullet = AnswerPlanItemV1::new(AnswerPlanItemKindV1::Bullet);
    bullet.strength = 400;
    bullet.evidence_item_ix.push(1);
    plan.items.push(bullet);

    (evidence, plan)
}

fn build_directives(tone: ToneV1) -> RealizerDirectivesV1 {
    RealizerDirectivesV1 {
        version: REALIZER_DIRECTIVES_V1_VERSION,
        tone,
        style: StyleV1::Default,
        format_flags: 0,
        max_softeners: 0,
        max_preface_sentences: 1,
        max_hedges: 0,
        max_questions: 0,
        rationale_codes: Vec::new(),
    }
}

fn build_valid_opener_hint(query_id: Hash32, choice_id: Id64) -> MarkovHintsV1 {
    MarkovHintsV1 {
        version: 1,
        query_id,
        flags: 0,
        order_n: 1,
        state_id: Id64(0),
        model_hash: [0u8; 32],
        context_hash: [0u8; 32],
        choices: vec![MarkovChoiceV1::new(
            MarkovChoiceKindV1::Opener,
            choice_id,
            10,
            0,
        )],
    }
}

fn build_transition_hint(query_id: Hash32, choice_id: Id64) -> MarkovHintsV1 {
    MarkovHintsV1 {
        version: 1,
        query_id,
        flags: 0,
        order_n: 1,
        state_id: Id64(0),
        model_hash: [0u8; 32],
        context_hash: [0u8; 32],
        choices: vec![MarkovChoiceV1::new(
            MarkovChoiceKindV1::Transition,
            choice_id,
            10,
            0,
        )],
    }
}

fn build_closer_hint(query_id: Hash32, choice_id: Id64) -> MarkovHintsV1 {
    MarkovHintsV1 {
        version: 1,
        query_id,
        flags: 0,
        order_n: 1,
        state_id: Id64(0),
        model_hash: [0u8; 32],
        context_hash: [0u8; 32],
        choices: vec![MarkovChoiceV1::new(
            MarkovChoiceKindV1::Closer,
            choice_id,
            10,
            0,
        )],
    }
}

fn build_invalid_opener_hint(query_id: Hash32, choice_id: Id64) -> MarkovHintsV1 {
    // order_n=0 is invalid and should cause the realizer to ignore hints.
    MarkovHintsV1 {
        version: 1,
        query_id,
        flags: 0,
        order_n: 0,
        state_id: Id64(0),
        model_hash: [0u8; 32],
        context_hash: [0u8; 32],
        choices: vec![MarkovChoiceV1::new(
            MarkovChoiceKindV1::Opener,
            choice_id,
            10,
            0,
        )],
    }
}

#[test]
fn markov_none_matches_legacy_api() {
    let store = NoopStore;
    let mut cfg = RealizerCfgV1::new();
    cfg.load_frame_rows = false;

    let (evidence, plan) = build_evidence_and_plan();

    for &tone in [
        ToneV1::Supportive,
        ToneV1::Neutral,
        ToneV1::Direct,
        ToneV1::Cautious,
    ]
    .iter()
    {
        let d = build_directives(tone);
        let a = realize_answer_plan_v1_with_directives(&store, &evidence, &plan, &cfg, Some(&d))
            .unwrap();
        let b = realize_answer_plan_v1_with_directives_and_markov(
            &store,
            &evidence,
            &plan,
            &cfg,
            Some(&d),
            None,
        )
        .unwrap();
        assert_eq!(a, b);
    }
}

#[test]
fn markov_valid_opener_selects_variant_1() {
    let store = NoopStore;
    let mut cfg = RealizerCfgV1::new();
    cfg.load_frame_rows = false;

    let (evidence, plan) = build_evidence_and_plan();

    for &tone in [
        ToneV1::Supportive,
        ToneV1::Neutral,
        ToneV1::Direct,
        ToneV1::Cautious,
    ]
    .iter()
    {
        let d = build_directives(tone);
        let (_v0, v1) = preface_variants_for_tone(tone);
        let choice_id = match tone {
            ToneV1::Supportive => derive_id64(b"markov_choice_v1", b"preface:supportive:1"),
            ToneV1::Neutral => derive_id64(b"markov_choice_v1", b"preface:neutral:1"),
            ToneV1::Direct => derive_id64(b"markov_choice_v1", b"preface:direct:1"),
            ToneV1::Cautious => derive_id64(b"markov_choice_v1", b"preface:cautious:1"),
        };
        let mh = build_valid_opener_hint(plan.query_id, choice_id);

        let out = realize_answer_plan_v1_with_directives_and_markov(
            &store,
            &evidence,
            &plan,
            &cfg,
            Some(&d),
            Some(&mh),
        )
        .unwrap();
        assert!(out.contains(v1));
    }
}

#[test]
fn markov_invalid_hints_are_ignored() {
    let store = NoopStore;
    let mut cfg = RealizerCfgV1::new();
    cfg.load_frame_rows = false;

    let (evidence, plan) = build_evidence_and_plan();

    for &tone in [
        ToneV1::Supportive,
        ToneV1::Neutral,
        ToneV1::Direct,
        ToneV1::Cautious,
    ]
    .iter()
    {
        let d = build_directives(tone);
        let (v0, _v1) = preface_variants_for_tone(tone);
        let choice_id = match tone {
            ToneV1::Supportive => derive_id64(b"markov_choice_v1", b"preface:supportive:1"),
            ToneV1::Neutral => derive_id64(b"markov_choice_v1", b"preface:neutral:1"),
            ToneV1::Direct => derive_id64(b"markov_choice_v1", b"preface:direct:1"),
            ToneV1::Cautious => derive_id64(b"markov_choice_v1", b"preface:cautious:1"),
        };
        let mh = build_invalid_opener_hint(plan.query_id, choice_id);

        let out = realize_answer_plan_v1_with_directives_and_markov(
            &store,
            &evidence,
            &plan,
            &cfg,
            Some(&d),
            Some(&mh),
        )
        .unwrap();
        assert!(out.contains(v0));
    }
}

fn build_evidence_and_plan_with_caveat() -> (EvidenceBundleV1, AnswerPlanV1) {
    let query_id = blake3_hash(b"q-caveat");
    let snapshot_id = blake3_hash(b"s-caveat");
    let limits = EvidenceLimitsV1 {
        segments_touched: 0,
        max_items: 8,
        max_bytes: 0,
    };
    let mut evidence = EvidenceBundleV1::new(query_id, snapshot_id, limits, 1);
    evidence.items.push(EvidenceItemV1 {
        score: 10,
        data: EvidenceItemDataV1::Proof(ProofRefV1 {
            proof_id: blake3_hash(b"proof-caveat-0"),
        }),
    });

    let eb_bytes = evidence.encode().unwrap();
    let evidence_bundle_id = blake3_hash(&eb_bytes);

    let mut plan = AnswerPlanV1::new(
        query_id,
        snapshot_id,
        evidence_bundle_id,
        evidence.items.len() as u32,
    );
    let mut caveat = AnswerPlanItemV1::new(AnswerPlanItemKindV1::Caveat);
    caveat.strength = 300;
    caveat.evidence_item_ix.push(0);
    plan.items.push(caveat);

    (evidence, plan)
}

#[test]
fn markov_valid_transition_selects_alternate_details_heading() {
    let store = NoopStore;
    let mut cfg = RealizerCfgV1::new();
    cfg.load_frame_rows = false;

    let (evidence, plan) = build_evidence_and_plan_with_bullet();
    let d = build_directives(ToneV1::Neutral);
    let choice_id = derive_id64(b"markov_choice_v1", b"transition:details_heading:1");
    let mh = build_transition_hint(plan.query_id, choice_id);

    let out = realize_answer_plan_v1_with_directives_and_markov(
        &store,
        &evidence,
        &plan,
        &cfg,
        Some(&d),
        Some(&mh),
    )
    .unwrap();
    assert!(out.contains("More detail"));
    assert!(!out.contains("Supporting points"));
}

#[test]
fn markov_valid_closer_selects_alternate_caveat_heading() {
    let store = NoopStore;
    let mut cfg = RealizerCfgV1::new();
    cfg.load_frame_rows = false;

    let (evidence, plan) = build_evidence_and_plan_with_caveat();
    let d = build_directives(ToneV1::Cautious);
    let choice_id = derive_id64(b"markov_choice_v1", b"closer:caveat_heading:1");
    let mh = build_closer_hint(plan.query_id, choice_id);

    let out = realize_answer_plan_v1_with_directives_and_markov(
        &store,
        &evidence,
        &plan,
        &cfg,
        Some(&d),
        Some(&mh),
    )
    .unwrap();
    assert!(out.contains("Final notes"));
    assert!(!out.contains("Things to keep in mind"));
}
