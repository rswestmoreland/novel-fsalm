// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Golden pack "turn-pairs" runner.
//!
//! This runner executes a deterministic two-turn answer workload and stores:
//! - answer text artifacts (2 turns)
//! - markov hints artifact (second turn only)
//! - markov trace artifacts (2 turns)
//! - a GoldenPackTurnPairsReportV1 artifact
//!
//! The intent is to cover the Markov opener surface-template selection path
//! (variant 0 vs 1) in a regression-friendly pack.

use crate::artifact::ArtifactStore;
use crate::golden_pack_turn_pairs::{
    GoldenPackTurnPairsReportV1, GOLDEN_PACK_TURN_PAIRS_REPORT_V1_VERSION,
};
use crate::golden_pack_turn_pairs_artifact::{
    put_golden_pack_turn_pairs_report_v1, GoldenPackTurnPairsArtifactError,
};
use crate::hash::{hex32, Hash32};
use crate::markov_hints::{
    MarkovChoiceKindV1, MarkovHintsFlagsV1, MH_FLAG_HAS_PRAGMATICS,
};
use crate::markov_model::{MarkovModelV1, MarkovNextV1, MarkovStateV1, MarkovTokenV1};
use crate::markov_model_artifact::{put_markov_model_v1, MarkovModelArtifactError};
use crate::markov_runtime::derive_markov_hints_opener_preface_v1;
use crate::markov_trace::MarkovTraceV1;
use crate::markov_trace_artifact::{put_markov_trace_v1, MarkovTraceArtifactError};
use crate::markov_hints_artifact::MarkovHintsArtifactError;
use crate::evidence_artifact::EvidenceArtifactError;
use crate::planner_v1::{
    plan_from_evidence_bundle_v1_with_guidance, PlannerCfgV1, PlannerV1Error,
};
use crate::realizer_directives::{
    RealizerDirectivesV1, REALIZER_DIRECTIVES_V1_VERSION, StyleV1, ToneV1,
    FORMAT_FLAG_INCLUDE_SUMMARY,
};
use crate::realizer_directives_artifact::{
    put_realizer_directives_v1, RealizerDirectivesArtifactError,
};
use crate::quality_gate_v1::build_markov_trace_tokens_v1;
use crate::realizer_v1::{
    append_clarifying_question_v1_with_markov_events,
    realize_answer_plan_v1_with_directives_and_markov_events, RealizerCfgV1, RealizerV1Error,
};
use crate::scale_demo::{
    run_scale_demo_build_evidence_bundles_v1, run_scale_demo_build_index_from_manifest_v1,
    run_scale_demo_generate_and_ingest_frames_v1, ScaleDemoCfgV1, ScaleDemoEvidenceError,
    ScaleDemoIndexError, ScaleDemoIngestError,
};
use crate::scale_report::HashListSummaryV1;
use crate::workload_gen::{WorkloadCfgV1, WORKLOAD_GEN_V1_VERSION};

/// Golden turn-pairs run config version.
pub const GOLDEN_PACK_TURN_PAIRS_RUN_CFG_V1_VERSION: u16 = 1;

/// Golden turn-pairs run config (v1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoldenPackTurnPairsRunCfgV1 {
    /// Schema version. Must equal `GOLDEN_PACK_TURN_PAIRS_RUN_CFG_V1_VERSION`.
    pub version: u16,
    /// Pack name to embed into the report.
    pub pack_name: String,
    /// Workload config (seed + sizes). query_count is forced to 2.
    pub workload: WorkloadCfgV1,
}

impl GoldenPackTurnPairsRunCfgV1 {
    /// Default "tiny" turn-pairs golden pack.
    pub fn default_tiny_v1() -> Self {
        Self {
            version: GOLDEN_PACK_TURN_PAIRS_RUN_CFG_V1_VERSION,
            pack_name: "golden_pack_turn_pairs_v1_tiny".to_string(),
            workload: WorkloadCfgV1 {
                version: WORKLOAD_GEN_V1_VERSION,
                seed: 9,
                doc_count: 16,
                query_count: 2,
                min_tokens_per_doc: 20,
                max_tokens_per_doc: 40,
                vocab_size: 256,
                query_tokens: 6,
                include_tie_pair: 0,
            },
        }
    }
}

/// Output of a turn-pairs golden pack run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoldenPackTurnPairsRunOutputV1 {
    /// Stored report hash.
    pub report_hash: Hash32,
    /// Stored report struct.
    pub report: GoldenPackTurnPairsReportV1,
    /// Stored MarkovModelV1 hash used by the second turn.
    pub markov_model_hash: Hash32,
    /// Answer hashes for the two turns.
    pub answer_hashes: [Hash32; 2],
    /// MarkovTrace hashes for the two turns.
    pub trace_hashes: [Hash32; 2],
    /// MarkovHints hash for turn 2 (all-zero if absent).
    pub markov_hints_hash_turn2: Hash32,
}

/// Errors from running the turn-pairs golden pack.
#[derive(Debug)]
pub enum GoldenPackTurnPairsRunError {
    /// Config is invalid.
    Cfg(&'static str),
    /// Scale demo frames+ingest stage failed.
    Frames(ScaleDemoIngestError),
    /// Scale demo index stage failed.
    Index(ScaleDemoIndexError),
    /// Scale demo evidence stage failed.
    Evidence(ScaleDemoEvidenceError),
    /// Planner failed.
    Planner(PlannerV1Error),
    /// Evidence bundle artifact failed.
    EvidenceArtifact(EvidenceArtifactError),
    /// Realizer failed.
    Realizer(RealizerV1Error),
    /// Store directives failed.
    DirectivesStore(RealizerDirectivesArtifactError),
    /// Store Markov model failed.
    MarkovModelStore(MarkovModelArtifactError),
    /// Store Markov trace failed.
    MarkovTraceStore(MarkovTraceArtifactError),
    /// Store Markov hints failed.
    MarkovHintsStore(MarkovHintsArtifactError),
    /// Store report failed.
    ReportStore(GoldenPackTurnPairsArtifactError),
    /// Store operation failed.
    Store(crate::artifact::ArtifactError),
}

impl core::fmt::Display for GoldenPackTurnPairsRunError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            GoldenPackTurnPairsRunError::Cfg(msg) => f.write_str(msg),
            GoldenPackTurnPairsRunError::Frames(e) => core::fmt::Display::fmt(e, f),
            GoldenPackTurnPairsRunError::Index(e) => core::fmt::Display::fmt(e, f),
            GoldenPackTurnPairsRunError::Evidence(e) => core::fmt::Display::fmt(e, f),
            GoldenPackTurnPairsRunError::Planner(e) => core::fmt::Display::fmt(e, f),
            GoldenPackTurnPairsRunError::EvidenceArtifact(e) => core::fmt::Display::fmt(e, f),
            GoldenPackTurnPairsRunError::Realizer(e) => core::fmt::Display::fmt(e, f),
            GoldenPackTurnPairsRunError::DirectivesStore(e) => core::fmt::Display::fmt(e, f),
            GoldenPackTurnPairsRunError::MarkovModelStore(e) => core::fmt::Display::fmt(e, f),
            GoldenPackTurnPairsRunError::MarkovTraceStore(e) => core::fmt::Display::fmt(e, f),
            GoldenPackTurnPairsRunError::MarkovHintsStore(e) => core::fmt::Display::fmt(e, f),
            GoldenPackTurnPairsRunError::ReportStore(e) => core::fmt::Display::fmt(e, f),
            GoldenPackTurnPairsRunError::Store(e) => core::fmt::Display::fmt(e, f),
        }
    }
}

impl From<ScaleDemoIngestError> for GoldenPackTurnPairsRunError {
    fn from(value: ScaleDemoIngestError) -> Self {
        GoldenPackTurnPairsRunError::Frames(value)
    }
}

impl From<ScaleDemoIndexError> for GoldenPackTurnPairsRunError {
    fn from(value: ScaleDemoIndexError) -> Self {
        GoldenPackTurnPairsRunError::Index(value)
    }
}

impl From<ScaleDemoEvidenceError> for GoldenPackTurnPairsRunError {
    fn from(value: ScaleDemoEvidenceError) -> Self {
        GoldenPackTurnPairsRunError::Evidence(value)
    }
}

impl From<PlannerV1Error> for GoldenPackTurnPairsRunError {
    fn from(value: PlannerV1Error) -> Self {
        GoldenPackTurnPairsRunError::Planner(value)
    }
}

impl From<EvidenceArtifactError> for GoldenPackTurnPairsRunError {
    fn from(value: EvidenceArtifactError) -> Self {
        GoldenPackTurnPairsRunError::EvidenceArtifact(value)
    }
}

impl From<RealizerV1Error> for GoldenPackTurnPairsRunError {
    fn from(value: RealizerV1Error) -> Self {
        GoldenPackTurnPairsRunError::Realizer(value)
    }
}

impl From<RealizerDirectivesArtifactError> for GoldenPackTurnPairsRunError {
    fn from(value: RealizerDirectivesArtifactError) -> Self {
        GoldenPackTurnPairsRunError::DirectivesStore(value)
    }
}

impl From<MarkovModelArtifactError> for GoldenPackTurnPairsRunError {
    fn from(value: MarkovModelArtifactError) -> Self {
        GoldenPackTurnPairsRunError::MarkovModelStore(value)
    }
}

impl From<MarkovTraceArtifactError> for GoldenPackTurnPairsRunError {
    fn from(value: MarkovTraceArtifactError) -> Self {
        GoldenPackTurnPairsRunError::MarkovTraceStore(value)
    }
}

impl From<MarkovHintsArtifactError> for GoldenPackTurnPairsRunError {
    fn from(value: MarkovHintsArtifactError) -> Self {
        GoldenPackTurnPairsRunError::MarkovHintsStore(value)
    }
}

impl From<GoldenPackTurnPairsArtifactError> for GoldenPackTurnPairsRunError {
    fn from(value: GoldenPackTurnPairsArtifactError) -> Self {
        GoldenPackTurnPairsRunError::ReportStore(value)
    }
}

impl From<crate::artifact::ArtifactError> for GoldenPackTurnPairsRunError {
    fn from(value: crate::artifact::ArtifactError) -> Self {
        GoldenPackTurnPairsRunError::Store(value)
    }
}

fn turn_pairs_realizer_directives_v1() -> RealizerDirectivesV1 {
    RealizerDirectivesV1 {
        version: REALIZER_DIRECTIVES_V1_VERSION,
        tone: ToneV1::Supportive,
        style: StyleV1::Default,
        format_flags: FORMAT_FLAG_INCLUDE_SUMMARY,
        max_softeners: 0,
        max_preface_sentences: 1,
        max_hedges: 0,
        max_questions: 0,
        rationale_codes: Vec::new(),
    }
}

fn build_preface_variant1_model_v1() -> MarkovModelV1 {
    // A minimal, canonical model with an empty-context state that prefers
    // preface:supportive:1 over preface:supportive:0.
    let s0 = MarkovStateV1 {
        escape_count: 0,
        context: Vec::new(),
        next: vec![
            MarkovNextV1 {
                token: MarkovTokenV1::new(
                    MarkovChoiceKindV1::Opener,
                    crate::frame::derive_id64(b"markov_choice_v1", b"preface:supportive:1"),
                ),
                count: 2,
            },
            MarkovNextV1 {
                token: MarkovTokenV1::new(
                    MarkovChoiceKindV1::Opener,
                    crate::frame::derive_id64(b"markov_choice_v1", b"preface:supportive:0"),
                ),
                count: 1,
            },
        ],
    };
    let m = MarkovModelV1 {
        version: crate::markov_model::MARKOV_MODEL_V1_VERSION,
        order_n_max: 2,
        max_next_per_state: 8,
        total_transitions: 3,
        corpus_hash: [0u8; 32],
        states: vec![s0],
    };
    assert!(m.validate().is_ok());
    m
}

/// Run the v1 turn-pairs golden pack in-process, store artifacts, and return the output.
pub fn run_golden_pack_turn_pairs_v1<S: ArtifactStore>(
    store: &S,
    cfg: GoldenPackTurnPairsRunCfgV1,
) -> Result<GoldenPackTurnPairsRunOutputV1, GoldenPackTurnPairsRunError> {
    if cfg.version != GOLDEN_PACK_TURN_PAIRS_RUN_CFG_V1_VERSION {
        return Err(GoldenPackTurnPairsRunError::Cfg("unsupported turn-pairs cfg version"));
    }
    if cfg.workload.version != WORKLOAD_GEN_V1_VERSION {
        return Err(GoldenPackTurnPairsRunError::Cfg("unsupported workload version"));
    }
    if cfg.workload.query_count != 2 {
        return Err(GoldenPackTurnPairsRunError::Cfg("workload.query_count must be 2"));
    }

    // Build a deterministic store of docs, index, and evidence bundles.
    let scale_cfg = ScaleDemoCfgV1 {
        version: crate::scale_demo::SCALE_DEMO_V1_VERSION,
        workload: cfg.workload.clone(),
    };

    let (_gen_report, frames_report) = run_scale_demo_generate_and_ingest_frames_v1(store, scale_cfg.clone())?;
    let index_report = run_scale_demo_build_index_from_manifest_v1(store, &frames_report.frame_manifest_hash)?;
    let evidence_report = run_scale_demo_build_evidence_bundles_v1(
        store,
        scale_cfg.clone(),
        &index_report.index_snapshot_hash,
        &index_report.index_sig_map_hash,
    )?;

    // Store directives used by both turns.
    let directives = turn_pairs_realizer_directives_v1();
    let _directives_hash = put_realizer_directives_v1(store, &directives)?;

    // Store Markov model used for the second turn.
    let model = build_preface_variant1_model_v1();
    let markov_model_hash = put_markov_model_v1(store, &model)?;

    let planner_cfg = PlannerCfgV1::default_v1();
    let realizer_cfg = RealizerCfgV1::new();

    let mut answer_hashes: Vec<Hash32> = Vec::with_capacity(2);
    let mut trace_hashes: Vec<Hash32> = Vec::with_capacity(2);
    let mut hints_hashes: Vec<Hash32> = Vec::new();
    let mut hints_hash_turn2: Hash32 = [0u8; 32];

    for (turn_idx, evh) in evidence_report.evidence_hashes.iter().enumerate() {
        let bundle_opt = crate::evidence_artifact::get_evidence_bundle_v1(store, evh)?;
        let bundle = match bundle_opt {
            Some(b) => b,
            None => return Err(GoldenPackTurnPairsRunError::Cfg("missing evidence bundle")),
        };

        // The evidence bundle artifact hash is the canonical id for planning.
        let pout = plan_from_evidence_bundle_v1_with_guidance(&bundle, *evh, &planner_cfg, None)?;
        let plan = pout.plan;
        let planner_hints = pout.hints;
        let forecast = pout.forecast;

        let markov_hints_opt = if turn_idx == 1 {
            let mut flags: MarkovHintsFlagsV1 = 0;
            flags |= MH_FLAG_HAS_PRAGMATICS;
            let ctx: [MarkovTokenV1; 0] = [];
            let h = derive_markov_hints_opener_preface_v1(
                bundle.query_id,
                flags,
                markov_model_hash,
                &model,
                directives.tone,
                &ctx,
                8,
            );
            let hh = crate::markov_hints_artifact::put_markov_hints_v1(store, &h)?;
            hints_hashes.push(hh);
            hints_hash_turn2 = hh;
            Some(h)
        } else {
            None
        };

        let ro = realize_answer_plan_v1_with_directives_and_markov_events(
            store,
            &bundle,
            &plan,
            &realizer_cfg,
            Some(&directives),
            markov_hints_opt.as_ref(),
        )?;

        let mut text = ro.text;
        let mut markov_events = ro.markov;
        let did_append_q = append_clarifying_question_v1_with_markov_events(
            &mut text,
            &planner_hints,
            &forecast,
            directives.max_questions,
            markov_hints_opt.as_ref(),
            &mut markov_events,
        );

        let ah = store.put(text.as_bytes())?;
        answer_hashes.push(ah);

        // Build Markov trace tokens.
        let mt_tokens: Vec<MarkovTokenV1> = build_markov_trace_tokens_v1(
            &plan,
            &markov_events,
            did_append_q,
        );

        let trace = MarkovTraceV1 {
            version: crate::markov_trace::MARKOV_TRACE_V1_VERSION,
            query_id: bundle.query_id,
            tokens: mt_tokens,
        };
        let th = put_markov_trace_v1(store, &trace)?;
        trace_hashes.push(th);
    }

    if answer_hashes.len() != 2 || trace_hashes.len() != 2 {
        return Err(GoldenPackTurnPairsRunError::Cfg("expected exactly 2 turns"));
    }

    let rep = GoldenPackTurnPairsReportV1 {
        version: GOLDEN_PACK_TURN_PAIRS_REPORT_V1_VERSION,
        pack_name: cfg.pack_name.clone(),
        workload_hash: evidence_report.workload_hash,
        turn_count: 2,
        answers: HashListSummaryV1::from_list("answers", &answer_hashes),
        markov_traces: HashListSummaryV1::from_list("markov_traces", &trace_hashes),
        markov_hints: HashListSummaryV1::from_list("markov_hints", &hints_hashes),
    };

    let report_hash = put_golden_pack_turn_pairs_report_v1(store, &rep)?;

    let ah0 = answer_hashes[0];
    let ah1 = answer_hashes[1];
    let th0 = trace_hashes[0];
    let th1 = trace_hashes[1];

    Ok(GoldenPackTurnPairsRunOutputV1 {
        report_hash,
        report: rep,
        markov_model_hash,
        answer_hashes: [ah0, ah1],
        trace_hashes: [th0, th1],
        markov_hints_hash_turn2: hints_hash_turn2,
    })
}

/// Render a stable, single-line summary of a turn-pairs golden pack run.
pub fn format_golden_pack_turn_pairs_run_line(out: &GoldenPackTurnPairsRunOutputV1) -> String {
    format!(
        "golden_pack_turn_pairs_report_v1 report={} workload={} turns={} answers_list_hash={} markov_traces_list_hash={} markov_hints_list_hash={} markov_model={}",
        hex32(&out.report_hash),
        hex32(&out.report.workload_hash),
        out.report.turn_count,
        hex32(&out.report.answers.list_hash),
        hex32(&out.report.markov_traces.list_hash),
        hex32(&out.report.markov_hints.list_hash),
        hex32(&out.markov_model_hash),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
    use crate::markov_trace_artifact::get_markov_trace_v1;
    use std::fs;
    use std::path::PathBuf;

    fn tmp_dir(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push("fsa_lm_tests");
        p.push(name);
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn golden_turn_pairs_v1_is_deterministic_over_two_runs() {
        let cfg = GoldenPackTurnPairsRunCfgV1::default_tiny_v1();
        let d1 = tmp_dir("golden_turn_pairs_v1_is_deterministic_over_two_runs_1");
        let d2 = tmp_dir("golden_turn_pairs_v1_is_deterministic_over_two_runs_2");
        let s1 = FsArtifactStore::new(&d1).unwrap();
        let s2 = FsArtifactStore::new(&d2).unwrap();

        let o1 = run_golden_pack_turn_pairs_v1(&s1, cfg.clone()).unwrap();
        let o2 = run_golden_pack_turn_pairs_v1(&s2, cfg).unwrap();

        assert_eq!(o1.report_hash, o2.report_hash);
        assert_eq!(o1.report, o2.report);
    }

    #[test]
    fn golden_turn_pairs_v1_records_preface_variant0_then_variant1() {
        let cfg = GoldenPackTurnPairsRunCfgV1::default_tiny_v1();
        let d = tmp_dir("golden_turn_pairs_v1_records_preface_variant0_then_variant1");
        let s = FsArtifactStore::new(&d).unwrap();
        let o = run_golden_pack_turn_pairs_v1(&s, cfg).unwrap();

        let t0 = get_markov_trace_v1(&s, &o.trace_hashes[0]).unwrap().unwrap();
        let t1 = get_markov_trace_v1(&s, &o.trace_hashes[1]).unwrap().unwrap();

        assert!(!t0.tokens.is_empty());
        assert!(!t1.tokens.is_empty());

        let v0 = crate::frame::derive_id64(b"markov_choice_v1", b"preface:supportive:0");
        let v1 = crate::frame::derive_id64(b"markov_choice_v1", b"preface:supportive:1");
        assert_eq!(t0.tokens[0].choice_id, v0);
        assert_eq!(t1.tokens[0].choice_id, v1);
    }
}
