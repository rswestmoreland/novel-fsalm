// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Golden pack runner.
//!
//! The v1 runner executes the scale-demo full loop in-process and stores:
//! - a `ScaleDemoScaleReportV1` artifact
//! - a `GoldenPackReportV1` artifact
//!
//! The golden pack runner forces evidence-stage environment overrides to their
//! defaults (by setting them to 0 for the duration of the run).

use crate::artifact::ArtifactStore;
use crate::golden_pack::{GoldenPackReportV1, GOLDEN_PACK_REPORT_V1_VERSION};
use crate::golden_pack_artifact::{put_golden_pack_report_v1, GoldenPackArtifactError};
use crate::hash::{hex32, Hash32};
use crate::scale_demo::{
    build_scale_demo_scale_report_v1,
    run_scale_demo_build_answers_v1_with_directives,
    run_scale_demo_build_evidence_bundles_v1, run_scale_demo_build_index_from_manifest_v1,
    run_scale_demo_generate_and_ingest_frames_v1, run_scale_demo_generate_and_store_prompts_v1,
    ScaleDemoAnswerError, ScaleDemoCfgV1, ScaleDemoEvidenceError, ScaleDemoIngestError,
    ScaleDemoIndexError, ScaleDemoPromptsError, ScaleDemoScaleReportError,
};
use crate::realizer_directives::{
    RealizerDirectivesV1, REALIZER_DIRECTIVES_V1_VERSION, StyleV1, ToneV1,
    FORMAT_FLAG_BULLETS, FORMAT_FLAG_INCLUDE_ASSUMPTIONS, FORMAT_FLAG_INCLUDE_NEXT_STEPS,
    FORMAT_FLAG_INCLUDE_RISKS,
    FORMAT_FLAG_INCLUDE_SUMMARY, FORMAT_FLAG_NUMBERED,
};
use crate::scale_report::ScaleDemoScaleReportV1;
use crate::scale_report_artifact::{
    put_scale_demo_scale_report_v1, ScaleReportArtifactError,
};
use crate::workload_gen::WorkloadCfgV1;
use crate::workload_gen::WORKLOAD_GEN_V1_VERSION;

/// Golden pack run config version.
pub const GOLDEN_PACK_RUN_CFG_V1_VERSION: u16 = 1;

/// Golden pack run config (v1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoldenPackRunCfgV1 {
    /// Schema version. Must equal `GOLDEN_PACK_RUN_CFG_V1_VERSION`.
    pub version: u16,
    /// Pack name to embed into the report.
    pub pack_name: String,
    /// Workload config (seed + sizes).
    pub workload: WorkloadCfgV1,
}

impl GoldenPackRunCfgV1 {
    /// Default "tiny" golden pack (fast enough for tests).
    pub fn default_tiny_v1() -> Self {
        Self {
            version: GOLDEN_PACK_RUN_CFG_V1_VERSION,
            pack_name: "golden_pack_v1_tiny".to_string(),
            workload: WorkloadCfgV1 {
                version: WORKLOAD_GEN_V1_VERSION,
                seed: 7,
                doc_count: 32,
                query_count: 16,
                min_tokens_per_doc: 24,
                max_tokens_per_doc: 48,
                vocab_size: 512,
                query_tokens: 6,
                include_tie_pair: 1,
            },
        }
    }
}

/// Output of a golden pack run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoldenPackRunOutputV1 {
    /// Stored golden pack report hash.
    pub report_hash: Hash32,
    /// Stored scale report hash.
    pub scale_report_hash: Hash32,
    /// Golden pack report struct.
    pub report: GoldenPackReportV1,
}

/// Errors from running the golden pack.
#[derive(Debug)]
pub enum GoldenPackRunError {
    /// Config is invalid.
    Cfg(&'static str),
    /// Scale demo frames+ingest stage failed.
    Frames(ScaleDemoIngestError),
    /// Scale demo index stage failed.
    Index(ScaleDemoIndexError),
    /// Scale demo prompts stage failed.
    Prompts(ScaleDemoPromptsError),
    /// Scale demo evidence stage failed.
    Evidence(ScaleDemoEvidenceError),
    /// Scale demo answers stage failed.
    Answers(ScaleDemoAnswerError),
    /// Scale report build failed.
    ScaleReportBuild(ScaleDemoScaleReportError),
    /// Scale report artifact operation failed.
    ScaleReportArtifact(ScaleReportArtifactError),
    /// Golden pack report artifact operation failed.
    GoldenReportArtifact(GoldenPackArtifactError),
}

impl core::fmt::Display for GoldenPackRunError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            GoldenPackRunError::Cfg(msg) => f.write_str(msg),
            GoldenPackRunError::Frames(e) => core::fmt::Display::fmt(e, f),
            GoldenPackRunError::Index(e) => core::fmt::Display::fmt(e, f),
            GoldenPackRunError::Prompts(e) => core::fmt::Display::fmt(e, f),
            GoldenPackRunError::Evidence(e) => core::fmt::Display::fmt(e, f),
            GoldenPackRunError::Answers(e) => core::fmt::Display::fmt(e, f),
            GoldenPackRunError::ScaleReportBuild(e) => core::fmt::Display::fmt(e, f),
            GoldenPackRunError::ScaleReportArtifact(e) => core::fmt::Display::fmt(e, f),
            GoldenPackRunError::GoldenReportArtifact(e) => core::fmt::Display::fmt(e, f),
        }
    }
}

impl From<ScaleDemoIngestError> for GoldenPackRunError {
    fn from(value: ScaleDemoIngestError) -> Self {
        GoldenPackRunError::Frames(value)
    }
}

impl From<ScaleDemoIndexError> for GoldenPackRunError {
    fn from(value: ScaleDemoIndexError) -> Self {
        GoldenPackRunError::Index(value)
    }
}

impl From<ScaleDemoPromptsError> for GoldenPackRunError {
    fn from(value: ScaleDemoPromptsError) -> Self {
        GoldenPackRunError::Prompts(value)
    }
}

impl From<ScaleDemoEvidenceError> for GoldenPackRunError {
    fn from(value: ScaleDemoEvidenceError) -> Self {
        GoldenPackRunError::Evidence(value)
    }
}

impl From<ScaleDemoAnswerError> for GoldenPackRunError {
    fn from(value: ScaleDemoAnswerError) -> Self {
        GoldenPackRunError::Answers(value)
    }
}

impl From<ScaleDemoScaleReportError> for GoldenPackRunError {
    fn from(value: ScaleDemoScaleReportError) -> Self {
        GoldenPackRunError::ScaleReportBuild(value)
    }
}

impl From<ScaleReportArtifactError> for GoldenPackRunError {
    fn from(value: ScaleReportArtifactError) -> Self {
        GoldenPackRunError::ScaleReportArtifact(value)
    }
}

impl From<GoldenPackArtifactError> for GoldenPackRunError {
    fn from(value: GoldenPackArtifactError) -> Self {
        GoldenPackRunError::GoldenReportArtifact(value)
    }
}

struct SavedEnv {
    key: &'static str,
    prev: Option<String>,
}

fn set_env(key: &'static str, value: &str) -> SavedEnv {
    let prev = std::env::var(key).ok();
    std::env::set_var(key, value);
    SavedEnv { key, prev }
}

fn restore_env(saved: SavedEnv) {
    match saved.prev {
        Some(v) => std::env::set_var(saved.key, v),
        None => std::env::remove_var(saved.key),
    }
}

fn golden_pack_realizer_directives_v1() -> RealizerDirectivesV1 {
    RealizerDirectivesV1 {
        version: REALIZER_DIRECTIVES_V1_VERSION,
        tone: ToneV1::Neutral,
        style: StyleV1::Debug,
        format_flags: FORMAT_FLAG_INCLUDE_SUMMARY
            | FORMAT_FLAG_INCLUDE_NEXT_STEPS
            | FORMAT_FLAG_INCLUDE_RISKS | FORMAT_FLAG_INCLUDE_ASSUMPTIONS
            | FORMAT_FLAG_BULLETS
            | FORMAT_FLAG_NUMBERED,
        max_softeners: 0,
        max_preface_sentences: 0,
        max_hedges: 0,
        max_questions: 0,
        rationale_codes: Vec::new(),
    }
}

/// Run the v1 golden pack in-process, store artifacts, and return the output.
pub fn run_golden_pack_v1<S: ArtifactStore>(
    store: &S,
    cfg: GoldenPackRunCfgV1,
) -> Result<GoldenPackRunOutputV1, GoldenPackRunError> {
    if cfg.version != GOLDEN_PACK_RUN_CFG_V1_VERSION {
        return Err(GoldenPackRunError::Cfg("unsupported golden pack run cfg version"));
    }

    // Force evidence-stage overrides to defaults.
    let e1 = set_env("FSA_LM_SCALE_DEMO_EVIDENCE_K", "0");
    let e2 = set_env("FSA_LM_SCALE_DEMO_EVIDENCE_MAX_BYTES", "0");
    let e3 = set_env("FSA_LM_SCALE_DEMO_EVIDENCE_FRAME_CACHE_BYTES", "0");

    let out = (|| {
        let scale_cfg = ScaleDemoCfgV1 {
            version: crate::scale_demo::SCALE_DEMO_V1_VERSION,
            workload: cfg.workload.clone(),
        };

        let (gen_report, frames_report) =
            run_scale_demo_generate_and_ingest_frames_v1(store, scale_cfg.clone())?;
        let index_report =
            run_scale_demo_build_index_from_manifest_v1(store, &frames_report.frame_manifest_hash)?;
        let prompts_report = run_scale_demo_generate_and_store_prompts_v1(store, scale_cfg.clone())?;
        let evidence_report = run_scale_demo_build_evidence_bundles_v1(
            store,
            scale_cfg.clone(),
            &index_report.index_snapshot_hash,
            &index_report.index_sig_map_hash,
        )?;
        let directives = golden_pack_realizer_directives_v1();
        let answers_report =
            run_scale_demo_build_answers_v1_with_directives(store, &evidence_report, &directives)?;

        let scale_report: ScaleDemoScaleReportV1 = build_scale_demo_scale_report_v1(
            &gen_report,
            &frames_report,
            Some(&index_report),
            Some(&prompts_report),
            Some(&evidence_report),
            Some(&answers_report),
        )?;

        let scale_report_hash = put_scale_demo_scale_report_v1(store, &scale_report)?;

        let rep = GoldenPackReportV1 {
            version: GOLDEN_PACK_REPORT_V1_VERSION,
            pack_name: cfg.pack_name.clone(),
            scale_report_hash,
            scale_report,
        };
        let report_hash = put_golden_pack_report_v1(store, &rep)?;

        Ok(GoldenPackRunOutputV1 {
            report_hash,
            scale_report_hash,
            report: rep,
        })
    })();

    // Restore env.
    restore_env(e3);
    restore_env(e2);
    restore_env(e1);

    out
}

/// Render a stable, single-line summary of a golden pack run.
pub fn format_golden_pack_run_line(out: &GoldenPackRunOutputV1) -> String {
    format!(
        "golden_pack_report_v1 report={} scale_report={} workload={} docs={} queries={} tie_pair={}",
        hex32(&out.report_hash),
        hex32(&out.scale_report_hash),
        hex32(&out.report.scale_report.workload_hash),
        out.report.scale_report.doc_count,
        out.report.scale_report.query_count,
        out.report.scale_report.tie_pair
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
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
    fn golden_pack_v1_is_deterministic_over_two_runs() {
        let cfg = GoldenPackRunCfgV1::default_tiny_v1();

        let d1 = tmp_dir("golden_pack_v1_is_deterministic_over_two_runs_1");
        let d2 = tmp_dir("golden_pack_v1_is_deterministic_over_two_runs_2");
        let s1 = FsArtifactStore::new(&d1).unwrap();
        let s2 = FsArtifactStore::new(&d2).unwrap();

        let o1 = run_golden_pack_v1(&s1, cfg.clone()).unwrap();
        let o2 = run_golden_pack_v1(&s2, cfg).unwrap();

        assert_eq!(o1.report_hash, o2.report_hash);
        assert_eq!(o1.scale_report_hash, o2.scale_report_hash);
        assert_eq!(o1.report, o2.report);
    }
}
