// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Golden pack conversation runner.
//!
//! The conversation runner executes:
//! - the v1 scale-demo golden pack
//! - the v1 turn-pairs golden pack
//!
//! It stores a GoldenPackConversationReportV1 that embeds both sub-reports.

use crate::artifact::ArtifactStore;
use crate::golden_pack_conversation::{
    GoldenPackConversationReportV1, GOLDEN_PACK_CONVERSATION_REPORT_V1_VERSION,
};
use crate::golden_pack_conversation_artifact::{
    put_golden_pack_conversation_report_v1, GoldenPackConversationArtifactError,
};
use crate::golden_pack_run::{
    run_golden_pack_v1, GoldenPackRunCfgV1, GoldenPackRunError, GoldenPackRunOutputV1,
};
use crate::golden_pack_turn_pairs_run::{
    run_golden_pack_turn_pairs_v1, GoldenPackTurnPairsRunCfgV1,
    GoldenPackTurnPairsRunError, GoldenPackTurnPairsRunOutputV1,
};
use crate::hash::{hex32, Hash32};

/// Golden conversation run config version.
pub const GOLDEN_PACK_CONVERSATION_RUN_CFG_V1_VERSION: u16 = 1;

/// Golden conversation run config (v1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoldenPackConversationRunCfgV1 {
    /// Schema version. Must equal `GOLDEN_PACK_CONVERSATION_RUN_CFG_V1_VERSION`.
    pub version: u16,
    /// Pack name to embed into the report.
    pub pack_name: String,
    /// Scale-demo golden pack config.
    pub golden_pack_cfg: GoldenPackRunCfgV1,
    /// Turn-pairs golden pack config.
    pub turn_pairs_cfg: GoldenPackTurnPairsRunCfgV1,
}

impl GoldenPackConversationRunCfgV1 {
    /// Default "tiny" conversation golden pack.
    pub fn default_tiny_v1() -> Self {
        Self {
            version: GOLDEN_PACK_CONVERSATION_RUN_CFG_V1_VERSION,
            pack_name: "golden_pack_conversation_v1_tiny".to_string(),
            golden_pack_cfg: GoldenPackRunCfgV1::default_tiny_v1(),
            turn_pairs_cfg: GoldenPackTurnPairsRunCfgV1::default_tiny_v1(),
        }
    }
}

/// Output of a conversation golden pack run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoldenPackConversationRunOutputV1 {
    /// Stored report hash.
    pub report_hash: Hash32,
    /// Stored report struct.
    pub report: GoldenPackConversationReportV1,
    /// Stored sub-report hash for the scale-demo golden pack.
    pub golden_pack_report_hash: Hash32,
    /// Stored sub-report hash for the turn-pairs golden pack.
    pub turn_pairs_report_hash: Hash32,
}

/// Errors from running the conversation golden pack.
#[derive(Debug)]
pub enum GoldenPackConversationRunError {
    /// Config is invalid.
    Cfg(&'static str),
    /// Running the scale-demo golden pack failed.
    GoldenPack(GoldenPackRunError),
    /// Running the turn-pairs golden pack failed.
    TurnPairs(GoldenPackTurnPairsRunError),
    /// Storing the conversation report failed.
    ReportStore(GoldenPackConversationArtifactError),
}

impl core::fmt::Display for GoldenPackConversationRunError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            GoldenPackConversationRunError::Cfg(msg) => f.write_str(msg),
            GoldenPackConversationRunError::GoldenPack(e) => core::fmt::Display::fmt(e, f),
            GoldenPackConversationRunError::TurnPairs(e) => core::fmt::Display::fmt(e, f),
            GoldenPackConversationRunError::ReportStore(e) => core::fmt::Display::fmt(e, f),
        }
    }
}

impl From<GoldenPackRunError> for GoldenPackConversationRunError {
    fn from(value: GoldenPackRunError) -> Self {
        GoldenPackConversationRunError::GoldenPack(value)
    }
}

impl From<GoldenPackTurnPairsRunError> for GoldenPackConversationRunError {
    fn from(value: GoldenPackTurnPairsRunError) -> Self {
        GoldenPackConversationRunError::TurnPairs(value)
    }
}

impl From<GoldenPackConversationArtifactError> for GoldenPackConversationRunError {
    fn from(value: GoldenPackConversationArtifactError) -> Self {
        GoldenPackConversationRunError::ReportStore(value)
    }
}

/// Run the v1 conversation golden pack in-process, store artifacts, and return the output.
pub fn run_golden_pack_conversation_v1<S: ArtifactStore>(
    store: &S,
    cfg: GoldenPackConversationRunCfgV1,
) -> Result<GoldenPackConversationRunOutputV1, GoldenPackConversationRunError> {
    if cfg.version != GOLDEN_PACK_CONVERSATION_RUN_CFG_V1_VERSION {
        return Err(GoldenPackConversationRunError::Cfg(
            "unsupported golden conversation run cfg version",
        ));
    }

    let gp: GoldenPackRunOutputV1 = run_golden_pack_v1(store, cfg.golden_pack_cfg.clone())?;
    let tp: GoldenPackTurnPairsRunOutputV1 =
        run_golden_pack_turn_pairs_v1(store, cfg.turn_pairs_cfg.clone())?;

    let rep = GoldenPackConversationReportV1 {
        version: GOLDEN_PACK_CONVERSATION_REPORT_V1_VERSION,
        pack_name: cfg.pack_name.clone(),
        golden_pack_report_hash: gp.report_hash,
        golden_pack_report: gp.report,
        turn_pairs_report_hash: tp.report_hash,
        turn_pairs_report: tp.report,
    };
    let report_hash = put_golden_pack_conversation_report_v1(store, &rep)?;

    Ok(GoldenPackConversationRunOutputV1 {
        report_hash,
        report: rep,
        golden_pack_report_hash: gp.report_hash,
        turn_pairs_report_hash: tp.report_hash,
    })
}

/// Render a stable, single-line summary of a conversation golden pack run.
pub fn format_golden_pack_conversation_run_line(out: &GoldenPackConversationRunOutputV1) -> String {
    format!(
        "golden_pack_conversation_report_v1 report={} golden_pack_report={} turn_pairs_report={}",
        hex32(&out.report_hash),
        hex32(&out.golden_pack_report_hash),
        hex32(&out.turn_pairs_report_hash),
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
    fn golden_pack_conversation_v1_is_deterministic_over_two_runs() {
        let cfg = GoldenPackConversationRunCfgV1::default_tiny_v1();
        let d1 = tmp_dir("golden_pack_conversation_v1_is_deterministic_over_two_runs_1");
        let d2 = tmp_dir("golden_pack_conversation_v1_is_deterministic_over_two_runs_2");
        let s1 = FsArtifactStore::new(&d1).unwrap();
        let s2 = FsArtifactStore::new(&d2).unwrap();

        let o1 = run_golden_pack_conversation_v1(&s1, cfg.clone()).unwrap();
        let o2 = run_golden_pack_conversation_v1(&s2, cfg).unwrap();

        assert_eq!(o1.report_hash, o2.report_hash);
        assert_eq!(o1.report, o2.report);
        assert_eq!(o1.golden_pack_report_hash, o2.golden_pack_report_hash);
        assert_eq!(o1.turn_pairs_report_hash, o2.turn_pairs_report_hash);
    }
}
