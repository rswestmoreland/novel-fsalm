// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Scale demo scale report artifact helpers.
//!
//! The scale report is a compact, canonically-encoded summary of a scale demo
//! run that is intended to be stored as a content-addressed artifact.

use crate::artifact::{ArtifactError, ArtifactStore};
use crate::codec::{DecodeError, EncodeError};
use crate::hash::Hash32;
use crate::scale_report::ScaleDemoScaleReportV1;
use core::fmt;

/// Errors while storing or loading a ScaleDemoScaleReportV1 artifact.
#[derive(Debug)]
pub enum ScaleReportArtifactError {
    /// Report could not be encoded.
    Encode(EncodeError),
    /// Report could not be decoded.
    Decode(DecodeError),
    /// Artifact store error.
    Store(ArtifactError),
}

impl fmt::Display for ScaleReportArtifactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScaleReportArtifactError::Encode(e) => write!(f, "encode: {}", e),
            ScaleReportArtifactError::Decode(e) => write!(f, "decode: {}", e),
            ScaleReportArtifactError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl std::error::Error for ScaleReportArtifactError {}

impl From<EncodeError> for ScaleReportArtifactError {
    fn from(e: EncodeError) -> Self {
        ScaleReportArtifactError::Encode(e)
    }
}

impl From<DecodeError> for ScaleReportArtifactError {
    fn from(e: DecodeError) -> Self {
        ScaleReportArtifactError::Decode(e)
    }
}

impl From<ArtifactError> for ScaleReportArtifactError {
    fn from(e: ArtifactError) -> Self {
        ScaleReportArtifactError::Store(e)
    }
}

/// Store a ScaleDemoScaleReportV1 as a content-addressed artifact.
pub fn put_scale_demo_scale_report_v1<S: ArtifactStore>(
    store: &S,
    rep: &ScaleDemoScaleReportV1,
) -> Result<Hash32, ScaleReportArtifactError> {
    let bytes = rep.encode()?;
    let h = store.put(&bytes)?;
    Ok(h)
}

/// Load a ScaleDemoScaleReportV1 from a content-addressed artifact.
pub fn get_scale_demo_scale_report_v1<S: ArtifactStore>(
    store: &S,
    hash: &Hash32,
) -> Result<Option<ScaleDemoScaleReportV1>, ScaleReportArtifactError> {
    let opt = store.get(hash)?;
    let bytes = match opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let rep = ScaleDemoScaleReportV1::decode(&bytes)?;
    Ok(Some(rep))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push("fsa_lm_tests");
        p.push(name);
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn h(b: u8) -> Hash32 {
        let mut x = [0u8; 32];
        x[0] = b;
        x
    }

    #[test]
    fn scale_report_artifact_round_trip() {
        let dir = tmp_dir("scale_report_artifact_round_trip");
        let store = FsArtifactStore::new(&dir).unwrap();

        let rep = ScaleDemoScaleReportV1 {
            version: crate::scale_report::SCALE_DEMO_SCALE_REPORT_V1_VERSION,
            workload_hash: h(9),
            doc_count: 10,
            query_count: 2,
            tie_pair: 1,
            seed: 123,
            frame_manifest_hash: h(7),
            docs_total: 10,
            rows_total: 10,
            frame_segments_total: 1,
            has_index: 1,
            index_snapshot_hash: h(3),
            index_sig_map_hash: h(4),
            index_segments_total: 1,
            has_prompts: 1,
            prompts_max_output_tokens: 256,
            prompts: crate::scale_report::HashListSummaryV1::from_list("prompts", &[h(10), h(11)]),
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
        };

        let hash = put_scale_demo_scale_report_v1(&store, &rep).unwrap();
        let got = get_scale_demo_scale_report_v1(&store, &hash)
            .unwrap()
            .unwrap();
        assert_eq!(rep, got);
    }
}
