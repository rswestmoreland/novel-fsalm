// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Golden pack report artifact helpers.

use crate::artifact::{ArtifactError, ArtifactStore};
use crate::golden_pack::GoldenPackReportV1;
use crate::hash::Hash32;

/// Errors for golden pack report artifact operations.
#[derive(Debug)]
pub enum GoldenPackArtifactError {
    /// Artifact store operation failed.
    Store(ArtifactError),
    /// Encode failed.
    Encode(crate::codec::EncodeError),
    /// Decode failed.
    Decode(crate::codec::DecodeError),
}

impl core::fmt::Display for GoldenPackArtifactError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            GoldenPackArtifactError::Store(e) => {
                f.write_str("artifact store: ")?;
                core::fmt::Display::fmt(e, f)
            }
            GoldenPackArtifactError::Encode(e) => {
                f.write_str("encode: ")?;
                core::fmt::Display::fmt(e, f)
            }
            GoldenPackArtifactError::Decode(e) => {
                f.write_str("decode: ")?;
                core::fmt::Display::fmt(e, f)
            }
        }
    }
}

impl From<ArtifactError> for GoldenPackArtifactError {
    fn from(value: ArtifactError) -> Self {
        GoldenPackArtifactError::Store(value)
    }
}

impl From<crate::codec::EncodeError> for GoldenPackArtifactError {
    fn from(value: crate::codec::EncodeError) -> Self {
        GoldenPackArtifactError::Encode(value)
    }
}

impl From<crate::codec::DecodeError> for GoldenPackArtifactError {
    fn from(value: crate::codec::DecodeError) -> Self {
        GoldenPackArtifactError::Decode(value)
    }
}

/// Store a GoldenPackReportV1 as an artifact and return its content hash.
pub fn put_golden_pack_report_v1<S: ArtifactStore>(
    store: &S,
    rep: &GoldenPackReportV1,
) -> Result<Hash32, GoldenPackArtifactError> {
    let bytes = rep.encode()?;
    let h = store.put(&bytes)?;
    Ok(h)
}

/// Load a GoldenPackReportV1 artifact by hash.
pub fn get_golden_pack_report_v1<S: ArtifactStore>(
    store: &S,
    h: &Hash32,
) -> Result<Option<GoldenPackReportV1>, GoldenPackArtifactError> {
    let opt = store.get(h)?;
    let bytes = match opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let rep = GoldenPackReportV1::decode(&bytes)?;
    Ok(Some(rep))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
    use crate::golden_pack::GOLDEN_PACK_REPORT_V1_VERSION;
    use crate::hash::blake3_hash;
    use crate::scale_report::HashListSummaryV1;
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

    fn h(n: u8) -> Hash32 {
        blake3_hash(&[n])
    }

    #[test]
    fn golden_pack_report_artifact_round_trip() {
        let dir = tmp_dir("golden_pack_report_artifact_round_trip");
        let store = FsArtifactStore::new(&dir).unwrap();
        let scale_report = crate::scale_report::ScaleDemoScaleReportV1 {
            version: crate::scale_report::SCALE_DEMO_SCALE_REPORT_V1_VERSION,
            workload_hash: h(1),
            doc_count: 1,
            query_count: 1,
            tie_pair: 0,
            seed: 1,
            frame_manifest_hash: h(2),
            docs_total: 1,
            rows_total: 1,
            frame_segments_total: 1,
            has_index: 0,
            index_snapshot_hash: [0u8; 32],
            index_sig_map_hash: [0u8; 32],
            index_segments_total: 0,
            has_prompts: 0,
            prompts_max_output_tokens: 0,
            prompts: HashListSummaryV1::empty(),
            has_evidence: 0,
            evidence_k: 0,
            evidence_max_bytes: 0,
            evidence: HashListSummaryV1::empty(),
            has_answers: 0,
            planner_max_plan_items: 0,
            realizer_max_evidence_items: 0,
            realizer_max_terms_per_row: 0,
            realizer_load_frame_rows: 0,
            answers: HashListSummaryV1::empty(),
            planner_hints: HashListSummaryV1::empty(),
            forecasts: HashListSummaryV1::empty(),
            markov_traces: HashListSummaryV1::empty(),
        };
        let rep = GoldenPackReportV1 {
            version: GOLDEN_PACK_REPORT_V1_VERSION,
            pack_name: "golden_pack_v1".to_string(),
            scale_report_hash: h(9),
            scale_report,
        };

        let hh = put_golden_pack_report_v1(&store, &rep).unwrap();
        let got = get_golden_pack_report_v1(&store, &hh).unwrap().unwrap();
        assert_eq!(rep, got);
    }
}
