// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Golden pack conversation report artifact helpers.

use crate::artifact::{ArtifactError, ArtifactStore};
use crate::golden_pack_conversation::GoldenPackConversationReportV1;
use crate::hash::Hash32;

/// Errors for golden pack conversation report artifact operations.
#[derive(Debug)]
pub enum GoldenPackConversationArtifactError {
    /// Artifact store operation failed.
    Store(ArtifactError),
    /// Encode failed.
    Encode(crate::codec::EncodeError),
    /// Decode failed.
    Decode(crate::codec::DecodeError),
}

impl core::fmt::Display for GoldenPackConversationArtifactError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            GoldenPackConversationArtifactError::Store(e) => {
                f.write_str("artifact store: ")?;
                core::fmt::Display::fmt(e, f)
            }
            GoldenPackConversationArtifactError::Encode(e) => {
                f.write_str("encode: ")?;
                core::fmt::Display::fmt(e, f)
            }
            GoldenPackConversationArtifactError::Decode(e) => {
                f.write_str("decode: ")?;
                core::fmt::Display::fmt(e, f)
            }
        }
    }
}

impl From<ArtifactError> for GoldenPackConversationArtifactError {
    fn from(value: ArtifactError) -> Self {
        GoldenPackConversationArtifactError::Store(value)
    }
}

impl From<crate::codec::EncodeError> for GoldenPackConversationArtifactError {
    fn from(value: crate::codec::EncodeError) -> Self {
        GoldenPackConversationArtifactError::Encode(value)
    }
}

impl From<crate::codec::DecodeError> for GoldenPackConversationArtifactError {
    fn from(value: crate::codec::DecodeError) -> Self {
        GoldenPackConversationArtifactError::Decode(value)
    }
}

/// Store a GoldenPackConversationReportV1 as an artifact and return its content hash.
pub fn put_golden_pack_conversation_report_v1<S: ArtifactStore>(
    store: &S,
    rep: &GoldenPackConversationReportV1,
) -> Result<Hash32, GoldenPackConversationArtifactError> {
    let bytes = rep.encode()?;
    let h = store.put(&bytes)?;
    Ok(h)
}

/// Load a GoldenPackConversationReportV1 artifact by hash.
pub fn get_golden_pack_conversation_report_v1<S: ArtifactStore>(
    store: &S,
    h: &Hash32,
) -> Result<Option<GoldenPackConversationReportV1>, GoldenPackConversationArtifactError> {
    let opt = store.get(h)?;
    let bytes = match opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let rep = GoldenPackConversationReportV1::decode(&bytes)?;
    Ok(Some(rep))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
    use crate::golden_pack_conversation::GOLDEN_PACK_CONVERSATION_REPORT_V1_VERSION;
    use crate::golden_pack::{GoldenPackReportV1, GOLDEN_PACK_REPORT_V1_VERSION};
    use crate::golden_pack_turn_pairs::{
        GoldenPackTurnPairsReportV1, GOLDEN_PACK_TURN_PAIRS_REPORT_V1_VERSION,
    };
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
    fn conversation_report_artifact_round_trip() {
        let dir = tmp_dir("conversation_report_artifact_round_trip");
        let store = FsArtifactStore::new(&dir).unwrap();

        let gp = GoldenPackReportV1 {
            version: GOLDEN_PACK_REPORT_V1_VERSION,
            pack_name: "gp".to_string(),
            scale_report_hash: h(1),
            scale_report: crate::scale_report::ScaleDemoScaleReportV1 {
                version: crate::scale_report::SCALE_DEMO_SCALE_REPORT_V1_VERSION,
                workload_hash: h(2),
                doc_count: 1,
                query_count: 1,
                tie_pair: 0,
                seed: 7,
                frame_manifest_hash: h(3),
                docs_total: 1,
                rows_total: 1,
                frame_segments_total: 1,
                has_index: 0,
                index_snapshot_hash: h(4),
                index_sig_map_hash: h(5),
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
            },
        };
        let gp_hash = blake3_hash(&gp.encode().unwrap());

        let tp = GoldenPackTurnPairsReportV1 {
            version: GOLDEN_PACK_TURN_PAIRS_REPORT_V1_VERSION,
            pack_name: "tp".to_string(),
            workload_hash: h(6),
            turn_count: 2,
            answers: HashListSummaryV1::from_list("answers", &[h(7), h(8)]),
            markov_traces: HashListSummaryV1::from_list("traces", &[h(9), h(10)]),
            markov_hints: HashListSummaryV1::from_list("hints", &[h(11)]),
        };
        let tp_hash = blake3_hash(&tp.encode().unwrap());

        let rep = GoldenPackConversationReportV1 {
            version: GOLDEN_PACK_CONVERSATION_REPORT_V1_VERSION,
            pack_name: "conv".to_string(),
            golden_pack_report_hash: gp_hash,
            golden_pack_report: gp,
            turn_pairs_report_hash: tp_hash,
            turn_pairs_report: tp,
        };

        let hh = put_golden_pack_conversation_report_v1(&store, &rep).unwrap();
        let got = get_golden_pack_conversation_report_v1(&store, &hh)
            .unwrap()
            .unwrap();
        assert_eq!(rep, got);
    }
}
