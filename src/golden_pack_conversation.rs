// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Golden pack conversation report schema.
//!
//! The conversation pack is a small deterministic workload intended to
//! regression-test the higher-level conversational loop.
//!
//! It bundles:
//! - the v1 scale-demo golden pack report
//! - the v1 turn-pairs golden pack report
//!
//! The conversation report embeds the canonical bytes of both sub-reports and
//! validates that their hashes match the embedded bytes.

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::golden_pack::GoldenPackReportV1;
use crate::golden_pack_turn_pairs::GoldenPackTurnPairsReportV1;
use crate::hash::{blake3_hash, Hash32};

/// Golden conversation report schema version.
pub const GOLDEN_PACK_CONVERSATION_REPORT_V1_VERSION: u16 = 1;

/// Golden conversation report (v1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoldenPackConversationReportV1 {
    /// Schema version. Must equal `GOLDEN_PACK_CONVERSATION_REPORT_V1_VERSION`.
    pub version: u16,
    /// Human-readable pack name (ASCII recommended).
    pub pack_name: String,
    /// Stored GoldenPackReportV1 hash.
    pub golden_pack_report_hash: Hash32,
    /// Embedded GoldenPackReportV1 contents.
    pub golden_pack_report: GoldenPackReportV1,
    /// Stored GoldenPackTurnPairsReportV1 hash.
    pub turn_pairs_report_hash: Hash32,
    /// Embedded GoldenPackTurnPairsReportV1 contents.
    pub turn_pairs_report: GoldenPackTurnPairsReportV1,
}

fn write_hash32(w: &mut ByteWriter, h: &Hash32) {
    w.write_raw(h);
}

fn read_hash32(r: &mut ByteReader<'_>) -> Result<Hash32, DecodeError> {
    let b = r.read_fixed(32)?;
    let mut out = [0u8; 32];
    out.copy_from_slice(b);
    Ok(out)
}

fn write_blob(w: &mut ByteWriter, bytes: &[u8]) {
    w.write_u32(bytes.len() as u32);
    w.write_raw(bytes);
}

fn read_blob(r: &mut ByteReader<'_>) -> Result<Vec<u8>, DecodeError> {
    let n = r.read_u32()? as usize;
    let b = r.read_fixed(n)?;
    Ok(b.to_vec())
}

impl GoldenPackConversationReportV1 {
    /// Encode this report to canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        if self.version != GOLDEN_PACK_CONVERSATION_REPORT_V1_VERSION {
            return Err(EncodeError::new(
                "unsupported golden conversation report version",
            ));
        }

        let gp_bytes = self.golden_pack_report.encode()?;
        let tp_bytes = self.turn_pairs_report.encode()?;

        let mut w = ByteWriter::with_capacity(256 + gp_bytes.len() + tp_bytes.len());
        w.write_u16(self.version);
        w.write_str(&self.pack_name)?;
        write_hash32(&mut w, &self.golden_pack_report_hash);
        write_blob(&mut w, &gp_bytes);
        write_hash32(&mut w, &self.turn_pairs_report_hash);
        write_blob(&mut w, &tp_bytes);
        Ok(w.into_bytes())
    }

    /// Decode a report from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let version = r.read_u16()?;
        if version != GOLDEN_PACK_CONVERSATION_REPORT_V1_VERSION {
            return Err(DecodeError::new(
                "unsupported golden conversation report version",
            ));
        }
        let pack_name = r.read_str_view()?.to_string();
        let golden_pack_report_hash = read_hash32(&mut r)?;
        let gp_bytes = read_blob(&mut r)?;
        let turn_pairs_report_hash = read_hash32(&mut r)?;
        let tp_bytes = read_blob(&mut r)?;

        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        if blake3_hash(&gp_bytes) != golden_pack_report_hash {
            return Err(DecodeError::new("golden pack report hash mismatch"));
        }
        if blake3_hash(&tp_bytes) != turn_pairs_report_hash {
            return Err(DecodeError::new("turn-pairs report hash mismatch"));
        }

        let golden_pack_report = GoldenPackReportV1::decode(&gp_bytes)?;
        let turn_pairs_report = GoldenPackTurnPairsReportV1::decode(&tp_bytes)?;

        Ok(GoldenPackConversationReportV1 {
            version,
            pack_name,
            golden_pack_report_hash,
            golden_pack_report,
            turn_pairs_report_hash,
            turn_pairs_report,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::golden_pack::GOLDEN_PACK_REPORT_V1_VERSION;
    use crate::golden_pack_turn_pairs::GOLDEN_PACK_TURN_PAIRS_REPORT_V1_VERSION;
    use crate::hash::blake3_hash;
    use crate::scale_report::HashListSummaryV1;

    fn h(n: u8) -> Hash32 {
        blake3_hash(&[n])
    }

    #[test]
    fn conversation_report_round_trip() {
        let gp = crate::golden_pack::GoldenPackReportV1 {
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
        let gp_bytes = gp.encode().unwrap();
        let gp_hash = blake3_hash(&gp_bytes);

        let tp = crate::golden_pack_turn_pairs::GoldenPackTurnPairsReportV1 {
            version: GOLDEN_PACK_TURN_PAIRS_REPORT_V1_VERSION,
            pack_name: "tp".to_string(),
            workload_hash: h(6),
            turn_count: 2,
            answers: HashListSummaryV1::from_list("answers", &[h(7), h(8)]),
            markov_traces: HashListSummaryV1::from_list("traces", &[h(9), h(10)]),
            markov_hints: HashListSummaryV1::from_list("hints", &[h(11)]),
        };
        let tp_bytes = tp.encode().unwrap();
        let tp_hash = blake3_hash(&tp_bytes);

        let rep = GoldenPackConversationReportV1 {
            version: GOLDEN_PACK_CONVERSATION_REPORT_V1_VERSION,
            pack_name: "conv".to_string(),
            golden_pack_report_hash: gp_hash,
            golden_pack_report: gp,
            turn_pairs_report_hash: tp_hash,
            turn_pairs_report: tp,
        };

        let bytes = rep.encode().unwrap();
        let got = GoldenPackConversationReportV1::decode(&bytes).unwrap();
        assert_eq!(rep, got);
    }
}
