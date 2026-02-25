// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Golden pack schema.
//!
//! A "golden pack" is a small, deterministic end-to-end workload used to
//! regression-test prompt generation, retrieval, and answering.
//!
//! The golden pack output is content-addressed. In CI, an expected hash can be
//! pinned to detect regressions.

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::hash::Hash32;
use crate::scale_report::{HashListSummaryV1, ScaleDemoScaleReportV1};

/// Golden pack report schema version.
pub const GOLDEN_PACK_REPORT_V1_VERSION: u16 = 1;

/// Golden pack report (v1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoldenPackReportV1 {
    /// Schema version. Must equal `GOLDEN_PACK_REPORT_V1_VERSION`.
    pub version: u16,
    /// Human-readable pack name (ASCII recommended).
    pub pack_name: String,
    /// Scale report artifact hash for the executed workload.
    pub scale_report_hash: Hash32,
    /// Embedded scale report contents.
    pub scale_report: ScaleDemoScaleReportV1,
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

fn write_list_summary(w: &mut ByteWriter, s: &HashListSummaryV1) {
    w.write_u32(s.count);
    w.write_raw(&s.list_hash);
    w.write_raw(&s.first);
    w.write_raw(&s.last);
}

fn read_list_summary(r: &mut ByteReader<'_>) -> Result<HashListSummaryV1, DecodeError> {
    let count = r.read_u32()?;
    let list_hash = read_hash32(r)?;
    let first = read_hash32(r)?;
    let last = read_hash32(r)?;
    let s = HashListSummaryV1 {
        count,
        list_hash,
        first,
        last,
    };
    s.validate_canonical()?;
    Ok(s)
}

fn write_scale_report(w: &mut ByteWriter, rep: &ScaleDemoScaleReportV1) -> Result<(), EncodeError> {
    w.write_u16(rep.version);

    write_hash32(w, &rep.workload_hash);
    w.write_u32(rep.doc_count);
    w.write_u32(rep.query_count);
    w.write_u8(rep.tie_pair);
    w.write_u64(rep.seed);

    write_hash32(w, &rep.frame_manifest_hash);
    w.write_u64(rep.docs_total);
    w.write_u64(rep.rows_total);
    w.write_u32(rep.frame_segments_total);

    w.write_u8(rep.has_index);
    write_hash32(w, &rep.index_snapshot_hash);
    write_hash32(w, &rep.index_sig_map_hash);
    w.write_u32(rep.index_segments_total);

    w.write_u8(rep.has_prompts);
    w.write_u32(rep.prompts_max_output_tokens);
    write_list_summary(w, &rep.prompts);

    w.write_u8(rep.has_evidence);
    w.write_u32(rep.evidence_k);
    w.write_u32(rep.evidence_max_bytes);
    write_list_summary(w, &rep.evidence);

    w.write_u8(rep.has_answers);
    w.write_u32(rep.planner_max_plan_items);
    w.write_u16(rep.realizer_max_evidence_items);
    w.write_u16(rep.realizer_max_terms_per_row);
    w.write_u8(rep.realizer_load_frame_rows);
    write_list_summary(w, &rep.answers);
    write_list_summary(w, &rep.planner_hints);
    write_list_summary(w, &rep.forecasts);
    write_list_summary(w, &rep.markov_traces);

    Ok(())
}

fn read_scale_report(r: &mut ByteReader<'_>) -> Result<ScaleDemoScaleReportV1, DecodeError> {
    let version = r.read_u16()?;
    if version != crate::scale_report::SCALE_DEMO_SCALE_REPORT_V1_VERSION {
        return Err(DecodeError::new("unsupported scale report version"));
    }

    let workload_hash = read_hash32(r)?;
    let doc_count = r.read_u32()?;
    let query_count = r.read_u32()?;
    let tie_pair = r.read_u8()?;
    let seed = r.read_u64()?;

    let frame_manifest_hash = read_hash32(r)?;
    let docs_total = r.read_u64()?;
    let rows_total = r.read_u64()?;
    let frame_segments_total = r.read_u32()?;

    let has_index = r.read_u8()?;
    let index_snapshot_hash = read_hash32(r)?;
    let index_sig_map_hash = read_hash32(r)?;
    let index_segments_total = r.read_u32()?;

    let has_prompts = r.read_u8()?;
    let prompts_max_output_tokens = r.read_u32()?;
    let prompts = read_list_summary(r)?;

    let has_evidence = r.read_u8()?;
    let evidence_k = r.read_u32()?;
    let evidence_max_bytes = r.read_u32()?;
    let evidence = read_list_summary(r)?;

    let has_answers = r.read_u8()?;
    let planner_max_plan_items = r.read_u32()?;
    let realizer_max_evidence_items = r.read_u16()?;
    let realizer_max_terms_per_row = r.read_u16()?;
    let realizer_load_frame_rows = r.read_u8()?;
    let answers = read_list_summary(r)?;
    let planner_hints = read_list_summary(r)?;
    let forecasts = read_list_summary(r)?;
    let markov_traces = read_list_summary(r)?;

    Ok(ScaleDemoScaleReportV1 {
        version,
        workload_hash,
        doc_count,
        query_count,
        tie_pair,
        seed,
        frame_manifest_hash,
        docs_total,
        rows_total,
        frame_segments_total,
        has_index,
        index_snapshot_hash,
        index_sig_map_hash,
        index_segments_total,
        has_prompts,
        prompts_max_output_tokens,
        prompts,
        has_evidence,
        evidence_k,
        evidence_max_bytes,
        evidence,
        has_answers,
        planner_max_plan_items,
        realizer_max_evidence_items,
        realizer_max_terms_per_row,
        realizer_load_frame_rows,
        answers,
        planner_hints,
        forecasts,
        markov_traces,
    })
}

impl GoldenPackReportV1 {
    /// Encode this report to canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        if self.version != GOLDEN_PACK_REPORT_V1_VERSION {
            return Err(EncodeError::new("unsupported golden pack report version"));
        }
        let mut w = ByteWriter::with_capacity(256);
        w.write_u16(self.version);
        w.write_str(&self.pack_name)?;
        write_hash32(&mut w, &self.scale_report_hash);
        write_scale_report(&mut w, &self.scale_report)?;
        Ok(w.into_bytes())
    }

    /// Decode a report from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);

        let version = r.read_u16()?;
        if version != GOLDEN_PACK_REPORT_V1_VERSION {
            return Err(DecodeError::new("unsupported golden pack report version"));
        }
        let pack_name = r.read_str_view()?.to_string();
        let scale_report_hash = read_hash32(&mut r)?;
        let scale_report = read_scale_report(&mut r)?;

        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        Ok(GoldenPackReportV1 {
            version,
            pack_name,
            scale_report_hash,
            scale_report,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::blake3_hash;

    fn h(n: u8) -> Hash32 {
        blake3_hash(&[n])
    }

    fn sample_scale_report() -> ScaleDemoScaleReportV1 {
        ScaleDemoScaleReportV1 {
            version: crate::scale_report::SCALE_DEMO_SCALE_REPORT_V1_VERSION,
            workload_hash: h(1),
            doc_count: 12,
            query_count: 7,
            tie_pair: 1,
            seed: 9,
            frame_manifest_hash: h(2),
            docs_total: 12,
            rows_total: 123,
            frame_segments_total: 2,
            has_index: 1,
            index_snapshot_hash: h(3),
            index_sig_map_hash: h(4),
            index_segments_total: 3,
            has_prompts: 1,
            prompts_max_output_tokens: 256,
            prompts: HashListSummaryV1::from_list("prompts", &[h(10), h(11)]),
            has_evidence: 1,
            evidence_k: 16,
            evidence_max_bytes: 65536,
            evidence: HashListSummaryV1::from_list("evidence", &[h(12), h(13)]),
            has_answers: 1,
            planner_max_plan_items: 8,
            realizer_max_evidence_items: 10,
            realizer_max_terms_per_row: 6,
            realizer_load_frame_rows: 1,
            answers: HashListSummaryV1::from_list("answers", &[h(14), h(15)]),
            planner_hints: HashListSummaryV1::from_list("planner_hints", &[h(16), h(17)]),
            forecasts: HashListSummaryV1::from_list("forecasts", &[h(18), h(19)]),
            markov_traces: HashListSummaryV1::from_list("markov_traces", &[h(20), h(21)]),
        }
    }

    #[test]
    fn golden_pack_report_round_trip() {
        let rep = GoldenPackReportV1 {
            version: GOLDEN_PACK_REPORT_V1_VERSION,
            pack_name: "golden_pack_v1".to_string(),
            scale_report_hash: h(9),
            scale_report: sample_scale_report(),
        };
        let b = rep.encode().unwrap();
        let d = GoldenPackReportV1::decode(&b).unwrap();
        assert_eq!(rep, d);
    }

    #[test]
    fn golden_pack_report_rejects_trailing_bytes() {
        let rep = GoldenPackReportV1 {
            version: GOLDEN_PACK_REPORT_V1_VERSION,
            pack_name: "golden_pack_v1".to_string(),
            scale_report_hash: h(9),
            scale_report: sample_scale_report(),
        };
        let mut b = rep.encode().unwrap();
        b.push(0);
        assert!(GoldenPackReportV1::decode(&b).is_err());
    }
}
