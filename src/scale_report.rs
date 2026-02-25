// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Scale demo scale report v1.
//!
//! The scale demo pipeline produces several stage reports that
//! contain per-query artifact hash lists (PromptPacks, EvidenceBundles, answers).
//! This module defines a compact, content-addressed summary artifact that:
//! - records the core workload identity and stage artifact identities
//! - summarizes per-query hash lists using a stable list hash and first/last
//! - is canonically encoded (bitwise deterministic) for hashing and persistence
//!
//! This artifact is intended for large runs where embedding full hash lists in a
//! single report would be too large.

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::hash::{hex32, Hash32};

/// Scale report schema version.
pub const SCALE_DEMO_SCALE_REPORT_V1_VERSION: u16 = 3;

/// Stable list hash domain separator.
const SCALE_DEMO_LIST_V1_DOMAIN: &[u8] = b"scale_demo_list_v1\0";

/// Compute a stable hash for a list of Hash32 values.
///
/// The hash is domain-separated and includes:
/// - a tag (to distinguish lists with different meaning)
/// - the list length
/// - each 32-byte hash in sequence
pub fn hash_hash32_list_v1(tag: &str, hashes: &[Hash32]) -> Hash32 {
    if tag.len() > (u32::MAX as usize) {
        // Tag is internal, so this should never happen.
        return [0u8; 32];
    }

    let mut h = blake3::Hasher::new();
    h.update(SCALE_DEMO_LIST_V1_DOMAIN);
    h.update(&(tag.len() as u32).to_le_bytes());
    h.update(tag.as_bytes());
    h.update(&(hashes.len() as u32).to_le_bytes());
    for x in hashes.iter() {
        h.update(x);
    }
    let out = h.finalize();
    *out.as_bytes()
}

/// Summary of a per-query artifact hash list.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HashListSummaryV1 {
    /// Number of items in the list.
    pub count: u32,
    /// Stable hash of the list contents.
    pub list_hash: Hash32,
    /// First hash in the list (all zero if count is zero).
    pub first: Hash32,
    /// Last hash in the list (all zero if count is zero).
    pub last: Hash32,
}

impl HashListSummaryV1 {
    /// Empty summary.
    pub fn empty() -> Self {
        Self {
            count: 0,
            list_hash: [0u8; 32],
            first: [0u8; 32],
            last: [0u8; 32],
        }
    }

    /// Build a summary from a hash list.
    pub fn from_list(tag: &str, hashes: &[Hash32]) -> Self {
        if hashes.is_empty() {
            return Self::empty();
        }
        let list_hash = hash_hash32_list_v1(tag, hashes);
        let first = hashes[0];
        let last = hashes[hashes.len() - 1];
        Self {
            count: hashes.len() as u32,
            list_hash,
            first,
            last,
        }
    }

    /// Validate canonical invariants.
    pub fn validate_canonical(&self) -> Result<(), DecodeError> {
        if self.count == 0 {
            if self.list_hash != [0u8; 32] || self.first != [0u8; 32] || self.last != [0u8; 32] {
                return Err(DecodeError::new("empty list summary must use zero hashes"));
            }
        }
        Ok(())
    }
}

/// Compact, content-addressed summary of a scale demo run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScaleDemoScaleReportV1 {
    /// Schema version (must equal 3).
    pub version: u16,

    /// Workload hash (content hash of deterministic workload).
    pub workload_hash: Hash32,
    /// Total number of docs in the workload.
    pub doc_count: u32,
    /// Total number of queries in the workload.
    pub query_count: u32,
    /// Tie pair enabled flag (0 or 1).
    pub tie_pair: u8,
    /// Workload seed.
    pub seed: u64,

    /// Ingest manifest hash.
    pub frame_manifest_hash: Hash32,
    /// Docs total recorded by ingest.
    pub docs_total: u64,
    /// Rows total recorded by ingest.
    pub rows_total: u64,
    /// Frame segments total recorded by ingest.
    pub frame_segments_total: u32,

    /// Whether index artifacts are present (0 or 1).
    pub has_index: u8,
    /// Index snapshot hash (zero if has_index is 0).
    pub index_snapshot_hash: Hash32,
    /// Index signature map hash (zero if has_index is 0).
    pub index_sig_map_hash: Hash32,
    /// Index segments total (0 if has_index is 0).
    pub index_segments_total: u32,

    /// Whether prompts are present (0 or 1).
    pub has_prompts: u8,
    /// PromptPack max output tokens.
    pub prompts_max_output_tokens: u32,
    /// PromptPack hash list summary.
    pub prompts: HashListSummaryV1,

    /// Whether evidence bundles are present (0 or 1).
    pub has_evidence: u8,
    /// Evidence top-k.
    pub evidence_k: u32,
    /// Evidence max bytes.
    pub evidence_max_bytes: u32,
    /// Evidence hash list summary.
    pub evidence: HashListSummaryV1,

    /// Whether answers are present (0 or 1).
    pub has_answers: u8,
    /// Planner max plan items.
    pub planner_max_plan_items: u32,
    /// Realizer max evidence items.
    pub realizer_max_evidence_items: u16,
    /// Realizer max terms per row.
    pub realizer_max_terms_per_row: u16,
    /// Realizer load frame rows flag (0 or 1).
    pub realizer_load_frame_rows: u8,
    /// Answer output hash list summary.
    pub answers: HashListSummaryV1,

    /// PlannerHints artifact hash list summary (empty when has_answers is 0).
    pub planner_hints: HashListSummaryV1,

    /// Forecast artifact hash list summary (empty when has_answers is 0).
    pub forecasts: HashListSummaryV1,

    /// MarkovTrace artifact hash list summary (empty when has_answers is 0).
    pub markov_traces: HashListSummaryV1,
}

impl ScaleDemoScaleReportV1 {
    /// Validate canonical invariants.
    pub fn validate_canonical(&self) -> Result<(), DecodeError> {
        if self.version != SCALE_DEMO_SCALE_REPORT_V1_VERSION {
            return Err(DecodeError::new("unsupported scale report version"));
        }
        if self.tie_pair > 1 {
            return Err(DecodeError::new("tie_pair must be 0 or 1"));
        }

        if self.has_index > 1 {
            return Err(DecodeError::new("has_index must be 0 or 1"));
        }
        if self.has_index == 0 {
            if self.index_snapshot_hash != [0u8; 32]
                || self.index_sig_map_hash != [0u8; 32]
                || self.index_segments_total != 0
            {
                return Err(DecodeError::new("index fields must be zero when has_index=0"));
            }
        }

        if self.has_prompts > 1 {
            return Err(DecodeError::new("has_prompts must be 0 or 1"));
        }
        if self.has_prompts == 0 {
            if self.prompts_max_output_tokens != 0 || self.prompts != HashListSummaryV1::empty() {
                return Err(DecodeError::new("prompts fields must be zero when has_prompts=0"));
            }
        }
        self.prompts.validate_canonical()?;

        if self.has_evidence > 1 {
            return Err(DecodeError::new("has_evidence must be 0 or 1"));
        }
        if self.has_evidence == 0 {
            if self.evidence_k != 0 || self.evidence_max_bytes != 0 || self.evidence != HashListSummaryV1::empty() {
                return Err(DecodeError::new("evidence fields must be zero when has_evidence=0"));
            }
        }
        self.evidence.validate_canonical()?;

        if self.has_answers > 1 {
            return Err(DecodeError::new("has_answers must be 0 or 1"));
        }
        if self.has_answers == 0 {
            if self.planner_max_plan_items != 0
                || self.realizer_max_evidence_items != 0
                || self.realizer_max_terms_per_row != 0
                || self.realizer_load_frame_rows != 0
                || self.answers != HashListSummaryV1::empty()
                || self.planner_hints != HashListSummaryV1::empty()
                || self.forecasts != HashListSummaryV1::empty()
                || self.markov_traces != HashListSummaryV1::empty()
            {
                return Err(DecodeError::new("answer fields must be zero when has_answers=0"));
            }
        }
        if self.realizer_load_frame_rows > 1 {
            return Err(DecodeError::new("realizer_load_frame_rows must be 0 or 1"));
        }
        self.answers.validate_canonical()?;
        self.planner_hints.validate_canonical()?;
        self.forecasts.validate_canonical()?;
        self.markov_traces.validate_canonical()?;

        if self.has_answers != 0 {
            if self.planner_hints.count != self.answers.count {
                return Err(DecodeError::new("planner_hints count must match answers count"));
            }
            if self.forecasts.count != self.answers.count {
                return Err(DecodeError::new("forecasts count must match answers count"));
            }
            if self.markov_traces.count != self.answers.count {
                return Err(DecodeError::new("markov_traces count must match answers count"));
            }
        }

        Ok(())
    }

    /// Encode into canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        // This structure is fixed-size; reserve a conservative capacity.
        let mut w = ByteWriter::with_capacity(512);

        w.write_u16(self.version);
        w.write_raw(&self.workload_hash);
        w.write_u32(self.doc_count);
        w.write_u32(self.query_count);
        w.write_u8(self.tie_pair);
        w.write_u64(self.seed);

        w.write_raw(&self.frame_manifest_hash);
        w.write_u64(self.docs_total);
        w.write_u64(self.rows_total);
        w.write_u32(self.frame_segments_total);

        w.write_u8(self.has_index);
        w.write_raw(&self.index_snapshot_hash);
        w.write_raw(&self.index_sig_map_hash);
        w.write_u32(self.index_segments_total);

        w.write_u8(self.has_prompts);
        w.write_u32(self.prompts_max_output_tokens);
        write_list_summary(&mut w, &self.prompts);

        w.write_u8(self.has_evidence);
        w.write_u32(self.evidence_k);
        w.write_u32(self.evidence_max_bytes);
        write_list_summary(&mut w, &self.evidence);

        w.write_u8(self.has_answers);
        w.write_u32(self.planner_max_plan_items);
        w.write_u16(self.realizer_max_evidence_items);
        w.write_u16(self.realizer_max_terms_per_row);
        w.write_u8(self.realizer_load_frame_rows);
        write_list_summary(&mut w, &self.answers);
        write_list_summary(&mut w, &self.planner_hints);
        write_list_summary(&mut w, &self.forecasts);
        write_list_summary(&mut w, &self.markov_traces);

        Ok(w.into_bytes())
    }

    /// Decode from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);

        let version = r.read_u16()?;
        if version != SCALE_DEMO_SCALE_REPORT_V1_VERSION {
            return Err(DecodeError::new("unsupported scale report version"));
        }

        let workload_hash = read_hash32(&mut r)?;
        let doc_count = r.read_u32()?;
        let query_count = r.read_u32()?;
        let tie_pair = r.read_u8()?;
        let seed = r.read_u64()?;

        let frame_manifest_hash = read_hash32(&mut r)?;
        let docs_total = r.read_u64()?;
        let rows_total = r.read_u64()?;
        let frame_segments_total = r.read_u32()?;

        let has_index = r.read_u8()?;
        let index_snapshot_hash = read_hash32(&mut r)?;
        let index_sig_map_hash = read_hash32(&mut r)?;
        let index_segments_total = r.read_u32()?;

        let has_prompts = r.read_u8()?;
        let prompts_max_output_tokens = r.read_u32()?;
        let prompts = read_list_summary(&mut r)?;

        let has_evidence = r.read_u8()?;
        let evidence_k = r.read_u32()?;
        let evidence_max_bytes = r.read_u32()?;
        let evidence = read_list_summary(&mut r)?;

        let has_answers = r.read_u8()?;
        let planner_max_plan_items = r.read_u32()?;
        let realizer_max_evidence_items = r.read_u16()?;
        let realizer_max_terms_per_row = r.read_u16()?;
        let realizer_load_frame_rows = r.read_u8()?;
        let answers = read_list_summary(&mut r)?;
        let planner_hints = read_list_summary(&mut r)?;
        let forecasts = read_list_summary(&mut r)?;
        let markov_traces = read_list_summary(&mut r)?;

        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        let rep = ScaleDemoScaleReportV1 {
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
        };

        rep.validate_canonical()?;
        Ok(rep)
    }
}

impl core::fmt::Display for ScaleDemoScaleReportV1 {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "scale_demo_scale_report_v3 workload_hash={} docs={} queries={} tie_pair={} seed={} manifest={} index_present={} prompts_present={} evidence_present={} answers_present={} ",
            hex32(&self.workload_hash),
            self.doc_count,
            self.query_count,
            self.tie_pair,
            self.seed,
            hex32(&self.frame_manifest_hash),
            self.has_index,
            self.has_prompts,
            self.has_evidence,
            self.has_answers
        )?;

        if self.has_index != 0 {
            write!(
                f,
                "snapshot={} sig_map={} ",
                hex32(&self.index_snapshot_hash),
                hex32(&self.index_sig_map_hash)
            )?;
        }

        if self.has_prompts != 0 {
            write!(
                f,
                "prompts_list_hash={} prompts_count={} ",
                hex32(&self.prompts.list_hash),
                self.prompts.count
            )?;
        }

        if self.has_evidence != 0 {
            write!(
                f,
                "evidence_list_hash={} evidence_count={} ",
                hex32(&self.evidence.list_hash),
                self.evidence.count
            )?;
        }

        if self.has_answers != 0 {
            write!(
                f,
                "answers_list_hash={} answers_count={} planner_hints_list_hash={} planner_hints_count={} forecasts_list_hash={} forecasts_count={} markov_traces_list_hash={} markov_traces_count={} ",
                hex32(&self.answers.list_hash),
                self.answers.count,
                hex32(&self.planner_hints.list_hash),
                self.planner_hints.count,
                hex32(&self.forecasts.list_hash),
                self.forecasts.count,
                hex32(&self.markov_traces.list_hash),
                self.markov_traces.count
            )
        } else {
            Ok(())
        }
    }
}

fn read_hash32(r: &mut ByteReader<'_>) -> Result<Hash32, DecodeError> {
    let b = r.read_fixed(32)?;
    let mut h = [0u8; 32];
    h.copy_from_slice(b);
    Ok(h)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn h(b: u8) -> Hash32 {
        let mut x = [0u8; 32];
        x[0] = b;
        x
    }

    #[test]
    fn list_hash_is_stable_and_order_sensitive() {
        let a = h(1);
        let b = h(2);

        let h1 = hash_hash32_list_v1("prompts", &[a, b]);
        let h2 = hash_hash32_list_v1("prompts", &[a, b]);
        assert_eq!(h1, h2);

        let h3 = hash_hash32_list_v1("prompts", &[b, a]);
        assert_ne!(h1, h3);

        let h4 = hash_hash32_list_v1("evidence", &[a, b]);
        assert_ne!(h1, h4);
    }

    #[test]
    fn scale_report_encode_decode_round_trip() {
        let rep = ScaleDemoScaleReportV1 {
            version: SCALE_DEMO_SCALE_REPORT_V1_VERSION,
            workload_hash: h(9),
            doc_count: 100,
            query_count: 50,
            tie_pair: 1,
            seed: 123,
            frame_manifest_hash: h(7),
            docs_total: 100,
            rows_total: 100,
            frame_segments_total: 4,
            has_index: 1,
            index_snapshot_hash: h(3),
            index_sig_map_hash: h(4),
            index_segments_total: 4,
            has_prompts: 1,
            prompts_max_output_tokens: 256,
            prompts: HashListSummaryV1::from_list("prompts", &[h(10), h(11)]),
            has_evidence: 1,
            evidence_k: 16,
            evidence_max_bytes: 65536,
            evidence: HashListSummaryV1::from_list("evidence", &[h(12), h(13)]),
            has_answers: 1,
            planner_max_plan_items: 64,
            realizer_max_evidence_items: 8,
            realizer_max_terms_per_row: 8,
            realizer_load_frame_rows: 1,
            answers: HashListSummaryV1::from_list("answers", &[h(14), h(15)]),
            planner_hints: HashListSummaryV1::from_list("planner_hints", &[h(16), h(17)]),
            forecasts: HashListSummaryV1::from_list("forecasts", &[h(18), h(19)]),
            markov_traces: HashListSummaryV1::from_list("markov_traces", &[h(20), h(21)]),
        };

        let bytes = rep.encode().unwrap();
        let rep2 = ScaleDemoScaleReportV1::decode(&bytes).unwrap();
        assert_eq!(rep, rep2);

        // Reject trailing bytes.
        let mut bytes2 = bytes.clone();
        bytes2.push(0);
        assert!(ScaleDemoScaleReportV1::decode(&bytes2).is_err());
    }

    #[test]
    fn scale_report_rejects_inconsistent_flags() {
        let rep = ScaleDemoScaleReportV1 {
            version: SCALE_DEMO_SCALE_REPORT_V1_VERSION,
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
            index_snapshot_hash: h(3),
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

        // Encode will still work, but decode must reject due to validate_canonical.
        let bytes = rep.encode().unwrap();
        assert!(ScaleDemoScaleReportV1::decode(&bytes).is_err());
    }
}
