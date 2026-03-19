// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Golden pack turn-pairs report schema.
//!
//! A "turn-pairs" pack is a small deterministic workload that executes two
//! answer turns back-to-back and stores a compact report.
//!
//! This pack is intended to cover:
//! - Realizer directives output (non-debug style)
//! - Markov opener template selection (variant 0 vs 1)
//! - Markov trace emission for surface-template choice ids

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::hash::Hash32;
use crate::scale_report::HashListSummaryV1;

/// Golden turn-pairs report schema version.
pub const GOLDEN_PACK_TURN_PAIRS_REPORT_V1_VERSION: u16 = 1;

/// Golden turn-pairs report (v1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoldenPackTurnPairsReportV1 {
    /// Schema version. Must equal `GOLDEN_PACK_TURN_PAIRS_REPORT_V1_VERSION`.
    pub version: u16,
    /// Human-readable pack name (ASCII recommended).
    pub pack_name: String,
    /// Workload hash used for this pack.
    pub workload_hash: Hash32,
    /// Number of turns executed (must be 2 in v1).
    pub turn_count: u8,
    /// Answer artifact hashes (stored as a list summary).
    pub answers: HashListSummaryV1,
    /// MarkovTrace artifacts (stored as a list summary).
    pub markov_traces: HashListSummaryV1,
    /// MarkovHints artifacts (stored as a list summary).
    pub markov_hints: HashListSummaryV1,
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

impl GoldenPackTurnPairsReportV1 {
    /// Encode this report to canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        if self.version != GOLDEN_PACK_TURN_PAIRS_REPORT_V1_VERSION {
            return Err(EncodeError::new(
                "unsupported golden turn-pairs report version",
            ));
        }
        if self.turn_count != 2 {
            return Err(EncodeError::new("turn_count must be 2"));
        }
        let mut w = ByteWriter::with_capacity(256);
        w.write_u16(self.version);
        w.write_str(&self.pack_name)?;
        write_hash32(&mut w, &self.workload_hash);
        w.write_u8(self.turn_count);
        write_list_summary(&mut w, &self.answers);
        write_list_summary(&mut w, &self.markov_traces);
        write_list_summary(&mut w, &self.markov_hints);
        Ok(w.into_bytes())
    }

    /// Decode a report from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let version = r.read_u16()?;
        if version != GOLDEN_PACK_TURN_PAIRS_REPORT_V1_VERSION {
            return Err(DecodeError::new(
                "unsupported golden turn-pairs report version",
            ));
        }
        let pack_name = r.read_str_view()?.to_string();
        let workload_hash = read_hash32(&mut r)?;
        let turn_count = r.read_u8()?;
        if turn_count != 2 {
            return Err(DecodeError::new("turn_count must be 2"));
        }
        let answers = read_list_summary(&mut r)?;
        let markov_traces = read_list_summary(&mut r)?;
        let markov_hints = read_list_summary(&mut r)?;

        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        Ok(GoldenPackTurnPairsReportV1 {
            version,
            pack_name,
            workload_hash,
            turn_count,
            answers,
            markov_traces,
            markov_hints,
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

    #[test]
    fn turn_pairs_report_round_trip() {
        let rep = GoldenPackTurnPairsReportV1 {
            version: GOLDEN_PACK_TURN_PAIRS_REPORT_V1_VERSION,
            pack_name: "golden_pack_turn_pairs_v1".to_string(),
            workload_hash: h(1),
            turn_count: 2,
            answers: HashListSummaryV1::from_list("answers", &[h(2), h(3)]),
            markov_traces: HashListSummaryV1::from_list("traces", &[h(4), h(5)]),
            markov_hints: HashListSummaryV1::from_list("hints", &[h(6)]),
        };

        let bytes = rep.encode().unwrap();
        let got = GoldenPackTurnPairsReportV1::decode(&bytes).unwrap();
        assert_eq!(rep, got);
    }
}
