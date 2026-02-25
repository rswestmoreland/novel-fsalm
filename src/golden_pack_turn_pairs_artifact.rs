// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Golden pack turn-pairs report artifact helpers.

use crate::artifact::{ArtifactError, ArtifactStore};
use crate::golden_pack_turn_pairs::GoldenPackTurnPairsReportV1;
use crate::hash::Hash32;

/// Errors for golden pack turn-pairs report artifact operations.
#[derive(Debug)]
pub enum GoldenPackTurnPairsArtifactError {
    /// Artifact store operation failed.
    Store(ArtifactError),
    /// Encode failed.
    Encode(crate::codec::EncodeError),
    /// Decode failed.
    Decode(crate::codec::DecodeError),
}

impl core::fmt::Display for GoldenPackTurnPairsArtifactError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            GoldenPackTurnPairsArtifactError::Store(e) => {
                f.write_str("artifact store: ")?;
                core::fmt::Display::fmt(e, f)
            }
            GoldenPackTurnPairsArtifactError::Encode(e) => {
                f.write_str("encode: ")?;
                core::fmt::Display::fmt(e, f)
            }
            GoldenPackTurnPairsArtifactError::Decode(e) => {
                f.write_str("decode: ")?;
                core::fmt::Display::fmt(e, f)
            }
        }
    }
}

impl From<ArtifactError> for GoldenPackTurnPairsArtifactError {
    fn from(value: ArtifactError) -> Self {
        GoldenPackTurnPairsArtifactError::Store(value)
    }
}

impl From<crate::codec::EncodeError> for GoldenPackTurnPairsArtifactError {
    fn from(value: crate::codec::EncodeError) -> Self {
        GoldenPackTurnPairsArtifactError::Encode(value)
    }
}

impl From<crate::codec::DecodeError> for GoldenPackTurnPairsArtifactError {
    fn from(value: crate::codec::DecodeError) -> Self {
        GoldenPackTurnPairsArtifactError::Decode(value)
    }
}

/// Store a GoldenPackTurnPairsReportV1 as an artifact and return its content hash.
pub fn put_golden_pack_turn_pairs_report_v1<S: ArtifactStore>(
    store: &S,
    rep: &GoldenPackTurnPairsReportV1,
) -> Result<Hash32, GoldenPackTurnPairsArtifactError> {
    let bytes = rep.encode()?;
    let h = store.put(&bytes)?;
    Ok(h)
}

/// Load a GoldenPackTurnPairsReportV1 artifact by hash.
pub fn get_golden_pack_turn_pairs_report_v1<S: ArtifactStore>(
    store: &S,
    h: &Hash32,
) -> Result<Option<GoldenPackTurnPairsReportV1>, GoldenPackTurnPairsArtifactError> {
    let opt = store.get(h)?;
    let bytes = match opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let rep = GoldenPackTurnPairsReportV1::decode(&bytes)?;
    Ok(Some(rep))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
    use crate::golden_pack_turn_pairs::GOLDEN_PACK_TURN_PAIRS_REPORT_V1_VERSION;
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
    fn turn_pairs_report_artifact_round_trip() {
        let dir = tmp_dir("turn_pairs_report_artifact_round_trip");
        let store = FsArtifactStore::new(&dir).unwrap();
        let rep = GoldenPackTurnPairsReportV1 {
            version: GOLDEN_PACK_TURN_PAIRS_REPORT_V1_VERSION,
            pack_name: "golden_turn_pairs_v1".to_string(),
            workload_hash: h(1),
            turn_count: 2,
            answers: HashListSummaryV1::from_list("answers", &[h(2), h(3)]),
            markov_traces: HashListSummaryV1::from_list("traces", &[h(4), h(5)]),
            markov_hints: HashListSummaryV1::from_list("hints", &[h(6)]),
        };

        let hh = put_golden_pack_turn_pairs_report_v1(&store, &rep).unwrap();
        let got = get_golden_pack_turn_pairs_report_v1(&store, &hh)
            .unwrap()
            .unwrap();
        assert_eq!(rep, got);
    }
}
