// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use fsa_lm::artifact::FsArtifactStore;
use fsa_lm::evidence_artifact::{get_evidence_bundle_v1, put_evidence_bundle_v1};
use fsa_lm::evidence_bundle::{
    EvidenceBundleV1, EvidenceItemDataV1, EvidenceItemV1, EvidenceLimitsV1, FrameRowRefV1,
};

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

fn h(b: u8) -> [u8; 32] {
    [b; 32]
}

#[test]
fn evidence_bundle_artifact_round_trip() {
    let dir = tmp_dir("evidence_bundle_artifact_round_trip");
    let store = FsArtifactStore::new(&dir).unwrap();

    let limits = EvidenceLimitsV1 {
        segments_touched: 2,
        max_items: 16,
        max_bytes: 64 * 1024,
    };

    let mut b = EvidenceBundleV1::new(h(1), h(2), limits, 7);

    b.items.push(EvidenceItemV1 {
        score: 10,
        data: EvidenceItemDataV1::Frame(FrameRowRefV1 {
            segment_id: h(9),
            row_ix: 1,
            sketch: None,
        }),
    });

    b.items.push(EvidenceItemV1 {
        score: 5,
        data: EvidenceItemDataV1::Frame(FrameRowRefV1 {
            segment_id: h(8),
            row_ix: 2,
            sketch: None,
        }),
    });

    // Ensure canonical ordering before storing.
    b.canonicalize_in_place().unwrap();

    let hash = put_evidence_bundle_v1(&store, &b).unwrap();
    let got = get_evidence_bundle_v1(&store, &hash).unwrap().unwrap();
    assert_eq!(got, b);
}
