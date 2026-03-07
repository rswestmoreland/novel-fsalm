// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use fsa_lm::artifact::{ArtifactStore, FsArtifactStore};
use fsa_lm::hash::blake3_hash;
use fsa_lm::replay::{ReplayLog, ReplayStep};

#[test]
fn artifacts_and_replay_smoke() {
    let dir = std::env::temp_dir()
        .join("fsa_lm_tests")
        .join("artifacts_and_replay_smoke");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let store = FsArtifactStore::new(&dir).unwrap();

    let a = b"artifact-a";
    let b = b"artifact-b";
    let ha = store.put(a).unwrap();
    let hb = store.put(b).unwrap();
    assert_eq!(ha, blake3_hash(a));
    assert_eq!(hb, blake3_hash(b));

    let mut log = ReplayLog::new();
    log.steps.push(ReplayStep {
        name: "put".to_string(),
        inputs: vec![],
        outputs: vec![ha, hb],
    });

    let enc = log.encode().unwrap();
    let dec = ReplayLog::decode(&enc).unwrap();
    assert_eq!(dec.steps.len(), 1);
    assert_eq!(dec.steps[0].name, "put");
}
