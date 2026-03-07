// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use fsa_lm::artifact::FsArtifactStore;
use fsa_lm::prompt_artifact::put_prompt_pack;
use fsa_lm::prompt_pack::{Message, PromptIds, PromptLimits, PromptPack, Role};
use fsa_lm::replay::ReplayLog;
use fsa_lm::replay_artifact::{append_prompt_step, get_replay_log, put_replay_log};

use std::path::PathBuf;

fn tmp_root(name: &str) -> PathBuf {
    let mut p = PathBuf::from(std::env::temp_dir());
    p.push("fsa_lm_tests");
    p.push(name);
    p
}

#[test]
fn replay_log_can_reference_prompt_pack_hash() {
    let root = tmp_root("replay_log_can_reference_prompt_pack_hash");
    let store = FsArtifactStore::new(&root).unwrap();

    let ids = PromptIds {
        snapshot_id: [1u8; 32],
        weights_id: [2u8; 32],
        tokenizer_id: [3u8; 32],
    };

    let mut p = PromptPack::new(1, 32, ids);
    p.messages.push(Message { role: Role::User, content: "hi".to_string() });

    let limits = PromptLimits::default_v1();
    let prompt_hash = put_prompt_pack(&store, &mut p, limits).unwrap();

    let mut log = ReplayLog::new();
    append_prompt_step(&mut log, "prompt", prompt_hash);

    let replay_hash = put_replay_log(&store, &log).unwrap();

    let got = get_replay_log(&store, &replay_hash).unwrap().unwrap();
    assert_eq!(got.steps.len(), 1);
    assert_eq!(got.steps[0].name, "prompt");
    assert_eq!(got.steps[0].inputs.len(), 0);
    assert_eq!(got.steps[0].outputs, vec![prompt_hash]);
}
