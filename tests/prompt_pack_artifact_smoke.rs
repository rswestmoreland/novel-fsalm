// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use fsa_lm::artifact::FsArtifactStore;
use fsa_lm::prompt_artifact::{get_prompt_pack, put_prompt_pack};
use fsa_lm::prompt_pack::{Message, PromptIds, PromptLimits, PromptPack, Role};

use std::path::PathBuf;

fn tmp_root(name: &str) -> PathBuf {
    let mut p = PathBuf::from(std::env::temp_dir());
    p.push("fsa_lm_tests");
    p.push(name);
    p
}

#[test]
fn prompt_pack_artifact_round_trip() {
    let root = tmp_root("prompt_pack_artifact_round_trip");
    let store = FsArtifactStore::new(&root).unwrap();

    let ids = PromptIds {
        snapshot_id: [1u8; 32],
        weights_id: [2u8; 32],
        tokenizer_id: [3u8; 32],
    };

    let mut p = PromptPack::new(7, 55, ids);
    p.messages.push(Message { role: Role::System, content: "SYS".to_string() });
    p.messages.push(Message { role: Role::User, content: "hello".to_string() });
    p.messages.push(Message { role: Role::Assistant, content: "world".to_string() });

    let limits = PromptLimits::default_v1();
    let h = put_prompt_pack(&store, &mut p, limits).unwrap();

    let got = get_prompt_pack(&store, &h).unwrap().unwrap();
    assert_eq!(got, p);
}

#[test]
fn prompt_pack_artifact_hash_is_stable_for_same_canonical_bytes() {
    let root = tmp_root("prompt_pack_artifact_hash_is_stable");
    let store = FsArtifactStore::new(&root).unwrap();

    let ids = PromptIds {
        snapshot_id: [9u8; 32],
        weights_id: [8u8; 32],
        tokenizer_id: [7u8; 32],
    };

    let limits = PromptLimits::default_v1();

    // Same logical content, different constraint insertion order.
    let mut p1 = PromptPack::new(1, 10, ids);
    p1.messages.push(Message { role: Role::User, content: "x".to_string() });
    p1.add_constraint("b", "2");
    p1.add_constraint("a", "1");

    let mut p2 = PromptPack::new(1, 10, ids);
    p2.messages.push(Message { role: Role::User, content: "x".to_string() });
    p2.add_constraint("a", "1");
    p2.add_constraint("b", "2");

    let h1 = put_prompt_pack(&store, &mut p1, limits).unwrap();
    let h2 = put_prompt_pack(&store, &mut p2, limits).unwrap();

    assert_eq!(h1, h2);

    let g1 = get_prompt_pack(&store, &h1).unwrap().unwrap();
    let g2 = get_prompt_pack(&store, &h2).unwrap().unwrap();
    assert_eq!(g1, g2);
}
