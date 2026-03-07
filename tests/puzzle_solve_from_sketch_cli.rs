// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use fsa_lm::artifact::FsArtifactStore;
use fsa_lm::conversation_pack::ConversationRole;
use fsa_lm::conversation_pack_artifact::get_conversation_pack;
use fsa_lm::hash::Hash32;
use fsa_lm::proof_artifact_store::get_proof_artifact_v1;
use fsa_lm::replay_artifact::get_replay_log;
use fsa_lm::replay_steps::STEP_PROOF_ARTIFACT_V1;

use std::path::{Path, PathBuf};
use std::process::Command;

fn tmp_dir(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push("fsa_lm_tests");
    p.push(format!("{}_{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn write_wiki_tsv(path: &Path) {
    // Minimal corpus just to satisfy workspace/index requirements.
    let line = "banana\tbanana banana is a fruit\n";
    std::fs::write(path, line.as_bytes()).unwrap();
}

fn run_cmd(bin: &str, args: &[&str]) -> (i32, String, String) {
    let out = Command::new(bin).args(args).output().unwrap();
    let code = out.status.code().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&out.stdout).replace("\r\n", "\n");
    let stderr = String::from_utf8_lossy(&out.stderr).replace("\r\n", "\n");
    (code, stdout, stderr)
}

fn is_hex64(s: &str) -> bool {
    s.len() == 64 && s.as_bytes().iter().all(|b| b"0123456789abcdef".contains(b))
}

fn parse_first_hex(stdout: &str) -> Option<String> {
    for line in stdout.lines() {
        let v = line.trim();
        if is_hex64(v) {
            return Some(v.to_string());
        }
    }
    None
}

fn parse_stderr_kv(stderr: &str, key: &str) -> Option<String> {
    let prefix = format!("{}=", key);
    for line in stderr.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix(&prefix) {
            let v = rest.trim();
            if is_hex64(v) {
                return Some(v.to_string());
            }
        }
    }
    None
}

fn parse_file_kv(path: &Path, key: &str) -> Option<String> {
    let s = std::fs::read_to_string(path).ok()?.replace("\r\n", "\n");
    let prefix = format!("{}=", key);
    let mut last: Option<String> = None;
    for raw in s.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix(&prefix) {
            let v = rest.trim();
            if is_hex64(v) {
                last = Some(v.to_string());
            }
        }
    }
    last
}

fn write_workspace(root: &Path, merged_snapshot: &str, merged_sig_map: &str) {
    let s = format!("merged_snapshot={}\nmerged_sig_map={}\n", merged_snapshot, merged_sig_map);
    std::fs::write(root.join("workspace_v1.txt"), s.as_bytes()).unwrap();
}

fn hex_to_hash32(s: &str) -> Hash32 {
    fsa_lm::hash::parse_hash32_hex(s).unwrap()
}

fn last_assistant_replay(pack: &fsa_lm::conversation_pack::ConversationPackV1) -> Hash32 {
    let mut last: Option<Hash32> = None;
    for m in &pack.messages {
        if m.role == ConversationRole::Assistant {
            last = m.replay_id;
        }
    }
    last.expect("assistant replay id")
}

fn find_proof_step_output(rlog: &fsa_lm::replay::ReplayLog) -> Hash32 {
    for s in &rlog.steps {
        if s.name == STEP_PROOF_ARTIFACT_V1 {
            assert_eq!(s.outputs.len(), 1, "outputs={:?}", s.outputs);
            return s.outputs[0];
        }
    }
    panic!("proof-artifact-v1 step not found");
}

fn build_min_workspace(base: &Path, root: &Path, bin: &str) {
    let wiki_tsv = base.join("wiki.tsv");
    write_wiki_tsv(&wiki_tsv);

    let (wcode, _wout, werr) = run_cmd(
        bin,
        &[
            "ingest-wiki",
            "--dump",
            wiki_tsv.to_str().unwrap(),
            "--root",
            root.to_str().unwrap(),
            "--max_docs",
            "1",
        ],
    );
    assert_eq!(wcode, 0, "stderr={}", werr);

    let (bcode, bout, berr) = run_cmd(bin, &["build-index", "--root", root.to_str().unwrap()]);
    assert_eq!(bcode, 0, "stderr={}", berr);
    let idx_snap_hex = parse_first_hex(&bout).expect("snapshot hash on stdout");
    let sig_map_hex = parse_stderr_kv(&berr, "index_sig_map").expect("index_sig_map on stderr");
    write_workspace(root, &idx_snap_hex, &sig_map_hex);
}

fn puzzle_block_prompt() -> String {
    // A strict, structured puzzle block that the solver supports deterministically.
    // Use newlines (not semicolons) so the parser sees separate constraint lines.
    let s = "[puzzle]\nvars: A,B,C\ndomain: 1..3\nexpect_unique: true\nconstraints:\nA = 1\nB = 2\nC = 3\n[/puzzle]\n";
    s.to_string()
}

#[test]
fn puzzle_two_turn_block_solves_and_records_proof() {
    let base = tmp_dir("puzzle_two_turn_block_solves_and_records_proof");
    let root = base.join("root");
    std::fs::create_dir_all(&root).unwrap();

    let bin = env!("CARGO_BIN_EXE_fsa_lm");
    build_min_workspace(&base, &root, bin);

    let session_file = base.join("session.txt");

    // Turn 1: any non-puzzle query to create a session entry.
    let (c1, _out1, err1) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_file.to_str().unwrap(),
            "--text",
            "hello",
        ],
    );
    assert_eq!(c1, 0, "stderr={}", err1);

    // Turn 2: a strict puzzle block that must produce a proof artifact + replay step.
    let prompt = puzzle_block_prompt();
    let (c2, _out2, err2) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_file.to_str().unwrap(),
            "--text",
            &prompt,
        ],
    );
    assert_eq!(c2, 0, "stderr={}", err2);

    let conv_hex = parse_file_kv(&session_file, "conversation_pack").expect("conversation_pack in session file");
    let conv_hash = hex_to_hash32(&conv_hex);

    let store = FsArtifactStore::new(&root).unwrap();
    let pack = get_conversation_pack(&store, &conv_hash).unwrap().unwrap();
    let replay_id = last_assistant_replay(&pack);

    let rlog = get_replay_log(&store, &replay_id).unwrap().unwrap();
    let proof_hash = find_proof_step_output(&rlog);

    let proof = get_proof_artifact_v1(&store, &proof_hash).unwrap().unwrap();
    assert_eq!(proof.vars, vec!["A".to_string(), "B".to_string(), "C".to_string()]);
    assert_eq!(proof.domain, vec![1, 2, 3]);
    assert!(!proof.solutions.is_empty(), "expected at least one solution");
}

#[test]
fn puzzle_block_proof_hash_is_stable_across_session_advances() {
    let base = tmp_dir("puzzle_block_proof_hash_is_stable_across_session_advances");
    let root = base.join("root");
    std::fs::create_dir_all(&root).unwrap();

    let bin = env!("CARGO_BIN_EXE_fsa_lm");
    build_min_workspace(&base, &root, bin);

    let session_file = base.join("session.txt");
    let prompt = puzzle_block_prompt();

    let store = FsArtifactStore::new(&root).unwrap();

    // Run 1
    let (c1, _out1, err1) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_file.to_str().unwrap(),
            "--text",
            &prompt,
        ],
    );
    assert_eq!(c1, 0, "stderr={}", err1);

    let conv1_hex = parse_file_kv(&session_file, "conversation_pack").expect("conversation_pack after first run");
    let conv1_hash = hex_to_hash32(&conv1_hex);
    let pack1 = get_conversation_pack(&store, &conv1_hash).unwrap().unwrap();
    let r1 = last_assistant_replay(&pack1);
    let log1 = get_replay_log(&store, &r1).unwrap().unwrap();
    let proof1 = find_proof_step_output(&log1);

    // Run 2 (same prompt, session advances)
    let (c2, _out2, err2) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_file.to_str().unwrap(),
            "--text",
            &prompt,
        ],
    );
    assert_eq!(c2, 0, "stderr={}", err2);

    let conv2_hex = parse_file_kv(&session_file, "conversation_pack").expect("conversation_pack after second run");
    let conv2_hash = hex_to_hash32(&conv2_hex);
    let pack2 = get_conversation_pack(&store, &conv2_hash).unwrap().unwrap();
    let r2 = last_assistant_replay(&pack2);
    let log2 = get_replay_log(&store, &r2).unwrap().unwrap();
    let proof2 = find_proof_step_output(&log2);

    assert_eq!(proof1, proof2, "proof hash must be stable");
}
