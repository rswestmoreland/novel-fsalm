// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use fsa_lm::artifact::FsArtifactStore;
use fsa_lm::conversation_pack::ConversationRole;
use fsa_lm::conversation_pack_artifact::get_conversation_pack;
use fsa_lm::hash::Hash32;
use fsa_lm::puzzle_sketch_artifact::{PSA_FLAG_PENDING, PUZZLE_SKETCH_ARTIFACT_V1_VERSION};
use fsa_lm::puzzle_sketch_artifact_store::get_puzzle_sketch_artifact_v1;
use fsa_lm::replay_artifact::get_replay_log;
use fsa_lm::replay_steps::STEP_PUZZLE_SKETCH_V1;

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
    for line in s.lines() {
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

fn hex_to_hash32(s: &str) -> Hash32 {
    fsa_lm::hash::parse_hash32_hex(s).unwrap()
}

fn write_workspace(root: &Path, merged_snapshot: &str, merged_sig_map: &str) {
    let mut s = String::new();
    s.push_str(&format!("merged_snapshot={}\n", merged_snapshot));
    s.push_str(&format!("merged_sig_map={}\n", merged_sig_map));
    std::fs::write(root.join("workspace_v1.txt"), s.as_bytes()).unwrap();
}

#[test]
fn puzzle_sketch_replay_step_is_recorded_when_clarifying() {
    let base = tmp_dir("puzzle_sketch_replay_step_is_recorded_when_clarifying");
    let root = base.join("root");
    std::fs::create_dir_all(&root).unwrap();

    let bin = env!("CARGO_BIN_EXE_fsa_lm");

    // Minimal wiki ingest + index so ask can run from workspace defaults.
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

    write_workspace(&root, &idx_snap_hex, &sig_map_hex);

    // Run ask with a session file so the assistant replay id is persisted.
    let session_file = base.join("session.txt");

    let prompt = "Alice, Bob, and Carol each have a different fruit. Alice does not have the banana. Bob has the apple. Who has the banana?";

    let (acode, aout, aerr) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_file.to_str().unwrap(),
            "--text",
            prompt,
        ],
    );
    assert_eq!(acode, 0, "stderr={}", aerr);
    assert!(aout.contains("Quick question:"), "stdout={}", aout);

    let conv_hex = parse_file_kv(&session_file, "conversation_pack")
        .expect("conversation_pack in session file");
    let conv_hash = hex_to_hash32(&conv_hex);

    let store = FsArtifactStore::new(&root).unwrap();
    let pack = get_conversation_pack(&store, &conv_hash).unwrap().unwrap();

    let mut last_replay: Option<Hash32> = None;
    for m in &pack.messages {
        if m.role == ConversationRole::Assistant {
            last_replay = m.replay_id;
        }
    }
    let replay_hash = last_replay.expect("assistant replay id");

    let rlog = get_replay_log(&store, &replay_hash).unwrap().unwrap();

    let mut sketch_hash_opt: Option<Hash32> = None;
    for s in &rlog.steps {
        if s.name == STEP_PUZZLE_SKETCH_V1 {
            assert_eq!(s.outputs.len(), 1, "outputs={:?}", s.outputs);
            sketch_hash_opt = Some(s.outputs[0]);
            break;
        }
    }
    let sketch_hash = sketch_hash_opt.expect("puzzle-sketch-v1 step present");

    let psa = get_puzzle_sketch_artifact_v1(&store, &sketch_hash)
        .unwrap()
        .unwrap();
    assert_eq!(psa.version, PUZZLE_SKETCH_ARTIFACT_V1_VERSION);
    assert!((psa.flags & PSA_FLAG_PENDING) != 0, "flags={}", psa.flags);
    assert!(psa.is_logic_puzzle_likely);
}
