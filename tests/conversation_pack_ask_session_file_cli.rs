// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

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

fn run_cmd(bin: &str, args: &[&str]) -> (i32, Vec<u8>, Vec<u8>) {
    let out = Command::new(bin).args(args).output().unwrap();
    (out.status.code().unwrap_or(-1), out.stdout, out.stderr)
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
    let s = std::fs::read_to_string(path).ok()?;
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
    let s = format!(
        "merged_snapshot={}\nmerged_sig_map={}\n",
        merged_snapshot, merged_sig_map
    );
    std::fs::write(root.join("workspace_v1.txt"), s.as_bytes()).unwrap();
}

#[test]
fn ask_session_file_saves_and_resumes_conversation() {
    let base = tmp_dir("ask_session_file_saves_and_resumes_conversation");
    let root = base.join("root");
    std::fs::create_dir_all(&root).unwrap();

    let wiki_tsv = base.join("wiki.tsv");
    write_wiki_tsv(&wiki_tsv);

    let bin = env!("CARGO_BIN_EXE_fsa_lm");

    // Build a minimal index and write workspace defaults.
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
    assert_eq!(wcode, 0, "stderr={}", String::from_utf8_lossy(&werr));

    let (bcode, bout, berr) = run_cmd(bin, &["build-index", "--root", root.to_str().unwrap()]);
    assert_eq!(bcode, 0, "stderr={}", String::from_utf8_lossy(&berr));
    let bout_s = String::from_utf8_lossy(&bout).replace("\r\n", "\n");
    let berr_s = String::from_utf8_lossy(&berr).replace("\r\n", "\n");
    let idx_snap_hex = parse_first_hex(&bout_s).expect("snapshot hash on stdout");
    let sig_map_hex = parse_stderr_kv(&berr_s, "index_sig_map").expect("index_sig_map= on stderr");
    write_workspace(&root, &idx_snap_hex, &sig_map_hex);

    let session_path = base.join("ask_session.txt");

    // First run: ask a question and ensure it writes the session file.
    let (acode, aout, aerr) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "--k",
            "8",
            "banana",
        ],
    );
    assert_eq!(acode, 0, "stderr={}", String::from_utf8_lossy(&aerr));
    let aout_s = String::from_utf8_lossy(&aout);
    assert!(aout_s.contains("Answer v1"), "stdout={}", aout_s);

    let conv1 = parse_file_kv(&session_path, "conversation_pack")
        .expect("conversation_pack= in session file");
    assert!(is_hex64(&conv1));

    let (scode, sout, serr) = run_cmd(
        bin,
        &[
            "show-conversation",
            "--root",
            root.to_str().unwrap(),
            &conv1,
        ],
    );
    assert_eq!(scode, 0, "stderr={}", String::from_utf8_lossy(&serr));
    let sout_s = String::from_utf8_lossy(&sout).replace("\r\n", "\n");
    assert!(sout_s.contains(&format!("conversation_pack={}", conv1)));
    assert!(sout_s.contains("content=banana"));

    // Second run: uses the same session file and should advance it.
    let (acode2, aout2, aerr2) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "--k",
            "8",
            "night",
        ],
    );
    assert_eq!(acode2, 0, "stderr={}", String::from_utf8_lossy(&aerr2));
    let aout2_s = String::from_utf8_lossy(&aout2);
    assert!(aout2_s.contains("Answer v1"), "stdout={}", aout2_s);

    let conv2 = parse_file_kv(&session_path, "conversation_pack")
        .expect("conversation_pack= in session file after resume");
    assert!(is_hex64(&conv2));
    assert_ne!(conv1, conv2, "session pointer should advance after ask");

    let (scode2, sout2, serr2) = run_cmd(
        bin,
        &[
            "show-conversation",
            "--root",
            root.to_str().unwrap(),
            &conv2,
        ],
    );
    assert_eq!(scode2, 0, "stderr={}", String::from_utf8_lossy(&serr2));
    let sout2_s = String::from_utf8_lossy(&sout2).replace("\r\n", "\n");
    assert!(sout2_s.contains("content=banana"));
    assert!(sout2_s.contains("content=night"));
}
