// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

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
    let s = format!("merged_snapshot={}\nmerged_sig_map={}\n", merged_snapshot, merged_sig_map);
    std::fs::write(root.join("workspace_v1.txt"), s.as_bytes()).unwrap();
}

#[test]
fn chat_session_file_autosave_and_resume() {
    let base = tmp_dir("chat_session_file_autosave_and_resume");
    let root = base.join("root");
    std::fs::create_dir_all(&root).unwrap();

    let wiki_tsv = base.join("wiki.tsv");
    write_wiki_tsv(&wiki_tsv);

    let bin = env!("CARGO_BIN_EXE_fsa_lm");

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

    let session_path = base.join("chat_session.txt");

    // First run: one question then exit. Autosave should write the session file.
    let mut child = Command::new(bin)
        .args([
            "chat",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "--autosave",
            "--k",
            "8",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    {
        let stdin = child.stdin.as_mut().unwrap();
        stdin.write_all(b"banana\n/exit\n").unwrap();
    }
    let out = child.wait_with_output().unwrap();
    assert_eq!(out.status.code().unwrap_or(-1), 0, "stderr={}", String::from_utf8_lossy(&out.stderr));

    let conv1 = parse_file_kv(&session_path, "conversation_pack").expect("conversation_pack= in session file");

    let (scode, sout, serr) = run_cmd(bin, &["show-conversation", "--root", root.to_str().unwrap(), &conv1]);
    assert_eq!(scode, 0, "stderr={}", String::from_utf8_lossy(&serr));
    let sout_s = String::from_utf8_lossy(&sout).replace("\r\n", "\n");
    assert!(sout_s.contains(&format!("conversation_pack={}", conv1)));
    assert!(sout_s.contains("content=banana"));

    // Second run: resume implicitly via session-file.
    let mut child2 = Command::new(bin)
        .args([
            "chat",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "--autosave",
            "--k",
            "8",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    {
        let stdin = child2.stdin.as_mut().unwrap();
        stdin.write_all(b"night\n/exit\n").unwrap();
    }
    let out2 = child2.wait_with_output().unwrap();
    assert_eq!(out2.status.code().unwrap_or(-1), 0, "stderr={}", String::from_utf8_lossy(&out2.stderr));
    let stdout2 = String::from_utf8_lossy(&out2.stdout);
    assert!(stdout2.contains("Answer v1"), "stdout={}", stdout2);

    let conv2 = parse_file_kv(&session_path, "conversation_pack").expect("conversation_pack= in session file after resume");
    assert!(is_hex64(&conv2));
    assert_ne!(conv1, conv2, "autosave should advance the session pointer");
}
