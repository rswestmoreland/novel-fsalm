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

fn write_workspace(root: &Path, merged_snapshot: &str, merged_sig_map: &str) {
    let s = format!("merged_snapshot={}\nmerged_sig_map={}\n", merged_snapshot, merged_sig_map);
    std::fs::write(root.join("workspace_v1.txt"), s.as_bytes()).unwrap();
}

#[test]
fn chat_save_and_resume_via_conversation_pack() {
    let base = tmp_dir("chat_save_and_resume_via_conversation_pack");
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

    // First run: ask one question, save conversation, exit.
    let mut child = Command::new(bin)
        .args(["chat", "--root", root.to_str().unwrap(), "--k", "8"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    {
        let stdin = child.stdin.as_mut().unwrap();
        stdin.write_all(b"banana\n/save\n/exit\n").unwrap();
    }
    let out = child.wait_with_output().unwrap();
    assert_eq!(out.status.code().unwrap_or(-1), 0, "stderr={}", String::from_utf8_lossy(&out.stderr));

    let stderr_s = String::from_utf8_lossy(&out.stderr).replace("\r\n", "\n");
    let conv_hex = parse_stderr_kv(&stderr_s, "conversation_pack").expect("conversation_pack= on stderr");

    // Show the stored conversation pack.
    let (scode, sout, serr) = run_cmd(
        bin,
        &["show-conversation", "--root", root.to_str().unwrap(), &conv_hex],
    );
    assert_eq!(scode, 0, "stderr={}", String::from_utf8_lossy(&serr));
    let sout_s = String::from_utf8_lossy(&sout).replace("\r\n", "\n");
    assert!(sout_s.contains(&format!("conversation_pack={}", conv_hex)));
    assert!(sout_s.contains("msg.0.role=user"));
    assert!(sout_s.contains("msg.0.content=banana"));

    // Second run: resume and ask another question.
    let mut child2 = Command::new(bin)
        .args([
            "chat",
            "--root",
            root.to_str().unwrap(),
            "--resume",
            &conv_hex,
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
}
