// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use std::path::PathBuf;
use std::process::Command;

fn tmp_dir(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push("fsa_lm_tests");
    p.push(format!("{}_{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn run_cmd(bin: &str, args: &[&str]) -> (i32, String, String) {
    let out = Command::new(bin).args(args).output().unwrap();
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
    )
}

fn find_line_value(stdout: &str, key: &str) -> Option<String> {
    let prefix = format!("{}=", key);
    for line in stdout.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix(&prefix) {
            return Some(rest.trim().to_string());
        }
    }
    None
}

fn h(ch: char) -> String {
    let mut s = String::new();
    for _ in 0..64 {
        s.push(ch);
    }
    s
}

#[test]
fn show_workspace_prints_resolved_values_and_last_wins() {
    let base = tmp_dir("show_workspace");
    let root = base.join("root");
    std::fs::create_dir_all(&root).unwrap();

    let ws_path = root.join("workspace_v1.txt");
    let txt = format!(
        "# comment\nunknown=abc\nmerged_snapshot={}\nmerged_snapshot={}\nmerged_sig_map={}\nlexicon_snapshot={}\ndefault_k=20\ndefault_expand=1\ndefault_meta=0\nmarkov_model={}\nexemplar_memory={}\ngraph_relevance={}\n",
        h('1'),
        h('2'),
        h('3'),
        h('4'),
        h('5'),
        h('6'),
        h('7')
    );
    std::fs::write(&ws_path, txt.as_bytes()).unwrap();

    let bin = env!("CARGO_BIN_EXE_fsa_lm");
    let (code, stdout, stderr) = run_cmd(
        bin,
        &[
            "show-workspace",
            "--root",
            root.to_str().unwrap(),
        ],
    );
    assert_eq!(code, 0, "stderr={}", stderr);

    assert_eq!(find_line_value(&stdout, "workspace_present").unwrap(), "1");
    assert_eq!(find_line_value(&stdout, "merged_snapshot").unwrap(), h('2'));
    assert_eq!(find_line_value(&stdout, "merged_sig_map").unwrap(), h('3'));
    assert_eq!(find_line_value(&stdout, "lexicon_snapshot").unwrap(), h('4'));
    assert_eq!(find_line_value(&stdout, "default_k").unwrap(), "20");
    assert_eq!(find_line_value(&stdout, "default_expand").unwrap(), "1");
    assert_eq!(find_line_value(&stdout, "default_meta").unwrap(), "0");
    assert_eq!(find_line_value(&stdout, "markov_model").unwrap(), h('5'));
    assert_eq!(find_line_value(&stdout, "exemplar_memory").unwrap(), h('6'));
    assert_eq!(find_line_value(&stdout, "graph_relevance").unwrap(), h('7'));
    assert_eq!(find_line_value(&stdout, "workspace_pair_ok").unwrap(), "1");
    assert_eq!(find_line_value(&stdout, "workspace_ready").unwrap(), "1");
}

#[test]
fn show_workspace_marks_pair_inconsistent() {
    let base = tmp_dir("show_workspace_inconsistent");
    let root = base.join("root");
    std::fs::create_dir_all(&root).unwrap();

    let ws_path = root.join("workspace_v1.txt");
    let txt = format!("merged_snapshot={}\n", h('a'));
    std::fs::write(&ws_path, txt.as_bytes()).unwrap();

    let bin = env!("CARGO_BIN_EXE_fsa_lm");
    let (code, stdout, stderr) = run_cmd(
        bin,
        &[
            "show-workspace",
            "--root",
            root.to_str().unwrap(),
        ],
    );
    assert_eq!(code, 0, "stderr={}", stderr);
    assert_eq!(find_line_value(&stdout, "workspace_pair_ok").unwrap(), "0");
    assert_eq!(find_line_value(&stdout, "workspace_ready").unwrap(), "0");
}
