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
    let mut s = String::new();
    s.push_str("banana\tbanana banana is a fruit\n");
    std::fs::write(path, s.as_bytes()).unwrap();
}

fn is_hex64(s: &str) -> bool {
    s.len() == 64 && s.as_bytes().iter().all(|b| b"0123456789abcdef".contains(b))
}

fn parse_hash_line(stdout: &str, key: &str) -> Option<String> {
    let prefix = format!("{}=", key);
    for line in stdout.lines() {
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

fn write_workspace_with_lexicon(
    root: &Path,
    lexicon_snapshot: &str,
    default_k: u32,
    markov_model: &str,
    exemplar_memory: &str,
    graph_relevance: &str,
) {
    let mut s = String::new();
    s.push_str(&format!("lexicon_snapshot={}\n", lexicon_snapshot));
    s.push_str(&format!("default_k={}\n", default_k));
    s.push_str(&format!("markov_model={}\n", markov_model));
    s.push_str(&format!("exemplar_memory={}\n", exemplar_memory));
    s.push_str(&format!("graph_relevance={}\n", graph_relevance));
    std::fs::write(root.join("workspace_v1.txt"), s.as_bytes()).unwrap();
}

fn read_file(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap().replace("\r\n", "\n")
}

#[test]
fn load_wikipedia_writes_workspace_defaults_and_preserves_lexicon() {
    let base = tmp_dir("load_wikipedia_writes_workspace");
    let root = base.join("root");
    std::fs::create_dir_all(&root).unwrap();

    let wiki_tsv = base.join("wiki.tsv");
    write_wiki_tsv(&wiki_tsv);

    // Pre-seed workspace with a lexicon snapshot and a user default.
    let lex = "2222222222222222222222222222222222222222222222222222222222222222";
    let markov_model = "5555555555555555555555555555555555555555555555555555555555555555";
    let exemplar_memory = "6666666666666666666666666666666666666666666666666666666666666666";
    let graph_relevance = "7777777777777777777777777777777777777777777777777777777777777777";
    write_workspace_with_lexicon(
        &root,
        lex,
        7,
        markov_model,
        exemplar_memory,
        graph_relevance,
    );

    let bin = env!("CARGO_BIN_EXE_fsa_lm");
    let out = Command::new(bin)
        .args([
            "load-wikipedia",
            "--root",
            root.to_str().unwrap(),
            "--dump",
            wiki_tsv.to_str().unwrap(),
            "--shards",
            "2",
            "--max_docs",
            "1",
        ])
        .output()
        .unwrap();

    assert_eq!(
        out.status.code().unwrap_or(-1),
        0,
        "stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout).replace("\r\n", "\n");
    assert!(stdout.contains("workspace_written=1"));

    let merged_snapshot =
        parse_hash_line(&stdout, "merged_snapshot").expect("merged_snapshot line");
    let merged_sig_map = parse_hash_line(&stdout, "merged_sig_map").expect("merged_sig_map line");

    let ws_text = read_file(&root.join("workspace_v1.txt"));
    assert!(ws_text.contains(&format!("merged_snapshot={}\n", merged_snapshot)));
    assert!(ws_text.contains(&format!("merged_sig_map={}\n", merged_sig_map)));
    assert!(
        ws_text.contains(&format!("lexicon_snapshot={}\n", lex)),
        "expected lexicon_snapshot preserved"
    );
    assert!(
        ws_text.contains("default_k=7\n"),
        "expected default_k preserved"
    );
    assert!(
        ws_text.contains(&format!("markov_model={}\n", markov_model)),
        "expected markov_model preserved"
    );
    assert!(
        ws_text.contains(&format!("exemplar_memory={}\n", exemplar_memory)),
        "expected exemplar_memory preserved"
    );
    assert!(
        ws_text.contains(&format!("graph_relevance={}\n", graph_relevance)),
        "expected graph_relevance preserved"
    );
}
