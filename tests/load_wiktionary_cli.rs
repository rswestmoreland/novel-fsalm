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

fn read_file(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap().replace("\r\n", "\n")
}

fn write_workspace_with_merged(root: &Path, merged_snapshot: &str, merged_sig_map: &str, default_k: u32) {
    let mut s = String::new();
    s.push_str(&format!("merged_snapshot={}\n", merged_snapshot));
    s.push_str(&format!("merged_sig_map={}\n", merged_sig_map));
    s.push_str(&format!("default_k={}\n", default_k));
    std::fs::write(root.join("workspace_v1.txt"), s.as_bytes()).unwrap();
}

#[test]
fn load_wiktionary_writes_workspace_lexicon_and_preserves_merged() {
    let base = tmp_dir("load_wiktionary_writes_workspace");
    let root = base.join("root");
    std::fs::create_dir_all(&root).unwrap();

    let merged_snapshot = "1111111111111111111111111111111111111111111111111111111111111111";
    let merged_sig_map = "3333333333333333333333333333333333333333333333333333333333333333";
    write_workspace_with_merged(&root, merged_snapshot, merged_sig_map, 9);

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let tiny_xml = manifest_dir.join("examples").join("wiktionary_tiny.xml");
    assert!(tiny_xml.exists(), "missing fixture: {}", tiny_xml.display());

    let bin = env!("CARGO_BIN_EXE_fsa_lm");
    let out = Command::new(bin)
        .args([
            "load-wiktionary",
            "--root",
            root.to_str().unwrap(),
            "--xml",
            tiny_xml.to_str().unwrap(),
            "--segments",
            "1",
            "--max_pages",
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

    let lexicon_snapshot = parse_hash_line(&stdout, "lexicon_snapshot").expect("lexicon_snapshot line");

    let ws_text = read_file(&root.join("workspace_v1.txt"));
    assert!(ws_text.contains(&format!("merged_snapshot={}\n", merged_snapshot)));
    assert!(ws_text.contains(&format!("merged_sig_map={}\n", merged_sig_map)));
    assert!(ws_text.contains(&format!("lexicon_snapshot={}\n", lexicon_snapshot)));
    assert!(ws_text.contains("default_k=9\n"));
}
