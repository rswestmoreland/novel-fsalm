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

fn write_wiktionary_xml(path: &Path) {
    let xml = r#"<mediawiki>
<page>
  <title>banana</title>
  <ns>0</ns>
  <revision>
    <text xml:space=\"preserve\">==English==
===Noun===
# {{lb|en|count noun}} A banana is a fruit.
====Synonyms====
* [[plantain|plantain]]
====Pronunciation====
* {{IPA|en|/bəˈnænə/}}
</text>
  </revision>
</page>
</mediawiki>"#;
    std::fs::write(path, xml.as_bytes()).unwrap();
}

fn run_cmd(bin: &str, args: &[&str]) -> (i32, String, String) {
    let out = Command::new(bin).args(args).output().unwrap();
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
    )
}

fn is_hex64(s: &str) -> bool {
    s.len() == 64 && s.as_bytes().iter().all(|b| b"0123456789abcdef".contains(b))
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

fn parse_u64_line(stdout: &str, key: &str) -> Option<u64> {
    let v = find_line_value(stdout, key)?;
    v.parse::<u64>().ok()
}

#[test]
fn ingest_wiktionary_xml_stats_flag_emits_counts() {
    let base = tmp_dir("wiktionary_ingest_stats");
    let root = base.join("root");
    std::fs::create_dir_all(&root).unwrap();

    let xml_path = base.join("wiktionary.xml");
    write_wiktionary_xml(&xml_path);

    let bin = env!("CARGO_BIN_EXE_fsa_lm");

    let (code, stdout, stderr) = run_cmd(
        bin,
        &[
            "ingest-wiktionary-xml",
            "--root",
            root.to_str().unwrap(),
            "--xml",
            xml_path.to_str().unwrap(),
            "--segments",
            "1",
            "--stats",
        ],
    );
    assert_eq!(code, 0, "stderr={}", stderr);

    let seg = find_line_value(&stdout, "segment").unwrap();
    assert!(is_hex64(&seg), "segment={}", seg);

    let snap = find_line_value(&stdout, "lexicon_snapshot").unwrap();
    assert!(is_hex64(&snap), "lexicon_snapshot={}", snap);

    assert_eq!(parse_u64_line(&stdout, "pages_seen").unwrap(), 1);
    assert_eq!(parse_u64_line(&stdout, "pages_english").unwrap(), 1);

    let lemmas = parse_u64_line(&stdout, "lemmas").unwrap();
    let senses = parse_u64_line(&stdout, "senses").unwrap();
    let rel_edges = parse_u64_line(&stdout, "rel_edges").unwrap();
    let prons = parse_u64_line(&stdout, "prons").unwrap();
    assert!(lemmas >= 1, "lemmas={}", lemmas);
    assert!(senses >= 1, "senses={}", senses);
    assert!(rel_edges >= 1, "rel_edges={}", rel_edges);
    assert!(prons >= 1, "prons={}", prons);

    assert_eq!(parse_u64_line(&stdout, "segments_written").unwrap(), 1);
}
