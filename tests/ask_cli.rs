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
* {{m|en|dessert banana}}
====Pronunciation====
* {{IPA|en|/bəˈnænə/}}
</text>
  </revision>
</page>
</mediawiki>"#;
    std::fs::write(path, xml.as_bytes()).unwrap();
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

fn write_workspace(
    root: &Path,
    merged_snapshot: &str,
    merged_sig_map: &str,
    lexicon_snapshot: Option<&str>,
) {
    let mut s = String::new();
    s.push_str(&format!("merged_snapshot={}\n", merged_snapshot));
    s.push_str(&format!("merged_sig_map={}\n", merged_sig_map));
    if let Some(h) = lexicon_snapshot {
        s.push_str(&format!("lexicon_snapshot={}\n", h));
    }
    std::fs::write(root.join("workspace_v1.txt"), s.as_bytes()).unwrap();
}

#[test]
fn ask_runs_answer_pipeline_without_prompt_hash() {
    let base = tmp_dir("ask_runs_answer_pipeline_without_prompt_hash");
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

    write_workspace(&root, &idx_snap_hex, &sig_map_hex, None);

    let out_path = base.join("ask_out.txt");
    let (acode, _aout, aerr) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--k",
            "8",
            "--out-file",
            out_path.to_str().unwrap(),
            "banana",
        ],
    );
    assert_eq!(acode, 0, "stderr={}", String::from_utf8_lossy(&aerr));
    let s = std::fs::read_to_string(&out_path).unwrap();
    assert!(s.contains("Answer v1"));
    assert!(
        s.contains("[E0]"),
        "expected evidence using workspace defaults"
    );
}

#[test]
fn ask_expand_uses_workspace_lexicon_snapshot_when_omitted() {
    let base = tmp_dir("ask_expand_uses_workspace_lexicon_snapshot_when_omitted");
    let root = base.join("root");
    std::fs::create_dir_all(&root).unwrap();

    let wiki_tsv = base.join("wiki.tsv");
    write_wiki_tsv(&wiki_tsv);

    let xml_path = base.join("wiktionary.xml");
    write_wiktionary_xml(&xml_path);

    let bin = env!("CARGO_BIN_EXE_fsa_lm");

    let (lxcode, lxout, lxerr) = run_cmd(
        bin,
        &[
            "ingest-wiktionary-xml",
            "--root",
            root.to_str().unwrap(),
            "--xml",
            xml_path.to_str().unwrap(),
            "--segments",
            "2",
            "--max_pages",
            "10",
        ],
    );
    assert_eq!(lxcode, 0, "stderr={}", String::from_utf8_lossy(&lxerr));
    let lxout_s = String::from_utf8_lossy(&lxout).replace("\r\n", "\n");
    let lex_snap_hex =
        parse_hash_line(&lxout_s, "lexicon_snapshot").expect("lexicon_snapshot line");

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

    write_workspace(&root, &idx_snap_hex, &sig_map_hex, Some(&lex_snap_hex));

    let out_path = base.join("ask_expand_out.txt");
    let (acode, _aout, aerr) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--expand",
            "--k",
            "8",
            "--out-file",
            out_path.to_str().unwrap(),
            "bananas",
        ],
    );
    assert_eq!(acode, 0, "stderr={}", String::from_utf8_lossy(&aerr));
    let s = std::fs::read_to_string(&out_path).unwrap();
    assert!(s.contains("Answer v1"));
    assert!(
        s.contains("[E0]"),
        "expected evidence using workspace defaults"
    );
}
