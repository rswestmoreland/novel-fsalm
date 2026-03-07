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
    // Minimal deterministic Wiktionary XML with an English section.
    // The entry includes a noun sense, relations, and an IPA template.
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
====Derived terms====
* [[banana bread|banana bread]]
====Meronyms====
* [[banana peel|banana peel]]
====Pronunciation====
* {{IPA|en|/bəˈnænə/}}
</text>
  </revision>
</page>
</mediawiki>"#;
    std::fs::write(path, xml.as_bytes()).unwrap();
}

fn write_wiki_tsv(path: &Path) {
    // TSV format: title<TAB>text\n
    // Include "banana" but not "bananas" so plural requires expansion.
    let line = "banana\tbanana banana is a fruit\n";
    std::fs::write(path, line.as_bytes()).unwrap();
}

fn run_cmd(bin: &str, args: &[&str]) -> (i32, Vec<u8>, Vec<u8>) {
    let out = Command::new(bin).args(args).output().unwrap();
    (
        out.status.code().unwrap_or(-1),
        out.stdout,
        out.stderr,
    )
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

#[test]
fn wiktionary_ingest_produces_snapshot_and_enables_query_expansion() {
    let base = tmp_dir("wiktionary_ingest_produces_snapshot_and_enables_query_expansion");
    let root1 = base.join("root1");
    let root2 = base.join("root2");
    std::fs::create_dir_all(&root1).unwrap();
    std::fs::create_dir_all(&root2).unwrap();

    let xml_path = base.join("wiktionary.xml");
    write_wiktionary_xml(&xml_path);

    let wiki_tsv = base.join("wiki.tsv");
    write_wiki_tsv(&wiki_tsv);

    let bin = env!("CARGO_BIN_EXE_fsa_lm");

    // Ingest Wiktionary into root1 and capture the lexicon snapshot hash.
    let (code1, out1, err1) = run_cmd(
        bin,
        &[
            "ingest-wiktionary-xml",
            "--root",
            root1.to_str().unwrap(),
            "--xml",
            xml_path.to_str().unwrap(),
            "--segments",
            "2",
            "--max_pages",
            "10",
        ],
    );
    assert_eq!(
        code1,
        0,
        "ingest-wiktionary-xml failed: stderr={}",
        String::from_utf8_lossy(&err1)
    );
    let out1s = String::from_utf8_lossy(&out1).replace("\r\n", "\n");
    let lex_snap_hex_1 = parse_hash_line(&out1s, "lexicon_snapshot").expect("lexicon_snapshot line");

    // Validate snapshot via CLI.
    let (vcode, _vout, verr) = run_cmd(
        bin,
        &[
            "validate-lexicon-snapshot",
            "--root",
            root1.to_str().unwrap(),
            "--snapshot",
            &lex_snap_hex_1,
        ],
    );
    assert_eq!(
        vcode,
        0,
        "validate-lexicon-snapshot failed: stderr={}",
        String::from_utf8_lossy(&verr)
    );

    // Determinism: ingest into a fresh root2 and confirm snapshot hash matches.
    let (code2, out2, err2) = run_cmd(
        bin,
        &[
            "ingest-wiktionary-xml",
            "--root",
            root2.to_str().unwrap(),
            "--xml",
            xml_path.to_str().unwrap(),
            "--segments",
            "2",
            "--max_pages",
            "10",
        ],
    );
    assert_eq!(
        code2,
        0,
        "ingest-wiktionary-xml (root2) failed: stderr={}",
        String::from_utf8_lossy(&err2)
    );
    let out2s = String::from_utf8_lossy(&out2).replace("\r\n", "\n");
    let lex_snap_hex_2 = parse_hash_line(&out2s, "lexicon_snapshot").expect("lexicon_snapshot line (root2)");
    assert_eq!(lex_snap_hex_2, lex_snap_hex_1);

    // Ingest a minimal Wikipedia TSV into root1.
    let (wcode, _wout, werr) = run_cmd(
        bin,
        &[
            "ingest-wiki",
            "--dump",
            wiki_tsv.to_str().unwrap(),
            "--root",
            root1.to_str().unwrap(),
            "--max_docs",
            "1",
        ],
    );
    assert_eq!(
        wcode,
        0,
        "ingest-wiki failed: stderr={}",
        String::from_utf8_lossy(&werr)
    );

    // Build index snapshot + sig-map.
    let (bcode, bout, berr) = run_cmd(
        bin,
        &["build-index", "--root", root1.to_str().unwrap()],
    );
    assert_eq!(
        bcode,
        0,
        "build-index failed: stderr={}",
        String::from_utf8_lossy(&berr)
    );
    let bout_s = String::from_utf8_lossy(&bout).replace("\r\n", "\n");
    let berr_s = String::from_utf8_lossy(&berr).replace("\r\n", "\n");
    let idx_snap_hex = parse_first_hex(&bout_s).expect("snapshot hash on stdout");
    let sig_map_hex = parse_stderr_kv(&berr_s, "index_sig_map").expect("index_sig_map= on stderr");

    // Create a prompt asking about the plural.
    let (pcode, pout, perr) = run_cmd(
        bin,
        &["prompt", "--root", root1.to_str().unwrap(), "bananas"],
    );
    assert_eq!(
        pcode,
        0,
        "prompt failed: stderr={}",
        String::from_utf8_lossy(&perr)
    );
    let prompt_hex = parse_first_hex(&String::from_utf8_lossy(&pout).replace("\r\n", "\n")).expect("prompt hash");

    // Answer without expansion: should not find evidence for plural.
    let out_no = base.join("answer_no.txt");
    let (acode_no, _aout_no, aerr_no) = run_cmd(
        bin,
        &[
            "answer",
            "--root",
            root1.to_str().unwrap(),
            "--prompt",
            &prompt_hex,
            "--snapshot",
            &idx_snap_hex,
            "--sig-map",
            &sig_map_hex,
            "--k",
            "8",
            "--out-file",
            out_no.to_str().unwrap(),
        ],
    );
    assert_eq!(
        acode_no,
        0,
        "answer (no expansion) failed: stderr={}",
        String::from_utf8_lossy(&aerr_no)
    );
    let s_no = std::fs::read_to_string(&out_no).unwrap();
    assert!(s_no.contains("Answer v1"));
    assert!(!s_no.contains("[E0]"), "expected no evidence without expansion");

    // Answer with expansion: should find evidence via variant "banana" (allowed by lexicon).
    let out_yes = base.join("answer_yes.txt");
    let (acode_yes, _aout_yes, aerr_yes) = run_cmd(
        bin,
        &[
            "answer",
            "--root",
            root1.to_str().unwrap(),
            "--prompt",
            &prompt_hex,
            "--snapshot",
            &idx_snap_hex,
            "--sig-map",
            &sig_map_hex,
            "--k",
            "8",
            "--expand",
            "--lexicon-snapshot",
            &lex_snap_hex_1,
            "--out-file",
            out_yes.to_str().unwrap(),
        ],
    );
    assert_eq!(
        acode_yes,
        0,
        "answer (with expansion) failed: stderr={}",
        String::from_utf8_lossy(&aerr_yes)
    );
    let s_yes = std::fs::read_to_string(&out_yes).unwrap();
    assert!(s_yes.contains("Answer v1"));
    assert!(s_yes.contains("[E0]"), "expected evidence with expansion");
}
