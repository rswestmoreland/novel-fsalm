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
</text>
  </revision>
</page>
</mediawiki>"#;
    std::fs::write(path, xml.as_bytes()).unwrap();
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
fn answer_problem_solve_prefers_steps_when_lexicon_pragmatics_present() {
    let base = tmp_dir("answer_problem_solve_prefers_steps_when_lexicon_pragmatics_present");
    let root = base.join("root");
    std::fs::create_dir_all(&root).unwrap();

    let bin = env!("CARGO_BIN_EXE_fsa_lm");

    // Minimal wiki ingest + index so we have evidence items.
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

    // Minimal wiktionary ingest to get a lexicon snapshot for lexicon-assisted pragmatics.
    let xml_path = base.join("wiktionary.xml");
    write_wiktionary_xml(&xml_path);

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
    assert_eq!(lxcode, 0, "stderr={}", lxerr);
    let lex_snap_hex = parse_hash_line(&lxout, "lexicon_snapshot").expect("lexicon_snapshot line");

    write_workspace(&root, &idx_snap_hex, &sig_map_hex, Some(&lex_snap_hex));

    // Create a prompt pack.
    let prompt_text = "Please help me troubleshoot why the banana query returns no results.";
    let (pcode, pout, perr) = run_cmd(
        bin,
        &[
            "prompt",
            "--root",
            root.to_str().unwrap(),
            "--role",
            "user",
            prompt_text,
        ],
    );
    assert_eq!(pcode, 0, "stderr={}", perr);
    let prompt_hash = parse_first_hex(&pout).expect("prompt hash on stdout");

    // Build pragmatics with lexicon assistance.
    let (prcode, prout, prerr) = run_cmd(
        bin,
        &[
            "build-pragmatics",
            "--root",
            root.to_str().unwrap(),
            "--prompt",
            &prompt_hash,
            "--lexicon-snapshot",
            &lex_snap_hex,
        ],
    );
    assert_eq!(prcode, 0, "stderr={}", prerr);
    let prag_hash = parse_first_hex(&prout).expect("pragmatics hash on stdout");

    // Run answer with explicit snapshot/sig-map so it is deterministic and does not depend on workspace reads.
    let out_path = base.join("answer_out.txt");
    let (acode, _aout, aerr) = run_cmd(
        bin,
        &[
            "answer",
            "--root",
            root.to_str().unwrap(),
            "--prompt",
            &prompt_hash,
            "--snapshot",
            &idx_snap_hex,
            "--sig-map",
            &sig_map_hex,
            "--pragmatics",
            &prag_hash,
            "--k",
            "8",
            "--out-file",
            out_path.to_str().unwrap(),
        ],
    );
    assert_eq!(acode, 0, "stderr={}", aerr);

    let s = std::fs::read_to_string(&out_path)
        .unwrap()
        .replace("\r\n", "\n");
    assert!(!s.contains("Answer v1\n"));
    assert!(!s.contains("query_id="));
    assert!(
        s.contains("Steps:") || s.contains("\nSteps\n"),
        "output={}",
        s
    );
    assert!(
        s.contains("Steps:") || s.contains("\nSteps\n"),
        "expected step-oriented output, got: {}",
        s
    );
    assert!(
        s.contains("\nSources\n") || s.contains("\nEvidence\n"),
        "expected sources or evidence section, got: {}",
        s
    );
    assert!(
        s.contains("Quick question:") || !s.contains("Clarifying question:"),
        "expected user-facing clarifier phrasing when clarification is present, got: {}",
        s
    );
}
