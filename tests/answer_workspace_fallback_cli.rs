// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use std::path::{Path, PathBuf};
use std::process::Command;

use fsa_lm::artifact::FsArtifactStore;
use fsa_lm::graph_relevance::{
    GraphNodeKindV1, GraphRelevanceEdgeV1, GraphRelevanceRowV1, GraphRelevanceV1,
    GRAPH_RELEVANCE_V1_VERSION, GREDGE_FLAG_SYMMETRIC, GR_FLAG_HAS_TERM_ROWS,
};
use fsa_lm::graph_relevance_artifact::put_graph_relevance_v1;
use fsa_lm::hash::blake3_hash;
use fsa_lm::tokenizer::{term_id_from_token, TokenizerCfg};

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
* {{m|en|dessert banana}}
====Pronunciation====
* {{IPA|en|/bəˈnænə/}}
</text>
  </revision>
</page>
</mediawiki>"#;
    std::fs::write(path, xml.as_bytes()).unwrap();
}

fn write_wiki_tsv(path: &Path) {
    let line = "banana\tbanana banana is a fruit\n";
    std::fs::write(path, line.as_bytes()).unwrap();
}

fn write_wiki_tsv_ranked(path: &Path) {
    let lines = concat!(
        "banana_top\tbanana banana banana ripe fruit\n",
        "banana_low\tbanana fruit\n",
        "carrot\tcarrot vegetable\n",
    );
    std::fs::write(path, lines.as_bytes()).unwrap();
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

fn parse_query_id_from_answer_text(s: &str) -> Option<String> {
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("query_id=") {
            let hex = match rest.split_whitespace().next() {
                Some(v) => v,
                None => rest,
            };
            if is_hex64(hex) {
                return Some(hex.to_string());
            }
        }
    }
    None
}

fn write_workspace(root: &Path, merged_snapshot: &str, merged_sig_map: &str, lexicon_snapshot: Option<&str>) {
    write_workspace_with_defaults(root, merged_snapshot, merged_sig_map, lexicon_snapshot, None, None, None);
}

fn write_workspace_with_default_k(
    root: &Path,
    merged_snapshot: &str,
    merged_sig_map: &str,
    lexicon_snapshot: Option<&str>,
    default_k: Option<u32>,
) {
    write_workspace_with_defaults(root, merged_snapshot, merged_sig_map, lexicon_snapshot, default_k, None, None);
}

fn write_workspace_with_default_expand(
    root: &Path,
    merged_snapshot: &str,
    merged_sig_map: &str,
    lexicon_snapshot: Option<&str>,
    default_expand: Option<bool>,
) {
    write_workspace_with_defaults(root, merged_snapshot, merged_sig_map, lexicon_snapshot, None, default_expand, None);
}

fn write_workspace_with_default_meta(
    root: &Path,
    merged_snapshot: &str,
    merged_sig_map: &str,
    lexicon_snapshot: Option<&str>,
    default_meta: Option<bool>,
) {
    write_workspace_with_defaults(root, merged_snapshot, merged_sig_map, lexicon_snapshot, None, None, default_meta);
}

fn write_workspace_with_defaults(
    root: &Path,
    merged_snapshot: &str,
    merged_sig_map: &str,
    lexicon_snapshot: Option<&str>,
    default_k: Option<u32>,
    default_expand: Option<bool>,
    default_meta: Option<bool>,
) {
    let mut s = String::new();
    s.push_str(&format!("merged_snapshot={}\n", merged_snapshot));
    s.push_str(&format!("merged_sig_map={}\n", merged_sig_map));
    if let Some(h) = lexicon_snapshot {
        s.push_str(&format!("lexicon_snapshot={}\n", h));
    }
    if let Some(v) = default_k {
        s.push_str(&format!("default_k={}\n", v));
    }
    if let Some(v) = default_expand {
        s.push_str(&format!("default_expand={}\n", if v { 1 } else { 0 }));
    }
    if let Some(v) = default_meta {
        s.push_str(&format!("default_meta={}\n", if v { 1 } else { 0 }));
    }
    std::fs::write(root.join("workspace_v1.txt"), s.as_bytes()).unwrap();
}

#[test]
fn answer_uses_workspace_defaults_for_snapshot_and_sig_map() {
    let base = tmp_dir("answer_uses_workspace_defaults_for_snapshot_and_sig_map");
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

    let (pcode, pout, perr) = run_cmd(bin, &["prompt", "--root", root.to_str().unwrap(), "banana"]);
    assert_eq!(pcode, 0, "stderr={}", String::from_utf8_lossy(&perr));
    let prompt_hex = parse_first_hex(&String::from_utf8_lossy(&pout).replace("\r\n", "\n")).expect("prompt hash");

    let out_path = base.join("answer_ws.txt");
    let (acode, _aout, aerr) = run_cmd(
        bin,
        &[
            "answer",
            "--root",
            root.to_str().unwrap(),
            "--prompt",
            &prompt_hex,
            "--k",
            "8",
            "--out-file",
            out_path.to_str().unwrap(),
        ],
    );
    assert_eq!(acode, 0, "stderr={}", String::from_utf8_lossy(&aerr));
    let s = std::fs::read_to_string(&out_path).unwrap();
    assert!(!s.contains("Answer v1"));
    assert!(!s.contains("query_id="));
    assert!(s.contains("[E0]"), "expected evidence using workspace defaults");
}

#[test]
fn answer_expand_uses_workspace_lexicon_snapshot_when_omitted() {
    let base = tmp_dir("answer_expand_uses_workspace_lexicon_snapshot_when_omitted");
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
    let lex_snap_hex = parse_hash_line(&lxout_s, "lexicon_snapshot").expect("lexicon_snapshot line");

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

    let (pcode, pout, perr) = run_cmd(bin, &["prompt", "--root", root.to_str().unwrap(), "bananas"]);
    assert_eq!(pcode, 0, "stderr={}", String::from_utf8_lossy(&perr));
    let prompt_hex = parse_first_hex(&String::from_utf8_lossy(&pout).replace("\r\n", "\n")).expect("prompt hash");

    let out_path = base.join("answer_expand_ws.txt");
    let (acode, _aout, aerr) = run_cmd(
        bin,
        &[
            "answer",
            "--root",
            root.to_str().unwrap(),
            "--prompt",
            &prompt_hex,
            "--k",
            "8",
            "--expand",
            "--out-file",
            out_path.to_str().unwrap(),
        ],
    );
    assert_eq!(acode, 0, "stderr={}", String::from_utf8_lossy(&aerr));
    let s = std::fs::read_to_string(&out_path).unwrap();
    assert!(!s.contains("Answer v1"));
    assert!(!s.contains("query_id="));
    assert!(s.contains("[E0]"), "expected evidence with expansion using workspace lexicon_snapshot");
}

#[test]
fn answer_without_snapshot_and_without_workspace_fails() {
    let base = tmp_dir("answer_without_snapshot_and_without_workspace_fails");
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

    let (pcode, pout, perr) = run_cmd(bin, &["prompt", "--root", root.to_str().unwrap(), "banana"]);
    assert_eq!(pcode, 0, "stderr={}", String::from_utf8_lossy(&perr));
    let prompt_hex = parse_first_hex(&String::from_utf8_lossy(&pout).replace("\r\n", "\n")).expect("prompt hash");

    let out_path = base.join("answer_fail.txt");
    let (acode, _aout, aerr) = run_cmd(
        bin,
        &[
            "answer",
            "--root",
            root.to_str().unwrap(),
            "--prompt",
            &prompt_hex,
            "--k",
            "8",
            "--out-file",
            out_path.to_str().unwrap(),
        ],
    );
    assert_eq!(acode, 1);
    let err_s = String::from_utf8_lossy(&aerr);
    assert!(err_s.contains("workspace"), "stderr={}", err_s);
}


#[test]
fn answer_uses_workspace_default_k_when_flag_is_omitted() {
    let base = tmp_dir("answer_uses_workspace_default_k_when_flag_is_omitted");
    let root = base.join("root");
    std::fs::create_dir_all(&root).unwrap();

    let wiki_tsv = base.join("wiki_ranked.tsv");
    write_wiki_tsv_ranked(&wiki_tsv);

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
            "3",
        ],
    );
    assert_eq!(wcode, 0, "stderr={}", String::from_utf8_lossy(&werr));

    let (bcode, bout, berr) = run_cmd(bin, &["build-index", "--root", root.to_str().unwrap()]);
    assert_eq!(bcode, 0, "stderr={}", String::from_utf8_lossy(&berr));
    let bout_s = String::from_utf8_lossy(&bout).replace("\r\n", "\n");
    let berr_s = String::from_utf8_lossy(&berr).replace("\r\n", "\n");
    let idx_snap_hex = parse_first_hex(&bout_s).expect("snapshot hash on stdout");
    let sig_map_hex = parse_stderr_kv(&berr_s, "index_sig_map").expect("index_sig_map= on stderr");

    write_workspace_with_default_k(&root, &idx_snap_hex, &sig_map_hex, None, Some(1));

    let (pcode, pout, perr) = run_cmd(bin, &["prompt", "--root", root.to_str().unwrap(), "banana"]);
    assert_eq!(pcode, 0, "stderr={}", String::from_utf8_lossy(&perr));
    let prompt_hex = parse_first_hex(&String::from_utf8_lossy(&pout).replace("\r\n", "\n")).expect("prompt hash");

    let out_path = base.join("answer_default_k.txt");
    let (acode, _aout, aerr) = run_cmd(
        bin,
        &[
            "answer",
            "--root",
            root.to_str().unwrap(),
            "--prompt",
            &prompt_hex,
            "--out-file",
            out_path.to_str().unwrap(),
        ],
    );
    assert_eq!(acode, 0, "stderr={}", String::from_utf8_lossy(&aerr));
    let s = std::fs::read_to_string(&out_path).unwrap();
    assert!(s.contains("[E0]"));
    assert!(!s.contains("[E1]"), "expected workspace default_k=1 to cap evidence items");
}


#[test]
fn answer_uses_workspace_default_expand_when_flag_is_omitted() {
    let base = tmp_dir("answer_uses_workspace_default_expand_when_flag_is_omitted");
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
    let lex_snap_hex = parse_hash_line(&lxout_s, "lexicon_snapshot").expect("lexicon_snapshot line");

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

    write_workspace_with_default_expand(&root, &idx_snap_hex, &sig_map_hex, Some(&lex_snap_hex), Some(true));

    let (pcode, pout, perr) = run_cmd(bin, &["prompt", "--root", root.to_str().unwrap(), "bananas"]);
    assert_eq!(pcode, 0, "stderr={}", String::from_utf8_lossy(&perr));
    let prompt_hex = parse_first_hex(&String::from_utf8_lossy(&pout).replace("\r\n", "\n")).expect("prompt hash");

    let out_path = base.join("answer_default_expand.txt");
    let (acode, _aout, aerr) = run_cmd(
        bin,
        &[
            "answer",
            "--root",
            root.to_str().unwrap(),
            "--prompt",
            &prompt_hex,
            "--k",
            "8",
            "--out-file",
            out_path.to_str().unwrap(),
        ],
    );
    assert_eq!(acode, 0, "stderr={}", String::from_utf8_lossy(&aerr));
    let s = std::fs::read_to_string(&out_path).unwrap();
    assert!(s.contains("[E0]"), "expected workspace default_expand=1 to enable expansion");
}


#[test]
fn answer_uses_workspace_default_meta_when_flag_is_omitted() {
    let base = tmp_dir("answer_uses_workspace_default_meta_when_flag_is_omitted");
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

    write_workspace_with_default_meta(&root, &idx_snap_hex, &sig_map_hex, None, Some(true));

    let (pcode, pout, perr) = run_cmd(bin, &["prompt", "--root", root.to_str().unwrap(), "banana"]);
    assert_eq!(pcode, 0, "stderr={}", String::from_utf8_lossy(&perr));
    let prompt_hex = parse_first_hex(&String::from_utf8_lossy(&pout).replace("\r\n", "\n")).expect("prompt hash");

    let out_default = base.join("answer_default_meta_operator.txt");
    let (dcode, _dout, derr) = run_cmd(
        bin,
        &[
            "answer",
            "--root",
            root.to_str().unwrap(),
            "--prompt",
            &prompt_hex,
            "--presentation",
            "operator",
            "--out-file",
            out_default.to_str().unwrap(),
        ],
    );
    assert_eq!(dcode, 0, "stderr={}", String::from_utf8_lossy(&derr));
    let s_default = std::fs::read_to_string(&out_default).unwrap();
    let qid_default = parse_query_id_from_answer_text(&s_default).expect("query_id in default-meta answer");

    let out_explicit = base.join("answer_explicit_meta_operator.txt");
    let (ecode, _eout, eerr) = run_cmd(
        bin,
        &[
            "answer",
            "--root",
            root.to_str().unwrap(),
            "--prompt",
            &prompt_hex,
            "--meta",
            "--presentation",
            "operator",
            "--out-file",
            out_explicit.to_str().unwrap(),
        ],
    );
    assert_eq!(ecode, 0, "stderr={}", String::from_utf8_lossy(&eerr));
    let s_explicit = std::fs::read_to_string(&out_explicit).unwrap();
    let qid_explicit = parse_query_id_from_answer_text(&s_explicit).expect("query_id in explicit-meta answer");

    assert_eq!(qid_default, qid_explicit, "expected workspace default_meta=1 to match explicit --meta query_id");
    assert!(s_default.contains("[E0]"));
    assert!(s_explicit.contains("[E0]"));
}

fn write_workspace_with_advisories(
    root: &Path,
    merged_snapshot: &str,
    merged_sig_map: &str,
    lexicon_snapshot: Option<&str>,
    default_k: Option<u32>,
    default_expand: Option<bool>,
    default_meta: Option<bool>,
    markov_model: Option<&str>,
    exemplar_memory: Option<&str>,
    graph_relevance: Option<&str>,
) {
    let mut s = String::new();
    s.push_str(&format!("merged_snapshot={}\n", merged_snapshot));
    s.push_str(&format!("merged_sig_map={}\n", merged_sig_map));
    if let Some(h) = lexicon_snapshot {
        s.push_str(&format!("lexicon_snapshot={}\n", h));
    }
    if let Some(v) = default_k {
        s.push_str(&format!("default_k={}\n", v));
    }
    if let Some(v) = default_expand {
        s.push_str(&format!("default_expand={}\n", if v { 1 } else { 0 }));
    }
    if let Some(v) = default_meta {
        s.push_str(&format!("default_meta={}\n", if v { 1 } else { 0 }));
    }
    if let Some(h) = markov_model {
        s.push_str(&format!("markov_model={}\n", h));
    }
    if let Some(h) = exemplar_memory {
        s.push_str(&format!("exemplar_memory={}\n", h));
    }
    if let Some(h) = graph_relevance {
        s.push_str(&format!("graph_relevance={}\n", h));
    }
    std::fs::write(root.join("workspace_v1.txt"), s.as_bytes()).unwrap();
}

#[test]
fn answer_uses_workspace_graph_relevance_when_flag_is_omitted() {
    let base = tmp_dir("answer_uses_workspace_graph_relevance_when_flag_is_omitted");
    let root = base.join("root");
    std::fs::create_dir_all(&root).unwrap();

    let wiki_tsv = base.join("wiki.tsv");
    write_wiki_tsv_ranked(&wiki_tsv);

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
            "3",
        ],
    );
    assert_eq!(wcode, 0, "stderr={}", String::from_utf8_lossy(&werr));

    let (bcode, bout, berr) = run_cmd(bin, &["build-index", "--root", root.to_str().unwrap()]);
    assert_eq!(bcode, 0, "stderr={}", String::from_utf8_lossy(&berr));
    let bout_s = String::from_utf8_lossy(&bout).replace("\r\n", "\n");
    let berr_s = String::from_utf8_lossy(&berr).replace("\r\n", "\n");
    let idx_snap_hex = parse_first_hex(&bout_s).expect("snapshot hash on stdout");
    let sig_map_hex = parse_stderr_kv(&berr_s, "index_sig_map").expect("index_sig_map= on stderr");

    let store = FsArtifactStore::new(&root).unwrap();
    let banana = term_id_from_token("banana", TokenizerCfg::default());
    let carrot = term_id_from_token("carrot", TokenizerCfg::default());
    let graph = GraphRelevanceV1 {
        version: GRAPH_RELEVANCE_V1_VERSION,
        build_id: blake3_hash(b"workspace-graph-answer"),
        flags: GR_FLAG_HAS_TERM_ROWS,
        rows: vec![GraphRelevanceRowV1 {
            seed_kind: GraphNodeKindV1::Term,
            seed_id: banana.0,
            edges: vec![GraphRelevanceEdgeV1::new(
                GraphNodeKindV1::Term,
                carrot.0,
                20_000,
                1,
                GREDGE_FLAG_SYMMETRIC,
            )],
        }],
    };
    let graph_hash = put_graph_relevance_v1(&store, &graph).unwrap();
    let graph_hex = fsa_lm::hash::hex32(&graph_hash);

    write_workspace_with_advisories(
        &root,
        &idx_snap_hex,
        &sig_map_hex,
        None,
        None,
        Some(true),
        None,
        None,
        None,
        Some(&graph_hex),
    );

    let (pcode, pout, perr) = run_cmd(bin, &["prompt", "--root", root.to_str().unwrap(), "banana"]);
    assert_eq!(pcode, 0, "stderr={}", String::from_utf8_lossy(&perr));
    let prompt_hex = parse_first_hex(&String::from_utf8_lossy(&pout).replace("\r\n", "\n")).expect("prompt hash");

    let out_path = base.join("answer_graph_ws.txt");
    let (acode, _aout, aerr) = run_cmd(
        bin,
        &[
            "answer",
            "--root",
            root.to_str().unwrap(),
            "--prompt",
            &prompt_hex,
            "--presentation",
            "operator",
            "--out-file",
            out_path.to_str().unwrap(),
        ],
    );
    assert_eq!(acode, 0, "stderr={}", String::from_utf8_lossy(&aerr));
    let s = std::fs::read_to_string(&out_path).unwrap();
    assert!(s.contains("graph_trace seeds=1 candidates=1 reasons=banana:"), "output={}", s);
    assert!(s.contains("[E1]"), "expected graph expansion to add a second evidence item; output={}", s);
}


#[test]
fn answer_missing_workspace_markov_model_falls_back_cleanly() {
    let base = tmp_dir("answer_missing_workspace_markov_model_falls_back_cleanly");
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

    let missing_markov_hex = fsa_lm::hash::hex32(&blake3_hash(b"missing-workspace-markov-model"));
    write_workspace_with_advisories(
        &root,
        &idx_snap_hex,
        &sig_map_hex,
        None,
        None,
        None,
        None,
        Some(&missing_markov_hex),
        None,
        None,
    );

    let (pcode, pout, perr) = run_cmd(bin, &["prompt", "--root", root.to_str().unwrap(), "banana"]);
    assert_eq!(pcode, 0, "stderr={}", String::from_utf8_lossy(&perr));
    let prompt_hex = parse_first_hex(&String::from_utf8_lossy(&pout).replace("\r\n", "\n")).expect("prompt hash");

    let out_path = base.join("answer_missing_markov_ws.txt");
    let (acode, _aout, aerr) = run_cmd(
        bin,
        &[
            "answer",
            "--root",
            root.to_str().unwrap(),
            "--prompt",
            &prompt_hex,
            "--presentation",
            "operator",
            "--out-file",
            out_path.to_str().unwrap(),
        ],
    );
    let aerr_s = String::from_utf8_lossy(&aerr).replace("\r\n", "\n");
    assert_eq!(acode, 0, "stderr={}", aerr_s);
    assert!(!aerr_s.contains("missing markov model"), "stderr={}", aerr_s);
    let s = std::fs::read_to_string(&out_path).unwrap();
    assert!(s.contains("Answer v1"), "output={}", s);
    assert!(s.contains("[E0]"), "output={}", s);
}

