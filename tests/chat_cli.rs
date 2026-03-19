// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use fsa_lm::artifact::FsArtifactStore;
use fsa_lm::frame::{derive_id64, Id64};
use fsa_lm::graph_relevance::{
    GraphNodeKindV1, GraphRelevanceEdgeV1, GraphRelevanceRowV1, GraphRelevanceV1,
    GRAPH_RELEVANCE_V1_VERSION, GREDGE_FLAG_SYMMETRIC, GR_FLAG_HAS_TERM_ROWS,
};
use fsa_lm::graph_relevance_artifact::put_graph_relevance_v1;
use fsa_lm::hash::blake3_hash;
use fsa_lm::markov_hints::MarkovChoiceKindV1;
use fsa_lm::markov_model::{
    MarkovModelV1, MarkovNextV1, MarkovStateV1, MarkovTokenV1, MARKOV_MODEL_V1_VERSION,
};
use fsa_lm::markov_model_artifact::put_markov_model_v1;
use fsa_lm::pragmatics_frame::{PragmaticsFrameV1, RhetoricModeV1, PRAGMATICS_FRAME_V1_VERSION};
use fsa_lm::pragmatics_frame_store::put_pragmatics_frame_v1;
use fsa_lm::tokenizer::{term_id_from_token, TokenizerCfg};

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

fn write_wiki_tsv_ranked(path: &Path) {
    let lines = concat!(
        "banana_top\tbanana banana banana ripe fruit\n",
        "banana_low\tbanana fruit\n",
        "carrot\tcarrot vegetable\n",
    );
    std::fs::write(path, lines.as_bytes()).unwrap();
}

fn write_wiktionary_xml(path: &Path) {
    let xml = r#"<mediawiki>
<page>
  <title>banana</title>
  <ns>0</ns>
  <revision>
    <text xml:space="preserve">==English==
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

fn put_supportive_ask_pragmatics(store: &FsArtifactStore) -> fsa_lm::hash::Hash32 {
    let pf = PragmaticsFrameV1 {
        version: PRAGMATICS_FRAME_V1_VERSION,
        source_id: Id64(1),
        msg_ix: 0,
        byte_len: 12,
        ascii_only: 1,
        temperature: 0,
        valence: 0,
        arousal: 0,
        politeness: 800,
        formality: 200,
        directness: 300,
        empathy_need: 800,
        mode: RhetoricModeV1::Ask,
        flags: 0,
        exclamations: 0,
        questions: 1,
        ellipses: 0,
        caps_words: 0,
        repeat_punct_runs: 0,
        quotes: 0,
        emphasis_score: 0,
        hedge_count: 0,
        intensifier_count: 0,
        profanity_count: 0,
        apology_count: 0,
        gratitude_count: 0,
        insult_count: 0,
    };
    put_pragmatics_frame_v1(store, &pf).unwrap()
}

fn put_supportive_preface_markov_model(store: &FsArtifactStore) -> fsa_lm::hash::Hash32 {
    let cid0 = derive_id64(b"markov_choice_v1", b"preface:supportive:0");
    let cid1 = derive_id64(b"markov_choice_v1", b"preface:supportive:1");
    let s0 = MarkovStateV1 {
        context: Vec::new(),
        escape_count: 0,
        next: vec![
            MarkovNextV1 {
                token: MarkovTokenV1::new(MarkovChoiceKindV1::Opener, cid1),
                count: 20,
            },
            MarkovNextV1 {
                token: MarkovTokenV1::new(MarkovChoiceKindV1::Opener, cid0),
                count: 10,
            },
        ],
    };
    let model = MarkovModelV1 {
        version: MARKOV_MODEL_V1_VERSION,
        order_n_max: 3,
        max_next_per_state: 8,
        total_transitions: 30,
        corpus_hash: [0u8; 32],
        states: vec![s0],
    };
    assert!(model.validate().is_ok());
    put_markov_model_v1(store, &model).unwrap()
}

fn supportive_preface_v1() -> &'static str {
    "Happy to help. Based on the evidence, here is the clearest answer:"
}

fn supportive_preface_v0() -> &'static str {
    "I can help with that. Based on the evidence, here is the clearest answer:"
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

#[test]
fn chat_uses_workspace_markov_model_when_flag_is_omitted() {
    let base = tmp_dir("chat_uses_workspace_markov_model_when_flag_is_omitted");
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

    let store = FsArtifactStore::new(&root).unwrap();
    let prag_hash = put_supportive_ask_pragmatics(&store);
    let markov_hash = put_supportive_preface_markov_model(&store);

    let ws = format!(
        "merged_snapshot={}\nmerged_sig_map={}\nmarkov_model={}\n",
        idx_snap_hex,
        sig_map_hex,
        fsa_lm::hash::hex32(&markov_hash),
    );
    std::fs::write(root.join("workspace_v1.txt"), ws.as_bytes()).unwrap();

    let mut child = Command::new(bin)
        .args([
            "chat",
            "--root",
            root.to_str().unwrap(),
            "--presentation",
            "operator",
            "--pragmatics",
            &fsa_lm::hash::hex32(&prag_hash),
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
    assert_eq!(
        out.status.code().unwrap_or(-1),
        0,
        "stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout_s = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout_s.contains(supportive_preface_v1()),
        "stdout={}",
        stdout_s
    );
    assert!(
        !stdout_s.contains(supportive_preface_v0()),
        "stdout={}",
        stdout_s
    );
}

fn evidence_lines_from_answer_text(s: &str) -> Vec<String> {
    s.lines()
        .filter(|line| line.starts_with("[E"))
        .map(|line| line.to_string())
        .collect()
}

fn write_workspace(root: &Path, merged_snapshot: &str, merged_sig_map: &str) {
    write_workspace_with_defaults(
        root,
        merged_snapshot,
        merged_sig_map,
        None,
        None,
        None,
        None,
    );
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
    let mut s = format!(
        "merged_snapshot={}
merged_sig_map={}
",
        merged_snapshot, merged_sig_map
    );
    if let Some(h) = lexicon_snapshot {
        s.push_str(&format!(
            "lexicon_snapshot={}
",
            h
        ));
    }
    if let Some(v) = default_k {
        s.push_str(&format!(
            "default_k={}
",
            v
        ));
    }
    if let Some(v) = default_expand {
        s.push_str(&format!(
            "default_expand={}
",
            if v { 1 } else { 0 }
        ));
    }
    if let Some(v) = default_meta {
        s.push_str(&format!(
            "default_meta={}
",
            if v { 1 } else { 0 }
        ));
    }
    std::fs::write(root.join("workspace_v1.txt"), s.as_bytes()).unwrap();
}

#[test]
fn chat_runs_answer_pipeline_using_workspace_defaults() {
    let base = tmp_dir("chat_runs_answer_pipeline_using_workspace_defaults");
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

    let mut child = Command::new(bin)
        .args(["chat", "--root", root.to_str().unwrap(), "--k", "8"])
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
    assert_eq!(
        out.status.code().unwrap_or(-1),
        0,
        "stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    let stdout_s = String::from_utf8_lossy(&out.stdout);
    assert!(!stdout_s.contains("Answer v1"), "stdout={}", stdout_s);
    assert!(!stdout_s.contains("query_id="), "stdout={}", stdout_s);
    assert!(!stdout_s.contains("routing_trace "), "stdout={}", stdout_s);
    assert!(
        stdout_s.contains("[E0]"),
        "expected evidence output; stdout={}",
        stdout_s
    );
}

#[test]
fn chat_uses_workspace_default_k_when_flag_is_omitted() {
    let base = tmp_dir("chat_uses_workspace_default_k_when_flag_is_omitted");
    let root = base.join("root");
    std::fs::create_dir_all(&root).unwrap();

    let wiki_tsv = base.join("wiki.tsv");
    std::fs::write(
        &wiki_tsv,
        b"banana	banana banana is a fruit
plantain	plantain is related to banana
",
    )
    .unwrap();

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
            "2",
        ],
    );
    assert_eq!(wcode, 0, "stderr={}", String::from_utf8_lossy(&werr));

    let (bcode, bout, berr) = run_cmd(bin, &["build-index", "--root", root.to_str().unwrap()]);
    assert_eq!(bcode, 0, "stderr={}", String::from_utf8_lossy(&berr));
    let bout_s = String::from_utf8_lossy(&bout).replace("\r\n", "\n");
    let berr_s = String::from_utf8_lossy(&berr).replace("\r\n", "\n");
    let idx_snap_hex = parse_first_hex(&bout_s).expect("snapshot hash on stdout");
    let sig_map_hex = parse_stderr_kv(&berr_s, "index_sig_map").expect("index_sig_map= on stderr");

    write_workspace_with_defaults(
        &root,
        &idx_snap_hex,
        &sig_map_hex,
        None,
        Some(1),
        None,
        None,
    );

    let mut child = Command::new(bin)
        .args(["chat", "--root", root.to_str().unwrap()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    {
        let stdin = child.stdin.as_mut().unwrap();
        stdin.write_all(b"banana plantain\n/exit\n").unwrap();
    }

    let out = child.wait_with_output().unwrap();
    assert_eq!(
        out.status.code().unwrap_or(-1),
        0,
        "stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    let stdout_s = String::from_utf8_lossy(&out.stdout);
    assert!(stdout_s.contains("[E0]"), "stdout={}", stdout_s);
    assert!(
        !stdout_s.contains("[E1]"),
        "expected workspace default_k=1 to cap evidence items; stdout={}",
        stdout_s
    );
}

#[test]
fn chat_operator_presentation_preserves_diagnostics() {
    let base = tmp_dir("chat_operator_presentation_preserves_diagnostics");
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

    let mut child = Command::new(bin)
        .args([
            "chat",
            "--root",
            root.to_str().unwrap(),
            "--k",
            "8",
            "--presentation",
            "operator",
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
    assert_eq!(
        out.status.code().unwrap_or(-1),
        0,
        "stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    let stdout_s = String::from_utf8_lossy(&out.stdout);
    assert!(stdout_s.contains("Answer v1"), "stdout={}", stdout_s);
    assert!(stdout_s.contains("query_id="), "stdout={}", stdout_s);
    assert!(stdout_s.contains("routing_trace "), "stdout={}", stdout_s);
    assert!(
        stdout_s.contains("[E0]"),
        "expected evidence output; stdout={}",
        stdout_s
    );
}

#[test]
fn chat_uses_workspace_default_expand_when_flag_is_omitted() {
    let base = tmp_dir("chat_uses_workspace_default_expand_when_flag_is_omitted");
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
    let lex_snap_hex = lxout_s
        .lines()
        .find_map(|line| {
            line.trim()
                .strip_prefix("lexicon_snapshot=")
                .map(|v| v.trim().to_string())
        })
        .expect("lexicon_snapshot line");

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

    write_workspace_with_defaults(
        &root,
        &idx_snap_hex,
        &sig_map_hex,
        Some(&lex_snap_hex),
        None,
        Some(true),
        None,
    );

    let mut child = Command::new(bin)
        .args(["chat", "--root", root.to_str().unwrap(), "--k", "8"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    {
        let stdin = child.stdin.as_mut().unwrap();
        stdin.write_all(b"bananas\n/exit\n").unwrap();
    }

    let out = child.wait_with_output().unwrap();
    assert_eq!(
        out.status.code().unwrap_or(-1),
        0,
        "stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    let stdout_s = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout_s.contains("[E0]"),
        "expected workspace default_expand=1 to enable expansion; stdout={}",
        stdout_s
    );
}

#[test]
fn chat_uses_workspace_default_meta_when_flag_is_omitted() {
    let base = tmp_dir("chat_uses_workspace_default_meta_when_flag_is_omitted");
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

    write_workspace_with_defaults(
        &root,
        &idx_snap_hex,
        &sig_map_hex,
        None,
        None,
        None,
        Some(true),
    );

    let mut child_default = Command::new(bin)
        .args([
            "chat",
            "--root",
            root.to_str().unwrap(),
            "--presentation",
            "operator",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    {
        let stdin = child_default.stdin.as_mut().unwrap();
        stdin.write_all(b"banana\n/exit\n").unwrap();
    }
    let out_default = child_default.wait_with_output().unwrap();
    assert_eq!(
        out_default.status.code().unwrap_or(-1),
        0,
        "stderr={}",
        String::from_utf8_lossy(&out_default.stderr)
    );
    let stdout_default = String::from_utf8_lossy(&out_default.stdout);
    let qid_default = parse_query_id_from_answer_text(&stdout_default)
        .expect("query_id in default-meta chat answer");

    let mut child_explicit = Command::new(bin)
        .args([
            "chat",
            "--root",
            root.to_str().unwrap(),
            "--meta",
            "--presentation",
            "operator",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    {
        let stdin = child_explicit.stdin.as_mut().unwrap();
        stdin.write_all(b"banana\n/exit\n").unwrap();
    }
    let out_explicit = child_explicit.wait_with_output().unwrap();
    assert_eq!(
        out_explicit.status.code().unwrap_or(-1),
        0,
        "stderr={}",
        String::from_utf8_lossy(&out_explicit.stderr)
    );
    let stdout_explicit = String::from_utf8_lossy(&out_explicit.stdout);
    let qid_explicit = parse_query_id_from_answer_text(&stdout_explicit)
        .expect("query_id in explicit-meta chat answer");

    assert_eq!(
        qid_default, qid_explicit,
        "expected workspace default_meta=1 to match explicit --meta query_id"
    );
    assert!(stdout_default.contains("[E0]"), "stdout={}", stdout_default);
    assert!(
        stdout_explicit.contains("[E0]"),
        "stdout={}",
        stdout_explicit
    );
}

#[test]
fn chat_uses_workspace_graph_relevance_when_flag_is_omitted() {
    let base = tmp_dir("chat_uses_workspace_graph_relevance_when_flag_is_omitted");
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
        build_id: blake3_hash(b"workspace-graph-chat"),
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

    write_workspace_with_defaults(
        &root,
        &idx_snap_hex,
        &sig_map_hex,
        None,
        None,
        Some(true),
        None,
    );
    let mut ws = std::fs::read_to_string(root.join("workspace_v1.txt")).unwrap();
    ws.push_str(&format!(
        "graph_relevance={}\n",
        fsa_lm::hash::hex32(&graph_hash)
    ));
    std::fs::write(root.join("workspace_v1.txt"), ws.as_bytes()).unwrap();

    let mut child = Command::new(bin)
        .args([
            "chat",
            "--root",
            root.to_str().unwrap(),
            "--presentation",
            "operator",
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
    assert_eq!(
        out.status.code().unwrap_or(-1),
        0,
        "stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    let stdout_s = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout_s.contains("graph_trace seeds=1 candidates=1 reasons=banana:"),
        "stdout={}",
        stdout_s
    );
    assert!(
        stdout_s.contains("[E1]"),
        "expected graph expansion to add a second evidence item; stdout={}",
        stdout_s
    );
}

#[test]
fn chat_workspace_graph_relevance_keeps_lexical_evidence_first() {
    let base = tmp_dir("chat_workspace_graph_relevance_keeps_lexical_evidence_first");
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
        build_id: blake3_hash(b"workspace-graph-chat-lexical-first"),
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

    write_workspace_with_defaults(&root, &idx_snap_hex, &sig_map_hex, None, None, None, None);

    let mut child_base = Command::new(bin)
        .args([
            "chat",
            "--root",
            root.to_str().unwrap(),
            "--presentation",
            "operator",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    {
        let stdin = child_base.stdin.as_mut().unwrap();
        stdin.write_all(b"banana\n/exit\n").unwrap();
    }
    let out_base = child_base.wait_with_output().unwrap();
    assert_eq!(
        out_base.status.code().unwrap_or(-1),
        0,
        "stderr={}",
        String::from_utf8_lossy(&out_base.stderr)
    );
    let stdout_base = String::from_utf8_lossy(&out_base.stdout);
    let base_evidence = evidence_lines_from_answer_text(&stdout_base);
    assert!(!base_evidence.is_empty(), "stdout={}", stdout_base);

    let mut ws = std::fs::read_to_string(root.join("workspace_v1.txt")).unwrap();
    ws.push_str("default_expand=1\n");
    ws.push_str(&format!(
        "graph_relevance={}\n",
        fsa_lm::hash::hex32(&graph_hash)
    ));
    std::fs::write(root.join("workspace_v1.txt"), ws.as_bytes()).unwrap();

    let mut child_graph = Command::new(bin)
        .args([
            "chat",
            "--root",
            root.to_str().unwrap(),
            "--presentation",
            "operator",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    {
        let stdin = child_graph.stdin.as_mut().unwrap();
        stdin.write_all(b"banana\n/exit\n").unwrap();
    }
    let out_graph = child_graph.wait_with_output().unwrap();
    assert_eq!(
        out_graph.status.code().unwrap_or(-1),
        0,
        "stderr={}",
        String::from_utf8_lossy(&out_graph.stderr)
    );
    let stdout_graph = String::from_utf8_lossy(&out_graph.stdout);
    let graph_evidence = evidence_lines_from_answer_text(&stdout_graph);
    assert!(!graph_evidence.is_empty(), "stdout={}", stdout_graph);
    assert!(
        stdout_graph.contains("graph_trace seeds=1 candidates=1 reasons=banana:"),
        "stdout={}",
        stdout_graph
    );
    assert_eq!(
        base_evidence[0], graph_evidence[0],
        "expected lexical top evidence to stay first when workspace graph enrichment is active"
    );
}

#[test]
fn chat_missing_workspace_graph_relevance_falls_back_cleanly() {
    let base = tmp_dir("chat_missing_workspace_graph_relevance_falls_back_cleanly");
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

    write_workspace_with_defaults(&root, &idx_snap_hex, &sig_map_hex, None, None, None, None);
    let mut ws = std::fs::read_to_string(root.join("workspace_v1.txt")).unwrap();
    ws.push_str(&format!(
        "graph_relevance={}\n",
        fsa_lm::hash::hex32(&fsa_lm::hash::blake3_hash(
            b"missing-workspace-graph-relevance"
        ))
    ));
    std::fs::write(root.join("workspace_v1.txt"), ws.as_bytes()).unwrap();

    let mut child = Command::new(bin)
        .args([
            "chat",
            "--root",
            root.to_str().unwrap(),
            "--presentation",
            "operator",
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
    let stderr_s = String::from_utf8_lossy(&out.stderr).replace("\r\n", "\n");
    assert_eq!(out.status.code().unwrap_or(-1), 0, "stderr={}", stderr_s);
    assert!(
        !stderr_s.contains("graph-relevance load failed"),
        "stderr={}",
        stderr_s
    );
    assert!(
        !stderr_s.contains("missing --lexicon-snapshot or --graph-relevance"),
        "stderr={}",
        stderr_s
    );

    let stdout_s = String::from_utf8_lossy(&out.stdout).replace("\r\n", "\n");
    assert!(stdout_s.contains("Answer v1"), "stdout={}", stdout_s);
    assert!(stdout_s.contains("[E0]"), "stdout={}", stdout_s);
    assert!(!stdout_s.contains("graph_trace"), "stdout={}", stdout_s);
}
