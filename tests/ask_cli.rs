// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use std::path::{Path, PathBuf};
use std::process::Command;

use fsa_lm::artifact::FsArtifactStore;
use fsa_lm::exemplar_memory::{
    ExemplarMemoryV1, ExemplarResponseModeV1, ExemplarRowV1, ExemplarStructureKindV1,
    ExemplarToneKindV1, EXEMPLAR_MEMORY_V1_VERSION,
};
use fsa_lm::exemplar_memory_artifact::put_exemplar_memory_v1;
use fsa_lm::frame::{derive_id64, Id64};
use fsa_lm::markov_hints::MarkovChoiceKindV1;
use fsa_lm::markov_model::{
    MarkovModelV1, MarkovNextV1, MarkovStateV1, MarkovTokenV1, MARKOV_MODEL_V1_VERSION,
};
use fsa_lm::markov_model_artifact::put_markov_model_v1;
use fsa_lm::pragmatics_frame::{PragmaticsFrameV1, RhetoricModeV1, PRAGMATICS_FRAME_V1_VERSION};
use fsa_lm::pragmatics_frame_store::put_pragmatics_frame_v1;

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

#[test]
fn ask_uses_workspace_markov_model_when_flag_is_omitted() {
    let base = tmp_dir("ask_uses_workspace_markov_model_when_flag_is_omitted");
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
    let bout_s = String::from_utf8_lossy(&bout).replace(
        "
", "
",
    );
    let berr_s = String::from_utf8_lossy(&berr).replace(
        "
", "
",
    );
    let idx_snap_hex = parse_first_hex(&bout_s).expect("snapshot hash on stdout");
    let sig_map_hex = parse_stderr_kv(&berr_s, "index_sig_map").expect("index_sig_map= on stderr");

    let store = FsArtifactStore::new(&root).unwrap();
    let prag_hash = put_supportive_ask_pragmatics(&store);
    let markov_hash = put_supportive_preface_markov_model(&store);

    let ws = format!(
        "merged_snapshot={}
merged_sig_map={}
markov_model={}
",
        idx_snap_hex,
        sig_map_hex,
        fsa_lm::hash::hex32(&markov_hash),
    );
    std::fs::write(root.join("workspace_v1.txt"), ws.as_bytes()).unwrap();

    let out_path = base.join("ask_workspace_markov_operator.txt");
    let (acode, aout, aerr) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--presentation",
            "operator",
            "--pragmatics",
            &fsa_lm::hash::hex32(&prag_hash),
            "banana",
        ],
    );
    assert_eq!(acode, 0, "stderr={}", String::from_utf8_lossy(&aerr));
    std::fs::write(&out_path, &aout).unwrap();
    let s = String::from_utf8_lossy(&aout);
    assert!(s.contains(supportive_preface_v1()), "stdout={}", s);
    assert!(!s.contains(supportive_preface_v0()), "stdout={}", s);
}

fn evidence_lines_from_answer_text(s: &str) -> Vec<String> {
    s.lines()
        .filter(|line| line.starts_with("[E"))
        .map(|line| line.to_string())
        .collect()
}

fn plan_ref_lines_from_answer_text(s: &str) -> Vec<String> {
    s.lines()
        .filter(|line| line.starts_with("- item=") && line.contains(" refs="))
        .map(|line| {
            let refs_ix = line.find(" refs=").expect("plan refs");
            line[refs_ix..].to_string()
        })
        .collect()
}

fn write_workspace(
    root: &Path,
    merged_snapshot: &str,
    merged_sig_map: &str,
    lexicon_snapshot: Option<&str>,
) {
    write_workspace_with_defaults(
        root,
        merged_snapshot,
        merged_sig_map,
        lexicon_snapshot,
        None,
        None,
        None,
    );
}

fn write_workspace_with_default_k(
    root: &Path,
    merged_snapshot: &str,
    merged_sig_map: &str,
    lexicon_snapshot: Option<&str>,
    default_k: Option<u32>,
) {
    write_workspace_with_defaults(
        root,
        merged_snapshot,
        merged_sig_map,
        lexicon_snapshot,
        default_k,
        None,
        None,
    );
}

fn write_workspace_with_default_expand(
    root: &Path,
    merged_snapshot: &str,
    merged_sig_map: &str,
    lexicon_snapshot: Option<&str>,
    default_expand: Option<bool>,
) {
    write_workspace_with_defaults(
        root,
        merged_snapshot,
        merged_sig_map,
        lexicon_snapshot,
        None,
        default_expand,
        None,
    );
}

fn write_workspace_with_default_meta(
    root: &Path,
    merged_snapshot: &str,
    merged_sig_map: &str,
    lexicon_snapshot: Option<&str>,
    default_meta: Option<bool>,
) {
    write_workspace_with_defaults(
        root,
        merged_snapshot,
        merged_sig_map,
        lexicon_snapshot,
        None,
        None,
        default_meta,
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
    assert!(!s.contains("Answer v1"));
    assert!(!s.contains("query_id="));
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
    assert!(!s.contains("Answer v1"));
    assert!(!s.contains("query_id="));
    assert!(
        s.contains("[E0]"),
        "expected evidence using workspace defaults"
    );
}

#[test]
fn ask_operator_presentation_preserves_diagnostics() {
    let base = tmp_dir("ask_operator_presentation_preserves_diagnostics");
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

    let out_path = base.join("ask_operator_out.txt");
    let (acode, _aout, aerr) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--k",
            "8",
            "--presentation",
            "operator",
            "--out-file",
            out_path.to_str().unwrap(),
            "banana",
        ],
    );
    assert_eq!(acode, 0, "stderr={}", String::from_utf8_lossy(&aerr));
    let s = std::fs::read_to_string(&out_path).unwrap();
    assert!(s.contains("Answer v1"));
    assert!(s.contains("query_id="));
    assert!(s.contains("routing_trace "));
    assert!(
        s.contains("[E0]"),
        "expected evidence using operator presentation"
    );
}

#[test]
fn ask_uses_workspace_default_k_when_flag_is_omitted() {
    let base = tmp_dir("ask_uses_workspace_default_k_when_flag_is_omitted");
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

    let out_path = base.join("ask_default_k.txt");
    let (acode, _aout, aerr) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--out-file",
            out_path.to_str().unwrap(),
            "banana",
        ],
    );
    assert_eq!(acode, 0, "stderr={}", String::from_utf8_lossy(&aerr));
    let s = std::fs::read_to_string(&out_path).unwrap();
    assert!(s.contains("[E0]"));
    assert!(
        !s.contains("[E1]"),
        "expected workspace default_k=1 to cap evidence items"
    );
}

#[test]
fn ask_uses_workspace_default_expand_when_flag_is_omitted() {
    let base = tmp_dir("ask_uses_workspace_default_expand_when_flag_is_omitted");
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

    write_workspace_with_default_expand(
        &root,
        &idx_snap_hex,
        &sig_map_hex,
        Some(&lex_snap_hex),
        Some(true),
    );

    let out_path = base.join("ask_default_expand.txt");
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
            "bananas",
        ],
    );
    assert_eq!(acode, 0, "stderr={}", String::from_utf8_lossy(&aerr));
    let s = std::fs::read_to_string(&out_path).unwrap();
    assert!(
        s.contains("[E0]"),
        "expected workspace default_expand=1 to enable expansion"
    );
}

#[test]
fn ask_uses_workspace_default_meta_when_flag_is_omitted() {
    let base = tmp_dir("ask_uses_workspace_default_meta_when_flag_is_omitted");
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

    let out_default = base.join("ask_default_meta_operator.txt");
    let (dcode, _dout, derr) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--presentation",
            "operator",
            "--out-file",
            out_default.to_str().unwrap(),
            "banana",
        ],
    );
    assert_eq!(dcode, 0, "stderr={}", String::from_utf8_lossy(&derr));
    let s_default = std::fs::read_to_string(&out_default).unwrap();
    let qid_default =
        parse_query_id_from_answer_text(&s_default).expect("query_id in default-meta ask answer");

    let out_explicit = base.join("ask_explicit_meta_operator.txt");
    let (ecode, _eout, eerr) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--meta",
            "--presentation",
            "operator",
            "--out-file",
            out_explicit.to_str().unwrap(),
            "banana",
        ],
    );
    assert_eq!(ecode, 0, "stderr={}", String::from_utf8_lossy(&eerr));
    let s_explicit = std::fs::read_to_string(&out_explicit).unwrap();
    let qid_explicit =
        parse_query_id_from_answer_text(&s_explicit).expect("query_id in explicit-meta ask answer");

    assert_eq!(
        qid_default, qid_explicit,
        "expected workspace default_meta=1 to match explicit --meta query_id"
    );
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
fn ask_uses_workspace_exemplar_memory_when_flag_is_omitted() {
    let base = tmp_dir("ask_uses_workspace_exemplar_memory_when_flag_is_omitted");
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
    let exemplar_memory = ExemplarMemoryV1 {
        version: EXEMPLAR_MEMORY_V1_VERSION,
        build_id: [0u8; 32],
        flags: 0,
        rows: vec![ExemplarRowV1 {
            exemplar_id: Id64(7),
            response_mode: ExemplarResponseModeV1::Direct,
            structure_kind: ExemplarStructureKindV1::Direct,
            tone_kind: ExemplarToneKindV1::Supportive,
            flags: 0,
            support_count: 1,
            support_refs: Vec::new(),
        }],
    };
    let exemplar_hash = put_exemplar_memory_v1(&store, &exemplar_memory).unwrap();
    let exemplar_hex = fsa_lm::hash::hex32(&exemplar_hash);

    write_workspace_with_advisories(
        &root,
        &idx_snap_hex,
        &sig_map_hex,
        None,
        None,
        None,
        None,
        None,
        Some(&exemplar_hex),
        None,
    );

    let out_path = base.join("ask_exemplar_out.txt");
    let (acode, _aout, aerr) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--presentation",
            "operator",
            "--out-file",
            out_path.to_str().unwrap(),
            "banana",
        ],
    );
    assert_eq!(acode, 0, "stderr={}", String::from_utf8_lossy(&aerr));
    let s = std::fs::read_to_string(&out_path).unwrap();
    assert!(
        s.contains(
            "exemplar_match exemplar_id=7 response_mode=Direct structure=Direct tone=Supportive"
        ),
        "output={}",
        s
    );
}

#[test]
fn ask_workspace_exemplar_memory_keeps_grounding_and_refs() {
    let base = tmp_dir("ask_workspace_exemplar_memory_keeps_grounding_and_refs");
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
    let exemplar_memory = ExemplarMemoryV1 {
        version: EXEMPLAR_MEMORY_V1_VERSION,
        build_id: [0u8; 32],
        flags: 0,
        rows: vec![ExemplarRowV1 {
            exemplar_id: Id64(11),
            response_mode: ExemplarResponseModeV1::Direct,
            structure_kind: ExemplarStructureKindV1::Direct,
            tone_kind: ExemplarToneKindV1::Supportive,
            flags: 0,
            support_count: 1,
            support_refs: Vec::new(),
        }],
    };
    let exemplar_hash = put_exemplar_memory_v1(&store, &exemplar_memory).unwrap();
    let exemplar_hex = fsa_lm::hash::hex32(&exemplar_hash);

    write_workspace_with_advisories(
        &root,
        &idx_snap_hex,
        &sig_map_hex,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    );

    let base_out = base.join("ask_workspace_exemplar_base.txt");
    let (rc0, _o0, e0) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--presentation",
            "operator",
            "--out-file",
            base_out.to_str().unwrap(),
            "banana",
        ],
    );
    assert_eq!(rc0, 0, "stderr={}", String::from_utf8_lossy(&e0));

    write_workspace_with_advisories(
        &root,
        &idx_snap_hex,
        &sig_map_hex,
        None,
        None,
        None,
        None,
        None,
        Some(&exemplar_hex),
        None,
    );

    let shaped_out = base.join("ask_workspace_exemplar_shaped.txt");
    let (rc1, _o1, e1) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--presentation",
            "operator",
            "--out-file",
            shaped_out.to_str().unwrap(),
            "banana",
        ],
    );
    assert_eq!(rc1, 0, "stderr={}", String::from_utf8_lossy(&e1));

    let base_text = std::fs::read_to_string(&base_out).unwrap();
    let shaped_text = std::fs::read_to_string(&shaped_out).unwrap();
    assert!(
        shaped_text.contains(
            "exemplar_match exemplar_id=11 response_mode=Direct structure=Direct tone=Supportive"
        ),
        "output={}",
        shaped_text
    );
    assert_eq!(
        evidence_lines_from_answer_text(&base_text),
        evidence_lines_from_answer_text(&shaped_text)
    );
    assert_eq!(
        plan_ref_lines_from_answer_text(&base_text),
        plan_ref_lines_from_answer_text(&shaped_text)
    );
}

#[test]
fn ask_missing_workspace_exemplar_memory_falls_back_cleanly() {
    let base = tmp_dir("ask_missing_workspace_exemplar_memory_falls_back_cleanly");
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

    let missing_exemplar_hex = fsa_lm::hash::hex32(&fsa_lm::hash::blake3_hash(
        b"missing-workspace-exemplar-memory",
    ));
    write_workspace_with_advisories(
        &root,
        &idx_snap_hex,
        &sig_map_hex,
        None,
        None,
        None,
        None,
        None,
        Some(&missing_exemplar_hex),
        None,
    );

    let out_path = base.join("ask_missing_exemplar_out.txt");
    let (acode, _aout, aerr) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--presentation",
            "operator",
            "--out-file",
            out_path.to_str().unwrap(),
            "banana",
        ],
    );
    let aerr_s = String::from_utf8_lossy(&aerr).replace("\r\n", "\n");
    assert_eq!(acode, 0, "stderr={}", aerr_s);
    assert!(
        !aerr_s.contains("missing exemplar memory"),
        "stderr={}",
        aerr_s
    );
    let s = std::fs::read_to_string(&out_path).unwrap();
    assert!(s.contains("Answer v1"), "output={}", s);
    assert!(s.contains("[E0]"), "output={}", s);
    assert!(!s.contains("exemplar_match"), "output={}", s);
}
