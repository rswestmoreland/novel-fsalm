// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use std::path::{Path, PathBuf};
use std::process::Command;

use fsa_lm::artifact::{ArtifactStore, FsArtifactStore};
use fsa_lm::codec::ByteWriter;
use fsa_lm::conversation_pack::{
    ConversationLimits, ConversationMessage, ConversationPackV1, ConversationRole,
};
use fsa_lm::exemplar_memory::{
    ExemplarMemoryV1, ExemplarResponseModeV1, ExemplarRowV1, ExemplarStructureKindV1,
    ExemplarToneKindV1, EXEMPLAR_MEMORY_V1_VERSION,
};
use fsa_lm::exemplar_memory_artifact::put_exemplar_memory_v1;
use fsa_lm::frame::{derive_id64, Id64};
use fsa_lm::graph_relevance::{
    GraphNodeKindV1, GraphRelevanceEdgeV1, GraphRelevanceRowV1, GraphRelevanceV1,
    GRAPH_RELEVANCE_V1_VERSION, GREDGE_FLAG_SYMMETRIC, GR_FLAG_HAS_TERM_ROWS,
};
use fsa_lm::graph_relevance_artifact::put_graph_relevance_v1;
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

fn parse_file_kv(path: &Path, key: &str) -> Option<String> {
    let s = std::fs::read_to_string(path).ok()?;
    let prefix = format!("{}=", key);
    let mut last: Option<String> = None;
    for raw in s.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix(&prefix) {
            let v = rest.trim();
            if is_hex64(v) {
                last = Some(v.to_string());
            }
        }
    }
    last
}

fn write_workspace(root: &Path, merged_snapshot: &str, merged_sig_map: &str) {
    let s = format!(
        "merged_snapshot={}\nmerged_sig_map={}\n",
        merged_snapshot, merged_sig_map
    );
    std::fs::write(root.join("workspace_v1.txt"), s.as_bytes()).unwrap();
}

fn write_workspace_with_lexicon_defaults(
    root: &Path,
    merged_snapshot: &str,
    merged_sig_map: &str,
    lexicon_snapshot: &str,
    default_expand: bool,
) {
    let s = format!(
        "merged_snapshot={}\nmerged_sig_map={}\nlexicon_snapshot={}\ndefault_expand={}\n",
        merged_snapshot,
        merged_sig_map,
        lexicon_snapshot,
        if default_expand { 1 } else { 0 },
    );
    std::fs::write(root.join("workspace_v1.txt"), s.as_bytes()).unwrap();
}

fn encode_legacy_conversation_pack(p: &ConversationPackV1) -> Vec<u8> {
    let mut w = ByteWriter::with_capacity(256);
    w.write_u16(p.version);
    w.write_u64(p.seed);
    w.write_u32(p.max_output_tokens);
    w.write_raw(&p.snapshot_id);
    w.write_raw(&p.sig_map_id);
    match &p.lexicon_snapshot_id {
        Some(h) => {
            w.write_u8(1);
            w.write_raw(h);
        }
        None => w.write_u8(0),
    }
    w.write_u32(p.limits.max_message_bytes);
    w.write_u32(p.limits.max_total_message_bytes);
    w.write_u32(p.limits.max_messages);
    w.write_u8(if p.limits.keep_system { 1 } else { 0 });
    w.write_u32(p.messages.len() as u32);
    for m in &p.messages {
        let role = match m.role {
            ConversationRole::System => 0,
            ConversationRole::User => 1,
            ConversationRole::Assistant => 2,
        };
        w.write_u8(role);
        w.write_str(&m.content).unwrap();
        match &m.replay_id {
            Some(h) => {
                w.write_u8(1);
                w.write_raw(h);
            }
            None => w.write_u8(0),
        }
    }
    w.into_bytes()
}

fn write_workspace_with_advisory_defaults(
    root: &Path,
    merged_snapshot: &str,
    merged_sig_map: &str,
    markov_model: &str,
    exemplar_memory: &str,
    graph_relevance: &str,
) {
    let s = format!(
        "merged_snapshot={}\nmerged_sig_map={}\nmarkov_model={}\nexemplar_memory={}\ngraph_relevance={}\n",
        merged_snapshot,
        merged_sig_map,
        markov_model,
        exemplar_memory,
        graph_relevance,
    );
    std::fs::write(root.join("workspace_v1.txt"), s.as_bytes()).unwrap();
}

#[test]
fn ask_session_file_saves_and_resumes_conversation() {
    let base = tmp_dir("ask_session_file_saves_and_resumes_conversation");
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

    let session_path = base.join("ask_session.txt");

    let (acode, aout, aerr) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "--k",
            "8",
            "banana",
        ],
    );
    assert_eq!(acode, 0, "stderr={}", String::from_utf8_lossy(&aerr));
    let aout_s = String::from_utf8_lossy(&aout);
    assert!(!aout_s.contains("Answer v1"), "stdout={}", aout_s);
    assert!(!aout_s.contains("query_id="), "stdout={}", aout_s);

    let conv1 = parse_file_kv(&session_path, "conversation_pack")
        .expect("conversation_pack= in session file");
    assert!(is_hex64(&conv1));

    let (scode, sout, serr) = run_cmd(
        bin,
        &[
            "show-conversation",
            "--root",
            root.to_str().unwrap(),
            &conv1,
        ],
    );
    assert_eq!(scode, 0, "stderr={}", String::from_utf8_lossy(&serr));
    let sout_s = String::from_utf8_lossy(&sout).replace("\r\n", "\n");
    assert!(sout_s.contains(&format!("conversation_pack={}", conv1)));
    assert!(sout_s.contains("content=banana"));

    let (acode2, aout2, aerr2) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "--k",
            "8",
            "night",
        ],
    );
    assert_eq!(acode2, 0, "stderr={}", String::from_utf8_lossy(&aerr2));
    let aout2_s = String::from_utf8_lossy(&aout2);
    assert!(!aout2_s.contains("Answer v1"), "stdout={}", aout2_s);
    assert!(!aout2_s.contains("query_id="), "stdout={}", aout2_s);

    let conv2 = parse_file_kv(&session_path, "conversation_pack")
        .expect("conversation_pack= in session file after resume");
    assert!(is_hex64(&conv2));
    assert_ne!(conv1, conv2, "session pointer should advance after ask");

    let (scode2, sout2, serr2) = run_cmd(
        bin,
        &[
            "show-conversation",
            "--root",
            root.to_str().unwrap(),
            &conv2,
        ],
    );
    assert_eq!(scode2, 0, "stderr={}", String::from_utf8_lossy(&serr2));
    let sout2_s = String::from_utf8_lossy(&sout2).replace("\r\n", "\n");
    assert!(sout2_s.contains("content=banana"));
    assert!(sout2_s.contains("content=night"));
}

#[test]
fn ask_session_file_binds_workspace_lexicon_when_default_expand_is_enabled() {
    let base = tmp_dir("ask_session_file_binds_workspace_lexicon_when_default_expand_is_enabled");
    let root = base.join("root");
    std::fs::create_dir_all(&root).unwrap();

    let wiki_tsv = base.join("wiki.tsv");
    write_wiki_tsv(&wiki_tsv);

    let xml_path = base.join("wiktionary.xml");
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
</text>
  </revision>
</page>
</mediawiki>"#;
    std::fs::write(&xml_path, xml.as_bytes()).unwrap();

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

    write_workspace_with_lexicon_defaults(&root, &idx_snap_hex, &sig_map_hex, &lex_snap_hex, true);

    let session_path = base.join("ask_session_expand.txt");
    let (acode, _aout, aerr) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "bananas",
        ],
    );
    assert_eq!(acode, 0, "stderr={}", String::from_utf8_lossy(&aerr));

    let conv = parse_file_kv(&session_path, "conversation_pack")
        .expect("conversation_pack= in session file");
    let (scode, sout, serr) = run_cmd(
        bin,
        &["show-conversation", "--root", root.to_str().unwrap(), &conv],
    );
    assert_eq!(scode, 0, "stderr={}", String::from_utf8_lossy(&serr));
    let sout_s = String::from_utf8_lossy(&sout).replace("\r\n", "\n");
    assert!(
        sout_s.contains(&format!("lexicon_snapshot_id={}", lex_snap_hex)),
        "stdout={}",
        sout_s
    );
}

#[test]
fn ask_session_file_persists_advisory_ids_in_conversation_pack() {
    let base = tmp_dir("ask_session_file_persists_advisory_ids_in_conversation_pack");
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

    let markov_hex = "1111111111111111111111111111111111111111111111111111111111111111";
    let exemplar_hex = "2222222222222222222222222222222222222222222222222222222222222222";
    let graph_hex = "3333333333333333333333333333333333333333333333333333333333333333";
    write_workspace_with_advisory_defaults(
        &root,
        &idx_snap_hex,
        &sig_map_hex,
        markov_hex,
        exemplar_hex,
        graph_hex,
    );

    let session_path = base.join("ask_session.txt");
    let (acode, _aout, aerr) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "banana",
        ],
    );
    assert_eq!(acode, 0, "stderr={}", String::from_utf8_lossy(&aerr));

    let conv = parse_file_kv(&session_path, "conversation_pack")
        .expect("conversation_pack= in session file");
    let (scode, sout, serr) = run_cmd(
        bin,
        &["show-conversation", "--root", root.to_str().unwrap(), &conv],
    );
    assert_eq!(scode, 0, "stderr={}", String::from_utf8_lossy(&serr));
    let sout_s = String::from_utf8_lossy(&sout).replace("\r\n", "\n");
    assert!(
        sout_s.contains(&format!("markov_model_id={}", markov_hex)),
        "stdout={}",
        sout_s
    );
    assert!(
        sout_s.contains(&format!("exemplar_memory_id={}", exemplar_hex)),
        "stdout={}",
        sout_s
    );
    assert!(
        sout_s.contains(&format!("graph_relevance_id={}", graph_hex)),
        "stdout={}",
        sout_s
    );
}

#[test]
fn ask_session_file_persists_presentation_mode_in_conversation_pack() {
    let base = tmp_dir("ask_session_file_persists_presentation_mode_in_conversation_pack");
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

    let session_path = base.join("ask_session_operator.txt");
    let (acode, _aout, aerr) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "--presentation",
            "operator",
            "banana",
        ],
    );
    assert_eq!(acode, 0, "stderr={}", String::from_utf8_lossy(&aerr));

    let conv = parse_file_kv(&session_path, "conversation_pack")
        .expect("conversation_pack= in session file");
    let (scode, sout, serr) = run_cmd(
        bin,
        &["show-conversation", "--root", root.to_str().unwrap(), &conv],
    );
    assert_eq!(scode, 0, "stderr={}", String::from_utf8_lossy(&serr));
    let sout_s = String::from_utf8_lossy(&sout).replace("\r\n", "\n");
    assert!(
        sout_s.contains("presentation_mode=operator"),
        "stdout={}",
        sout_s
    );
}

#[test]
fn ask_session_file_resume_restores_presentation_and_exemplar_memory() {
    let base = tmp_dir("ask_session_file_resume_restores_presentation_and_exemplar_memory");
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

    let ws1 = format!(
        "merged_snapshot={}\nmerged_sig_map={}\nexemplar_memory={}\n",
        idx_snap_hex, sig_map_hex, exemplar_hex,
    );
    std::fs::write(root.join("workspace_v1.txt"), ws1.as_bytes()).unwrap();

    let session_path = base.join("ask_resume_operator_exemplar.txt");
    let (acode1, aout1, aerr1) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "--presentation",
            "operator",
            "banana",
        ],
    );
    assert_eq!(acode1, 0, "stderr={}", String::from_utf8_lossy(&aerr1));
    let stdout1 = String::from_utf8_lossy(&aout1);
    assert!(stdout1.contains("Answer v1"), "stdout={}", stdout1);
    assert!(
        stdout1.contains(
            "exemplar_match exemplar_id=7 response_mode=Direct structure=Direct tone=Supportive"
        ),
        "stdout={}",
        stdout1
    );

    write_workspace(&root, &idx_snap_hex, &sig_map_hex);

    let (acode2, aout2, aerr2) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "banana",
        ],
    );
    assert_eq!(acode2, 0, "stderr={}", String::from_utf8_lossy(&aerr2));
    let stdout2 = String::from_utf8_lossy(&aout2);
    assert!(stdout2.contains("Answer v1"), "stdout={}", stdout2);
    assert!(
        stdout2.contains(
            "exemplar_match exemplar_id=7 response_mode=Direct structure=Direct tone=Supportive"
        ),
        "stdout={}",
        stdout2
    );
}

#[test]
fn ask_session_file_resume_restores_presentation_and_markov_model() {
    let base = tmp_dir("ask_session_file_resume_restores_presentation_and_markov_model");
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
    let markov_hex = fsa_lm::hash::hex32(&markov_hash);

    let ws1 = format!(
        "merged_snapshot={}\nmerged_sig_map={}\nmarkov_model={}\n",
        idx_snap_hex, sig_map_hex, markov_hex,
    );
    std::fs::write(root.join("workspace_v1.txt"), ws1.as_bytes()).unwrap();

    let session_path = base.join("ask_resume_operator_markov.txt");
    let (acode1, aout1, aerr1) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "--presentation",
            "operator",
            "--pragmatics",
            &fsa_lm::hash::hex32(&prag_hash),
            "banana",
        ],
    );
    assert_eq!(acode1, 0, "stderr={}", String::from_utf8_lossy(&aerr1));
    let stdout1 = String::from_utf8_lossy(&aout1);
    assert!(stdout1.contains("Answer v1"), "stdout={}", stdout1);
    assert!(
        stdout1.contains(supportive_preface_v1()),
        "stdout={}",
        stdout1
    );

    write_workspace(&root, &idx_snap_hex, &sig_map_hex);

    let (acode2, aout2, aerr2) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "--pragmatics",
            &fsa_lm::hash::hex32(&prag_hash),
            "banana",
        ],
    );
    assert_eq!(acode2, 0, "stderr={}", String::from_utf8_lossy(&aerr2));
    let stdout2 = String::from_utf8_lossy(&aout2);
    assert!(stdout2.contains("Answer v1"), "stdout={}", stdout2);
    assert!(
        stdout2.contains(supportive_preface_v1()),
        "stdout={}",
        stdout2
    );
}

#[test]
fn ask_session_file_resume_restores_presentation_and_graph_relevance() {
    let base = tmp_dir("ask_session_file_resume_restores_presentation_and_graph_relevance");
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
        build_id: fsa_lm::hash::blake3_hash(b"ask-session-file-resume-graph"),
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

    let ws1 = format!(
        "merged_snapshot={}\nmerged_sig_map={}\ndefault_expand=1\ngraph_relevance={}\n",
        idx_snap_hex, sig_map_hex, graph_hex,
    );
    std::fs::write(root.join("workspace_v1.txt"), ws1.as_bytes()).unwrap();

    let session_path = base.join("ask_resume_operator_graph.txt");
    let (acode1, aout1, aerr1) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "--presentation",
            "operator",
            "banana",
        ],
    );
    assert_eq!(acode1, 0, "stderr={}", String::from_utf8_lossy(&aerr1));
    let stdout1 = String::from_utf8_lossy(&aout1);
    assert!(stdout1.contains("Answer v1"), "stdout={}", stdout1);
    assert!(
        stdout1.contains("graph_trace seeds=1 candidates=1 reasons=banana:"),
        "stdout={}",
        stdout1
    );

    write_workspace(&root, &idx_snap_hex, &sig_map_hex);

    let (acode2, aout2, aerr2) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "banana",
        ],
    );
    assert_eq!(acode2, 0, "stderr={}", String::from_utf8_lossy(&aerr2));
    let stdout2 = String::from_utf8_lossy(&aout2);
    assert!(stdout2.contains("Answer v1"), "stdout={}", stdout2);
    assert!(
        stdout2.contains("graph_trace seeds=1 candidates=1 reasons=banana:"),
        "stdout={}",
        stdout2
    );
    assert!(stdout2.contains("[E1]"), "stdout={}", stdout2);
}

#[test]
fn ask_session_file_resume_from_legacy_pack_uses_workspace_exemplar_and_user_default_surface() {
    let base = tmp_dir(
        "ask_session_file_resume_from_legacy_pack_uses_workspace_exemplar_and_user_default_surface",
    );
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
            exemplar_id: Id64(9),
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

    let ws1 = format!(
        "merged_snapshot={}\nmerged_sig_map={}\nexemplar_memory={}\n",
        idx_snap_hex, sig_map_hex, exemplar_hex,
    );
    std::fs::write(root.join("workspace_v1.txt"), ws1.as_bytes()).unwrap();

    let mut legacy = ConversationPackV1::new(
        7,
        256,
        fsa_lm::hash::parse_hash32_hex(&idx_snap_hex).unwrap(),
        fsa_lm::hash::parse_hash32_hex(&sig_map_hex).unwrap(),
        None,
        ConversationLimits::default_v1(),
    );
    legacy.messages.push(ConversationMessage {
        role: ConversationRole::User,
        content: "earlier banana".to_string(),
        replay_id: None,
    });
    legacy.messages.push(ConversationMessage {
        role: ConversationRole::Assistant,
        content: "earlier reply".to_string(),
        replay_id: None,
    });
    legacy.canonicalize_in_place();
    let legacy_bytes = encode_legacy_conversation_pack(&legacy);
    let legacy_hash = store.put(&legacy_bytes).unwrap();
    let legacy_hex = fsa_lm::hash::hex32(&legacy_hash);

    let session_path = base.join("ask_resume_legacy.txt");
    std::fs::write(
        &session_path,
        format!("conversation_pack={}\n", legacy_hex).as_bytes(),
    )
    .unwrap();

    let (acode, aout, aerr) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "banana",
        ],
    );
    assert_eq!(acode, 0, "stderr={}", String::from_utf8_lossy(&aerr));
    let stdout = String::from_utf8_lossy(&aout);
    assert!(!stdout.contains("Answer v1"), "stdout={}", stdout);
    assert!(!stdout.contains("query_id="), "stdout={}", stdout);

    let (acode_op, aout_op, aerr_op) = run_cmd(
        bin,
        &[
            "ask",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "--presentation",
            "operator",
            "banana",
        ],
    );
    assert_eq!(acode_op, 0, "stderr={}", String::from_utf8_lossy(&aerr_op));
    let stdout_op = String::from_utf8_lossy(&aout_op);
    assert!(stdout_op.contains("Answer v1"), "stdout={}", stdout_op);
    assert!(
        stdout_op.contains(
            "exemplar_match exemplar_id=9 response_mode=Direct structure=Direct tone=Supportive"
        ),
        "stdout={}",
        stdout_op
    );
}
