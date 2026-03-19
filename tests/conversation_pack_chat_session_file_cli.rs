// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

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
        "banana_top	banana banana banana ripe fruit\n",
        "banana_low	banana fruit\n",
        "carrot	carrot vegetable\n",
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
fn chat_session_file_autosave_and_resume() {
    let base = tmp_dir("chat_session_file_autosave_and_resume");
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

    let session_path = base.join("chat_session.txt");

    // First run: one question then exit. Autosave should write the session file.
    let mut child = Command::new(bin)
        .args([
            "chat",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "--autosave",
            "--k",
            "8",
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

    let conv1 = parse_file_kv(&session_path, "conversation_pack")
        .expect("conversation_pack= in session file");

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

    // Second run: resume implicitly via session-file.
    let mut child2 = Command::new(bin)
        .args([
            "chat",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "--autosave",
            "--k",
            "8",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    {
        let stdin = child2.stdin.as_mut().unwrap();
        stdin.write_all(b"night\n/exit\n").unwrap();
    }
    let out2 = child2.wait_with_output().unwrap();
    assert_eq!(
        out2.status.code().unwrap_or(-1),
        0,
        "stderr={}",
        String::from_utf8_lossy(&out2.stderr)
    );
    let stdout2 = String::from_utf8_lossy(&out2.stdout);
    assert!(!stdout2.contains("Answer v1"), "stdout={}", stdout2);
    assert!(!stdout2.contains("query_id="), "stdout={}", stdout2);

    let conv2 = parse_file_kv(&session_path, "conversation_pack")
        .expect("conversation_pack= in session file after resume");
    assert!(is_hex64(&conv2));
    assert_ne!(conv1, conv2, "autosave should advance the session pointer");
}

#[test]
fn chat_session_file_persists_advisory_ids_in_conversation_pack() {
    let base = tmp_dir("chat_session_file_persists_advisory_ids_in_conversation_pack");
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

    let session_path = base.join("chat_session.txt");
    let mut child = Command::new(bin)
        .args([
            "chat",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "--autosave",
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
fn chat_session_file_persists_presentation_mode_in_conversation_pack() {
    let base = tmp_dir("chat_session_file_persists_presentation_mode_in_conversation_pack");
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

    let session_path = base.join("chat_session_operator.txt");
    let mut child = Command::new(bin)
        .args([
            "chat",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "--autosave",
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
fn chat_session_file_resume_restores_presentation_and_graph_relevance() {
    let base = tmp_dir("chat_session_file_resume_restores_presentation_and_graph_relevance");
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
        build_id: blake3_hash(b"session-file-resume-graph"),
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

    let session_path = base.join("chat_resume_operator_graph.txt");
    let mut child1 = Command::new(bin)
        .args([
            "chat",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "--autosave",
            "--presentation",
            "operator",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    {
        let stdin = child1.stdin.as_mut().unwrap();
        stdin.write_all(b"banana\n/exit\n").unwrap();
    }
    let out1 = child1.wait_with_output().unwrap();
    assert_eq!(
        out1.status.code().unwrap_or(-1),
        0,
        "stderr={}",
        String::from_utf8_lossy(&out1.stderr)
    );
    let stdout1 = String::from_utf8_lossy(&out1.stdout);
    assert!(stdout1.contains("Answer v1"), "stdout={}", stdout1);
    assert!(
        stdout1.contains("graph_trace seeds=1 candidates=1 reasons=banana:"),
        "stdout={}",
        stdout1
    );

    write_workspace(&root, &idx_snap_hex, &sig_map_hex);

    let mut child2 = Command::new(bin)
        .args([
            "chat",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "--autosave",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    {
        let stdin = child2.stdin.as_mut().unwrap();
        stdin.write_all(b"banana\n/exit\n").unwrap();
    }
    let out2 = child2.wait_with_output().unwrap();
    assert_eq!(
        out2.status.code().unwrap_or(-1),
        0,
        "stderr={}",
        String::from_utf8_lossy(&out2.stderr)
    );
    let stdout2 = String::from_utf8_lossy(&out2.stdout);
    assert!(stdout2.contains("Answer v1"), "stdout={}", stdout2);
    assert!(
        stdout2.contains("graph_trace seeds=1 candidates=1 reasons=banana:"),
        "stdout={}",
        stdout2
    );
    assert!(stdout2.contains("[E1]"), "stdout={}", stdout2);
}

#[test]
fn chat_session_file_resume_restores_presentation_and_exemplar_memory() {
    let base = tmp_dir("chat_session_file_resume_restores_presentation_and_exemplar_memory");
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

    let ws1 = format!(
        "merged_snapshot={}\nmerged_sig_map={}\nexemplar_memory={}\n",
        idx_snap_hex, sig_map_hex, exemplar_hex,
    );
    std::fs::write(root.join("workspace_v1.txt"), ws1.as_bytes()).unwrap();

    let session_path = base.join("chat_resume_operator_exemplar.txt");
    let mut child1 = Command::new(bin)
        .args([
            "chat",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "--autosave",
            "--presentation",
            "operator",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    {
        let stdin = child1.stdin.as_mut().unwrap();
        stdin.write_all(b"banana\n/exit\n").unwrap();
    }
    let out1 = child1.wait_with_output().unwrap();
    assert_eq!(
        out1.status.code().unwrap_or(-1),
        0,
        "stderr={}",
        String::from_utf8_lossy(&out1.stderr)
    );
    let stdout1 = String::from_utf8_lossy(&out1.stdout);
    assert!(stdout1.contains("Answer v1"), "stdout={}", stdout1);
    assert!(
        stdout1.contains(
            "exemplar_match exemplar_id=11 response_mode=Direct structure=Direct tone=Supportive"
        ),
        "stdout={}",
        stdout1
    );

    let ws2 = format!(
        "merged_snapshot={}\nmerged_sig_map={}\n",
        idx_snap_hex, sig_map_hex
    );
    std::fs::write(root.join("workspace_v1.txt"), ws2.as_bytes()).unwrap();

    let mut child2 = Command::new(bin)
        .args([
            "chat",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "--autosave",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    {
        let stdin = child2.stdin.as_mut().unwrap();
        stdin.write_all(b"banana\n/exit\n").unwrap();
    }
    let out2 = child2.wait_with_output().unwrap();
    assert_eq!(
        out2.status.code().unwrap_or(-1),
        0,
        "stderr={}",
        String::from_utf8_lossy(&out2.stderr)
    );
    let stdout2 = String::from_utf8_lossy(&out2.stdout);
    assert!(stdout2.contains("Answer v1"), "stdout={}", stdout2);
    assert!(
        stdout2.contains(
            "exemplar_match exemplar_id=11 response_mode=Direct structure=Direct tone=Supportive"
        ),
        "stdout={}",
        stdout2
    );
}

#[test]
fn chat_session_file_resume_restores_presentation_and_markov_model() {
    let base = tmp_dir("chat_session_file_resume_restores_presentation_and_markov_model");
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

    let session_path = base.join("chat_resume_operator_markov.txt");
    let mut child1 = Command::new(bin)
        .args([
            "chat",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "--autosave",
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
        let stdin = child1.stdin.as_mut().unwrap();
        stdin.write_all(b"banana\n/exit\n").unwrap();
    }
    let out1 = child1.wait_with_output().unwrap();
    assert_eq!(
        out1.status.code().unwrap_or(-1),
        0,
        "stderr={}",
        String::from_utf8_lossy(&out1.stderr)
    );
    let stdout1 = String::from_utf8_lossy(&out1.stdout);
    assert!(stdout1.contains("Answer v1"), "stdout={}", stdout1);
    assert!(
        stdout1.contains(supportive_preface_v1()),
        "stdout={}",
        stdout1
    );

    let ws2 = format!(
        "merged_snapshot={}\nmerged_sig_map={}\n",
        idx_snap_hex, sig_map_hex
    );
    std::fs::write(root.join("workspace_v1.txt"), ws2.as_bytes()).unwrap();

    let mut child2 = Command::new(bin)
        .args([
            "chat",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "--autosave",
            "--pragmatics",
            &fsa_lm::hash::hex32(&prag_hash),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    {
        let stdin = child2.stdin.as_mut().unwrap();
        stdin.write_all(b"banana\n/exit\n").unwrap();
    }
    let out2 = child2.wait_with_output().unwrap();
    assert_eq!(
        out2.status.code().unwrap_or(-1),
        0,
        "stderr={}",
        String::from_utf8_lossy(&out2.stderr)
    );
    let stdout2 = String::from_utf8_lossy(&out2.stdout);
    assert!(stdout2.contains("Answer v1"), "stdout={}", stdout2);
    assert!(
        stdout2.contains(supportive_preface_v1()),
        "stdout={}",
        stdout2
    );
}

#[test]
fn chat_session_file_resume_from_legacy_pack_uses_workspace_graph_and_user_default_surface() {
    let base = tmp_dir(
        "chat_session_file_resume_from_legacy_pack_uses_workspace_graph_and_user_default_surface",
    );
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
        build_id: blake3_hash(b"legacy-session-graph"),
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

    let mut legacy = ConversationPackV1::new(
        5,
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

    let session_path = base.join("chat_resume_legacy.txt");
    std::fs::write(
        &session_path,
        format!("conversation_pack={}\n", legacy_hex).as_bytes(),
    )
    .unwrap();

    let mut child1 = Command::new(bin)
        .args([
            "chat",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "--autosave",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    {
        let stdin = child1.stdin.as_mut().unwrap();
        stdin.write_all(b"banana\n/exit\n").unwrap();
    }
    let out1 = child1.wait_with_output().unwrap();
    assert_eq!(
        out1.status.code().unwrap_or(-1),
        0,
        "stderr={}",
        String::from_utf8_lossy(&out1.stderr)
    );
    let stdout1 = String::from_utf8_lossy(&out1.stdout);
    assert!(!stdout1.contains("Answer v1"), "stdout={}", stdout1);
    assert!(!stdout1.contains("graph_trace"), "stdout={}", stdout1);

    let mut child2 = Command::new(bin)
        .args([
            "chat",
            "--root",
            root.to_str().unwrap(),
            "--session-file",
            session_path.to_str().unwrap(),
            "--autosave",
            "--presentation",
            "operator",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    {
        let stdin = child2.stdin.as_mut().unwrap();
        stdin.write_all(b"banana\n/exit\n").unwrap();
    }
    let out2 = child2.wait_with_output().unwrap();
    assert_eq!(
        out2.status.code().unwrap_or(-1),
        0,
        "stderr={}",
        String::from_utf8_lossy(&out2.stderr)
    );
    let stdout2 = String::from_utf8_lossy(&out2.stdout);
    assert!(stdout2.contains("Answer v1"), "stdout={}", stdout2);
    assert!(
        stdout2.contains("graph_trace seeds=1 candidates=1 reasons=banana:"),
        "stdout={}",
        stdout2
    );
    assert!(stdout2.contains("[E1]"), "stdout={}", stdout2);
}
