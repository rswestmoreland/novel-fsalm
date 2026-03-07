// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use fsa_lm::artifact::FsArtifactStore;
use fsa_lm::hash::Hash32;
use fsa_lm::markov_hints::{MH_FLAG_HAS_HISTORY};
use fsa_lm::markov_hints_artifact::get_markov_hints_v1;
use fsa_lm::markov_model::{MarkovModelV1, MarkovNextV1, MarkovStateV1, MarkovTokenV1, MARKOV_MODEL_V1_VERSION};
use fsa_lm::markov_model_artifact::put_markov_model_v1;
use fsa_lm::markov_trace_artifact::get_markov_trace_v1;
use fsa_lm::pragmatics_frame::{PragmaticsFrameV1, PRAGMATICS_FRAME_V1_VERSION, RhetoricModeV1};
use fsa_lm::pragmatics_frame_store::put_pragmatics_frame_v1;
use fsa_lm::replay_artifact::get_replay_log;
use fsa_lm::replay_steps::{STEP_MARKOV_HINTS_V1, STEP_MARKOV_TRACE_V1};
use fsa_lm::realizer_directives::ToneV1;

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

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
    let s = format!("merged_snapshot={}\nmerged_sig_map={}\n", merged_snapshot, merged_sig_map);
    std::fs::write(root.join("workspace_v1.txt"), s.as_bytes()).unwrap();
}

fn hex_to_hash32(s: &str) -> Hash32 {
    let mut out = [0u8; 32];
    for i in 0..32 {
        let b = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).unwrap();
        out[i] = b;
    }
    out
}

fn sample_markov_model_all_tones() -> MarkovModelV1 {
    use fsa_lm::frame::{derive_id64, Id64};
    use fsa_lm::markov_hints::MarkovChoiceKindV1;

    // Include all tone preface ids so filtering never empties due to tone.
    fn preface_id(t: ToneV1, v: u8) -> Id64 {
        let key = match (t, v) {
            (ToneV1::Supportive, 0) => b"preface:supportive:0".as_slice(),
            (ToneV1::Supportive, _) => b"preface:supportive:1".as_slice(),
            (ToneV1::Neutral, 0) => b"preface:neutral:0".as_slice(),
            (ToneV1::Neutral, _) => b"preface:neutral:1".as_slice(),
            (ToneV1::Direct, 0) => b"preface:direct:0".as_slice(),
            (ToneV1::Direct, _) => b"preface:direct:1".as_slice(),
            (ToneV1::Cautious, 0) => b"preface:cautious:0".as_slice(),
            (ToneV1::Cautious, _) => b"preface:cautious:1".as_slice(),
        };
        derive_id64(b"markov_choice_v1", key)
    }

    let mut next: Vec<MarkovNextV1> = Vec::new();
    let mut c: u32 = 100;
    for t in [ToneV1::Supportive, ToneV1::Neutral, ToneV1::Direct, ToneV1::Cautious] {
        for v in [0u8, 1u8] {
            next.push(MarkovNextV1 {
                token: MarkovTokenV1::new(MarkovChoiceKindV1::Opener, preface_id(t, v)),
                count: c,
            });
            c = c.saturating_sub(1);
        }
    }

    // Canonical: count desc, token asc for ties.
    next.sort_by(|a, b| {
        match b.count.cmp(&a.count) {
            core::cmp::Ordering::Equal => a.token.cmp(&b.token),
            o => o,
        }
    });

    let s0 = MarkovStateV1 { context: Vec::new(), escape_count: 0, next };
    MarkovModelV1 {
        version: MARKOV_MODEL_V1_VERSION,
        order_n_max: 3,
        max_next_per_state: 16,
        total_transitions: 100,
        corpus_hash: [0u8; 32],
        states: vec![s0],
    }
}

fn sample_pragmatics() -> PragmaticsFrameV1 {
    PragmaticsFrameV1 {
        version: PRAGMATICS_FRAME_V1_VERSION,
        source_id: fsa_lm::frame::Id64(1),
        msg_ix: 0,
        byte_len: 12,
        ascii_only: 1,
        temperature: 0,
        valence: 0,
        arousal: 0,
        politeness: 500,
        formality: 500,
        directness: 500,
        empathy_need: 700,
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
    }
}

#[test]
fn chat_resume_rebuilds_markov_context_from_saved_replay_ids() {
    let base = tmp_dir("chat_resume_rebuilds_markov_context");
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

    let store = FsArtifactStore::new(root.clone()).unwrap();
    let model = sample_markov_model_all_tones();
    assert!(model.validate().is_ok());
    let model_hash = put_markov_model_v1(&store, &model).unwrap();
    let prag_hash = put_pragmatics_frame_v1(&store, &sample_pragmatics()).unwrap();

    let session_path = base.join("chat_session.txt");

    // First run: create a saved conversation with at least one assistant replay id.
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
    assert_eq!(out.status.code().unwrap_or(-1), 0, "stderr={}", String::from_utf8_lossy(&out.stderr));

    let conv1_hex = parse_file_kv(&session_path, "conversation_pack").expect("conversation_pack= in session file");
    let conv1 = hex_to_hash32(&conv1_hex);
    let pack1 = fsa_lm::conversation_pack_artifact::get_conversation_pack(&store, &conv1).unwrap().unwrap();
    let replay1 = pack1
        .messages
        .iter()
        .rev()
        .find(|m| m.role == fsa_lm::conversation_pack::ConversationRole::Assistant)
        .and_then(|m| m.replay_id)
        .expect("assistant replay_id present");

    // Ensure the first run produced a non-empty MarkovTrace token stream.
    let log1 = get_replay_log(&store, &replay1).unwrap().unwrap();
    let mut mt1: Option<Hash32> = None;
    for st in log1.steps.iter() {
        if st.name == STEP_MARKOV_TRACE_V1 {
            if !st.outputs.is_empty() {
                mt1 = Some(st.outputs[0]);
                break;
            }
        }
    }
    let mt1 = mt1.expect("markov trace output");
    let trace1 = get_markov_trace_v1(&store, &mt1).unwrap().unwrap();
    assert!(!trace1.tokens.is_empty(), "expected non-empty MarkovTrace tokens");

    // Second run: resume via session file, enable markov hints. The resume path should
    // rebuild markov context tail from prior assistant replay ids, setting HAS_HISTORY.
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
            "--pragmatics",
            &fsa_lm::hash::hex32(&prag_hash),
            "--markov-model",
            &fsa_lm::hash::hex32(&model_hash),
            "--markov-max-choices",
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
    assert_eq!(out2.status.code().unwrap_or(-1), 0, "stderr={}", String::from_utf8_lossy(&out2.stderr));

    let conv2_hex = parse_file_kv(&session_path, "conversation_pack").expect("conversation_pack= after resume");
    let conv2 = hex_to_hash32(&conv2_hex);
    let pack2 = fsa_lm::conversation_pack_artifact::get_conversation_pack(&store, &conv2).unwrap().unwrap();
    let replay2 = pack2
        .messages
        .iter()
        .rev()
        .find(|m| m.role == fsa_lm::conversation_pack::ConversationRole::Assistant)
        .and_then(|m| m.replay_id)
        .expect("assistant replay_id present after resume");

    let log2 = get_replay_log(&store, &replay2).unwrap().unwrap();
    let mut mh_hash_opt: Option<Hash32> = None;
    for st in log2.steps.iter() {
        if st.name == STEP_MARKOV_HINTS_V1 {
            if !st.outputs.is_empty() {
                mh_hash_opt = Some(st.outputs[0]);
                break;
            }
        }
    }
    let mh_hash = mh_hash_opt.expect("markov-hints-v1 output");
    let hints = get_markov_hints_v1(&store, &mh_hash).unwrap().unwrap();
    assert_ne!(hints.flags & MH_FLAG_HAS_HISTORY, 0, "expected HAS_HISTORY flag");
}
