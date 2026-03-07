// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use fsa_lm::artifact::FsArtifactStore;
use fsa_lm::hash::Hash32;
use fsa_lm::markov_hints::MH_FLAG_HAS_HISTORY;
use fsa_lm::markov_hints_artifact::get_markov_hints_v1;
use fsa_lm::markov_model::{
    MarkovModelV1, MarkovNextV1, MarkovStateV1, MarkovTokenV1, MARKOV_MODEL_V1_VERSION,
};
use fsa_lm::markov_model_artifact::put_markov_model_v1;
use fsa_lm::pragmatics_frame::{PragmaticsFrameV1, RhetoricModeV1, PRAGMATICS_FRAME_V1_VERSION};
use fsa_lm::pragmatics_frame_store::put_pragmatics_frame_v1;
use fsa_lm::realizer_directives::ToneV1;
use fsa_lm::replay_artifact::get_replay_log;
use fsa_lm::replay_steps::STEP_MARKOV_HINTS_V1;

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
    let s = std::fs::read_to_string(path).ok()?.replace("\r\n", "\n");
    let prefix = format!("{}=", key);
    for line in s.lines() {
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

fn hex_to_hash32(s: &str) -> Hash32 {
    fsa_lm::hash::parse_hash32_hex(s).unwrap()
}

fn write_workspace(root: &Path, merged_snapshot: &str, merged_sig_map: &str) {
    let s = format!(
        "merged_snapshot={}\nmerged_sig_map={}\n",
        merged_snapshot, merged_sig_map
    );
    std::fs::write(root.join("workspace_v1.txt"), s.as_bytes()).unwrap();
}

fn sample_markov_model_all_tones() -> MarkovModelV1 {
    use fsa_lm::frame::{derive_id64, Id64};
    use fsa_lm::markov_hints::MarkovChoiceKindV1;

    // Include all tone preface ids so filtering never empties due to tone.
    fn preface_choice_id(t: ToneV1, variant: u8) -> Id64 {
        match t {
            ToneV1::Supportive => match variant {
                0 => derive_id64(b"markov_choice_v1", b"preface:supportive:0"),
                _ => derive_id64(b"markov_choice_v1", b"preface:supportive:1"),
            },
            ToneV1::Neutral => match variant {
                0 => derive_id64(b"markov_choice_v1", b"preface:neutral:0"),
                _ => derive_id64(b"markov_choice_v1", b"preface:neutral:1"),
            },
            ToneV1::Direct => match variant {
                0 => derive_id64(b"markov_choice_v1", b"preface:direct:0"),
                _ => derive_id64(b"markov_choice_v1", b"preface:direct:1"),
            },
            ToneV1::Cautious => match variant {
                0 => derive_id64(b"markov_choice_v1", b"preface:cautious:0"),
                _ => derive_id64(b"markov_choice_v1", b"preface:cautious:1"),
            },
        }
    }

    fn tok_preface(t: ToneV1, v: u8) -> MarkovTokenV1 {
        MarkovTokenV1::new(MarkovChoiceKindV1::Opener, preface_choice_id(t, v))
    }

    let mut states: Vec<MarkovStateV1> = Vec::new();

    // One state keyed on the first preface token (higher-order context first).
    let ctx = vec![tok_preface(ToneV1::Neutral, 0)];
    states.push(MarkovStateV1 {
        context: ctx,
        escape_count: 0,
        next: vec![MarkovNextV1 {
            token: tok_preface(ToneV1::Neutral, 1),
            count: 1,
        }],
    });

    // Unconditional distribution with one preface for each tone.
    // Use strictly descending counts so canonical ordering does not depend on token ordering.
    let mut next: Vec<MarkovNextV1> = Vec::new();
    next.push(MarkovNextV1 {
        token: tok_preface(ToneV1::Supportive, 0),
        count: 8,
    });
    next.push(MarkovNextV1 {
        token: tok_preface(ToneV1::Supportive, 1),
        count: 7,
    });
    next.push(MarkovNextV1 {
        token: tok_preface(ToneV1::Neutral, 0),
        count: 6,
    });
    next.push(MarkovNextV1 {
        token: tok_preface(ToneV1::Neutral, 1),
        count: 5,
    });
    next.push(MarkovNextV1 {
        token: tok_preface(ToneV1::Direct, 0),
        count: 4,
    });
    next.push(MarkovNextV1 {
        token: tok_preface(ToneV1::Direct, 1),
        count: 3,
    });
    next.push(MarkovNextV1 {
        token: tok_preface(ToneV1::Cautious, 0),
        count: 2,
    });
    next.push(MarkovNextV1 {
        token: tok_preface(ToneV1::Cautious, 1),
        count: 1,
    });

    states.push(MarkovStateV1 {
        context: vec![],
        escape_count: 0,
        next,
    });

    let model = MarkovModelV1 {
        version: MARKOV_MODEL_V1_VERSION,
        order_n_max: 2,
        max_next_per_state: 8,
        total_transitions: 36,
        corpus_hash: [0u8; 32],
        states,
    };
    assert!(model.validate().is_ok());
    model
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
fn chat_in_session_markov_hints_set_has_history_after_first_turn() {
    let base = tmp_dir("chat_in_session_markov_hints_set_has_history");
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
        let stdin = child.stdin.as_mut().unwrap();
        stdin.write_all(b"banana\nnight\n/exit\n").unwrap();
    }

    let out = child.wait_with_output().unwrap();
    assert_eq!(
        out.status.code().unwrap_or(-1),
        0,
        "stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );

    let conv_hex = parse_file_kv(&session_path, "conversation_pack")
        .expect("conversation_pack= in session file");
    let conv = hex_to_hash32(&conv_hex);
    let pack = fsa_lm::conversation_pack_artifact::get_conversation_pack(&store, &conv)
        .unwrap()
        .unwrap();

    let mut assistant_replays: Vec<Hash32> = Vec::new();
    for m in pack.messages.iter() {
        if m.role == fsa_lm::conversation_pack::ConversationRole::Assistant {
            if let Some(rid) = m.replay_id {
                assistant_replays.push(rid);
            }
        }
    }
    assert!(
        assistant_replays.len() >= 2,
        "expected at least two assistant turns"
    );

    // First assistant turn should not claim history.
    let log1 = get_replay_log(&store, &assistant_replays[0])
        .unwrap()
        .unwrap();
    let mut mh1: Option<Hash32> = None;
    for st in log1.steps.iter() {
        if st.name == STEP_MARKOV_HINTS_V1 && !st.outputs.is_empty() {
            mh1 = Some(st.outputs[0]);
            break;
        }
    }
    let mh1 = mh1.expect("markov-hints-v1 output on first turn");
    let hints1 = get_markov_hints_v1(&store, &mh1).unwrap().unwrap();
    assert_eq!(
        hints1.flags & MH_FLAG_HAS_HISTORY,
        0,
        "did not expect HAS_HISTORY on first turn"
    );

    // Second assistant turn should set HAS_HISTORY based on in-session tail.
    let log2 = get_replay_log(&store, &assistant_replays[1])
        .unwrap()
        .unwrap();
    let mut mh2: Option<Hash32> = None;
    for st in log2.steps.iter() {
        if st.name == STEP_MARKOV_HINTS_V1 && !st.outputs.is_empty() {
            mh2 = Some(st.outputs[0]);
            break;
        }
    }
    let mh2 = mh2.expect("markov-hints-v1 output on second turn");
    let hints2 = get_markov_hints_v1(&store, &mh2).unwrap().unwrap();
    assert_ne!(
        hints2.flags & MH_FLAG_HAS_HISTORY,
        0,
        "expected HAS_HISTORY on second turn"
    );
}
