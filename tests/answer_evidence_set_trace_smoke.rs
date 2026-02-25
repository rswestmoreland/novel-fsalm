// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use std::path::{Path, PathBuf};
use std::process::Command;

use fsa_lm::artifact::{ArtifactStore, FsArtifactStore};
use fsa_lm::evidence_bundle::EvidenceBundleV1;
use fsa_lm::evidence_set::EvidenceSetV1;
use fsa_lm::forecast::ForecastV1;
use fsa_lm::frame::{DocId, FrameRowV1, Id64, SourceId};
use fsa_lm::frame_segment::FrameSegmentV1;
use fsa_lm::frame_store::put_frame_segment_v1;
use fsa_lm::hash::{blake3_hash, hex32, Hash32};
use fsa_lm::index_segment::IndexSegmentV1;
use fsa_lm::index_snapshot::{IndexSnapshotEntryV1, IndexSnapshotV1};
use fsa_lm::index_snapshot_store::put_index_snapshot_v1;
use fsa_lm::index_store::put_index_segment_v1;
use fsa_lm::markov_trace::MarkovTraceV1;
use fsa_lm::planner_hints::PlannerHintsV1;
use fsa_lm::pragmatics_frame::{
    PragmaticsFrameV1, RhetoricModeV1, INTENT_FLAG_HAS_CODE, PRAGMATICS_FRAME_V1_VERSION,
};
use fsa_lm::pragmatics_frame_store::put_pragmatics_frame_v1;
use fsa_lm::prompt_artifact::put_prompt_pack;
use fsa_lm::prompt_pack::{Message, PromptIds, PromptLimits, PromptPack, Role};
use fsa_lm::replay::ReplayLog;
use fsa_lm::tokenizer::{term_freqs_from_text, TokenizerCfg};

fn tmp_root(name: &str) -> PathBuf {
    let base = std::env::temp_dir();
    let p = base.join(format!("fsa_lm_test_{}_{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn collect_bin_paths(dir: &Path, out: &mut Vec<PathBuf>) {
    let rd = match std::fs::read_dir(dir) {
        Ok(r) => r,
        Err(_) => return,
    };

    let mut entries: Vec<PathBuf> = Vec::new();
    for ent in rd {
        if let Ok(ent) = ent {
            entries.push(ent.path());
        }
    }
    entries.sort_by(|a, b| a.to_string_lossy().cmp(&b.to_string_lossy()));

    for p in entries {
        let md = match std::fs::metadata(&p) {
            Ok(m) => m,
            Err(_) => continue,
        };
        if md.is_dir() {
            collect_bin_paths(&p, out);
            continue;
        }
        if !md.is_file() {
            continue;
        }
        if p.extension().map(|e| e == "bin").unwrap_or(false) {
            out.push(p);
        }
    }
}

fn find_answer_replay_log(store_root: &Path) -> (Hash32, Vec<u8>, ReplayLog) {
    let mut bins: Vec<PathBuf> = Vec::new();
    collect_bin_paths(store_root, &mut bins);

    // Deterministic: paths are collected in lexical order.
    for p in bins {
        let bytes = match std::fs::read(&p) {
            Ok(b) => b,
            Err(_) => continue,
        };
        let log = match ReplayLog::decode(&bytes) {
            Ok(l) => l,
            Err(_) => continue,
        };
        if log.steps.iter().any(|s| s.name == "answer-v1") {
            let h = blake3_hash(&bytes);
            return (h, bytes, log);
        }
    }

    panic!("answer-v1 replay log not found in store");
}

#[test]
fn answer_emits_replay_log_and_evidence_set_artifacts() {
    let root = tmp_root("answer_emits_replay_log_and_evidence_set_artifacts");
    let store = FsArtifactStore::new(&root).unwrap();

    // Build a minimal FrameSegment + IndexSegment + IndexSnapshot.
    let terms = term_freqs_from_text("banana", TokenizerCfg::default());
    let mut row = FrameRowV1::new(DocId(Id64(1)), SourceId(Id64(1)));
    row.terms = terms;
    row.recompute_doc_len();

    let frame_seg = FrameSegmentV1::from_rows(&[row], 1024).unwrap();
    let frame_hash = put_frame_segment_v1(&store, &frame_seg).unwrap();

    let idx_seg = IndexSegmentV1::build_from_segment(frame_hash, &frame_seg).unwrap();
    let idx_hash = put_index_segment_v1(&store, &idx_seg).unwrap();

    let mut snap = IndexSnapshotV1::new(SourceId(Id64(1)));
    snap.entries.push(IndexSnapshotEntryV1 {
        frame_seg: frame_hash,
        index_seg: idx_hash,
        row_count: idx_seg.row_count,
        term_count: idx_seg.terms.len() as u32,
        postings_bytes: idx_seg.postings.len() as u32,
    });
    let snap_hash = put_index_snapshot_v1(&store, &snap).unwrap();

    // Prompt pack with a user message.
    let ids = PromptIds {
        snapshot_id: [0u8; 32],
        weights_id: [0u8; 32],
        tokenizer_id: [0u8; 32],
    };
    let mut pack = PromptPack::new(123, 256, ids);
    pack.messages.push(Message {
        role: Role::User,
        content: "banana".to_string(),
    });
    let prompt_hash = put_prompt_pack(&store, &mut pack, PromptLimits::default_v1()).unwrap();

    // Run the CLI.
    let out_path = root.join("answer.txt");

    let bin = env!("CARGO_BIN_EXE_fsa_lm");
    let out = Command::new(bin)
        .args([
            "answer",
            "--root",
            root.to_str().unwrap(),
            "--prompt",
            &hex32(&prompt_hash),
            "--snapshot",
            &hex32(&snap_hash),
            "--out-file",
            out_path.to_str().unwrap(),
        ])
        .output()
        .unwrap();

    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let answer_text = std::fs::read_to_string(&out_path).unwrap();
    let answer_hash = blake3_hash(answer_text.as_bytes());

    // Find the ReplayLog artifact and validate its deterministic links.
    let (replay_hash, replay_bytes, replay) = find_answer_replay_log(&root);

    let stored_replay = store
        .get(&replay_hash)
        .unwrap()
        .expect("replay log artifact");
    assert_eq!(stored_replay, replay_bytes);

    //: planner guidance steps are recorded alongside answer.
    // This smoke test does not include pragmatics, so expect:
    // - planner-hints-v1
    // - forecast-v1
    // - answer-v1
    // - markov-trace-v1
    assert_eq!(
        replay.steps.len(),
        4,
        "steps={:?}",
        replay
            .steps
            .iter()
            .map(|s| s.name.as_str())
            .collect::<Vec<_>>()
    );

    // Stable order: planner-hints -> forecast -> answer -> markov-trace.
    assert_eq!(replay.steps[0].name, "planner-hints-v1");
    assert_eq!(replay.steps[1].name, "forecast-v1");
    assert_eq!(replay.steps[2].name, "answer-v1");
    assert_eq!(replay.steps[3].name, "markov-trace-v1");

    let ph_step = replay
        .steps
        .iter()
        .find(|s| s.name == "planner-hints-v1")
        .unwrap();
    let fc_step = replay
        .steps
        .iter()
        .find(|s| s.name == "forecast-v1")
        .unwrap();
    let step = replay.steps.iter().find(|s| s.name == "answer-v1").unwrap();
    let mt_step = replay
        .steps
        .iter()
        .find(|s| s.name == "markov-trace-v1")
        .unwrap();

    assert_eq!(ph_step.outputs.len(), 1);
    assert_eq!(fc_step.outputs.len(), 1);
    assert_eq!(step.outputs.len(), 2, "outputs={:?}", step.outputs);
    assert_eq!(mt_step.outputs.len(), 1);

    // Inputs should include prompt and snapshot.
    assert!(step.inputs.contains(&prompt_hash));
    assert!(step.inputs.contains(&snap_hash));

    let ph_hash = ph_step.outputs[0];
    let fc_hash = fc_step.outputs[0];

    // Find the evidence bundle hash by decoding candidates.
    let mut ev_hash: Option<Hash32> = None;
    for h in &step.inputs {
        if *h == prompt_hash || *h == snap_hash || *h == ph_hash || *h == fc_hash {
            continue;
        }
        let Some(bytes) = store.get(h).unwrap() else {
            continue;
        };
        if let Ok(ev) = EvidenceBundleV1::decode(&bytes) {
            assert_eq!(ev.snapshot_id, snap_hash);
            ev_hash = Some(*h);
            break;
        }
    }
    let ev_hash = ev_hash.expect("evidence bundle hash in inputs");

    // Outputs should include the answer text hash and the evidence set hash.
    assert!(step.outputs.contains(&answer_hash));

    let set_hash = step
        .outputs
        .iter()
        .copied()
        .find(|h| *h != answer_hash)
        .expect("evidence set hash in outputs");

    // Planner hints artifact exists and decodes.
    let ph_bytes = store
        .get(&ph_hash)
        .unwrap()
        .expect("planner hints artifact");
    let _ph = PlannerHintsV1::decode(&ph_bytes).unwrap();
    assert!(ph_step.inputs.contains(&ev_hash));

    // Forecast artifact exists and decodes.
    let fc_bytes = store.get(&fc_hash).unwrap().expect("forecast artifact");
    let _fc = ForecastV1::decode(&fc_bytes).unwrap();
    assert!(fc_step.inputs.contains(&ph_hash));

    // Answer step should include guidance hashes as dependencies.
    assert!(step.inputs.contains(&ph_hash));
    assert!(step.inputs.contains(&fc_hash));

    // Markov trace step binds answer + guidance to a token stream artifact.
    assert!(mt_step.inputs.contains(&answer_hash));
    assert!(mt_step.inputs.contains(&ph_hash));
    assert!(mt_step.inputs.contains(&fc_hash));

    let mt_hash = mt_step.outputs[0];
    let mt_bytes = store.get(&mt_hash).unwrap().expect("markov trace artifact");
    let mt = MarkovTraceV1::decode(&mt_bytes).unwrap();
    assert_eq!(mt.version, 1);
    assert!(!mt.tokens.is_empty());

    // Evidence set artifact exists, decodes, and points to the bundle.
    let set_bytes = store
        .get(&set_hash)
        .unwrap()
        .expect("evidence set artifact");
    let set = EvidenceSetV1::decode(&set_bytes).unwrap();
    assert_eq!(set.evidence_bundle_id, ev_hash);
    assert_eq!(set.items.len(), 1);
    assert_eq!(set.items[0].claim_id, 1);
    assert_eq!(set.items[0].claim_text, answer_text);
    assert!(!set.items[0].evidence_refs.is_empty());

    // Answer artifact exists.
    let ans_bytes = store.get(&answer_hash).unwrap().expect("answer artifact");
    assert_eq!(ans_bytes, answer_text.as_bytes());
}

#[test]
fn answer_with_pragmatics_emits_directives_and_guidance_steps_and_verifies_trace() {
    // Keep the temp path short for Windows.
    let root = tmp_root("answer_pragmatics_replay");
    let store = FsArtifactStore::new(&root).unwrap();

    // Build a minimal FrameSegment + IndexSegment + IndexSnapshot.
    let terms = term_freqs_from_text("banana", TokenizerCfg::default());
    let mut row = FrameRowV1::new(DocId(Id64(1)), SourceId(Id64(1)));
    row.terms = terms;
    row.recompute_doc_len();

    let frame_seg = FrameSegmentV1::from_rows(&[row], 1024).unwrap();
    let frame_hash = put_frame_segment_v1(&store, &frame_seg).unwrap();

    let idx_seg = IndexSegmentV1::build_from_segment(frame_hash, &frame_seg).unwrap();
    let idx_hash = put_index_segment_v1(&store, &idx_seg).unwrap();

    let mut snap = IndexSnapshotV1::new(SourceId(Id64(1)));
    snap.entries.push(IndexSnapshotEntryV1 {
        frame_seg: frame_hash,
        index_seg: idx_hash,
        row_count: idx_seg.row_count,
        term_count: idx_seg.terms.len() as u32,
        postings_bytes: idx_seg.postings.len() as u32,
    });
    let snap_hash = put_index_snapshot_v1(&store, &snap).unwrap();

    // Prompt pack with a user message.
    let ids = PromptIds {
        snapshot_id: [0u8; 32],
        weights_id: [0u8; 32],
        tokenizer_id: [0u8; 32],
    };
    let mut pack = PromptPack::new(123, 256, ids);
    pack.messages.push(Message {
        role: Role::User,
        content: "banana".to_string(),
    });
    let prompt_hash = put_prompt_pack(&store, &mut pack, PromptLimits::default_v1()).unwrap();

    // Minimal pragmatics frame.
    let pf = PragmaticsFrameV1 {
        version: PRAGMATICS_FRAME_V1_VERSION,
        source_id: Id64(1),
        msg_ix: 0,
        byte_len: 6,
        ascii_only: 1,
        temperature: 0,
        valence: 0,
        arousal: 0,
        politeness: 800,
        formality: 200,
        directness: 700,
        empathy_need: 600,
        mode: RhetoricModeV1::Ask,
        flags: INTENT_FLAG_HAS_CODE,
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
    let prag_hash = put_pragmatics_frame_v1(&store, &pf).unwrap();

    // Run the CLI with --verify-trace.
    let out_path = root.join("answer.txt");
    let bin = env!("CARGO_BIN_EXE_fsa_lm");
    let out = Command::new(bin)
        .args([
            "answer",
            "--root",
            root.to_str().unwrap(),
            "--prompt",
            &hex32(&prompt_hash),
            "--snapshot",
            &hex32(&snap_hash),
            "--pragmatics",
            &hex32(&prag_hash),
            "--verify-trace",
            "1",
            "--out-file",
            out_path.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let answer_text = std::fs::read_to_string(&out_path).unwrap();
    let answer_hash = blake3_hash(answer_text.as_bytes());

    // Find the ReplayLog artifact and validate its deterministic links.
    let (_replay_hash, _replay_bytes, replay) = find_answer_replay_log(&root);
    assert_eq!(
        replay.steps.len(),
        5,
        "steps={:?}",
        replay
            .steps
            .iter()
            .map(|s| s.name.as_str())
            .collect::<Vec<_>>()
    );
    assert_eq!(replay.steps[0].name, "realizer-directives-v1");
    assert_eq!(replay.steps[1].name, "planner-hints-v1");
    assert_eq!(replay.steps[2].name, "forecast-v1");
    assert_eq!(replay.steps[3].name, "answer-v1");
    assert_eq!(replay.steps[4].name, "markov-trace-v1");

    let d_step = &replay.steps[0];
    let ph_step = &replay.steps[1];
    let fc_step = &replay.steps[2];
    let ans_step = &replay.steps[3];
    let mt_step = &replay.steps[4];

    assert_eq!(d_step.outputs.len(), 1);
    assert_eq!(ph_step.outputs.len(), 1);
    assert_eq!(fc_step.outputs.len(), 1);
    assert_eq!(ans_step.outputs.len(), 2);
    assert_eq!(mt_step.outputs.len(), 1);

    // Directives step binds pragmatics -> directives.
    assert!(d_step.inputs.contains(&prag_hash));
    let directives_hash = d_step.outputs[0];

    // Planner hints step binds pragmatics + evidence -> planner hints.
    assert!(ph_step.inputs.contains(&prag_hash));
    let ph_hash = ph_step.outputs[0];

    // Evidence bundle hash should be the other input (besides pragmatics).
    assert_eq!(ph_step.inputs.len(), 2, "ph_inputs={:?}", ph_step.inputs);
    let ev_hash = if ph_step.inputs[0] == prag_hash {
        ph_step.inputs[1]
    } else {
        ph_step.inputs[0]
    };
    assert_ne!(ev_hash, prag_hash);

    // Forecast step binds pragmatics + planner hints -> forecast.
    assert!(fc_step.inputs.contains(&prag_hash));
    assert!(fc_step.inputs.contains(&ph_hash));
    let fc_hash = fc_step.outputs[0];

    // Answer step includes prompt/snapshot and guidance + directives hashes.
    assert!(ans_step.inputs.contains(&prompt_hash));
    assert!(ans_step.inputs.contains(&snap_hash));
    assert!(ans_step.inputs.contains(&prag_hash));
    assert!(ans_step.inputs.contains(&ev_hash));
    assert!(ans_step.inputs.contains(&directives_hash));
    assert!(ans_step.inputs.contains(&ph_hash));
    assert!(ans_step.inputs.contains(&fc_hash));

    // Answer outputs include the answer text hash.
    assert!(ans_step.outputs.contains(&answer_hash));

    // Markov trace step binds answer + guidance to a token stream artifact.
    assert!(mt_step.inputs.contains(&answer_hash));
    assert!(mt_step.inputs.contains(&directives_hash));
    assert!(mt_step.inputs.contains(&ph_hash));
    assert!(mt_step.inputs.contains(&fc_hash));

    let mt_hash = mt_step.outputs[0];
    let mt_bytes = store.get(&mt_hash).unwrap().expect("markov trace artifact");
    let mt = MarkovTraceV1::decode(&mt_bytes).unwrap();
    assert_eq!(mt.version, 1);
    assert!(!mt.tokens.is_empty());
}
