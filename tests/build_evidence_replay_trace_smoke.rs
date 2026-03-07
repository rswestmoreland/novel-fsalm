// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use std::path::{Path, PathBuf};
use std::process::Command;

use fsa_lm::artifact::{ArtifactStore, FsArtifactStore};
use fsa_lm::evidence_bundle::EvidenceBundleV1;
use fsa_lm::frame::{DocId, FrameRowV1, Id64, SourceId};
use fsa_lm::frame_segment::FrameSegmentV1;
use fsa_lm::frame_store::put_frame_segment_v1;
use fsa_lm::hash::{blake3_hash, hex32, parse_hash32_hex, Hash32};
use fsa_lm::index_segment::IndexSegmentV1;
use fsa_lm::index_snapshot::{IndexSnapshotEntryV1, IndexSnapshotV1};
use fsa_lm::index_snapshot_store::put_index_snapshot_v1;
use fsa_lm::index_store::put_index_segment_v1;
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

fn find_build_evidence_replay_log(store_root: &Path) -> (Hash32, Vec<u8>, ReplayLog) {
    let mut bins: Vec<PathBuf> = Vec::new();
    collect_bin_paths(store_root, &mut bins);

    for p in bins {
        let bytes = match std::fs::read(&p) {
            Ok(b) => b,
            Err(_) => continue,
        };
        let log = match ReplayLog::decode(&bytes) {
            Ok(l) => l,
            Err(_) => continue,
        };
        if log.steps.iter().any(|s| s.name == "build-evidence-v1") {
            let h = blake3_hash(&bytes);
            return (h, bytes, log);
        }
    }

    panic!("build-evidence-v1 replay log not found in store");
}

#[test]
fn build_evidence_emits_replay_log_with_query_id_blob() {
    let root = tmp_root("build_evidence_emits_replay_log_with_query_id_blob");
    let store = FsArtifactStore::new(&root).unwrap();

    // Minimal FrameSegment + IndexSegment + IndexSnapshot.
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

    // Run the CLI.
    let bin = env!("CARGO_BIN_EXE_fsa_lm");
    let out = Command::new(bin)
        .args([
            "build-evidence",
            "--root",
            root.to_str().unwrap(),
            "--snapshot",
            &hex32(&snap_hash),
            "--text",
            "banana",
        ])
        .output()
        .unwrap();

    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));

    let stdout = String::from_utf8_lossy(&out.stdout);
    let ev_hex = stdout.lines().next().unwrap_or("").trim();
    let ev_hash = parse_hash32_hex(ev_hex).unwrap();

    // Find the ReplayLog artifact.
    let (replay_hash, replay_bytes, replay) = find_build_evidence_replay_log(&root);

    let stored_replay = store.get(&replay_hash).unwrap().expect("replay log artifact");
    assert_eq!(stored_replay, replay_bytes);

    assert_eq!(replay.steps.len(), 1);
    let step = replay.steps.iter().find(|s| s.name == "build-evidence-v1").unwrap();

    // Inputs: snapshot + query-id blob (and no sig map in this test).
    assert_eq!(step.inputs.len(), 2, "inputs={:?}", step.inputs);
    assert!(step.inputs.contains(&snap_hash));

    // Recompute the expected query-id blob hash and validate it is referenced.
    let include_meta: u8 = 0;
    let no_sketch: u8 = 0;
    let no_verify: u8 = 0;
    let k_u32: u32 = 10;
    let score_model_id: u32 = 0;
    let max_items: u32 = 10;
    let max_bytes: u32 = 64 * 1024;

    let mut qid_bytes: Vec<u8> = Vec::new();
    qid_bytes.extend_from_slice(b"build-evidence-v1\0");
    qid_bytes.push(include_meta);
    qid_bytes.push(no_sketch);
    qid_bytes.push(no_verify);
    qid_bytes.extend_from_slice(&k_u32.to_le_bytes());
    qid_bytes.extend_from_slice(&score_model_id.to_le_bytes());
    qid_bytes.extend_from_slice(&max_items.to_le_bytes());
    qid_bytes.extend_from_slice(&max_bytes.to_le_bytes());
    qid_bytes.extend_from_slice(b"banana");

    let qid_hash = blake3_hash(&qid_bytes);

    assert!(step.inputs.contains(&qid_hash));
    assert_eq!(step.outputs, vec![ev_hash]);

    // Query-id blob artifact exists and matches.
    let stored_qid = store.get(&qid_hash).unwrap().expect("query-id blob artifact");
    assert_eq!(stored_qid, qid_bytes);

    // Evidence bundle artifact exists and decodes.
    let ev_bytes = store.get(&ev_hash).unwrap().expect("evidence bundle artifact");
    let ev = EvidenceBundleV1::decode(&ev_bytes).unwrap();
    assert_eq!(ev.snapshot_id, snap_hash);
}
