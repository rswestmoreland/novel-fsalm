// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use std::path::{Path, PathBuf};
use std::process::Command;

use fsa_lm::artifact::{ArtifactStore, FsArtifactStore};
use fsa_lm::frame::{DocId, FrameRowV1, Id64, SourceId};
use fsa_lm::frame_segment::FrameSegmentV1;
use fsa_lm::frame_store::put_frame_segment_v1;
use fsa_lm::hash::{blake3_hash, hex32, parse_hash32_hex, Hash32};
use fsa_lm::hit_list::HitListV1;
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

fn find_retrieve_replay_log(store_root: &Path) -> (Hash32, Vec<u8>, ReplayLog) {
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
        if log.steps.iter().any(|s| s.name == "retrieve-v1") {
            let h = blake3_hash(&bytes);
            return (h, bytes, log);
        }
    }

    panic!("retrieve-v1 replay log not found in store");
}

#[test]
fn query_index_emits_replay_log_and_hit_list() {
    let root = tmp_root("query_index_emits_replay_log_and_hit_list");
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
            "query-index",
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

    // Find the ReplayLog artifact.
    let (replay_hash, replay_bytes, replay) = find_retrieve_replay_log(&root);

    let stored_replay = store.get(&replay_hash).unwrap().expect("replay log artifact");
    assert_eq!(stored_replay, replay_bytes);

    assert_eq!(replay.steps.len(), 1);
    let step = replay.steps.iter().find(|s| s.name == "retrieve-v1").unwrap();

    // Inputs: snapshot + query-id blob (and no sig map in this test).
    assert_eq!(step.inputs.len(), 2, "inputs={:?}", step.inputs);
    assert!(step.inputs.contains(&snap_hash));

    // Recompute the expected query-id blob hash and validate it is referenced.
    let include_meta: u8 = 0;
    let k_u32: u32 = 10;
    let entry_cap_u32: u32 = 0;
    let dense_row_threshold: u32 = 200_000;

    let mut qid_bytes: Vec<u8> = Vec::new();
    qid_bytes.extend_from_slice(b"retrieve-v1\0");
    qid_bytes.push(include_meta);
    qid_bytes.extend_from_slice(&k_u32.to_le_bytes());
    qid_bytes.extend_from_slice(&entry_cap_u32.to_le_bytes());
    qid_bytes.extend_from_slice(&dense_row_threshold.to_le_bytes());
    qid_bytes.extend_from_slice(b"banana");

    let qid_hash = blake3_hash(&qid_bytes);

    assert!(step.inputs.contains(&qid_hash));

    // Outputs: HitList hash.
    assert_eq!(step.outputs.len(), 1);
    let hit_list_hash = step.outputs[0];

    // Query-id blob artifact exists and matches.
    let stored_qid = store.get(&qid_hash).unwrap().expect("query-id blob artifact");
    assert_eq!(stored_qid, qid_bytes);

    // HitList artifact exists and decodes.
    let hl_bytes = store.get(&hit_list_hash).unwrap().expect("hit list artifact");
    assert_eq!(blake3_hash(&hl_bytes), hit_list_hash);
    let hl = HitListV1::decode(&hl_bytes).unwrap();

    assert_eq!(hl.query_id, qid_hash);
    assert_eq!(hl.snapshot_id, snap_hash);
    assert!(hl.tie_control_id.is_none());
    assert_eq!(hl.hits.len(), 1);
    assert_eq!(hl.hits[0].frame_seg, frame_hash);
    assert_eq!(hl.hits[0].row_ix, 0);
    assert!(hl.hits[0].score > 0);

    // CLI stdout contains hit lines.
    let stdout = String::from_utf8_lossy(&out.stdout);
    let first = stdout.lines().next().unwrap_or("");
    assert!(first.contains(&hex32(&frame_hash)), "stdout={}", stdout);

    // Ensure the output is a valid triple.
    let parts: Vec<&str> = first.trim().split('\t').collect();
    assert_eq!(parts.len(), 3);
    let _score = parts[0].parse::<u64>().unwrap();
    let _seg = parse_hash32_hex(parts[1]).unwrap();
    let _row = parts[2].parse::<u32>().unwrap();
}
