// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use std::process::Command;

use fsa_lm::artifact::FsArtifactStore;
use fsa_lm::frame::{DocId, FrameRowV1, Id64, SourceId, TermFreq, TermId};
use fsa_lm::frame_segment::FrameSegmentV1;
use fsa_lm::frame_store::put_frame_segment_v1;
use fsa_lm::hash::Hash32;
use fsa_lm::index_sig_map_store::get_index_sig_map_v1;
use fsa_lm::index_snapshot_store::get_index_snapshot_v1;
use fsa_lm::segment_sig_store::get_segment_sig_v1;

fn from_hex_nybble(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        b'A'..=b'F' => c - b'A' + 10,
        _ => panic!("invalid hex"),
    }
}

fn parse_hex32(s: &str) -> Hash32 {
    let t = s.trim();
    assert_eq!(t.len(), 64, "expected 64 hex chars");
    let b = t.as_bytes();
    let mut out = [0u8; 32];
    for i in 0..32 {
        let hi = from_hex_nybble(b[i * 2]) << 4;
        let lo = from_hex_nybble(b[i * 2 + 1]);
        out[i] = hi | lo;
    }
    out
}

fn parse_sig_map_from_stderr(stderr: &str) -> Hash32 {
    for line in stderr.lines() {
        if let Some(rest) = line.strip_prefix("index_sig_map=") {
            return parse_hex32(rest);
        }
    }
    panic!("index_sig_map=... not found in stderr")
}

#[test]
fn build_index_emits_segment_sigs_and_sig_map() {
    let mut root = std::env::temp_dir();
    root.push("novel_fsalm_test_build_index_sig_map");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    let store = FsArtifactStore::new(&root).unwrap();

    // Build two tiny frame segments.
    let src = SourceId(Id64(1));
    let t1 = TermId(Id64(100));
    let t2 = TermId(Id64(101));
    let t3 = TermId(Id64(102));

    let mut r1 = FrameRowV1::new(DocId(Id64(10)), src);
    r1.terms = vec![TermFreq { term: t1, tf: 1 }, TermFreq { term: t2, tf: 2 }];
    r1.recompute_doc_len();

    let mut r2 = FrameRowV1::new(DocId(Id64(11)), src);
    r2.terms = vec![TermFreq { term: t2, tf: 1 }, TermFreq { term: t3, tf: 1 }];
    r2.recompute_doc_len();

    let seg1 = FrameSegmentV1::from_rows(&[r1.clone()], 1024).unwrap();
    let seg2 = FrameSegmentV1::from_rows(&[r2.clone()], 1024).unwrap();

    let _seg1_id = put_frame_segment_v1(&store, &seg1).unwrap();
    let _seg2_id = put_frame_segment_v1(&store, &seg2).unwrap();

    // Run the CLI to build the index.
    let bin = env!("CARGO_BIN_EXE_fsa_lm");
    let out = Command::new(bin)
        .args(["build-index", "--root", root.to_str().unwrap()])
        .output()
        .unwrap();

    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);

    let snap_id = parse_hex32(stdout.trim());
    let sig_map_id = parse_sig_map_from_stderr(&stderr);

    let snap = get_index_snapshot_v1(&store, &snap_id)
        .unwrap()
        .expect("index snapshot");
    let sig_map = get_index_sig_map_v1(&store, &sig_map_id)
        .unwrap()
        .expect("index sig map");

    assert_eq!(sig_map.source_id, snap.source_id);

    // Every snapshot entry should have a signature mapping, and the signature should
    // refer back to the index artifact hash.
    for ent in &snap.entries {
        let sig_id = sig_map.lookup_sig(&ent.index_seg).expect("missing sig for entry");
        let sig = get_segment_sig_v1(&store, &sig_id)
            .unwrap()
            .expect("segment sig");
        assert_eq!(sig.index_seg, ent.index_seg);
    }
}
