// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use std::path::PathBuf;

use fsa_lm::artifact::FsArtifactStore;
use fsa_lm::lexicon::{LemmaRowV1, SenseRowV1};
use fsa_lm::lexicon_segment::LexiconSegmentV1;
use fsa_lm::lexicon_segment_store::put_lexicon_segment_v1;
use fsa_lm::lexicon_snapshot_builder::build_lexicon_snapshot_v1_from_segments;
use fsa_lm::lexicon_snapshot_store::put_lexicon_snapshot_v1;
use fsa_lm::lexicon_snapshot_validate::{
    validate_lexicon_snapshot_v1_disjoint_owners, LexiconSnapshotValidateError,
};

fn mk_tmp_dir(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push("novel_fsalm_tests");
    p.push(name);
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

#[test]
fn lexicon_snapshot_validate_disjoint_ok() {
    let td = mk_tmp_dir("lexicon_snapshot_validate_disjoint_ok");
    let store = FsArtifactStore::new(&td).unwrap();

    let l1 = LemmaRowV1::new("cat", 1, 0);
    let l2 = LemmaRowV1::new("dog", 1, 0);

    let seg1 = LexiconSegmentV1::build_from_rows(&[l1.clone()], &[], &[], &[]).unwrap();
    let seg2 = LexiconSegmentV1::build_from_rows(&[l2.clone()], &[], &[], &[]).unwrap();

    let h1 = put_lexicon_segment_v1(&store, &seg1).unwrap();
    let h2 = put_lexicon_segment_v1(&store, &seg2).unwrap();

    let (snap_hash, snap) = build_lexicon_snapshot_v1_from_segments(&store, &[h1, h2]).unwrap();
    let _ = put_lexicon_snapshot_v1(&store, &snap).unwrap();

    validate_lexicon_snapshot_v1_disjoint_owners(&store, &snap_hash).unwrap();
}

#[test]
fn lexicon_snapshot_validate_detects_overlap() {
    let td = mk_tmp_dir("lexicon_snapshot_validate_detects_overlap");
    let store = FsArtifactStore::new(&td).unwrap();

    // Two segments that both claim ownership of the same lemma id.
    // Ensure they have different hashes by adding an extra sense row to one segment.
    let l = LemmaRowV1::new("cat", 1, 0);
    let l_dup = LemmaRowV1::new("cat", 1, 0);
    assert_eq!(l.lemma_id, l_dup.lemma_id);

    let seg_a = LexiconSegmentV1::build_from_rows(&[l.clone()], &[], &[], &[]).unwrap();

    let s1 = SenseRowV1::new(l_dup.lemma_id, 0, "a small feline", 0);
    let seg_b = LexiconSegmentV1::build_from_rows(&[l_dup.clone()], &[s1], &[], &[]).unwrap();

    let ha = put_lexicon_segment_v1(&store, &seg_a).unwrap();
    let hb = put_lexicon_segment_v1(&store, &seg_b).unwrap();
    assert_ne!(ha, hb);

    let (snap_hash, snap) = build_lexicon_snapshot_v1_from_segments(&store, &[ha, hb]).unwrap();
    let _ = put_lexicon_snapshot_v1(&store, &snap).unwrap();

    let err = validate_lexicon_snapshot_v1_disjoint_owners(&store, &snap_hash).unwrap_err();
    match err {
        LexiconSnapshotValidateError::OverlappingLemmaOwner {
            lemma_id,
            segment_a,
            segment_b,
        } => {
            assert_eq!(lemma_id, l.lemma_id);
            assert_ne!(segment_a, segment_b);
            assert!(
                (segment_a == ha && segment_b == hb) || (segment_a == hb && segment_b == ha),
                "expected overlap to reference both segment hashes"
            );
        }
        other => panic!("unexpected error: {}", other),
    }

}
