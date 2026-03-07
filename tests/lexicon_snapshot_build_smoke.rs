// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use fsa_lm::artifact::FsArtifactStore;
use fsa_lm::lexicon::{
    LemmaRowV1, PronunciationRowV1, RelFromId, RelTypeId, RelationEdgeRowV1, SenseRowV1, POS_NOUN,
};
use fsa_lm::lexicon_segment::LexiconSegmentV1;
use fsa_lm::lexicon_segment_store::put_lexicon_segment_v1;
use fsa_lm::lexicon_snapshot_builder::build_lexicon_snapshot_v1_from_segments;
use fsa_lm::lexicon_snapshot_store::get_lexicon_snapshot_v1;

#[test]
fn lexicon_snapshot_build_and_store_round_trip() {
    let dir = std::env::temp_dir()
        .join("fsa_lm_tests")
        .join("lexicon_snapshot_build_and_store_round_trip");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let store = FsArtifactStore::new(&dir).unwrap();

    // Segment A: 2 lemmas, no senses/rels/prons.
    let l1 = LemmaRowV1::new("Cat", POS_NOUN, 0);
    let l2 = LemmaRowV1::new("Dog", POS_NOUN, 0);
    let seg_a =
        LexiconSegmentV1::build_from_rows(&[l1.clone(), l2.clone()], &[], &[], &[]).unwrap();
    let h_a = put_lexicon_segment_v1(&store, &seg_a).unwrap();

    // Segment B: 1 lemma with 1 sense, 1 relation, 1 pronunciation.
    let l3 = LemmaRowV1::new("Knight", POS_NOUN, 0);
    let s1 = SenseRowV1::new(l3.lemma_id, 0, "armored warrior", 0);
    let r1 = RelationEdgeRowV1::new(RelFromId::Lemma(l3.lemma_id), RelTypeId(1u16), l3.lemma_id);
    let p1 = PronunciationRowV1::new(l3.lemma_id, "naIt", Vec::new(), 0);
    let seg_b = LexiconSegmentV1::build_from_rows(&[l3.clone()], &[s1], &[r1], &[p1]).unwrap();
    let h_b = put_lexicon_segment_v1(&store, &seg_b).unwrap();

    // Build snapshot with hashes in non-canonical order.
    let (snap_hash, _) = build_lexicon_snapshot_v1_from_segments(&store, &[h_b, h_a]).unwrap();

    let snap = get_lexicon_snapshot_v1(&store, &snap_hash)
        .unwrap()
        .unwrap();
    assert_eq!(snap.version, 1);
    assert_eq!(snap.entries.len(), 2);
    assert!(snap.entries[0].lex_seg < snap.entries[1].lex_seg);

    // Verify counts for each segment hash.
    for e in &snap.entries {
        if e.lex_seg == h_a {
            assert_eq!(e.lemma_count, 2);
            assert_eq!(e.sense_count, 0);
            assert_eq!(e.rel_count, 0);
            assert_eq!(e.pron_count, 0);
        } else if e.lex_seg == h_b {
            assert_eq!(e.lemma_count, 1);
            assert_eq!(e.sense_count, 1);
            assert_eq!(e.rel_count, 1);
            assert_eq!(e.pron_count, 1);
        } else {
            panic!("unexpected segment hash");
        }
    }

    // Determinism: building again with the same set yields the same snapshot hash.
    let (snap_hash2, _) = build_lexicon_snapshot_v1_from_segments(&store, &[h_a, h_b]).unwrap();
    assert_eq!(snap_hash, snap_hash2);
}
