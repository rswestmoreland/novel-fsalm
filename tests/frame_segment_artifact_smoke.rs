// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use fsa_lm::artifact::FsArtifactStore;
use fsa_lm::frame::{derive_id64, DocId, FrameRowV1, SourceId};
use fsa_lm::frame_segment::FrameSegmentV1;
use fsa_lm::frame_store::{get_frame_segment_v1, put_frame_segment_v1};
use fsa_lm::hash::blake3_hash;
use fsa_lm::tokenizer::{term_freqs_from_text, TokenizerCfg};

#[test]
fn frame_segment_store_round_trip() {
    let dir = std::env::temp_dir()
        .join("fsa_lm_tests")
        .join("frame_segment_store_round_trip");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let store = FsArtifactStore::new(&dir).unwrap();

    let tok_cfg = TokenizerCfg::default();

    let text1 = "Knights are brave.";
    let text2 = "Night is quiet.";

    let mut r1 = FrameRowV1::new(
        DocId(derive_id64(b"doc\0", text1.as_bytes())),
        SourceId(derive_id64(b"src\0", b"test")),
    );
    r1.terms = term_freqs_from_text(text1, tok_cfg);
    r1.recompute_doc_len();

    let mut r2 = FrameRowV1::new(
        DocId(derive_id64(b"doc\0", text2.as_bytes())),
        SourceId(derive_id64(b"src\0", b"test")),
    );
    r2.terms = term_freqs_from_text(text2, tok_cfg);
    r2.recompute_doc_len();

    let rows = vec![r1, r2];

    // Force small chunks to cover multi-chunk behavior.
    let seg = FrameSegmentV1::from_rows(&rows, 1).unwrap();

    let h1 = put_frame_segment_v1(&store, &seg).unwrap();
    let h2 = put_frame_segment_v1(&store, &seg).unwrap();
    assert_eq!(h1, h2);

    let seg2 = get_frame_segment_v1(&store, &h1).unwrap().unwrap();
    assert_eq!(seg, seg2);

    let missing = blake3_hash(b"missing_frame_segment");
    let none = get_frame_segment_v1(&store, &missing).unwrap();
    assert!(none.is_none());
}
