// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

// Build evidence bundle equivalence: plain query vs gated query.
//
// This test validates that retrieval gating (Bloom-based skip) does not change
// returned hits nor the derived evidence bundle, while reducing index artifact
// decoding work.
//
// ASCII-only comments.

use fsa_lm::artifact::FsArtifactStore;
use fsa_lm::evidence_builder::{build_evidence_bundle_v1_from_hits, EvidenceBuildCfgV1};
use fsa_lm::evidence_bundle::EvidenceLimitsV1;
use fsa_lm::frame::{DocId, FrameRowV1, Id64, SourceId, TermFreq, TermId};
use fsa_lm::frame_segment::FrameSegmentV1;
use fsa_lm::frame_store::put_frame_segment_v1;
use fsa_lm::hash::{blake3_hash, Hash32};
use fsa_lm::index_query::{search_snapshot, search_snapshot_gated, QueryTerm, SearchCfg};
use fsa_lm::index_segment::IndexSegmentV1;
use fsa_lm::index_sig_map::IndexSigMapV1;
use fsa_lm::index_sig_map_store::put_index_sig_map_v1;
use fsa_lm::index_snapshot::{IndexSnapshotEntryV1, IndexSnapshotV1};
use fsa_lm::index_snapshot_store::put_index_snapshot_v1;
use fsa_lm::index_store::put_index_segment_v1;
use fsa_lm::segment_sig::SegmentSigV1;
use fsa_lm::segment_sig_store::put_segment_sig_v1;

use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(prefix: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    p.push(format!("fsa_lm_test_{}_{}", prefix, nanos));
    let _ = fs::remove_dir_all(&p);
    fs::create_dir_all(&p).unwrap();
    p
}

fn make_row(doc_id: u64, source_id: SourceId, terms: &[TermId]) -> FrameRowV1 {
    let mut r = FrameRowV1::new(DocId(Id64(doc_id)), source_id);
    r.terms = terms.iter().map(|t| TermFreq { term: *t, tf: 1 }).collect();
    r.recompute_doc_len();
    r
}

fn find_definite_miss(sig: &SegmentSigV1, avoid: &[TermId]) -> TermId {
    let mut x: u64 = 1;
    loop {
        let t = TermId(Id64(x));
        if avoid.iter().any(|a| *a == t) {
            x = x.saturating_add(1);
            continue;
        }
        if !sig.might_contain_term(t) {
            return t;
        }
        x = x.saturating_add(1);
        if x == 0 {
            panic!("exhausted TermId search space");
        }
    }
}

#[test]
fn build_evidence_bundle_plain_vs_gated_equivalent() {
    let td = temp_dir("build_evidence_gating_equivalence");
    let store = FsArtifactStore::new(&td).unwrap();

    let sid = SourceId(Id64(7777));

    // Build two one-row segments with disjoint term sets.
    // Segment signatures use a large Bloom to make false positives vanishingly unlikely.
    //
    // For determinism, we first build sig2, then select a TermId that sig2 definitively
    // does not contain, and inject that term into segment 1. Bloom filters have no false
    // negatives, so sig1 must contain the injected term.

    let other_terms: Vec<TermId> = (20_000u64..20_032u64).map(|x| TermId(Id64(x))).collect();
    let row2 = make_row(2, sid, &other_terms);
    let seg2 = FrameSegmentV1::from_rows(&[row2], 1024).unwrap();
    let seg2_hash = put_frame_segment_v1(&store, &seg2).unwrap();
    let idx2 = IndexSegmentV1::build_from_segment(seg2_hash, &seg2).unwrap();
    let idx2_hash = put_index_segment_v1(&store, &idx2).unwrap();

    let idx2_term_ids: Vec<TermId> = idx2.terms.iter().map(|e| e.term).collect();
    let sig2 = SegmentSigV1::build(idx2_hash, &idx2_term_ids, 2048, 7).unwrap();
    let sig2_hash = put_segment_sig_v1(&store, &sig2).unwrap();

    // Select a query term that sig2 definitively does not contain.
    let qterm = find_definite_miss(&sig2, &other_terms);
    assert!(!sig2.might_contain_term(qterm));

    // Segment 1 contains qterm plus some additional terms.
    let mut hit_terms: Vec<TermId> = (10_000u64..10_064u64).map(|x| TermId(Id64(x))).collect();
    hit_terms.push(qterm);
    let row1 = make_row(1, sid, &hit_terms);
    let seg1 = FrameSegmentV1::from_rows(&[row1], 1024).unwrap();
    let seg1_hash = put_frame_segment_v1(&store, &seg1).unwrap();
    let idx1 = IndexSegmentV1::build_from_segment(seg1_hash, &seg1).unwrap();
    let idx1_hash = put_index_segment_v1(&store, &idx1).unwrap();

    let idx1_term_ids: Vec<TermId> = idx1.terms.iter().map(|e| e.term).collect();
    let sig1 = SegmentSigV1::build(idx1_hash, &idx1_term_ids, 2048, 7).unwrap();
    let sig1_hash = put_segment_sig_v1(&store, &sig1).unwrap();

    assert!(sig1.might_contain_term(qterm));

    // Build index snapshot with both entries.
    let mut snap = IndexSnapshotV1::new(sid);
    snap.entries.push(IndexSnapshotEntryV1 {
        frame_seg: seg1_hash,
        index_seg: idx1_hash,
        row_count: idx1.row_count,
        term_count: idx1.terms.len() as u32,
        postings_bytes: idx1.postings.len() as u32,
    });
    snap.entries.push(IndexSnapshotEntryV1 {
        frame_seg: seg2_hash,
        index_seg: idx2_hash,
        row_count: idx2.row_count,
        term_count: idx2.terms.len() as u32,
        postings_bytes: idx2.postings.len() as u32,
    });
    let snap_hash = put_index_snapshot_v1(&store, &snap).unwrap();

    // Build sig map linking snapshot entries to segment signatures.
    let mut sig_map = IndexSigMapV1::new(sid);
    sig_map.push(idx1_hash, sig1_hash);
    sig_map.push(idx2_hash, sig2_hash);
    let sig_map_hash = put_index_sig_map_v1(&store, &sig_map).unwrap();

    // Plain and gated search must return identical hits.
    let qterms: Vec<QueryTerm> = vec![QueryTerm {
        term: qterm,
        qtf: 1,
    }];
    let mut scfg = SearchCfg::new();
    scfg.k = 64;
    scfg.entry_cap = 0;

    let hits_plain = search_snapshot(&store, &snap_hash, &qterms, &scfg).unwrap();
    let (hits_gated, gate) =
        search_snapshot_gated(&store, &snap_hash, &sig_map_hash, &qterms, &scfg).unwrap();

    assert_eq!(hits_plain, hits_gated);
    assert!(
        gate.entries_skipped_sig > 0,
        "expected at least one skipped entry"
    );

    // Build evidence from both hit lists and assert exact equivalence.
    let query_id: Hash32 = blake3_hash(b"test_query_id");
    let limits = EvidenceLimitsV1 {
        segments_touched: 0,
        max_items: 1_000,
        max_bytes: 1_000_000,
    };
    let cfg = EvidenceBuildCfgV1::new();
    let score_model_id: u32 = 1;

    let out_plain = build_evidence_bundle_v1_from_hits(
        &store,
        query_id,
        snap_hash,
        limits,
        score_model_id,
        &hits_plain,
        &cfg,
    )
    .unwrap();
    let out_gated = build_evidence_bundle_v1_from_hits(
        &store,
        query_id,
        snap_hash,
        limits,
        score_model_id,
        &hits_gated,
        &cfg,
    )
    .unwrap();

    assert_eq!(out_plain, out_gated);
}
