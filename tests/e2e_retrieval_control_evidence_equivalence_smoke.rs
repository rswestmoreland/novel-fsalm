// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use fsa_lm::artifact::{ArtifactStore, FsArtifactStore};
use fsa_lm::evidence_artifact::put_evidence_bundle_v1;
use fsa_lm::evidence_builder::{
    build_evidence_bundle_v1_from_hits_with_control, EvidenceBuildCfgV1,
};
use fsa_lm::evidence_bundle::EvidenceLimitsV1;
use fsa_lm::frame::{Id64, SourceId};
use fsa_lm::frame_store::get_frame_segment_v1;
use fsa_lm::hash::{blake3_hash, Hash32};
use fsa_lm::index_query::{
    query_terms_from_text, search_snapshot_with_control, QueryTermsCfg, SearchCfg, SearchHit,
};
use fsa_lm::index_segment::IndexSegmentV1;
use fsa_lm::index_snapshot::{IndexSnapshotEntryV1, IndexSnapshotV1};
use fsa_lm::retrieval_control::RetrievalControlV1;
use fsa_lm::wiki_ingest::{ingest_wiki_tsv, WikiIngestCfg, WikiIngestManifestV1};

use std::fs;
use std::io::{BufReader, Cursor};
use std::path::PathBuf;

fn tmp_dir(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push("fsa_lm_tests");
    // Include PID to avoid collisions under parallel test execution.
    p.push(format!("{}_{}", name, std::process::id()));
    let _ = fs::remove_dir_all(&p);
    fs::create_dir_all(&p).unwrap();
    p
}

fn hit_keys(hits: &[SearchHit]) -> Vec<(Hash32, u32)> {
    let mut out: Vec<(Hash32, u32)> = Vec::with_capacity(hits.len());
    for h in hits.iter() {
        out.push((h.frame_seg, h.row_ix));
    }
    out.sort_unstable();
    out
}

#[test]
fn e2e_retrieval_control_does_not_change_evidence_smoke() {
    let dir = tmp_dir("e2e_retrieval_control_evidence_equivalence_smoke");
    let store = FsArtifactStore::new(&dir).unwrap();

    // TSV format: title<TAB>text
    // Keep each doc to one short row with identical token stats so scores tie.
    let tsv = "DocOne\tbanana\nDocTwo\tbanana\nDocThree\tbanana\n";

    let cfg = WikiIngestCfg {
        source_id: SourceId(Id64(4242)),
        tok_cfg: fsa_lm::tokenizer::TokenizerCfg::default(),
        chunk_rows: 16,
        seg_rows: 64,
        row_max_bytes: 4096,
        max_docs: Some(3),
    };

    // Ingest -> manifest.
    let mh = ingest_wiki_tsv(&store, BufReader::new(Cursor::new(tsv.as_bytes())), cfg).unwrap();
    let mbytes = store.get(&mh).unwrap().unwrap();
    let manifest = WikiIngestManifestV1::decode(&mbytes).unwrap();
    assert_eq!(manifest.docs_total, 3);
    assert!(manifest.rows_total >= 3);
    assert!(!manifest.segments.is_empty());

    // Build index snapshot for produced segments.
    let mut entries: Vec<IndexSnapshotEntryV1> = Vec::new();
    for seg_hash in &manifest.segments {
        let seg = get_frame_segment_v1(&store, seg_hash).unwrap().unwrap();
        let idx = IndexSegmentV1::build_from_segment(*seg_hash, &seg).unwrap();

        let idx_bytes = idx.encode().unwrap();
        let idx_hash = store.put(&idx_bytes).unwrap();

        let term_count = if idx.terms.len() > (u32::MAX as usize) {
            u32::MAX
        } else {
            idx.terms.len() as u32
        };
        let postings_bytes = if idx.postings.len() > (u32::MAX as usize) {
            u32::MAX
        } else {
            idx.postings.len() as u32
        };

        entries.push(IndexSnapshotEntryV1 {
            frame_seg: *seg_hash,
            index_seg: idx_hash,
            row_count: idx.row_count,
            term_count,
            postings_bytes,
        });
    }

    let mut snap = IndexSnapshotV1::new(manifest.source_id);
    snap.entries = entries;
    let snap_bytes = snap.encode().unwrap();
    let snap_hash = store.put(&snap_bytes).unwrap();

    // Query.
    let mut qcfg = QueryTermsCfg::new();
    qcfg.include_metaphone = false;
    let qtext = "banana";
    let qterms = query_terms_from_text(qtext, &qcfg);

    // Use k=1 to force a cutoff inside an equal-score tie group.
    let scfg = SearchCfg {
        k: 1,
        entry_cap: 0,
        dense_row_threshold: 200_000,
    };

    let c1 = RetrievalControlV1::new(blake3_hash(b"prompt-a"));
    let c2 = RetrievalControlV1::new(blake3_hash(b"prompt-b"));
    assert!(c1.validate().is_ok());
    assert!(c2.validate().is_ok());

    let hits1 =
        search_snapshot_with_control(&store, &snap_hash, &qterms, &scfg, Some(&c1)).unwrap();
    let hits2 =
        search_snapshot_with_control(&store, &snap_hash, &qterms, &scfg, Some(&c2)).unwrap();

    // With control present, include_ties must expand to include all tied hits at the cutoff.
    assert!(hits1.len() >= 2);
    assert_eq!(hit_keys(&hits1), hit_keys(&hits2));

    // Evidence build config.
    let score_model_id: u32 = 0;
    let limits = EvidenceLimitsV1 {
        segments_touched: 0,
        max_items: 64,
        max_bytes: 16 * 1024,
    };
    let bcfg = EvidenceBuildCfgV1::new();

    // Keep query_id independent of control so evidence artifacts are comparable.
    let query_id = blake3_hash(b"e2e-control-evidence-equivalence\0banana");

    let bundle1 = build_evidence_bundle_v1_from_hits_with_control(
        &store,
        query_id,
        snap_hash,
        limits,
        score_model_id,
        &hits1,
        &bcfg,
        Some(&c1),
    )
    .unwrap();

    let bundle2 = build_evidence_bundle_v1_from_hits_with_control(
        &store,
        query_id,
        snap_hash,
        limits,
        score_model_id,
        &hits2,
        &bcfg,
        Some(&c2),
    )
    .unwrap();

    // Evidence must be identical even if control changes tie-break ordering.
    assert_eq!(bundle2, bundle1);

    let h1 = put_evidence_bundle_v1(&store, &bundle1).unwrap();
    let h2 = put_evidence_bundle_v1(&store, &bundle2).unwrap();
    assert_eq!(h2, h1);
}
