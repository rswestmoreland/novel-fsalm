// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use fsa_lm::artifact::{ArtifactStore, FsArtifactStore};
use fsa_lm::evidence_builder::{build_evidence_bundle_v1_from_hits, EvidenceBuildCfgV1};
use fsa_lm::evidence_bundle::{EvidenceItemDataV1, EvidenceLimitsV1};
use fsa_lm::frame::{Id64, SourceId};
use fsa_lm::frame_store::get_frame_segment_v1;
use fsa_lm::hash::blake3_hash;
use fsa_lm::index_compaction::compact_index_snapshot_v1;
use fsa_lm::index_query::{query_terms_from_text, search_snapshot, QueryTermsCfg, SearchCfg};
use fsa_lm::index_segment::IndexSegmentV1;
use fsa_lm::index_snapshot::{IndexSnapshotEntryV1, IndexSnapshotV1};
use fsa_lm::index_snapshot_store::put_index_snapshot_v1;
use fsa_lm::index_store::put_index_segment_v1;
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

fn unique_count<T: Ord + Copy>(xs: &[T]) -> usize {
    let mut v: Vec<T> = xs.to_vec();
    v.sort();
    v.dedup();
    v.len()
}

#[test]
fn e2e_compact_index_preserves_query_hits_and_evidence_items() {
    let dir = tmp_dir("e2e_compact_index_equivalence_smoke");
    let store = FsArtifactStore::new(&dir).unwrap();

    // TSV format: title<TAB>text
    // Force multiple segments by using seg_rows = 1.
    let tsv = "DocOne\tbanana banana apple\nDocTwo\tbanana carrot\nDocThree\tcarrot apple\n";

    let cfg = WikiIngestCfg {
        source_id: SourceId(Id64(4242)),
        tok_cfg: fsa_lm::tokenizer::TokenizerCfg::default(),
        chunk_rows: 8,
        seg_rows: 1,
        row_max_bytes: 4096,
        max_docs: Some(3),
    };

    // Ingest -> manifest.
    let mh = ingest_wiki_tsv(&store, BufReader::new(Cursor::new(tsv.as_bytes())), cfg).unwrap();
    let mbytes = store.get(&mh).unwrap().unwrap();
    let manifest = WikiIngestManifestV1::decode(&mbytes).unwrap();
    assert_eq!(manifest.docs_total, 3);
    assert!(manifest.segments.len() >= 2);

    // Build index snapshot for produced segments.
    let mut entries: Vec<IndexSnapshotEntryV1> = Vec::new();
    for seg_hash in &manifest.segments {
        let seg = get_frame_segment_v1(&store, seg_hash).unwrap().unwrap();
        let idx = IndexSegmentV1::build_from_segment(*seg_hash, &seg).unwrap();
        let idx_hash = put_index_segment_v1(&store, &idx).unwrap();

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
    let snap_id = put_index_snapshot_v1(&store, &snap).unwrap();

    // Query before compaction.
    let mut qcfg = QueryTermsCfg::new();
    qcfg.include_metaphone = false;
    let qtext = "banana";
    let qterms = query_terms_from_text(qtext, &qcfg);

    let mut scfg = SearchCfg::new();
    scfg.k = 16;
    let hits_before = search_snapshot(&store, &snap_id, &qterms, &scfg).unwrap();
    assert!(!hits_before.is_empty());

    // Evidence before.
    let k_u32: u32 = 16;
    let score_model_id: u32 = 0;
    let max_items: u32 = k_u32;
    let max_bytes: u32 = 16 * 1024;

    let mut qid_bytes: Vec<u8> = Vec::new();
    qid_bytes.extend_from_slice(b"build-evidence-v1\0");
    qid_bytes.push(0);
    qid_bytes.push(0);
    qid_bytes.push(0);
    qid_bytes.extend_from_slice(&k_u32.to_le_bytes());
    qid_bytes.extend_from_slice(&score_model_id.to_le_bytes());
    qid_bytes.extend_from_slice(&max_items.to_le_bytes());
    qid_bytes.extend_from_slice(&max_bytes.to_le_bytes());
    qid_bytes.extend_from_slice(qtext.as_bytes());
    let query_id = blake3_hash(&qid_bytes);

    let limits = EvidenceLimitsV1 { segments_touched: 0, max_items, max_bytes };
    let bcfg = EvidenceBuildCfgV1::new();

    let bundle_before = build_evidence_bundle_v1_from_hits(
        &store,
        query_id,
        snap_id,
        limits,
        score_model_id,
        &hits_before,
        &bcfg,
    )
    .unwrap();

    // Compact snapshot into a single output pack (exercise IndexPack path).
    let comp_cfg = fsa_lm::compaction_report::CompactionCfgV1 {
        target_bytes_per_out_segment: 1024 * 1024,
        max_out_segments: 1,
        used_even_pack_fallback: false,
        dry_run: false,
    };
    let res = compact_index_snapshot_v1(&store, &snap_id, comp_cfg).unwrap();
    assert!(res.report_id.is_some());
    let out_snap_id = res.report.output_snapshot_id.unwrap();

    // Query after compaction.
    let hits_after = search_snapshot(&store, &out_snap_id, &qterms, &scfg).unwrap();
    assert_eq!(hits_after, hits_before);

    // Evidence after compaction should preserve items and scores; snapshot_id will differ.
    let bundle_after = build_evidence_bundle_v1_from_hits(
        &store,
        query_id,
        out_snap_id,
        limits,
        score_model_id,
        &hits_after,
        &bcfg,
    )
    .unwrap();

    assert_eq!(bundle_after.query_id, bundle_before.query_id);
    assert_eq!(bundle_after.score_model_id, bundle_before.score_model_id);
    assert_eq!(bundle_after.items, bundle_before.items);
    assert_ne!(bundle_after.snapshot_id, bundle_before.snapshot_id);
    assert_eq!(bundle_after.snapshot_id, out_snap_id);

    // Sanity: compacted snapshot should reference <= max_out_segments unique index hashes.
    let out_bytes = store.get(&out_snap_id).unwrap().unwrap();
    let out_snap = IndexSnapshotV1::decode(&out_bytes).unwrap();
    let mut segs: Vec<[u8; 32]> = Vec::with_capacity(out_snap.entries.len());
    for e in &out_snap.entries {
        segs.push(e.index_seg);
    }
    assert_eq!(unique_count(&segs), 1);

    // Validate evidence frame references still point at the ingested segments.
    for it in &bundle_after.items {
        match &it.data {
            EvidenceItemDataV1::Frame(fr) => {
                assert!(manifest.segments.iter().any(|h| h == &fr.segment_id));
            }
            EvidenceItemDataV1::Lexicon(_) => {
                panic!("unexpected lexicon evidence item in v1 compaction e2e");
            }
            EvidenceItemDataV1::Proof(_) => {
                panic!("unexpected proof evidence item in v1 compaction e2e");
            }
        }
    }
}
