// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use fsa_lm::artifact::{ArtifactStore, FsArtifactStore};
use fsa_lm::evidence_artifact::{get_evidence_bundle_v1, put_evidence_bundle_v1};
use fsa_lm::evidence_builder::{build_evidence_bundle_v1_from_hits, EvidenceBuildCfgV1};
use fsa_lm::evidence_bundle::{EvidenceItemDataV1, EvidenceLimitsV1};
use fsa_lm::frame::{Id64, SourceId};
use fsa_lm::frame_store::get_frame_segment_v1;
use fsa_lm::hash::blake3_hash;
use fsa_lm::index_query::{query_terms_from_text, search_snapshot, QueryTermsCfg, SearchCfg};
use fsa_lm::index_segment::IndexSegmentV1;
use fsa_lm::index_snapshot::{IndexSnapshotEntryV1, IndexSnapshotV1};
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

fn row_count_for(entries: &[IndexSnapshotEntryV1], seg: &[u8; 32]) -> Option<u32> {
    for e in entries {
        if &e.frame_seg == seg {
            return Some(e.row_count);
        }
    }
    None
}

fn contains_seg(segs: &[[u8; 32]], seg: &[u8; 32]) -> bool {
    for s in segs {
        if s == seg {
            return true;
        }
    }
    false
}

#[test]
fn e2e_ingest_index_query_evidence_smoke() {
    let dir = tmp_dir("e2e_ingest_index_query_evidence_smoke");
    let store = FsArtifactStore::new(&dir).unwrap();

    // TSV format: title<TAB>text
    // Keep the texts short so each becomes a single row.
    let tsv = "DocOne\tbanana banana banana apple\nDocTwo\tbanana carrot\n";

    let cfg = WikiIngestCfg {
        source_id: SourceId(Id64(4242)),
        tok_cfg: fsa_lm::tokenizer::TokenizerCfg::default(),
        chunk_rows: 16,
        seg_rows: 64,
        row_max_bytes: 4096,
        max_docs: Some(2),
    };

    // Ingest -> manifest.
    let mh = ingest_wiki_tsv(&store, BufReader::new(Cursor::new(tsv.as_bytes())), cfg).unwrap();
    let mbytes = store.get(&mh).unwrap().unwrap();
    let manifest = WikiIngestManifestV1::decode(&mbytes).unwrap();
    assert_eq!(manifest.docs_total, 2);
    assert!(manifest.rows_total >= 2);
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
    snap.entries = entries.clone();
    let snap_bytes = snap.encode().unwrap();
    let snap_hash = store.put(&snap_bytes).unwrap();

    // Query.
    let mut qcfg = QueryTermsCfg::new();
    qcfg.include_metaphone = false;
    let qtext = "banana";
    let qterms = query_terms_from_text(qtext, &qcfg);

    let scfg = SearchCfg {
        k: 8,
        entry_cap: 0,
        dense_row_threshold: 200_000,
    };
    let hits = search_snapshot(&store, &snap_hash, &qterms, &scfg).unwrap();
    assert!(!hits.is_empty());

    // Evidence.
    let k_u32: u32 = 8;
    let score_model_id: u32 = 0;
    let max_items: u32 = k_u32;
    let max_bytes: u32 = 16 * 1024;

    let mut qid_bytes: Vec<u8> = Vec::new();
    qid_bytes.extend_from_slice(b"build-evidence-v1\0");
    qid_bytes.push(0); // include_meta
    qid_bytes.push(0); // no_sketch
    qid_bytes.push(0); // no_verify
    qid_bytes.extend_from_slice(&k_u32.to_le_bytes());
    qid_bytes.extend_from_slice(&score_model_id.to_le_bytes());
    qid_bytes.extend_from_slice(&max_items.to_le_bytes());
    qid_bytes.extend_from_slice(&max_bytes.to_le_bytes());
    qid_bytes.extend_from_slice(qtext.as_bytes());
    let query_id = blake3_hash(&qid_bytes);

    let limits = EvidenceLimitsV1 {
        segments_touched: 0,
        max_items,
        max_bytes,
    };
    let bcfg = EvidenceBuildCfgV1::new();

    let bundle = build_evidence_bundle_v1_from_hits(
        &store,
        query_id,
        snap_hash,
        limits,
        score_model_id,
        &hits,
        &bcfg,
    )
    .unwrap();

    let ev_hash = put_evidence_bundle_v1(&store, &bundle).unwrap();
    let got = get_evidence_bundle_v1(&store, &ev_hash).unwrap().unwrap();
    assert_eq!(got, bundle);

    // Validate frame references against the snapshot entries.
    for it in &bundle.items {
        match &it.data {
            EvidenceItemDataV1::Frame(fr) => {
                assert!(contains_seg(&manifest.segments, &fr.segment_id));
                let rc = row_count_for(&entries, &fr.segment_id).unwrap();
                assert!(fr.row_ix < rc);
            }
            EvidenceItemDataV1::Lexicon(_) => {
                panic!("unexpected lexicon evidence item in v1 smoke");
            }
            EvidenceItemDataV1::Proof(_) => {
                panic!("unexpected proof evidence item in v1 smoke");
            }
        }
    }

    // Determinism: re-run evidence build and ensure the canonical bytes hash is identical.
    let bundle2 = build_evidence_bundle_v1_from_hits(
        &store,
        query_id,
        snap_hash,
        limits,
        score_model_id,
        &hits,
        &bcfg,
    )
    .unwrap();
    assert_eq!(bundle2, bundle);
    let ev_hash2 = put_evidence_bundle_v1(&store, &bundle2).unwrap();
    assert_eq!(ev_hash2, ev_hash);

    // Also sanity-check that changing the query id changes the artifact hash, even if hits are the same.
    let alt_query_id = blake3_hash(b"alt-qid");
    let bundle3 = build_evidence_bundle_v1_from_hits(
        &store,
        alt_query_id,
        snap_hash,
        limits,
        score_model_id,
        &hits,
        &bcfg,
    )
    .unwrap();
    let ev_hash3 = put_evidence_bundle_v1(&store, &bundle3).unwrap();
    assert_ne!(ev_hash3, ev_hash);
}
