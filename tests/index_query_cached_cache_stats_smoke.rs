// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use fsa_lm::artifact::{ArtifactResult, ArtifactStore, FsArtifactStore};
use fsa_lm::cache::{Cache2Q, CacheCfgV1};
use fsa_lm::frame::{derive_id64, DocId, FrameRowV1, SourceId};
use fsa_lm::frame_segment::FrameSegmentV1;
use fsa_lm::hash::Hash32;
use fsa_lm::index_query::{query_terms_from_text, search_snapshot_cached, QueryTermsCfg, SearchCfg};
use fsa_lm::index_segment::IndexSegmentV1;
use fsa_lm::index_snapshot::{IndexSnapshotEntryV1, IndexSnapshotV1};
use fsa_lm::index_snapshot_store::put_index_snapshot_v1;
use fsa_lm::index_store::put_index_segment_v1;
use fsa_lm::frame_store::put_frame_segment_v1;
use fsa_lm::tokenizer::{term_freqs_from_text, TokenizerCfg};

#[derive(Clone)]
struct CountingStore {
    inner: FsArtifactStore,
    get_calls: Arc<AtomicU64>,
}

impl CountingStore {
    fn new(inner: FsArtifactStore) -> Self {
        Self {
            inner,
            get_calls: Arc::new(AtomicU64::new(0)),
        }
    }

    fn reset_get_calls(&self) {
        self.get_calls.store(0, Ordering::Relaxed);
    }

    fn get_calls(&self) -> u64 {
        self.get_calls.load(Ordering::Relaxed)
    }
}

impl ArtifactStore for CountingStore {
    fn put(&self, bytes: &[u8]) -> ArtifactResult<Hash32> {
        self.inner.put(bytes)
    }

    fn get(&self, hash: &Hash32) -> ArtifactResult<Option<Vec<u8>>> {
        self.get_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.get(hash)
    }

    fn path_for(&self, hash: &Hash32) -> std::path::PathBuf {
        self.inner.path_for(hash)
    }
}

fn make_row(src: SourceId, doc: DocId, text: &str) -> FrameRowV1 {
    let mut r = FrameRowV1::new(doc, src);
    let tcfg = TokenizerCfg::default();
    r.terms = term_freqs_from_text(text, tcfg);
    r.recompute_doc_len();
    r
}

#[test]
fn index_query_cached_hits_avoid_second_store_read_and_increment_hits() {
    let dir = std::env::temp_dir()
        .join("fsa_lm_tests")
        .join("index_query_cached_hits_avoid_second_store_read_and_increment_hits");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let store = CountingStore::new(FsArtifactStore::new(&dir).unwrap());

    let src = SourceId(derive_id64(b"src\0", b"s1"));
    let d1 = DocId(derive_id64(b"doc\0", b"d1"));
    let d2 = DocId(derive_id64(b"doc\0", b"d2"));
    let d3 = DocId(derive_id64(b"doc\0", b"d3"));

    let rows = vec![
        make_row(src, d1, "alpha beta gamma"),
        make_row(src, d2, "alpha beta"),
        make_row(src, d3, "alpha"),
    ];

    let seg = FrameSegmentV1::from_rows(&rows, 1024).unwrap();
    let seg_hash = put_frame_segment_v1(&store, &seg).unwrap();

    let idx = IndexSegmentV1::build_from_segment(seg_hash, &seg).unwrap();
    let idx_hash = put_index_segment_v1(&store, &idx).unwrap();

    let mut snap = IndexSnapshotV1::new(src);
    snap.entries.push(IndexSnapshotEntryV1 {
        frame_seg: seg_hash,
        index_seg: idx_hash,
        row_count: idx.row_count,
        term_count: idx.terms.len() as u32,
        postings_bytes: idx.postings.len() as u32,
    });

    let snap_hash = put_index_snapshot_v1(&store, &snap).unwrap();

    store.reset_get_calls();

    let mut qcfg = QueryTermsCfg::new();
    qcfg.include_metaphone = false;
    let qterms = query_terms_from_text("alpha", &qcfg);

    let scfg = SearchCfg {
        k: 10,
        entry_cap: 0,
        dense_row_threshold: 200_000,
    };

    let mut snap_cache: Cache2Q<Hash32, Arc<IndexSnapshotV1>> = Cache2Q::new(CacheCfgV1::new(10_000_000));
    let mut idx_cache: Cache2Q<Hash32, Arc<IndexSegmentV1>> = Cache2Q::new(CacheCfgV1::new(10_000_000));

    let hits1 = search_snapshot_cached(
        &store,
        &snap_hash,
        &qterms,
        &scfg,
        Some(&mut snap_cache),
        Some(&mut idx_cache),
    )
    .unwrap();

    // First run should load snapshot + index segment from cold store.
    assert_eq!(store.get_calls(), 2);

    let s1 = snap_cache.stats();
    assert_eq!(s1.lookups, 1);
    assert_eq!(s1.misses, 1);
    let i1 = idx_cache.stats();
    assert_eq!(i1.lookups, 1);
    assert_eq!(i1.misses, 1);

    let hits2 = search_snapshot_cached(
        &store,
        &snap_hash,
        &qterms,
        &scfg,
        Some(&mut snap_cache),
        Some(&mut idx_cache),
    )
    .unwrap();

    // Second run should be served entirely from warm caches.
    assert_eq!(store.get_calls(), 2);

    assert_eq!(hits1.len(), hits2.len());
    for (a, b) in hits1.iter().zip(hits2.iter()) {
        assert_eq!(a.score, b.score);
        assert_eq!(a.frame_seg, b.frame_seg);
        assert_eq!(a.row_ix, b.row_ix);
    }

    let s2 = snap_cache.stats();
    assert_eq!(s2.lookups, 2);
    assert_eq!(s2.misses, 1);
    assert_eq!(s2.hits_a1 + s2.hits_am, 1);

    let i2 = idx_cache.stats();
    assert_eq!(i2.lookups, 2);
    assert_eq!(i2.misses, 1);
    assert_eq!(i2.hits_a1 + i2.hits_am, 1);
}
