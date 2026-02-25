// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use fsa_lm::artifact::{ArtifactResult, ArtifactStore, FsArtifactStore};
use fsa_lm::cache::{Cache2Q, CacheCfgV1};
use fsa_lm::frame::{derive_id64, DocId, FrameRowV1, SourceId};
use fsa_lm::frame_segment::FrameSegmentV1;
use fsa_lm::frame_store::{get_frame_segment_v1_cached, put_frame_segment_v1};
use fsa_lm::hash::Hash32;
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

#[test]
fn frame_segment_cached_get_avoids_second_store_read() {
    let dir = std::env::temp_dir()
        .join("fsa_lm_tests")
        .join("frame_segment_cached_get_avoids_second_store_read");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let store = CountingStore::new(FsArtifactStore::new(&dir).unwrap());

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
    let seg = FrameSegmentV1::from_rows(&rows, 1).unwrap();

    let h = put_frame_segment_v1(&store, &seg).unwrap();

    store.reset_get_calls();

    let mut cfg = CacheCfgV1::new(10_000_000);
    cfg.a1_ratio = 100;
    let mut cache: Cache2Q<Hash32, Arc<FrameSegmentV1>> = Cache2Q::new(cfg);

    let s1 = get_frame_segment_v1_cached(&store, &mut cache, &h)
        .unwrap()
        .unwrap();
    assert_eq!(&*s1, &seg);
    assert_eq!(store.get_calls(), 1);

    let stats1 = cache.stats();
    assert_eq!(stats1.lookups, 1);
    assert_eq!(stats1.misses, 1);

    let s2 = get_frame_segment_v1_cached(&store, &mut cache, &h)
        .unwrap()
        .unwrap();
    assert_eq!(&*s2, &seg);
    assert_eq!(store.get_calls(), 1);

    let stats2 = cache.stats();
    assert_eq!(stats2.lookups, 2);
    assert_eq!(stats2.misses, 1);
    assert_eq!(stats2.hits_a1 + stats2.hits_am, 1);
}
