// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use fsa_lm::artifact::{ArtifactResult, ArtifactStore, FsArtifactStore};
use fsa_lm::cache::{Cache2Q, CacheCfgV1};
use fsa_lm::frame::{Id64, SourceId};
use fsa_lm::hash::Hash32;
use fsa_lm::index_snapshot::{IndexSnapshotEntryV1, IndexSnapshotV1};
use fsa_lm::index_snapshot_store::{get_index_snapshot_v1_cached, put_index_snapshot_v1};

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

fn h(b: u8) -> Hash32 {
    let mut x = [0u8; 32];
    x[0] = b;
    x
}

#[test]
fn index_snapshot_cached_get_avoids_second_store_read() {
    let dir = std::env::temp_dir()
        .join("fsa_lm_tests")
        .join("index_snapshot_cached_get_avoids_second_store_read");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let store = CountingStore::new(FsArtifactStore::new(&dir).unwrap());

    let src = SourceId(Id64(9));
    let mut snap = IndexSnapshotV1::new(src);
    snap.entries.push(IndexSnapshotEntryV1 {
        frame_seg: h(1),
        index_seg: h(2),
        row_count: 10,
        term_count: 20,
        postings_bytes: 30,
    });

    let snap_hash = put_index_snapshot_v1(&store, &snap).unwrap();

    store.reset_get_calls();

    let mut cfg = CacheCfgV1::new(10_000_000);
    cfg.a1_ratio = 100;
    let mut cache: Cache2Q<Hash32, Arc<IndexSnapshotV1>> = Cache2Q::new(cfg);

    let s1 = get_index_snapshot_v1_cached(&store, &mut cache, &snap_hash)
        .unwrap()
        .unwrap();
    assert_eq!(&*s1, &snap);
    assert_eq!(store.get_calls(), 1);

    let stats1 = cache.stats();
    assert_eq!(stats1.lookups, 1);
    assert_eq!(stats1.misses, 1);

    let s2 = get_index_snapshot_v1_cached(&store, &mut cache, &snap_hash)
        .unwrap()
        .unwrap();
    assert_eq!(&*s2, &snap);
    assert_eq!(store.get_calls(), 1);

    let stats2 = cache.stats();
    assert_eq!(stats2.lookups, 2);
    assert_eq!(stats2.misses, 1);
    assert_eq!(stats2.hits_a1 + stats2.hits_am, 1);
}
