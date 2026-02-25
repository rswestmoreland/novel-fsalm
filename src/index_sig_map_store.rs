// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

// IndexSigMap artifact helpers.
//
// ASCII-only comments.
// No extra crates.
//
// This module provides put/get helpers plus an optional read-through cache.

use crate::artifact::{ArtifactError, ArtifactStore};
use crate::cache::Cache2Q;
use crate::codec::{DecodeError, EncodeError};
use crate::hash::Hash32;
use crate::index_sig_map::IndexSigMapV1;

use std::sync::Arc;

/// IndexSigMap artifact helper errors.
#[derive(Debug)]
pub enum IndexSigMapStoreError {
    /// Encode failure.
    Encode(EncodeError),
    /// Decode failure.
    Decode(DecodeError),
    /// Artifact store error.
    Store(ArtifactError),
}

impl core::fmt::Display for IndexSigMapStoreError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            IndexSigMapStoreError::Encode(e) => write!(f, "encode: {}", e),
            IndexSigMapStoreError::Decode(e) => write!(f, "decode: {}", e),
            IndexSigMapStoreError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl std::error::Error for IndexSigMapStoreError {}

/// Store an IndexSigMapV1 as an artifact.
pub fn put_index_sig_map_v1<S: ArtifactStore>(
    store: &S,
    map: &IndexSigMapV1,
) -> Result<Hash32, IndexSigMapStoreError> {
    let bytes = map.encode().map_err(IndexSigMapStoreError::Encode)?;
    let h = store.put(&bytes).map_err(IndexSigMapStoreError::Store)?;
    Ok(h)
}

/// Load and decode an IndexSigMapV1 by hash.
pub fn get_index_sig_map_v1<S: ArtifactStore>(
    store: &S,
    hash: &Hash32,
) -> Result<Option<IndexSigMapV1>, IndexSigMapStoreError> {
    let bytes_opt = store.get(hash).map_err(IndexSigMapStoreError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let sig = IndexSigMapV1::decode(&bytes).map_err(IndexSigMapStoreError::Decode)?;
    Ok(Some(sig))
}

/// Load and decode an IndexSigMapV1 with an optional read-through cache.
///
/// Cache key: map artifact hash.
/// Cache value: Arc<IndexSigMapV1>.
/// Cost bytes: encoded artifact length.
///
/// If the map is not present in the store, returns Ok(None).
/// If the map is too large for the cache, it is still returned.
pub fn get_index_sig_map_v1_cached<S: ArtifactStore>(
    store: &S,
    cache: &mut Cache2Q<Hash32, Arc<IndexSigMapV1>>,
    hash: &Hash32,
) -> Result<Option<Arc<IndexSigMapV1>>, IndexSigMapStoreError> {
    if let Some(v) = cache.get(hash) {
        return Ok(Some(v.clone()));
    }

    let bytes_opt = store.get(hash).map_err(IndexSigMapStoreError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };

    let cost: u64 = bytes.len() as u64;

    let sig = IndexSigMapV1::decode(&bytes).map_err(IndexSigMapStoreError::Decode)?;
    let arc = Arc::new(sig);

    let _ = cache.insert_cost(*hash, arc.clone(), cost);

    Ok(Some(arc))
}



#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
    use crate::cache::{Cache2Q, CacheCfgV1};
    use crate::frame::{Id64, SourceId};
    use crate::hash::Hash32;

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        use std::fs;
        let mut p = std::env::temp_dir();
        p.push("fsa_lm_tests");
        p.push(name);
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn h(b: u8) -> Hash32 {
        let mut x = [0u8; 32];
        x[0] = b;
        x
    }

    fn sample_map() -> IndexSigMapV1 {
        let mut m = IndexSigMapV1::new(SourceId(Id64(7)));
        // Intentionally push unsorted to verify canonicalization on encode.
        m.push(h(2), h(20));
        m.push(h(1), h(10));
        m
    }

    #[test]
    fn index_sig_map_store_round_trip_and_lookup() {
        let dir = tmp_dir("index_sig_map_store_round_trip_and_lookup");
        let store = FsArtifactStore::new(&dir).unwrap();

        let m = sample_map();
        let hh = put_index_sig_map_v1(&store, &m).unwrap();
        let got = get_index_sig_map_v1(&store, &hh).unwrap().unwrap();

        assert_eq!(got.source_id, SourceId(Id64(7)));
        assert_eq!(got.lookup_sig(&h(1)), Some(h(10)));
        assert_eq!(got.lookup_sig(&h(2)), Some(h(20)));
        assert_eq!(got.lookup_sig(&h(9)), None);
    }

    #[test]
    fn index_sig_map_store_cached_ptr_eq_on_second_get() {
        let dir = tmp_dir("index_sig_map_store_cached_ptr_eq_on_second_get");
        let store = FsArtifactStore::new(&dir).unwrap();

        let m = sample_map();
        let hh = put_index_sig_map_v1(&store, &m).unwrap();

        let mut cache: Cache2Q<Hash32, Arc<IndexSigMapV1>> = Cache2Q::new(CacheCfgV1::new(1_000_000));

        let a1 = get_index_sig_map_v1_cached(&store, &mut cache, &hh).unwrap().unwrap();
        let a2 = get_index_sig_map_v1_cached(&store, &mut cache, &hh).unwrap().unwrap();

        assert!(Arc::ptr_eq(&a1, &a2));
    }
}
