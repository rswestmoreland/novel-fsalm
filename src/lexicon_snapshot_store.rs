// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

// LexiconSnapshot artifact helpers.
//
// ASCII-only comments.
// No extra crates.
//
// This module provides put/get helpers plus an optional read-through cache.

use crate::artifact::{ArtifactError, ArtifactStore};
use crate::cache::Cache2Q;
use crate::codec::{DecodeError, EncodeError};
use crate::hash::Hash32;
use crate::lexicon_snapshot::LexiconSnapshotV1;

use std::sync::Arc;

/// LexiconSnapshot artifact helper errors.
#[derive(Debug)]
pub enum LexiconSnapshotStoreError {
    /// Encode failure.
    Encode(EncodeError),
    /// Decode failure.
    Decode(DecodeError),
    /// Artifact store error.
    Store(ArtifactError),
}

impl core::fmt::Display for LexiconSnapshotStoreError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            LexiconSnapshotStoreError::Encode(e) => write!(f, "encode: {}", e),
            LexiconSnapshotStoreError::Decode(e) => write!(f, "decode: {}", e),
            LexiconSnapshotStoreError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl std::error::Error for LexiconSnapshotStoreError {}

/// Store a LexiconSnapshotV1 as an artifact.
pub fn put_lexicon_snapshot_v1<S: ArtifactStore>(
    store: &S,
    snap: &LexiconSnapshotV1,
) -> Result<Hash32, LexiconSnapshotStoreError> {
    let bytes = snap.encode().map_err(LexiconSnapshotStoreError::Encode)?;
    let h = store
        .put(&bytes)
        .map_err(LexiconSnapshotStoreError::Store)?;
    Ok(h)
}

/// Load and decode a LexiconSnapshotV1 by hash.
pub fn get_lexicon_snapshot_v1<S: ArtifactStore>(
    store: &S,
    hash: &Hash32,
) -> Result<Option<LexiconSnapshotV1>, LexiconSnapshotStoreError> {
    let bytes_opt = store.get(hash).map_err(LexiconSnapshotStoreError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let snap = LexiconSnapshotV1::decode(&bytes).map_err(LexiconSnapshotStoreError::Decode)?;
    Ok(Some(snap))
}

/// Load and decode a LexiconSnapshotV1 with an optional read-through cache.
///
/// Cache key: snapshot artifact hash.
/// Cache value: Arc<LexiconSnapshotV1>.
/// Cost bytes: encoded artifact length.
///
/// If the snapshot is not present in the store, returns Ok(None).
/// If the snapshot is too large for the cache, it is still returned.
pub fn get_lexicon_snapshot_v1_cached<S: ArtifactStore>(
    store: &S,
    cache: &mut Cache2Q<Hash32, Arc<LexiconSnapshotV1>>,
    hash: &Hash32,
) -> Result<Option<Arc<LexiconSnapshotV1>>, LexiconSnapshotStoreError> {
    if let Some(v) = cache.get(hash) {
        return Ok(Some(v.clone()));
    }

    let bytes_opt = store.get(hash).map_err(LexiconSnapshotStoreError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };

    let cost: u64 = bytes.len() as u64;

    let snap = LexiconSnapshotV1::decode(&bytes).map_err(LexiconSnapshotStoreError::Decode)?;
    let arc = Arc::new(snap);

    let _ = cache.insert_cost(*hash, arc.clone(), cost);

    Ok(Some(arc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
    use crate::cache::{Cache2Q, CacheCfgV1};
    use crate::lexicon_snapshot::{LexiconSnapshotEntryV1, LexiconSnapshotV1};

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

    fn sample_snapshot() -> LexiconSnapshotV1 {
        let mut s = LexiconSnapshotV1::new();
        s.entries.push(LexiconSnapshotEntryV1 {
            lex_seg: h(9),
            lemma_count: 1,
            sense_count: 0,
            rel_count: 0,
            pron_count: 0,
        });
        s
    }

    #[test]
    fn lexicon_snapshot_store_round_trip() {
        let dir = tmp_dir("lexicon_snapshot_store_round_trip");
        let store = FsArtifactStore::new(&dir).unwrap();

        let snap = sample_snapshot();
        let h = put_lexicon_snapshot_v1(&store, &snap).unwrap();
        let got = get_lexicon_snapshot_v1(&store, &h).unwrap().unwrap();
        assert_eq!(got, snap);
    }

    #[test]
    fn lexicon_snapshot_store_cached_ptr_eq_on_second_get() {
        let dir = tmp_dir("lexicon_snapshot_store_cached_ptr_eq_on_second_get");
        let store = FsArtifactStore::new(&dir).unwrap();

        let snap = sample_snapshot();
        let h = put_lexicon_snapshot_v1(&store, &snap).unwrap();

        let mut cache: Cache2Q<Hash32, Arc<LexiconSnapshotV1>> =
            Cache2Q::new(CacheCfgV1::new(1_000_000));

        let a1 = get_lexicon_snapshot_v1_cached(&store, &mut cache, &h)
            .unwrap()
            .unwrap();
        let a2 = get_lexicon_snapshot_v1_cached(&store, &mut cache, &h)
            .unwrap()
            .unwrap();

        assert!(Arc::ptr_eq(&a1, &a2));
    }
}
