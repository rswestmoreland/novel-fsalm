// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

// IndexSnapshot artifact helpers.
//
// ASCII-only comments.
// No extra crates.
//
// This module provides put/get helpers plus an optional read-through cache.

use crate::artifact::{ArtifactError, ArtifactStore};
use crate::cache::Cache2Q;
use crate::codec::{DecodeError, EncodeError};
use crate::hash::Hash32;
use crate::index_snapshot::IndexSnapshotV1;

use std::sync::Arc;

/// IndexSnapshot artifact helper errors.
#[derive(Debug)]
pub enum IndexSnapshotStoreError {
    /// Encode failure.
    Encode(EncodeError),
    /// Decode failure.
    Decode(DecodeError),
    /// Artifact store error.
    Store(ArtifactError),
}

impl core::fmt::Display for IndexSnapshotStoreError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            IndexSnapshotStoreError::Encode(e) => write!(f, "encode: {}", e),
            IndexSnapshotStoreError::Decode(e) => write!(f, "decode: {}", e),
            IndexSnapshotStoreError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl std::error::Error for IndexSnapshotStoreError {}

/// Store an IndexSnapshotV1 as an artifact.
pub fn put_index_snapshot_v1<S: ArtifactStore>(
    store: &S,
    snap: &IndexSnapshotV1,
) -> Result<Hash32, IndexSnapshotStoreError> {
    let bytes = snap.encode().map_err(IndexSnapshotStoreError::Encode)?;
    let h = store.put(&bytes).map_err(IndexSnapshotStoreError::Store)?;
    Ok(h)
}

/// Load and decode an IndexSnapshotV1 by hash.
pub fn get_index_snapshot_v1<S: ArtifactStore>(
    store: &S,
    hash: &Hash32,
) -> Result<Option<IndexSnapshotV1>, IndexSnapshotStoreError> {
    let bytes_opt = store.get(hash).map_err(IndexSnapshotStoreError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let snap = IndexSnapshotV1::decode(&bytes).map_err(IndexSnapshotStoreError::Decode)?;
    Ok(Some(snap))
}

/// Load and decode an IndexSnapshotV1 with an optional read-through cache.
///
/// Cache key: snapshot artifact hash.
/// Cache value: Arc<IndexSnapshotV1>.
/// Cost bytes: encoded artifact length.
///
/// If the snapshot is not present in the store, returns Ok(None).
/// If the snapshot is too large for the cache, it is still returned.
pub fn get_index_snapshot_v1_cached<S: ArtifactStore>(
    store: &S,
    cache: &mut Cache2Q<Hash32, Arc<IndexSnapshotV1>>,
    hash: &Hash32,
) -> Result<Option<Arc<IndexSnapshotV1>>, IndexSnapshotStoreError> {
    if let Some(v) = cache.get(hash) {
        return Ok(Some(v.clone()));
    }

    let bytes_opt = store.get(hash).map_err(IndexSnapshotStoreError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };

    let cost: u64 = bytes.len() as u64;

    let snap = IndexSnapshotV1::decode(&bytes).map_err(IndexSnapshotStoreError::Decode)?;
    let arc = Arc::new(snap);

    let _ = cache.insert_cost(hash.clone(), arc.clone(), cost);

    Ok(Some(arc))
}
