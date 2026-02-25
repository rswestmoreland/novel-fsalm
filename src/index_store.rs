// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! IndexSegment persistence helpers.
//!
//! IndexSegments are immutable, content-addressed artifacts built from
//! FrameSegments. This module provides artifact-store helpers for storing and
//! loading `IndexSegmentV1` by content hash.
//!
//! Cold storage: `ArtifactStore`
//! Warm cache: `Cache2Q` (read-through)
//!
//! No extra crates.

use crate::artifact::ArtifactStore;
use crate::cache::Cache2Q;
use crate::codec::{DecodeError, EncodeError};
use crate::hash::Hash32;
use crate::index_segment::IndexSegmentV1;
use std::sync::Arc;

/// Store an IndexSegmentV1 as an artifact.
///
/// The index encoding is canonical, so the returned hash is stable for the
/// same logical index content.
pub fn put_index_segment_v1<S: ArtifactStore>(
    store: &S,
    seg: &IndexSegmentV1,
) -> Result<Hash32, IndexStoreError> {
    let bytes = seg.encode().map_err(IndexStoreError::Encode)?;
    store.put(&bytes).map_err(IndexStoreError::Store)
}

/// Load and decode an IndexSegmentV1 artifact by hash.
///
/// Returns None if not found.
pub fn get_index_segment_v1<S: ArtifactStore>(
    store: &S,
    hash: &Hash32,
) -> Result<Option<IndexSegmentV1>, IndexStoreError> {
    let bytes_opt = store.get(hash).map_err(IndexStoreError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let seg = IndexSegmentV1::decode(&bytes).map_err(IndexStoreError::Decode)?;
    Ok(Some(seg))
}

/// Load and decode an IndexSegmentV1 artifact by hash, using a read-through cache.
///
/// Behavior:
/// - If present in cache: returns the cached decoded segment.
/// - If not cached: loads bytes from the store, decodes, and inserts into cache (best effort).
/// - Returns None if not found in the store.
///
/// Cache notes:
/// - Key is the artifact hash (Hash32).
/// - Value is an Arc-wrapped decoded IndexSegmentV1 to avoid cloning large segments.
/// - cost_bytes uses the encoded artifact length (bytes.len).
pub fn get_index_segment_v1_cached<S: ArtifactStore>(
    store: &S,
    cache: &mut Cache2Q<Hash32, Arc<IndexSegmentV1>>,
    hash: &Hash32,
) -> Result<Option<Arc<IndexSegmentV1>>, IndexStoreError> {
    if let Some(v) = cache.get(hash) {
        return Ok(Some(v.clone()));
    }

    let bytes_opt = store.get(hash).map_err(IndexStoreError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };

    let cost_bytes = bytes.len() as u64;
    let seg = IndexSegmentV1::decode(&bytes).map_err(IndexStoreError::Decode)?;
    let arc = Arc::new(seg);

    // Best effort insert. Even if it does not fit, return the decoded segment.
    let _ = cache.insert_cost(*hash, arc.clone(), cost_bytes);

    Ok(Some(arc))
}

/// Errors for IndexSegment persistence helpers.
#[derive(Debug)]
pub enum IndexStoreError {
    /// IndexSegment encoding error.
    Encode(EncodeError),
    /// IndexSegment decoding error.
    Decode(DecodeError),
    /// Artifact store error.
    Store(crate::artifact::ArtifactError),
}

impl core::fmt::Display for IndexStoreError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            IndexStoreError::Encode(e) => write!(f, "encode: {}", e),
            IndexStoreError::Decode(e) => write!(f, "decode: {}", e),
            IndexStoreError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl From<crate::artifact::ArtifactError> for IndexStoreError {
    fn from(e: crate::artifact::ArtifactError) -> Self {
        IndexStoreError::Store(e)
    }
}
