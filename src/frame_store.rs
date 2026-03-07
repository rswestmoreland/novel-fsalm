// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! FrameSegment persistence helpers.
//!
//! Novel FSA-LM stores knowledge as immutable, columnar `FrameSegmentV1` blobs.
//! This module provides artifact-store helpers for putting and getting those
//! segments by content hash.
//!
//! This is the "cold storage" layer:
//! - segments are immutable and content-addressed
//! - a segment can be stored on disk without needing to fit in RAM
//! - future stages build warm/hot indexes over segment metadata
//!
//! No extra crates.

use crate::artifact::ArtifactStore;
use crate::cache::Cache2Q;
use crate::codec::{DecodeError, EncodeError};
use crate::frame_segment::FrameSegmentV1;
use crate::hash::Hash32;
use std::sync::Arc;

/// Store a FrameSegmentV1 as an artifact.
///
/// The segment encoding is canonical, so the returned hash is stable for the
/// same logical segment content.
pub fn put_frame_segment_v1<S: ArtifactStore>(
    store: &S,
    seg: &FrameSegmentV1,
) -> Result<Hash32, FrameStoreError> {
    let bytes = seg.encode().map_err(FrameStoreError::Encode)?;
    store.put(&bytes).map_err(FrameStoreError::Store)
}

/// Load and decode a FrameSegmentV1 artifact by hash.
///
/// Returns None if not found.
pub fn get_frame_segment_v1<S: ArtifactStore>(
    store: &S,
    hash: &Hash32,
) -> Result<Option<FrameSegmentV1>, FrameStoreError> {
    let bytes_opt = store.get(hash).map_err(FrameStoreError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let seg = FrameSegmentV1::decode(&bytes).map_err(FrameStoreError::Decode)?;
    Ok(Some(seg))
}

/// Load and decode a FrameSegmentV1 artifact by hash, using a read-through cache.
///
/// Behavior:
/// - If present in cache: returns the cached decoded segment.
/// - If not cached: loads bytes from the store, decodes, and inserts into cache (best effort).
/// - Returns None if not found in the store.
///
/// Cache notes:
/// - Key is the artifact hash (Hash32).
/// - Value is an Arc-wrapped decoded FrameSegmentV1 to avoid cloning large segments.
/// - cost_bytes uses the encoded artifact length (bytes.len).
pub fn get_frame_segment_v1_cached<S: ArtifactStore>(
    store: &S,
    cache: &mut Cache2Q<Hash32, Arc<FrameSegmentV1>>,
    hash: &Hash32,
) -> Result<Option<Arc<FrameSegmentV1>>, FrameStoreError> {
    if let Some(v) = cache.get(hash) {
        return Ok(Some(v.clone()));
    }

    let bytes_opt = store.get(hash).map_err(FrameStoreError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };

    let cost_bytes = bytes.len() as u64;
    let seg = FrameSegmentV1::decode(&bytes).map_err(FrameStoreError::Decode)?;
    let arc = Arc::new(seg);

    // Best effort insert. Even if it does not fit, return the decoded segment.
    let _ = cache.insert_cost(*hash, arc.clone(), cost_bytes);

    Ok(Some(arc))
}

/// Errors for FrameSegment persistence helpers.
#[derive(Debug)]
pub enum FrameStoreError {
    /// FrameSegment encoding error.
    Encode(EncodeError),
    /// FrameSegment decoding error.
    Decode(DecodeError),
    /// Artifact store error.
    Store(crate::artifact::ArtifactError),
}

impl core::fmt::Display for FrameStoreError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            FrameStoreError::Encode(e) => write!(f, "encode: {}", e),
            FrameStoreError::Decode(e) => write!(f, "decode: {}", e),
            FrameStoreError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl From<crate::artifact::ArtifactError> for FrameStoreError {
    fn from(e: crate::artifact::ArtifactError) -> Self {
        FrameStoreError::Store(e)
    }
}
