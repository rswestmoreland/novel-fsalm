// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

// SegmentSig artifact helpers.
//
// ASCII-only comments.
// No extra crates.
//
// This module provides put/get helpers plus an optional read-through cache.

use crate::artifact::{ArtifactError, ArtifactStore};
use crate::cache::Cache2Q;
use crate::codec::{DecodeError, EncodeError};
use crate::hash::Hash32;
use crate::segment_sig::SegmentSigV1;

use std::sync::Arc;

/// SegmentSig artifact helper errors.
#[derive(Debug)]
pub enum SegmentSigStoreError {
    /// Encode failure.
    Encode(EncodeError),
    /// Decode failure.
    Decode(DecodeError),
    /// Artifact store error.
    Store(ArtifactError),
}

impl core::fmt::Display for SegmentSigStoreError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            SegmentSigStoreError::Encode(e) => write!(f, "encode: {}", e),
            SegmentSigStoreError::Decode(e) => write!(f, "decode: {}", e),
            SegmentSigStoreError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl std::error::Error for SegmentSigStoreError {}

/// Store a SegmentSigV1 as an artifact.
pub fn put_segment_sig_v1<S: ArtifactStore>(
    store: &S,
    sig: &SegmentSigV1,
) -> Result<Hash32, SegmentSigStoreError> {
    let bytes = sig.encode().map_err(SegmentSigStoreError::Encode)?;
    let h = store.put(&bytes).map_err(SegmentSigStoreError::Store)?;
    Ok(h)
}

/// Load and decode a SegmentSigV1 by hash.
pub fn get_segment_sig_v1<S: ArtifactStore>(
    store: &S,
    hash: &Hash32,
) -> Result<Option<SegmentSigV1>, SegmentSigStoreError> {
    let bytes_opt = store.get(hash).map_err(SegmentSigStoreError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let sig = SegmentSigV1::decode(&bytes).map_err(SegmentSigStoreError::Decode)?;
    Ok(Some(sig))
}

/// Load and decode a SegmentSigV1 with an optional read-through cache.
///
/// Cache key: signature artifact hash.
/// Cache value: Arc<SegmentSigV1>.
/// Cost bytes: encoded artifact length.
///
/// If the signature is not present in the store, returns Ok(None).
/// If the signature is too large for the cache, it is still returned.
pub fn get_segment_sig_v1_cached<S: ArtifactStore>(
    store: &S,
    cache: &mut Cache2Q<Hash32, Arc<SegmentSigV1>>,
    hash: &Hash32,
) -> Result<Option<Arc<SegmentSigV1>>, SegmentSigStoreError> {
    if let Some(v) = cache.get(hash) {
        return Ok(Some(v.clone()));
    }

    let bytes_opt = store.get(hash).map_err(SegmentSigStoreError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };

    let cost: u64 = bytes.len() as u64;

    let sig = SegmentSigV1::decode(&bytes).map_err(SegmentSigStoreError::Decode)?;
    let arc = Arc::new(sig);

    let _ = cache.insert_cost(*hash, arc.clone(), cost);

    Ok(Some(arc))
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
    use crate::cache::{Cache2Q, CacheCfgV1};
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

    fn sample_sig() -> SegmentSigV1 {
        SegmentSigV1 {
            index_seg: h(7),
            bloom_k: 2,
            bloom_bits: vec![0xAA, 0x55, 0x00, 0xFF],
            sketch: Vec::new(),
        }
    }

    #[test]
    fn segment_sig_store_round_trip() {
        let dir = tmp_dir("segment_sig_store_round_trip");
        let store = FsArtifactStore::new(&dir).unwrap();

        let sig = sample_sig();
        let hh = put_segment_sig_v1(&store, &sig).unwrap();
        let got = get_segment_sig_v1(&store, &hh).unwrap().unwrap();
        assert_eq!(got, sig);
    }

    #[test]
    fn segment_sig_store_cached_ptr_eq_on_second_get() {
        let dir = tmp_dir("segment_sig_store_cached_ptr_eq_on_second_get");
        let store = FsArtifactStore::new(&dir).unwrap();

        let sig = sample_sig();
        let hh = put_segment_sig_v1(&store, &sig).unwrap();

        let mut cache: Cache2Q<Hash32, Arc<SegmentSigV1>> = Cache2Q::new(CacheCfgV1::new(1_000_000));

        let a1 = get_segment_sig_v1_cached(&store, &mut cache, &hh).unwrap().unwrap();
        let a2 = get_segment_sig_v1_cached(&store, &mut cache, &hh).unwrap().unwrap();

        assert!(Arc::ptr_eq(&a1, &a2));
    }
}
