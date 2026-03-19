// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! ReduceManifestV1 artifact helpers.
//!
//! The reduce manifest is a canonically encoded, content-addressed inventory of
//! reduce outputs. It is stored in the primary artifact store root.

use crate::artifact::{ArtifactError, ArtifactStore};
use crate::codec::{DecodeError, EncodeError};
use crate::hash::Hash32;
use crate::reduce_manifest::ReduceManifestV1;
use core::fmt;

/// Errors while storing or loading a ReduceManifestV1 artifact.
#[derive(Debug)]
pub enum ReduceManifestArtifactError {
    /// Manifest could not be encoded.
    Encode(EncodeError),
    /// Manifest could not be decoded.
    Decode(DecodeError),
    /// Artifact store error.
    Store(ArtifactError),
}

impl fmt::Display for ReduceManifestArtifactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReduceManifestArtifactError::Encode(e) => write!(f, "encode: {}", e),
            ReduceManifestArtifactError::Decode(e) => write!(f, "decode: {}", e),
            ReduceManifestArtifactError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl std::error::Error for ReduceManifestArtifactError {}

impl From<EncodeError> for ReduceManifestArtifactError {
    fn from(e: EncodeError) -> Self {
        ReduceManifestArtifactError::Encode(e)
    }
}

impl From<DecodeError> for ReduceManifestArtifactError {
    fn from(e: DecodeError) -> Self {
        ReduceManifestArtifactError::Decode(e)
    }
}

impl From<ArtifactError> for ReduceManifestArtifactError {
    fn from(e: ArtifactError) -> Self {
        ReduceManifestArtifactError::Store(e)
    }
}

/// Store a ReduceManifestV1 as a content-addressed artifact.
pub fn put_reduce_manifest_v1<S: ArtifactStore>(
    store: &S,
    man: &ReduceManifestV1,
) -> Result<Hash32, ReduceManifestArtifactError> {
    let bytes = man.encode()?;
    let h = store.put(&bytes)?;
    Ok(h)
}

/// Load a ReduceManifestV1 from a content-addressed artifact.
pub fn get_reduce_manifest_v1<S: ArtifactStore>(
    store: &S,
    hash: &Hash32,
) -> Result<Option<ReduceManifestV1>, ReduceManifestArtifactError> {
    let opt = store.get(hash)?;
    let bytes = match opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let man = ReduceManifestV1::decode(&bytes)?;
    Ok(Some(man))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
    use crate::reduce_manifest::{ReduceOutputV1, REDUCE_MANIFEST_V1_VERSION};

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push("fsa_lm_tests");
        p.push(name);
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn h(b: u8) -> Hash32 {
        let mut x = [0u8; 32];
        x[0] = b;
        x
    }

    #[test]
    fn reduce_manifest_artifact_round_trip() {
        let dir = tmp_dir("reduce_manifest_artifact_round_trip");
        let store = FsArtifactStore::new(&dir).unwrap();

        let man = ReduceManifestV1 {
            version: REDUCE_MANIFEST_V1_VERSION,
            shard_manifest: h(9),
            shard_count: 2,
            mapping_id: "doc_id_hash32_v1".to_string(),
            source_id_u64: 7,
            snapshot_entries: 0,
            copied_frame_segs: 0,
            copied_index_segs: 0,
            copied_segment_sigs: 0,
            outputs: vec![
                ReduceOutputV1 { tag: "index_sig_map_v1".to_string(), hash: h(2) },
                ReduceOutputV1 { tag: "index_snapshot_v1".to_string(), hash: h(1) },
            ],
        };

        let hash = put_reduce_manifest_v1(&store, &man).unwrap();
        let got = get_reduce_manifest_v1(&store, &hash).unwrap().unwrap();
        assert_eq!(man, got);
    }
}
