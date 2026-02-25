// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! ShardManifestV1 artifact helpers.
//!
//! The shard manifest is a canonically encoded, content-addressed inventory of
//! shard outputs. It is stored in the primary artifact store root.

use crate::artifact::{ArtifactError, ArtifactStore};
use crate::codec::{DecodeError, EncodeError};
use crate::hash::Hash32;
use crate::shard_manifest::ShardManifestV1;
use core::fmt;

/// Errors while storing or loading a ShardManifestV1 artifact.
#[derive(Debug)]
pub enum ShardManifestArtifactError {
    /// Manifest could not be encoded.
    Encode(EncodeError),
    /// Manifest could not be decoded.
    Decode(DecodeError),
    /// Artifact store error.
    Store(ArtifactError),
}

impl fmt::Display for ShardManifestArtifactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShardManifestArtifactError::Encode(e) => write!(f, "encode: {}", e),
            ShardManifestArtifactError::Decode(e) => write!(f, "decode: {}", e),
            ShardManifestArtifactError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl std::error::Error for ShardManifestArtifactError {}

impl From<EncodeError> for ShardManifestArtifactError {
    fn from(e: EncodeError) -> Self {
        ShardManifestArtifactError::Encode(e)
    }
}

impl From<DecodeError> for ShardManifestArtifactError {
    fn from(e: DecodeError) -> Self {
        ShardManifestArtifactError::Decode(e)
    }
}

impl From<ArtifactError> for ShardManifestArtifactError {
    fn from(e: ArtifactError) -> Self {
        ShardManifestArtifactError::Store(e)
    }
}

/// Store a ShardManifestV1 as a content-addressed artifact.
pub fn put_shard_manifest_v1<S: ArtifactStore>(
    store: &S,
    man: &ShardManifestV1,
) -> Result<Hash32, ShardManifestArtifactError> {
    let bytes = man.encode()?;
    let h = store.put(&bytes)?;
    Ok(h)
}

/// Load a ShardManifestV1 from a content-addressed artifact.
pub fn get_shard_manifest_v1<S: ArtifactStore>(
    store: &S,
    hash: &Hash32,
) -> Result<Option<ShardManifestV1>, ShardManifestArtifactError> {
    let opt = store.get(hash)?;
    let bytes = match opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let man = ShardManifestV1::decode(&bytes)?;
    Ok(Some(man))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
    use crate::shard_manifest::{ShardEntryV1, ShardOutputV1, SHARD_MANIFEST_V1_VERSION};

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
    fn shard_manifest_artifact_round_trip() {
        let dir = tmp_dir("shard_manifest_artifact_round_trip");
        let store = FsArtifactStore::new(&dir).unwrap();

        let man = ShardManifestV1 {
            version: SHARD_MANIFEST_V1_VERSION,
            shard_count: 2,
            mapping_id: "doc_id_hash32_v1".to_string(),
            shards: vec![ShardEntryV1 {
                shard_id: 0,
                shard_root_rel: "shards/0000".to_string(),
                outputs: vec![ShardOutputV1 {
                    tag: "index_snapshot".to_string(),
                    hash: h(7),
                }],
            }],
        };

        let hash = put_shard_manifest_v1(&store, &man).unwrap();
        let got = get_shard_manifest_v1(&store, &hash).unwrap().unwrap();
        assert_eq!(man, got);
    }
}
