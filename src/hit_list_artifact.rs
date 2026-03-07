// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! HitList artifact helpers.
//!
//! These helpers store and load HitListV1 artifacts using the
//! content-addressed ArtifactStore.

use crate::artifact::{ArtifactError, ArtifactStore};
use crate::codec::{DecodeError, EncodeError};
use crate::hash::Hash32;
use crate::hit_list::HitListV1;
use core::fmt;

/// Errors while storing or loading a HitListV1 artifact.
#[derive(Debug)]
pub enum HitListArtifactError {
    /// HitList encoding error.
    Encode(EncodeError),
    /// HitList decoding error.
    Decode(DecodeError),
    /// Artifact store error.
    Store(ArtifactError),
}

impl fmt::Display for HitListArtifactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HitListArtifactError::Encode(e) => write!(f, "encode: {}", e),
            HitListArtifactError::Decode(e) => write!(f, "decode: {}", e),
            HitListArtifactError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl std::error::Error for HitListArtifactError {}

impl From<EncodeError> for HitListArtifactError {
    fn from(e: EncodeError) -> Self {
        HitListArtifactError::Encode(e)
    }
}

impl From<DecodeError> for HitListArtifactError {
    fn from(e: DecodeError) -> Self {
        HitListArtifactError::Decode(e)
    }
}

impl From<ArtifactError> for HitListArtifactError {
    fn from(e: ArtifactError) -> Self {
        HitListArtifactError::Store(e)
    }
}

/// Store a HitListV1 as a content-addressed artifact.
///
/// The list is canonically encoded before storing.
pub fn put_hit_list_v1<S: ArtifactStore>(
    store: &S,
    list: &HitListV1,
) -> Result<Hash32, HitListArtifactError> {
    let bytes = list.encode()?;
    let h = store.put(&bytes)?;
    Ok(h)
}

/// Load and decode a HitListV1 artifact by hash.
///
/// Returns None if the artifact is not present in the store.
pub fn get_hit_list_v1<S: ArtifactStore>(
    store: &S,
    hash: &Hash32,
) -> Result<Option<HitListV1>, HitListArtifactError> {
    let opt = store.get(hash)?;
    let bytes = match opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let list = HitListV1::decode(&bytes)?;
    Ok(Some(list))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
    use crate::hash::blake3_hash;

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push("fsa_lm_tests");
        p.push(name);
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn hit_list_artifact_round_trip() {
        let dir = tmp_dir("hit_list_artifact_round_trip");
        let store = FsArtifactStore::new(&dir).unwrap();

        let q = blake3_hash(b"q");
        let s = blake3_hash(b"snap");
        let a = blake3_hash(b"seg-a");

        let list = HitListV1 {
            query_id: q,
            snapshot_id: s,
            tie_control_id: None,
            hits: vec![crate::hit_list::HitV1 {
                frame_seg: a,
                row_ix: 1,
                score: 7,
            }],
        };

        let hash = put_hit_list_v1(&store, &list).unwrap();
        let got = get_hit_list_v1(&store, &hash).unwrap().unwrap();
        assert_eq!(list, got);
    }
}
