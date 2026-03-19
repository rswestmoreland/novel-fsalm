// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! PuzzleSketchArtifactV1 store/load helpers.

use crate::artifact::ArtifactStore;
use crate::codec::{DecodeError, EncodeError};
use crate::hash::Hash32;
use crate::puzzle_sketch_artifact::PuzzleSketchArtifactV1;

/// Store a PuzzleSketchArtifactV1 as an artifact.
pub fn put_puzzle_sketch_artifact_v1<S: ArtifactStore>(
    store: &S,
    a: &PuzzleSketchArtifactV1,
) -> Result<Hash32, PuzzleSketchArtifactStoreError> {
    let bytes = a.encode().map_err(PuzzleSketchArtifactStoreError::Encode)?;
    store
        .put(&bytes)
        .map_err(PuzzleSketchArtifactStoreError::Store)
}

/// Load and decode a PuzzleSketchArtifactV1 by hash.
pub fn get_puzzle_sketch_artifact_v1<S: ArtifactStore>(
    store: &S,
    hash: &Hash32,
) -> Result<Option<PuzzleSketchArtifactV1>, PuzzleSketchArtifactStoreError> {
    let bytes_opt = store
        .get(hash)
        .map_err(PuzzleSketchArtifactStoreError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let a =
        PuzzleSketchArtifactV1::decode(&bytes).map_err(PuzzleSketchArtifactStoreError::Decode)?;
    Ok(Some(a))
}

/// Errors for PuzzleSketchArtifactV1 store/load helpers.
#[derive(Debug)]
pub enum PuzzleSketchArtifactStoreError {
    /// Encode error.
    Encode(EncodeError),
    /// Decode error.
    Decode(DecodeError),
    /// Store error.
    Store(crate::artifact::ArtifactError),
}

impl core::fmt::Display for PuzzleSketchArtifactStoreError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            PuzzleSketchArtifactStoreError::Encode(e) => write!(f, "encode: {}", e),
            PuzzleSketchArtifactStoreError::Decode(e) => write!(f, "decode: {}", e),
            PuzzleSketchArtifactStoreError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl std::error::Error for PuzzleSketchArtifactStoreError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
    use crate::hash::blake3_hash;
    use crate::puzzle_sketch_artifact::puzzle_sketch_source_hash_v1;
    use crate::puzzle_sketch_v1::{build_puzzle_sketch_v1, PuzzleSketchCfgV1};
    use std::fs;
    use std::path::PathBuf;

    fn tmp_dir(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push("novel_fsalm_tests");
        p.push(name);
        p
    }

    #[test]
    fn store_roundtrip_works() {
        let td = tmp_dir("puzzle_sketch_store_roundtrip");
        let _ = fs::remove_dir_all(&td);
        fs::create_dir_all(&td).unwrap();

        let store = FsArtifactStore::new(td.clone()).unwrap();
        let cfg = PuzzleSketchCfgV1::default();
        let sk = build_puzzle_sketch_v1("A,B,C are numbers 1..3. A != B.", None, None, cfg);
        let pid = blake3_hash(b"pid");
        let sh = puzzle_sketch_source_hash_v1("A,B,C are numbers 1..3. A != B.");
        let a = PuzzleSketchArtifactV1::from_sketch(pid, 3, false, false, true, sh, &sk).unwrap();
        let h = put_puzzle_sketch_artifact_v1(&store, &a).unwrap();
        let b = get_puzzle_sketch_artifact_v1(&store, &h).unwrap().unwrap();
        assert_eq!(a, b);

        let _ = fs::remove_dir_all(&td);
    }
}
