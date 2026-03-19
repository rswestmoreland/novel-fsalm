// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! ExemplarMemoryV1 artifact helpers.

use crate::artifact::{ArtifactError, ArtifactStore};
use crate::codec::{DecodeError, EncodeError};
use crate::exemplar_memory::ExemplarMemoryV1;
use crate::hash::Hash32;
use core::fmt;

/// Errors while storing or loading an ExemplarMemoryV1 artifact.
#[derive(Debug)]
pub enum ExemplarMemoryArtifactError {
    /// Exemplar memory could not be encoded.
    Encode(EncodeError),
    /// Exemplar memory could not be decoded.
    Decode(DecodeError),
    /// Artifact store error.
    Store(ArtifactError),
}

impl fmt::Display for ExemplarMemoryArtifactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExemplarMemoryArtifactError::Encode(e) => write!(f, "encode: {}", e),
            ExemplarMemoryArtifactError::Decode(e) => write!(f, "decode: {}", e),
            ExemplarMemoryArtifactError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl std::error::Error for ExemplarMemoryArtifactError {}

/// Store ExemplarMemoryV1 as a content-addressed artifact.
pub fn put_exemplar_memory_v1<S: ArtifactStore>(
    store: &S,
    exemplar_memory: &ExemplarMemoryV1,
) -> Result<Hash32, ExemplarMemoryArtifactError> {
    let bytes = exemplar_memory
        .encode()
        .map_err(ExemplarMemoryArtifactError::Encode)?;
    store
        .put(&bytes)
        .map_err(ExemplarMemoryArtifactError::Store)
}

/// Load ExemplarMemoryV1 by content hash.
///
/// Returns None if the artifact is not present in the store.
pub fn get_exemplar_memory_v1<S: ArtifactStore>(
    store: &S,
    hash: &Hash32,
) -> Result<Option<ExemplarMemoryV1>, ExemplarMemoryArtifactError> {
    let bytes_opt = store
        .get(hash)
        .map_err(ExemplarMemoryArtifactError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let exemplar_memory =
        ExemplarMemoryV1::decode(&bytes).map_err(ExemplarMemoryArtifactError::Decode)?;
    Ok(Some(exemplar_memory))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
    use crate::exemplar_memory::{
        ExemplarMemoryV1, ExemplarResponseModeV1, ExemplarRowV1, ExemplarStructureKindV1,
        ExemplarSupportRefV1, ExemplarSupportSourceKindV1, ExemplarToneKindV1,
        EXEMPLAR_MEMORY_V1_VERSION, EXMEM_FLAG_HAS_REPLAY_LOG, EXROW_FLAG_HAS_STEPS,
    };
    use crate::frame::Id64;
    use crate::hash::blake3_hash;

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        let mut base = std::env::temp_dir();
        base.push("fsa_lm_tests");
        base.push(name);
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).expect("tmp create_dir_all");
        base
    }

    #[test]
    fn exemplar_memory_artifact_roundtrip() {
        let root = tmp_dir("exemplar_memory_artifact_roundtrip");
        let store = FsArtifactStore::new(&root).unwrap();
        let exemplar_memory = ExemplarMemoryV1 {
            version: EXEMPLAR_MEMORY_V1_VERSION,
            build_id: blake3_hash(b"build"),
            flags: EXMEM_FLAG_HAS_REPLAY_LOG,
            rows: vec![ExemplarRowV1 {
                exemplar_id: Id64(1),
                response_mode: ExemplarResponseModeV1::Explain,
                structure_kind: ExemplarStructureKindV1::Steps,
                tone_kind: ExemplarToneKindV1::Supportive,
                flags: EXROW_FLAG_HAS_STEPS,
                support_count: 1,
                support_refs: vec![ExemplarSupportRefV1::new(
                    ExemplarSupportSourceKindV1::ReplayLog,
                    blake3_hash(b"replay"),
                    0,
                )],
            }],
        };

        let hash = put_exemplar_memory_v1(&store, &exemplar_memory).expect("put");
        let got = get_exemplar_memory_v1(&store, &hash).expect("get").unwrap();
        assert_eq!(got, exemplar_memory);

        let _ = std::fs::remove_dir_all(&root);
    }
}
