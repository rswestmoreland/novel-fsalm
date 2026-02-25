// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! PlannerHintsV1 artifact helpers.
//!
//! Stores and retrieves `PlannerHintsV1` objects in an `ArtifactStore`.
//! The bytes are canonical encodings as defined by `PlannerHintsV1::encode`.

use crate::artifact::{ArtifactError, ArtifactStore};
use crate::codec::{DecodeError, EncodeError};
use crate::hash::Hash32;
use crate::planner_hints::PlannerHintsV1;
use core::fmt;

/// Errors while storing or loading a PlannerHintsV1 artifact.
#[derive(Debug)]
pub enum PlannerHintsArtifactError {
    /// PlannerHints could not be encoded.
    Encode(EncodeError),
    /// PlannerHints could not be decoded.
    Decode(DecodeError),
    /// Artifact store error.
    Store(ArtifactError),
}

impl fmt::Display for PlannerHintsArtifactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlannerHintsArtifactError::Encode(e) => write!(f, "encode: {}", e),
            PlannerHintsArtifactError::Decode(e) => write!(f, "decode: {}", e),
            PlannerHintsArtifactError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl std::error::Error for PlannerHintsArtifactError {}

/// Store `PlannerHintsV1` as a content-addressed artifact.
pub fn put_planner_hints_v1<S: ArtifactStore>(
    store: &S,
    hints: &PlannerHintsV1,
) -> Result<Hash32, PlannerHintsArtifactError> {
    let bytes = hints.encode().map_err(PlannerHintsArtifactError::Encode)?;
    store.put(&bytes).map_err(PlannerHintsArtifactError::Store)
}

/// Load a `PlannerHintsV1` artifact by its content hash.
///
/// Returns None if the artifact is not present in the store.
pub fn get_planner_hints_v1<S: ArtifactStore>(
    store: &S,
    id: &Hash32,
) -> Result<Option<PlannerHintsV1>, PlannerHintsArtifactError> {
    let bytes_opt = store.get(id).map_err(PlannerHintsArtifactError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let hints = PlannerHintsV1::decode(&bytes).map_err(PlannerHintsArtifactError::Decode)?;
    Ok(Some(hints))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
    use crate::planner_hints::PLANNER_HINTS_V1_VERSION;

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        let mut base = std::env::temp_dir();
        base.push("fsa_lm_tests");
        base.push(name);
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).expect("tmp create_dir_all");
        base
    }

    #[test]
    fn planner_hints_artifact_roundtrip() {
        let root = tmp_dir("planner_hints_artifact_roundtrip");
        let store = FsArtifactStore::new(&root).unwrap();

        let hints = PlannerHintsV1 {
            version: PLANNER_HINTS_V1_VERSION,
            query_id: [1u8; 32],
            flags: 0,
            hints: Vec::new(),
            followups: Vec::new(),
        };

        let id = put_planner_hints_v1(&store, &hints).expect("put");
        let got = get_planner_hints_v1(&store, &id).expect("get").unwrap();
        assert_eq!(got, hints);

        let _ = std::fs::remove_dir_all(&root);
    }
}
