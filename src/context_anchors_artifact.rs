// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! ContextAnchorsV1 artifact helpers.
//!
//! Stores and retrieves `ContextAnchorsV1` objects in an `ArtifactStore`.
//! The bytes are canonical encodings as defined by `ContextAnchorsV1::encode`.

use crate::artifact::{ArtifactError, ArtifactStore};
use crate::codec::{DecodeError, EncodeError};
use crate::context_anchors::ContextAnchorsV1;
use crate::hash::Hash32;
use core::fmt;

/// Errors while storing or loading a ContextAnchorsV1 artifact.
#[derive(Debug)]
pub enum ContextAnchorsArtifactError {
    /// ContextAnchors could not be encoded.
    Encode(EncodeError),
    /// ContextAnchors could not be decoded.
    Decode(DecodeError),
    /// Artifact store error.
    Store(ArtifactError),
}

impl fmt::Display for ContextAnchorsArtifactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ContextAnchorsArtifactError::Encode(e) => write!(f, "encode: {}", e),
            ContextAnchorsArtifactError::Decode(e) => write!(f, "decode: {}", e),
            ContextAnchorsArtifactError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl std::error::Error for ContextAnchorsArtifactError {}

/// Store `ContextAnchorsV1` as a content-addressed artifact.
pub fn put_context_anchors_v1<S: ArtifactStore>(
    store: &S,
    ca: &ContextAnchorsV1,
) -> Result<Hash32, ContextAnchorsArtifactError> {
    let bytes = ca.encode().map_err(ContextAnchorsArtifactError::Encode)?;
    store.put(&bytes).map_err(ContextAnchorsArtifactError::Store)
}

/// Load a `ContextAnchorsV1` artifact by its content hash.
///
/// Returns None if the artifact is not present in the store.
pub fn get_context_anchors_v1<S: ArtifactStore>(
    store: &S,
    id: &Hash32,
) -> Result<Option<ContextAnchorsV1>, ContextAnchorsArtifactError> {
    let bytes_opt = store.get(id).map_err(ContextAnchorsArtifactError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let ca = ContextAnchorsV1::decode(&bytes).map_err(ContextAnchorsArtifactError::Decode)?;
    Ok(Some(ca))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
    use crate::context_anchors::{ContextAnchorTermV1, CONTEXT_ANCHORS_V1_VERSION};
    use crate::frame::Id64;

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        let mut base = std::env::temp_dir();
        base.push("fsa_lm_tests");
        base.push(name);
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).expect("tmp create_dir_all");
        base
    }

    #[test]
    fn context_anchors_artifact_roundtrip() {
        let root = tmp_dir("context_anchors_artifact_roundtrip");
        let store = FsArtifactStore::new(&root).unwrap();

        let ca = ContextAnchorsV1 {
            version: CONTEXT_ANCHORS_V1_VERSION,
            prompt_id: [5u8; 32],
            query_msg_ix: 2,
            flags: 0,
            source_hash: [9u8; 32],
            terms: vec![ContextAnchorTermV1 { term_id: Id64(7), qtf: 1 }],
        };

        let id = put_context_anchors_v1(&store, &ca).expect("put");
        let got = get_context_anchors_v1(&store, &id).expect("get").unwrap();
        assert_eq!(got, ca);

        let _ = std::fs::remove_dir_all(&root);
    }
}
