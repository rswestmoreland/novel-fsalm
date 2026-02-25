// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! MarkovHintsV1 artifact helpers.

use crate::artifact::{ArtifactError, ArtifactStore};
use crate::codec::{DecodeError, EncodeError};
use crate::hash::Hash32;
use crate::markov_hints::MarkovHintsV1;
use core::fmt;

/// Errors while storing or loading a MarkovHintsV1 artifact.
#[derive(Debug)]
pub enum MarkovHintsArtifactError {
    /// Hints could not be encoded.
    Encode(EncodeError),
    /// Hints could not be decoded.
    Decode(DecodeError),
    /// Artifact store error.
    Store(ArtifactError),
}

impl fmt::Display for MarkovHintsArtifactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MarkovHintsArtifactError::Encode(e) => write!(f, "encode: {}", e),
            MarkovHintsArtifactError::Decode(e) => write!(f, "decode: {}", e),
            MarkovHintsArtifactError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl std::error::Error for MarkovHintsArtifactError {}

/// Store a MarkovHintsV1 record as a content-addressed artifact.
pub fn put_markov_hints_v1<S: ArtifactStore>(
    store: &S,
    hints: &MarkovHintsV1,
) -> Result<Hash32, MarkovHintsArtifactError> {
    let bytes = hints.encode().map_err(MarkovHintsArtifactError::Encode)?;
    store.put(&bytes).map_err(MarkovHintsArtifactError::Store)
}

/// Load a MarkovHintsV1 by hash.
///
/// Returns None if the artifact is not present in the store.
pub fn get_markov_hints_v1<S: ArtifactStore>(
    store: &S,
    hash: &Hash32,
) -> Result<Option<MarkovHintsV1>, MarkovHintsArtifactError> {
    let bytes_opt = store.get(hash).map_err(MarkovHintsArtifactError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let hints = MarkovHintsV1::decode(&bytes).map_err(MarkovHintsArtifactError::Decode)?;
    Ok(Some(hints))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
    use crate::hash::blake3_hash;
    use crate::markov_hints::{MarkovChoiceKindV1, MarkovChoiceV1, MARKOV_HINTS_V1_VERSION};
    use std::sync::atomic::{AtomicUsize, Ordering};

    static TMP_I: AtomicUsize = AtomicUsize::new(0);

    fn temp_store() -> FsArtifactStore {
        let i = TMP_I.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("fsa_lm_markov_hints_artifact_{i}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create_dir_all");
        FsArtifactStore::new(dir).unwrap()
    }

    fn sample_hints() -> MarkovHintsV1 {
        let qid = blake3_hash(b"q");
        MarkovHintsV1 {
            version: MARKOV_HINTS_V1_VERSION,
            query_id: qid,
            flags: 0,
            order_n: 2,
            state_id: crate::frame::Id64(7),
            model_hash: blake3_hash(b"model"),
            context_hash: blake3_hash(b"ctx"),
            choices: vec![MarkovChoiceV1::new(
                MarkovChoiceKindV1::Opener,
                crate::frame::Id64(1),
                10,
                0,
            )],
        }
    }

    #[test]
    fn hints_artifact_roundtrip() {
        let store = temp_store();
        let h1 = sample_hints();
        let hash = put_markov_hints_v1(&store, &h1).expect("put");
        let h2 = get_markov_hints_v1(&store, &hash).expect("get").unwrap();
        assert_eq!(h1, h2);
    }
}
