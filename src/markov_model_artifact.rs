// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! MarkovModelV1 artifact helpers.

use crate::artifact::{ArtifactError, ArtifactStore};
use crate::codec::{DecodeError, EncodeError};
use crate::hash::Hash32;
use crate::markov_model::MarkovModelV1;
use core::fmt;

/// Errors while storing or loading a MarkovModelV1 artifact.
#[derive(Debug)]
pub enum MarkovModelArtifactError {
    /// Model could not be encoded.
    Encode(EncodeError),
    /// Model could not be decoded.
    Decode(DecodeError),
    /// Artifact store error.
    Store(ArtifactError),
}

impl fmt::Display for MarkovModelArtifactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MarkovModelArtifactError::Encode(e) => write!(f, "encode: {}", e),
            MarkovModelArtifactError::Decode(e) => write!(f, "decode: {}", e),
            MarkovModelArtifactError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl std::error::Error for MarkovModelArtifactError {}

/// Store a MarkovModelV1 as a content-addressed artifact.
pub fn put_markov_model_v1<S: ArtifactStore>(
    store: &S,
    model: &MarkovModelV1,
) -> Result<Hash32, MarkovModelArtifactError> {
    let bytes = model.encode().map_err(MarkovModelArtifactError::Encode)?;
    store.put(&bytes).map_err(MarkovModelArtifactError::Store)
}

/// Load a MarkovModelV1 by hash.
///
/// Returns None if the artifact is not present in the store.
pub fn get_markov_model_v1<S: ArtifactStore>(
    store: &S,
    hash: &Hash32,
) -> Result<Option<MarkovModelV1>, MarkovModelArtifactError> {
    let bytes_opt = store.get(hash).map_err(MarkovModelArtifactError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let model = MarkovModelV1::decode(&bytes).map_err(MarkovModelArtifactError::Decode)?;
    Ok(Some(model))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
    use crate::hash::blake3_hash;
    use crate::markov_hints::MarkovChoiceKindV1;
    use crate::markov_model::{
        MarkovNextV1, MarkovStateV1, MarkovTokenV1, MARKOV_MODEL_V1_VERSION,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    static TMP_I: AtomicUsize = AtomicUsize::new(0);

    fn temp_store() -> FsArtifactStore {
        let i = TMP_I.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("fsa_lm_markov_model_artifact_{i}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create_dir_all");
        FsArtifactStore::new(dir).unwrap()
    }

    fn sample_model() -> MarkovModelV1 {
        let tok = MarkovTokenV1::new(MarkovChoiceKindV1::Opener, crate::frame::Id64(7));
        let st = MarkovStateV1 {
            context: vec![tok],
            escape_count: 0,
            next: vec![MarkovNextV1 {
                token: MarkovTokenV1::new(MarkovChoiceKindV1::Closer, crate::frame::Id64(9)),
                count: 3,
            }],
        };
        MarkovModelV1 {
            version: MARKOV_MODEL_V1_VERSION,
            order_n_max: 2,
            max_next_per_state: 8,
            corpus_hash: blake3_hash(b"corpus"),
            total_transitions: 3,
            states: vec![st],
        }
    }

    #[test]
    fn model_artifact_roundtrip() {
        let store = temp_store();
        let m = sample_model();
        let h = put_markov_model_v1(&store, &m).expect("put");
        let m2 = get_markov_model_v1(&store, &h).expect("get").unwrap();
        assert_eq!(m, m2);
    }
}
