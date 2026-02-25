// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! MarkovTraceV1 artifact helpers.

use crate::artifact::{ArtifactError, ArtifactStore};
use crate::codec::{DecodeError, EncodeError};
use crate::hash::Hash32;
use crate::markov_trace::MarkovTraceV1;
use core::fmt;

/// Errors while storing or loading a MarkovTraceV1 artifact.
#[derive(Debug)]
pub enum MarkovTraceArtifactError {
    /// Trace could not be encoded.
    Encode(EncodeError),
    /// Trace could not be decoded.
    Decode(DecodeError),
    /// Artifact store error.
    Store(ArtifactError),
}

impl fmt::Display for MarkovTraceArtifactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MarkovTraceArtifactError::Encode(e) => write!(f, "encode: {}", e),
            MarkovTraceArtifactError::Decode(e) => write!(f, "decode: {}", e),
            MarkovTraceArtifactError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl std::error::Error for MarkovTraceArtifactError {}

/// Store a MarkovTraceV1 as a content-addressed artifact.
pub fn put_markov_trace_v1<S: ArtifactStore>(
    store: &S,
    trace: &MarkovTraceV1,
) -> Result<Hash32, MarkovTraceArtifactError> {
    let bytes = trace.encode().map_err(MarkovTraceArtifactError::Encode)?;
    store.put(&bytes).map_err(MarkovTraceArtifactError::Store)
}

/// Load a MarkovTraceV1 by hash.
///
/// Returns None if the artifact is not present in the store.
pub fn get_markov_trace_v1<S: ArtifactStore>(
    store: &S,
    hash: &Hash32,
) -> Result<Option<MarkovTraceV1>, MarkovTraceArtifactError> {
    let bytes_opt = store.get(hash).map_err(MarkovTraceArtifactError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let trace = MarkovTraceV1::decode(&bytes).map_err(MarkovTraceArtifactError::Decode)?;
    Ok(Some(trace))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
    use crate::hash::blake3_hash;
    use crate::markov_hints::MarkovChoiceKindV1;
    use crate::markov_model::MarkovTokenV1;
    use crate::markov_trace::{MARKOV_TRACE_V1_VERSION};
    use std::sync::atomic::{AtomicUsize, Ordering};

    static TMP_I: AtomicUsize = AtomicUsize::new(0);

    fn temp_store() -> FsArtifactStore {
        let i = TMP_I.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("fsa_lm_markov_trace_artifact_{i}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create_dir_all");
        FsArtifactStore::new(dir).unwrap()
    }

    #[test]
    fn trace_artifact_roundtrip() {
        let store = temp_store();
        let tr = MarkovTraceV1 {
            version: MARKOV_TRACE_V1_VERSION,
            query_id: blake3_hash(b"q"),
            tokens: vec![
                MarkovTokenV1::new(MarkovChoiceKindV1::Opener, crate::frame::Id64(1)),
                MarkovTokenV1::new(MarkovChoiceKindV1::Closer, crate::frame::Id64(2)),
            ],
        };
        let h = put_markov_trace_v1(&store, &tr).expect("put");
        let tr2 = get_markov_trace_v1(&store, &h).expect("get").unwrap();
        assert_eq!(tr, tr2);
    }
}
