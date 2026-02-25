// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! ForecastV1 artifact helpers.
//!
//! Stores and retrieves `ForecastV1` objects in an `ArtifactStore`.
//! The bytes are canonical encodings as defined by `ForecastV1::encode`.

use crate::artifact::{ArtifactError, ArtifactStore};
use crate::codec::{DecodeError, EncodeError};
use crate::forecast::ForecastV1;
use crate::hash::Hash32;
use core::fmt;

/// Errors while storing or loading a ForecastV1 artifact.
#[derive(Debug)]
pub enum ForecastArtifactError {
    /// Forecast could not be encoded.
    Encode(EncodeError),
    /// Forecast could not be decoded.
    Decode(DecodeError),
    /// Artifact store error.
    Store(ArtifactError),
}

impl fmt::Display for ForecastArtifactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ForecastArtifactError::Encode(e) => write!(f, "encode: {}", e),
            ForecastArtifactError::Decode(e) => write!(f, "decode: {}", e),
            ForecastArtifactError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl std::error::Error for ForecastArtifactError {}

/// Store `ForecastV1` as a content-addressed artifact.
pub fn put_forecast_v1<S: ArtifactStore>(
    store: &S,
    fc: &ForecastV1,
) -> Result<Hash32, ForecastArtifactError> {
    let bytes = fc.encode().map_err(ForecastArtifactError::Encode)?;
    store.put(&bytes).map_err(ForecastArtifactError::Store)
}

/// Load a `ForecastV1` artifact by its content hash.
///
/// Returns None if the artifact is not present in the store.
pub fn get_forecast_v1<S: ArtifactStore>(
    store: &S,
    id: &Hash32,
) -> Result<Option<ForecastV1>, ForecastArtifactError> {
    let bytes_opt = store.get(id).map_err(ForecastArtifactError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let fc = ForecastV1::decode(&bytes).map_err(ForecastArtifactError::Decode)?;
    Ok(Some(fc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
    use crate::forecast::FORECAST_V1_VERSION;

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        let mut base = std::env::temp_dir();
        base.push("fsa_lm_tests");
        base.push(name);
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).expect("tmp create_dir_all");
        base
    }

    #[test]
    fn forecast_artifact_roundtrip() {
        let root = tmp_dir("forecast_artifact_roundtrip");
        let store = FsArtifactStore::new(&root).unwrap();

        let fc = ForecastV1 {
            version: FORECAST_V1_VERSION,
            query_id: [2u8; 32],
            flags: 0,
            horizon_turns: 1,
            intents: Vec::new(),
            questions: Vec::new(),
        };

        let id = put_forecast_v1(&store, &fc).expect("put");
        let got = get_forecast_v1(&store, &id).expect("get").unwrap();
        assert_eq!(got, fc);

        let _ = std::fs::remove_dir_all(&root);
    }
}
