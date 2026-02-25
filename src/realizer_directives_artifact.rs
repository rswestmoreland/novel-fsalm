// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! RealizerDirectives artifact helpers.
//!
//! These helpers store and load RealizerDirectivesV1 artifacts using the
//! content-addressed ArtifactStore.

use crate::artifact::{ArtifactError, ArtifactStore};
use crate::codec::{DecodeError, EncodeError};
use crate::hash::Hash32;
use crate::realizer_directives::RealizerDirectivesV1;
use core::fmt;

/// Errors while storing or loading a RealizerDirectivesV1 artifact.
#[derive(Debug)]
pub enum RealizerDirectivesArtifactError {
    /// RealizerDirectives encoding error.
    Encode(EncodeError),
    /// RealizerDirectives decoding error.
    Decode(DecodeError),
    /// Artifact store error.
    Store(ArtifactError),
}

impl fmt::Display for RealizerDirectivesArtifactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RealizerDirectivesArtifactError::Encode(e) => write!(f, "encode: {}", e),
            RealizerDirectivesArtifactError::Decode(e) => write!(f, "decode: {}", e),
            RealizerDirectivesArtifactError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl std::error::Error for RealizerDirectivesArtifactError {}

impl From<EncodeError> for RealizerDirectivesArtifactError {
    fn from(e: EncodeError) -> Self {
        RealizerDirectivesArtifactError::Encode(e)
    }
}

impl From<DecodeError> for RealizerDirectivesArtifactError {
    fn from(e: DecodeError) -> Self {
        RealizerDirectivesArtifactError::Decode(e)
    }
}

impl From<ArtifactError> for RealizerDirectivesArtifactError {
    fn from(e: ArtifactError) -> Self {
        RealizerDirectivesArtifactError::Store(e)
    }
}

/// Store a RealizerDirectivesV1 as a content-addressed artifact.
///
/// The directives are canonically encoded before storing.
pub fn put_realizer_directives_v1<S: ArtifactStore>(
    store: &S,
    d: &RealizerDirectivesV1,
) -> Result<Hash32, RealizerDirectivesArtifactError> {
    let bytes = d.encode()?;
    let h = store.put(&bytes)?;
    Ok(h)
}

/// Load and decode a RealizerDirectivesV1 artifact by hash.
///
/// Returns None if the artifact is not present in the store.
pub fn get_realizer_directives_v1<S: ArtifactStore>(
    store: &S,
    hash: &Hash32,
) -> Result<Option<RealizerDirectivesV1>, RealizerDirectivesArtifactError> {
    let opt = store.get(hash)?;
    let bytes = match opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let d = RealizerDirectivesV1::decode(&bytes)?;
    Ok(Some(d))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
    use crate::realizer_directives::{StyleV1, ToneV1};

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push("fsa_lm_tests");
        p.push(name);
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn realizer_directives_artifact_round_trip() {
        let dir = tmp_dir("realizer_directives_artifact_round_trip");
        let store = FsArtifactStore::new(&dir).unwrap();

        let d = RealizerDirectivesV1 {
            version: crate::realizer_directives::REALIZER_DIRECTIVES_V1_VERSION,
            tone: ToneV1::Neutral,
            style: StyleV1::Debug,
            format_flags: 0,
            max_softeners: 1,
            max_preface_sentences: 0,
            max_hedges: 2,
            max_questions: 0,
            rationale_codes: Vec::new(),
        };

        let h = put_realizer_directives_v1(&store, &d).unwrap();
        let got = get_realizer_directives_v1(&store, &h).unwrap().unwrap();
        assert_eq!(got, d);
    }
}
