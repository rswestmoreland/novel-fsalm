// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! EvidenceBundle artifact helpers.
//!
//! These helpers store and load EvidenceBundleV1 artifacts using the
//! content-addressed ArtifactStore.

use crate::artifact::{ArtifactError, ArtifactStore};
use crate::codec::{DecodeError, EncodeError};
use crate::evidence_bundle::EvidenceBundleV1;
use crate::hash::Hash32;

/// Store an EvidenceBundleV1 as an artifact.
///
/// The bundle must already be canonical. Use EvidenceBundleV1::encode if you
/// want canonicalization before storing.
pub fn put_evidence_bundle_v1<S: ArtifactStore>(
    store: &S,
    bundle: &EvidenceBundleV1,
) -> Result<Hash32, EvidenceArtifactError> {
    let bytes = bundle
        .encode_assuming_canonical()
        .map_err(EvidenceArtifactError::Encode)?;
    store.put(&bytes).map_err(EvidenceArtifactError::Store)
}

/// Load and decode an EvidenceBundleV1 artifact by hash.
///
/// Returns None if the artifact is not present in the store.
pub fn get_evidence_bundle_v1<S: ArtifactStore>(
    store: &S,
    hash: &Hash32,
) -> Result<Option<EvidenceBundleV1>, EvidenceArtifactError> {
    let bytes_opt = store.get(hash).map_err(EvidenceArtifactError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let bundle = EvidenceBundleV1::decode(&bytes).map_err(EvidenceArtifactError::Decode)?;
    Ok(Some(bundle))
}

/// Errors for EvidenceBundle artifact helpers.
#[derive(Debug)]
pub enum EvidenceArtifactError {
    /// EvidenceBundle encoding error.
    Encode(EncodeError),
    /// EvidenceBundle decoding error.
    Decode(DecodeError),
    /// Artifact store error.
    Store(ArtifactError),
}

impl core::fmt::Display for EvidenceArtifactError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            EvidenceArtifactError::Encode(e) => write!(f, "encode: {}", e),
            EvidenceArtifactError::Decode(e) => write!(f, "decode: {}", e),
            EvidenceArtifactError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl std::error::Error for EvidenceArtifactError {}
