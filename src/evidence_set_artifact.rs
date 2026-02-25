// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! EvidenceSet artifact helpers.
//!
//! These helpers store and load EvidenceSetV1 artifacts using the
//! content-addressed ArtifactStore.

use crate::artifact::{ArtifactError, ArtifactStore};
use crate::codec::{DecodeError, EncodeError};
use crate::evidence_set::EvidenceSetV1;
use crate::hash::Hash32;
use core::fmt;

/// Errors while storing or loading an EvidenceSetV1 artifact.
#[derive(Debug)]
pub enum EvidenceSetArtifactError {
    /// EvidenceSet encoding error.
    Encode(EncodeError),
    /// EvidenceSet decoding error.
    Decode(DecodeError),
    /// Artifact store error.
    Store(ArtifactError),
}

impl fmt::Display for EvidenceSetArtifactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EvidenceSetArtifactError::Encode(e) => write!(f, "encode: {}", e),
            EvidenceSetArtifactError::Decode(e) => write!(f, "decode: {}", e),
            EvidenceSetArtifactError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl std::error::Error for EvidenceSetArtifactError {}

impl From<EncodeError> for EvidenceSetArtifactError {
    fn from(e: EncodeError) -> Self {
        EvidenceSetArtifactError::Encode(e)
    }
}

impl From<DecodeError> for EvidenceSetArtifactError {
    fn from(e: DecodeError) -> Self {
        EvidenceSetArtifactError::Decode(e)
    }
}

impl From<ArtifactError> for EvidenceSetArtifactError {
    fn from(e: ArtifactError) -> Self {
        EvidenceSetArtifactError::Store(e)
    }
}

/// Store an EvidenceSetV1 as a content-addressed artifact.
///
/// The set is canonically encoded before storing.
pub fn put_evidence_set_v1<S: ArtifactStore>(
    store: &S,
    set: &EvidenceSetV1,
) -> Result<Hash32, EvidenceSetArtifactError> {
    let bytes = set.encode()?;
    let h = store.put(&bytes)?;
    Ok(h)
}

/// Load and decode an EvidenceSetV1 artifact by hash.
///
/// Returns None if the artifact is not present in the store.
pub fn get_evidence_set_v1<S: ArtifactStore>(
    store: &S,
    hash: &Hash32,
) -> Result<Option<EvidenceSetV1>, EvidenceSetArtifactError> {
    let opt = store.get(hash)?;
    let bytes = match opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let set = EvidenceSetV1::decode(&bytes)?;
    Ok(Some(set))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push("fsa_lm_tests");
        p.push(name);
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn h(b: u8) -> Hash32 {
        let mut x = [0u8; 32];
        x[0] = b;
        x
    }

    #[test]
    fn evidence_set_artifact_round_trip() {
        let dir = tmp_dir("evidence_set_artifact_round_trip");
        let store = FsArtifactStore::new(&dir).unwrap();

        let set = EvidenceSetV1 {
            version: 1,
            evidence_bundle_id: h(9),
            items: vec![crate::evidence_set::EvidenceSetItemV1 {
                claim_id: 1,
                claim_text: "claim".to_string(),
                evidence_refs: vec![crate::evidence_set::EvidenceRowRefV1 {
                    segment_id: h(2),
                    row_ix: 7,
                    score: 123,
                }],
            }],
        };

        let hash = put_evidence_set_v1(&store, &set).unwrap();
        let got = get_evidence_set_v1(&store, &hash).unwrap().unwrap();
        assert_eq!(set, got);
    }
}
