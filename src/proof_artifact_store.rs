// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Proof artifact store helpers.
//!
//! ProofArtifactV1 is content-addressed bytes stored via ArtifactStore.

use crate::artifact::{ArtifactError, ArtifactStore};
use crate::codec::{DecodeError, EncodeError};
use crate::hash::Hash32;
use crate::proof_artifact::ProofArtifactV1;

/// Store a ProofArtifactV1 as an artifact.
pub fn put_proof_artifact_v1<S: ArtifactStore>(
    store: &S,
    proof: &ProofArtifactV1,
) -> Result<Hash32, ProofArtifactStoreError> {
    let bytes = proof.encode().map_err(ProofArtifactStoreError::Encode)?;
    store.put(&bytes).map_err(ProofArtifactStoreError::Store)
}

/// Load and decode a ProofArtifactV1 artifact.
pub fn get_proof_artifact_v1<S: ArtifactStore>(
    store: &S,
    hash: &Hash32,
) -> Result<Option<ProofArtifactV1>, ProofArtifactStoreError> {
    let bytes_opt = store.get(hash).map_err(ProofArtifactStoreError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let proof = ProofArtifactV1::decode(&bytes).map_err(ProofArtifactStoreError::Decode)?;
    Ok(Some(proof))
}

/// Errors for proof artifact store helpers.
#[derive(Debug)]
pub enum ProofArtifactStoreError {
    /// ProofArtifact encoding error.
    Encode(EncodeError),
    /// ProofArtifact decoding error.
    Decode(DecodeError),
    /// Artifact store error.
    Store(ArtifactError),
}

impl core::fmt::Display for ProofArtifactStoreError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ProofArtifactStoreError::Encode(e) => write!(f, "encode: {}", e),
            ProofArtifactStoreError::Decode(e) => write!(f, "decode: {}", e),
            ProofArtifactStoreError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl std::error::Error for ProofArtifactStoreError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
    use crate::proof_artifact::{
        ConstraintV1, ProofSolveStatsV1, PROOF_ARTIFACT_V1_VERSION, PA_FLAG_EXPECT_UNIQUE,
    };
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn tmp_dir(prefix: &str) -> PathBuf {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let mut p = std::env::temp_dir();
        p.push("fsa_lm_tests");
        p.push(format!("{}_{}_{}", prefix, pid, n));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn proof_artifact_store_round_trip() {
        let root = tmp_dir("proof_artifact_store");
        let store = FsArtifactStore::new(&root).unwrap();

        let p = ProofArtifactV1 {
            version: PROOF_ARTIFACT_V1_VERSION,
            flags: PA_FLAG_EXPECT_UNIQUE,
            vars: vec!["A".to_string(), "B".to_string()],
            domain: vec![1, 2, 3],
            constraints: vec![ConstraintV1::NeqVarVar { a: 0, b: 1 }],
            solutions: Vec::new(),
            stats: ProofSolveStatsV1 { nodes: 0, backtracks: 0 },
        };

        let hid = put_proof_artifact_v1(&store, &p).unwrap();
        let got = get_proof_artifact_v1(&store, &hid).unwrap().unwrap();
        assert_eq!(p, got);
    }
}
