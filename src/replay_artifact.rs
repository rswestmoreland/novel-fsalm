// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

// ReplayLog artifact helpers.
//
// ASCII-only comments.
// No extra crates.
//
// Goal:
// - store ReplayLog as an artifact
// - load and decode ReplayLog by hash
// - provide small helpers for common step conventions

use crate::artifact::ArtifactStore;
use crate::codec::{DecodeError, EncodeError};
use crate::hash::Hash32;
use crate::replay::{ReplayLog, ReplayStep};

/// Store a ReplayLog as an artifact.
///
/// ReplayLog encoding is canonical (it sorts inputs/outputs per step during encode).
pub fn put_replay_log<S: ArtifactStore>(
    store: &S,
    log: &ReplayLog,
) -> Result<Hash32, ReplayArtifactError> {
    let bytes = log.encode().map_err(ReplayArtifactError::Encode)?;
    store.put(&bytes).map_err(ReplayArtifactError::Store)
}

/// Load and decode a ReplayLog artifact by hash.
pub fn get_replay_log<S: ArtifactStore>(
    store: &S,
    hash: &Hash32,
) -> Result<Option<ReplayLog>, ReplayArtifactError> {
    let bytes_opt = store.get(hash).map_err(ReplayArtifactError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let log = ReplayLog::decode(&bytes).map_err(ReplayArtifactError::Decode)?;
    Ok(Some(log))
}

/// Append a "prompt created" step convention to an existing ReplayLog.
///
/// Convention:
/// - name = step_name (default is "prompt")
/// - inputs = empty
/// - outputs = [prompt_pack_hash]
pub fn append_prompt_step(log: &mut ReplayLog, step_name: &str, prompt_hash: Hash32) {
    log.steps.push(ReplayStep {
        name: step_name.to_string(),
        inputs: Vec::new(),
        outputs: vec![prompt_hash],
    });
}

/// Errors for ReplayLog artifact helpers.
#[derive(Debug)]
pub enum ReplayArtifactError {
    /// ReplayLog encoding error.
    Encode(EncodeError),
    /// ReplayLog decoding error.
    Decode(DecodeError),
    /// Artifact store error.
    Store(crate::artifact::ArtifactError),
}

impl core::fmt::Display for ReplayArtifactError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ReplayArtifactError::Encode(e) => write!(f, "encode: {}", e),
            ReplayArtifactError::Decode(e) => write!(f, "decode: {}", e),
            ReplayArtifactError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl std::error::Error for ReplayArtifactError {}
