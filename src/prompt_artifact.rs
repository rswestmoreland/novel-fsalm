// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

// PromptPack artifact helpers.
//
// ASCII-only comments.
// No extra crates.
//
// Goal:
// - store PromptPack as an artifact
// - load and decode PromptPack by hash
// - keep canonicalization rules explicit and deterministic

use crate::artifact::ArtifactStore;
use crate::codec::{DecodeError, EncodeError};
use crate::hash::Hash32;
use crate::prompt_pack::{PromptLimits, PromptPack};

/// Store a PromptPack as an artifact.
///
/// Steps:
/// - canonicalize in-place under `limits`
/// - encode using the canonical fast path (no constraint clone/sort)
/// - store bytes in the artifact store
pub fn put_prompt_pack<S: ArtifactStore>(
    store: &S,
    pack: &mut PromptPack,
    limits: PromptLimits,
) -> Result<Hash32, PromptArtifactError> {
    pack.canonicalize_in_place(limits);
    let bytes = pack
        .encode_assuming_canonical()
        .map_err(PromptArtifactError::Encode)?;
    store.put(&bytes).map_err(PromptArtifactError::Store)
}

/// Load and decode a PromptPack artifact by hash.
pub fn get_prompt_pack<S: ArtifactStore>(
    store: &S,
    hash: &Hash32,
) -> Result<Option<PromptPack>, PromptArtifactError> {
    let bytes_opt = store.get(hash).map_err(PromptArtifactError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let pack = PromptPack::decode(&bytes).map_err(PromptArtifactError::Decode)?;
    Ok(Some(pack))
}

/// Errors for PromptPack artifact helpers.
#[derive(Debug)]
pub enum PromptArtifactError {
    /// PromptPack encoding error.
    Encode(EncodeError),
    /// PromptPack decoding error.
    Decode(DecodeError),
    /// Artifact store error.
    Store(crate::artifact::ArtifactError),
}

impl core::fmt::Display for PromptArtifactError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            PromptArtifactError::Encode(e) => write!(f, "encode: {}", e),
            PromptArtifactError::Decode(e) => write!(f, "decode: {}", e),
            PromptArtifactError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl std::error::Error for PromptArtifactError {}
