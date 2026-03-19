// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! ConversationPackV1 artifact helpers.
//!
//! Goals:
//! - Store ConversationPackV1 as a content-addressed artifact.
//! - Load and decode ConversationPackV1 by hash.
//! - Keep canonicalization rules explicit and deterministic.

use crate::artifact::ArtifactStore;
use crate::codec::{DecodeError, EncodeError};
use crate::conversation_pack::ConversationPackV1;
use crate::hash::Hash32;

/// Store a ConversationPackV1 as an artifact.
///
/// Steps:
/// - canonicalize in-place using the recorded limits
/// - encode using the canonical fast path
/// - store bytes in the artifact store
pub fn put_conversation_pack<S: ArtifactStore>(
    store: &S,
    pack: &mut ConversationPackV1,
) -> Result<Hash32, ConversationPackArtifactError> {
    pack.canonicalize_in_place();
    let bytes = pack
        .encode_assuming_canonical()
        .map_err(ConversationPackArtifactError::Encode)?;
    store.put(&bytes).map_err(ConversationPackArtifactError::Store)
}

/// Load and decode a ConversationPackV1 artifact by hash.
pub fn get_conversation_pack<S: ArtifactStore>(
    store: &S,
    hash: &Hash32,
) -> Result<Option<ConversationPackV1>, ConversationPackArtifactError> {
    let bytes_opt = store.get(hash).map_err(ConversationPackArtifactError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let pack = ConversationPackV1::decode(&bytes).map_err(ConversationPackArtifactError::Decode)?;
    Ok(Some(pack))
}

/// Errors for ConversationPackV1 artifact helpers.
#[derive(Debug)]
pub enum ConversationPackArtifactError {
    /// Encoding error.
    Encode(EncodeError),
    /// Decoding error.
    Decode(DecodeError),
    /// Artifact store error.
    Store(crate::artifact::ArtifactError),
}

impl core::fmt::Display for ConversationPackArtifactError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ConversationPackArtifactError::Encode(e) => write!(f, "encode: {}", e),
            ConversationPackArtifactError::Decode(e) => write!(f, "decode: {}", e),
            ConversationPackArtifactError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl std::error::Error for ConversationPackArtifactError {}
