// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Retrieval control signals.
//!
//! Pragmatics is a deterministic coprocessor that produces per-message control
//! signals (tone, tact, emphasis). Retrieval should remain evidence-first: the
//! selected evidence must not change based on style preferences.
//!
//! However, the retrieval and planning pipeline needs an explicit attachment
//! point for control signals so later stages can:
//! - apply deterministic tie-break rules (no changes to evidence selection)
//! - choose answer rendering modes and tactics
//! - preserve replayability by recording which control signals were in effect
//!
//! v1 keeps this minimal: it references a PromptPack artifact id and the
//! PragmaticsFrameV1 artifact ids derived from that PromptPack.
//!
//! This module is schema-only plus light validation and stable hashing.

use crate::hash::{blake3_hash, Hash32};

/// RetrievalControlV1 schema version.
pub const RETRIEVAL_CONTROL_V1_VERSION: u16 = 1;

/// Errors returned by [`RetrievalControlV1::validate`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetrievalControlError {
    /// The version field is not supported.
    BadVersion,
    /// Too many referenced pragmatics frames.
    TooManyFrames,
    /// Pragmatics frame ids must not contain duplicates.
    DuplicateFrameId,
}

impl core::fmt::Display for RetrievalControlError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            RetrievalControlError::BadVersion => f.write_str("bad version"),
            RetrievalControlError::TooManyFrames => f.write_str("too many pragmatics frames"),
            RetrievalControlError::DuplicateFrameId => f.write_str("duplicate pragmatics frame id"),
        }
    }
}

/// Retrieval control-signal attachment point.
///
/// Canonical rules (v1):
/// - `version` must be [`RETRIEVAL_CONTROL_V1_VERSION`]
/// - `pragmatics_frame_ids` is ordered by message index (ascending)
/// - `pragmatics_frame_ids` has no duplicates
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetrievalControlV1 {
    /// Schema version.
    pub version: u16,
    /// PromptPack artifact id that produced these control signals.
    pub prompt_id: Hash32,
    /// PragmaticsFrameV1 artifact ids in message order.
    pub pragmatics_frame_ids: Vec<Hash32>,
}

impl RetrievalControlV1 {
    /// Create an empty control record for a prompt.
    pub fn new(prompt_id: Hash32) -> RetrievalControlV1 {
        RetrievalControlV1 {
            version: RETRIEVAL_CONTROL_V1_VERSION,
            prompt_id,
            pragmatics_frame_ids: Vec::new(),
        }
    }

    /// Validate canonical invariants.
    pub fn validate(&self) -> Result<(), RetrievalControlError> {
        if self.version != RETRIEVAL_CONTROL_V1_VERSION {
            return Err(RetrievalControlError::BadVersion);
        }
        // Hard cap to keep this bounded and replay-friendly.
        if self.pragmatics_frame_ids.len() > 16_384 {
            return Err(RetrievalControlError::TooManyFrames);
        }

        // Enforce no duplicates while preserving message order.
        // v1 uses O(n^2) to avoid bringing in a hash set in this schema module.
        for i in 0..self.pragmatics_frame_ids.len() {
            let a = &self.pragmatics_frame_ids[i];
            for j in (i + 1)..self.pragmatics_frame_ids.len() {
                if a == &self.pragmatics_frame_ids[j] {
                    return Err(RetrievalControlError::DuplicateFrameId);
                }
            }
        }
        Ok(())
    }

    /// Compute a stable id for this control record.
    ///
    /// This is intended for replay logs and deterministic tie-breaking.
    pub fn control_id(&self) -> Hash32 {
        // Versioned domain separation.
        let mut buf: Vec<u8> = Vec::with_capacity(32 + 2 + (self.pragmatics_frame_ids.len() * 32));
        buf.extend_from_slice(b"retrieval-control-v1\0");
        buf.extend_from_slice(&self.version.to_le_bytes());
        buf.extend_from_slice(&self.prompt_id);
        for h in self.pragmatics_frame_ids.iter() {
            buf.extend_from_slice(h);
        }
        blake3_hash(&buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_id_is_stable_and_sensitive_to_order() {
        let p = blake3_hash(b"prompt");
        let a = blake3_hash(b"a");
        let b = blake3_hash(b"b");

        let mut c1 = RetrievalControlV1::new(p);
        c1.pragmatics_frame_ids.push(a);
        c1.pragmatics_frame_ids.push(b);
        assert!(c1.validate().is_ok());
        let id1 = c1.control_id();

        let mut c2 = RetrievalControlV1::new(p);
        c2.pragmatics_frame_ids.push(a);
        c2.pragmatics_frame_ids.push(b);
        let id2 = c2.control_id();
        assert_eq!(id1, id2);

        let mut c3 = RetrievalControlV1::new(p);
        c3.pragmatics_frame_ids.push(b);
        c3.pragmatics_frame_ids.push(a);
        let id3 = c3.control_id();
        assert_ne!(id1, id3);
    }

    #[test]
    fn validate_rejects_duplicates() {
        let p = blake3_hash(b"prompt");
        let a = blake3_hash(b"a");

        let mut c = RetrievalControlV1::new(p);
        c.pragmatics_frame_ids.push(a);
        c.pragmatics_frame_ids.push(a);
        assert_eq!(c.validate(), Err(RetrievalControlError::DuplicateFrameId));
    }
}
