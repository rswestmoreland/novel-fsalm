// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! PragmaticsFrame persistence helpers.
//!
//! PragmaticsFrames are immutable, content-addressed artifacts. This module
//! provides artifact-store helpers for storing and loading `PragmaticsFrameV1`
//! by content hash.
//!
//! Cold storage: `ArtifactStore`
//! Warm cache: `Cache2Q` (read-through)
//!
//! ASCII-only comments.
//! No extra crates.

use crate::artifact::{ArtifactError, ArtifactStore};
use crate::cache::Cache2Q;
use crate::codec::{DecodeError, EncodeError};
use crate::hash::Hash32;
use crate::pragmatics_frame::PragmaticsFrameV1;

/// PragmaticsFrame artifact helper errors.
#[derive(Debug)]
pub enum PragmaticsFrameStoreError {
    /// Encode failure.
    Encode(EncodeError),
    /// Decode failure.
    Decode(DecodeError),
    /// Artifact store error.
    Store(ArtifactError),
}

impl core::fmt::Display for PragmaticsFrameStoreError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            PragmaticsFrameStoreError::Encode(e) => write!(f, "encode: {}", e),
            PragmaticsFrameStoreError::Decode(e) => write!(f, "decode: {}", e),
            PragmaticsFrameStoreError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl std::error::Error for PragmaticsFrameStoreError {}

/// Store a PragmaticsFrameV1 as an artifact.
pub fn put_pragmatics_frame_v1<S: ArtifactStore>(
    store: &S,
    frame: &PragmaticsFrameV1,
) -> Result<Hash32, PragmaticsFrameStoreError> {
    let bytes = frame.encode().map_err(PragmaticsFrameStoreError::Encode)?;
    let h = store.put(&bytes).map_err(PragmaticsFrameStoreError::Store)?;
    Ok(h)
}

/// Load and decode a PragmaticsFrameV1 artifact by hash.
///
/// Returns None if not found.
pub fn get_pragmatics_frame_v1<S: ArtifactStore>(
    store: &S,
    hash: &Hash32,
) -> Result<Option<PragmaticsFrameV1>, PragmaticsFrameStoreError> {
    let bytes_opt = store.get(hash).map_err(PragmaticsFrameStoreError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let frame = PragmaticsFrameV1::decode(&bytes).map_err(PragmaticsFrameStoreError::Decode)?;
    Ok(Some(frame))
}

/// Load and decode a PragmaticsFrameV1 artifact by hash, using a read-through cache.
///
/// Cache key: artifact hash.
/// Cache value: PragmaticsFrameV1.
/// Cost bytes: encoded artifact length.
///
/// If the artifact is not present in the store, returns Ok(None).
/// If the artifact is too large for the cache, it is still returned.
pub fn get_pragmatics_frame_v1_cached<S: ArtifactStore>(
    store: &S,
    cache: &mut Cache2Q<Hash32, PragmaticsFrameV1>,
    hash: &Hash32,
) -> Result<Option<PragmaticsFrameV1>, PragmaticsFrameStoreError> {
    if let Some(v) = cache.get(hash) {
        return Ok(Some(*v));
    }

    let bytes_opt = store.get(hash).map_err(PragmaticsFrameStoreError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };

    let cost: u64 = bytes.len() as u64;
    let frame = PragmaticsFrameV1::decode(&bytes).map_err(PragmaticsFrameStoreError::Decode)?;

    let _ = cache.insert_cost(*hash, frame, cost);

    Ok(Some(frame))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
    use crate::cache::CacheCfgV1;
    use crate::frame::Id64;
    use crate::pragmatics_frame::{
        IntentFlagsV1, PragmaticsFrameV1, RhetoricModeV1, INTENT_FLAG_HAS_CONSTRAINTS,
        INTENT_FLAG_HAS_QUESTION, PRAGMATICS_FRAME_V1_VERSION,
    };

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        use std::fs;
        let mut p = std::env::temp_dir();
        p.push("fsa_lm_tests");
        p.push(name);
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn sample_frame() -> PragmaticsFrameV1 {
        let flags: IntentFlagsV1 = INTENT_FLAG_HAS_QUESTION | INTENT_FLAG_HAS_CONSTRAINTS;
        PragmaticsFrameV1 {
            version: PRAGMATICS_FRAME_V1_VERSION,
            source_id: Id64(1),
            msg_ix: 0,
            byte_len: 64,
            ascii_only: 1,
            temperature: 200,
            valence: -50,
            arousal: 100,
            politeness: 800,
            formality: 100,
            directness: 900,
            empathy_need: 0,
            mode: RhetoricModeV1::Ask,
            flags,
            exclamations: 1,
            questions: 1,
            ellipses: 1,
            caps_words: 1,
            repeat_punct_runs: 1,
            quotes: 0,
            emphasis_score: 400,
            hedge_count: 0,
            intensifier_count: 0,
            profanity_count: 0,
            apology_count: 0,
            gratitude_count: 0,
            insult_count: 0,
        }
    }

    #[test]
    fn pragmatics_frame_store_round_trip() {
        let dir = tmp_dir("pragmatics_frame_store_round_trip");
        let store = FsArtifactStore::new(&dir).unwrap();

        let frame = sample_frame();
        let h = put_pragmatics_frame_v1(&store, &frame).unwrap();
        let got = get_pragmatics_frame_v1(&store, &h).unwrap().unwrap();
        assert_eq!(got, frame);
    }

    #[test]
    fn pragmatics_frame_store_cached_hit_on_second_get() {
        let dir = tmp_dir("pragmatics_frame_store_cached_hit_on_second_get");
        let store = FsArtifactStore::new(&dir).unwrap();

        let frame = sample_frame();
        let h = put_pragmatics_frame_v1(&store, &frame).unwrap();

        let mut cache: Cache2Q<Hash32, PragmaticsFrameV1> = Cache2Q::new(CacheCfgV1::new(1_000_000));

        let a1 = get_pragmatics_frame_v1_cached(&store, &mut cache, &h).unwrap().unwrap();
        let st1 = cache.stats();
        let a2 = get_pragmatics_frame_v1_cached(&store, &mut cache, &h).unwrap().unwrap();
        let st2 = cache.stats();

        assert_eq!(a1, frame);
        assert_eq!(a2, frame);

        assert_eq!(st1.misses, 1);
        assert_eq!(st2.misses, 1);
        assert_eq!(st2.hits_a1 + st2.hits_am, 1);
    }
}
