// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! LexiconSegment persistence helpers.
//!
//! LexiconSegments are immutable, content-addressed artifacts built from
//! Wiktionary lexicon rows. This module provides artifact-store helpers for
//! storing and loading `LexiconSegmentV1` by content hash.
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
use crate::lexicon_segment::LexiconSegmentV1;
use std::sync::Arc;

/// LexiconSegment artifact helper errors.
#[derive(Debug)]
pub enum LexiconSegmentStoreError {
    /// Encode failure.
    Encode(EncodeError),
    /// Decode failure.
    Decode(DecodeError),
    /// Artifact store error.
    Store(ArtifactError),
}

impl core::fmt::Display for LexiconSegmentStoreError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            LexiconSegmentStoreError::Encode(e) => write!(f, "encode: {}", e),
            LexiconSegmentStoreError::Decode(e) => write!(f, "decode: {}", e),
            LexiconSegmentStoreError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl std::error::Error for LexiconSegmentStoreError {}

/// Store a LexiconSegmentV1 as an artifact.
///
/// The segment encoding is canonical, so the returned hash is stable for the
/// same logical segment content.
pub fn put_lexicon_segment_v1<S: ArtifactStore>(
    store: &S,
    seg: &LexiconSegmentV1,
) -> Result<Hash32, LexiconSegmentStoreError> {
    let bytes = seg.encode().map_err(LexiconSegmentStoreError::Encode)?;
    let h = store.put(&bytes).map_err(LexiconSegmentStoreError::Store)?;
    Ok(h)
}

/// Load and decode a LexiconSegmentV1 artifact by hash.
///
/// Returns None if not found.
pub fn get_lexicon_segment_v1<S: ArtifactStore>(
    store: &S,
    hash: &Hash32,
) -> Result<Option<LexiconSegmentV1>, LexiconSegmentStoreError> {
    let bytes_opt = store.get(hash).map_err(LexiconSegmentStoreError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let seg = LexiconSegmentV1::decode(&bytes).map_err(LexiconSegmentStoreError::Decode)?;
    Ok(Some(seg))
}

/// Load and decode a LexiconSegmentV1 artifact by hash, using a read-through cache.
///
/// Cache key: segment artifact hash.
/// Cache value: Arc<LexiconSegmentV1>.
/// Cost bytes: encoded artifact length.
///
/// If the segment is not present in the store, returns Ok(None).
/// If the segment is too large for the cache, it is still returned.
pub fn get_lexicon_segment_v1_cached<S: ArtifactStore>(
    store: &S,
    cache: &mut Cache2Q<Hash32, Arc<LexiconSegmentV1>>,
    hash: &Hash32,
) -> Result<Option<Arc<LexiconSegmentV1>>, LexiconSegmentStoreError> {
    if let Some(v) = cache.get(hash) {
        return Ok(Some(v.clone()));
    }

    let bytes_opt = store.get(hash).map_err(LexiconSegmentStoreError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };

    let cost: u64 = bytes.len() as u64;
    let seg = LexiconSegmentV1::decode(&bytes).map_err(LexiconSegmentStoreError::Decode)?;
    let arc = Arc::new(seg);

    let _ = cache.insert_cost(*hash, arc.clone(), cost);

    Ok(Some(arc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
    use crate::cache::CacheCfgV1;
    use crate::frame::Id64;
    use crate::lexicon::{LemmaRowV1, LemmaId, LemmaKeyId, TextId, LEXICON_SCHEMA_V1};

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        use std::fs;
        let mut p = std::env::temp_dir();
        p.push("fsa_lm_tests");
        p.push(name);
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn sample_segment() -> LexiconSegmentV1 {
        let lemmas = vec![LemmaRowV1 {
            version: LEXICON_SCHEMA_V1,
            lemma_id: LemmaId(Id64(1)),
            lemma_key_id: LemmaKeyId(Id64(2)),
            lemma_text_id: TextId(Id64(3)),
            pos_mask: 0x1,
            flags: 0,
        }];
        LexiconSegmentV1::build_from_rows(&lemmas, &[], &[], &[]).unwrap()
    }

    #[test]
    fn lexicon_segment_store_round_trip() {
        let dir = tmp_dir("lexicon_segment_store_round_trip");
        let store = FsArtifactStore::new(&dir).unwrap();

        let seg = sample_segment();
        let h = put_lexicon_segment_v1(&store, &seg).unwrap();
        let got = get_lexicon_segment_v1(&store, &h).unwrap().unwrap();
        assert_eq!(got, seg);
    }

    #[test]
    fn lexicon_segment_store_cached_ptr_eq_on_second_get() {
        let dir = tmp_dir("lexicon_segment_store_cached_ptr_eq_on_second_get");
        let store = FsArtifactStore::new(&dir).unwrap();

        let seg = sample_segment();
        let h = put_lexicon_segment_v1(&store, &seg).unwrap();

        let mut cache: Cache2Q<Hash32, Arc<LexiconSegmentV1>> = Cache2Q::new(CacheCfgV1::new(1_000_000));

        let a1 = get_lexicon_segment_v1_cached(&store, &mut cache, &h).unwrap().unwrap();
        let a2 = get_lexicon_segment_v1_cached(&store, &mut cache, &h).unwrap().unwrap();

        assert!(Arc::ptr_eq(&a1, &a2));
    }
}
