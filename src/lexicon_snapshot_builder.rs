// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! LexiconSnapshot builder.
//!
//! This module builds a LexiconSnapshotV1 from a list of LexiconSegment hashes
//! already present in the artifact store.
//!
//! v1 behavior:
//! - Each input hash must exist and decode as LexiconSegmentV1.
//! - Counts are derived from decoded column lengths.
//! - The resulting snapshot is canonicalized and stored as an artifact.

use crate::artifact::ArtifactStore;
use crate::hash::Hash32;
use crate::lexicon_segment_store::{get_lexicon_segment_v1, LexiconSegmentStoreError};
use crate::lexicon_snapshot::{LexiconSnapshotEntryV1, LexiconSnapshotV1};
use crate::lexicon_snapshot_store::{put_lexicon_snapshot_v1, LexiconSnapshotStoreError};

/// Errors that can occur while building a LexiconSnapshotV1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LexiconSnapshotBuildError {
    /// A segment hash was not found in the artifact store.
    MissingLexiconSegment,
    /// A decoded segment has a row count that exceeds u32.
    CountOverflow,
    /// Segment load or decode failed.
    SegmentStore(String),
    /// Snapshot encode or artifact store failed.
    SnapshotStore(String),
}

impl core::fmt::Display for LexiconSnapshotBuildError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            LexiconSnapshotBuildError::MissingLexiconSegment => f.write_str("missing lexicon segment"),
            LexiconSnapshotBuildError::CountOverflow => f.write_str("lexicon count overflow"),
            LexiconSnapshotBuildError::SegmentStore(s) => write!(f, "segment: {}", s),
            LexiconSnapshotBuildError::SnapshotStore(s) => write!(f, "snapshot: {}", s),
        }
    }
}

impl std::error::Error for LexiconSnapshotBuildError {}

fn map_seg_err(e: LexiconSegmentStoreError) -> LexiconSnapshotBuildError {
    LexiconSnapshotBuildError::SegmentStore(e.to_string())
}

fn map_snap_err(e: LexiconSnapshotStoreError) -> LexiconSnapshotBuildError {
    LexiconSnapshotBuildError::SnapshotStore(e.to_string())
}

/// Build and store a LexiconSnapshotV1 from segment hashes.
///
/// Returns (snapshot_hash, snapshot_struct).
pub fn build_lexicon_snapshot_v1_from_segments<S: ArtifactStore>(
    store: &S,
    seg_hashes: &[Hash32],
) -> Result<(Hash32, LexiconSnapshotV1), LexiconSnapshotBuildError> {
    if seg_hashes.is_empty() {
        // v1 requires at least one segment to be meaningful.
        return Err(LexiconSnapshotBuildError::MissingLexiconSegment);
    }

    let mut snap = LexiconSnapshotV1::new();
    snap.entries.reserve(seg_hashes.len());

    for h in seg_hashes {
        let seg = match get_lexicon_segment_v1(store, h).map_err(map_seg_err)? {
            Some(s) => s,
            None => return Err(LexiconSnapshotBuildError::MissingLexiconSegment),
        };

        let lemma_count = seg
            .lemma_id
            .len()
            .try_into()
            .map_err(|_| LexiconSnapshotBuildError::CountOverflow)?;
        let sense_count = seg
            .sense_id
            .len()
            .try_into()
            .map_err(|_| LexiconSnapshotBuildError::CountOverflow)?;
        let rel_count = seg
            .rel_from_id
            .len()
            .try_into()
            .map_err(|_| LexiconSnapshotBuildError::CountOverflow)?;
        let pron_count = seg
            .pron_lemma_id
            .len()
            .try_into()
            .map_err(|_| LexiconSnapshotBuildError::CountOverflow)?;

        snap.entries.push(LexiconSnapshotEntryV1 {
            lex_seg: *h,
            lemma_count,
            sense_count,
            rel_count,
            pron_count,
        });
    }

    // Store uses snapshot.encode which canonicalizes and rejects duplicates.
    let snap_hash = put_lexicon_snapshot_v1(store, &snap).map_err(map_snap_err)?;

    Ok((snap_hash, snap))
}
