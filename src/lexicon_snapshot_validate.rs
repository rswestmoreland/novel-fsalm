// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! LexiconSnapshot validation helpers.
//!
//! The primary v1 contract is that a snapshot's segments must have disjoint
//! lemma ownership: a `LemmaId` may appear in exactly one `LexiconSegmentV1`.
//!
//! Validation is deterministic:
//! - Snapshot entries are processed in canonical order (sorted by segment hash).
//! - Overlap detection reports the first overlapping `LemmaId` in ascending order.

use crate::artifact::{ArtifactError, ArtifactStore};
use crate::codec::DecodeError;
use crate::hash::{hex32, Hash32};
use crate::lexicon::LemmaId;
use crate::lexicon_segment::LexiconSegmentV1;
use crate::lexicon_snapshot::LexiconSnapshotV1;

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::error::Error;
use std::fmt;

/// Errors produced by LexiconSnapshot validation.
#[derive(Debug)]
pub enum LexiconSnapshotValidateError {
    /// Artifact store error.
    Store {
        /// Underlying store error.
        err: ArtifactError,
    },

    /// Snapshot was not found in the artifact store.
    SnapshotNotFound {
        /// Snapshot hash.
        snapshot: Hash32,
    },

    /// Snapshot failed to decode.
    SnapshotDecode {
        /// Snapshot hash.
        snapshot: Hash32,
        /// Decode error.
        err: DecodeError,
    },

    /// Snapshot contains the same segment hash more than once.
    DuplicateSegment {
        /// Repeated segment hash.
        segment: Hash32,
    },

    /// Segment was not found in the artifact store.
    SegmentNotFound {
        /// Segment hash.
        segment: Hash32,
    },

    /// Segment failed to decode.
    SegmentDecode {
        /// Segment hash.
        segment: Hash32,
        /// Decode error.
        err: DecodeError,
    },

    /// Two different segments claim ownership of the same lemma.
    OverlappingLemmaOwner {
        /// The overlapping lemma id.
        lemma_id: LemmaId,
        /// First segment hash involved.
        segment_a: Hash32,
        /// Second segment hash involved.
        segment_b: Hash32,
    },
}

impl fmt::Display for LexiconSnapshotValidateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LexiconSnapshotValidateError::Store { err } => {
                write!(f, "artifact store error: {err}")
            }
            LexiconSnapshotValidateError::SnapshotNotFound { snapshot } => {
                write!(f, "snapshot not found: {}", hex32(snapshot))
            }
            LexiconSnapshotValidateError::SnapshotDecode { snapshot, err } => {
                write!(f, "snapshot decode failed: {}: {err}", hex32(snapshot))
            }
            LexiconSnapshotValidateError::DuplicateSegment { segment } => {
                write!(f, "snapshot contains duplicate segment: {}", hex32(segment))
            }
            LexiconSnapshotValidateError::SegmentNotFound { segment } => {
                write!(f, "segment not found: {}", hex32(segment))
            }
            LexiconSnapshotValidateError::SegmentDecode { segment, err } => {
                write!(f, "segment decode failed: {}: {err}", hex32(segment))
            }
            LexiconSnapshotValidateError::OverlappingLemmaOwner {
                lemma_id,
                segment_a,
                segment_b,
            } => {
                write!(
                    f,
                    "overlapping lemma owner: lemma={} seg_a={} seg_b={}",
                    (lemma_id.0).0,
                    hex32(segment_a),
                    hex32(segment_b)
                )
            }
        }
    }
}

impl Error for LexiconSnapshotValidateError {}

impl From<ArtifactError> for LexiconSnapshotValidateError {
    fn from(err: ArtifactError) -> Self {
        LexiconSnapshotValidateError::Store { err }
    }
}

/// Validate that a LexiconSnapshotV1 has disjoint lemma ownership across segments.
///
/// This loads the snapshot, loads each referenced segment, and checks that no
/// `LemmaId` appears in more than one segment.
pub fn validate_lexicon_snapshot_v1_disjoint_owners<S: ArtifactStore>(
    store: &S,
    snapshot_hash: &Hash32,
) -> Result<(), LexiconSnapshotValidateError> {
    let snap_bytes = match store.get(snapshot_hash)? {
        Some(b) => b,
        None => {
            return Err(LexiconSnapshotValidateError::SnapshotNotFound {
                snapshot: *snapshot_hash,
            })
        }
    };

    let snap = LexiconSnapshotV1::decode(&snap_bytes).map_err(|e| {
        LexiconSnapshotValidateError::SnapshotDecode {
            snapshot: *snapshot_hash,
            err: e,
        }
    })?;

    // Canonical processing order: sort by segment hash.
    let mut seg_hashes: Vec<Hash32> = snap.entries.iter().map(|e| e.lex_seg).collect();
    seg_hashes.sort();
    for i in 1..seg_hashes.len() {
        if seg_hashes[i - 1] == seg_hashes[i] {
            return Err(LexiconSnapshotValidateError::DuplicateSegment {
                segment: seg_hashes[i],
            });
        }
    }

    // Load only lemma-id lists for each segment to reduce memory.
    let mut segs: Vec<SegLemmas> = Vec::with_capacity(seg_hashes.len());
    for seg_hash in seg_hashes {
        let seg_bytes = match store.get(&seg_hash)? {
            Some(b) => b,
            None => {
                return Err(LexiconSnapshotValidateError::SegmentNotFound {
                    segment: seg_hash,
                })
            }
        };

        let seg = LexiconSegmentV1::decode(&seg_bytes).map_err(|e| {
            LexiconSnapshotValidateError::SegmentDecode {
                segment: seg_hash,
                err: e,
            }
        })?;

        segs.push(SegLemmas {
            seg_hash,
            lemma_ids: seg.lemma_id,
        });
    }

    if segs.len() <= 1 {
        return Ok(());
    }

    // K-way merge over sorted lemma-id lists.
    let mut heap: BinaryHeap<Reverse<(u64, usize, usize)>> = BinaryHeap::new();
    for (seg_idx, seg) in segs.iter().enumerate() {
        if let Some(first) = seg.lemma_ids.first() {
            heap.push(Reverse((lemma_u64(*first), seg_idx, 0)));
        }
    }

    let mut prev: Option<(u64, usize)> = None;
    while let Some(Reverse((lemma, seg_idx, pos))) = heap.pop() {
        if let Some((prev_lemma, prev_seg_idx)) = prev {
            if lemma == prev_lemma {
                return Err(LexiconSnapshotValidateError::OverlappingLemmaOwner {
                    lemma_id: segs[seg_idx].lemma_ids[pos],
                    segment_a: segs[prev_seg_idx].seg_hash,
                    segment_b: segs[seg_idx].seg_hash,
                });
            }
        }

        prev = Some((lemma, seg_idx));

        let next_pos = pos + 1;
        if next_pos < segs[seg_idx].lemma_ids.len() {
            let next_lemma = lemma_u64(segs[seg_idx].lemma_ids[next_pos]);
            heap.push(Reverse((next_lemma, seg_idx, next_pos)));
        }
    }

    Ok(())
}

#[derive(Debug)]
struct SegLemmas {
    seg_hash: Hash32,
    lemma_ids: Vec<LemmaId>,
}

fn lemma_u64(id: LemmaId) -> u64 {
    (id.0).0
}
