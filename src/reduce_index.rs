// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Sharded index reduce/merge.
//!
//! This module implements a deterministic reduce step that merges per-shard
//! index outputs (IndexSnapshotV1 + IndexSigMapV1) into a single merged
//! snapshot and sig map stored in the primary root.
//!
//! In addition, reduce-index performs a deterministic copy of referenced
//! artifacts into the primary root so that existing single-root commands
//! (query-index, build-evidence, answer) operate on the merged view without
//! needing multi-store logic.

use crate::artifact::{ArtifactError, ArtifactStore, FsArtifactStore};
use crate::frame::SourceId;
use crate::hash::Hash32;
use crate::index_sig_map::{IndexSigMapEntryV1, IndexSigMapV1};
use crate::index_sig_map_store::{get_index_sig_map_v1, put_index_sig_map_v1, IndexSigMapStoreError};
use crate::index_snapshot::{IndexSnapshotEntryV1, IndexSnapshotV1};
use crate::index_snapshot_store::{get_index_snapshot_v1, put_index_snapshot_v1, IndexSnapshotStoreError};
use crate::reduce_manifest::{ReduceManifestV1, ReduceOutputV1, REDUCE_MANIFEST_V1_VERSION};
use crate::reduce_manifest_artifact::{put_reduce_manifest_v1, ReduceManifestArtifactError};
use crate::shard_manifest::{ShardEntryV1, ShardManifestV1, ShardOutputV1};
use crate::shard_manifest_artifact::{get_shard_manifest_v1, ShardManifestArtifactError};

use std::collections::BTreeSet;
use std::path::Path;

/// Result of a reduce-index run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReduceIndexResultV1 {
    /// Hash of the stored merged IndexSnapshotV1 artifact in the primary root.
    pub merged_snapshot: Hash32,
    /// Hash of the stored merged IndexSigMapV1 artifact in the primary root.
    pub merged_sig_map: Hash32,
    /// Hash of the stored ReduceManifestV1 artifact in the primary root.
    pub reduce_manifest: Hash32,
}

/// Errors produced by reduce-index.
#[derive(Debug)]
pub enum ReduceIndexError {
    /// Artifact store error.
    Store(ArtifactError),
    /// Shard manifest not found in the primary root.
    ShardManifestNotFound,
    /// Shard manifest load error.
    ShardManifestLoad(ShardManifestArtifactError),
    /// No shard had index outputs.
    NoIndexOutputs,
    /// Shard has only one of the required index outputs.
    ShardOutputsIncomplete(u16),
    /// IndexSnapshotV1 artifact not found in a shard.
    SnapshotNotFound(u16),
    /// IndexSnapshotV1 load/decode error.
    SnapshotLoad(IndexSnapshotStoreError),
    /// IndexSigMapV1 artifact not found in a shard.
    SigMapNotFound(u16),
    /// IndexSigMapV1 load/decode error.
    SigMapLoad(IndexSigMapStoreError),
    /// Source id mismatch across shards.
    SourceIdMismatch,
    /// Duplicate (frame_seg, index_seg) entry with conflicting metadata.
    SnapshotEntryConflict,
    /// Duplicate index_seg across shards with conflicting signature.
    SigMapConflict,
    /// Required artifact referenced by the merged view is missing from all shard stores.
    CopyNotFound(Hash32),
    /// Copied bytes hashed to an unexpected id.
    CopyHashMismatch(Hash32),
    /// ReduceManifestV1 store error.
    ReduceManifestStore(ReduceManifestArtifactError),
}

impl core::fmt::Display for ReduceIndexError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ReduceIndexError::Store(e) => write!(f, "store: {}", e),
            ReduceIndexError::ShardManifestNotFound => f.write_str("shard manifest not found"),
            ReduceIndexError::ShardManifestLoad(e) => write!(f, "shard manifest load: {}", e),
            ReduceIndexError::NoIndexOutputs => f.write_str("no index outputs in shard manifest"),
            ReduceIndexError::ShardOutputsIncomplete(sid) => {
                write!(f, "shard outputs incomplete for shard {}", sid)
            }
            ReduceIndexError::SnapshotNotFound(sid) => write!(f, "snapshot not found for shard {}", sid),
            ReduceIndexError::SnapshotLoad(e) => write!(f, "snapshot load: {}", e),
            ReduceIndexError::SigMapNotFound(sid) => write!(f, "sig map not found for shard {}", sid),
            ReduceIndexError::SigMapLoad(e) => write!(f, "sig map load: {}", e),
            ReduceIndexError::SourceIdMismatch => f.write_str("source id mismatch"),
            ReduceIndexError::SnapshotEntryConflict => f.write_str("snapshot entry conflict"),
            ReduceIndexError::SigMapConflict => f.write_str("sig map conflict"),
            ReduceIndexError::CopyNotFound(h) => write!(f, "copy not found: {:?}", h),
            ReduceIndexError::CopyHashMismatch(h) => write!(f, "copy hash mismatch: {:?}", h),
            ReduceIndexError::ReduceManifestStore(e) => write!(f, "reduce manifest store: {}", e),
        }
    }
}

impl std::error::Error for ReduceIndexError {}

impl From<ArtifactError> for ReduceIndexError {
    fn from(e: ArtifactError) -> Self {
        ReduceIndexError::Store(e)
    }
}

fn find_index_outputs(outputs: &[ShardOutputV1]) -> (Option<Hash32>, Option<Hash32>) {
    let mut snap: Option<Hash32> = None;
    let mut sig: Option<Hash32> = None;
    for o in outputs.iter() {
        if o.tag == "index_snapshot_v1" {
            snap = Some(o.hash);
            continue;
        }
        if o.tag == "index_sig_map_v1" {
            sig = Some(o.hash);
            continue;
        }
    }
    (snap, sig)
}

fn open_shard_store(root: &Path, ent: &ShardEntryV1) -> Result<FsArtifactStore, ReduceIndexError> {
    let p = root.join(&ent.shard_root_rel);
    let s = FsArtifactStore::new(&p)?;
    Ok(s)
}



fn build_copy_plan(frame_segs: &[Hash32], index_segs: &[Hash32], sigs: &[Hash32]) -> Vec<Hash32> {
    // Deterministic plan: group by artifact type (frame, index, sig) and preserve
    // stable ordering within each group. Dedup across groups while preserving
    // group precedence.
    let mut out: Vec<Hash32> = Vec::with_capacity(frame_segs.len() + index_segs.len() + sigs.len());
    let mut seen: BTreeSet<Hash32> = BTreeSet::new();

    for h in frame_segs.iter() {
        if seen.insert(*h) {
            out.push(*h);
        }
    }
    for h in index_segs.iter() {
        if seen.insert(*h) {
            out.push(*h);
        }
    }
    for h in sigs.iter() {
        if seen.insert(*h) {
            out.push(*h);
        }
    }

    out
}

fn copy_plan_from_shards_with_locality(
    base: &FsArtifactStore,
    shard_stores: &[(u16, FsArtifactStore)],
    plan: &[Hash32],
) -> Result<(), ReduceIndexError> {
    // Deterministic, locality-friendly copy:
    // - If already present in base, skip.
    // - Otherwise, scan shard stores in ascending shard id order and use
    // path existence checks to avoid repeated failing opens.
    // - Copy bytes exactly once and verify the resulting hash matches.
    let mut done: Vec<bool> = vec![false; plan.len()];

    for (i, h) in plan.iter().enumerate() {
        if base.path_for(h).exists() {
            done[i] = true;
        }
    }

    for (_sid, ss) in shard_stores.iter() {
        for (i, h) in plan.iter().enumerate() {
            if done[i] {
                continue;
            }
            if !ss.path_for(h).exists() {
                continue;
            }
            let bytes = match ss.get(h)? {
                Some(b) => b,
                None => return Err(ReduceIndexError::CopyNotFound(*h)),
            };
            let hh = base.put(&bytes)?;
            if hh != *h {
                return Err(ReduceIndexError::CopyHashMismatch(*h));
            }
            done[i] = true;
        }
    }

    for (i, h) in plan.iter().enumerate() {
        if !done[i] {
            return Err(ReduceIndexError::CopyNotFound(*h));
        }
    }

    Ok(())
}
fn merge_snapshot_entries(mut all: Vec<IndexSnapshotEntryV1>) -> Result<Vec<IndexSnapshotEntryV1>, ReduceIndexError> {
    all.sort_unstable_by(|a, b| {
        match a.frame_seg.cmp(&b.frame_seg) {
            core::cmp::Ordering::Equal => a.index_seg.cmp(&b.index_seg),
            other => other,
        }
    });

    let mut out: Vec<IndexSnapshotEntryV1> = Vec::with_capacity(all.len());
    for e in all.into_iter() {
        if let Some(last) = out.last() {
            if last.frame_seg == e.frame_seg && last.index_seg == e.index_seg {
                // Duplicate pair: allow only if identical metadata.
                if last.row_count != e.row_count
                    || last.term_count != e.term_count
                    || last.postings_bytes != e.postings_bytes
                {
                    return Err(ReduceIndexError::SnapshotEntryConflict);
                }
                continue;
            }
        }
        out.push(e);
    }

    Ok(out)
}

fn merge_sig_entries(mut all: Vec<IndexSigMapEntryV1>) -> Result<Vec<IndexSigMapEntryV1>, ReduceIndexError> {
    all.sort_unstable_by(|a, b| a.index_seg.cmp(&b.index_seg));

    let mut out: Vec<IndexSigMapEntryV1> = Vec::with_capacity(all.len());
    for e in all.into_iter() {
        if let Some(last) = out.last() {
            if last.index_seg == e.index_seg {
                if last.sig != e.sig {
                    return Err(ReduceIndexError::SigMapConflict);
                }
                continue;
            }
        }
        out.push(e);
    }

    Ok(out)
}

/// Reduce per-shard index outputs into a merged view in the primary root.
///
/// Inputs:
/// - `root`: primary store root containing the ShardManifestV1.
/// - `manifest_hash`: hash of the ShardManifestV1 stored in the primary root.
///
/// Outputs:
/// - merged IndexSnapshotV1 stored in the primary root
/// - merged IndexSigMapV1 stored in the primary root
/// - ReduceManifestV1 stored in the primary root
/// - deterministic copy of referenced artifacts into the primary root
pub fn reduce_index_v1(root: &Path, manifest_hash: &Hash32) -> Result<ReduceIndexResultV1, ReduceIndexError> {
    let base_store = FsArtifactStore::new(root)?;

    let man_opt: Option<ShardManifestV1> = match get_shard_manifest_v1(&base_store, manifest_hash) {
        Ok(v) => v,
        Err(e) => return Err(ReduceIndexError::ShardManifestLoad(e)),
    };
    let man = match man_opt {
        Some(m) => m,
        None => return Err(ReduceIndexError::ShardManifestNotFound),
    };

    let mut shard_stores: Vec<(u16, FsArtifactStore)> = Vec::with_capacity(man.shards.len());
    let mut src: Option<SourceId> = None;
    let mut snap_entries: Vec<IndexSnapshotEntryV1> = Vec::new();
    let mut sig_entries: Vec<IndexSigMapEntryV1> = Vec::new();

    for se in man.shards.iter() {
        let (snap_id_opt, sig_id_opt) = find_index_outputs(&se.outputs);

        match (snap_id_opt, sig_id_opt) {
            (None, None) => {
                // Empty shard (no index outputs).
                continue;
            }
            (Some(_), None) | (None, Some(_)) => {
                return Err(ReduceIndexError::ShardOutputsIncomplete(se.shard_id));
            }
            (Some(snap_id), Some(sig_id)) => {
                // Load snapshot and sig map from this shard store.
                let ss = open_shard_store(root, se)?;

                let snap_opt = get_index_snapshot_v1(&ss, &snap_id).map_err(ReduceIndexError::SnapshotLoad)?;
                let snap = match snap_opt {
                    Some(v) => v,
                    None => return Err(ReduceIndexError::SnapshotNotFound(se.shard_id)),
                };

                let sig_opt = get_index_sig_map_v1(&ss, &sig_id).map_err(ReduceIndexError::SigMapLoad)?;
                let sig = match sig_opt {
                    Some(v) => v,
                    None => return Err(ReduceIndexError::SigMapNotFound(se.shard_id)),
                };

                if snap.source_id != sig.source_id {
                    return Err(ReduceIndexError::SourceIdMismatch);
                }

                if let Some(prev) = src {
                    if prev != snap.source_id {
                        return Err(ReduceIndexError::SourceIdMismatch);
                    }
                } else {
                    src = Some(snap.source_id);
                }

                let n_snap = snap.entries.len();
                snap_entries.reserve(n_snap);
                snap_entries.extend(snap.entries.into_iter());

                let n_sig = sig.entries.len();
                sig_entries.reserve(n_sig);
                sig_entries.extend(sig.entries.into_iter());
                shard_stores.push((se.shard_id, ss));
            }
        }
    }

    let source_id = match src {
        Some(v) => v,
        None => return Err(ReduceIndexError::NoIndexOutputs),
    };

    let merged_entries = merge_snapshot_entries(snap_entries)?;
    let merged_sig_entries = merge_sig_entries(sig_entries)?;
    let snapshot_entries_count = merged_entries.len() as u32;

    // Copy referenced artifacts into the primary root.
    let mut frame_segs: Vec<Hash32> = Vec::with_capacity(merged_entries.len());
    let mut index_segs: Vec<Hash32> = Vec::with_capacity(merged_entries.len());
    for e in merged_entries.iter() {
        frame_segs.push(e.frame_seg);
        index_segs.push(e.index_seg);
    }
    frame_segs.sort();
    frame_segs.dedup();
    index_segs.sort();
    index_segs.dedup();

    let mut sigs: Vec<Hash32> = Vec::with_capacity(merged_sig_entries.len());
    for e in merged_sig_entries.iter() {
        sigs.push(e.sig);
    }
    sigs.sort();
    sigs.dedup();

    let copy_plan = build_copy_plan(&frame_segs, &index_segs, &sigs);
    copy_plan_from_shards_with_locality(&base_store, &shard_stores, &copy_plan)?;

    // Store merged artifacts into the primary root.
    let merged_snapshot = {
        let snap = IndexSnapshotV1 { version: 1, source_id, entries: merged_entries };
        put_index_snapshot_v1(&base_store, &snap).map_err(ReduceIndexError::SnapshotLoad)?
    };

    let merged_sig_map = {
        let map = IndexSigMapV1 { source_id, entries: merged_sig_entries };
        put_index_sig_map_v1(&base_store, &map).map_err(ReduceIndexError::SigMapLoad)?
    };

    // Build and store ReduceManifestV1.
    let mut outputs: Vec<ReduceOutputV1> = vec![
        ReduceOutputV1 { tag: "index_sig_map_v1".to_string(), hash: merged_sig_map },
        ReduceOutputV1 { tag: "index_snapshot_v1".to_string(), hash: merged_snapshot },
    ];
    outputs.sort_by(|a, b| a.tag.cmp(&b.tag));

    let man = ReduceManifestV1 {
        version: REDUCE_MANIFEST_V1_VERSION,
        shard_manifest: *manifest_hash,
        shard_count: man.shard_count,
        mapping_id: man.mapping_id,
        source_id_u64: source_id.0 .0,
        snapshot_entries: snapshot_entries_count,
        copied_frame_segs: frame_segs.len() as u32,
        copied_index_segs: index_segs.len() as u32,
        copied_segment_sigs: sigs.len() as u32,
        outputs,
    };

    let reduce_manifest = put_reduce_manifest_v1(&base_store, &man).map_err(ReduceIndexError::ReduceManifestStore)?;

    Ok(ReduceIndexResultV1 { merged_snapshot, merged_sig_map, reduce_manifest })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(b: u8) -> Hash32 {
        let mut x = [0u8; 32];
        x[0] = b;
        x
    }

    #[test]
    fn merge_snapshot_entries_dedup_allows_identical() {
        let e1 = IndexSnapshotEntryV1 { frame_seg: h(1), index_seg: h(2), row_count: 1, term_count: 2, postings_bytes: 3 };
        let e2 = e1.clone();
        let out = merge_snapshot_entries(vec![e1, e2]).unwrap();
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn build_copy_plan_dedup_across_groups_preserves_precedence() {
        let f1 = h(1);
        let i1 = h(2);
        let s1 = h(3);
        // Duplicate across groups: i1 appears as both frame and index.
        let frames = vec![f1, i1];
        let index = vec![i1];
        let sigs = vec![s1];
        let plan = build_copy_plan(&frames, &index, &sigs);
        assert_eq!(plan, vec![f1, i1, s1]);
    }


    #[test]
    fn merge_sig_entries_dedup_allows_identical() {
        let e1 = IndexSigMapEntryV1 { index_seg: h(2), sig: h(9) };
        let e2 = e1.clone();
        let out = merge_sig_entries(vec![e1, e2]).unwrap();
        assert_eq!(out.len(), 1);
    }
}
