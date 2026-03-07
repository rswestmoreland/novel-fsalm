// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! IndexSnapshot and index artifact compaction.
//!
//! This module performs deterministic compaction of index artifacts referenced
//! by an IndexSnapshotV1.
//!
//! Current implementation notes:
//! - The input index artifacts are IndexSegmentV1 blobs.
//! - The output artifacts are IndexPackV1 blobs, each bundling multiple
//! IndexSegmentV1 blobs.
//! - The output IndexSnapshotV1 keeps per-FrameSegment entries unchanged,
//! but rewrites `index_seg` to point at the pack hash containing that
//! segment's index bytes.
//!
//! This preserves query semantics exactly, because query-time decoding selects
//! the inner IndexSegmentV1 by frame_seg hash.

use crate::artifact::ArtifactStore;
use crate::codec::DecodeError;
use crate::compaction_report::{CompactionCfgV1, CompactionGroupV1, CompactionReportV1};
use crate::hash::Hash32;
use crate::index_pack::{IndexPackEntryV1, IndexPackV1};
use crate::index_segment::IndexSegmentV1;
use crate::index_snapshot::{IndexSnapshotEntryV1, IndexSnapshotV1};
use crate::index_sig_map::IndexSigMapV1;
use crate::index_sig_map_store::{put_index_sig_map_v1, IndexSigMapStoreError};
use crate::segment_sig::{SegmentSigBuildError, SegmentSigV1};
use crate::segment_sig_store::{put_segment_sig_v1, SegmentSigStoreError};

/// Result of a compaction run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexCompactionResultV1 {
    /// Compaction report.
    pub report: CompactionReportV1,
    /// Hash of the stored report artifact, if written.
    pub report_id: Option<Hash32>,
    /// Hash of the stored IndexSigMapV1 artifact for the output snapshot, if written.
    pub output_sig_map_id: Option<Hash32>,
}


/// Errors produced by index compaction.
#[derive(Debug)]
pub enum IndexCompactionError {
    /// Artifact store error.
    Store(crate::artifact::ArtifactError),
    /// Snapshot artifact not found.
    SnapshotNotFound,
    /// Snapshot decode error.
    SnapshotDecode(DecodeError),
    /// Index artifact not found.
    IndexNotFound(Hash32),
    /// Index decode error.
    IndexDecode(DecodeError),
    /// Pack encode error.
    PackEncode(crate::codec::EncodeError),
    /// Output snapshot encode error.
    SnapshotEncode(crate::codec::EncodeError),
    /// SegmentSig build error.
    SigBuild(SegmentSigBuildError),
    /// SegmentSig store error.
    SigStore(SegmentSigStoreError),
    /// IndexSigMap store error.
    SigMapStore(IndexSigMapStoreError),
    /// Invalid configuration.
    BadCfg(&'static str),
    /// Pack invariant violation.
    BadPack(&'static str),
}

impl core::fmt::Display for IndexCompactionError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            IndexCompactionError::Store(e) => write!(f, "store: {}", e),
            IndexCompactionError::SnapshotNotFound => f.write_str("snapshot not found"),
            IndexCompactionError::SnapshotDecode(e) => write!(f, "snapshot decode: {}", e),
            IndexCompactionError::IndexNotFound(h) => write!(f, "index not found: {:?}", h),
            IndexCompactionError::IndexDecode(e) => write!(f, "index decode: {}", e),
            IndexCompactionError::PackEncode(e) => write!(f, "pack encode: {}", e),
            IndexCompactionError::SnapshotEncode(e) => write!(f, "snapshot encode: {}", e),
            IndexCompactionError::SigBuild(e) => write!(f, "segment sig build: {}", e),
            IndexCompactionError::SigStore(e) => write!(f, "segment sig store: {}", e),
            IndexCompactionError::SigMapStore(e) => write!(f, "sig map store: {}", e),
            IndexCompactionError::BadCfg(s) => write!(f, "bad cfg: {}", s),
            IndexCompactionError::BadPack(s) => write!(f, "bad pack: {}", s),
        }
    }
}

impl From<crate::artifact::ArtifactError> for IndexCompactionError {
    fn from(e: crate::artifact::ArtifactError) -> Self {
        IndexCompactionError::Store(e)
    }
}

fn artifact_len_bytes<S: ArtifactStore>(store: &S, hash: &Hash32) -> Result<u64, IndexCompactionError> {
    let p = store.path_for(hash);
    match std::fs::metadata(&p) {
        Ok(m) => Ok(m.len()),
        Err(_) => {
            let b = store.get(hash)?;
            match b {
                Some(bytes) => Ok(bytes.len() as u64),
                None => Err(IndexCompactionError::IndexNotFound(*hash)),
            }
        }
    }
}

fn load_snapshot_v1<S: ArtifactStore>(store: &S, snapshot_id: &Hash32) -> Result<IndexSnapshotV1, IndexCompactionError> {
    let bytes_opt = store.get(snapshot_id)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Err(IndexCompactionError::SnapshotNotFound),
    };
    let snap = IndexSnapshotV1::decode(&bytes).map_err(IndexCompactionError::SnapshotDecode)?;
    Ok(snap)
}

fn collect_unique_sorted_input_segments(snap: &IndexSnapshotV1) -> Vec<Hash32> {
    let mut v: Vec<Hash32> = Vec::with_capacity(snap.entries.len());
    for e in &snap.entries {
        v.push(e.index_seg);
    }
    v.sort();
    v.dedup();
    v
}

fn greedy_groups(sizes: &[u64], target: u64) -> Vec<CompactionGroupV1> {
    let mut out: Vec<CompactionGroupV1> = Vec::new();
    let mut i: usize = 0;
    while i < sizes.len() {
        let start = i;
        let mut sum: u64 = 0;
        while i < sizes.len() {
            let sz = sizes[i];
            if sum == 0 {
                sum = sz;
                i += 1;
                continue;
            }
            if sum.saturating_add(sz) <= target {
                sum = sum.saturating_add(sz);
                i += 1;
                continue;
            }
            break;
        }
        let len = i - start;
        out.push(CompactionGroupV1 {
            start_ix: start as u32,
            len: len as u32,
            est_bytes_in: sum,
            out_segment_id: None,
            out_bytes: 0,
        });
    }
    out
}

fn even_pack_groups(sizes: &[u64], max_out: u32) -> Vec<CompactionGroupV1> {
    let n = sizes.len();
    let k = max_out as usize;
    if k == 0 {
        return Vec::new();
    }
    if n == 0 {
        return Vec::new();
    }
    let k = if k > n { n } else { k };

    let base = n / k;
    let rem = n % k;
    let mut out: Vec<CompactionGroupV1> = Vec::with_capacity(k);
    let mut start: usize = 0;
    for gi in 0..k {
        let mut len = base;
        if gi < rem {
            len += 1;
        }
        let mut sum: u64 = 0;
        for j in 0..len {
            sum = sum.saturating_add(sizes[start + j]);
        }
        out.push(CompactionGroupV1 {
            start_ix: start as u32,
            len: len as u32,
            est_bytes_in: sum,
            out_segment_id: None,
            out_bytes: 0,
        });
        start += len;
    }
    out
}

/// Run deterministic index compaction for a given input snapshot.
///
/// Behavior:
/// - Always plans using greedy byte packing; if that produces more than
/// cfg.max_out_segments groups, uses the even-pack fallback.
/// - If cfg.dry_run is true, produces a report only (no outputs written).
/// - If cfg.dry_run is false, writes:
/// - output IndexPackV1 artifacts (one per group)
/// - output IndexSnapshotV1 referencing those packs
/// - CompactionReportV1 artifact
pub fn compact_index_snapshot_v1<S: ArtifactStore>(
    store: &S,
    input_snapshot_id: &Hash32,
    mut cfg: CompactionCfgV1,
) -> Result<IndexCompactionResultV1, IndexCompactionError> {
    if cfg.max_out_segments == 0 {
        return Err(IndexCompactionError::BadCfg("max_out_segments must be >= 1"));
    }
    if cfg.target_bytes_per_out_segment == 0 {
        return Err(IndexCompactionError::BadCfg(
            "target_bytes_per_out_segment must be >= 1",
        ));
    }

    let snap = load_snapshot_v1(store, input_snapshot_id)?;
    let input_index_segments = collect_unique_sorted_input_segments(&snap);

    // Collect exact sizes without reading full bytes when possible.
    let mut sizes: Vec<u64> = Vec::with_capacity(input_index_segments.len());
    let mut bytes_input_total: u64 = 0;
    for h in &input_index_segments {
        let sz = artifact_len_bytes(store, h)?;
        sizes.push(sz);
        bytes_input_total = bytes_input_total.saturating_add(sz);
    }

    let mut groups = greedy_groups(&sizes, cfg.target_bytes_per_out_segment);
    if (groups.len() as u32) > cfg.max_out_segments {
        cfg.used_even_pack_fallback = true;
        groups = even_pack_groups(&sizes, cfg.max_out_segments);
    } else {
        cfg.used_even_pack_fallback = false;
    }

    let mut report = CompactionReportV1 {
        input_snapshot_id: *input_snapshot_id,
        output_snapshot_id: None,
        cfg_id: cfg.cfg_id(),
        cfg,
        input_index_segments: input_index_segments.clone(),
        groups,
        output_index_segments: Vec::new(),
        bytes_input_total,
        bytes_output_total: 0,
    };

    if report.cfg.dry_run {
        // Dry run: no output artifacts.
        report.bytes_output_total = 0;
        report.output_index_segments.clear();
        report.output_snapshot_id = None;
        return Ok(IndexCompactionResultV1 { report, report_id: None, output_sig_map_id: None });
    }

    // Build a map from old index_seg hash -> out pack hash.
    let mut old_to_pack: Vec<(Hash32, Hash32)> = Vec::with_capacity(input_index_segments.len());
    let mut out_ids: Vec<Hash32> = Vec::with_capacity(report.groups.len());

    // Sidecar signatures for index artifacts referenced by the output snapshot.
    // For compaction outputs, the index artifact is an IndexPackV1, so we build
    // a conservative signature across the union of terms in the pack's entries.
    let bloom_bytes: usize = 4096;
    let bloom_k: u8 = 6;
    let mut sig_pairs: Vec<(Hash32, Hash32)> = Vec::with_capacity(report.groups.len());

    for g in report.groups.iter_mut() {
        let start = g.start_ix as usize;
        let len = g.len as usize;

        let mut entries: Vec<IndexPackEntryV1> = Vec::with_capacity(len);
        let mut pack_terms: Vec<crate::frame::TermId> = Vec::new();
        for i in 0..len {
            let h = input_index_segments[start + i];
            let b_opt = store.get(&h)?;
            let b = match b_opt {
                Some(x) => x,
                None => return Err(IndexCompactionError::IndexNotFound(h)),
            };
            let idx = IndexSegmentV1::decode(&b).map_err(IndexCompactionError::IndexDecode)?;
            if idx.source_id != snap.source_id {
                return Err(IndexCompactionError::BadPack("source_id mismatch"));
            }
            for te in &idx.terms {
                pack_terms.push(te.term);
            }
            entries.push(IndexPackEntryV1 { frame_seg: idx.seg_hash, index_bytes: b });
        }

        pack_terms.sort_by_key(|t| (t.0).0);
        pack_terms.dedup_by_key(|t| (t.0).0);

        let pack = IndexPackV1 { source_id: snap.source_id, entries };
        let pack_bytes = pack.encode().map_err(IndexCompactionError::PackEncode)?;
        let pack_id = store.put(&pack_bytes)?;

        let sig = SegmentSigV1::build(pack_id, &pack_terms, bloom_bytes, bloom_k).map_err(IndexCompactionError::SigBuild)?;
        let sig_id = put_segment_sig_v1(store, &sig).map_err(IndexCompactionError::SigStore)?;
        sig_pairs.push((pack_id, sig_id));

        // Update group metadata.
        g.out_segment_id = Some(pack_id);
        g.out_bytes = pack_bytes.len() as u64;

        // Map old segment hashes to this pack.
        for i in 0..len {
            let old = input_index_segments[start + i];
            old_to_pack.push((old, pack_id));
        }
        out_ids.push(pack_id);
        report.bytes_output_total = report.bytes_output_total.saturating_add(pack_bytes.len() as u64);
    }

    // Canonicalize output segment list.
    out_ids.sort();
    out_ids.dedup();
    report.output_index_segments = out_ids;

    // Rewrite snapshot entries.
    old_to_pack.sort_by(|a, b| a.0.cmp(&b.0));
    let mut out_snap = IndexSnapshotV1::new(snap.source_id);
    out_snap.entries = Vec::with_capacity(snap.entries.len());

    for e in snap.entries.iter() {
        let pack_id = match old_to_pack.binary_search_by(|p| p.0.cmp(&e.index_seg)) {
            Ok(ix) => old_to_pack[ix].1,
            Err(_) => return Err(IndexCompactionError::BadPack("missing old->pack mapping")),
        };
        out_snap.entries.push(IndexSnapshotEntryV1 {
            frame_seg: e.frame_seg,
            index_seg: pack_id,
            row_count: e.row_count,
            term_count: e.term_count,
            postings_bytes: e.postings_bytes,
        });
    }

    let out_bytes = out_snap.encode().map_err(IndexCompactionError::SnapshotEncode)?;
    let out_snapshot_id = store.put(&out_bytes)?;
    report.output_snapshot_id = Some(out_snapshot_id);


    // Store report artifact.
    let report_id = {
        let rep_bytes = report
            .encode()
            .map_err(|_| IndexCompactionError::BadPack("report encode failed"))?;
        Some(store.put(&rep_bytes)?)
    };

    // Store the sidecar IndexSigMapV1 for the output snapshot.
    sig_pairs.sort_by(|a, b| a.0.cmp(&b.0));
    // If duplicate pack ids appear (identical packs), require their sig ids to match.
    let mut uniq_pairs: Vec<(Hash32, Hash32)> = Vec::with_capacity(sig_pairs.len());
    for (idx_id, sig_id) in sig_pairs {
        if let Some((last_idx, last_sig)) = uniq_pairs.last() {
            if last_idx == &idx_id {
                if last_sig != &sig_id {
                    return Err(IndexCompactionError::BadPack("duplicate pack id with different sig"));
                }
                continue;
            }
        }
        uniq_pairs.push((idx_id, sig_id));
    }

    let mut sig_map = IndexSigMapV1::new(snap.source_id);
    for (idx_id, sig_id) in uniq_pairs {
        sig_map.push(idx_id, sig_id);
    }
    let sig_map_id = put_index_sig_map_v1(store, &sig_map).map_err(IndexCompactionError::SigMapStore)?;

    Ok(IndexCompactionResultV1 {
        report,
        report_id,
        output_sig_map_id: Some(sig_map_id),
    })
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
    use crate::frame::{DocId, Id64, SourceId, TermFreq, TermId};
    use crate::frame_segment::FrameSegmentV1;
    use crate::frame_store::put_frame_segment_v1;
    use crate::hash::blake3_hash;
    use crate::index_query::{search_snapshot, QueryTerm, SearchCfg};
    use crate::index_segment::IndexBuildError;
    use crate::index_store::put_index_segment_v1;
    use crate::index_snapshot_store::put_index_snapshot_v1;

    fn mk_row(doc: u64, sid: SourceId, terms: &[(u64, u32)]) -> crate::frame::FrameRowV1 {
        let mut r = crate::frame::FrameRowV1::new(DocId(Id64(doc)), sid);
        for (t, tf) in terms {
            r.terms.push(TermFreq { term: TermId(Id64(*t)), tf: *tf });
        }
        r.recompute_doc_len();
        r
    }

    fn mk_seg(rows: &[crate::frame::FrameRowV1]) -> FrameSegmentV1 {
        FrameSegmentV1::from_rows(rows, 16).unwrap()
    }

    fn mk_index(seg_hash: Hash32, seg: &FrameSegmentV1) -> Result<IndexSegmentV1, IndexBuildError> {
        IndexSegmentV1::build_from_segment(seg_hash, seg)
    }

    #[test]
    fn pack_compaction_preserves_query_hits_small() {
        let mut root = std::env::temp_dir();
        root.push("novel_fsalm_test_index_compaction");
        let _ = std::fs::remove_dir_all(&root);
        let store = FsArtifactStore::new(&root).unwrap();

        let sid = SourceId(Id64(42));

        // Build 3 frame segments, each with 2 rows.
        let seg1 = mk_seg(&[
            mk_row(1, sid, &[(1001, 1), (2001, 1)]),
            mk_row(2, sid, &[(1001, 2)]),
        ]);
        let seg1_id = put_frame_segment_v1(&store, &seg1).unwrap();

        let seg2 = mk_seg(&[
            mk_row(3, sid, &[(1001, 1)]),
            mk_row(4, sid, &[(3001, 1)]),
        ]);
        let seg2_id = put_frame_segment_v1(&store, &seg2).unwrap();

        let seg3 = mk_seg(&[
            mk_row(5, sid, &[(2001, 1)]),
            mk_row(6, sid, &[(1001, 1), (3001, 2)]),
        ]);
        let seg3_id = put_frame_segment_v1(&store, &seg3).unwrap();

        // Build index segments.
        let idx1 = mk_index(seg1_id, &seg1).unwrap();
        let idx1_id = put_index_segment_v1(&store, &idx1).unwrap();

        let idx2 = mk_index(seg2_id, &seg2).unwrap();
        let idx2_id = put_index_segment_v1(&store, &idx2).unwrap();

        let idx3 = mk_index(seg3_id, &seg3).unwrap();
        let idx3_id = put_index_segment_v1(&store, &idx3).unwrap();

        // Snapshot.
        let mut snap = IndexSnapshotV1::new(sid);
        snap.entries.push(IndexSnapshotEntryV1 {
            frame_seg: seg1_id,
            index_seg: idx1_id,
            row_count: idx1.row_count,
            term_count: idx1.terms.len() as u32,
            postings_bytes: idx1.postings.len() as u32,
        });
        snap.entries.push(IndexSnapshotEntryV1 {
            frame_seg: seg2_id,
            index_seg: idx2_id,
            row_count: idx2.row_count,
            term_count: idx2.terms.len() as u32,
            postings_bytes: idx2.postings.len() as u32,
        });
        snap.entries.push(IndexSnapshotEntryV1 {
            frame_seg: seg3_id,
            index_seg: idx3_id,
            row_count: idx3.row_count,
            term_count: idx3.terms.len() as u32,
            postings_bytes: idx3.postings.len() as u32,
        });
        let snap_id = put_index_snapshot_v1(&store, &snap).unwrap();

        // Query before.
        let terms = vec![QueryTerm { term: TermId(Id64(1001)), qtf: 1 }];
        let mut cfg = SearchCfg::new();
        cfg.k = 32;
        let before = search_snapshot(&store, &snap_id, &terms, &cfg).unwrap();

        // Compact with a small target to force grouping.
        let cfg_compact = CompactionCfgV1 {
            target_bytes_per_out_segment: 1,
            max_out_segments: 1,
            used_even_pack_fallback: false,
            dry_run: false,
        };
        let res = compact_index_snapshot_v1(&store, &snap_id, cfg_compact).unwrap();
        let out_snap_id = res.report.output_snapshot_id.unwrap();

        // Query after.
        let after = search_snapshot(&store, &out_snap_id, &terms, &cfg).unwrap();

        assert_eq!(before, after);
    }


    #[test]
    fn compaction_emits_index_sig_map_for_output_snapshot() {
        let mut root = std::env::temp_dir();
        root.push("novel_fsalm_test_index_compaction_sig_map");
        let _ = std::fs::remove_dir_all(&root);
        let store = FsArtifactStore::new(&root).unwrap();

        let sid = SourceId(Id64(7));
        let seg1 = FrameSegmentV1::from_rows(&[mk_row(1, sid, &[(1001, 3), (1002, 1)])], 16).unwrap();
        let seg2 = FrameSegmentV1::from_rows(&[mk_row(2, sid, &[(1001, 1), (2001, 1)])], 16).unwrap();
        let seg1_id = put_frame_segment_v1(&store, &seg1).unwrap();
        let seg2_id = put_frame_segment_v1(&store, &seg2).unwrap();
        let idx1 = IndexSegmentV1::build_from_segment(seg1_id, &seg1).unwrap();
        let idx2 = IndexSegmentV1::build_from_segment(seg2_id, &seg2).unwrap();
        let idx1_id = put_index_segment_v1(&store, &idx1).unwrap();
        let idx2_id = put_index_segment_v1(&store, &idx2).unwrap();

        let mut snap = IndexSnapshotV1::new(sid);
        snap.entries.push(IndexSnapshotEntryV1 {
            frame_seg: seg1_id,
            index_seg: idx1_id,
            row_count: idx1.row_count,
            term_count: idx1.terms.len() as u32,
            postings_bytes: idx1.postings.len() as u32,
        });
        snap.entries.push(IndexSnapshotEntryV1 {
            frame_seg: seg2_id,
            index_seg: idx2_id,
            row_count: idx2.row_count,
            term_count: idx2.terms.len() as u32,
            postings_bytes: idx2.postings.len() as u32,
        });
        let snap_id = put_index_snapshot_v1(&store, &snap).unwrap();

        let cfg_compact = CompactionCfgV1 {
            target_bytes_per_out_segment: 1,
            max_out_segments: 1,
            used_even_pack_fallback: false,
            dry_run: false,
        };
        let res = compact_index_snapshot_v1(&store, &snap_id, cfg_compact).unwrap();
        let out_snap_id = res.report.output_snapshot_id.unwrap();
        let map_id = res.output_sig_map_id.unwrap();

        let map = crate::index_sig_map_store::get_index_sig_map_v1(&store, &map_id)
            .unwrap()
            .expect("index sig map");
        assert_eq!(map.source_id, sid);

        // In the compacted snapshot, entries reference packs. The sig map should have an entry per unique pack.
        let out_snap = crate::index_snapshot_store::get_index_snapshot_v1(&store, &out_snap_id)
            .unwrap()
            .expect("output snapshot");
        let mut pack_ids = out_snap.entries.iter().map(|e| e.index_seg).collect::<Vec<_>>();
        pack_ids.sort();
        pack_ids.dedup();
        assert_eq!(map.entries.len(), pack_ids.len());

        for pack_id in pack_ids {
            let sig_id = map.lookup_sig(&pack_id).expect("missing sig for pack");
            let sig = crate::segment_sig_store::get_segment_sig_v1(&store, &sig_id)
                .unwrap()
                .expect("segment sig");
            assert_eq!(sig.index_seg, pack_id);
        }
    }

    #[test]
    fn planner_uses_even_pack_when_too_many_groups() {
        let mut root = std::env::temp_dir();
        root.push("novel_fsalm_test_index_compaction_plan");
        let _ = std::fs::remove_dir_all(&root);
        let store = FsArtifactStore::new(&root).unwrap();

        let sid = SourceId(Id64(7));
        let seg = FrameSegmentV1::from_rows(&[mk_row(1, sid, &[(1, 1)])], 16).unwrap();
        let seg_id = put_frame_segment_v1(&store, &seg).unwrap();
        let idx = IndexSegmentV1::build_from_segment(seg_id, &seg).unwrap();

        // Snapshot with repeated entries pointing to distinct fake index ids by storing copies.
        let mut snap = IndexSnapshotV1::new(sid);
        for i in 0..5u8 {
            // Slightly perturb bytes to create different hashes.
            let mut b = idx.encode().unwrap();
            b.push(i);
            let h = store.put(&b).unwrap();
            snap.entries.push(IndexSnapshotEntryV1 {
                frame_seg: blake3_hash(&[i]),
                index_seg: h,
                row_count: 1,
                term_count: 1,
                postings_bytes: 1,
            });
        }
        let snap_id = put_index_snapshot_v1(&store, &snap).unwrap();

        let cfg_compact = CompactionCfgV1 {
            target_bytes_per_out_segment: 1,
            max_out_segments: 2,
            used_even_pack_fallback: false,
            dry_run: true,
        };
        let res = compact_index_snapshot_v1(&store, &snap_id, cfg_compact).unwrap();
        assert!(res.report.cfg.used_even_pack_fallback);
        assert_eq!(res.report.groups.len(), 2);
    }
}
