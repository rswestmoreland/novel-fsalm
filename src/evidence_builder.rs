// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Evidence builder.
//!
//! Evidence building is the bridge between retrieval hits (row addresses + scores)
//! and a canonical [`crate::evidence_bundle::EvidenceBundleV1`] artifact.
//!
//! v1 uses a deterministic two-pass process:
//! - Pass 1: normalize and deduplicate hit addresses, then rank and cap items.
//! - Pass 2: optionally attach compact row sketches under a strict byte budget.
//!
//! The bundle produced by this module is valid input for
//! [`crate::evidence_bundle::EvidenceBundleV1::encode_assuming_canonical`].
//!
//! Notes:
//! - Sketch ids are stored as u32 for compactness. They are derived from 64-bit
//! stable ids by XOR-folding the high and low 32-bit halves.
//! - meta_codes are reserved for later stages and are empty in v1.

use crate::artifact::ArtifactStore;
use crate::evidence_bundle::{
    EvidenceBundleV1, EvidenceItemDataV1, EvidenceItemV1, EvidenceLimitsV1, FrameRowRefV1,
    FrameRowSketchV1, TermTfV1,
};
use crate::frame::FrameRowV1;
use crate::cache::Cache2Q;
use crate::frame_segment::FrameSegmentV1;
use crate::frame_store::{get_frame_segment_v1, get_frame_segment_v1_cached, FrameStoreError};
use std::sync::Arc;
use crate::hash::Hash32;
use crate::index_query::SearchHit;
use crate::retrieval_control::RetrievalControlV1;

/// Byte size of the fixed EvidenceBundleV1 header.
///
/// This includes: version, query_id, snapshot_id, limits, score_model_id, items_len.
const BUNDLE_HEADER_BYTES: usize = 86;

/// Byte size of a Frame evidence item without a sketch.
///
/// This includes: score(i64), kind(u8), segment_id, row_ix, has_sketch flag.
const FRAME_ITEM_BASE_BYTES: usize = 46;

/// Configuration for building frame row sketches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvidenceSketchCfgV1 {
    /// If false, no sketches are attached.
    pub enable: bool,
    /// Maximum term entries per sketch.
    pub max_terms: u32,
    /// Maximum entity ids per sketch.
    pub max_entities: u32,
}

impl EvidenceSketchCfgV1 {
    /// Conservative default sketch configuration.
    pub fn new() -> EvidenceSketchCfgV1 {
        EvidenceSketchCfgV1 { enable: true, max_terms: 16, max_entities: 16 }
    }
}

/// Configuration for evidence bundle building.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvidenceBuildCfgV1 {
    /// Sketch configuration.
    pub sketch: EvidenceSketchCfgV1,
    /// If true, ensure each referenced segment exists and the row index is in range.
    pub verify_refs: bool,
    /// If true and limits.segments_touched is zero, infer it from unique segments in hits.
    pub infer_segments_touched: bool,
}

impl EvidenceBuildCfgV1 {
    /// Conservative default evidence builder configuration.
    pub fn new() -> EvidenceBuildCfgV1 {
        EvidenceBuildCfgV1 {
            sketch: EvidenceSketchCfgV1::new(),
            verify_refs: true,
            infer_segments_touched: true,
        }
    }
}

/// Errors that can occur while building evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvidenceBuildError {
    /// Artifact store or segment decode error.
    Store(String),
    /// A referenced frame segment was not found in the store.
    MissingFrameSegment,
    /// A referenced row index was out of range for its segment.
    RowOutOfRange,
}

impl core::fmt::Display for EvidenceBuildError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            EvidenceBuildError::Store(s) => write!(f, "store: {}", s),
            EvidenceBuildError::MissingFrameSegment => f.write_str("missing frame segment"),
            EvidenceBuildError::RowOutOfRange => f.write_str("row index out of range"),
        }
    }
}

fn map_store_err(e: FrameStoreError) -> EvidenceBuildError {
    EvidenceBuildError::Store(e.to_string())
}

/// Build a canonical EvidenceBundleV1 from ranked SearchHits.
///
/// Inputs:
/// - `hits` are treated as suggestions and may contain duplicates.
/// - Scores are clamped into i64.
/// - Items are ranked deterministically and capped by `limits.max_items`.
/// - If sketches are enabled, sketches are attached in canonical item order
/// until the `limits.max_bytes` budget would be exceeded.
pub fn build_evidence_bundle_v1_from_hits<S: ArtifactStore>(
    store: &S,
    query_id: Hash32,
    snapshot_id: Hash32,
    mut limits: EvidenceLimitsV1,
    score_model_id: u32,
    hits: &[SearchHit],
    cfg: &EvidenceBuildCfgV1,
) -> Result<EvidenceBundleV1, EvidenceBuildError> {
    if cfg.infer_segments_touched && limits.segments_touched == 0 {
        let mut segs: Vec<Hash32> = Vec::new();
        for h in hits.iter() {
            segs.push(h.frame_seg);
        }
        segs.sort_unstable();
        segs.dedup();
        limits.segments_touched = if segs.len() > (u32::MAX as usize) {
            u32::MAX
        } else {
            segs.len() as u32
        };
    }

    // Pass 1: normalize to unique (segment_id, row_ix) keeping the highest score.
    let mut flat: Vec<(Hash32, u32, i64)> = Vec::with_capacity(hits.len());
    for h in hits.iter() {
        let score_i64 = if h.score > (i64::MAX as u64) { i64::MAX } else { h.score as i64 };
        flat.push((h.frame_seg, h.row_ix, score_i64));
    }

    // Sort by (segment_id asc, row_ix asc, score desc) to keep best first per key.
    flat.sort_by(|a, b| {
        let (sa, ra, sca) = a;
        let (sb, rb, scb) = b;
        match sa.cmp(sb) {
            core::cmp::Ordering::Equal => match ra.cmp(rb) {
                core::cmp::Ordering::Equal => scb.cmp(sca),
                other => other,
            },
            other => other,
        }
    });

    let mut items: Vec<EvidenceItemV1> = Vec::new();
    let mut last_key: Option<(Hash32, u32)> = None;
    for (seg, row_ix, score) in flat.into_iter() {
        let key = (seg, row_ix);
        if let Some(prev) = last_key {
            if prev.0 == key.0 && prev.1 == key.1 {
                continue;
            }
        }
        last_key = Some(key);
        items.push(EvidenceItemV1 {
            score,
            data: EvidenceItemDataV1::Frame(FrameRowRefV1 { segment_id: seg, row_ix, sketch: None }),
        });
    }

    let mut bundle = EvidenceBundleV1::new(query_id, snapshot_id, limits, score_model_id);
    bundle.items = items;
    bundle.canonicalize_in_place().map_err(|e| EvidenceBuildError::Store(e.to_string()))?;

    if bundle.limits.max_items != 0 && bundle.items.len() > (bundle.limits.max_items as usize) {
        bundle.items.truncate(bundle.limits.max_items as usize);
    }

    // Pass 2: verify references and optionally attach sketches.
    if cfg.verify_refs || cfg.sketch.enable {
        attach_sketches_and_verify(store, &mut bundle, cfg)?;
        bundle.canonicalize_in_place().map_err(|e| EvidenceBuildError::Store(e.to_string()))?;
    }

    Ok(bundle)
}

/// Cached evidence bundle build with an optional control record.
///
/// This is identical to [`build_evidence_bundle_v1_from_hits_cached`] and
/// ignores `control` in v1.
pub fn build_evidence_bundle_v1_from_hits_cached_with_control<S: ArtifactStore>(
    store: &S,
    frame_cache: &mut Cache2Q<Hash32, Arc<FrameSegmentV1>>,
    query_id: Hash32,
    snapshot_id: Hash32,
    limits: EvidenceLimitsV1,
    score_model_id: u32,
    hits: &[SearchHit],
    cfg: &EvidenceBuildCfgV1,
    control: Option<&RetrievalControlV1>,
) -> Result<EvidenceBundleV1, EvidenceBuildError> {
    let _ = control;
    build_evidence_bundle_v1_from_hits_cached(
        store,
        frame_cache,
        query_id,
        snapshot_id,
        limits,
        score_model_id,
        hits,
        cfg,
    )
}

/// Build an evidence bundle with an optional control record.
///
/// integrates pragmatics as a control-signal track. v1 of this
/// integration does not change evidence selection or ordering.
pub fn build_evidence_bundle_v1_from_hits_with_control<S: ArtifactStore>(
    store: &S,
    query_id: Hash32,
    snapshot_id: Hash32,
    limits: EvidenceLimitsV1,
    score_model_id: u32,
    hits: &[SearchHit],
    cfg: &EvidenceBuildCfgV1,
    control: Option<&RetrievalControlV1>,
) -> Result<EvidenceBundleV1, EvidenceBuildError> {
    let _ = control;
    build_evidence_bundle_v1_from_hits(store, query_id, snapshot_id, limits, score_model_id, hits, cfg)
}

/// Build a canonical EvidenceBundleV1 from ranked SearchHits, using a warm FrameSegment cache.
///
/// This is identical to [`build_evidence_bundle_v1_from_hits`] but reuses decoded
/// FrameSegmentV1 values across calls when attaching sketches or verifying refs.
///
/// The cache does not change ranking or canonicalization; it only reduces artifact reads
/// and decode work.
pub fn build_evidence_bundle_v1_from_hits_cached<S: ArtifactStore>(
    store: &S,
    frame_cache: &mut Cache2Q<Hash32, Arc<FrameSegmentV1>>,
    query_id: Hash32,
    snapshot_id: Hash32,
    mut limits: EvidenceLimitsV1,
    score_model_id: u32,
    hits: &[SearchHit],
    cfg: &EvidenceBuildCfgV1,
) -> Result<EvidenceBundleV1, EvidenceBuildError> {
    if cfg.infer_segments_touched && limits.segments_touched == 0 {
        let mut segs: Vec<Hash32> = Vec::new();
        for h in hits.iter() {
            segs.push(h.frame_seg);
        }
        segs.sort_unstable();
        segs.dedup();
        limits.segments_touched = if segs.len() > (u32::MAX as usize) {
            u32::MAX
        } else {
            segs.len() as u32
        };
    }

    // Pass 1: normalize to unique (segment_id, row_ix) keeping the highest score.
    let mut flat: Vec<(Hash32, u32, i64)> = Vec::with_capacity(hits.len());
    for h in hits.iter() {
        let score_i64 = if h.score > (i64::MAX as u64) { i64::MAX } else { h.score as i64 };
        flat.push((h.frame_seg, h.row_ix, score_i64));
    }

    // Sort by (segment_id asc, row_ix asc, score desc) to keep best first per key.
    flat.sort_by(|a, b| {
        let (sa, ra, sca) = a;
        let (sb, rb, scb) = b;
        match sa.cmp(sb) {
            core::cmp::Ordering::Equal => match ra.cmp(rb) {
                core::cmp::Ordering::Equal => scb.cmp(sca),
                other => other,
            },
            other => other,
        }
    });

    let mut items: Vec<EvidenceItemV1> = Vec::new();
    let mut last_key: Option<(Hash32, u32)> = None;
    for (seg, row_ix, score) in flat.into_iter() {
        let key = (seg, row_ix);
        if let Some(prev) = last_key {
            if prev.0 == key.0 && prev.1 == key.1 {
                continue;
            }
        }
        last_key = Some(key);
        items.push(EvidenceItemV1 {
            score,
            data: EvidenceItemDataV1::Frame(FrameRowRefV1 { segment_id: seg, row_ix, sketch: None }),
        });
    }

    let mut bundle = EvidenceBundleV1::new(query_id, snapshot_id, limits, score_model_id);
    bundle.items = items;
    bundle.canonicalize_in_place().map_err(|e| EvidenceBuildError::Store(e.to_string()))?;

    if bundle.limits.max_items != 0 && bundle.items.len() > (bundle.limits.max_items as usize) {
        bundle.items.truncate(bundle.limits.max_items as usize);
    }

    // Pass 2: verify references and optionally attach sketches.
    if cfg.verify_refs || cfg.sketch.enable {
        attach_sketches_and_verify_cached(store, frame_cache, &mut bundle, cfg)?;
        bundle.canonicalize_in_place().map_err(|e| EvidenceBuildError::Store(e.to_string()))?;
    }

    Ok(bundle)
}


fn attach_sketches_and_verify<S: ArtifactStore>(
    store: &S,
    bundle: &mut EvidenceBundleV1,
    cfg: &EvidenceBuildCfgV1,
) -> Result<(), EvidenceBuildError> {
    // Byte budget baseline assumes no sketches.
    let mut used = BUNDLE_HEADER_BYTES + (bundle.items.len() * FRAME_ITEM_BASE_BYTES);
    let max_bytes = bundle.limits.max_bytes as usize;

    let mut cache: Vec<(Hash32, crate::frame_segment::FrameSegmentV1)> = Vec::new();

    for it in bundle.items.iter_mut() {
        let (seg_id, row_ix) = match &mut it.data {
            EvidenceItemDataV1::Frame(r) => (r.segment_id, r.row_ix),
            _ => continue,
        };

        let seg = get_or_load_segment(store, &mut cache, &seg_id)?;
        let row_opt = seg.get_row(row_ix);
        let row = match row_opt {
            Some(r) => r,
            None => {
                if cfg.verify_refs {
                    return Err(EvidenceBuildError::RowOutOfRange);
                }
                continue;
            }
        };

        if !cfg.sketch.enable {
            continue;
        }

        let sk = row_to_sketch(&row, &cfg.sketch);
        let extra = sketch_extra_bytes(&sk);

        if max_bytes != 0 && used.saturating_add(extra) > max_bytes {
            continue;
        }

        if let EvidenceItemDataV1::Frame(r) = &mut it.data {
            r.sketch = Some(sk);
        }
        used = used.saturating_add(extra);
    }

    Ok(())
}

fn attach_sketches_and_verify_cached<S: ArtifactStore>(
    store: &S,
    frame_cache: &mut Cache2Q<Hash32, Arc<FrameSegmentV1>>,
    bundle: &mut EvidenceBundleV1,
    cfg: &EvidenceBuildCfgV1,
) -> Result<(), EvidenceBuildError> {
    // Byte budget baseline assumes no sketches.
    let mut used = BUNDLE_HEADER_BYTES + (bundle.items.len() * FRAME_ITEM_BASE_BYTES);
    let max_bytes = bundle.limits.max_bytes as usize;

    // Local per-call map from segment id to the Arc loaded from the warm cache.
    let mut local: Vec<(Hash32, Arc<FrameSegmentV1>)> = Vec::new();

    for it in bundle.items.iter_mut() {
        let (seg_id, row_ix) = match &mut it.data {
            EvidenceItemDataV1::Frame(r) => (r.segment_id, r.row_ix),
            _ => continue,
        };

        let seg = get_or_load_segment_cached(store, frame_cache, &mut local, &seg_id)?;
        let row_opt = seg.get_row(row_ix);
        let row = match row_opt {
            Some(r) => r,
            None => {
                if cfg.verify_refs {
                    return Err(EvidenceBuildError::RowOutOfRange);
                }
                continue;
            }
        };

        if !cfg.sketch.enable {
            continue;
        }

        let sk = row_to_sketch(&row, &cfg.sketch);
        let extra = sketch_extra_bytes(&sk);

        if max_bytes != 0 && used.saturating_add(extra) > max_bytes {
            continue;
        }

        if let EvidenceItemDataV1::Frame(r) = &mut it.data {
            r.sketch = Some(sk);
        }
        used = used.saturating_add(extra);
    }

    Ok(())
}

fn get_or_load_segment_cached<'a, S: ArtifactStore>(
    store: &S,
    frame_cache: &mut Cache2Q<Hash32, Arc<FrameSegmentV1>>,
    local: &'a mut Vec<(Hash32, Arc<FrameSegmentV1>)>,
    seg_id: &Hash32,
) -> Result<&'a FrameSegmentV1, EvidenceBuildError> {
    if let Some(pos) = local.iter().position(|(h, _)| h == seg_id) {
        return Ok(local[pos].1.as_ref());
    }

    let seg_opt = get_frame_segment_v1_cached(store, frame_cache, seg_id).map_err(map_store_err)?;
    let seg = match seg_opt {
        Some(s) => s,
        None => return Err(EvidenceBuildError::MissingFrameSegment),
    };

    local.push((*seg_id, seg));
    let idx = local.len() - 1;
    Ok(local[idx].1.as_ref())
}


fn get_or_load_segment<'a, S: ArtifactStore>(
    store: &S,
    cache: &'a mut Vec<(Hash32, crate::frame_segment::FrameSegmentV1)>,
    seg_id: &Hash32,
) -> Result<&'a crate::frame_segment::FrameSegmentV1, EvidenceBuildError> {
    if let Some(pos) = cache.iter().position(|(h, _)| h == seg_id) {
        return Ok(&cache[pos].1);
    }

    let seg_opt = get_frame_segment_v1(store, seg_id).map_err(map_store_err)?;
    let seg = match seg_opt {
        Some(s) => s,
        None => return Err(EvidenceBuildError::MissingFrameSegment),
    };

    cache.push((seg_id.clone(), seg));
    let idx = cache.len() - 1;
    Ok(&cache[idx].1)
}


fn fold_u64_to_u32(x: u64) -> u32 {
    (x as u32) ^ ((x >> 32) as u32)
}

fn row_to_sketch(row: &FrameRowV1, cfg: &EvidenceSketchCfgV1) -> FrameRowSketchV1 {
    let mut entity_ids: Vec<u32> = Vec::new();

    if let Some(who) = row.who {
        entity_ids.push(fold_u64_to_u32(who.0.0));
    }
    if let Some(what) = row.what {
        entity_ids.push(fold_u64_to_u32(what.0.0));
    }
    for e in row.entity_ids.iter() {
        entity_ids.push(fold_u64_to_u32(e.0.0));
    }
    entity_ids.sort_unstable();
    entity_ids.dedup();
    if cfg.max_entities != 0 && entity_ids.len() > (cfg.max_entities as usize) {
        entity_ids.truncate(cfg.max_entities as usize);
    }

    let mut tmp_terms: Vec<(u32, u32, u64)> = Vec::new();
    for t in row.terms.iter() {
        if t.tf == 0 {
            continue;
        }
        let term_u64 = t.term.0.0;
        let sig = fold_u64_to_u32(term_u64);
        tmp_terms.push((sig, t.tf, term_u64));
    }

    // Sort by (tf desc, sig asc, term_u64 asc).
    tmp_terms.sort_by(|a, b| {
        let (sa, tfa, ua) = *a;
        let (sb, tfb, ub) = *b;
        match tfb.cmp(&tfa) {
            core::cmp::Ordering::Equal => match sa.cmp(&sb) {
                core::cmp::Ordering::Equal => ua.cmp(&ub),
                other => other,
            },
            other => other,
        }
    });

    let mut seen: rustc_hash::FxHashSet<u32> =
        rustc_hash::FxHashSet::with_capacity_and_hasher(tmp_terms.len(), Default::default());
    let mut terms: Vec<TermTfV1> = Vec::new();
    let max_terms = cfg.max_terms as usize;

    for (sig, tf, _u64) in tmp_terms.into_iter() {
        if !seen.insert(sig) {
            continue;
        }
        terms.push(TermTfV1 { term_id: sig, tf });
        if cfg.max_terms != 0 && terms.len() >= max_terms {
            break;
        }
    }

    // meta_codes reserved for later stages.
    FrameRowSketchV1 { terms, entity_ids, meta_codes: Vec::new() }
}

fn sketch_extra_bytes(sk: &FrameRowSketchV1) -> usize {
    // entity_ids vec: u32 len + u32 items
    // meta_codes vec: u32 len + u32 items
    // terms vec: u32 len + (term_id u32 + tf u32) items
    let e = sk.entity_ids.len();
    let m = sk.meta_codes.len();
    let t = sk.terms.len();
    12usize
        .saturating_add(4usize.saturating_mul(e))
        .saturating_add(4usize.saturating_mul(m))
        .saturating_add(8usize.saturating_mul(t))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
    use crate::frame::{derive_id64, DocId, EntityId, FrameRowV1, SourceId};
    use crate::frame_segment::FrameSegmentV1;
    use crate::frame_store::put_frame_segment_v1;
    use crate::tokenizer::{term_id_from_token, TokenizerCfg};
    use std::fs;
    use std::path::PathBuf;

    fn tmp_dir(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push("fsa_lm_tests");
        p.push(name);
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn h(b: u8) -> Hash32 {
        [b; 32]
    }

    #[test]
    fn build_dedup_and_budgeted_sketches() {
        let dir = tmp_dir("evidence_builder_dedup_and_budgeted_sketches");
        let store = FsArtifactStore::new(&dir).unwrap();

        let doc_id = DocId(derive_id64(b"doc\0", b"d1"));
        let source_id = SourceId(derive_id64(b"src\0", b"s1"));

        let mut r0 = FrameRowV1::new(doc_id, source_id);
        let e1 = EntityId(derive_id64(b"ent\0", b"alice"));
        let e2 = EntityId(derive_id64(b"ent\0", b"bob"));
        r0.who = Some(e1);
        r0.what = Some(e2);
        let tc = TokenizerCfg { max_token_bytes: 32 };
        r0.terms.push(crate::frame::TermFreq { term: term_id_from_token("hello", tc), tf: 1 });
        r0.terms.push(crate::frame::TermFreq { term: term_id_from_token("world", tc), tf: 3 });
        r0.terms.sort_unstable_by_key(|t| t.term.0.0);
        r0.doc_len = 4;

        let mut r1 = FrameRowV1::new(doc_id, source_id);
        // More terms so the sketch is larger.
        r1.terms.push(crate::frame::TermFreq { term: term_id_from_token("alpha", tc), tf: 1 });
        r1.terms.push(crate::frame::TermFreq { term: term_id_from_token("beta", tc), tf: 1 });
        r1.terms.push(crate::frame::TermFreq { term: term_id_from_token("gamma", tc), tf: 1 });
        r1.terms.push(crate::frame::TermFreq { term: term_id_from_token("delta", tc), tf: 1 });
        r1.terms.push(crate::frame::TermFreq { term: term_id_from_token("epsilon", tc), tf: 1 });
        r1.terms.push(crate::frame::TermFreq { term: term_id_from_token("zeta", tc), tf: 1 });

        r1.terms.sort_unstable_by_key(|t| t.term.0.0);
        r1.doc_len = 6;

        let seg = FrameSegmentV1::from_rows(&[r0.clone(), r1.clone()], 1024).unwrap();
        let seg_hash = put_frame_segment_v1(&store, &seg).unwrap();

        let hits = vec![
            SearchHit { frame_seg: seg_hash, row_ix: 0, score: 10 },
            SearchHit { frame_seg: seg_hash, row_ix: 0, score: 12 },
            SearchHit { frame_seg: seg_hash, row_ix: 1, score: 8 },
        ];

        // Base bytes with 2 items and no sketches:
        // header(86) + 2 * item_base(46) = 178.
        // Budget leaves room for only the first sketch.
        let limits = EvidenceLimitsV1 { segments_touched: 0, max_items: 2, max_bytes: 220 };
        let mut cfg = EvidenceBuildCfgV1::new();
        cfg.sketch.max_terms = 4;
        cfg.sketch.max_entities = 8;

        let b = build_evidence_bundle_v1_from_hits(&store, h(1), h(2), limits, 7, &hits, &cfg).unwrap();
        assert_eq!(b.items.len(), 2);

        // Dedup keeps the higher score for row 0.
        match &b.items[0].data {
            EvidenceItemDataV1::Frame(r) => {
                assert_eq!(r.segment_id, seg_hash);
                assert_eq!(r.row_ix, 0);
                assert!(r.sketch.is_some());
            }
            _ => panic!("expected frame"),
        }

        match &b.items[1].data {
            EvidenceItemDataV1::Frame(r) => {
                assert_eq!(r.segment_id, seg_hash);
                assert_eq!(r.row_ix, 1);
                assert!(r.sketch.is_none());
            }
            _ => panic!("expected frame"),
        }

        // Sketch term order: tf desc, tie by term_id asc.
        let sk = match &b.items[0].data {
            EvidenceItemDataV1::Frame(r) => r.sketch.as_ref().unwrap(),
            _ => panic!("expected frame"),
        };
        if sk.terms.len() >= 2 {
            let a = sk.terms[0];
            let c = sk.terms[1];
            assert!(a.tf >= c.tf);
            if a.tf == c.tf {
                assert!(a.term_id < c.term_id);
            }
        }

        // Entity ids sorted unique.
        for i in 1..sk.entity_ids.len() {
            assert!(sk.entity_ids[i - 1] < sk.entity_ids[i]);
        }

        // Encoded bundle respects the byte cap.
        let enc = b.encode().unwrap();
        assert!(enc.len() <= (limits.max_bytes as usize));
    }
}
