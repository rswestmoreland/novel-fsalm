// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Retrieval policy configuration and counters.
//!
//! Purpose
//! -------
//! The index query engine (`index_query`) provides deterministic scoring and
//! low-level controls (k, entry caps, dense threshold, and gating variants).
//! introduces an explicit "retrieval policy" layer that:
//! - defines small, versioned configuration knobs (budgets and toggles)
//! - collects stable counters for observability and replay
//! - remains evidence-first (policy must not inject non-determinism)
//!
//! introduces the policy types () and an apply wrapper ().

use crate::retrieval_gating::GateStatsV1;
use crate::artifact::ArtifactStore;
use crate::hash::Hash32;
use crate::index_query::{IndexQueryError, QueryTerm, SearchCfg, SearchHit};
use crate::retrieval_control::RetrievalControlV1;

/// Retrieval policy config schema version.
pub const RETRIEVAL_POLICY_CFG_V1_VERSION: u16 = 1;

/// Errors returned by [`RetrievalPolicyCfgV1::validate`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetrievalPolicyCfgError {
    /// The version field is not supported.
    BadVersion,
    /// max_query_terms must be at least 1.
    MaxQueryTermsZero,
    /// max_hits must be at least 1.
    MaxHitsZero,
    /// include_ties_at_cutoff must be 0 or 1.
    BadIncludeTiesFlag,
    /// dense_row_threshold must be non-zero.
    DenseRowThresholdZero,
    /// enable_query_expansion must be 0 or 1.
    BadQueryExpansionFlag,
    /// max_hits_per_frame_seg must be 0 (disabled) or >= 1.
    BadMaxHitsPerFrameSeg,
    /// max_hits_per_doc must be 0 (disabled) or >= 1.
    BadMaxHitsPerDoc,
    /// novelty_mode must be 0..=3.
    BadNoveltyMode,
}

impl core::fmt::Display for RetrievalPolicyCfgError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            RetrievalPolicyCfgError::BadVersion => f.write_str("bad version"),
            RetrievalPolicyCfgError::MaxQueryTermsZero => f.write_str("max_query_terms must be >= 1"),
            RetrievalPolicyCfgError::MaxHitsZero => f.write_str("max_hits must be >= 1"),
            RetrievalPolicyCfgError::BadIncludeTiesFlag => f.write_str("include_ties_at_cutoff must be 0 or 1"),
            RetrievalPolicyCfgError::DenseRowThresholdZero => f.write_str("dense_row_threshold must be >= 1"),
            RetrievalPolicyCfgError::BadQueryExpansionFlag => f.write_str("enable_query_expansion must be 0 or 1"),
            RetrievalPolicyCfgError::BadMaxHitsPerFrameSeg => f.write_str("max_hits_per_frame_seg must be 0 or >= 1"),
            RetrievalPolicyCfgError::BadMaxHitsPerDoc => f.write_str("max_hits_per_doc must be 0 or >= 1"),
            RetrievalPolicyCfgError::BadNoveltyMode => f.write_str("novelty_mode must be 0..=3"),
        }
    }
}

impl std::error::Error for RetrievalPolicyCfgError {}

/// Retrieval policy configuration (v1).
///
/// Canonical rules (v1):
/// - `version` must be [`RETRIEVAL_POLICY_CFG_V1_VERSION`]
/// - `max_query_terms` and `max_hits` are bounded and non-zero
/// - `include_ties_at_cutoff` is a bool encoded as 0/1
///
/// This config is applied by higher-level wrappers in future updates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetrievalPolicyCfgV1 {
    /// Schema version.
    pub version: u16,
    /// Maximum number of query terms used for retrieval.
    ///
    /// Terms beyond this cap may be dropped by policy (future updates).
    pub max_query_terms: u16,
    /// Maximum number of hits returned by retrieval.
    pub max_hits: u16,
    /// Limit the number of snapshot entries to scan (0 means no limit).
    pub entry_cap: u32,
    /// Include all hits tied with the cutoff score (0/1).
    ///
    /// This is a policy-level toggle that maps to the tie-inclusive behavior
    /// implemented in the index query layer.
    pub include_ties_at_cutoff: u8,
    /// Enable query expansion using the lexicon (0/1).
    ///
    /// When set, higher-level wrappers may expand query terms using a
    /// LexiconSnapshot-derived lookup before applying caps.
    pub enable_query_expansion: u8,
    /// If row_count is <= this threshold, use dense scoring arrays.
    pub dense_row_threshold: u32,

    /// Maximum hits allowed per FrameSegment (0 means disabled).
    ///
    /// This is a diversity cap to avoid a single segment dominating the hit list.
    pub max_hits_per_frame_seg: u16,

    /// Maximum hits allowed per DocId (0 means disabled).
    ///
    /// This is a diversity cap to avoid a single document dominating the hit list.
    pub max_hits_per_doc: u16,

    /// Novelty scoring mode (0 disables).
    ///
    /// 0 = off
    /// 1 = doc frequency (prefer rarer docs)
    /// 2 = frame segment frequency (prefer rarer segments)
    /// 3 = doc + frame segment frequency
    pub novelty_mode: u8,
}

impl RetrievalPolicyCfgV1 {
    /// Create a default policy config for interactive usage.
    pub fn new() -> RetrievalPolicyCfgV1 {
        RetrievalPolicyCfgV1 {
            version: RETRIEVAL_POLICY_CFG_V1_VERSION,
            max_query_terms: 32,
            max_hits: 10,
            entry_cap: 0,
            include_ties_at_cutoff: 1,
            enable_query_expansion: 0,
            dense_row_threshold: 200_000,
            max_hits_per_frame_seg: 0,
            max_hits_per_doc: 0,
            novelty_mode: 0,
        }
    }

    /// Validate canonical invariants.
    pub fn validate(&self) -> Result<(), RetrievalPolicyCfgError> {
        if self.version != RETRIEVAL_POLICY_CFG_V1_VERSION {
            return Err(RetrievalPolicyCfgError::BadVersion);
        }
        if self.max_query_terms == 0 {
            return Err(RetrievalPolicyCfgError::MaxQueryTermsZero);
        }
        if self.max_hits == 0 {
            return Err(RetrievalPolicyCfgError::MaxHitsZero);
        }
        if self.include_ties_at_cutoff > 1 {
            return Err(RetrievalPolicyCfgError::BadIncludeTiesFlag);
        }
        if self.enable_query_expansion > 1 {
            return Err(RetrievalPolicyCfgError::BadQueryExpansionFlag);
        }
        if self.dense_row_threshold == 0 {
            return Err(RetrievalPolicyCfgError::DenseRowThresholdZero);
        }
        if self.max_hits_per_frame_seg != 0 && self.max_hits_per_frame_seg < 1 {
            return Err(RetrievalPolicyCfgError::BadMaxHitsPerFrameSeg);
        }
        if self.max_hits_per_doc != 0 && self.max_hits_per_doc < 1 {
            return Err(RetrievalPolicyCfgError::BadMaxHitsPerDoc);
        }
        if self.novelty_mode > 3 {
            return Err(RetrievalPolicyCfgError::BadNoveltyMode);
        }
        Ok(())
    }
}

impl Default for RetrievalPolicyCfgV1 {
    fn default() -> Self {
        RetrievalPolicyCfgV1::new()
    }
}

/// Retrieval policy counters (v1).
///
/// This is intended for logs, replay, and diagnostics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RetrievalPolicyStatsV1 {
    /// Total query terms available to the policy.
    pub query_terms_total: u32,
    /// Query terms from the base tokenizer (before optional expansion).
    pub query_terms_original: u32,
    /// New unique query terms added via optional expansion.
    pub query_terms_expanded_new: u32,
    /// Query terms used after applying policy caps.
    pub query_terms_used: u32,
    /// Total hits produced before final truncation.
    pub hits_total: u32,
    /// Hits returned to the caller after truncation.
    pub hits_returned: u32,
    /// Hits included beyond `max_hits` due to tie-inclusive cutoff.
    pub hits_included_ties: u32,
    /// Stage 1 gating counters (signature-based skip).
    pub gate: GateStatsV1,
}

impl RetrievalPolicyStatsV1 {
    /// Create a new zeroed stats struct.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Errors returned by [`apply_retrieval_policy_v1`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetrievalPolicyApplyError {
    /// Policy config was invalid.
    Cfg(RetrievalPolicyCfgError),
    /// Index query failed.
    Query(IndexQueryError),
    /// Query expansion config was invalid.
    ExpandCfg(crate::query_expansion::QueryExpansionCfgError),
    /// Query expansion was enabled but no lexicon snapshot hash was provided.
    ExpandMissingLexiconSnapshot,
    /// Query expansion was enabled but the lexicon snapshot is missing from the store.
    ExpandLexiconSnapshotNotFound(Hash32),
    /// Lexicon expand-lookup load failed.
    ExpandLexiconLookup(String),
    /// Bridge expansion failed.
    ExpandBridge(String),
    /// Frame segment required for diversity caps was not found.
    RefineFrameSegmentNotFound(Hash32),
    /// Frame segment required for diversity caps failed to decode.
    RefineFrameSegmentDecode(String),
}

impl core::fmt::Display for RetrievalPolicyApplyError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            RetrievalPolicyApplyError::Cfg(e) => {
                f.write_str("policy cfg invalid: ")?;
                core::fmt::Display::fmt(e, f)
            }
            RetrievalPolicyApplyError::Query(e) => {
                f.write_str("index query failed: ")?;
                // IndexQueryError does not implement Display; use Debug for a stable error string.
                write!(f, "{:?}", e)
            }
            RetrievalPolicyApplyError::ExpandCfg(e) => {
                f.write_str("query expansion cfg invalid: ")?;
                core::fmt::Display::fmt(e, f)
            }
            RetrievalPolicyApplyError::ExpandMissingLexiconSnapshot => {
                f.write_str("query expansion enabled but lexicon snapshot hash missing")
            }
            RetrievalPolicyApplyError::ExpandLexiconSnapshotNotFound(h) => {
                write!(f, "lexicon snapshot not found: {}", crate::hash::hex32(&h))
            }
            RetrievalPolicyApplyError::ExpandLexiconLookup(msg) => {
                write!(f, "lexicon expand-lookup load failed: {}", msg)
            }
            RetrievalPolicyApplyError::ExpandBridge(msg) => {
                write!(f, "bridge expansion failed: {}", msg)
            }
            RetrievalPolicyApplyError::RefineFrameSegmentNotFound(h) => {
                write!(f, "frame segment not found during refine: {}", crate::hash::hex32(&h))
            }
            RetrievalPolicyApplyError::RefineFrameSegmentDecode(msg) => {
                write!(f, "frame segment decode failed during refine: {}", msg)
            }
        }
    }
}

impl std::error::Error for RetrievalPolicyApplyError {}

impl From<RetrievalPolicyCfgError> for RetrievalPolicyApplyError {
    fn from(e: RetrievalPolicyCfgError) -> Self {
        RetrievalPolicyApplyError::Cfg(e)
    }
}

impl From<IndexQueryError> for RetrievalPolicyApplyError {
    fn from(e: IndexQueryError) -> Self {
        RetrievalPolicyApplyError::Query(e)
    }
}

impl From<crate::query_expansion::QueryExpansionCfgError> for RetrievalPolicyApplyError {
    fn from(e: crate::query_expansion::QueryExpansionCfgError) -> Self {
        RetrievalPolicyApplyError::ExpandCfg(e)
    }
}

fn seed64_from_control_id(id: &Hash32) -> u64 {
    u64::from_le_bytes([id[0], id[1], id[2], id[3], id[4], id[5], id[6], id[7]])
}

fn mix64(mut z: u64) -> u64 {
    // splitmix64 finalizer (deterministic, integer-only)
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

fn tiebreak_key(seed: u64, frame_seg: &Hash32, row_ix: u32) -> u64 {
    let seg0 = u64::from_le_bytes([
        frame_seg[0], frame_seg[1], frame_seg[2], frame_seg[3], frame_seg[4], frame_seg[5], frame_seg[6], frame_seg[7],
    ]);
    let x = seed ^ seg0 ^ (row_ix as u64).wrapping_mul(0x9E3779B97F4A7C15);
    mix64(x.wrapping_add(0x9E3779B97F4A7C15))
}

fn frame_segment_doc_id(seg: &crate::frame_segment::FrameSegmentV1, row_ix: u32) -> Option<crate::frame::DocId> {
    let cr = seg.chunk_rows;
    if cr == 0 {
        return None;
    }
    let cix = (row_ix / cr) as usize;
    let off = (row_ix % cr) as usize;
    if cix >= seg.chunks.len() {
        return None;
    }
    let c = &seg.chunks[cix];
    if off >= (c.rows as usize) {
        return None;
    }
    if off >= c.doc_id.len() {
        return None;
    }
    Some(crate::frame::DocId(crate::frame::Id64(c.doc_id[off])))
}

fn rerank_hits_novelty_v1<S: ArtifactStore>(
    store: &S,
    hits: &mut Vec<SearchHit>,
    policy: &RetrievalPolicyCfgV1,
    tie_seed: Option<u64>,
) -> Result<(), RetrievalPolicyApplyError> {
    if hits.len() <= 1 {
        return Ok(());
    }
    let mode = policy.novelty_mode;
    if mode == 0 {
        return Ok(());
    }
    let doc_enabled = mode == 1 || mode == 3;
    let seg_enabled = mode == 2 || mode == 3;

    // Frequency counts are computed over the full candidate list.
    let mut seg_freq: Vec<(Hash32, u16)> = Vec::new();
    let mut doc_freq: Vec<(crate::frame::DocId, u16)> = Vec::new();
    let mut doc_ids: Vec<Option<crate::frame::DocId>> = Vec::new();
    let mut seg_cache: Vec<(Hash32, crate::frame_segment::FrameSegmentV1)> = Vec::new();

    fn inc_hash_count(v: &mut Vec<(Hash32, u16)>, h: Hash32) {
        for i in 0..v.len() {
            if v[i].0 == h {
                v[i].1 = v[i].1.saturating_add(1);
                return;
            }
        }
        v.push((h, 1));
    }
    fn get_hash_count(v: &Vec<(Hash32, u16)>, h: Hash32) -> u16 {
        for i in 0..v.len() {
            if v[i].0 == h {
                return v[i].1;
            }
        }
        0
    }
    fn inc_doc_count(v: &mut Vec<(crate::frame::DocId, u16)>, d: crate::frame::DocId) {
        for i in 0..v.len() {
            if v[i].0 == d {
                v[i].1 = v[i].1.saturating_add(1);
                return;
            }
        }
        v.push((d, 1));
    }
    fn get_doc_count(v: &Vec<(crate::frame::DocId, u16)>, d: crate::frame::DocId) -> u16 {
        for i in 0..v.len() {
            if v[i].0 == d {
                return v[i].1;
            }
        }
        0
    }

    fn get_or_load_seg<'a, S: ArtifactStore>(
        store: &S,
        cache: &'a mut Vec<(Hash32, crate::frame_segment::FrameSegmentV1)>,
        h: &Hash32,
    ) -> Result<&'a crate::frame_segment::FrameSegmentV1, RetrievalPolicyApplyError> {
        for i in 0..cache.len() {
            if cache[i].0 == *h {
                return Ok(&cache[i].1);
            }
        }
        let bytes_opt = store
            .get(h)
            .map_err(|e| RetrievalPolicyApplyError::RefineFrameSegmentDecode(e.to_string()))?;
        let bytes = match bytes_opt {
            Some(b) => b,
            None => return Err(RetrievalPolicyApplyError::RefineFrameSegmentNotFound(*h)),
        };
        let seg = crate::frame_segment::FrameSegmentV1::decode(&bytes)
            .map_err(|e| RetrievalPolicyApplyError::RefineFrameSegmentDecode(format!("{:?}", e)))?;
        cache.push((*h, seg));
        let idx = cache.len() - 1;
        Ok(&cache[idx].1)
    }

    // First pass: compute segment frequency and (optional) doc frequency.
    for hit in hits.iter() {
        if seg_enabled {
            inc_hash_count(&mut seg_freq, hit.frame_seg);
        }
        if doc_enabled {
            let seg = get_or_load_seg(store, &mut seg_cache, &hit.frame_seg)?;
            let doc_id = frame_segment_doc_id(seg, hit.row_ix);
            doc_ids.push(doc_id);
            if let Some(d) = doc_id {
                inc_doc_count(&mut doc_freq, d);
            }
        }
    }
    if doc_enabled && doc_ids.len() != hits.len() {
        // Defensive; should never occur.
        while doc_ids.len() < hits.len() {
            doc_ids.push(None);
        }
    }

    // Compute per-hit novelty key and rerank as a stable secondary key.
    // Novelty is inverse-frequency: 65535 / freq (freq >= 1).
    let mut keyed: Vec<(SearchHit, u32, u64)> = Vec::with_capacity(hits.len());
    for (i, hit) in hits.iter().enumerate() {
        let mut doc_inv: u16 = 0;
        let mut seg_inv: u16 = 0;
        if doc_enabled {
            if let Some(d) = doc_ids[i] {
                let f = get_doc_count(&doc_freq, d);
                if f != 0 {
                    doc_inv = (u16::MAX / f) as u16;
                }
            }
        }
        if seg_enabled {
            let f = get_hash_count(&seg_freq, hit.frame_seg);
            if f != 0 {
                seg_inv = (u16::MAX / f) as u16;
            }
        }
        let novelty: u32 = ((doc_inv as u32) << 16) | (seg_inv as u32);
        let tk = match tie_seed {
            Some(seed) => tiebreak_key(seed, &hit.frame_seg, hit.row_ix),
            None => 0,
        };
        keyed.push((hit.clone(), novelty, tk));
    }

    keyed.sort_by(|a, b| {
        match b.0.score.cmp(&a.0.score) {
            core::cmp::Ordering::Equal => match b.1.cmp(&a.1) {
                core::cmp::Ordering::Equal => {
                    if tie_seed.is_some() {
                        match a.2.cmp(&b.2) {
                            core::cmp::Ordering::Equal => match a.0.frame_seg.cmp(&b.0.frame_seg) {
                                core::cmp::Ordering::Equal => a.0.row_ix.cmp(&b.0.row_ix),
                                other => other,
                            },
                            other => other,
                        }
                    } else {
                        match a.0.frame_seg.cmp(&b.0.frame_seg) {
                            core::cmp::Ordering::Equal => a.0.row_ix.cmp(&b.0.row_ix),
                            other => other,
                        }
                    }
                }
                other => other,
            },
            other => other,
        }
    });

    hits.clear();
    for (h, _nov, _tk) in keyed.into_iter() {
        hits.push(h);
    }

    Ok(())
}

fn refine_hits_diversity_caps_v1<S: ArtifactStore>(
    store: &S,
    hits: &[SearchHit],
    max_hits: usize,
    include_ties: bool,
    policy: &RetrievalPolicyCfgV1,
) -> Result<Vec<SearchHit>, RetrievalPolicyApplyError> {
    let seg_cap = policy.max_hits_per_frame_seg as u16;
    let doc_cap = policy.max_hits_per_doc as u16;
    let cap_seg_enabled = seg_cap != 0;
    let cap_doc_enabled = doc_cap != 0;

    let cutoff_score = if include_ties && hits.len() > max_hits && max_hits > 0 {
        Some(hits[max_hits - 1].score)
    } else {
        None
    };

    let mut out: Vec<SearchHit> = Vec::new();
    let mut seen: Vec<(Hash32, u32)> = Vec::new();
    let mut seg_counts: Vec<(Hash32, u16)> = Vec::new();
    let mut doc_counts: Vec<(crate::frame::DocId, u16)> = Vec::new();
    let mut seg_cache: Vec<(Hash32, crate::frame_segment::FrameSegmentV1)> = Vec::new();

    // Small helper: linear lookup/insert for deterministic counters.
    fn inc_hash_count(v: &mut Vec<(Hash32, u16)>, h: Hash32) -> u16 {
        for i in 0..v.len() {
            if v[i].0 == h {
                v[i].1 = v[i].1.saturating_add(1);
                return v[i].1;
            }
        }
        v.push((h, 1));
        1
    }
    fn get_hash_count(v: &Vec<(Hash32, u16)>, h: Hash32) -> u16 {
        for i in 0..v.len() {
            if v[i].0 == h {
                return v[i].1;
            }
        }
        0
    }
    fn inc_doc_count(v: &mut Vec<(crate::frame::DocId, u16)>, d: crate::frame::DocId) -> u16 {
        for i in 0..v.len() {
            if v[i].0 == d {
                v[i].1 = v[i].1.saturating_add(1);
                return v[i].1;
            }
        }
        v.push((d, 1));
        1
    }
    fn get_doc_count(v: &Vec<(crate::frame::DocId, u16)>, d: crate::frame::DocId) -> u16 {
        for i in 0..v.len() {
            if v[i].0 == d {
                return v[i].1;
            }
        }
        0
    }

    fn get_or_load_seg<'a, S: ArtifactStore>(
        store: &S,
        cache: &'a mut Vec<(Hash32, crate::frame_segment::FrameSegmentV1)>,
        h: &Hash32,
    ) -> Result<&'a crate::frame_segment::FrameSegmentV1, RetrievalPolicyApplyError> {
        for i in 0..cache.len() {
            if cache[i].0 == *h {
                return Ok(&cache[i].1);
            }
        }
        let bytes_opt = store
            .get(h)
            .map_err(|e| RetrievalPolicyApplyError::RefineFrameSegmentDecode(e.to_string()))?;
        let bytes = match bytes_opt {
            Some(b) => b,
            None => return Err(RetrievalPolicyApplyError::RefineFrameSegmentNotFound(*h)),
        };
        let seg = crate::frame_segment::FrameSegmentV1::decode(&bytes)
            .map_err(|e| RetrievalPolicyApplyError::RefineFrameSegmentDecode(format!("{:?}", e)))?;
        cache.push((*h, seg));
        let idx = cache.len() - 1;
        Ok(&cache[idx].1)
    }

    for hit in hits.iter() {
        if !include_ties && out.len() >= max_hits {
            break;
        }
        if let Some(cs) = cutoff_score {
            if out.len() >= max_hits && hit.score < cs {
                break;
            }
        }

        // Dedupe by exact row identity.
        let mut dup = false;
        for (h, ix) in seen.iter() {
            if *h == hit.frame_seg && *ix == hit.row_ix {
                dup = true;
                break;
            }
        }
        if dup {
            continue;
        }

        if cap_seg_enabled {
            let c = get_hash_count(&seg_counts, hit.frame_seg);
            if c >= seg_cap {
                continue;
            }
        }

        if cap_doc_enabled {
            let seg = get_or_load_seg(store, &mut seg_cache, &hit.frame_seg)?;
            let doc_id = match frame_segment_doc_id(seg, hit.row_ix) {
                Some(d) => d,
                None => {
                    // Row out of range; treat as filtered.
                    continue;
                }
            };
            let c = get_doc_count(&doc_counts, doc_id);
            if c >= doc_cap {
                continue;
            }
            inc_doc_count(&mut doc_counts, doc_id);
        }

        // Accept.
        seen.push((hit.frame_seg, hit.row_ix));
        if cap_seg_enabled {
            inc_hash_count(&mut seg_counts, hit.frame_seg);
        }
        out.push(hit.clone());
    }

    Ok(out)
}

/// Apply a retrieval policy to an already-built query term list.
///
/// This is a small wrapper around the low-level index query engine. It:
/// - validates the policy config
/// - truncates query terms to `max_query_terms`
/// - runs snapshot search (optionally gated)
/// - returns ranked hits plus stable policy counters
///
/// Determinism contract:
/// - No randomness.
/// - If `control` is present, tie-breaking uses its stable `control_id`.
/// - If `include_ties_at_cutoff` is set, all hits tied with the cutoff score
/// are included (even if this exceeds `max_hits`).
pub fn apply_retrieval_policy_v1<S: ArtifactStore>(
    store: &S,
    snapshot_hash: &Hash32,
    sig_map_hash: Option<&Hash32>,
    query_terms: &[QueryTerm],
    policy: &RetrievalPolicyCfgV1,
    control: Option<&RetrievalControlV1>,
) -> Result<(Vec<SearchHit>, RetrievalPolicyStatsV1), RetrievalPolicyApplyError> {
    policy.validate()?;

    let mut stats = RetrievalPolicyStatsV1::new();
    stats.query_terms_total = query_terms.len() as u32;
    stats.query_terms_original = stats.query_terms_total;

    let cap = policy.max_query_terms as usize;
    let used_terms: &[QueryTerm] = if query_terms.len() > cap { &query_terms[..cap] } else { query_terms };
    stats.query_terms_used = used_terms.len() as u32;

    let include_ties = policy.include_ties_at_cutoff != 0;

    // If diversity caps are enabled, we may need more than k candidates so we can
    // fill the final list after filtering.
    let caps_enabled = policy.max_hits_per_frame_seg != 0 || policy.max_hits_per_doc != 0;
    let novelty_enabled = policy.novelty_mode != 0;
    let mut engine_k: usize = policy.max_hits as usize;
    if (caps_enabled || novelty_enabled) && !include_ties {
        // Conservative oversample factor; deterministic.
        let k2 = engine_k.saturating_mul(8);
        engine_k = core::cmp::min(1024, core::cmp::max(engine_k, k2));
    }

    let scfg = SearchCfg {
        k: engine_k,
        entry_cap: policy.entry_cap as usize,
        dense_row_threshold: policy.dense_row_threshold,
    };
    let tie_seed = control.map(|c| {
        let id = c.control_id();
        seed64_from_control_id(&id)
    });

    let (hits0, gate_stats) = match sig_map_hash {
        Some(sig) => {
            let (h, gs) = crate::index_query::search_snapshot_gated_inner(
                store,
                snapshot_hash,
                sig,
                used_terms,
                &scfg,
                tie_seed,
                include_ties,
            )?;
            (h, gs)
        }
        None => {
            let h = crate::index_query::search_snapshot_inner(
                store,
                snapshot_hash,
                used_terms,
                &scfg,
                tie_seed,
                include_ties,
            )?;
            (h, GateStatsV1::new())
        }
    };

    stats.gate = gate_stats;

    stats.hits_total = hits0.len() as u32;

    // Optional novelty re-ranking.
    let mut hits1 = hits0;
    if novelty_enabled {
        rerank_hits_novelty_v1(store, &mut hits1, policy, tie_seed)?;
    }

    // Apply diversity caps (dedupe + per-segment/per-doc caps) if configured.
    let hits2 = if caps_enabled {
        refine_hits_diversity_caps_v1(store, &hits1, policy.max_hits as usize, include_ties, policy)?
    } else {
        // No caps: if novelty is enabled, we may have oversampled and must truncate.
        if novelty_enabled && !include_ties {
            let k = policy.max_hits as usize;
            if hits1.len() > k {
                hits1.truncate(k);
            }
        }
        hits1
    };
    stats.hits_returned = hits2.len() as u32;

    if include_ties {
        let k = policy.max_hits as u32;
        if stats.hits_returned > k {
            stats.hits_included_ties = stats.hits_returned - k;
        }
    }

    Ok((hits2, stats))
}


/// Apply a retrieval policy directly from query text.
///
/// This wrapper:
/// - derives canonical query terms from `query_text`
/// - optionally performs lexicon-backed query expansion when enabled
/// - applies the retrieval policy and returns hits + stats
///
/// Note: query expansion is controlled by `policy_cfg.enable_query_expansion`.
/// When enabled, callers must provide `lexicon_snapshot_hash_opt`.
pub fn apply_retrieval_policy_from_text_v1<S: ArtifactStore>(
    store: &S,
    snapshot_hash: &Hash32,
    sig_map_hash_opt: Option<&Hash32>,
    query_text: &str,
    qcfg: &crate::index_query::QueryTermsCfg,
    policy_cfg: &RetrievalPolicyCfgV1,
    control_opt: Option<&RetrievalControlV1>,
    lexicon_snapshot_hash_opt: Option<&Hash32>,
    expand_cfg_opt: Option<&crate::query_expansion::QueryExpansionCfgV1>,
) -> Result<(Vec<SearchHit>, RetrievalPolicyStatsV1), RetrievalPolicyApplyError> {
    // Base terms from the tokenizer.
    let mut qterms = crate::index_query::query_terms_from_text(query_text, qcfg);
    let base_count: u32 = qterms.len() as u32;

    // Optional lexicon expansion.
    let mut new_unique: u32 = 0;
    if policy_cfg.enable_query_expansion == 1 {
        let lex_hash = match lexicon_snapshot_hash_opt {
            Some(h) => h,
            None => return Err(RetrievalPolicyApplyError::ExpandMissingLexiconSnapshot),
        };

        let lex_opt = crate::lexicon_expand_lookup::load_lexicon_expand_lookup_v1(store, lex_hash)
            .map_err(|e| RetrievalPolicyApplyError::ExpandLexiconLookup(e.to_string()))?;
        let lex = match lex_opt {
            Some(v) => v,
            None => return Err(RetrievalPolicyApplyError::ExpandLexiconSnapshotNotFound(*lex_hash)),
        };

        let (qterms2, nu) = match crate::bridge_expansion::bridge_expand_query_terms_v1(
            query_text,
            qcfg,
            Some(&lex),
            control_opt,
            expand_cfg_opt,
        ) {
            Ok(x) => x,
            Err(crate::bridge_expansion::BridgeExpansionError::BadCfg(e)) => {
                return Err(RetrievalPolicyApplyError::ExpandCfg(e));
            }
            Err(e) => return Err(RetrievalPolicyApplyError::ExpandBridge(e.to_string())),
        };

        qterms = qterms2;
        new_unique = nu;
    }

    let (hits, mut stats) = apply_retrieval_policy_v1(
        store,
        snapshot_hash,
        sig_map_hash_opt,
        &qterms,
        policy_cfg,
        control_opt,
    )?;

    // Override expansion counters.
    stats.query_terms_original = base_count;
    stats.query_terms_expanded_new = new_unique;

    Ok((hits, stats))
}


#[cfg(test)]
mod tests {
    use super::*;

    use crate::frame::{DocId, FrameRowV1, SourceId, TermId, Id64};
    use crate::frame_segment::FrameSegmentV1;
    use crate::hash::blake3_hash;
    use crate::index_segment::IndexSegmentV1;
    use crate::index_snapshot::{IndexSnapshotEntryV1, IndexSnapshotV1};
    use crate::tokenizer::{term_freqs_from_text, TokenizerCfg};
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    /// In-memory deterministic artifact store used for tests.
    struct MemStore {
        m: std::cell::RefCell<BTreeMap<Hash32, Vec<u8>>>,
    }

    impl MemStore {
        fn new() -> MemStore {
            MemStore { m: std::cell::RefCell::new(BTreeMap::new()) }
        }
    }

    impl ArtifactStore for MemStore {
        fn put(&self, bytes: &[u8]) -> crate::artifact::ArtifactResult<Hash32> {
            let h = blake3_hash(bytes);
            let mut mm = self.m.borrow_mut();
            mm.entry(h).or_insert_with(|| bytes.to_vec());
            Ok(h)
        }

        fn get(&self, hash: &Hash32) -> crate::artifact::ArtifactResult<Option<Vec<u8>>> {
            Ok(self.m.borrow().get(hash).cloned())
        }

        fn path_for(&self, _hash: &Hash32) -> PathBuf {
            PathBuf::from("mem://")
        }
    }

    fn make_row(doc_u64: u64, source_u64: u64, text: &str) -> FrameRowV1 {
        let doc = DocId(Id64(doc_u64));
        let src = SourceId(Id64(source_u64));
        let mut r = FrameRowV1::new(doc, src);
        let tcfg = TokenizerCfg { max_token_bytes: 32 };
        r.terms = term_freqs_from_text(text, tcfg);
        r.recompute_doc_len();
        r
    }

    fn build_small_snapshot(store: &MemStore) -> (Hash32, Hash32) {
        let rows = vec![
            make_row(1, 7, "apple banana"),
            make_row(2, 7, "apple carrot"),
            make_row(3, 7, "banana carrot"),
        ];
        let seg = FrameSegmentV1::from_rows(&rows, 4).unwrap();
        let seg_bytes = seg.encode().unwrap();
        let seg_hash = store.put(&seg_bytes).unwrap();

        let idx = IndexSegmentV1::build_from_segment(seg_hash, &seg).unwrap();
        let idx_bytes = idx.encode().unwrap();
        let idx_hash = store.put(&idx_bytes).unwrap();

        let snap = IndexSnapshotV1 {
            version: 1,
            source_id: idx.source_id,
            entries: vec![IndexSnapshotEntryV1 {
                frame_seg: seg_hash,
                index_seg: idx_hash,
                row_count: 3,
                term_count: idx.terms.len() as u32,
                postings_bytes: idx.postings.len() as u32,
            }],
        };
        let snap_bytes = snap.encode().unwrap();
        let snap_hash = store.put(&snap_bytes).unwrap();
        (snap_hash, seg_hash)
    }

    fn build_two_segment_snapshot(store: &MemStore) -> Hash32 {
        // Segment A has repeated docs to exercise caps.
        let rows_a = vec![
            make_row(1, 7, "apple"),
            make_row(1, 7, "apple"),
            make_row(2, 7, "apple"),
        ];
        let seg_a = FrameSegmentV1::from_rows(&rows_a, 4).unwrap();
        let seg_a_hash = store.put(&seg_a.encode().unwrap()).unwrap();
        let idx_a = IndexSegmentV1::build_from_segment(seg_a_hash, &seg_a).unwrap();
        let idx_a_hash = store.put(&idx_a.encode().unwrap()).unwrap();

        // Segment B overlaps doc 2 and introduces doc 3.
        let rows_b = vec![make_row(2, 7, "apple"), make_row(3, 7, "apple")];
        let seg_b = FrameSegmentV1::from_rows(&rows_b, 4).unwrap();
        let seg_b_hash = store.put(&seg_b.encode().unwrap()).unwrap();
        let idx_b = IndexSegmentV1::build_from_segment(seg_b_hash, &seg_b).unwrap();
        let idx_b_hash = store.put(&idx_b.encode().unwrap()).unwrap();

        let snap = IndexSnapshotV1 {
            version: 1,
            source_id: idx_a.source_id,
            entries: vec![
                IndexSnapshotEntryV1 {
                    frame_seg: seg_a_hash,
                    index_seg: idx_a_hash,
                    row_count: 3,
                    term_count: idx_a.terms.len() as u32,
                    postings_bytes: idx_a.postings.len() as u32,
                },
                IndexSnapshotEntryV1 {
                    frame_seg: seg_b_hash,
                    index_seg: idx_b_hash,
                    row_count: 2,
                    term_count: idx_b.terms.len() as u32,
                    postings_bytes: idx_b.postings.len() as u32,
                },
            ],
        };
        store.put(&snap.encode().unwrap()).unwrap()
    }

    fn build_two_segment_snapshot_with_sigs(store: &MemStore) -> (Hash32, Hash32) {
        use crate::index_sig_map::IndexSigMapV1;
        use crate::segment_sig::SegmentSigV1;

        // Segment A has repeated docs to exercise caps.
        let rows_a = vec![
            make_row(1, 7, "apple"),
            make_row(1, 7, "apple"),
            make_row(2, 7, "apple"),
        ];
        let seg_a = FrameSegmentV1::from_rows(&rows_a, 4).unwrap();
        let seg_a_hash = store.put(&seg_a.encode().unwrap()).unwrap();
        let idx_a = IndexSegmentV1::build_from_segment(seg_a_hash, &seg_a).unwrap();
        let idx_a_hash = store.put(&idx_a.encode().unwrap()).unwrap();

        // Segment B overlaps doc 2 and introduces doc 3.
        let rows_b = vec![make_row(2, 7, "apple"), make_row(3, 7, "apple")];
        let seg_b = FrameSegmentV1::from_rows(&rows_b, 4).unwrap();
        let seg_b_hash = store.put(&seg_b.encode().unwrap()).unwrap();
        let idx_b = IndexSegmentV1::build_from_segment(seg_b_hash, &seg_b).unwrap();
        let idx_b_hash = store.put(&idx_b.encode().unwrap()).unwrap();

        let snap = IndexSnapshotV1 {
            version: 1,
            source_id: idx_a.source_id,
            entries: vec![
                IndexSnapshotEntryV1 {
                    frame_seg: seg_a_hash,
                    index_seg: idx_a_hash,
                    row_count: 3,
                    term_count: idx_a.terms.len() as u32,
                    postings_bytes: idx_a.postings.len() as u32,
                },
                IndexSnapshotEntryV1 {
                    frame_seg: seg_b_hash,
                    index_seg: idx_b_hash,
                    row_count: 2,
                    term_count: idx_b.terms.len() as u32,
                    postings_bytes: idx_b.postings.len() as u32,
                },
            ],
        };
        let snap_hash = store.put(&snap.encode().unwrap()).unwrap();

        // Build SegmentSigV1 artifacts and the IndexSigMapV1.
        let terms_a: Vec<TermId> = idx_a.terms.iter().map(|e| e.term).collect();
        let sig_a = SegmentSigV1::build(idx_a_hash, &terms_a, 256, 4).unwrap();
        let sig_a_hash = store.put(&sig_a.encode().unwrap()).unwrap();

        let terms_b: Vec<TermId> = idx_b.terms.iter().map(|e| e.term).collect();
        let sig_b = SegmentSigV1::build(idx_b_hash, &terms_b, 256, 4).unwrap();
        let sig_b_hash = store.put(&sig_b.encode().unwrap()).unwrap();

        let mut sm = IndexSigMapV1::new(idx_a.source_id);
        sm.push(idx_a_hash, sig_a_hash);
        sm.push(idx_b_hash, sig_b_hash);
        let sig_map_hash = store.put(&sm.encode().unwrap()).unwrap();

        (snap_hash, sig_map_hash)
    }

    fn build_novelty_doc_snapshot(store: &MemStore) -> Hash32 {
        // Doc 1 appears 3 times; docs 2 and 3 appear once. All rows have the same score for query "apple".
        let rows = vec![
            make_row(1, 7, "apple"),
            make_row(1, 7, "apple"),
            make_row(1, 7, "apple"),
            make_row(2, 7, "apple"),
            make_row(3, 7, "apple"),
        ];
        let seg = FrameSegmentV1::from_rows(&rows, 8).unwrap();
        let seg_hash = store.put(&seg.encode().unwrap()).unwrap();
        let idx = IndexSegmentV1::build_from_segment(seg_hash, &seg).unwrap();
        let idx_hash = store.put(&idx.encode().unwrap()).unwrap();

        let snap = IndexSnapshotV1 {
            version: 1,
            source_id: idx.source_id,
            entries: vec![IndexSnapshotEntryV1 {
                frame_seg: seg_hash,
                index_seg: idx_hash,
                row_count: rows.len() as u32,
                term_count: idx.terms.len() as u32,
                postings_bytes: idx.postings.len() as u32,
            }],
        };
        store.put(&snap.encode().unwrap()).unwrap()
    }

    fn build_novelty_seg_snapshot(store: &MemStore) -> Hash32 {
        // Segment A has 4 rows; Segment B has 1 row. All rows score equally for query "apple".
        let rows_a = vec![
            make_row(1, 7, "apple"),
            make_row(2, 7, "apple"),
            make_row(3, 7, "apple"),
            make_row(4, 7, "apple"),
        ];
        let seg_a = FrameSegmentV1::from_rows(&rows_a, 8).unwrap();
        let seg_a_hash = store.put(&seg_a.encode().unwrap()).unwrap();
        let idx_a = IndexSegmentV1::build_from_segment(seg_a_hash, &seg_a).unwrap();
        let idx_a_hash = store.put(&idx_a.encode().unwrap()).unwrap();

        let rows_b = vec![make_row(5, 7, "apple")];
        let seg_b = FrameSegmentV1::from_rows(&rows_b, 8).unwrap();
        let seg_b_hash = store.put(&seg_b.encode().unwrap()).unwrap();
        let idx_b = IndexSegmentV1::build_from_segment(seg_b_hash, &seg_b).unwrap();
        let idx_b_hash = store.put(&idx_b.encode().unwrap()).unwrap();

        let snap = IndexSnapshotV1 {
            version: 1,
            source_id: idx_a.source_id,
            entries: vec![
                IndexSnapshotEntryV1 {
                    frame_seg: seg_a_hash,
                    index_seg: idx_a_hash,
                    row_count: rows_a.len() as u32,
                    term_count: idx_a.terms.len() as u32,
                    postings_bytes: idx_a.postings.len() as u32,
                },
                IndexSnapshotEntryV1 {
                    frame_seg: seg_b_hash,
                    index_seg: idx_b_hash,
                    row_count: rows_b.len() as u32,
                    term_count: idx_b.terms.len() as u32,
                    postings_bytes: idx_b.postings.len() as u32,
                },
            ],
        };
        store.put(&snap.encode().unwrap()).unwrap()
    }

    #[test]
    fn default_cfg_is_valid() {
        let cfg = RetrievalPolicyCfgV1::new();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn apply_policy_respects_include_ties_flag_without_control() {
        let store = MemStore::new();
        let (snap_hash, _seg_hash) = build_small_snapshot(&store);

        let qcfg = crate::index_query::QueryTermsCfg::new();
        let q = crate::index_query::query_terms_from_text("apple", &qcfg);

        let mut p0 = RetrievalPolicyCfgV1::new();
        p0.max_hits = 1;
        p0.include_ties_at_cutoff = 0;
        p0.validate().unwrap();

        let (h0, s0) = apply_retrieval_policy_v1(&store, &snap_hash, None, &q, &p0, None).unwrap();
        assert_eq!(h0.len(), 1);
        assert_eq!(s0.hits_returned, 1);
        assert_eq!(s0.hits_included_ties, 0);

        let mut p1 = RetrievalPolicyCfgV1::new();
        p1.max_hits = 1;
        p1.include_ties_at_cutoff = 1;
        p1.validate().unwrap();

        let (h1, s1) = apply_retrieval_policy_v1(&store, &snap_hash, None, &q, &p1, None).unwrap();
        // The top two rows both contain "apple" with the same score.
        assert_eq!(h1.len(), 2);
        assert_eq!(s1.hits_returned, 2);
        assert_eq!(s1.hits_included_ties, 1);

        // Default tie-break is (score desc, frame_seg asc, row_ix asc).
        assert!(h1[0].row_ix <= h1[1].row_ix);
    }

    #[test]
    fn apply_policy_truncates_query_terms_and_records_stats() {
        let store = MemStore::new();
        let (snap_hash, _seg_hash) = build_small_snapshot(&store);

        let mut q: Vec<QueryTerm> = Vec::new();
        for i in 0..5u64 {
            q.push(QueryTerm { term: TermId(Id64(100 + i)), qtf: 1 });
        }

        let mut p = RetrievalPolicyCfgV1::new();
        p.max_query_terms = 2;
        p.max_hits = 3;
        p.include_ties_at_cutoff = 0;
        p.validate().unwrap();

        let (_hits, stats) = apply_retrieval_policy_v1(&store, &snap_hash, None, &q, &p, None).unwrap();
        assert_eq!(stats.query_terms_total, 5);
        assert_eq!(stats.query_terms_used, 2);
    }

    #[test]
    fn diversity_caps_enforce_per_segment_and_per_doc() {
        let store = MemStore::new();
        let snap_hash = build_two_segment_snapshot(&store);

        let qcfg = crate::index_query::QueryTermsCfg::new();
        let q = crate::index_query::query_terms_from_text("apple", &qcfg);

        let mut p = RetrievalPolicyCfgV1::new();
        p.max_hits = 4;
        p.include_ties_at_cutoff = 0;
        p.max_hits_per_frame_seg = 2;
        p.max_hits_per_doc = 1;
        p.validate().unwrap();

        let (hits, _stats) = apply_retrieval_policy_v1(&store, &snap_hash, None, &q, &p, None).unwrap();

        // With max_hits_per_doc=1 and docs {1,2,3}, we can return at most 3 hits.
        assert!(hits.len() <= 3);

        let mut seg_counts: BTreeMap<Hash32, u32> = BTreeMap::new();
        let mut doc_counts: BTreeMap<u64, u32> = BTreeMap::new();
        for h in hits.iter() {
            *seg_counts.entry(h.frame_seg).or_insert(0) += 1;

            let seg_bytes = store.get(&h.frame_seg).unwrap().unwrap();
            let seg = FrameSegmentV1::decode(&seg_bytes).unwrap();
            let doc = frame_segment_doc_id(&seg, h.row_ix).unwrap();
            *doc_counts.entry((doc.0).0).or_insert(0) += 1;
        }

        for (_k, v) in seg_counts.iter() {
            assert!(*v <= 2);
        }
        for (_k, v) in doc_counts.iter() {
            assert!(*v <= 1);
        }
    }

    #[test]
    fn diversity_caps_preserve_include_ties_semantics() {
        let store = MemStore::new();
        let snap_hash = build_two_segment_snapshot(&store);

        let qcfg = crate::index_query::QueryTermsCfg::new();
        let q = crate::index_query::query_terms_from_text("apple", &qcfg);

        let mut p = RetrievalPolicyCfgV1::new();
        p.max_hits = 1;
        p.include_ties_at_cutoff = 1;
        p.max_hits_per_doc = 1;
        p.validate().unwrap();

        let (hits, stats) = apply_retrieval_policy_v1(&store, &snap_hash, None, &q, &p, None).unwrap();

        // All hits have the same score; include_ties should allow returning more than max_hits.
        assert!(hits.len() >= 1);
        assert_eq!(stats.hits_included_ties, (hits.len() as u32).saturating_sub(1));
    }

    #[test]
    fn novelty_rerank_prefers_rare_docs_on_ties() {
        let store = MemStore::new();
        let snap_hash = build_novelty_doc_snapshot(&store);

        let qcfg = crate::index_query::QueryTermsCfg::new();
        let q = crate::index_query::query_terms_from_text("apple", &qcfg);

        let mut p = RetrievalPolicyCfgV1::new();
        p.max_hits = 2;
        p.include_ties_at_cutoff = 0;
        p.novelty_mode = 1; // doc
        p.validate().unwrap();

        let (hits, _stats) = apply_retrieval_policy_v1(&store, &snap_hash, None, &q, &p, None).unwrap();
        assert_eq!(hits.len(), 2);

        let mut docs: Vec<u64> = Vec::new();
        for h in hits.iter() {
            let seg_bytes = store.get(&h.frame_seg).unwrap().unwrap();
            let seg = FrameSegmentV1::decode(&seg_bytes).unwrap();
            let doc = frame_segment_doc_id(&seg, h.row_ix).unwrap();
            docs.push((doc.0).0);
        }
        // Docs 2 and 3 have higher novelty (freq=1) than doc 1 (freq=3).
        docs.sort();
        assert_eq!(docs, vec![2, 3]);
    }

    #[test]
    fn novelty_rerank_prefers_rare_segments_on_ties() {
        let store = MemStore::new();
        let snap_hash = build_novelty_seg_snapshot(&store);

        let qcfg = crate::index_query::QueryTermsCfg::new();
        let q = crate::index_query::query_terms_from_text("apple", &qcfg);

        let mut p = RetrievalPolicyCfgV1::new();
        p.max_hits = 1;
        p.include_ties_at_cutoff = 0;
        p.novelty_mode = 2; // frame segment
        p.validate().unwrap();

        let (hits, _stats) = apply_retrieval_policy_v1(&store, &snap_hash, None, &q, &p, None).unwrap();
        assert_eq!(hits.len(), 1);

        // Segment frequencies are derived from candidate hits. Identify the rare
        // segment using snapshot entry row_count (Segment B has 1 row; Segment A has 4).
        let chosen_seg = hits[0].frame_seg;
        let snap_bytes = store.get(&snap_hash).unwrap().unwrap();
        let snap = IndexSnapshotV1::decode(&snap_bytes).unwrap();
        assert_eq!(snap.entries.len(), 2);

        let mut rare_seg = snap.entries[0].frame_seg;
        let mut rare_rows = snap.entries[0].row_count;
        for e in snap.entries.iter().skip(1) {
            if e.row_count < rare_rows {
                rare_rows = e.row_count;
                rare_seg = e.frame_seg;
            }
        }

        assert_eq!(rare_rows, 1);
        assert_eq!(chosen_seg, rare_seg);
    }

    #[test]
    fn gated_and_ungated_search_produce_identical_hits_with_refine_enabled() {
        use crate::hash::blake3_hash;
        use crate::retrieval_control::RetrievalControlV1;

        let store = MemStore::new();
        let (snap_hash, sig_map_hash) = build_two_segment_snapshot_with_sigs(&store);

        let qcfg = crate::index_query::QueryTermsCfg::new();
        let q = crate::index_query::query_terms_from_text("apple", &qcfg);

        let mut p = RetrievalPolicyCfgV1::new();
        p.max_hits = 8;
        p.include_ties_at_cutoff = 0;
        p.max_hits_per_frame_seg = 2;
        p.max_hits_per_doc = 1;
        p.novelty_mode = 3; // doc+seg
        p.validate().unwrap();

        let ctrl = RetrievalControlV1::new(blake3_hash(b"prompt"));

        let (h_ungated, s_ungated) =
            apply_retrieval_policy_v1(&store, &snap_hash, None, &q, &p, Some(&ctrl)).unwrap();
        let (h_gated, s_gated) = apply_retrieval_policy_v1(
            &store,
            &snap_hash,
            Some(&sig_map_hash),
            &q,
            &p,
            Some(&ctrl),
        )
        .unwrap();

        assert_eq!(h_ungated, h_gated);
        assert_eq!(s_ungated.hits_returned, s_gated.hits_returned);
        assert_eq!(s_ungated.hits_included_ties, s_gated.hits_included_ties);
    }

    fn build_lexicon_snapshot_with_lemma(store: &MemStore, lemma: &str) -> Hash32 {
        use crate::lexicon::{
            derive_lemma_id, derive_lemma_key_id, derive_text_id, LemmaRowV1, LEXICON_SCHEMA_V1,
        };
        use crate::lexicon_segment::LexiconSegmentV1;
        use crate::lexicon_segment_store::put_lexicon_segment_v1;
        use crate::lexicon_snapshot::{LexiconSnapshotEntryV1, LexiconSnapshotV1};
        use crate::lexicon_snapshot_store::put_lexicon_snapshot_v1;

        let lemma_id = derive_lemma_id(lemma);
        let lemma_key_id = derive_lemma_key_id(lemma);
        let lemma_text_id = derive_text_id(lemma);

        let lemmas = vec![LemmaRowV1 {
            version: LEXICON_SCHEMA_V1,
            lemma_id,
            lemma_key_id,
            lemma_text_id,
            pos_mask: 0,
            flags: 0,
        }];

        let seg = LexiconSegmentV1::build_from_rows(&lemmas, &[], &[], &[]).expect("seg");
        let seg_hash = put_lexicon_segment_v1(store, &seg).expect("put seg");

        let mut snap = LexiconSnapshotV1::new();
        snap.entries.push(LexiconSnapshotEntryV1 {
            lex_seg: seg_hash,
            lemma_count: 1,
            sense_count: 0,
            rel_count: 0,
            pron_count: 0,
        });

        put_lexicon_snapshot_v1(store, &snap).expect("put snap")
    }

    #[test]
    fn apply_from_text_with_expansion_recovers_plural_variant() {
        let store = MemStore::new();
        let (snapshot_hash, _seg_hash) = build_small_snapshot(&store);
        let lex_snap = build_lexicon_snapshot_with_lemma(&store, "banana");

        let qcfg = crate::index_query::QueryTermsCfg::new();

        let mut pcfg = RetrievalPolicyCfgV1::new();
        pcfg.max_hits = 10;
        pcfg.max_query_terms = 32;
        pcfg.include_ties_at_cutoff = 0;

        // Without expansion, "bananas" should not match rows that contain only "banana".
        pcfg.enable_query_expansion = 0;
        let (hits0, stats0) = apply_retrieval_policy_from_text_v1(
            &store,
            &snapshot_hash,
            None,
            "bananas",
            &qcfg,
            &pcfg,
            None,
            None,
            None,
        )
        .expect("apply");
        assert_eq!(hits0.len(), 0);
        assert_eq!(stats0.query_terms_expanded_new, 0);

        // With expansion enabled, we should add "banana" and get matches.
        pcfg.enable_query_expansion = 1;
        let (hits1, stats1) = apply_retrieval_policy_from_text_v1(
            &store,
            &snapshot_hash,
            None,
            "bananas",
            &qcfg,
            &pcfg,
            None,
            Some(&lex_snap),
            None,
        )
        .expect("apply");
        assert!(hits1.len() >= 1);
        assert!(stats1.query_terms_expanded_new >= 1);
    }

    #[test]
    fn apply_from_text_with_expansion_requires_lexicon_snapshot_hash() {
        let store = MemStore::new();
        let (snapshot_hash, _seg_hash) = build_small_snapshot(&store);
        let qcfg = crate::index_query::QueryTermsCfg::new();

        let mut pcfg = RetrievalPolicyCfgV1::new();
        pcfg.enable_query_expansion = 1;

        let err = apply_retrieval_policy_from_text_v1(
            &store,
            &snapshot_hash,
            None,
            "bananas",
            &qcfg,
            &pcfg,
            None,
            None,
            None,
        )
        .err()
        .expect("err");

        assert_eq!(err, RetrievalPolicyApplyError::ExpandMissingLexiconSnapshot);
    }

}
