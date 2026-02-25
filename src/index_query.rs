// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Query-time index lookup and scoring.
//!
//! This module provides a deterministic, CPU-friendly way to run a query against
//! an [`IndexSnapshotV1`](crate::index_snapshot::IndexSnapshotV1) and its referenced
//! [`IndexSegmentV1`](crate::index_segment::IndexSegmentV1) postings.
//!
//! Design goals (v1):
//! - Bitwise determinism (no randomized hash maps, no floating point math).
//! - Minimal allocations and bounded work per query.
//! - Integer-only scoring (simple TF * IDF ratio).
//!
//! Notes:
//! - v1 does not attempt to generate natural language responses. It only returns
//! ranked row addresses (frame segment hash + row index).
//! - Metaphonetic expansion is optional. It becomes effective once ingestion
//! includes metaphone ids into each row's `terms` list.

use crate::artifact::{ArtifactResult, ArtifactStore};
use crate::codec::DecodeError;
use crate::frame::TermId;
use crate::hash::Hash32;
use crate::index_pack::IndexPackV1;
use crate::index_segment::{IndexSegmentV1, PostingV1};
use crate::index_sig_map::IndexSigMapV1;
use crate::index_snapshot::IndexSnapshotV1;
use crate::metaphone::{meta_freqs_from_text, MetaphoneCfg};
use crate::retrieval_control::RetrievalControlV1;
use crate::retrieval_gating::{should_decode_index_artifact_any, GateStatsV1};
use crate::segment_sig::SegmentSigV1;
use crate::tokenizer::{term_freqs_from_text, TokenizerCfg};

/// Default scaling shift for IDF-like ratios.
///
/// Larger values increase score resolution but risk overflow. v1 keeps it small.
pub const IDF_SHIFT: u32 = 8;

/// Errors that can occur during query-time index lookup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IndexQueryError {
    /// Failed to load a referenced artifact.
    Store(String),
    /// Failed to decode an index snapshot or index segment.
    Decode(String),
    /// Snapshot did not contain any entries.
    EmptySnapshot,
    /// Caller requested an invalid `k` value.
    InvalidK,
}

impl IndexQueryError {
    fn store(msg: &str) -> IndexQueryError {
        IndexQueryError::Store(msg.to_string())
    }
    fn decode(msg: &str) -> IndexQueryError {
        IndexQueryError::Decode(msg.to_string())
    }
}

fn map_store_err<T>(r: ArtifactResult<T>) -> Result<T, IndexQueryError> {
    r.map_err(|e| IndexQueryError::store(&e.to_string()))
}

fn map_decode_err(e: DecodeError) -> IndexQueryError {
    IndexQueryError::decode(&e.to_string())
}

/// Configuration for building query term ids from text.
pub struct QueryTermsCfg {
    /// Tokenizer configuration for term ids.
    pub tok_cfg: TokenizerCfg,
    /// Whether to include metaphone ids as additional query terms.
    pub include_metaphone: bool,
    /// Metaphone configuration (used only if `include_metaphone` is true).
    pub meta_cfg: MetaphoneCfg,
    /// Maximum number of unique query terms to consider (post-dedup).
    pub max_terms: usize,
}

impl QueryTermsCfg {
    /// Construct a conservative default config.
    pub fn new() -> QueryTermsCfg {
        QueryTermsCfg {
            tok_cfg: TokenizerCfg {
                max_token_bytes: 32,
            },
            include_metaphone: false,
            meta_cfg: MetaphoneCfg {
                max_token_bytes: 32,
                max_code_len: 8,
            },
            max_terms: 128,
        }
    }
}

/// A single query term (term id + query term frequency).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryTerm {
    /// Term identifier (domain-separated by hashing).
    pub term: TermId,
    /// Query term frequency (occurrences in query).
    pub qtf: u32,
}

/// Extract query terms from text and canonicalize them.
///
/// Output is:
/// - sorted ascending by term id
/// - duplicate ids combined via saturating addition of `qtf`
/// - truncated to `cfg.max_terms`
///
/// This function uses only deterministic operations.
pub fn query_terms_from_text(text: &str, cfg: &QueryTermsCfg) -> Vec<QueryTerm> {
    // Token terms.
    let tok_cfg = TokenizerCfg {
        max_token_bytes: cfg.tok_cfg.max_token_bytes,
    };
    let mut out: Vec<QueryTerm> = Vec::new();
    for tf in term_freqs_from_text(text, tok_cfg) {
        out.push(QueryTerm {
            term: tf.term,
            qtf: tf.tf,
        });
    }

    // Optional metaphone terms (domain-separated ids).
    if cfg.include_metaphone {
        let tok_cfg2 = TokenizerCfg {
            max_token_bytes: cfg.tok_cfg.max_token_bytes,
        };
        let meta_cfg = MetaphoneCfg {
            max_token_bytes: cfg.meta_cfg.max_token_bytes,
            max_code_len: cfg.meta_cfg.max_code_len,
        };
        for mf in meta_freqs_from_text(text, tok_cfg2, meta_cfg) {
            out.push(QueryTerm {
                term: TermId(mf.meta.0),
                qtf: mf.tf,
            });
        }
    }

    // Canonicalize: sort by term id and merge duplicates.
    out.sort_by(|a, b| (a.term.0).0.cmp(&(b.term.0).0));
    let mut merged: Vec<QueryTerm> = Vec::new();
    for qt in out {
        if let Some(last) = merged.last_mut() {
            if (last.term.0).0 == (qt.term.0).0 {
                last.qtf = last.qtf.saturating_add(qt.qtf);
                continue;
            }
        }
        merged.push(qt);
        if merged.len() >= cfg.max_terms {
            break;
        }
    }
    merged
}

/// A ranked hit produced by index lookup.
///
/// `row_ix` is the absolute row index within the referenced frame segment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SearchHit {
    /// Frame segment hash that owns the row.
    pub frame_seg: Hash32,
    /// Row index within that frame segment.
    pub row_ix: u32,
    /// Deterministic integer score (larger is better).
    pub score: u64,
}

/// Configuration for searching an index snapshot.
#[derive(Clone, Debug)]
pub struct SearchCfg {
    /// Return at most this many hits.
    pub k: usize,
    /// Limit the number of snapshot entries to scan (0 means no limit).
    pub entry_cap: usize,
    /// If row_count is <= this threshold, use dense scoring arrays.
    ///
    /// Dense scoring has low constant factors and is deterministic, but allocates
    /// `row_count * 8` bytes for the scores array.
    pub dense_row_threshold: u32,
}

impl SearchCfg {
    /// Default search config for interactive usage.
    pub fn new() -> SearchCfg {
        SearchCfg {
            k: 10,
            entry_cap: 0,
            dense_row_threshold: 200_000,
        }
    }
}

fn seed64_from_control(control: &RetrievalControlV1) -> u64 {
    let id = control.control_id();
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
        frame_seg[0],
        frame_seg[1],
        frame_seg[2],
        frame_seg[3],
        frame_seg[4],
        frame_seg[5],
        frame_seg[6],
        frame_seg[7],
    ]);
    let x = seed ^ seg0 ^ (row_ix as u64).wrapping_mul(0x9E3779B97F4A7C15);
    mix64(x.wrapping_add(0x9E3779B97F4A7C15))
}

fn rank_and_truncate_hits(
    hits: &mut Vec<SearchHit>,
    k: usize,
    tie_seed: Option<u64>,
    include_ties: bool,
) {
    if let Some(seed) = tie_seed {
        hits.sort_by(|a, b| match b.score.cmp(&a.score) {
            core::cmp::Ordering::Equal => {
                let ka = tiebreak_key(seed, &a.frame_seg, a.row_ix);
                let kb = tiebreak_key(seed, &b.frame_seg, b.row_ix);
                match ka.cmp(&kb) {
                    core::cmp::Ordering::Equal => match a.frame_seg.cmp(&b.frame_seg) {
                        core::cmp::Ordering::Equal => a.row_ix.cmp(&b.row_ix),
                        x => x,
                    },
                    x => x,
                }
            }
            x => x,
        });
    } else {
        // Default canonical tie-break: (score desc, frame_seg asc, row_ix asc)
        hits.sort_by(|a, b| match b.score.cmp(&a.score) {
            core::cmp::Ordering::Equal => match a.frame_seg.cmp(&b.frame_seg) {
                core::cmp::Ordering::Equal => a.row_ix.cmp(&b.row_ix),
                x => x,
            },
            x => x,
        });
    }

    if k == 0 {
        hits.clear();
        return;
    }

    if hits.len() > k {
        if include_ties {
            let cutoff = hits[k - 1].score;
            let mut end = k;
            while end < hits.len() {
                if hits[end].score != cutoff {
                    break;
                }
                end += 1;
            }
            hits.truncate(end);
        } else {
            hits.truncate(k);
        }
    }
}
const INDEX_PACK_MAGIC: &[u8; 8] = b"FSALMIPK";

fn is_index_pack(bytes: &[u8]) -> bool {
    bytes.len() >= 8 && &bytes[..8] == INDEX_PACK_MAGIC
}

fn find_pack<'a>(
    cache: &'a Vec<(Hash32, IndexPackV1)>,
    pack_hash: &Hash32,
) -> Option<&'a IndexPackV1> {
    for (h, p) in cache.iter() {
        if h == pack_hash {
            return Some(p);
        }
    }
    None
}

fn insert_pack<'a>(
    cache: &'a mut Vec<(Hash32, IndexPackV1)>,
    pack_hash: Hash32,
    pack: IndexPackV1,
) -> &'a IndexPackV1 {
    cache.push((pack_hash, pack));
    &cache.last().expect("cache just pushed").1
}

fn decode_and_cache_pack<'a>(
    cache: &'a mut Vec<(Hash32, IndexPackV1)>,
    pack_hash: Hash32,
    pack_bytes: &[u8],
) -> Result<&'a IndexPackV1, IndexQueryError> {
    let pack = IndexPackV1::decode(pack_bytes).map_err(map_decode_err)?;
    Ok(insert_pack(cache, pack_hash, pack))
}

/// Search an index snapshot by hash and return ranked hits.
///
/// This function:
/// - loads and decodes the snapshot
/// - scans referenced index segments
/// - accumulates integer-only scores per row
/// - sorts the final hits by (score desc, frame_seg asc, row_ix asc)
pub fn search_snapshot<S: ArtifactStore>(
    store: &S,
    snapshot_hash: &Hash32,
    query_terms: &[QueryTerm],
    cfg: &SearchCfg,
) -> Result<Vec<SearchHit>, IndexQueryError> {
    search_snapshot_inner(store, snapshot_hash, query_terms, cfg, None, false)
}

pub(crate) fn search_snapshot_inner<S: ArtifactStore>(
    store: &S,
    snapshot_hash: &Hash32,
    query_terms: &[QueryTerm],
    cfg: &SearchCfg,
    tie_seed: Option<u64>,
    include_ties: bool,
) -> Result<Vec<SearchHit>, IndexQueryError> {
    if cfg.k == 0 {
        return Err(IndexQueryError::InvalidK);
    }
    let snap_bytes = map_store_err(store.get(snapshot_hash))?
        .ok_or_else(|| IndexQueryError::store("snapshot not found"))?;
    let snap = IndexSnapshotV1::decode(&snap_bytes).map_err(map_decode_err)?;
    if snap.entries.is_empty() {
        return Err(IndexQueryError::EmptySnapshot);
    }

    // Accumulate all hits then sort; v1 keeps this simple.
    let mut hits: Vec<SearchHit> = Vec::new();

    // IndexPackV1 decode cache (per-call).
    let mut pack_cache: Vec<(Hash32, IndexPackV1)> = Vec::new();

    let mut entries_scanned: usize = 0;
    for e in &snap.entries {
        entries_scanned += 1;
        if cfg.entry_cap != 0 && entries_scanned > cfg.entry_cap {
            break;
        }

        let idx: IndexSegmentV1 = if let Some(pack) = find_pack(&pack_cache, &e.index_seg) {
            let inner = pack
                .get_index_bytes(&e.frame_seg)
                .ok_or_else(|| IndexQueryError::decode("index pack missing frame segment"))?;
            IndexSegmentV1::decode(inner).map_err(map_decode_err)?
        } else {
            let idx_bytes = map_store_err(store.get(&e.index_seg))?
                .ok_or_else(|| IndexQueryError::store("index artifact not found"))?;
            if is_index_pack(&idx_bytes) {
                let pack = decode_and_cache_pack(&mut pack_cache, e.index_seg, &idx_bytes)?;
                let inner = pack
                    .get_index_bytes(&e.frame_seg)
                    .ok_or_else(|| IndexQueryError::decode("index pack missing frame segment"))?;
                IndexSegmentV1::decode(inner).map_err(map_decode_err)?
            } else {
                IndexSegmentV1::decode(&idx_bytes).map_err(map_decode_err)?
            }
        };

        // v1 uses per-segment N for IDF-like ratios. It is stable and cheap.
        let n = idx.row_count;
        if n == 0 {
            continue;
        }

        if n <= cfg.dense_row_threshold {
            score_segment_dense(&idx, &e.frame_seg, query_terms, &mut hits)?;
        } else {
            score_segment_sparse(&idx, &e.frame_seg, query_terms, &mut hits)?;
        }
    }
    // Rank deterministically.
    rank_and_truncate_hits(&mut hits, cfg.k, tie_seed, include_ties);

    Ok(hits)
}

/// Search an index snapshot with an optional control record.
///
/// integrates pragmatics as a control-signal track. v1 of this
/// integration does not change retrieval behavior; the control record is
/// accepted so higher layers can thread it through deterministically.
pub fn search_snapshot_with_control<S: ArtifactStore>(
    store: &S,
    snapshot_hash: &Hash32,
    query_terms: &[QueryTerm],
    cfg: &SearchCfg,
    control: Option<&RetrievalControlV1>,
) -> Result<Vec<SearchHit>, IndexQueryError> {
    let (seed, include_ties) = match control {
        Some(c) => (Some(seed64_from_control(c)), true),
        None => (None, false),
    };
    search_snapshot_inner(store, snapshot_hash, query_terms, cfg, seed, include_ties)
}

/// Cached snapshot search with an optional control record.
///
/// This is identical to [`search_snapshot_cached`] and ignores `control` in v1.
pub fn search_snapshot_cached_with_control<S: ArtifactStore>(
    store: &S,
    snapshot_hash: &Hash32,
    query_terms: &[QueryTerm],
    cfg: &SearchCfg,
    snap_cache: Option<
        &mut crate::cache::Cache2Q<Hash32, std::sync::Arc<crate::index_snapshot::IndexSnapshotV1>>,
    >,
    idx_cache: Option<
        &mut crate::cache::Cache2Q<Hash32, std::sync::Arc<crate::index_segment::IndexSegmentV1>>,
    >,
    control: Option<&RetrievalControlV1>,
) -> Result<Vec<SearchHit>, IndexQueryError> {
    let (seed, include_ties) = match control {
        Some(c) => (Some(seed64_from_control(c)), true),
        None => (None, false),
    };
    search_snapshot_cached_inner(
        store,
        snapshot_hash,
        query_terms,
        cfg,
        snap_cache,
        idx_cache,
        seed,
        include_ties,
    )
}

/// Search an IndexSnapshotV1 using optional warm caches.
///
/// This is identical to [`search_snapshot`] but can reuse decoded
/// IndexSnapshotV1 and IndexSegmentV1 values across calls when caches are
/// supplied.
///
/// Determinism contract:
/// - Cache hits may reduce work but MUST NOT change ranking or output ordering.
/// - Missing artifacts still raise the same errors as the non-cached path.
pub fn search_snapshot_cached<S: ArtifactStore>(
    store: &S,
    snapshot_hash: &Hash32,
    query_terms: &[QueryTerm],
    cfg: &SearchCfg,
    snap_cache: Option<
        &mut crate::cache::Cache2Q<Hash32, std::sync::Arc<crate::index_snapshot::IndexSnapshotV1>>,
    >,
    idx_cache: Option<
        &mut crate::cache::Cache2Q<Hash32, std::sync::Arc<crate::index_segment::IndexSegmentV1>>,
    >,
) -> Result<Vec<SearchHit>, IndexQueryError> {
    search_snapshot_cached_inner(
        store,
        snapshot_hash,
        query_terms,
        cfg,
        snap_cache,
        idx_cache,
        None,
        false,
    )
}

fn search_snapshot_cached_inner<S: ArtifactStore>(
    store: &S,
    snapshot_hash: &Hash32,
    query_terms: &[QueryTerm],
    cfg: &SearchCfg,
    snap_cache: Option<
        &mut crate::cache::Cache2Q<Hash32, std::sync::Arc<crate::index_snapshot::IndexSnapshotV1>>,
    >,
    mut idx_cache: Option<
        &mut crate::cache::Cache2Q<Hash32, std::sync::Arc<crate::index_segment::IndexSegmentV1>>,
    >,
    tie_seed: Option<u64>,
    include_ties: bool,
) -> Result<Vec<SearchHit>, IndexQueryError> {
    use crate::index_snapshot_store::{get_index_snapshot_v1_cached, IndexSnapshotStoreError};
    use std::sync::Arc;

    if cfg.k == 0 {
        return Err(IndexQueryError::InvalidK);
    }

    let snap_arc: Arc<crate::index_snapshot::IndexSnapshotV1> = match snap_cache {
        Some(cache) => {
            let sopt =
                get_index_snapshot_v1_cached(store, cache, snapshot_hash).map_err(|e| match e {
                    IndexSnapshotStoreError::Decode(d) => IndexQueryError::Decode(d.to_string()),
                    IndexSnapshotStoreError::Store(s) => IndexQueryError::Store(s.to_string()),
                    IndexSnapshotStoreError::Encode(en) => IndexQueryError::Decode(en.to_string()),
                })?;
            sopt.ok_or_else(|| IndexQueryError::store("snapshot not found"))?
        }
        None => {
            let bytes = map_store_err(store.get(snapshot_hash))?
                .ok_or_else(|| IndexQueryError::store("snapshot not found"))?;
            let snap =
                crate::index_snapshot::IndexSnapshotV1::decode(&bytes).map_err(map_decode_err)?;
            Arc::new(snap)
        }
    };

    let snapshot: &crate::index_snapshot::IndexSnapshotV1 = snap_arc.as_ref();
    if snapshot.entries.is_empty() {
        return Err(IndexQueryError::EmptySnapshot);
    }

    let mut hits: Vec<SearchHit> = Vec::new();

    // IndexPackV1 decode cache (per-call).
    let mut pack_cache: Vec<(Hash32, IndexPackV1)> = Vec::new();

    let mut entries_scanned: usize = 0;
    for e in snapshot.entries.iter() {
        entries_scanned += 1;
        if cfg.entry_cap != 0 && entries_scanned > cfg.entry_cap {
            break;
        }

        let idx_arc: Arc<crate::index_segment::IndexSegmentV1> = if let Some(pack) =
            find_pack(&pack_cache, &e.index_seg)
        {
            // Pack already cached; no store read.
            let inner = pack
                .get_index_bytes(&e.frame_seg)
                .ok_or_else(|| IndexQueryError::decode("index pack missing frame segment"))?;
            let idx =
                crate::index_segment::IndexSegmentV1::decode(inner).map_err(map_decode_err)?;
            Arc::new(idx)
        } else if let Some(cache) = idx_cache.as_deref_mut() {
            // First consult the cache. Only IndexSegmentV1 blobs are cached by hash.
            if let Some(v) = cache.get(&e.index_seg) {
                v.clone()
            } else {
                let idx_bytes = map_store_err(store.get(&e.index_seg))?
                    .ok_or_else(|| IndexQueryError::store("index artifact not found"))?;

                if is_index_pack(&idx_bytes) {
                    let dec = IndexPackV1::decode(&idx_bytes).map_err(map_decode_err)?;
                    let pack = insert_pack(&mut pack_cache, e.index_seg.clone(), dec);
                    let inner = pack.get_index_bytes(&e.frame_seg).ok_or_else(|| {
                        IndexQueryError::decode("index pack missing frame segment")
                    })?;
                    let idx = crate::index_segment::IndexSegmentV1::decode(inner)
                        .map_err(map_decode_err)?;
                    Arc::new(idx)
                } else {
                    let idx = crate::index_segment::IndexSegmentV1::decode(&idx_bytes)
                        .map_err(map_decode_err)?;
                    let arc = Arc::new(idx);
                    let _ =
                        cache.insert_cost(e.index_seg.clone(), arc.clone(), idx_bytes.len() as u64);
                    arc
                }
            }
        } else {
            let idx_bytes = map_store_err(store.get(&e.index_seg))?
                .ok_or_else(|| IndexQueryError::store("index artifact not found"))?;
            if is_index_pack(&idx_bytes) {
                let dec = IndexPackV1::decode(&idx_bytes).map_err(map_decode_err)?;
                let pack = insert_pack(&mut pack_cache, e.index_seg.clone(), dec);
                let inner = pack
                    .get_index_bytes(&e.frame_seg)
                    .ok_or_else(|| IndexQueryError::decode("index pack missing frame segment"))?;
                let idx =
                    crate::index_segment::IndexSegmentV1::decode(inner).map_err(map_decode_err)?;
                Arc::new(idx)
            } else {
                let idx = crate::index_segment::IndexSegmentV1::decode(&idx_bytes)
                    .map_err(map_decode_err)?;
                Arc::new(idx)
            }
        };

        let idx: &crate::index_segment::IndexSegmentV1 = idx_arc.as_ref();

        // v1 uses per-segment N for IDF-like ratios. It is stable and cheap.
        let n = idx.row_count;
        if n == 0 {
            continue;
        }

        if n <= cfg.dense_row_threshold {
            score_segment_dense(idx, &e.frame_seg, query_terms, &mut hits)?;
        } else {
            score_segment_sparse(idx, &e.frame_seg, query_terms, &mut hits)?;
        }
    }

    rank_and_truncate_hits(&mut hits, cfg.k, tie_seed, include_ties);

    Ok(hits)
}

fn idf_ratio_scaled(n: u32, df: u32) -> u32 {
    // idf = ((n + 1) << IDF_SHIFT) / (df + 1)
    // df==0 should not happen for real terms, but we keep it safe.
    let nn = (n as u64).saturating_add(1);
    let dd = (df as u64).saturating_add(1);
    let num = match nn.checked_shl(IDF_SHIFT) {
        Some(v) => v,
        None => u64::MAX,
    };
    let out = num / dd;
    if out > (u32::MAX as u64) {
        u32::MAX
    } else {
        out as u32
    }
}

fn score_posting(tf: u32, qtf: u32, idf: u32) -> u64 {
    // score = qtf * tf * idf (scaled by IDF_SHIFT)
    let a = (qtf as u64).saturating_mul(tf as u64);
    a.saturating_mul(idf as u64)
}

fn find_term_ix(idx: &IndexSegmentV1, term: TermId) -> Option<usize> {
    // Binary search terms dict by term id.
    let key = (term.0).0;
    let mut lo: usize = 0;
    let mut hi: usize = idx.terms.len();
    while lo < hi {
        let mid = lo + ((hi - lo) >> 1);
        let v = (idx.terms[mid].term.0).0;
        if v < key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if lo < idx.terms.len() && (idx.terms[lo].term.0).0 == key {
        Some(lo)
    } else {
        None
    }
}

fn score_segment_dense(
    idx: &IndexSegmentV1,
    frame_seg: &Hash32,
    query_terms: &[QueryTerm],
    hits: &mut Vec<SearchHit>,
) -> Result<(), IndexQueryError> {
    let n = idx.row_count as usize;
    let mut scores: Vec<u64> = vec![0u64; n];
    let mut touched: Vec<u32> = Vec::new();

    for qt in query_terms {
        let ix = match find_term_ix(idx, qt.term) {
            Some(i) => i,
            None => continue,
        };
        let e = &idx.terms[ix];
        let idf = idf_ratio_scaled(idx.row_count, e.df);
        let mut it = idx.postings_iter(ix).map_err(map_decode_err)?;

        while let Some(p) = it.next() {
            let PostingV1 { row_ix, tf } = p.map_err(map_decode_err)?;
            let r = row_ix as usize;
            if r >= scores.len() {
                // Defensive: postings must not refer outside row_count.
                return Err(IndexQueryError::decode("posting row out of bounds"));
            }
            if scores[r] == 0 {
                touched.push(row_ix);
            }
            scores[r] = scores[r].saturating_add(score_posting(tf, qt.qtf, idf));
        }
    }

    for &row_ix in &touched {
        let s = scores[row_ix as usize];
        if s != 0 {
            hits.push(SearchHit {
                frame_seg: *frame_seg,
                row_ix,
                score: s,
            });
        }
    }
    Ok(())
}

fn score_segment_sparse(
    idx: &IndexSegmentV1,
    frame_seg: &Hash32,
    query_terms: &[QueryTerm],
    hits: &mut Vec<SearchHit>,
) -> Result<(), IndexQueryError> {
    // Deterministic sparse accumulator: keep a sorted Vec<(row_ix, score)>.
    //
    // This avoids HashMap randomness, at the cost of O(m log m) inserts.
    let mut acc: Vec<(u32, u64)> = Vec::new();

    for qt in query_terms {
        let ix = match find_term_ix(idx, qt.term) {
            Some(i) => i,
            None => continue,
        };
        let e = &idx.terms[ix];
        let idf = idf_ratio_scaled(idx.row_count, e.df);
        let mut it = idx.postings_iter(ix).map_err(map_decode_err)?;

        while let Some(p) = it.next() {
            let PostingV1 { row_ix, tf } = p.map_err(map_decode_err)?;
            let add = score_posting(tf, qt.qtf, idf);
            // Binary search by row_ix and insert/update.
            match acc.binary_search_by(|x| x.0.cmp(&row_ix)) {
                Ok(i) => acc[i].1 = acc[i].1.saturating_add(add),
                Err(i) => acc.insert(i, (row_ix, add)),
            }
        }
    }

    for (row_ix, score) in acc {
        if score != 0 {
            hits.push(SearchHit {
                frame_seg: *frame_seg,
                row_ix,
                score,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::ArtifactStore;
    use crate::frame::{DocId, FrameRowV1, SourceId};
    use crate::frame_segment::FrameSegmentV1;
    use crate::hash::blake3_hash;
    use crate::index_segment::IndexSegmentV1;
    use crate::index_snapshot::{IndexSnapshotEntryV1, IndexSnapshotV1};
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    /// In-memory deterministic artifact store used for tests.
    ///
    /// This store is deterministic and uses no randomized hashing.
    struct MemStore {
        m: std::cell::RefCell<BTreeMap<Hash32, Vec<u8>>>,
    }

    impl MemStore {
        fn new() -> MemStore {
            MemStore {
                m: std::cell::RefCell::new(BTreeMap::new()),
            }
        }
    }

    impl ArtifactStore for MemStore {
        fn put(&self, bytes: &[u8]) -> ArtifactResult<Hash32> {
            let h = blake3_hash(bytes);
            let mut mm = self.m.borrow_mut();
            mm.entry(h).or_insert_with(|| bytes.to_vec());
            Ok(h)
        }

        fn get(&self, hash: &Hash32) -> ArtifactResult<Option<Vec<u8>>> {
            Ok(self.m.borrow().get(hash).cloned())
        }

        fn path_for(&self, _hash: &Hash32) -> PathBuf {
            PathBuf::from("mem://")
        }
    }
    fn make_row(doc_u64: u64, source_u64: u64, text: &str) -> FrameRowV1 {
        let doc = DocId(crate::frame::Id64(doc_u64));
        let src = SourceId(crate::frame::Id64(source_u64));
        let mut r = FrameRowV1::new(doc, src);
        let tcfg = TokenizerCfg {
            max_token_bytes: 32,
        };
        r.terms = term_freqs_from_text(text, tcfg);
        r.recompute_doc_len();
        r
    }

    fn build_sig_map_for_snapshot(store: &MemStore, snap: &IndexSnapshotV1) -> Hash32 {
        use crate::index_sig_map::IndexSigMapV1;
        use crate::segment_sig::SegmentSigV1;

        let mut sm = IndexSigMapV1::new(snap.source_id);
        for e in snap.entries.iter() {
            let idx_bytes = store.get(&e.index_seg).unwrap().unwrap();
            let idx = IndexSegmentV1::decode(&idx_bytes).unwrap();
            let terms: Vec<crate::frame::TermId> = idx.terms.iter().map(|x| x.term).collect();
            let sig = SegmentSigV1::build(e.index_seg, &terms, 256, 4).unwrap();
            let sig_hash = store.put(&sig.encode().unwrap()).unwrap();
            sm.push(e.index_seg, sig_hash);
        }
        store.put(&sm.encode().unwrap()).unwrap()
    }

    #[test]
    fn search_snapshot_returns_expected_rows_and_is_deterministic() {
        let store = MemStore::new();

        // 3-row segment.
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

        let qcfg = QueryTermsCfg::new();
        let q = query_terms_from_text("apple", &qcfg);
        let scfg = SearchCfg {
            k: 10,
            entry_cap: 0,
            dense_row_threshold: 200_000,
        };

        let h1 = search_snapshot(&store, &snap_hash, &q, &scfg).unwrap();
        let h2 = search_snapshot(&store, &snap_hash, &q, &scfg).unwrap();

        // Expect two rows, row 0 then 1 (tie broken by row_ix).
        assert_eq!(h1.len(), 2);
        assert_eq!(h1[0].row_ix, 0);
        assert_eq!(h1[1].row_ix, 1);
        assert_eq!(h1, h2);
    }

    #[test]
    fn gated_search_matches_ungated_search_for_same_snapshot() {
        let store = MemStore::new();

        // 3-row segment.
        let rows = vec![
            make_row(1, 7, "apple banana"),
            make_row(2, 7, "apple carrot"),
            make_row(3, 7, "banana carrot"),
        ];
        let seg = FrameSegmentV1::from_rows(&rows, 4).unwrap();
        let seg_hash = store.put(&seg.encode().unwrap()).unwrap();

        let idx = IndexSegmentV1::build_from_segment(seg_hash, &seg).unwrap();
        let idx_hash = store.put(&idx.encode().unwrap()).unwrap();

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
        let snap_hash = store.put(&snap.encode().unwrap()).unwrap();
        let sig_map_hash = build_sig_map_for_snapshot(&store, &snap);

        let qcfg = QueryTermsCfg::new();
        let q = query_terms_from_text("apple", &qcfg);
        let scfg = SearchCfg {
            k: 10,
            entry_cap: 0,
            dense_row_threshold: 200_000,
        };

        let h0 = search_snapshot(&store, &snap_hash, &q, &scfg).unwrap();
        let (h1, _g1) =
            search_snapshot_gated(&store, &snap_hash, &sig_map_hash, &q, &scfg).unwrap();
        assert_eq!(h1, h0);
    }

    #[test]
    fn search_snapshot_with_control_includes_ties_at_cutoff_and_uses_control_tiebreak() {
        let store = MemStore::new();

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

        let qcfg = QueryTermsCfg::new();
        let q = query_terms_from_text("apple", &qcfg);
        let scfg = SearchCfg {
            k: 1,
            entry_cap: 0,
            dense_row_threshold: 200_000,
        };

        // Without control, truncation to k is strict.
        let base = search_snapshot(&store, &snap_hash, &q, &scfg).unwrap();
        assert_eq!(base.len(), 1);
        assert_eq!(base[0].row_ix, 0);

        // With control, include all ties at the cutoff score.
        let mut c1 = RetrievalControlV1::new(blake3_hash(b"prompt-a"));
        c1.pragmatics_frame_ids.push(blake3_hash(b"p1"));
        c1.pragmatics_frame_ids.push(blake3_hash(b"p2"));
        c1.validate().unwrap();

        let h1 = search_snapshot_with_control(&store, &snap_hash, &q, &scfg, Some(&c1)).unwrap();
        assert_eq!(h1.len(), 2);
        assert!(h1.iter().any(|h| h.row_ix == 0));
        assert!(h1.iter().any(|h| h.row_ix == 1));

        let seed1 = seed64_from_control(&c1);
        let k0 = tiebreak_key(seed1, &seg_hash, 0);
        let k1 = tiebreak_key(seed1, &seg_hash, 1);
        let expected_first = if k0 <= k1 { 0 } else { 1 };
        assert_eq!(h1[0].row_ix, expected_first);
        assert_eq!(h1[1].row_ix, 1 - expected_first);

        // Determinism: same inputs and same control yield identical ordering.
        let h1b = search_snapshot_with_control(&store, &snap_hash, &q, &scfg, Some(&c1)).unwrap();
        assert_eq!(h1b, h1);

        // A different control id yields a different (but still deterministic) tie ordering.
        let mut c2 = RetrievalControlV1::new(blake3_hash(b"prompt-b"));
        c2.pragmatics_frame_ids.push(blake3_hash(b"q1"));
        c2.validate().unwrap();

        let h2 = search_snapshot_with_control(&store, &snap_hash, &q, &scfg, Some(&c2)).unwrap();
        assert_eq!(h2.len(), 2);
        assert!(h2.iter().any(|h| h.row_ix == 0));
        assert!(h2.iter().any(|h| h.row_ix == 1));

        // Tie-inclusive truncation ensures the set of returned rows is stable across controls.
        let mut s1: Vec<u32> = h1.iter().map(|h| h.row_ix).collect();
        s1.sort_unstable();
        let mut s2: Vec<u32> = h2.iter().map(|h| h.row_ix).collect();
        s2.sort_unstable();
        assert_eq!(s2, s1);

        let seed2 = seed64_from_control(&c2);
        let k0b = tiebreak_key(seed2, &seg_hash, 0);
        let k1b = tiebreak_key(seed2, &seg_hash, 1);
        let expected_first_b = if k0b <= k1b { 0 } else { 1 };
        assert_eq!(h2[0].row_ix, expected_first_b);
        assert_eq!(h2[1].row_ix, 1 - expected_first_b);
    }

    #[test]
    fn query_terms_from_text_is_sorted_and_deduped() {
        let mut cfg = QueryTermsCfg::new();
        cfg.include_metaphone = false;
        let q = query_terms_from_text("a a b a", &cfg);
        assert!(q.len() >= 2);
        // Ensure sorted ascending.
        for i in 1..q.len() {
            assert!((q[i - 1].term.0).0 <= (q[i].term.0).0);
        }
        // Ensure no adjacent duplicates.
        for i in 1..q.len() {
            assert_ne!((q[i - 1].term.0).0, (q[i].term.0).0);
        }
    }
}

/// Search an IndexSnapshotV1 using SegmentSigV1 gating.
///
/// This function consults an IndexSigMapV1 (index artifact hash -> signature hash)
/// and uses SegmentSigV1 Bloom filters to skip decoding index artifacts that
/// cannot match any query term.
///
/// Behavior:
/// - Fail-open: if a signature mapping or signature is missing/invalid for an entry,
/// the index artifact is decoded to preserve recall.
/// - Deterministic: gating decisions and tie-breaking are stable for the same inputs.
///
/// Returns (hits, gate_stats).

/// Search an index snapshot with signature gating.
///
/// This is identical to `search_snapshot`, except it uses an `IndexSigMapV1` and
/// per-index `SegmentSigV1` artifacts to decide whether an index artifact is
/// worth decoding for this query. The gating rule is conservative: if signature
/// data is missing or inconsistent, we decode the index artifact.
pub fn search_snapshot_gated<S: ArtifactStore>(
    store: &S,
    snapshot_hash: &Hash32,
    sig_map_hash: &Hash32,
    query_terms: &[QueryTerm],
    cfg: &SearchCfg,
) -> Result<(Vec<SearchHit>, GateStatsV1), IndexQueryError> {
    search_snapshot_gated_inner(
        store,
        snapshot_hash,
        sig_map_hash,
        query_terms,
        cfg,
        None,
        false,
    )
}

pub(crate) fn search_snapshot_gated_inner<S: ArtifactStore>(
    store: &S,
    snapshot_hash: &Hash32,
    sig_map_hash: &Hash32,
    query_terms: &[QueryTerm],
    cfg: &SearchCfg,
    tie_seed: Option<u64>,
    include_ties: bool,
) -> Result<(Vec<SearchHit>, GateStatsV1), IndexQueryError> {
    use std::sync::Arc;

    if cfg.k == 0 {
        return Err(IndexQueryError::InvalidK);
    }

    let mut gate_stats = GateStatsV1::new();
    gate_stats.query_terms_total = query_terms.len() as u64;

    let snap_bytes = map_store_err(store.get(snapshot_hash))?
        .ok_or_else(|| IndexQueryError::store("snapshot not found"))?;
    let snap = IndexSnapshotV1::decode(&snap_bytes).map_err(map_decode_err)?;
    if snap.entries.is_empty() {
        return Err(IndexQueryError::EmptySnapshot);
    }

    let sm_bytes = map_store_err(store.get(sig_map_hash))?
        .ok_or_else(|| IndexQueryError::store("sig map not found"))?;
    let sig_map = IndexSigMapV1::decode(&sm_bytes).map_err(map_decode_err)?;

    let term_ids: Vec<TermId> = query_terms.iter().map(|qt| qt.term).collect();

    // Per-call signature decode cache.
    let mut sig_cache: Vec<(Hash32, Arc<SegmentSigV1>)> = Vec::new();

    // IndexPackV1 decode cache (per-call).
    let mut pack_cache: Vec<(Hash32, IndexPackV1)> = Vec::new();

    // Accumulate all hits then sort; v1 keeps this simple.
    let mut hits: Vec<SearchHit> = Vec::new();

    let mut entries_scanned: usize = 0;
    for e in &snap.entries {
        entries_scanned += 1;
        if cfg.entry_cap != 0 && entries_scanned > cfg.entry_cap {
            break;
        }

        let mut sig_load_err: Option<IndexQueryError> = None;
        let should_decode = should_decode_index_artifact_any(
            &term_ids,
            &e.index_seg,
            &sig_map,
            |sig_hash| {
                if sig_load_err.is_some() {
                    return None;
                }
                if let Some((_, arc)) = sig_cache.iter().find(|(h, _)| h == sig_hash) {
                    return Some(arc.clone());
                }
                match store.get(sig_hash) {
                    Ok(Some(bytes)) => match SegmentSigV1::decode(&bytes) {
                        Ok(sig) => {
                            let arc = Arc::new(sig);
                            sig_cache.push((sig_hash.clone(), arc.clone()));
                            Some(arc)
                        }
                        Err(de) => {
                            sig_load_err =
                                Some(IndexQueryError::Decode(format!("segment_sig: {}", de)));
                            None
                        }
                    },
                    Ok(None) => None,
                    Err(se) => {
                        sig_load_err = Some(IndexQueryError::Store(format!("segment_sig: {}", se)));
                        None
                    }
                }
            },
            &mut gate_stats,
        );
        if let Some(err) = sig_load_err {
            return Err(err);
        }
        if !should_decode {
            continue;
        }

        let idx: IndexSegmentV1 = if let Some(pack) = find_pack(&pack_cache, &e.index_seg) {
            let inner = pack
                .get_index_bytes(&e.frame_seg)
                .ok_or_else(|| IndexQueryError::decode("index pack missing frame segment"))?;
            IndexSegmentV1::decode(inner).map_err(map_decode_err)?
        } else {
            let idx_bytes = map_store_err(store.get(&e.index_seg))?
                .ok_or_else(|| IndexQueryError::store("index artifact not found"))?;
            if is_index_pack(&idx_bytes) {
                let pack = decode_and_cache_pack(&mut pack_cache, e.index_seg, &idx_bytes)?;
                let inner = pack
                    .get_index_bytes(&e.frame_seg)
                    .ok_or_else(|| IndexQueryError::decode("index pack missing frame segment"))?;
                IndexSegmentV1::decode(inner).map_err(map_decode_err)?
            } else {
                IndexSegmentV1::decode(&idx_bytes).map_err(map_decode_err)?
            }
        };

        let n = idx.row_count;
        if n == 0 {
            continue;
        }

        if n <= cfg.dense_row_threshold {
            score_segment_dense(&idx, &e.frame_seg, query_terms, &mut hits)?;
        } else {
            score_segment_sparse(&idx, &e.frame_seg, query_terms, &mut hits)?;
        }
    }

    rank_and_truncate_hits(&mut hits, cfg.k, tie_seed, include_ties);

    Ok((hits, gate_stats))
}

/// Signature-gated snapshot search with an optional control record.
///
/// This is identical to [`search_snapshot_gated`] and ignores `control` in v1.
pub fn search_snapshot_gated_with_control<S: ArtifactStore>(
    store: &S,
    snapshot_hash: &Hash32,
    sig_map_hash: &Hash32,
    query_terms: &[QueryTerm],
    cfg: &SearchCfg,
    control: Option<&RetrievalControlV1>,
) -> Result<(Vec<SearchHit>, GateStatsV1), IndexQueryError> {
    let (seed, include_ties) = match control {
        Some(c) => (Some(seed64_from_control(c)), true),
        None => (None, false),
    };
    search_snapshot_gated_inner(
        store,
        snapshot_hash,
        sig_map_hash,
        query_terms,
        cfg,
        seed,
        include_ties,
    )
}

/// Cached variant of `search_snapshot_gated`.
///
/// Snapshot and non-pack index artifacts can be cached by the caller.
pub fn search_snapshot_cached_gated<S: ArtifactStore>(
    store: &S,
    snapshot_hash: &Hash32,
    sig_map_hash: &Hash32,
    query_terms: &[QueryTerm],
    cfg: &SearchCfg,
    snap_cache: Option<&mut crate::cache::Cache2Q<Hash32, std::sync::Arc<IndexSnapshotV1>>>,
    idx_cache: Option<&mut crate::cache::Cache2Q<Hash32, std::sync::Arc<IndexSegmentV1>>>,
) -> Result<(Vec<SearchHit>, GateStatsV1), IndexQueryError> {
    search_snapshot_cached_gated_inner(
        store,
        snapshot_hash,
        sig_map_hash,
        query_terms,
        cfg,
        snap_cache,
        idx_cache,
        None,
        false,
    )
}

fn search_snapshot_cached_gated_inner<S: ArtifactStore>(
    store: &S,
    snapshot_hash: &Hash32,
    sig_map_hash: &Hash32,
    query_terms: &[QueryTerm],
    cfg: &SearchCfg,
    snap_cache: Option<&mut crate::cache::Cache2Q<Hash32, std::sync::Arc<IndexSnapshotV1>>>,
    mut idx_cache: Option<&mut crate::cache::Cache2Q<Hash32, std::sync::Arc<IndexSegmentV1>>>,
    tie_seed: Option<u64>,
    include_ties: bool,
) -> Result<(Vec<SearchHit>, GateStatsV1), IndexQueryError> {
    use crate::index_snapshot_store::{get_index_snapshot_v1_cached, IndexSnapshotStoreError};
    use std::sync::Arc;

    if cfg.k == 0 {
        return Err(IndexQueryError::InvalidK);
    }

    let mut gate_stats = GateStatsV1::new();
    gate_stats.query_terms_total = query_terms.len() as u64;

    let snap_arc: Arc<IndexSnapshotV1> = match snap_cache {
        Some(cache) => {
            let sopt =
                get_index_snapshot_v1_cached(store, cache, snapshot_hash).map_err(|e| match e {
                    IndexSnapshotStoreError::Decode(d) => IndexQueryError::Decode(d.to_string()),
                    IndexSnapshotStoreError::Store(s) => IndexQueryError::Store(s.to_string()),
                    IndexSnapshotStoreError::Encode(en) => IndexQueryError::Decode(en.to_string()),
                })?;
            sopt.ok_or_else(|| IndexQueryError::store("snapshot not found"))?
        }
        None => {
            let bytes = map_store_err(store.get(snapshot_hash))?
                .ok_or_else(|| IndexQueryError::store("snapshot not found"))?;
            let snap = IndexSnapshotV1::decode(&bytes).map_err(map_decode_err)?;
            Arc::new(snap)
        }
    };

    let snapshot: &IndexSnapshotV1 = snap_arc.as_ref();
    if snapshot.entries.is_empty() {
        return Err(IndexQueryError::EmptySnapshot);
    }

    let sm_bytes = map_store_err(store.get(sig_map_hash))?
        .ok_or_else(|| IndexQueryError::store("sig map not found"))?;
    let sig_map = IndexSigMapV1::decode(&sm_bytes).map_err(map_decode_err)?;

    let term_ids: Vec<TermId> = query_terms.iter().map(|qt| qt.term).collect();

    // Per-call signature decode cache.
    let mut sig_cache: Vec<(Hash32, Arc<SegmentSigV1>)> = Vec::new();

    // IndexPackV1 decode cache (per-call).
    let mut pack_cache: Vec<(Hash32, IndexPackV1)> = Vec::new();

    let mut hits: Vec<SearchHit> = Vec::new();

    let mut entries_scanned: usize = 0;
    for e in snapshot.entries.iter() {
        entries_scanned += 1;
        if cfg.entry_cap != 0 && entries_scanned > cfg.entry_cap {
            break;
        }

        let mut sig_load_err: Option<IndexQueryError> = None;
        let should_decode = should_decode_index_artifact_any(
            &term_ids,
            &e.index_seg,
            &sig_map,
            |sig_hash| {
                if sig_load_err.is_some() {
                    return None;
                }
                if let Some((_, arc)) = sig_cache.iter().find(|(h, _)| h == sig_hash) {
                    return Some(arc.clone());
                }
                match store.get(sig_hash) {
                    Ok(Some(bytes)) => match SegmentSigV1::decode(&bytes) {
                        Ok(sig) => {
                            let arc = Arc::new(sig);
                            sig_cache.push((sig_hash.clone(), arc.clone()));
                            Some(arc)
                        }
                        Err(de) => {
                            sig_load_err =
                                Some(IndexQueryError::Decode(format!("segment_sig: {}", de)));
                            None
                        }
                    },
                    Ok(None) => None,
                    Err(se) => {
                        sig_load_err = Some(IndexQueryError::Store(format!("segment_sig: {}", se)));
                        None
                    }
                }
            },
            &mut gate_stats,
        );
        if let Some(err) = sig_load_err {
            return Err(err);
        }
        if !should_decode {
            continue;
        }

        let idx_arc: Arc<IndexSegmentV1> = if let Some(pack) = find_pack(&pack_cache, &e.index_seg)
        {
            let inner = pack
                .get_index_bytes(&e.frame_seg)
                .ok_or_else(|| IndexQueryError::decode("index pack missing frame segment"))?;
            let idx = IndexSegmentV1::decode(inner).map_err(map_decode_err)?;
            Arc::new(idx)
        } else if let Some(cache) = idx_cache.as_deref_mut() {
            if let Some(v) = cache.get(&e.index_seg) {
                v.clone()
            } else {
                let idx_bytes = map_store_err(store.get(&e.index_seg))?
                    .ok_or_else(|| IndexQueryError::store("index artifact not found"))?;

                if is_index_pack(&idx_bytes) {
                    let dec = IndexPackV1::decode(&idx_bytes).map_err(map_decode_err)?;
                    let pack = insert_pack(&mut pack_cache, e.index_seg.clone(), dec);
                    let inner = pack.get_index_bytes(&e.frame_seg).ok_or_else(|| {
                        IndexQueryError::decode("index pack missing frame segment")
                    })?;
                    let idx = IndexSegmentV1::decode(inner).map_err(map_decode_err)?;
                    Arc::new(idx)
                } else {
                    let idx = IndexSegmentV1::decode(&idx_bytes).map_err(map_decode_err)?;
                    let arc = Arc::new(idx);
                    let _ =
                        cache.insert_cost(e.index_seg.clone(), arc.clone(), idx_bytes.len() as u64);
                    arc
                }
            }
        } else {
            let idx_bytes = map_store_err(store.get(&e.index_seg))?
                .ok_or_else(|| IndexQueryError::store("index artifact not found"))?;
            if is_index_pack(&idx_bytes) {
                let dec = IndexPackV1::decode(&idx_bytes).map_err(map_decode_err)?;
                let pack = insert_pack(&mut pack_cache, e.index_seg.clone(), dec);
                let inner = pack
                    .get_index_bytes(&e.frame_seg)
                    .ok_or_else(|| IndexQueryError::decode("index pack missing frame segment"))?;
                let idx = IndexSegmentV1::decode(inner).map_err(map_decode_err)?;
                Arc::new(idx)
            } else {
                let idx = IndexSegmentV1::decode(&idx_bytes).map_err(map_decode_err)?;
                Arc::new(idx)
            }
        };

        let idx: &IndexSegmentV1 = idx_arc.as_ref();

        let n = idx.row_count;
        if n == 0 {
            continue;
        }

        if n <= cfg.dense_row_threshold {
            score_segment_dense(idx, &e.frame_seg, query_terms, &mut hits)?;
        } else {
            score_segment_sparse(idx, &e.frame_seg, query_terms, &mut hits)?;
        }
    }

    rank_and_truncate_hits(&mut hits, cfg.k, tie_seed, include_ties);

    Ok((hits, gate_stats))
}

/// Cached signature-gated snapshot search with an optional control record.
///
/// This is identical to [`search_snapshot_cached_gated`] and ignores `control` in v1.
pub fn search_snapshot_cached_gated_with_control<S: ArtifactStore>(
    store: &S,
    snapshot_hash: &Hash32,
    sig_map_hash: &Hash32,
    query_terms: &[QueryTerm],
    cfg: &SearchCfg,
    snap_cache: Option<&mut crate::cache::Cache2Q<Hash32, std::sync::Arc<IndexSnapshotV1>>>,
    idx_cache: Option<&mut crate::cache::Cache2Q<Hash32, std::sync::Arc<IndexSegmentV1>>>,
    control: Option<&RetrievalControlV1>,
) -> Result<(Vec<SearchHit>, GateStatsV1), IndexQueryError> {
    let (seed, include_ties) = match control {
        Some(c) => (Some(seed64_from_control(c)), true),
        None => (None, false),
    };
    search_snapshot_cached_gated_inner(
        store,
        snapshot_hash,
        sig_map_hash,
        query_terms,
        cfg,
        snap_cache,
        idx_cache,
        seed,
        include_ties,
    )
}
