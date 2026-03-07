// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Query-time gating helpers.
//!
//! Purpose
//! -------
//! Stage 1 gating is an optimization that avoids loading and decoding index
//! artifacts that cannot match a query. This module provides a small, testable
//! core for consulting SegmentSigV1 (Bloom over TermId) via IndexSigMapV1.
//!
//! Safety and determinism
//! ----------------------
//! - Conservative: we only skip an index artifact when the Bloom filter
//! definitively rejects at least one required term.
//! - If any gating input is missing or inconsistent, we fall back to decoding.
//! - No randomness: the decision is a pure function of inputs.

use crate::frame::TermId;
use crate::hash::Hash32;
use crate::index_sig_map::IndexSigMapV1;
use crate::segment_sig::SegmentSigV1;
use std::sync::Arc;

/// Lightweight counters for Stage 1 gating.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct GateStatsV1 {
    /// Number of index artifacts considered.
    pub entries_total: u64,
    /// Number of entries skipped due to a definitive signature miss.
    pub entries_skipped_sig: u64,
    /// Number of entries that were not skipped (decode path).
    pub entries_decoded: u64,
    /// Number of entries where signature data was missing or inconsistent.
    pub entries_missing_sig: u64,
    /// Number of query terms used for gating (set from the query).
    pub query_terms_total: u64,
    /// Total number of Bloom probes performed across all entries.
    pub bloom_probes_total: u64,
    /// The sig-map did not contain an entry for the index artifact.
    pub sig_map_miss: u64,
    /// The sig-map referenced a signature hash that could not be loaded.
    pub sig_missing: u64,
    /// The loaded signature's index_seg did not match the target index artifact.
    pub sig_mismatch: u64,
    /// Number of entries that were skipped due to Bloom rejecting a required term.
    pub bloom_definite_miss: u64,
    /// Number of entries that passed Bloom checks (may still miss later).
    pub bloom_maybe: u64,
}

impl GateStatsV1 {
    /// Create a new zeroed stats struct.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Decide whether an index artifact should be decoded, using signatures.
///
/// This function is conservative:
/// - If the sig-map does not reference a signature for the index artifact,
/// it returns true (decode).
/// - If the signature cannot be loaded, it returns true (decode).
/// - If the signature is inconsistent (sig.index_seg != index_seg), it returns
/// true (decode).
/// - Otherwise, it checks all required terms; if any term is definitively absent
/// (Bloom test false), it returns false (skip).
///
/// The signature loader is provided by the caller to allow integration with
/// caches and different storage backends.
pub fn should_decode_index_artifact<F>(
    required_terms: &[TermId],
    index_seg: &Hash32,
    sig_map: &IndexSigMapV1,
    mut load_sig_by_hash: F,
    stats: &mut GateStatsV1,
) -> bool
where
    F: FnMut(&Hash32) -> Option<Arc<SegmentSigV1>>,
{
    stats.entries_total = stats.entries_total.saturating_add(1);
    stats.query_terms_total = required_terms.len() as u64;

    let sig_hash = match sig_map.lookup_sig(index_seg) {
        Some(h) => h,
        None => {
            stats.sig_map_miss = stats.sig_map_miss.saturating_add(1);
            stats.entries_missing_sig = stats.entries_missing_sig.saturating_add(1);
            stats.entries_decoded = stats.entries_decoded.saturating_add(1);
            return true;
        }
    };

    let sig = match load_sig_by_hash(&sig_hash) {
        Some(s) => s,
        None => {
            stats.sig_missing = stats.sig_missing.saturating_add(1);
            stats.entries_missing_sig = stats.entries_missing_sig.saturating_add(1);
            stats.entries_decoded = stats.entries_decoded.saturating_add(1);
            return true;
        }
    };

    if sig.index_seg != *index_seg {
        stats.sig_mismatch = stats.sig_mismatch.saturating_add(1);
        stats.entries_missing_sig = stats.entries_missing_sig.saturating_add(1);
        stats.entries_decoded = stats.entries_decoded.saturating_add(1);
        return true;
    }

    for &t in required_terms.iter() {
        stats.bloom_probes_total = stats.bloom_probes_total.saturating_add(1);
        if !sig.might_contain_term(t) {
            stats.entries_skipped_sig = stats.entries_skipped_sig.saturating_add(1);
            stats.bloom_definite_miss = stats.bloom_definite_miss.saturating_add(1);
            return false;
        }
        stats.bloom_maybe = stats.bloom_maybe.saturating_add(1);
    }

    stats.entries_decoded = stats.entries_decoded.saturating_add(1);
    true
}


/// Decide whether an index artifact should be decoded for an OR-style query (any term may match).
///
/// This is conservative:
/// - Missing sig-map entry => decode
/// - Missing signature => decode
/// - Signature mismatch => decode
/// - Otherwise, skip only if Bloom rejects ALL query terms
///
/// Stats are updated similarly to should_decode_index_artifact.
pub fn should_decode_index_artifact_any<F>(
    query_terms_any: &[TermId],
    index_seg: &Hash32,
    sig_map: &IndexSigMapV1,
    mut load_sig_by_hash: F,
    stats: &mut GateStatsV1,
) -> bool
where
    F: FnMut(&Hash32) -> Option<Arc<SegmentSigV1>>,
{
    stats.entries_total = stats.entries_total.saturating_add(1);
    stats.query_terms_total = query_terms_any.len() as u64;

    let sig_hash = match sig_map.lookup_sig(index_seg) {
        Some(h) => h,
        None => {
            stats.sig_map_miss = stats.sig_map_miss.saturating_add(1);
            stats.entries_missing_sig = stats.entries_missing_sig.saturating_add(1);
            stats.entries_decoded = stats.entries_decoded.saturating_add(1);
            return true;
        }
    };

    let sig = match load_sig_by_hash(&sig_hash) {
        Some(s) => s,
        None => {
            stats.sig_missing = stats.sig_missing.saturating_add(1);
            stats.entries_missing_sig = stats.entries_missing_sig.saturating_add(1);
            stats.entries_decoded = stats.entries_decoded.saturating_add(1);
            return true;
        }
    };

    if sig.index_seg != *index_seg {
        stats.sig_mismatch = stats.sig_mismatch.saturating_add(1);
        stats.entries_missing_sig = stats.entries_missing_sig.saturating_add(1);
        stats.entries_decoded = stats.entries_decoded.saturating_add(1);
        return true;
    }

    // If any query term might be present, we must decode.
    // Skip only if Bloom rejects all terms.
    let mut any_maybe = false;
    for &t in query_terms_any.iter() {
        stats.bloom_probes_total = stats.bloom_probes_total.saturating_add(1);
        if sig.might_contain_term(t) {
            stats.bloom_maybe = stats.bloom_maybe.saturating_add(1);
            any_maybe = true;
            break;
        } else {
            stats.bloom_definite_miss = stats.bloom_definite_miss.saturating_add(1);
        }
    }

    if !any_maybe {
        stats.entries_skipped_sig = stats.entries_skipped_sig.saturating_add(1);
        return false;
    }

    stats.entries_decoded = stats.entries_decoded.saturating_add(1);
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{Id64, SourceId};

    fn h(b: u8) -> Hash32 {
        let mut out = [0u8; 32];
        out[0] = b;
        out
    }

    fn t(v: u64) -> TermId {
        TermId(Id64(v))
    }

    #[test]
    fn gating_missing_map_entry_decodes() {
        let sid = SourceId(Id64(1));
        let map = IndexSigMapV1::new(sid);
        let idx = h(7);

        let mut stats = GateStatsV1::new();
        let dec = should_decode_index_artifact(&[t(123)], &idx, &map, |_h| None, &mut stats);
        assert!(dec);
        assert_eq!(stats.entries_total, 1);
        assert_eq!(stats.sig_map_miss, 1);
        assert_eq!(stats.entries_decoded, 1);
        assert_eq!(stats.entries_skipped_sig, 0);
    }

    #[test]
    fn gating_missing_sig_decodes() {
        let sid = SourceId(Id64(1));
        let mut map = IndexSigMapV1::new(sid);
        let idx = h(7);
        let sig_hash = h(9);
        map.push(idx, sig_hash);

        let mut stats = GateStatsV1::new();
        let dec = should_decode_index_artifact(&[t(123)], &idx, &map, |_h| None, &mut stats);
        assert!(dec);
        assert_eq!(stats.entries_total, 1);
        assert_eq!(stats.sig_missing, 1);
        assert_eq!(stats.entries_decoded, 1);
        assert_eq!(stats.entries_skipped_sig, 0);
    }

    #[test]
    fn gating_definite_miss_skips() {
        let sid = SourceId(Id64(1));
        let mut map = IndexSigMapV1::new(sid);
        let idx = h(7);
        let sig_hash = h(9);
        map.push(idx, sig_hash);

        // Build an empty Bloom filter: it must reject any term.
        let sig = SegmentSigV1::build(idx, &[], 64, 4).unwrap();
        let sig_arc = Arc::new(sig);

        let mut stats = GateStatsV1::new();
        let dec = should_decode_index_artifact(
            &[t(123)],
            &idx,
            &map,
            |h| {
                if *h == sig_hash {
                    Some(Arc::clone(&sig_arc))
                } else {
                    None
                }
            },
            &mut stats,
        );
        assert!(!dec);
        assert_eq!(stats.entries_total, 1);
        assert_eq!(stats.entries_skipped_sig, 1);
        assert_eq!(stats.bloom_definite_miss, 1);
        assert_eq!(stats.entries_decoded, 0);
    }

    #[test]
    fn gating_maybe_decodes() {
        let sid = SourceId(Id64(1));
        let mut map = IndexSigMapV1::new(sid);
        let idx = h(7);
        let sig_hash = h(9);
        map.push(idx, sig_hash);

        let sig = SegmentSigV1::build(idx, &[t(123)], 64, 4).unwrap();
        let sig_arc = Arc::new(sig);

        let mut stats = GateStatsV1::new();
        let dec = should_decode_index_artifact(
            &[t(123)],
            &idx,
            &map,
            |h| {
                if *h == sig_hash {
                    Some(Arc::clone(&sig_arc))
                } else {
                    None
                }
            },
            &mut stats,
        );
        assert!(dec);
        assert_eq!(stats.entries_total, 1);
        assert_eq!(stats.entries_decoded, 1);
        assert_eq!(stats.bloom_maybe, 1);
        assert_eq!(stats.entries_skipped_sig, 0);
    }

    #[test]
    fn gating_sig_mismatch_decodes() {
        let sid = SourceId(Id64(1));
        let mut map = IndexSigMapV1::new(sid);
        let idx = h(7);
        let sig_hash = h(9);
        map.push(idx, sig_hash);

        // Signature claims to describe a different index artifact.
        let other_idx = h(8);
        let sig = SegmentSigV1::build(other_idx, &[t(123)], 64, 4).unwrap();
        let sig_arc = Arc::new(sig);

        let mut stats = GateStatsV1::new();
        let dec = should_decode_index_artifact(
            &[t(123)],
            &idx,
            &map,
            |h| {
                if *h == sig_hash {
                    Some(Arc::clone(&sig_arc))
                } else {
                    None
                }
            },
            &mut stats,
        );
        assert!(dec);
        assert_eq!(stats.entries_total, 1);
        assert_eq!(stats.sig_mismatch, 1);
        assert_eq!(stats.entries_decoded, 1);
    }

    fn find_definite_miss(sig: &SegmentSigV1, start: u64) -> TermId {
        // Find a TermId that the Bloom filter definitively rejects.
        // This avoids flaky tests due to Bloom false positives.
        for i in 0..50000u64 {
            let cand = t(start + i);
            if !sig.might_contain_term(cand) {
                return cand;
            }
        }
        panic!("no Bloom definite-miss found in scan range");
    }

    #[test]
    fn any_gating_definite_miss_skips_deterministic() {
        let sid = SourceId(Id64(1));
        let mut map = IndexSigMapV1::new(sid);
        let idx = h(7);
        let sig_hash = h(9);
        map.push(idx, sig_hash);

        let sig = SegmentSigV1::build(idx, &[t(123)], 64, 4).unwrap();
        let sig_arc = Arc::new(sig);

        let miss = find_definite_miss(sig_arc.as_ref(), 1000);

        let mut stats = GateStatsV1::new();
        let dec = should_decode_index_artifact_any(
            &[miss],
            &idx,
            &map,
            |h| {
                if *h == sig_hash {
                    Some(Arc::clone(&sig_arc))
                } else {
                    None
                }
            },
            &mut stats,
        );
        assert!(!dec);
        assert_eq!(stats.entries_total, 1);
        assert_eq!(stats.entries_skipped_sig, 1);
        assert_eq!(stats.bloom_definite_miss, 1);
        assert_eq!(stats.entries_decoded, 0);
    }

    #[test]
    fn any_gating_maybe_decodes_if_any_term_present() {
        let sid = SourceId(Id64(1));
        let mut map = IndexSigMapV1::new(sid);
        let idx = h(7);
        let sig_hash = h(9);
        map.push(idx, sig_hash);

        let sig = SegmentSigV1::build(idx, &[t(123)], 64, 4).unwrap();
        let sig_arc = Arc::new(sig);

        let miss = find_definite_miss(sig_arc.as_ref(), 1000);

        let mut stats = GateStatsV1::new();
        let dec = should_decode_index_artifact_any(
            &[miss, t(123)],
            &idx,
            &map,
            |h| {
                if *h == sig_hash {
                    Some(Arc::clone(&sig_arc))
                } else {
                    None
                }
            },
            &mut stats,
        );
        assert!(dec);
        assert_eq!(stats.entries_total, 1);
        assert_eq!(stats.entries_decoded, 1);
        assert_eq!(stats.bloom_maybe, 1);
        assert_eq!(stats.entries_skipped_sig, 0);
    }

}
