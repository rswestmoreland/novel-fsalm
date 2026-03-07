// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Lexicon-powered cue neighborhoods (v1).
//!
//! This module builds small, bounded lemma-id neighborhoods for intent-style
//! classification and prompt-context feature extraction.
//!
//! Design goals:
//! - Deterministic given identical lexicon artifacts + config.
//! - Bounded memory and traversal (caps + depth limit).
//! - Reuse existing LexiconExpandLookupV1 utilities.
//! - Optional helpers for POS checks without building large global maps.

use crate::lexicon::{
    derive_lemma_key_id, LemmaId, LemmaKeyId, RelTypeId, POS_ADJ, POS_ADV, POS_NOUN,
    POS_PROPER_NOUN, POS_VERB, REL_COORDINATE_TERM, REL_DERIVED_TERM, REL_HOLONYM, REL_HYPERNYM,
    REL_HYPONYM, REL_MERONYM, REL_RELATED, REL_SYNONYM,
};
use crate::lexicon_expand_lookup::LexiconExpandLookupV1;

use std::collections::{BTreeSet, VecDeque};

/// Neighborhood builder configuration (v1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LexiconNeighborhoodCfgV1 {
    /// Maximum number of lemma ids returned for a neighborhood.
    pub cap_total: usize,
    /// Maximum number of lemma ids loaded per seed key (lemma_key_id lookup).
    pub cap_seed_per_key: usize,
    /// Maximum number of neighbor lemma ids considered for a given (from, rel_type) edge query.
    pub cap_neighbors_per_rel: usize,
    /// Maximum traversal depth from the seed set.
    /// Depth 0 yields only seeds. Depth 1 yields seeds + immediate neighbors.
    pub max_depth: u8,
}

impl LexiconNeighborhoodCfgV1 {
    /// Default configuration tuned for small intent neighborhoods.
    pub fn new() -> Self {
        LexiconNeighborhoodCfgV1 {
            cap_total: 4096,
            cap_seed_per_key: 8,
            cap_neighbors_per_rel: 64,
            max_depth: 2,
        }
    }
}

/// Lexicon cue neighborhoods (v1).
///
/// These are lemma-id sets intended for deterministic, rules-first intent
/// detection. The sets are sorted ascending by lemma id and unique.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LexiconCueNeighborhoodsV1 {
    /// Planning-related lemma ids.
    pub planning: Vec<LemmaId>,
    /// Problem-solving lemma ids (troubleshooting, retrospection, investigation).
    pub problem_solve: Vec<LemmaId>,
    /// Logic puzzle / constraint reasoning lemma ids.
    pub logic_puzzle: Vec<LemmaId>,
}

/// Build lexicon cue neighborhoods using a loaded expansion lookup view.
///
/// If the lexicon snapshot does not contain any of the seed keys, the
/// corresponding neighborhood will be empty.
pub fn build_lexicon_cue_neighborhoods_v1(
    view: &LexiconExpandLookupV1,
    cfg: &LexiconNeighborhoodCfgV1,
) -> LexiconCueNeighborhoodsV1 {
    LexiconCueNeighborhoodsV1 {
        planning: expand_from_seed_keys(view, &PLANNING_SEED_KEYS, cfg),
        problem_solve: expand_from_seed_keys(view, &PROBLEM_SOLVE_SEED_KEYS, cfg),
        logic_puzzle: expand_from_seed_keys(view, &LOGIC_PUZZLE_SEED_KEYS, cfg),
    }
}

/// Optional helper: lookup lemma ids for an ASCII-lowercased seed string.
///
/// This is a thin wrapper around LexiconExpandLookupV1::lemma_ids_for_key.
pub fn lemma_ids_for_seed_key(
    view: &LexiconExpandLookupV1,
    seed: &str,
    cap: usize,
) -> Vec<LemmaId> {
    if cap == 0 {
        return Vec::new();
    }
    let key: LemmaKeyId = derive_lemma_key_id(seed);
    view.lemma_ids_for_key(key, cap)
}

/// Optional helper: lookup a lemma POS mask (if present) without building a global map.
///
/// This searches segments in snapshot order and performs a binary search within
/// each segment's lemma_id column.
pub fn lemma_pos_mask(view: &LexiconExpandLookupV1, lemma_id: LemmaId) -> Option<u32> {
    let target: u64 = (lemma_id.0).0;
    for s in &view.segments {
        let ids = &s.seg.lemma_id;
        if ids.is_empty() {
            continue;
        }
        // lemma_id is sorted ascending by LexiconSegment canonicalization.
        let mut lo: usize = 0;
        let mut hi: usize = ids.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            let v: u64 = (ids[mid].0).0;
            if v < target {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo < ids.len() {
            if (ids[lo].0).0 == target {
                return Some(s.seg.lemma_pos_mask[lo]);
            }
        }
    }
    None
}

/// Optional helper: check if a lemma POS mask includes any content POS.
pub fn pos_is_content(pos_mask: u32) -> bool {
    (pos_mask & (POS_NOUN | POS_VERB | POS_ADJ | POS_ADV | POS_PROPER_NOUN)) != 0
}

// Seed keys are small and stable. Expansion grows coverage via lexicon relations.
const PLANNING_SEED_KEYS: [&str; 5] = ["plan", "step", "implement", "roadmap", "procedure"];
const PROBLEM_SOLVE_SEED_KEYS: [&str; 6] = [
    "diagnose",
    "investigate",
    "analyze",
    "troubleshoot",
    "understand",
    "explain",
];
const LOGIC_PUZZLE_SEED_KEYS: [&str; 5] = ["logic", "puzzle", "constraint", "deduce", "inference"];

const NEIGHBOR_REL_TYPES: [RelTypeId; 8] = [
    REL_SYNONYM,
    REL_RELATED,
    REL_HYPERNYM,
    REL_HYPONYM,
    REL_DERIVED_TERM,
    REL_COORDINATE_TERM,
    REL_HOLONYM,
    REL_MERONYM,
];

fn expand_from_seed_keys(
    view: &LexiconExpandLookupV1,
    seed_keys: &[&str],
    cfg: &LexiconNeighborhoodCfgV1,
) -> Vec<LemmaId> {
    if cfg.cap_total == 0 {
        return Vec::new();
    }

    // Load seeds as lemma ids.
    let mut seed_ids: Vec<LemmaId> = Vec::new();
    for &s in seed_keys {
        let mut got = lemma_ids_for_seed_key(view, s, cfg.cap_seed_per_key);
        seed_ids.append(&mut got);
        if seed_ids.len() >= cfg.cap_total {
            break;
        }
    }

    // Canonicalize seeds: sort + unique.
    seed_ids.sort_by(|a, b| ((a.0).0).cmp(&((b.0).0)));
    seed_ids.dedup();
    if seed_ids.is_empty() {
        return Vec::new();
    }
    if seed_ids.len() > cfg.cap_total {
        seed_ids.truncate(cfg.cap_total);
    }

    // Deterministic BFS with bounded depth.
    let mut visited: BTreeSet<u64> = BTreeSet::new();
    let mut q: VecDeque<(LemmaId, u8)> = VecDeque::new();
    for &id in &seed_ids {
        visited.insert((id.0).0);
        q.push_back((id, 0));
    }

    let max_depth = cfg.max_depth;
    while let Some((cur, depth)) = q.pop_front() {
        if visited.len() >= cfg.cap_total {
            break;
        }
        if depth >= max_depth {
            continue;
        }

        for &rt in &NEIGHBOR_REL_TYPES {
            if visited.len() >= cfg.cap_total {
                break;
            }
            let neigh = view.related_lemmas_from_lemma(cur, rt, cfg.cap_neighbors_per_rel);
            for n in neigh {
                if visited.len() >= cfg.cap_total {
                    break;
                }
                let v: u64 = (n.0).0;
                if visited.insert(v) {
                    q.push_back((n, depth + 1));
                }
            }
        }
    }

    let mut out: Vec<LemmaId> = Vec::with_capacity(visited.len());
    for v in visited {
        out.push(LemmaId(crate::frame::Id64(v)));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::artifact::FsArtifactStore;
    use crate::frame::Id64;
    use crate::lexicon::{
        derive_lemma_id, derive_text_id, LemmaRowV1, RelFromId, RelationEdgeRowV1, SenseRowV1,
        LEXICON_SCHEMA_V1, REL_SYNONYM,
    };
    use crate::lexicon_expand_lookup::load_lexicon_expand_lookup_v1;
    use crate::lexicon_segment::LexiconSegmentV1;
    use crate::lexicon_segment_store::put_lexicon_segment_v1;
    use crate::lexicon_snapshot::{LexiconSnapshotEntryV1, LexiconSnapshotV1};
    use crate::lexicon_snapshot_store::put_lexicon_snapshot_v1;

    use core::sync::atomic::{AtomicUsize, Ordering};

    static TMP_DIR_SEQ: AtomicUsize = AtomicUsize::new(0);

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        use std::fs;
        let mut p = std::env::temp_dir();
        p.push("fsa_lm_tests");

        // Tests run in parallel by default. Use a per-call unique directory name
        // to avoid cross-test races where one test deletes another test's store
        // directory during an atomic write/rename.
        let pid = std::process::id();
        let seq = TMP_DIR_SEQ.fetch_add(1, Ordering::Relaxed);
        p.push(format!("{}_{}_{}", name, pid, seq));

        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn lemma_row(text: &str, pos_mask: u32) -> LemmaRowV1 {
        LemmaRowV1 {
            version: LEXICON_SCHEMA_V1,
            lemma_id: derive_lemma_id(text),
            lemma_key_id: derive_lemma_key_id(text),
            lemma_text_id: derive_text_id(text),
            pos_mask,
            flags: 0,
        }
    }

    fn build_test_segment() -> LexiconSegmentV1 {
        // Include a subset of built-in seed keys plus a few synonym chains.
        // plan -> scheme -> strategy
        // diagnose -> determine -> decide
        // logic -> reasoning
        let lemmas = vec![
            lemma_row("plan", POS_VERB),
            lemma_row("scheme", POS_NOUN),
            lemma_row("strategy", POS_NOUN),
            lemma_row("diagnose", POS_VERB),
            lemma_row("determine", POS_VERB),
            lemma_row("decide", POS_VERB),
            lemma_row("logic", POS_NOUN),
            lemma_row("reasoning", POS_NOUN),
        ];

        let senses: Vec<SenseRowV1> = Vec::new();

        let rels = vec![
            RelationEdgeRowV1 {
                version: LEXICON_SCHEMA_V1,
                from: RelFromId::Lemma(derive_lemma_id("plan")),
                rel_type_id: REL_SYNONYM,
                to_lemma_id: derive_lemma_id("scheme"),
            },
            RelationEdgeRowV1 {
                version: LEXICON_SCHEMA_V1,
                from: RelFromId::Lemma(derive_lemma_id("scheme")),
                rel_type_id: REL_SYNONYM,
                to_lemma_id: derive_lemma_id("strategy"),
            },
            RelationEdgeRowV1 {
                version: LEXICON_SCHEMA_V1,
                from: RelFromId::Lemma(derive_lemma_id("diagnose")),
                rel_type_id: REL_SYNONYM,
                to_lemma_id: derive_lemma_id("determine"),
            },
            RelationEdgeRowV1 {
                version: LEXICON_SCHEMA_V1,
                from: RelFromId::Lemma(derive_lemma_id("determine")),
                rel_type_id: REL_SYNONYM,
                to_lemma_id: derive_lemma_id("decide"),
            },
            RelationEdgeRowV1 {
                version: LEXICON_SCHEMA_V1,
                from: RelFromId::Lemma(derive_lemma_id("logic")),
                rel_type_id: REL_SYNONYM,
                to_lemma_id: derive_lemma_id("reasoning"),
            },
        ];

        LexiconSegmentV1::build_from_rows(&lemmas, &senses, &rels, &[]).unwrap()
    }

    fn build_view() -> LexiconExpandLookupV1 {
        let dir = tmp_dir("lexicon_neighborhoods_build_view");
        let store = FsArtifactStore::new(&dir).unwrap();

        let seg = build_test_segment();
        let seg_h = put_lexicon_segment_v1(&store, &seg).unwrap();

        let mut snap = LexiconSnapshotV1::new();
        snap.entries.push(LexiconSnapshotEntryV1 {
            lex_seg: seg_h,
            lemma_count: seg.lemma_id.len() as u32,
            sense_count: seg.sense_id.len() as u32,
            rel_count: seg.rel_from_tag.len() as u32,
            pron_count: seg.pron_lemma_id.len() as u32,
        });
        let snap_h = put_lexicon_snapshot_v1(&store, &snap).unwrap();

        load_lexicon_expand_lookup_v1(&store, &snap_h)
            .unwrap()
            .unwrap()
    }

    #[test]
    fn neighborhoods_are_sorted_unique_and_deterministic() {
        let view = build_view();
        let cfg = LexiconNeighborhoodCfgV1::new();
        let a = build_lexicon_cue_neighborhoods_v1(&view, &cfg);
        let b = build_lexicon_cue_neighborhoods_v1(&view, &cfg);
        assert_eq!(a, b);

        fn is_sorted_unique(xs: &[LemmaId]) -> bool {
            if xs.is_empty() {
                return true;
            }
            let mut prev = (xs[0].0).0;
            for x in &xs[1..] {
                let v = (x.0).0;
                if v <= prev {
                    return false;
                }
                prev = v;
            }
            true
        }

        assert!(is_sorted_unique(&a.planning));
        assert!(is_sorted_unique(&a.problem_solve));
        assert!(is_sorted_unique(&a.logic_puzzle));

        // Sanity: verify the synonym chain is reachable within default depth.
        let plan_id = derive_lemma_id("plan");
        let strategy_id = derive_lemma_id("strategy");
        assert!(a.planning.contains(&plan_id));
        assert!(a.planning.contains(&strategy_id));

        let diag_id = derive_lemma_id("diagnose");
        let decide_id = derive_lemma_id("decide");
        assert!(a.problem_solve.contains(&diag_id));
        assert!(a.problem_solve.contains(&decide_id));

        let logic_id = derive_lemma_id("logic");
        let reasoning_id = derive_lemma_id("reasoning");
        assert!(a.logic_puzzle.contains(&logic_id));
        assert!(a.logic_puzzle.contains(&reasoning_id));
    }

    #[test]
    fn neighborhoods_respect_caps() {
        let view = build_view();
        let mut cfg = LexiconNeighborhoodCfgV1::new();
        cfg.cap_total = 1;
        let out = build_lexicon_cue_neighborhoods_v1(&view, &cfg);
        assert!(out.planning.len() <= 1);
        assert!(out.problem_solve.len() <= 1);
        assert!(out.logic_puzzle.len() <= 1);
    }

    #[test]
    fn lemma_pos_mask_lookup_finds_values() {
        let view = build_view();
        let id = derive_lemma_id("plan");
        let m = lemma_pos_mask(&view, id).unwrap();
        assert_eq!(m, POS_VERB);
        assert!(pos_is_content(m));

        let missing = LemmaId(Id64(0));
        assert!(lemma_pos_mask(&view, missing).is_none());
    }
}
