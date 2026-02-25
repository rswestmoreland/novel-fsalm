// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Bridge expansion builder.
//!
//! This module implements the deterministic selection algorithm described in
//! docs/BRIDGE_EXPANSION.md, using:
//! - [`ExpansionBudgetV1`](crate::expansion_budget::ExpansionBudgetV1)
//! - [`ExpandedQfvV1`](crate::expanded_qfv::ExpandedQfvV1)
//!
//! is still not wired into retrieval. It is a pure builder:
//! - callers provide base anchors and candidate expansion items
//! - the builder applies multipliers, dedup, stable ranking, and budget fill
//! - the output is a canonical ExpandedQfvV1

use crate::expanded_qfv::{ExpandedQfvItemV1, ExpandedQfvV1, EXPANDED_QFV_V1_VERSION};
use crate::expansion_budget::{ExpansionBudgetV1, ExpansionKindBudgetV1, ExpansionKindV1};
use crate::frame::Id64;

/// Base feature key used to classify candidate origins.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct BaseFeatureV1 {
    /// Channel kind for the base feature.
    pub kind: ExpansionKindV1,
    /// Base feature id (domain depends on kind).
    pub id: Id64,
}

impl BaseFeatureV1 {
    /// Construct a new base feature.
    pub fn new(kind: ExpansionKindV1, id: Id64) -> Self {
        BaseFeatureV1 { kind, id }
    }
}

/// Errors produced by the expansion builder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpansionBuildError {
    /// The provided budget is invalid.
    BadBudget,
    /// A base feature appeared more than once across required/optional bases.
    DuplicateBase,
    /// The builder produced a non-canonical output (should not happen).
    OutputNotCanonical,
}

impl core::fmt::Display for ExpansionBuildError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ExpansionBuildError::BadBudget => f.write_str("bad expansion budget"),
            ExpansionBuildError::DuplicateBase => f.write_str("duplicate base feature"),
            ExpansionBuildError::OutputNotCanonical => f.write_str("builder output not canonical"),
        }
    }
}

impl std::error::Error for ExpansionBuildError {}

fn kind_index(kind: ExpansionKindV1) -> Option<usize> {
    // v1: kinds are 1..=4
    let k = kind as u8;
    if (1..=4).contains(&k) {
        Some((k - 1) as usize)
    } else {
        None
    }
}

fn clamp_u16_from_u32(x: u32) -> u16 {
    if x > 65535 {
        65535
    } else {
        x as u16
    }
}

fn effective_weight(weight: u16, kb: &ExpansionKindBudgetV1) -> u16 {
    // effective = clamp_u16((weight * mul_q16) >> 16)
    let prod = (weight as u32).saturating_mul(kb.weight_mul_q16);
    clamp_u16_from_u32(prod >> 16)
}

fn base_key_u72(kind: ExpansionKindV1, id: Id64) -> (u8, u64) {
    (kind as u8, id.0)
}

fn item_key_u72(kind: ExpansionKindV1, id: Id64) -> (u8, u64) {
    (kind as u8, id.0)
}

fn cmp_rank(a: &ExpandedQfvItemV1, b: &ExpandedQfvItemV1) -> core::cmp::Ordering {
    // Canonical rank used for selection:
    // - weight desc
    // - kind asc
    // - id asc
    match b.weight.cmp(&a.weight) {
        core::cmp::Ordering::Equal => {}
        o => return o,
    }
    match (a.kind as u8).cmp(&(b.kind as u8)) {
        core::cmp::Ordering::Equal => {}
        o => return o,
    }
    (a.id.0).cmp(&(b.id.0))
}

fn find_kind_budget<'a>(
    budget: &'a ExpansionBudgetV1,
    kind: ExpansionKindV1,
) -> Option<&'a ExpansionKindBudgetV1> {
    // budget.kinds is canonical (sorted by kind asc)
    budget
        .kinds
        .binary_search_by_key(&(kind as u8), |kb| kb.kind as u8)
        .ok()
        .map(|ix| &budget.kinds[ix])
}

#[derive(Clone, Debug)]
struct BaseCountsV1 {
    key: (u8, u64),
    total: u16,
    per_kind: [u16; 4],
}

fn base_counts_get_mut<'a>(xs: &'a mut Vec<BaseCountsV1>, key: (u8, u64)) -> &'a mut BaseCountsV1 {
    match xs.binary_search_by_key(&key, |bc| bc.key) {
        Ok(ix) => &mut xs[ix],
        Err(pos) => {
            xs.insert(
                pos,
                BaseCountsV1 {
                    key,
                    total: 0,
                    per_kind: [0, 0, 0, 0],
                },
            );
            &mut xs[pos]
        }
    }
}

fn base_key_is_required(required_bases: &[(u8, u64)], base_key: (u8, u64)) -> bool {
    required_bases.binary_search(&base_key).is_ok()
}

fn dedup_candidates(mut xs: Vec<ExpandedQfvItemV1>) -> Vec<ExpandedQfvItemV1> {
    // Dedup by (kind,id): keep the item with the highest weight.
    // Deterministic tie-break within same (kind,id,weight):
    // origin_base_kind asc, origin_base_id asc, origin_rule_id asc.
    xs.sort_by(|a, b| {
        let ka = item_key_u72(a.kind, a.id);
        let kb = item_key_u72(b.kind, b.id);
        match ka.cmp(&kb) {
            core::cmp::Ordering::Equal => {}
            o => return o,
        }
        match b.weight.cmp(&a.weight) {
            core::cmp::Ordering::Equal => {}
            o => return o,
        }
        match (a.origin_base_kind as u8).cmp(&(b.origin_base_kind as u8)) {
            core::cmp::Ordering::Equal => {}
            o => return o,
        }
        match (a.origin_base_id.0).cmp(&(b.origin_base_id.0)) {
            core::cmp::Ordering::Equal => {}
            o => return o,
        }
        (a.origin_rule_id).cmp(&(b.origin_rule_id))
    });

    let mut out: Vec<ExpandedQfvItemV1> = Vec::new();
    for it in xs {
        if let Some(last) = out.last() {
            if last.kind == it.kind && last.id == it.id {
                // last already has >= weight due to sort
                continue;
            }
        }
        out.push(it);
    }
    out
}

fn apply_budget_filters(
    candidates: Vec<ExpandedQfvItemV1>,
    budget: &ExpansionBudgetV1,
) -> Vec<ExpandedQfvItemV1> {
    let mut out: Vec<ExpandedQfvItemV1> = Vec::new();
    for mut it in candidates {
        let kb = match find_kind_budget(budget, it.kind) {
            Some(kb) => kb,
            None => continue,
        };
        if kb.max_total == 0 {
            continue;
        }
        let w = effective_weight(it.weight, kb);
        if w < kb.weight_floor {
            continue;
        }
        it.weight = w;
        out.push(it);
    }
    out
}

fn select_from_pool(
    pool: &[ExpandedQfvItemV1],
    budget: &ExpansionBudgetV1,
    out: &mut Vec<ExpandedQfvItemV1>,
    base_counts: &mut Vec<BaseCountsV1>,
    kind_counts: &mut [u16; 4],
    total_selected: &mut u16,
    required_selected: &mut u16,
    is_required_pool: bool,
) {
    for it in pool {
        if *total_selected >= budget.max_expansions_total {
            break;
        }
        if is_required_pool && *required_selected >= budget.max_required_total {
            break;
        }

        let kb = match find_kind_budget(budget, it.kind) {
            Some(kb) => kb,
            None => continue,
        };
        let kix = match kind_index(it.kind) {
            Some(ix) => ix,
            None => continue,
        };
        if kind_counts[kix] >= kb.max_total {
            continue;
        }

        let base_key = base_key_u72(it.origin_base_kind, it.origin_base_id);
        let bc = base_counts_get_mut(base_counts, base_key);
        if bc.total >= (budget.max_expansions_per_base as u16) {
            continue;
        }
        if bc.per_kind[kix] >= (kb.max_per_base as u16) {
            continue;
        }

        out.push(*it);
        bc.total = bc.total.saturating_add(1);
        bc.per_kind[kix] = bc.per_kind[kix].saturating_add(1);
        kind_counts[kix] = kind_counts[kix].saturating_add(1);
        *total_selected = total_selected.saturating_add(1);
        if is_required_pool {
            *required_selected = required_selected.saturating_add(1);
        }
    }
}

/// Build an ExpandedQfvV1 from base anchors and candidate expansion items.
///
/// Inputs:
/// - `tie_control_id`: stable tie-break id for later pipeline stages
/// - `required_bases`: base anchors for precision (required)
/// - `optional_bases`: base anchors for recall boosts (optional)
/// - `candidates`: expansion candidates (may contain duplicates)
/// - `budget`: global and per-kind caps + weight multipliers
///
/// Output:
/// - A canonical [`ExpandedQfvV1`], with required expansions selected first.
pub fn build_expanded_qfv_v1(
    tie_control_id: Id64,
    mut required_bases: Vec<BaseFeatureV1>,
    mut optional_bases: Vec<BaseFeatureV1>,
    candidates: Vec<ExpandedQfvItemV1>,
    budget: &ExpansionBudgetV1,
) -> Result<ExpandedQfvV1, ExpansionBuildError> {
    if budget.validate().is_err() {
        return Err(ExpansionBuildError::BadBudget);
    }

    // Canonicalize base lists and ensure uniqueness across pools.
    required_bases.sort();
    required_bases.dedup();
    optional_bases.sort();
    optional_bases.dedup();

    // Reject duplicates across pools.
    let mut seen: Vec<(u8, u64)> = Vec::with_capacity(required_bases.len() + optional_bases.len());
    for b in required_bases.iter().chain(optional_bases.iter()) {
        let key = base_key_u72(b.kind, b.id);
        match seen.binary_search(&key) {
            Ok(_) => return Err(ExpansionBuildError::DuplicateBase),
            Err(pos) => seen.insert(pos, key),
        }
    }

    let required_keys: Vec<(u8, u64)> = required_bases
        .iter()
        .map(|b| base_key_u72(b.kind, b.id))
        .collect();

    // Apply multipliers/floors and drop disabled kinds.
    let filtered = apply_budget_filters(candidates, budget);
    // Dedup by (kind,id), keeping max weight.
    let deduped = dedup_candidates(filtered);

    // Partition by origin base requiredness.
    let mut req_pool: Vec<ExpandedQfvItemV1> = Vec::new();
    let mut opt_pool: Vec<ExpandedQfvItemV1> = Vec::new();
    for it in deduped {
        let bkey = base_key_u72(it.origin_base_kind, it.origin_base_id);
        if base_key_is_required(&required_keys, bkey) {
            req_pool.push(it);
        } else {
            opt_pool.push(it);
        }
    }

    // Rank pools.
    req_pool.sort_by(cmp_rank);
    opt_pool.sort_by(cmp_rank);

    // Fill budgets.
    let mut required_out: Vec<ExpandedQfvItemV1> = Vec::new();
    let mut optional_out: Vec<ExpandedQfvItemV1> = Vec::new();

    let mut base_counts: Vec<BaseCountsV1> = Vec::new();
    let mut kind_counts: [u16; 4] = [0, 0, 0, 0];
    let mut total_selected: u16 = 0;
    let mut required_selected: u16 = 0;

    select_from_pool(
        &req_pool,
        budget,
        &mut required_out,
        &mut base_counts,
        &mut kind_counts,
        &mut total_selected,
        &mut required_selected,
        true,
    );
    select_from_pool(
        &opt_pool,
        budget,
        &mut optional_out,
        &mut base_counts,
        &mut kind_counts,
        &mut total_selected,
        &mut required_selected,
        false,
    );

    let out = ExpandedQfvV1 {
        version: EXPANDED_QFV_V1_VERSION,
        tie_control_id,
        required: required_out,
        optional: optional_out,
    };
    if out.validate().is_err() {
        return Err(ExpansionBuildError::OutputNotCanonical);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(x: u64) -> Id64 {
        Id64(x)
    }

    fn item(
        kind: ExpansionKindV1,
        idv: u64,
        w: u16,
        ob_kind: ExpansionKindV1,
        ob_idv: u64,
        rule: u16,
    ) -> ExpandedQfvItemV1 {
        ExpandedQfvItemV1::new(kind, id(idv), w, ob_kind, id(ob_idv), rule)
    }

    #[test]
    fn builder_dedup_and_required_partition_and_caps() {
        // Budget:
        // - total 4
        // - required 2
        // - per base 2
        // - Lex max_total 2, per base 2
        // - Meta max_total 2, per base 1
        let mut b = ExpansionBudgetV1::default_v1();
        b.max_expansions_total = 4;
        b.max_required_total = 2;
        b.max_expansions_per_base = 2;
        b.kinds = vec![
            ExpansionKindBudgetV1::new(ExpansionKindV1::Lex, 2, 2, 65536, 0),
            ExpansionKindBudgetV1::new(ExpansionKindV1::Meta, 2, 1, 65536, 0),
            ExpansionKindBudgetV1::new(ExpansionKindV1::Ent, 0, 0, 65536, 0),
            ExpansionKindBudgetV1::new(ExpansionKindV1::Graph, 0, 0, 65536, 0),
        ];
        b.canonicalize_in_place();
        b.validate().unwrap();

        let required_bases = vec![BaseFeatureV1::new(ExpansionKindV1::Lex, id(10))];
        let optional_bases = vec![BaseFeatureV1::new(ExpansionKindV1::Lex, id(20))];

        // Candidates include duplicates for (Lex, 100) with different weights.
        // Also include Meta candidates from same base; per-base per-kind cap for Meta is 1.
        let candidates = vec![
            item(
                ExpansionKindV1::Lex,
                100,
                30000,
                ExpansionKindV1::Lex,
                10,
                1,
            ),
            item(
                ExpansionKindV1::Lex,
                100,
                40000,
                ExpansionKindV1::Lex,
                10,
                2,
            ), // should win
            item(
                ExpansionKindV1::Lex,
                101,
                35000,
                ExpansionKindV1::Lex,
                10,
                3,
            ),
            item(
                ExpansionKindV1::Meta,
                200,
                20000,
                ExpansionKindV1::Lex,
                10,
                4,
            ),
            item(
                ExpansionKindV1::Meta,
                201,
                19000,
                ExpansionKindV1::Lex,
                10,
                5,
            ), // blocked by Meta per-base cap
            item(
                ExpansionKindV1::Lex,
                102,
                10000,
                ExpansionKindV1::Lex,
                20,
                6,
            ), // optional base
            item(
                ExpansionKindV1::Meta,
                202,
                25000,
                ExpansionKindV1::Lex,
                20,
                7,
            ),
        ];

        let out =
            build_expanded_qfv_v1(id(999), required_bases, optional_bases, candidates, &b).unwrap();
        assert_eq!(out.version, EXPANDED_QFV_V1_VERSION);
        assert_eq!(out.tie_control_id, id(999));

        // Required pool should take 2 (required cap): Lex(100 w=40000), Lex(101 w=35000).
        assert_eq!(out.required.len(), 2);
        assert_eq!(out.required[0].kind, ExpansionKindV1::Lex);
        assert_eq!(out.required[0].id, id(100));
        assert_eq!(out.required[0].weight, 40000);
        assert_eq!(out.required[1].id, id(101));

        // Optional pool fills remaining up to total 4, respecting Lex max_total=2.
        // Lex already used 2 in required, so no more Lex can be selected.
        // That means only Meta(202) from optional base should be selected.
        assert_eq!(out.optional.len(), 1);
        assert_eq!(out.optional[0].kind, ExpansionKindV1::Meta);
        assert_eq!(out.optional[0].id, id(202));
        assert_eq!(out.required.len() + out.optional.len(), 3);
        out.validate().unwrap();
    }

    #[test]
    fn builder_weight_multiplier_and_floor() {
        // Meta: multiplier 0.5, floor 15000
        let mut b = ExpansionBudgetV1::default_v1();
        b.max_expansions_total = 8;
        b.max_required_total = 8;
        b.max_expansions_per_base = 8;
        b.kinds = vec![
            ExpansionKindBudgetV1::new(ExpansionKindV1::Lex, 0, 0, 65536, 0),
            ExpansionKindBudgetV1::new(ExpansionKindV1::Meta, 8, 8, 32768, 15000),
            ExpansionKindBudgetV1::new(ExpansionKindV1::Ent, 0, 0, 65536, 0),
            ExpansionKindBudgetV1::new(ExpansionKindV1::Graph, 0, 0, 65536, 0),
        ];
        b.canonicalize_in_place();
        b.validate().unwrap();

        let required_bases = vec![BaseFeatureV1::new(ExpansionKindV1::Meta, id(1))];
        let optional_bases = vec![];

        // After multiplier: 40000 -> 20000 (kept), 20000 -> 10000 (dropped by floor)
        let candidates = vec![
            item(
                ExpansionKindV1::Meta,
                10,
                40000,
                ExpansionKindV1::Meta,
                1,
                1,
            ),
            item(
                ExpansionKindV1::Meta,
                11,
                20000,
                ExpansionKindV1::Meta,
                1,
                2,
            ),
        ];

        let out =
            build_expanded_qfv_v1(id(7), required_bases, optional_bases, candidates, &b).unwrap();
        assert_eq!(out.required.len(), 1);
        assert_eq!(out.required[0].id, id(10));
        assert_eq!(out.required[0].weight, 20000);
        assert!(out.optional.is_empty());
    }
}
