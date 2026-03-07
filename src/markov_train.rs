// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Offline Markov model training + hint derivation.
//!
//! This module provides deterministic builders for:
//! - Markov corpus hash derivation
//! - MarkovModelV1 training from choice token streams
//! - MarkovHintsV1 derivation from a trained model and a context stream
//!
//! Training is rules-first and bounded:
//! - fixed Markov order (n-gram length)
//! - per-state next pruning
//! - global state pruning
//! - canonical sorting for replay stability

use crate::frame::{derive_id64, Id64};
use crate::hash::{blake3_hash, Hash32};
use crate::markov_hints::{MarkovChoiceV1, MarkovHintsFlagsV1, MarkovHintsV1, MH_FLAG_USED_PPM, MARKOV_HINTS_V1_VERSION};
use crate::markov_model::{
    MarkovModelV1, MarkovNextV1, MarkovStateV1, MarkovTokenV1, MARKOV_MODEL_V1_MAX_NEXT_PER_STATE, MARKOV_MODEL_V1_MAX_ORDER_N,
    MARKOV_MODEL_V1_MAX_STATES, MARKOV_MODEL_V1_VERSION,
};
use crate::markov_trace::MarkovTraceV1;
use core::cmp::Ordering;
use std::collections::BTreeMap;

/// Training configuration for MarkovModelV1.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MarkovTrainCfgV1 {
    /// Maximum order (n-gram length). Context length is (order_n_max - 1).
    pub order_n_max: u8,
    /// Maximum next tokens per state (hard pruning cap).
    pub max_next_per_state: u8,
    /// Maximum number of states to retain (hard pruning cap).
    pub max_states: u32,
}

impl MarkovTrainCfgV1 {
    /// Validate config invariants against v1 hard limits.
    pub fn validate(&self) -> Result<(), MarkovTrainError> {
        if self.order_n_max == 0 || self.order_n_max > MARKOV_MODEL_V1_MAX_ORDER_N {
            return Err(MarkovTrainError::BadOrder);
        }
        if self.max_next_per_state == 0 || (self.max_next_per_state as usize) > MARKOV_MODEL_V1_MAX_NEXT_PER_STATE {
            return Err(MarkovTrainError::BadMaxNext);
        }
        if self.max_states == 0 || (self.max_states as usize) > MARKOV_MODEL_V1_MAX_STATES {
            return Err(MarkovTrainError::BadMaxStates);
        }
        Ok(())
    }
}

/// Errors returned by Markov training helpers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MarkovTrainError {
    /// order_n_max is invalid.
    BadOrder,
    /// max_next_per_state is invalid.
    BadMaxNext,
    /// max_states is invalid.
    BadMaxStates,
    /// Model validation failed.
    BadModel,
}

impl core::fmt::Display for MarkovTrainError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            MarkovTrainError::BadOrder => f.write_str("bad markov order"),
            MarkovTrainError::BadMaxNext => f.write_str("bad max_next_per_state"),
            MarkovTrainError::BadMaxStates => f.write_str("bad max_states"),
            MarkovTrainError::BadModel => f.write_str("bad trained markov model"),
        }
    }
}

fn cmp_token_slice(a: &[MarkovTokenV1], b: &[MarkovTokenV1]) -> Ordering {
    let n = core::cmp::min(a.len(), b.len());
    for i in 0..n {
        match a[i].cmp(&b[i]) {
            Ordering::Equal => {}
            o => return o,
        }
    }
    a.len().cmp(&b.len())
}

fn cmp_ctx_canon(a: &[MarkovTokenV1], b: &[MarkovTokenV1]) -> Ordering {
    // Canonical state order matches MarkovModelV1:
    // - context length desc
    // - context tokens asc
    match b.len().cmp(&a.len()) {
        Ordering::Equal => {}
        o => return o,
    }
    cmp_token_slice(a, b)
}

fn cmp_next_canon(a: &MarkovNextV1, b: &MarkovNextV1) -> Ordering {
    // Canonical next order matches MarkovModelV1:
    // - count desc
    // - token asc
    match b.count.cmp(&a.count) {
        Ordering::Equal => {}
        o => return o,
    }
    a.token.cmp(&b.token)
}

fn encode_cfg_bytes(cfg: &MarkovTrainCfgV1) -> [u8; 6] {
    let mut out = [0u8; 6];
    out[0] = cfg.order_n_max;
    out[1] = cfg.max_next_per_state;
    out[2..6].copy_from_slice(&(cfg.max_states as u32).to_le_bytes());
    out
}

/// Compute the corpus hash used for a Markov model.
///
/// Domain separation:
/// blake3("markov_corpus_v1" || cfg_bytes || sorted_unique(input_hashes))
pub fn markov_corpus_hash_v1(cfg: &MarkovTrainCfgV1, input_hashes: &[Hash32]) -> Result<Hash32, MarkovTrainError> {
    cfg.validate()?;
    let mut hs: Vec<Hash32> = input_hashes.to_vec();
    hs.sort();
    hs.dedup();

    let mut buf: Vec<u8> = Vec::with_capacity(14 + 6 + (hs.len() * 32));
    buf.extend_from_slice(b"markov_corpus_v1");
    buf.extend_from_slice(&encode_cfg_bytes(cfg));
    for h in hs {
        buf.extend_from_slice(&h);
    }
    Ok(blake3_hash(&buf))
}

/// Compute the context hash used by MarkovHintsV1.
///
/// Domain separation:
/// blake3("markov_context_v1" || encode(tokens)).
pub fn markov_context_hash_v1(tokens: &[MarkovTokenV1]) -> Hash32 {
    let mut buf: Vec<u8> = Vec::with_capacity(15 + (tokens.len() * 9));
    buf.extend_from_slice(b"markov_context_v1");
    for t in tokens {
        buf.push(t.kind as u8);
        buf.extend_from_slice(&t.choice_id.0.to_le_bytes());
    }
    blake3_hash(&buf)
}

/// Derive a stable state id for a context token slice.
pub fn markov_state_id_v1(tokens: &[MarkovTokenV1]) -> Id64 {
    let mut buf: Vec<u8> = Vec::with_capacity(tokens.len() * 9);
    for t in tokens {
        buf.push(t.kind as u8);
        buf.extend_from_slice(&t.choice_id.0.to_le_bytes());
    }
    derive_id64(b"markov_state_v1", &buf)
}

/// Deterministic Markov trainer (v1).
pub struct MarkovTrainerV1 {
    cfg: MarkovTrainCfgV1,
    // context -> (next token -> count)
    counts: BTreeMap<Vec<MarkovTokenV1>, BTreeMap<MarkovTokenV1, u32>>,
    total_transitions: u64,
}

impl MarkovTrainerV1 {
    /// Construct a trainer.
    pub fn new(cfg: MarkovTrainCfgV1) -> Result<Self, MarkovTrainError> {
        cfg.validate()?;
        Ok(MarkovTrainerV1 {
            cfg,
            counts: BTreeMap::new(),
            total_transitions: 0,
        })
    }

    /// Observe a single token stream.
    pub fn observe_stream(&mut self, tokens: &[MarkovTokenV1]) {
        if tokens.is_empty() {
            return;
        }
        self.total_transitions = self.total_transitions.saturating_add(tokens.len() as u64);

        let order = self.cfg.order_n_max as usize;
        for i in 0..tokens.len() {
            // Context length k in 0..=min(i, order-1).
            let k_max = core::cmp::min(i, order.saturating_sub(1));
            for k in 0..=k_max {
                let start = i - k;
                let ctx: Vec<MarkovTokenV1> = tokens[start..i].to_vec();
                let nxt = tokens[i];
                let entry = self.counts.entry(ctx).or_insert_with(BTreeMap::new);
                let v = entry.entry(nxt).or_insert(0);
                *v = v.saturating_add(1);
            }
        }
    }

    /// Observe a MarkovTraceV1.
    pub fn observe_trace(&mut self, trace: &MarkovTraceV1) {
        self.observe_stream(&trace.tokens);
    }

    /// Finish training and build a canonical MarkovModelV1.
    pub fn build_model(mut self, corpus_hash: Hash32) -> Result<MarkovModelV1, MarkovTrainError> {
        // Convert internal maps to canonical vectors, apply next pruning.
        let mut states: Vec<MarkovStateV1> = Vec::with_capacity(self.counts.len());

        let counts = core::mem::take(&mut self.counts);
        for (ctx, next_map) in counts {
            let mut next: Vec<MarkovNextV1> = Vec::with_capacity(next_map.len());
            for (tok, ct) in next_map {
                next.push(MarkovNextV1 { token: tok, count: ct });
            }
            next.sort_by(cmp_next_canon);
            let cap = self.cfg.max_next_per_state as usize;
            if next.len() > cap {
                next.truncate(cap);
            }
            states.push(MarkovStateV1 {
                context: ctx,
                escape_count: 0,
                next,
            });
        }

        // Global state pruning.
        let max_states = self.cfg.max_states as usize;
        if states.len() > max_states {
            // Weight = sum(next.count) after next pruning.
            states.sort_by(|a, b| {
                let wa: u64 = a.next.iter().map(|n| n.count as u64).sum();
                let wb: u64 = b.next.iter().map(|n| n.count as u64).sum();
                match wb.cmp(&wa) {
                    Ordering::Equal => {}
                    o => return o,
                }
                // Prefer longer contexts for pruning ties.
                match b.context.len().cmp(&a.context.len()) {
                    Ordering::Equal => {}
                    o => return o,
                }
                cmp_token_slice(&a.context, &b.context)
            });
            states.truncate(max_states);
        }

        // Final canonical ordering.
        states.sort_by(|a, b| cmp_ctx_canon(&a.context, &b.context));

        let model = MarkovModelV1 {
            version: MARKOV_MODEL_V1_VERSION,
            order_n_max: self.cfg.order_n_max,
            max_next_per_state: self.cfg.max_next_per_state,
            corpus_hash,
            total_transitions: self.total_transitions,
            states,
        };

        if model.validate().is_err() {
            return Err(MarkovTrainError::BadModel);
        }
        Ok(model)
    }
}

fn find_state<'a>(model: &'a MarkovModelV1, ctx: &[MarkovTokenV1]) -> Option<&'a MarkovStateV1> {
    if model.states.is_empty() {
        return None;
    }
    let res = model.states.binary_search_by(|s| cmp_ctx_canon(&s.context, ctx));
    match res {
        Ok(ix) => Some(&model.states[ix]),
        Err(_) => None,
    }
}

/// Derive MarkovHintsV1 for a given context.
pub fn derive_markov_hints_v1(
    query_id: Hash32,
    mut flags: MarkovHintsFlagsV1,
    model_hash: Hash32,
    model: &MarkovModelV1,
    context_tokens: &[MarkovTokenV1],
    max_choices: usize,
) -> MarkovHintsV1 {
    let mut choices: Vec<MarkovChoiceV1> = Vec::new();
    let max_out = core::cmp::min(max_choices, crate::markov_hints::MARKOV_HINTS_V1_MAX_CHOICES);

    let mut used_ctx_len: usize = 0;

    let max_ctx = core::cmp::min(
        context_tokens.len(),
        (model.order_n_max as usize).saturating_sub(1),
    );
    for ctx_len in (0..=max_ctx).rev() {
        let start = context_tokens.len().saturating_sub(ctx_len);
        let ctx = &context_tokens[start..context_tokens.len()];
        if let Some(st) = find_state(model, ctx) {
            used_ctx_len = ctx_len;
            // st.next is already canonical: count desc, token asc.
            for n in &st.next {
                if choices.len() >= max_out {
                    break;
                }
                choices.push(MarkovChoiceV1::new(n.token.kind, n.token.choice_id, n.count as i64, 0));
            }
            break;
        }
    }

    if !choices.is_empty() {
        flags |= MH_FLAG_USED_PPM;
    }

    let ctx_hash = markov_context_hash_v1(context_tokens);
    let state_id = markov_state_id_v1(&context_tokens[context_tokens.len().saturating_sub(used_ctx_len)..]);
    let order_n = core::cmp::max(1, used_ctx_len + 1) as u8;

    MarkovHintsV1 {
        version: MARKOV_HINTS_V1_VERSION,
        query_id,
        flags,
        order_n,
        state_id,
        model_hash,
        context_hash: ctx_hash,
        choices,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::blake3_hash;
    use crate::markov_hints::MarkovChoiceKindV1;
    use crate::markov_model::MarkovTokenV1;

    fn tok(k: MarkovChoiceKindV1, id: u64) -> MarkovTokenV1 {
        MarkovTokenV1::new(k, Id64(id))
    }

    #[test]
    fn corpus_hash_sorts_and_dedups() {
        let cfg = MarkovTrainCfgV1 {
            order_n_max: 3,
            max_next_per_state: 8,
            max_states: 128,
        };
        let a = blake3_hash(b"a");
        let b = blake3_hash(b"b");
        let h1 = markov_corpus_hash_v1(&cfg, &[b, a, a]).unwrap();
        let h2 = markov_corpus_hash_v1(&cfg, &[a, b]).unwrap();
        assert_eq!(h1, h2);
    }

    #[test]
    fn train_builds_canonical_model() {
        let cfg = MarkovTrainCfgV1 {
            order_n_max: 3,
            max_next_per_state: 4,
            max_states: 64,
        };
        let corpus = markov_corpus_hash_v1(&cfg, &[blake3_hash(b"x")]).unwrap();
        let mut tr = MarkovTrainerV1::new(cfg).unwrap();
        tr.observe_stream(&[tok(MarkovChoiceKindV1::Opener, 1), tok(MarkovChoiceKindV1::Transition, 2), tok(MarkovChoiceKindV1::Closer, 3)]);
        tr.observe_stream(&[tok(MarkovChoiceKindV1::Opener, 1), tok(MarkovChoiceKindV1::Closer, 3)]);
        let model = tr.build_model(corpus).unwrap();
        assert!(model.validate().is_ok());
        assert!(model.is_canonical());
        assert_eq!(model.order_n_max, 3);
        assert_eq!(model.max_next_per_state, 4);
        assert_eq!(model.corpus_hash, corpus);
        assert!(model.total_transitions >= 5);
    }

    #[test]
    fn derive_hints_is_canonical_and_bounded() {
        let cfg = MarkovTrainCfgV1 {
            order_n_max: 2,
            max_next_per_state: 8,
            max_states: 64,
        };
        let corpus = markov_corpus_hash_v1(&cfg, &[]).unwrap();
        let mut tr = MarkovTrainerV1::new(cfg).unwrap();
        tr.observe_stream(&[tok(MarkovChoiceKindV1::Opener, 1), tok(MarkovChoiceKindV1::Closer, 9)]);
        tr.observe_stream(&[tok(MarkovChoiceKindV1::Opener, 1), tok(MarkovChoiceKindV1::Closer, 8)]);
        let model = tr.build_model(corpus).unwrap();
        let model_hash = blake3_hash(b"model");
        let hints = derive_markov_hints_v1(blake3_hash(b"q"), 0, model_hash, &model, &[tok(MarkovChoiceKindV1::Opener, 1)], 3);
        assert!(hints.validate().is_ok());
        assert!(hints.is_canonical());
        assert!(hints.choices.len() <= 3);
        if !hints.choices.is_empty() {
            assert!((hints.flags & MH_FLAG_USED_PPM) != 0);
        }
    }
}
