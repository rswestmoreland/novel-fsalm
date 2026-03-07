// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Deterministic workload generation.
//!
//! This module provides a small, deterministic corpus generator used for:
//! - scale demos
//! - regression testing of retrieval behavior under controlled distributions
//!
//! Design goals:
//! - Deterministic across OS and runs (no system RNG).
//! - ASCII-only text output.
//! - Minimal allocations where reasonable.

use crate::hash::{blake3_hash, Hash32};

/// Workload generator version.
pub const WORKLOAD_GEN_V1_VERSION: u32 = 1;

/// Configuration for the deterministic workload generator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkloadCfgV1 {
    /// Schema version. Must equal `WORKLOAD_GEN_V1_VERSION`.
    pub version: u32,
    /// Seed controlling the entire generated workload.
    pub seed: u64,
    /// Number of documents to generate.
    pub doc_count: u32,
    /// Minimum number of tokens per document.
    pub min_tokens_per_doc: u16,
    /// Maximum number of tokens per document.
    pub max_tokens_per_doc: u16,
    /// Size of the token vocabulary.
    ///
    /// Tokens are rendered as `wNNNNNN` (6 digits). Max allowed is 1_000_000.
    pub vocab_size: u32,
    /// Number of synthetic queries to generate.
    pub query_count: u32,
    /// Number of tokens per query.
    pub query_tokens: u16,
    /// Whether to emit a deterministic "tie pair" (two identical docs).
    pub include_tie_pair: u8,
}

impl Default for WorkloadCfgV1 {
    fn default() -> Self {
        Self {
            version: WORKLOAD_GEN_V1_VERSION,
            seed: 1,
            doc_count: 64,
            min_tokens_per_doc: 16,
            max_tokens_per_doc: 48,
            vocab_size: 4096,
            query_count: 32,
            query_tokens: 3,
            include_tie_pair: 1,
        }
    }
}

/// Configuration validation errors.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkloadCfgError {
    /// Version mismatch.
    VersionMismatch,
    /// doc_count is zero.
    DocCountZero,
    /// query_count is zero.
    QueryCountZero,
    /// min_tokens_per_doc is zero.
    MinTokensZero,
    /// max_tokens_per_doc is less than min_tokens_per_doc.
    MaxTokensLessThanMin,
    /// vocab_size is zero.
    VocabSizeZero,
    /// vocab_size exceeds the maximum supported by `wNNNNNN` rendering.
    VocabSizeTooLarge,
    /// query_tokens is zero.
    QueryTokensZero,
    /// include_tie_pair must be 0 or 1.
    BadTiePairFlag,
}

impl core::fmt::Display for WorkloadCfgError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            WorkloadCfgError::VersionMismatch => f.write_str("workload cfg version mismatch"),
            WorkloadCfgError::DocCountZero => f.write_str("doc_count must be > 0"),
            WorkloadCfgError::QueryCountZero => f.write_str("query_count must be > 0"),
            WorkloadCfgError::MinTokensZero => f.write_str("min_tokens_per_doc must be > 0"),
            WorkloadCfgError::MaxTokensLessThanMin => {
                f.write_str("max_tokens_per_doc must be >= min_tokens_per_doc")
            }
            WorkloadCfgError::VocabSizeZero => f.write_str("vocab_size must be > 0"),
            WorkloadCfgError::VocabSizeTooLarge => f.write_str("vocab_size must be <= 1_000_000"),
            WorkloadCfgError::QueryTokensZero => f.write_str("query_tokens must be > 0"),
            WorkloadCfgError::BadTiePairFlag => f.write_str("include_tie_pair must be 0 or 1"),
        }
    }
}

impl WorkloadCfgV1 {
    /// Validate config invariants.
    pub fn validate(&self) -> Result<(), WorkloadCfgError> {
        if self.version != WORKLOAD_GEN_V1_VERSION {
            return Err(WorkloadCfgError::VersionMismatch);
        }
        if self.doc_count == 0 {
            return Err(WorkloadCfgError::DocCountZero);
        }
        if self.query_count == 0 {
            return Err(WorkloadCfgError::QueryCountZero);
        }
        if self.min_tokens_per_doc == 0 {
            return Err(WorkloadCfgError::MinTokensZero);
        }
        if self.max_tokens_per_doc < self.min_tokens_per_doc {
            return Err(WorkloadCfgError::MaxTokensLessThanMin);
        }
        if self.vocab_size == 0 {
            return Err(WorkloadCfgError::VocabSizeZero);
        }
        if self.vocab_size > 1_000_000 {
            return Err(WorkloadCfgError::VocabSizeTooLarge);
        }
        if self.query_tokens == 0 {
            return Err(WorkloadCfgError::QueryTokensZero);
        }
        if self.include_tie_pair > 1 {
            return Err(WorkloadCfgError::BadTiePairFlag);
        }
        Ok(())
    }
}

/// A generated synthetic document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkloadDocV1 {
    /// Logical document id.
    pub doc_id: u32,
    /// Document text (ASCII tokens separated by spaces).
    pub text: String,
}

/// A generated synthetic query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkloadQueryV1 {
    /// Logical query id.
    pub query_id: u32,
    /// Query text.
    pub text: String,
}

/// A deterministic workload: documents + queries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkloadV1 {
    /// Generator config.
    pub cfg: WorkloadCfgV1,
    /// Documents in ascending doc_id.
    pub docs: Vec<WorkloadDocV1>,
    /// Queries in ascending query_id.
    pub queries: Vec<WorkloadQueryV1>,
}

/// Generator errors.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkloadGenError {
    /// Config invalid.
    BadCfg(WorkloadCfgError),
    /// include_tie_pair is requested but doc_count < 2.
    TiePairRequiresAtLeastTwoDocs,
}

impl core::fmt::Display for WorkloadGenError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            WorkloadGenError::BadCfg(e) => {
                f.write_str("bad workload cfg: ")?;
                core::fmt::Display::fmt(e, f)
            }
            WorkloadGenError::TiePairRequiresAtLeastTwoDocs => {
                f.write_str("include_tie_pair requires doc_count >= 2")
            }
        }
    }
}

impl From<WorkloadCfgError> for WorkloadGenError {
    fn from(value: WorkloadCfgError) -> Self {
        WorkloadGenError::BadCfg(value)
    }
}

/// Generate a deterministic workload.
pub fn generate_workload_v1(cfg: WorkloadCfgV1) -> Result<WorkloadV1, WorkloadGenError> {
    cfg.validate()?;
    if cfg.include_tie_pair == 1 && cfg.doc_count < 2 {
        return Err(WorkloadGenError::TiePairRequiresAtLeastTwoDocs);
    }

    let mut rng = SplitMix64::new(cfg.seed);

    let mut docs = Vec::with_capacity(cfg.doc_count as usize);
    for doc_id in 0..cfg.doc_count {
        let tok_len = rng.range_u32(cfg.min_tokens_per_doc as u32, cfg.max_tokens_per_doc as u32);
        let mut text = String::with_capacity(tok_len as usize * 8);
        let topic_tok = rng.next_u32() % cfg.vocab_size;
        for i in 0..tok_len {
            if i != 0 {
                text.push(' ');
            }
            let pick = rng.next_u32();
            let tok = if (pick & 3) == 0 {
                topic_tok
            } else {
                pick % cfg.vocab_size
            };
            push_tok6(&mut text, tok);
        }
        docs.push(WorkloadDocV1 { doc_id, text });
    }

    if cfg.include_tie_pair == 1 {
        // Force a deterministic tie pair: doc 0 and doc 1 identical.
        let d0 = docs[0].text.clone();
        docs[1].text = d0;
    }

    let mut queries = Vec::with_capacity(cfg.query_count as usize);
    for qid in 0..cfg.query_count {
        let mut text = String::with_capacity(cfg.query_tokens as usize * 8);
        // Make queries likely to hit by selecting tokens from a deterministic slice of doc 0.
        // If doc 0 is empty (should not happen due to validation), fall back to vocabulary.
        let base = (qid as u32) % cfg.vocab_size;
        for i in 0..(cfg.query_tokens as u32) {
            if i != 0 {
                text.push(' ');
            }
            let tok = (base.wrapping_add(i)) % cfg.vocab_size;
            push_tok6(&mut text, tok);
        }
        queries.push(WorkloadQueryV1 {
            query_id: qid,
            text,
        });
    }

    Ok(WorkloadV1 { cfg, docs, queries })
}

/// Compute a stable content hash for a workload.
pub fn workload_hash_v1(w: &WorkloadV1) -> Hash32 {
    // Stable, versioned binary encoding.
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"WORKLOADV1");
    bytes.extend_from_slice(&w.cfg.version.to_le_bytes());
    bytes.extend_from_slice(&w.cfg.seed.to_le_bytes());
    bytes.extend_from_slice(&w.cfg.doc_count.to_le_bytes());
    bytes.extend_from_slice(&w.cfg.min_tokens_per_doc.to_le_bytes());
    bytes.extend_from_slice(&w.cfg.max_tokens_per_doc.to_le_bytes());
    bytes.extend_from_slice(&w.cfg.vocab_size.to_le_bytes());
    bytes.extend_from_slice(&w.cfg.query_count.to_le_bytes());
    bytes.extend_from_slice(&w.cfg.query_tokens.to_le_bytes());
    bytes.push(w.cfg.include_tie_pair);

    for d in &w.docs {
        bytes.extend_from_slice(&d.doc_id.to_le_bytes());
        bytes.extend_from_slice(d.text.as_bytes());
        bytes.push(0);
    }
    for q in &w.queries {
        bytes.extend_from_slice(&q.query_id.to_le_bytes());
        bytes.extend_from_slice(q.text.as_bytes());
        bytes.push(0);
    }
    blake3_hash(&bytes)
}

// --- Implementation details ---

// SplitMix64: small deterministic PRNG.
// Reference: Steele et al. (public domain). This is not cryptographic.
#[derive(Clone, Copy, Debug)]
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn next_u32(&mut self) -> u32 {
        (self.next_u64() >> 32) as u32
    }

    fn range_u32(&mut self, lo: u32, hi: u32) -> u32 {
        debug_assert!(lo <= hi);
        if lo == hi {
            return lo;
        }
        let span = hi - lo + 1;
        lo + (self.next_u32() % span)
    }
}

fn push_tok6(out: &mut String, tok: u32) {
    // Render token as wNNNNNN. tok must be < 1_000_000.
    out.push('w');
    let d0 = (tok / 100_000) % 10;
    let d1 = (tok / 10_000) % 10;
    let d2 = (tok / 1_000) % 10;
    let d3 = (tok / 100) % 10;
    let d4 = (tok / 10) % 10;
    let d5 = tok % 10;
    out.push((b'0' + (d0 as u8)) as char);
    out.push((b'0' + (d1 as u8)) as char);
    out.push((b'0' + (d2 as u8)) as char);
    out.push((b'0' + (d3 as u8)) as char);
    out.push((b'0' + (d4 as u8)) as char);
    out.push((b'0' + (d5 as u8)) as char);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cfg_validate_rejects_bad_flags() {
        let mut c = WorkloadCfgV1::default();
        c.include_tie_pair = 2;
        assert_eq!(c.validate().unwrap_err(), WorkloadCfgError::BadTiePairFlag);
    }

    #[test]
    fn workload_snapshot_is_stable_for_seed() {
        let cfg = WorkloadCfgV1 {
            version: WORKLOAD_GEN_V1_VERSION,
            seed: 7,
            doc_count: 8,
            min_tokens_per_doc: 4,
            max_tokens_per_doc: 9,
            vocab_size: 512,
            query_count: 6,
            query_tokens: 3,
            include_tie_pair: 1,
        };
        let w = generate_workload_v1(cfg).unwrap();
        // Snapshot: changing these outputs should be a deliberate decision.
        assert_eq!(w.docs.len(), 8);
        assert_eq!(w.queries.len(), 6);

        let want_docs = [
            "w000215 w000215 w000358 w000134 w000161 w000081",
            "w000215 w000215 w000358 w000134 w000161 w000081",
            "w000122 w000454 w000279 w000122",
            "w000001 w000433 w000255 w000441 w000202 w000171 w000474 w000302 w000434",
            "w000322 w000059 w000063 w000092 w000426 w000092 w000167 w000134",
            "w000457 w000493 w000037 w000145 w000027 w000071",
            "w000291 w000271 w000493 w000002 w000447 w000145 w000307 w000239 w000385",
            "w000194 w000365 w000489 w000502",
        ];
        for (i, want) in want_docs.iter().enumerate() {
            assert_eq!(w.docs[i].doc_id, i as u32);
            assert_eq!(w.docs[i].text, *want);
        }

        let want_queries = [
            "w000000 w000001 w000002",
            "w000001 w000002 w000003",
            "w000002 w000003 w000004",
            "w000003 w000004 w000005",
            "w000004 w000005 w000006",
            "w000005 w000006 w000007",
        ];
        for (i, want) in want_queries.iter().enumerate() {
            assert_eq!(w.queries[i].query_id, i as u32);
            assert_eq!(w.queries[i].text, *want);
        }
    }

    #[test]
    fn same_seed_same_hash_different_seed_different_hash() {
        let cfg = WorkloadCfgV1 {
            version: WORKLOAD_GEN_V1_VERSION,
            seed: 1,
            doc_count: 16,
            min_tokens_per_doc: 3,
            max_tokens_per_doc: 7,
            vocab_size: 1024,
            query_count: 8,
            query_tokens: 3,
            include_tie_pair: 1,
        };
        let w1 = generate_workload_v1(cfg).unwrap();
        let w2 = generate_workload_v1(cfg).unwrap();
        assert_eq!(workload_hash_v1(&w1), workload_hash_v1(&w2));

        let mut cfg2 = cfg;
        cfg2.seed = 2;
        let w3 = generate_workload_v1(cfg2).unwrap();
        assert_ne!(workload_hash_v1(&w1), workload_hash_v1(&w3));
    }
}
