// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Query expansion types.
//!
//! Track B goal:
//! - Improve recall deterministically by expanding a query with additional terms.
//! - Keep behavior bounded and stable (no randomization).
//!
//! scope:
//! - Rule-based expansions that can be derived from the query text itself.
//! - Lexicon membership filter: only emit a candidate term if a matching
//! lemma_key_id exists in the loaded lexicon lookup view.
//!
//! Note: Lexicon relation-based expansions (synonym/related edges) require a
//! surface-form mapping for target lemmas (a text table). That mapping is
//! deferred, so does not emit relation-derived term ids.

use crate::frame::{Id64, TermId};
use crate::lexicon::derive_lemma_key_id;
use crate::lexicon_expand_lookup::LexiconExpandLookupV1;
use crate::tokenizer::{term_id_from_token, TokenIter, TokenizerCfg};

/// Query expansion config (v1).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryExpansionCfgV1 {
    /// Maximum depth for expansion graph walk.
    ///
    /// only emits depth 1 (variants) when max_depth >= 1.
    pub max_depth: u8,
    /// Maximum number of new terms to add (excluding original terms).
    pub max_new_terms: u16,
    /// Maximum total terms (original + new) to emit.
    pub max_total_terms: u16,
    /// Allowed relation types mask (lexicon rel_type ids).
    ///
    /// Reserved for future extensions; does not emit relation-derived terms.
    pub allow_relations_mask: u32,
}

impl Default for QueryExpansionCfgV1 {
    fn default() -> Self {
        QueryExpansionCfgV1 {
            max_depth: 1,
            max_new_terms: 8,
            max_total_terms: 16,
            allow_relations_mask: 0,
        }
    }
}

/// Query expansion config validation errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryExpansionCfgError {
    /// max_total_terms must be >= 1.
    MaxTotalZero,
    /// max_new_terms must be <= max_total_terms.
    MaxNewExceedsTotal,
}

impl core::fmt::Display for QueryExpansionCfgError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            QueryExpansionCfgError::MaxTotalZero => f.write_str("max_total_terms must be >= 1"),
            QueryExpansionCfgError::MaxNewExceedsTotal => {
                f.write_str("max_new_terms exceeds max_total_terms")
            }
        }
    }
}

impl std::error::Error for QueryExpansionCfgError {}

impl QueryExpansionCfgV1 {
    /// Validate configuration invariants.
    pub fn validate(&self) -> Result<(), QueryExpansionCfgError> {
        if self.max_total_terms == 0 {
            return Err(QueryExpansionCfgError::MaxTotalZero);
        }
        if self.max_new_terms > self.max_total_terms {
            return Err(QueryExpansionCfgError::MaxNewExceedsTotal);
        }
        Ok(())
    }
}

/// Expansion reason codes (v1).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum QueryExpansionReasonV1 {
    /// Original term from the input query.
    Original = 1,
    /// Term derived via lexicon lemma match.
    Lemma = 2,
    /// Term derived via lexicon relation edge.
    Relation = 3,
    /// Term derived via deterministic variant rule.
    Variant = 4,
}

/// An expanded term with metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExpandedTermV1 {
    /// Term id to use in index lookups.
    pub term_id: TermId,
    /// Depth from the original term.
    pub depth: u8,
    /// Reason code for the expansion.
    pub reason_code: u16,
}

impl ExpandedTermV1 {
    /// Construct a new expanded term.
    pub fn new(term_id: TermId, depth: u8, reason: QueryExpansionReasonV1) -> Self {
        ExpandedTermV1 {
            term_id,
            depth,
            reason_code: reason as u16,
        }
    }
}

fn seen_insert_u64(seen_sorted: &mut Vec<u64>, v: u64) -> bool {
    match seen_sorted.binary_search(&v) {
        Ok(_) => false,
        Err(pos) => {
            seen_sorted.insert(pos, v);
            true
        }
    }
}

fn lexicon_has_lemma_key(lex: &LexiconExpandLookupV1, s: &str) -> bool {
    let key = derive_lemma_key_id(s);
    !lex.lemma_ids_for_key(key, 1).is_empty()
}

fn try_add_variant(
    out: &mut Vec<ExpandedTermV1>,
    seen_u64: &mut Vec<u64>,
    candidate: &str,
    tok_cfg: TokenizerCfg,
    lex: &LexiconExpandLookupV1,
) {
    if candidate.is_empty() {
        return;
    }
    if !lexicon_has_lemma_key(lex, candidate) {
        return;
    }
    let tid = term_id_from_token(candidate, tok_cfg);
    let u = (tid.0).0;
    if !seen_insert_u64(seen_u64, u) {
        return;
    }
    out.push(ExpandedTermV1::new(tid, 1, QueryExpansionReasonV1::Variant));
}

fn emit_morphology_variants(
    token: &str,
    out: &mut Vec<ExpandedTermV1>,
    seen_u64: &mut Vec<u64>,
    tok_cfg: TokenizerCfg,
    lex: &LexiconExpandLookupV1,
) {
    // Work on an ASCII-lowercased copy so suffix checks are deterministic.
    let t = token.to_ascii_lowercase();
    let n = t.len();
    if n < 3 {
        return;
    }

    // Order matters: more specific rules first.

    // ies -> y
    if n >= 4 && t.ends_with("ies") {
        let stem = &t[..n - 3];
        let cand = format!("{}y", stem);
        try_add_variant(out, seen_u64, &cand, tok_cfg, lex);
    }

    // es -> (drop)
    if n >= 4 && t.ends_with("es") {
        let cand = &t[..n - 2];
        try_add_variant(out, seen_u64, cand, tok_cfg, lex);
    }

    // s -> (drop)
    if n >= 4 && t.ends_with('s') {
        let cand = &t[..n - 1];
        try_add_variant(out, seen_u64, cand, tok_cfg, lex);
    }

    // ing -> (drop) and ing -> +e
    if n >= 6 && t.ends_with("ing") {
        let stem = &t[..n - 3];
        try_add_variant(out, seen_u64, stem, tok_cfg, lex);
        let cand_e = format!("{}e", stem);
        try_add_variant(out, seen_u64, &cand_e, tok_cfg, lex);
    }

    // ed -> (drop) and ed -> (drop d)
    if n >= 5 && t.ends_with("ed") {
        let stem = &t[..n - 2];
        try_add_variant(out, seen_u64, stem, tok_cfg, lex);
        let cand_drop_d = &t[..n - 1];
        try_add_variant(out, seen_u64, cand_drop_d, tok_cfg, lex);
    }
}

/// Expand query text into a bounded, deterministic list of terms.
///
/// Behavior:
/// - Emits original terms (depth 0, reason Original), sorted by term id.
/// - Optionally emits variant terms (depth 1, reason Variant) when max_depth >= 1.
/// - Uses the lexicon lookup view as a membership filter for candidates.
/// - Caps expansions by max_new_terms and max_total_terms.
///
/// This function does not emit relation-derived term ids in.
pub fn expand_query_terms_v1(
    text: &str,
    tok_cfg: TokenizerCfg,
    lex: &LexiconExpandLookupV1,
    cfg: &QueryExpansionCfgV1,
) -> Result<Vec<ExpandedTermV1>, QueryExpansionCfgError> {
    cfg.validate()?;

    let max_total: usize = cfg.max_total_terms as usize;

    // Collect token spans once for later variant generation.
    let mut spans = Vec::new();
    for sp in TokenIter::new(text) {
        spans.push(sp);
    }

    // Original terms: term ids derived from the query tokens.
    let mut orig_u64: Vec<u64> = Vec::with_capacity(spans.len());
    for sp in &spans {
        let tok = &text[sp.start..sp.end];
        let tid = term_id_from_token(tok, tok_cfg);
        orig_u64.push((tid.0).0);
    }

    orig_u64.sort_unstable();
    orig_u64.dedup();

    if orig_u64.len() > max_total {
        orig_u64.truncate(max_total);
    }

    let mut out: Vec<ExpandedTermV1> = Vec::with_capacity(max_total);
    for u in &orig_u64 {
        out.push(ExpandedTermV1::new(
            TermId(Id64(*u)),
            0,
            QueryExpansionReasonV1::Original,
        ));
    }

    let mut seen_u64: Vec<u64> = orig_u64;

    if cfg.max_depth == 0 {
        return Ok(out);
    }
    if cfg.max_new_terms == 0 {
        return Ok(out);
    }
    if out.len() >= max_total {
        return Ok(out);
    }

    let budget_new = core::cmp::min(cfg.max_new_terms as usize, max_total - out.len());

    let mut added: Vec<ExpandedTermV1> = Vec::new();

    for sp in &spans {
        if added.len() >= budget_new {
            break;
        }
        let tok = &text[sp.start..sp.end];
        emit_morphology_variants(tok, &mut added, &mut seen_u64, tok_cfg, lex);
        if added.len() >= budget_new {
            break;
        }
    }

    // Deterministic ordering for added terms.
    added.sort_by(|a, b| {
        let au = (a.term_id.0).0;
        let bu = (b.term_id.0).0;
        au.cmp(&bu)
    });

    if added.len() > budget_new {
        added.truncate(budget_new);
    }

    out.extend(added);
    if out.len() > max_total {
        out.truncate(max_total);
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::artifact::FsArtifactStore;
    use crate::frame::Id64;
    use crate::lexicon::{derive_text_id, LemmaId, LemmaRowV1, LEXICON_SCHEMA_V1};
    use crate::lexicon_expand_lookup::load_lexicon_expand_lookup_v1;
    use crate::lexicon_segment::LexiconSegmentV1;
    use crate::lexicon_segment_store::put_lexicon_segment_v1;
    use crate::lexicon_snapshot::{LexiconSnapshotEntryV1, LexiconSnapshotV1};
    use crate::lexicon_snapshot_store::put_lexicon_snapshot_v1;

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        use std::fs;
        let mut p = std::env::temp_dir();
        p.push("fsa_lm_tests");
        p.push(name);
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn lemma_row(id: u64, text: &str) -> LemmaRowV1 {
        LemmaRowV1 {
            version: LEXICON_SCHEMA_V1,
            lemma_id: LemmaId(Id64(id)),
            lemma_key_id: derive_lemma_key_id(text),
            lemma_text_id: derive_text_id(text),
            pos_mask: 1,
            flags: 0,
        }
    }

    fn build_lookup_with_lemmas(name: &str, lemmas: Vec<LemmaRowV1>) -> LexiconExpandLookupV1 {
        let dir = tmp_dir(name);
        let store = FsArtifactStore::new(&dir).unwrap();

        let seg = LexiconSegmentV1::build_from_rows(&lemmas, &[], &[], &[]).unwrap();
        let seg_h = put_lexicon_segment_v1(&store, &seg).unwrap();

        let mut snap = LexiconSnapshotV1::new();
        snap.entries.push(LexiconSnapshotEntryV1 {
            lex_seg: seg_h,
            lemma_count: seg.lemma_id.len() as u32,
            sense_count: seg.sense_id.len() as u32,
            rel_count: seg.rel_from_tag.len() as u32,
            pron_count: seg.pron_lemma_id.len() as u32,
        });
        snap.canonicalize_in_place();

        let snap_h = put_lexicon_snapshot_v1(&store, &snap).unwrap();
        load_lexicon_expand_lookup_v1(&store, &snap_h)
            .unwrap()
            .unwrap()
    }

    fn term_u64(s: &str) -> u64 {
        (term_id_from_token(s, TokenizerCfg::default()).0).0
    }

    #[test]
    fn expand_plural_s_strips_when_lemma_exists() {
        let lex = build_lookup_with_lemmas(
            "expand_plural_s_strips_when_lemma_exists",
            vec![lemma_row(1, "banana")],
        );

        let cfg = QueryExpansionCfgV1 {
            max_depth: 1,
            max_new_terms: 8,
            max_total_terms: 16,
            allow_relations_mask: 0,
        };

        let out = expand_query_terms_v1("bananas", TokenizerCfg::default(), &lex, &cfg).unwrap();
        let ids: Vec<u64> = out.iter().map(|x| (x.term_id.0).0).collect();

        assert!(ids.contains(&term_u64("bananas")));
        assert!(ids.contains(&term_u64("banana")));

        // Original must be present with depth 0.
        assert!(out
            .iter()
            .any(|x| x.depth == 0 && x.reason_code == (QueryExpansionReasonV1::Original as u16)));
        // Variant must be present with depth 1.
        assert!(out
            .iter()
            .any(|x| x.depth == 1 && x.reason_code == (QueryExpansionReasonV1::Variant as u16)));
    }

    #[test]
    fn expand_does_not_emit_variant_when_not_in_lexicon() {
        let lex = build_lookup_with_lemmas(
            "expand_does_not_emit_variant_when_not_in_lexicon",
            vec![lemma_row(1, "banana")],
        );

        let cfg = QueryExpansionCfgV1::default();
        let out = expand_query_terms_v1("cars", TokenizerCfg::default(), &lex, &cfg).unwrap();

        // No lemma "car" in lexicon, so only original term should appear.
        assert_eq!(out.len(), 1);
        assert_eq!((out[0].term_id.0).0, term_u64("cars"));
        assert_eq!(out[0].depth, 0);
    }

    #[test]
    fn expansion_respects_caps() {
        let lex = build_lookup_with_lemmas(
            "expansion_respects_caps",
            vec![
                lemma_row(1, "banana"),
                lemma_row(2, "bake"),
                lemma_row(3, "make"),
            ],
        );

        let cfg = QueryExpansionCfgV1 {
            max_depth: 1,
            max_new_terms: 1,
            // Two original terms are present ("bananas", "baked").
            // Leave room for exactly one new term.
            max_total_terms: 3,
            allow_relations_mask: 0,
        };

        // "bananas baked" could emit two variants (banana, bake), but caps allow only one.
        let out =
            expand_query_terms_v1("bananas baked", TokenizerCfg::default(), &lex, &cfg).unwrap();
        assert!(out.len() <= 3);

        let n_variant = out
            .iter()
            .filter(|x| x.depth == 1 && x.reason_code == (QueryExpansionReasonV1::Variant as u16))
            .count();
        assert_eq!(n_variant, 1);
    }

    #[test]
    fn cfg_validate_rejects_zero_total() {
        let cfg = QueryExpansionCfgV1 {
            max_depth: 1,
            max_new_terms: 0,
            max_total_terms: 0,
            allow_relations_mask: 0,
        };
        assert_eq!(
            cfg.validate().unwrap_err(),
            QueryExpansionCfgError::MaxTotalZero
        );
    }

    #[test]
    fn cfg_validate_rejects_new_exceeds_total() {
        let cfg = QueryExpansionCfgV1 {
            max_depth: 1,
            max_new_terms: 10,
            max_total_terms: 3,
            allow_relations_mask: 0,
        };
        assert_eq!(
            cfg.validate().unwrap_err(),
            QueryExpansionCfgError::MaxNewExceedsTotal
        );
    }
}
