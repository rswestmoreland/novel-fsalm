// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Bridge expansion integration.
//!
//! This module wires the bridge expansion contracts into a
//! concrete query-term expansion routine.
//!
//! v1 scope:
//! - Base anchors: tokenizer terms (and optional metaphone terms).
//! - Candidate generation: lexical morphology variants (bounded, lexicon-checked).
//! - Selection: [`crate::expansion_builder::build_expanded_qfv_v1`] with a bounded
//! [`crate::expansion_budget::ExpansionBudgetV1`].
//! - Output: canonical query terms (sorted by term id, merged qtf) plus a count
//! of newly-added unique term ids.
//!
//! This intentionally keeps the rest of the retrieval engine unchanged. The
//! current index scorer uses `qtf` as a multiplicative factor; bridge weights
//! are mapped into a small integer `qtf` range so weights influence ranking
//! without exploding scores.

use crate::expanded_qfv::ExpandedQfvItemV1;
use crate::expansion_budget::{ExpansionBudgetV1, ExpansionKindV1};
use crate::expansion_builder::{build_expanded_qfv_v1, BaseFeatureV1, ExpansionBuildError};
use crate::frame::{Id64, TermId};
use crate::graph_relevance::{GraphNodeKindV1, GraphRelevanceV1};
use crate::hash::blake3_hash;
use crate::index_query::{QueryTerm, QueryTermsCfg};
use crate::lexicon::derive_lemma_key_id;
use crate::lexicon_expand_lookup::LexiconExpandLookupV1;
use crate::metaphone::{meta_freqs_from_text, MetaphoneCfg};
use crate::query_expansion::{QueryExpansionCfgError, QueryExpansionCfgV1};
use crate::retrieval_control::RetrievalControlV1;
use crate::tokenizer::{term_freqs_from_text, term_id_from_token, TokenIter, TokenizerCfg};

/// Bridge expansion errors (v1).
#[derive(Debug, PartialEq, Eq)]
pub enum BridgeExpansionError {
    /// Expansion configuration is invalid.
    BadCfg(QueryExpansionCfgError),
    /// Bridge expansion builder failed.
    BuildFailed(ExpansionBuildError),
}

impl core::fmt::Display for BridgeExpansionError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            BridgeExpansionError::BadCfg(e) => write!(f, "bad bridge expansion config: {}", e),
            BridgeExpansionError::BuildFailed(e) => {
                write!(f, "bridge expansion build failed: {}", e)
            }
        }
    }
}

impl std::error::Error for BridgeExpansionError {}

fn tie_control_id_v1(control: Option<&RetrievalControlV1>, text: &str) -> Id64 {
    if let Some(c) = control {
        // Use the retrieval control id bytes as a deterministic seed.
        let id = c.control_id();
        let mut b8 = [0u8; 8];
        b8.copy_from_slice(&id[0..8]);
        return Id64(u64::from_le_bytes(b8));
    }

    // Derive a stable tie id from the query text.
    // Note: keep the domain separator explicit and escaped (no raw NULs).
    let mut buf: Vec<u8> = Vec::with_capacity(32 + text.len());
    buf.extend_from_slice(b"bridge_expand_tie_v1\0");
    buf.extend_from_slice(text.as_bytes());
    let h = blake3_hash(&buf);
    let mut b8 = [0u8; 8];
    b8.copy_from_slice(&h[0..8]);
    Id64(u64::from_le_bytes(b8))
}

fn qtf_from_weight_u16(weight: u16) -> u32 {
    // Map 0..65535 into a small integer qtf range (1..=32).
    // - rounding: (w + 2047) >> 11 gives 0..32
    // - clamp: keep >= 1
    let mut q = ((weight as u32).saturating_add(2047)) >> 11;
    if q == 0 {
        q = 1;
    }
    if q > 32 {
        32
    } else {
        q
    }
}

fn canonicalize_query_terms_in_place(xs: &mut Vec<QueryTerm>) {
    xs.sort_by(|a, b| (a.term.0).0.cmp(&(b.term.0).0));

    let mut out: Vec<QueryTerm> = Vec::with_capacity(xs.len());
    for qt in xs.drain(..) {
        if let Some(last) = out.last_mut() {
            if (last.term.0).0 == (qt.term.0).0 {
                last.qtf = last.qtf.saturating_add(qt.qtf);
                continue;
            }
        }
        out.push(qt);
    }
    *xs = out;
}

fn lexicon_has_lemma(lex: &LexiconExpandLookupV1, s: &str) -> bool {
    let key = derive_lemma_key_id(s);
    !lex.lemma_ids_for_key(key, 1).is_empty()
}

fn make_budget_from_cfg(
    base_count: usize,
    cfg: &QueryExpansionCfgV1,
    enable_graph: bool,
) -> ExpansionBudgetV1 {
    // Start from the canonical default.
    let mut b = ExpansionBudgetV1::default_v1();

    // Preserve existing knobs by mapping QueryExpansionCfgV1 caps into the
    // bridge budget. In v1, we generate lexical morphology candidates and,
    // when a graph artifact is present, bounded term-to-term graph candidates.
    let total_cap = cfg.max_total_terms as usize;
    let mut allowed_new: usize = cfg.max_new_terms as usize;
    if total_cap <= base_count {
        allowed_new = 0;
    } else {
        let cap2 = total_cap - base_count;
        if allowed_new > cap2 {
            allowed_new = cap2;
        }
    }

    if allowed_new == 0 {
        // Leave budget as-is; callers should skip build when no new allowed.
        return b;
    }

    if allowed_new < (b.max_expansions_total as usize) {
        b.max_expansions_total = allowed_new as u16;
    }
    if b.max_required_total > b.max_expansions_total {
        b.max_required_total = b.max_expansions_total;
    }

    // Tighten per-kind caps. Graph candidates are only enabled when a graph
    // artifact is present.
    for kb in &mut b.kinds {
        if kb.kind == ExpansionKindV1::Lex || (enable_graph && kb.kind == ExpansionKindV1::Graph) {
            if kb.max_total > b.max_expansions_total {
                kb.max_total = b.max_expansions_total;
            }
        } else {
            kb.max_total = 0;
        }
    }

    // Ensure canonical ordering is preserved.
    b.kinds.sort_by_key(|k| k.kind as u8);
    b
}

fn lex_morphology_candidates(
    text: &str,
    tok_cfg: TokenizerCfg,
    lex: &LexiconExpandLookupV1,
) -> Vec<ExpandedQfvItemV1> {
    // Weight matches docs/BRIDGE_EXPANSION.md "LEX morphology variant".
    const W_MORPH: u16 = 48_000;

    // Stable rule ids for morphology rules.
    const R_PL_IES_TO_Y: u16 = 1;
    const R_PL_ES_DROP: u16 = 2;
    const R_PL_S_DROP: u16 = 3;
    const R_V_ING_DROP: u16 = 4;
    const R_V_ING_E: u16 = 5;
    const R_V_ED_DROP: u16 = 6;
    const R_V_ED_DROP_D: u16 = 7;

    let mut out: Vec<ExpandedQfvItemV1> = Vec::new();

    for sp in TokenIter::new(text) {
        let tok = &text[sp.start..sp.end];
        let tl = tok.to_ascii_lowercase();
        if tl.len() < 3 {
            continue;
        }

        let base_term = term_id_from_token(&tl, tok_cfg);
        let base_id = Id64((base_term.0).0);

        // Plural variants.
        if tl.ends_with("ies") && tl.len() > 3 {
            let cand = format!("{}y", &tl[..tl.len() - 3]);
            if lexicon_has_lemma(lex, &cand) {
                let tid = term_id_from_token(&cand, tok_cfg);
                if (tid.0).0 != (base_term.0).0 {
                    out.push(ExpandedQfvItemV1::new(
                        ExpansionKindV1::Lex,
                        Id64((tid.0).0),
                        W_MORPH,
                        ExpansionKindV1::Lex,
                        base_id,
                        R_PL_IES_TO_Y,
                    ));
                }
            }
        }

        if tl.ends_with("es") && tl.len() > 2 {
            let cand = tl[..tl.len() - 2].to_string();
            if lexicon_has_lemma(lex, &cand) {
                let tid = term_id_from_token(&cand, tok_cfg);
                if (tid.0).0 != (base_term.0).0 {
                    out.push(ExpandedQfvItemV1::new(
                        ExpansionKindV1::Lex,
                        Id64((tid.0).0),
                        W_MORPH,
                        ExpansionKindV1::Lex,
                        base_id,
                        R_PL_ES_DROP,
                    ));
                }
            }
        }

        if tl.ends_with('s') && tl.len() > 1 {
            let cand = tl[..tl.len() - 1].to_string();
            if lexicon_has_lemma(lex, &cand) {
                let tid = term_id_from_token(&cand, tok_cfg);
                if (tid.0).0 != (base_term.0).0 {
                    out.push(ExpandedQfvItemV1::new(
                        ExpansionKindV1::Lex,
                        Id64((tid.0).0),
                        W_MORPH,
                        ExpansionKindV1::Lex,
                        base_id,
                        R_PL_S_DROP,
                    ));
                }
            }
        }

        // Verb forms.
        if tl.ends_with("ing") && tl.len() > 3 {
            let stem = &tl[..tl.len() - 3];
            if stem.len() >= 2 {
                let cand = stem.to_string();
                if lexicon_has_lemma(lex, &cand) {
                    let tid = term_id_from_token(&cand, tok_cfg);
                    if (tid.0).0 != (base_term.0).0 {
                        out.push(ExpandedQfvItemV1::new(
                            ExpansionKindV1::Lex,
                            Id64((tid.0).0),
                            W_MORPH,
                            ExpansionKindV1::Lex,
                            base_id,
                            R_V_ING_DROP,
                        ));
                    }
                }

                let cand2 = format!("{}e", stem);
                if lexicon_has_lemma(lex, &cand2) {
                    let tid = term_id_from_token(&cand2, tok_cfg);
                    if (tid.0).0 != (base_term.0).0 {
                        out.push(ExpandedQfvItemV1::new(
                            ExpansionKindV1::Lex,
                            Id64((tid.0).0),
                            W_MORPH,
                            ExpansionKindV1::Lex,
                            base_id,
                            R_V_ING_E,
                        ));
                    }
                }
            }
        }

        if tl.ends_with("ed") && tl.len() > 2 {
            let stem = &tl[..tl.len() - 2];
            if stem.len() >= 2 {
                let cand = stem.to_string();
                if lexicon_has_lemma(lex, &cand) {
                    let tid = term_id_from_token(&cand, tok_cfg);
                    if (tid.0).0 != (base_term.0).0 {
                        out.push(ExpandedQfvItemV1::new(
                            ExpansionKindV1::Lex,
                            Id64((tid.0).0),
                            W_MORPH,
                            ExpansionKindV1::Lex,
                            base_id,
                            R_V_ED_DROP,
                        ));
                    }
                }

                // Handle doubled consonant ("stopped" -> "stop").
                if stem.as_bytes().len() >= 2 {
                    let b = stem.as_bytes();
                    if b[b.len() - 1] == b[b.len() - 2] {
                        let cand2 = stem[..stem.len() - 1].to_string();
                        if lexicon_has_lemma(lex, &cand2) {
                            let tid = term_id_from_token(&cand2, tok_cfg);
                            if (tid.0).0 != (base_term.0).0 {
                                out.push(ExpandedQfvItemV1::new(
                                    ExpansionKindV1::Lex,
                                    Id64((tid.0).0),
                                    W_MORPH,
                                    ExpansionKindV1::Lex,
                                    base_id,
                                    R_V_ED_DROP_D,
                                ));
                            }
                        }
                    }
                }
            }
        }
    }
    out
}

fn graph_candidate_weight_u16(edge_weight_q16: u16, hop_count: u8) -> u16 {
    // Graph candidates stay intentionally weak so lexical hits remain primary.
    let mut w = (edge_weight_q16 as u32) / 64;
    if hop_count > 1 {
        w = (w.saturating_mul(3)) / 4;
    }
    if w == 0 {
        w = 1;
    }
    if w > 2_047 {
        2_047
    } else {
        w as u16
    }
}

fn graph_rule_id_v1(hop_count: u8, flags: u8) -> u16 {
    match (
        hop_count > 1,
        (flags & crate::graph_relevance::GREDGE_FLAG_SYMMETRIC) != 0,
    ) {
        (false, false) => 1,
        (false, true) => 2,
        (true, false) => 3,
        (true, true) => 4,
    }
}

fn find_graph_term_row<'a>(
    graph: &'a GraphRelevanceV1,
    term_id: Id64,
) -> Option<&'a crate::graph_relevance::GraphRelevanceRowV1> {
    let key = (GraphNodeKindV1::Term as u8, term_id.0);
    match graph
        .rows
        .binary_search_by(|row| ((row.seed_kind as u8), row.seed_id.0).cmp(&key))
    {
        Ok(ix) => Some(&graph.rows[ix]),
        Err(_) => None,
    }
}

fn graph_term_candidates(
    bases: &[BaseFeatureV1],
    graph: &GraphRelevanceV1,
) -> Vec<ExpandedQfvItemV1> {
    let mut out: Vec<ExpandedQfvItemV1> = Vec::new();
    for base in bases {
        if base.kind != ExpansionKindV1::Lex {
            continue;
        }
        let row = match find_graph_term_row(graph, base.id) {
            Some(v) => v,
            None => continue,
        };
        for edge in &row.edges {
            if edge.target_kind != GraphNodeKindV1::Term {
                continue;
            }
            if edge.target_id.0 == base.id.0 {
                continue;
            }
            let weight = graph_candidate_weight_u16(edge.weight_q16, edge.hop_count);
            out.push(ExpandedQfvItemV1::new(
                ExpansionKindV1::Graph,
                edge.target_id,
                weight,
                base.kind,
                base.id,
                graph_rule_id_v1(edge.hop_count, edge.flags),
            ));
        }
    }
    out
}

fn base_terms_and_bases(text: &str, qcfg: &QueryTermsCfg) -> (Vec<QueryTerm>, Vec<BaseFeatureV1>) {
    let tok_cfg = TokenizerCfg {
        max_token_bytes: qcfg.tok_cfg.max_token_bytes,
    };
    let mut tmp: Vec<(QueryTerm, ExpansionKindV1, Id64)> = Vec::new();
    for tf in term_freqs_from_text(text, tok_cfg) {
        tmp.push((
            QueryTerm {
                term: tf.term,
                qtf: tf.tf,
            },
            ExpansionKindV1::Lex,
            Id64((tf.term.0).0),
        ));
    }

    if qcfg.include_metaphone {
        let tok_cfg2 = TokenizerCfg {
            max_token_bytes: qcfg.tok_cfg.max_token_bytes,
        };
        let meta_cfg = MetaphoneCfg {
            max_token_bytes: qcfg.meta_cfg.max_token_bytes,
            max_code_len: qcfg.meta_cfg.max_code_len,
        };
        for mf in meta_freqs_from_text(text, tok_cfg2, meta_cfg) {
            tmp.push((
                QueryTerm {
                    term: TermId(mf.meta.0),
                    qtf: mf.tf,
                },
                ExpansionKindV1::Meta,
                mf.meta.0,
            ));
        }
    }

    // Canonicalize: sort by term id and merge duplicates. Keep the first kind/id.
    tmp.sort_by(|a, b| ((a.0.term.0).0).cmp(&((b.0.term.0).0)));

    let mut out_terms: Vec<QueryTerm> = Vec::new();
    let mut bases: Vec<BaseFeatureV1> = Vec::new();

    for (qt, kind, id) in tmp {
        if let Some(last) = out_terms.last_mut() {
            if (last.term.0).0 == (qt.term.0).0 {
                last.qtf = last.qtf.saturating_add(qt.qtf);
                continue;
            }
        }
        out_terms.push(qt);
        bases.push(BaseFeatureV1::new(kind, id));
        if out_terms.len() >= qcfg.max_terms {
            break;
        }
    }

    (out_terms, bases)
}

/// Expand query terms using bridge expansion v1.
///
/// Returns:
/// - canonical query terms (sorted by term id, merged qtf)
/// - number of newly-added unique term ids relative to the base anchors
pub fn bridge_expand_query_terms_v1(
    text: &str,
    qcfg: &QueryTermsCfg,
    lex: Option<&LexiconExpandLookupV1>,
    graph: Option<&GraphRelevanceV1>,
    control: Option<&RetrievalControlV1>,
    expand_cfg_opt: Option<&QueryExpansionCfgV1>,
) -> Result<(Vec<QueryTerm>, u32), BridgeExpansionError> {
    let (mut base_terms, bases) = base_terms_and_bases(text, qcfg);
    let base_count = base_terms.len();

    let cfg = expand_cfg_opt
        .copied()
        .unwrap_or_else(QueryExpansionCfgV1::default);
    if let Err(e) = cfg.validate() {
        return Err(BridgeExpansionError::BadCfg(e));
    }

    if cfg.max_depth == 0 {
        return Ok((base_terms, 0));
    }

    let enable_graph = graph.is_some();
    if lex.is_none() && !enable_graph {
        return Ok((base_terms, 0));
    }

    // If no new terms are allowed under legacy caps, skip.
    let budget = make_budget_from_cfg(base_count, &cfg, enable_graph);
    if budget.max_expansions_total == 0 {
        return Ok((base_terms, 0));
    }

    // Generate candidates and build expanded qfv.
    let tok_cfg = TokenizerCfg {
        max_token_bytes: qcfg.tok_cfg.max_token_bytes,
    };
    let mut candidates: Vec<ExpandedQfvItemV1> = Vec::new();
    if let Some(lex2) = lex {
        candidates.extend(lex_morphology_candidates(text, tok_cfg, lex2));
    }
    if let Some(graph2) = graph {
        candidates.extend(graph_term_candidates(&bases, graph2));
    }
    if candidates.is_empty() {
        return Ok((base_terms, 0));
    }

    let tie_id = tie_control_id_v1(control, text);
    let required_bases: Vec<BaseFeatureV1> = Vec::new();
    let optional_bases: Vec<BaseFeatureV1> = bases;

    let qfv = build_expanded_qfv_v1(tie_id, required_bases, optional_bases, candidates, &budget)
        .map_err(BridgeExpansionError::BuildFailed)?;

    // Track newly-added unique ids relative to base terms.
    let mut seen: Vec<u64> = base_terms.iter().map(|qt| (qt.term.0).0).collect();
    seen.sort_unstable();
    seen.dedup();

    let mut new_unique: u32 = 0;
    for it in qfv.required.iter().chain(qfv.optional.iter()) {
        // In v1, we only emit Lex candidates. Future kinds may need domain adapters.
        let term_u64 = it.id.0;
        let qtf = qtf_from_weight_u16(it.weight);
        base_terms.push(QueryTerm {
            term: TermId(Id64(term_u64)),
            qtf,
        });
        if seen.binary_search(&term_u64).is_err() {
            let pos = match seen.binary_search(&term_u64) {
                Ok(_) => 0,
                Err(p) => p,
            };
            seen.insert(pos, term_u64);
            new_unique = new_unique.saturating_add(1);
        }
    }

    canonicalize_query_terms_in_place(&mut base_terms);
    Ok((base_terms, new_unique))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::ArtifactStore;
    use crate::graph_relevance::{
        GraphNodeKindV1, GraphRelevanceEdgeV1, GraphRelevanceRowV1, GraphRelevanceV1,
        GR_FLAG_HAS_TERM_ROWS,
    };
    use crate::hash::blake3_hash;
    use crate::hash::Hash32;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    /// In-memory deterministic artifact store used for tests.
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
    fn bridge_expand_is_deterministic_across_runs() {
        let store = MemStore::new();
        let lex_snap = build_lexicon_snapshot_with_lemma(&store, "banana");
        let lex = crate::lexicon_expand_lookup::load_lexicon_expand_lookup_v1(&store, &lex_snap)
            .expect("lex load")
            .expect("lex present");

        let qcfg = QueryTermsCfg::new();
        let cfg = QueryExpansionCfgV1::default();

        let (t0, n0) =
            bridge_expand_query_terms_v1("bananas", &qcfg, Some(&lex), None, None, Some(&cfg))
                .expect("ok");
        let (t1, n1) =
            bridge_expand_query_terms_v1("bananas", &qcfg, Some(&lex), None, None, Some(&cfg))
                .expect("ok");

        assert_eq!(t0, t1);
        assert_eq!(n0, n1);
        assert!(n0 >= 1);
    }

    #[test]
    fn bridge_expand_with_graph_adds_related_term() {
        let qcfg = QueryTermsCfg::new();
        let banana = term_id_from_token("banana", TokenizerCfg::default());
        let split = term_id_from_token("split", TokenizerCfg::default());
        let graph = GraphRelevanceV1 {
            version: crate::graph_relevance::GRAPH_RELEVANCE_V1_VERSION,
            build_id: blake3_hash(b"graph-bridge"),
            flags: GR_FLAG_HAS_TERM_ROWS,
            rows: vec![GraphRelevanceRowV1 {
                seed_kind: GraphNodeKindV1::Term,
                seed_id: banana.0,
                edges: vec![GraphRelevanceEdgeV1::new(
                    GraphNodeKindV1::Term,
                    split.0,
                    20_000,
                    1,
                    crate::graph_relevance::GREDGE_FLAG_SYMMETRIC,
                )],
            }],
        };

        let (terms, added) = bridge_expand_query_terms_v1(
            "banana",
            &qcfg,
            None,
            Some(&graph),
            None,
            Some(&QueryExpansionCfgV1::default()),
        )
        .expect("ok");

        assert!(added >= 1);
        assert!(terms.iter().any(|qt| (qt.term.0).0 == (split.0).0));
    }
}
