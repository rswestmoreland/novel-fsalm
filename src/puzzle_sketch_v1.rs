// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Deterministic logic-puzzle sketch extraction (v1).
//!
//! This module provides a conservative, rules-first normalizer that attempts to
//! extract a small, bounded sketch from natural-language prompts. The goal is
//! not to guess missing details, but to:
//! - detect when the user likely intends a logic/constraint puzzle
//! - extract obvious variables/domains/constraint signals when present
//! - choose a single best clarifying question when required
//!
//! The sketch is intended to be used as an internal intermediate structure.
//! Persistence and cross-turn continuation are handled in a later milestone.

use crate::frame::derive_id64;
use crate::lexicon::{
    derive_lemma_key_id, LemmaId, LemmaKeyId, POS_ADJ, POS_ADV, POS_NOUN, POS_PROPER_NOUN, POS_VERB,
};
use crate::lexicon_expand_lookup::LexiconExpandLookupV1;
use crate::lexicon_neighborhoods::{lemma_pos_mask, LexiconCueNeighborhoodsV1};
use crate::tokenizer::{TokenIter, TokenizerCfg};

/// Default caps for puzzle sketch extraction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PuzzleSketchCfgV1 {
    /// Maximum number of variable candidates to keep.
    pub max_vars: usize,
    /// Maximum tokens to scan.
    pub max_tokens_scan: usize,
    /// Maximum lemma ids considered per token.
    pub lemma_ids_cap_per_token: usize,
    /// Tokenizer configuration.
    pub tokenizer_cfg: TokenizerCfg,
}

impl Default for PuzzleSketchCfgV1 {
    fn default() -> Self {
        Self {
            max_vars: 8,
            max_tokens_scan: 256,
            lemma_ids_cap_per_token: 4,
            tokenizer_cfg: TokenizerCfg::default(),
        }
    }
}

/// Coarse puzzle shape hint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PuzzleShapeHintV1 {
    /// Unknown shape.
    Unknown,
    /// Ordering/arrangement (positions, before/after, left/right).
    Ordering,
    /// Matching/categorization (each has one of X, all different, etc.).
    Matching,
}

/// Clarify kinds for logic puzzles.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PuzzleClarifyKindV1 {
    /// Ask the user to list the entities/variables.
    NeedVars,
    /// Ask the user to provide the value domain.
    NeedDomain,
    /// Ask the user to clarify the puzzle shape.
    NeedShape,
    /// Ask the user to provide constraints.
    NeedConstraints,
}

/// Deterministic clarify question suggestion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PuzzleClarifyV1 {
    /// Stable question id (Id64).
    pub question_id: crate::frame::Id64,
    /// Question score (higher is preferred).
    pub score: i32,
    /// Question text.
    pub text: String,
    /// Kind for diagnostics.
    pub kind: PuzzleClarifyKindV1,
}

/// Extracted sketch for a potential logic puzzle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PuzzleSketchV1 {
    /// True if this prompt likely intends a logic/constraint puzzle.
    pub is_logic_puzzle_likely: bool,
    /// Variable/entity name candidates (stable order, unique).
    pub var_names: Vec<String>,
    /// Numeric domain range hint, inclusive (lo, hi).
    pub domain_range: Option<(i32, i32)>,
    /// True if the text contains explicit constraint operator signals.
    pub has_constraints: bool,
    /// Coarse shape hint.
    pub shape: PuzzleShapeHintV1,
}

fn pos_is_content(mask: u32) -> bool {
    (mask & (POS_NOUN | POS_VERB | POS_ADJ | POS_ADV | POS_PROPER_NOUN)) != 0
}

fn is_small_hex_like(s: &str) -> bool {
    // Avoid treating hashes and hex ids as variables.
    let n = s.len();
    if n < 8 {
        return false;
    }
    if n == 32 || n == 64 {
        return s.as_bytes().iter().all(|b| b"0123456789abcdef".contains(b));
    }
    false
}

fn prev_nonspace_char(text: &str, start: usize) -> Option<u8> {
    if start == 0 {
        return None;
    }
    let bytes = text.as_bytes();
    let mut i = start;
    while i > 0 {
        i -= 1;
        let b = bytes[i];
        if b != b' ' && b != b'\t' && b != b'\r' && b != b'\n' {
            return Some(b);
        }
    }
    None
}

fn next_nonspace_char(text: &str, end: usize) -> Option<u8> {
    let bytes = text.as_bytes();
    let mut i = end;
    while i < bytes.len() {
        let b = bytes[i];
        if b != b' ' && b != b'\t' && b != b'\r' && b != b'\n' {
            return Some(b);
        }
        i += 1;
    }
    None
}

fn next_nonspace_op(text: &str, end: usize) -> bool {
    // Detect a constraint operator immediately after a token (after spaces).
    let bytes = text.as_bytes();
    let mut i = end;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b' ' || b == b'\t' || b == b'\r' || b == b'\n' {
            i += 1;
            continue;
        }
        if b == b'=' || b == b'<' || b == b'>' {
            return true;
        }
        if b == b'!' && i + 1 < bytes.len() && bytes[i + 1] == b'=' {
            return true;
        }
        return false;
    }
    false
}

fn token_is_shape_word(tok: &str) -> bool {
    let low = tok.to_ascii_lowercase();
    low == "ordering" || low == "order" || low == "matching" || low == "match"
}
fn is_capitalized_name(tok: &str) -> bool {
    if tok.len() < 2 || tok.len() > 24 {
        return false;
    }
    if is_small_hex_like(tok) {
        return false;
    }
    // Avoid treating common sentence starters and determiners as entity names.
    // This is a conservative fallback for lexicon-absent mode.
    match tok.to_ascii_lowercase().as_str() {
        "each" | "every" | "the" | "a" | "an" | "if" | "then" | "and" | "or" | "but" | "in"
        | "on" | "at" | "from" | "to" | "with" | "without" | "by" | "as" => {
            return false;
        }
        _ => {}
    }
    let b = tok.as_bytes();
    if !(b'A' <= b[0] && b[0] <= b'Z') {
        return false;
    }
    // Require remaining bytes to be ASCII letters or apostrophe/hyphen.
    for &x in &b[1..] {
        if (b'a' <= x && x <= b'z') || (b'A' <= x && x <= b'Z') {
            continue;
        }
        if x == b'-' || x == b'\'' {
            continue;
        }
        return false;
    }
    true
}

fn is_single_letter_var(tok: &str) -> bool {
    if tok.len() != 1 {
        return false;
    }
    let b = tok.as_bytes()[0];
    // Only treat uppercase single-letter tokens as variables. Lowercase letters
    // appear frequently in natural language (for example, "a") and would cause
    // false positives in lexicon-absent mode.
    b'A' <= b && b <= b'Z'
}

fn find_numeric_range_hint(text: &str) -> Option<(i32, i32)> {
    let bytes = text.as_bytes();
    let mut i: usize = 0;
    while i + 2 < bytes.len() {
        // Find first digit.
        if !(b'0' <= bytes[i] && bytes[i] <= b'9') {
            i += 1;
            continue;
        }
        // Parse left number.
        let mut j = i;
        while j < bytes.len() && (b'0' <= bytes[j] && bytes[j] <= b'9') {
            j += 1;
        }
        let left = &text[i..j];
        let left_v: i32 = left.parse().ok()?;

        // Skip spaces.
        let mut k = j;
        while k < bytes.len() && bytes[k].is_ascii_whitespace() {
            k += 1;
        }
        // Range operator: ".." or "-".
        let mut op_len: usize = 0;
        if k + 1 < bytes.len() && bytes[k] == b'.' && bytes[k + 1] == b'.' {
            op_len = 2;
        } else if k < bytes.len() && bytes[k] == b'-' {
            op_len = 1;
        }
        if op_len == 0 {
            i = j;
            continue;
        }
        k += op_len;
        while k < bytes.len() && bytes[k].is_ascii_whitespace() {
            k += 1;
        }
        if k >= bytes.len() || !(b'0' <= bytes[k] && bytes[k] <= b'9') {
            i = j;
            continue;
        }
        let mut m = k;
        while m < bytes.len() && (b'0' <= bytes[m] && bytes[m] <= b'9') {
            m += 1;
        }
        let right = &text[k..m];
        let right_v: i32 = right.parse().ok()?;
        if left_v <= right_v {
            return Some((left_v, right_v));
        }
        return Some((right_v, left_v));
    }
    None
}

fn has_constraint_operators(text: &str) -> bool {
    // Very small set of structural operators.
    text.contains("!=")
        || text.contains("<=")
        || text.contains(">=")
        || text.contains("=")
        || text.contains("<")
        || text.contains(">")
}

fn token_hits_any_lemma(
    view: &LexiconExpandLookupV1,
    set: &[LemmaId],
    tok: &str,
    cap: usize,
    scratch: &mut Vec<LemmaId>,
) -> bool {
    if set.is_empty() {
        return false;
    }
    let key: LemmaKeyId = derive_lemma_key_id(tok);
    view.lemma_ids_for_key_into(key, cap, scratch);
    for id in scratch.iter() {
        if set.binary_search(id).is_ok() {
            return true;
        }
    }
    false
}

fn ordering_seed_lemma_ids(view: &LexiconExpandLookupV1) -> Vec<LemmaId> {
    // Small, stable seed list. Expansion is handled by the lexicon snapshot.
    let seeds = [
        "before", "after", "left", "right", "next", "adjacent", "between", "first", "last",
    ];
    let mut out: Vec<LemmaId> = Vec::new();
    for s in seeds.iter() {
        let key = derive_lemma_key_id(s);
        let mut ids = view.lemma_ids_for_key(key, 8);
        out.append(&mut ids);
    }
    out.sort_by(|a, b| ((a.0).0).cmp(&((b.0).0)));
    out.dedup();
    out
}

fn matching_seed_lemma_ids(view: &LexiconExpandLookupV1) -> Vec<LemmaId> {
    let seeds = [
        "each",
        "every",
        "different",
        "exactly",
        "either",
        "neither",
        "only",
        "unique",
    ];
    let mut out: Vec<LemmaId> = Vec::new();
    for s in seeds.iter() {
        let key = derive_lemma_key_id(s);
        let mut ids = view.lemma_ids_for_key(key, 8);
        out.append(&mut ids);
    }
    out.sort_by(|a, b| ((a.0).0).cmp(&((b.0).0)));
    out.dedup();
    out
}

fn fallback_ordering_token(tok: &str) -> bool {
    match tok {
        "before" | "after" | "left" | "right" | "next" | "adjacent" | "between" | "first"
        | "last" => true,
        _ => false,
    }
}

fn fallback_matching_token(tok: &str) -> bool {
    match tok {
        "each" | "every" | "different" | "exactly" | "either" | "neither" | "only" | "unique" => {
            true
        }
        _ => false,
    }
}

/// Build a conservative sketch from free text.
///
/// If `lex_view_opt` and `cues_opt` are present, lemma-id matches are used to
/// detect puzzle intent and shape signals. Otherwise, a small structural fallback
/// is used.
pub fn build_puzzle_sketch_v1(
    text: &str,
    lex_view_opt: Option<&LexiconExpandLookupV1>,
    cues_opt: Option<&LexiconCueNeighborhoodsV1>,
    cfg: PuzzleSketchCfgV1,
) -> PuzzleSketchV1 {
    let mut var_names: Vec<String> = Vec::new();

    // Track a short run of consecutive capitalized tokens (for example: "Alice Bob Carol").
    // We only commit the run as variable candidates if it has length >= 2. This prevents
    // single sentence-start words from becoming false-positive entity names.
    let mut pending_cap_run: Vec<String> = Vec::new();

    let mut domain_range: Option<(i32, i32)> = find_numeric_range_hint(text);
    let has_constraints: bool = has_constraint_operators(text);

    let mut shape_ordering_hits: u32 = 0;
    let mut shape_matching_hits: u32 = 0;
    let mut logic_hits: u32 = 0;

    let mut lemma_scratch: Vec<LemmaId> = Vec::new();
    let mut ordering_ids: Vec<LemmaId> = Vec::new();
    let mut matching_ids: Vec<LemmaId> = Vec::new();

    if let Some(view) = lex_view_opt {
        ordering_ids = ordering_seed_lemma_ids(view);
        matching_ids = matching_seed_lemma_ids(view);
    }

    let mut tok_count: usize = 0;
    for sp in TokenIter::new(text) {
        tok_count += 1;
        if tok_count > cfg.max_tokens_scan {
            break;
        }
        let raw_tok = &text[sp.start..sp.end];
        let tok = raw_tok;

        if is_single_letter_var(tok) {
            // Prefer uppercase for stability.
            let v = tok.to_ascii_uppercase();
            if !var_names.iter().any(|x| x == &v) {
                var_names.push(v);
                if var_names.len() >= cfg.max_vars {
                    // keep scanning for intent signals, but stop adding vars
                }
            }
        } else if is_capitalized_name(tok) {
            // Avoid sentence-initial shape words like "Ordering" becoming vars.
            if token_is_shape_word(tok) {
                // Before skipping, flush any pending cap run.
                if pending_cap_run.len() >= 2 {
                    for v in pending_cap_run.drain(..) {
                        if var_names.len() >= cfg.max_vars {
                            break;
                        }
                        if !var_names.iter().any(|x| x == &v) {
                            var_names.push(v);
                        }
                    }
                } else {
                    pending_cap_run.clear();
                }
            } else if var_names.len() < cfg.max_vars {
                let prev = prev_nonspace_char(text, sp.start);
                let next = next_nonspace_char(text, sp.end);
                let in_list = prev == Some(b',') || next == Some(b',');
                let in_constraint = next_nonspace_op(text, sp.end);

                if in_list || in_constraint {
                    // Strong signal: treat this as a variable, and also commit any pending
                    // run (for example "Alice Bob," should keep both Alice and Bob).
                    if pending_cap_run.len() >= 2 {
                        for v in pending_cap_run.drain(..) {
                            if var_names.len() >= cfg.max_vars {
                                break;
                            }
                            if !var_names.iter().any(|x| x == &v) {
                                var_names.push(v);
                            }
                        }
                    } else {
                        pending_cap_run.clear();
                    }
                    if var_names.len() < cfg.max_vars {
                        if !var_names.iter().any(|x| x == tok) {
                            var_names.push(tok.to_string());
                        }
                    }
                } else {
                    // Weak signal: keep a short consecutive run and commit only if it is
                    // length >= 2.
                    if pending_cap_run.len() < cfg.max_vars {
                        pending_cap_run.push(tok.to_string());
                    }
                }
            }
        } else {
            // Commit a pending capitalized run when it ends.
            if pending_cap_run.len() >= 2 {
                for v in pending_cap_run.drain(..) {
                    if var_names.len() >= cfg.max_vars {
                        break;
                    }
                    if !var_names.iter().any(|x| x == &v) {
                        var_names.push(v);
                    }
                }
            } else {
                pending_cap_run.clear();
            }
        }

        if let Some(view) = lex_view_opt {
            if let Some(cues) = cues_opt {
                if token_hits_any_lemma(
                    view,
                    &cues.logic_puzzle,
                    tok,
                    cfg.lemma_ids_cap_per_token,
                    &mut lemma_scratch,
                ) {
                    logic_hits = logic_hits.saturating_add(1);
                }
            }
            if !ordering_ids.is_empty() {
                if token_hits_any_lemma(
                    view,
                    &ordering_ids,
                    tok,
                    cfg.lemma_ids_cap_per_token,
                    &mut lemma_scratch,
                ) {
                    shape_ordering_hits = shape_ordering_hits.saturating_add(1);
                }
            }
            if !matching_ids.is_empty() {
                if token_hits_any_lemma(
                    view,
                    &matching_ids,
                    tok,
                    cfg.lemma_ids_cap_per_token,
                    &mut lemma_scratch,
                ) {
                    shape_matching_hits = shape_matching_hits.saturating_add(1);
                }
            }

            // If a token maps to a content POS lemma, treat it as more likely a semantic cue.
            // This avoids overcounting function words when the lexicon is present.
            if !lemma_scratch.is_empty() {
                for id in lemma_scratch.iter() {
                    if let Some(pm) = lemma_pos_mask(view, *id) {
                        if pos_is_content(pm) {
                            // no-op marker; kept for future scoring
                        }
                    }
                }
            }
        } else {
            let lt = tok.to_ascii_lowercase();
            if fallback_ordering_token(&lt) {
                shape_ordering_hits = shape_ordering_hits.saturating_add(1);
            }
            if fallback_matching_token(&lt) {
                shape_matching_hits = shape_matching_hits.saturating_add(1);
            }
            // Very small fallback: treat "each"+"different" style phrasing as a logic-puzzle signal.
            if lt == "each"
                || lt == "different"
                || lt == "either"
                || lt == "neither"
                || lt == "exactly"
            {
                logic_hits = logic_hits.saturating_add(1);
            }
        }
    }

    // Flush any pending cap run at end-of-input.
    if pending_cap_run.len() >= 2 {
        for v in pending_cap_run.drain(..) {
            if var_names.len() >= cfg.max_vars {
                break;
            }
            if !var_names.iter().any(|x| x == &v) {
                var_names.push(v);
            }
        }
    }

    // Shape hint.
    let shape = if shape_ordering_hits > shape_matching_hits && shape_ordering_hits != 0 {
        PuzzleShapeHintV1::Ordering
    } else if shape_matching_hits > shape_ordering_hits && shape_matching_hits != 0 {
        PuzzleShapeHintV1::Matching
    } else {
        PuzzleShapeHintV1::Unknown
    };

    // Logic puzzle likelihood heuristic.
    // Conservative: require at least one explicit logic hit OR multiple structural signals.
    let mut is_logic = false;
    if logic_hits != 0 {
        is_logic = true;
    } else if var_names.len() >= 2 && (domain_range.is_some() || has_constraints) {
        is_logic = true;
    }

    // Keep has_constraints conservative: it only indicates explicit constraint
    // operators in the text. Shape hints alone do not imply the user provided
    // sufficient constraints to solve the puzzle.

    // Normalize domain_range if it is degenerate.
    if let Some((a, b)) = domain_range {
        if a == b {
            domain_range = None;
        }
    }

    PuzzleSketchV1 {
        is_logic_puzzle_likely: is_logic,
        var_names,
        domain_range,
        has_constraints,
        shape,
    }
}

/// Choose a single best clarifying question for a sketch.
///
/// Returns None if no clarification is needed.
pub fn choose_puzzle_clarify_question_v1(sketch: &PuzzleSketchV1) -> Option<PuzzleClarifyV1> {
    if !sketch.is_logic_puzzle_likely {
        return None;
    }

    // Priority order:
    // 1) variables
    // 2) domain
    // 3) shape
    // 4) constraints

    if sketch.var_names.is_empty() {
        let qid = derive_id64(b"forecast_question_v1", b"clarify:logic_puzzle:vars");
        let txt = "Which entities or variables are involved? Please list them (for example: Alice,Bob,Carol).";
        return Some(PuzzleClarifyV1 {
            question_id: qid,
            score: 10_000,
            text: txt.to_string(),
            kind: PuzzleClarifyKindV1::NeedVars,
        });
    }

    if sketch.domain_range.is_none() {
        let qid = derive_id64(b"forecast_question_v1", b"clarify:logic_puzzle:domain");
        let txt = "What are the possible values or positions for each entity (for example: numbers 1..N, colors {red,blue,...}, positions 1..N)?";
        return Some(PuzzleClarifyV1 {
            question_id: qid,
            score: 9_500,
            text: txt.to_string(),
            kind: PuzzleClarifyKindV1::NeedDomain,
        });
    }

    if sketch.shape == PuzzleShapeHintV1::Unknown {
        let qid = derive_id64(b"forecast_question_v1", b"clarify:logic_puzzle:shape");
        let txt = "Is this an ordering puzzle (arrangement/positions) or a matching puzzle (assigning categories)?";
        return Some(PuzzleClarifyV1 {
            question_id: qid,
            score: 9_000,
            text: txt.to_string(),
            kind: PuzzleClarifyKindV1::NeedShape,
        });
    }

    if !sketch.has_constraints {
        let qid = derive_id64(b"forecast_question_v1", b"clarify:logic_puzzle:constraints");
        let txt = "What constraints relate the entities (for example: A != B, A before B, exactly one rule)?";
        return Some(PuzzleClarifyV1 {
            question_id: qid,
            score: 8_500,
            text: txt.to_string(),
            kind: PuzzleClarifyKindV1::NeedConstraints,
        });
    }

    None
}

/// Parsed clarification reply for a pending puzzle sketch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PuzzleClarifyReplyV1 {
    /// Variable/entity names provided in the reply (canonical order: lexical asc, unique).
    pub var_names: Vec<String>,
    /// Numeric domain range hint, inclusive (lo, hi).
    pub domain_range: Option<(i32, i32)>,
    /// Optional shape hint explicitly stated by the user.
    pub shape: Option<PuzzleShapeHintV1>,
    /// True if the reply includes explicit constraint operators.
    pub has_constraints: bool,
}

fn normalize_reply_var_token(tok: &str) -> Option<String> {
    let t = tok.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '-' && c != '\'');
    if t.is_empty() {
        return None;
    }
    if is_small_hex_like(t) {
        return None;
    }

    // Single-letter variables: allow both upper and lower (reply context).
    if t.len() == 1 {
        let b = t.as_bytes()[0];
        if (b'a' <= b && b <= b'z') || (b'A' <= b && b <= b'Z') {
            return Some(((b as char).to_ascii_uppercase()).to_string());
        }
        return None;
    }

    // Words: allow ASCII letters plus '-' and '''.
    if t.len() > 24 {
        return None;
    }
    let b = t.as_bytes();
    if !((b'a' <= b[0] && b[0] <= b'z') || (b'A' <= b[0] && b[0] <= b'Z')) {
        return None;
    }
    for &x in b {
        if (b'a' <= x && x <= b'z') || (b'A' <= x && x <= b'Z') || x == b'-' || x == b'\'' {
            continue;
        }
        return None;
    }

    // Avoid common function words.
    match t.to_ascii_lowercase().as_str() {
        "each" | "every" | "the" | "a" | "an" | "if" | "then" | "and" | "or" | "but" => {
            return None
        }
        _ => {}
    }

    // Canonicalize: if the token is all-lowercase, titlecase the first letter.
    let all_lower = b
        .iter()
        .all(|&x| (b'a' <= x && x <= b'z') || x == b'-' || x == b'\'');
    if all_lower {
        let mut chars = t.chars();
        if let Some(first) = chars.next() {
            let mut s2 = String::new();
            s2.push(first.to_ascii_uppercase());
            s2.push_str(chars.as_str());
            return Some(s2);
        }
        return None;
    }

    Some(t.to_string())
}

fn parse_shape_hint_from_reply(text: &str) -> Option<PuzzleShapeHintV1> {
    let lt = text.to_ascii_lowercase();
    let has_ordering = lt.contains("ordering")
        || lt.contains("order")
        || lt.contains("arrange")
        || lt.contains("position")
        || lt.contains("before")
        || lt.contains("after")
        || lt.contains("left")
        || lt.contains("right")
        || lt.contains("seating")
        || lt.contains("line");

    let has_matching = lt.contains("matching")
        || lt.contains("match")
        || lt.contains("category")
        || lt.contains("categories")
        || lt.contains("assign")
        || lt.contains("pair")
        || lt.contains("pairs");

    if has_ordering && !has_matching {
        return Some(PuzzleShapeHintV1::Ordering);
    }
    if has_matching && !has_ordering {
        return Some(PuzzleShapeHintV1::Matching);
    }
    None
}

fn is_shape_hint_token(tok_lc: &str) -> bool {
    match tok_lc {
        "ordering" | "order" | "arrange" | "arrangement" | "position" | "positions" | "seating"
        | "line" => true,
        "matching" | "match" | "category" | "categories" | "assign" | "pair" | "pairs" => true,
        _ => false,
    }
}

/// Parse a short clarification reply and extract any explicit fields.
///
/// This is intentionally conservative: it does not attempt full constraint parsing.
pub fn parse_puzzle_clarify_reply_v1(text: &str, max_vars: usize) -> PuzzleClarifyReplyV1 {
    let mut vars: Vec<String> = Vec::new();

    let shape = parse_shape_hint_from_reply(text);
    let shape_filter_enabled = shape.is_some();

    // Prefer comma-separated lists for variables.
    if text.contains(',') {
        for p in text.split(',') {
            for t in p.split_whitespace() {
                if shape_filter_enabled {
                    let tl = t
                        .trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '-' && c != '\'')
                        .to_ascii_lowercase();
                    if !tl.is_empty() && is_shape_hint_token(&tl) {
                        continue;
                    }
                }
                if let Some(v) = normalize_reply_var_token(t) {
                    vars.push(v);
                    if vars.len() >= max_vars {
                        break;
                    }
                }
            }
            if vars.len() >= max_vars {
                break;
            }
        }
    } else {
        for t in text.split_whitespace() {
            if shape_filter_enabled {
                let tl = t
                    .trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '-' && c != '\'')
                    .to_ascii_lowercase();
                if !tl.is_empty() && is_shape_hint_token(&tl) {
                    continue;
                }
            }
            if let Some(v) = normalize_reply_var_token(t) {
                vars.push(v);
                if vars.len() >= max_vars {
                    break;
                }
            }
        }
    }

    vars.sort();
    vars.dedup();
    if vars.len() > max_vars {
        vars.truncate(max_vars);
    }

    let domain_range = find_numeric_range_hint(text);
    let has_constraints = has_constraint_operators(text);

    PuzzleClarifyReplyV1 {
        var_names: vars,
        domain_range,
        shape,
        has_constraints,
    }
}

/// Merge a pending sketch with a clarification reply.
///
/// The merge is deterministic and bounded. Existing fields are preserved unless the
/// reply provides an explicit missing value.
pub fn merge_puzzle_sketch_with_reply_v1(
    prev: &PuzzleSketchV1,
    reply: &PuzzleClarifyReplyV1,
    max_vars: usize,
) -> PuzzleSketchV1 {
    let mut out = prev.clone();

    if !reply.var_names.is_empty() {
        for v in reply.var_names.iter() {
            out.var_names.push(v.clone());
        }
        out.var_names.sort();
        out.var_names.dedup();
        if out.var_names.len() > max_vars {
            out.var_names.truncate(max_vars);
        }
    }

    if out.domain_range.is_none() {
        if let Some(dr) = reply.domain_range {
            out.domain_range = Some(dr);
        }
    }

    if out.shape == PuzzleShapeHintV1::Unknown {
        if let Some(sh) = reply.shape {
            out.shape = sh;
        }
    }

    out.has_constraints = out.has_constraints || reply.has_constraints;

    // Once we have a pending sketch, keep logic-puzzle likelihood true.
    out.is_logic_puzzle_likely = true;

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sketch_is_deterministic_without_lexicon() {
        let cfg = PuzzleSketchCfgV1::default();
        let a = build_puzzle_sketch_v1("A, B, C are numbers 1..3. A != B.", None, None, cfg);
        let b = build_puzzle_sketch_v1("A, B, C are numbers 1..3. A != B.", None, None, cfg);
        assert_eq!(a, b);
        assert!(a.is_logic_puzzle_likely);
        assert!(a.domain_range.is_some());
        assert!(a.has_constraints);
    }

    #[test]
    fn clarify_prefers_vars_then_domain() {
        let cfg = PuzzleSketchCfgV1::default();
        let s = build_puzzle_sketch_v1("Each person has a different fruit.", None, None, cfg);
        assert!(s.is_logic_puzzle_likely);
        let q = choose_puzzle_clarify_question_v1(&s).expect("clarify");
        assert_eq!(q.kind, PuzzleClarifyKindV1::NeedVars);

        let s2 = build_puzzle_sketch_v1(
            "Alice Bob Carol each has a different fruit.",
            None,
            None,
            cfg,
        );
        let q2 = choose_puzzle_clarify_question_v1(&s2).expect("clarify");
        assert_eq!(q2.kind, PuzzleClarifyKindV1::NeedDomain);
    }

    #[test]
    fn parse_reply_extracts_vars_domain_shape() {
        let r1 = parse_puzzle_clarify_reply_v1("Alice,Bob,carol", 16);
        assert_eq!(
            r1.var_names,
            vec!["Alice".to_string(), "Bob".to_string(), "Carol".to_string()]
        );

        let r2 = parse_puzzle_clarify_reply_v1("1..3", 16);
        assert_eq!(r2.domain_range, Some((1, 3)));

        let r3 = parse_puzzle_clarify_reply_v1("ordering", 16);
        assert_eq!(r3.shape, Some(PuzzleShapeHintV1::Ordering));

        let r3b = parse_puzzle_clarify_reply_v1("Alice Bob ordering", 16);
        assert_eq!(r3b.var_names, vec!["Alice".to_string(), "Bob".to_string()]);
        assert_eq!(r3b.shape, Some(PuzzleShapeHintV1::Ordering));

        let r4 = parse_puzzle_clarify_reply_v1("matching", 16);
        assert_eq!(r4.shape, Some(PuzzleShapeHintV1::Matching));
    }

    #[test]
    fn merge_reply_fills_missing_fields() {
        let cfg = PuzzleSketchCfgV1::default();
        let prev = build_puzzle_sketch_v1("Alice Bob Carol are numbers 1..3", None, None, cfg);
        assert!(prev.is_logic_puzzle_likely);
        assert_eq!(prev.shape, PuzzleShapeHintV1::Unknown);
        assert!(prev.domain_range.is_some());
        assert!(!prev.has_constraints);

        let reply = parse_puzzle_clarify_reply_v1("ordering", 16);
        let merged = merge_puzzle_sketch_with_reply_v1(&prev, &reply, 16);
        assert_eq!(merged.shape, PuzzleShapeHintV1::Ordering);
        assert_eq!(merged.domain_range, prev.domain_range);
        assert_eq!(merged.var_names, prev.var_names);
    }
}
