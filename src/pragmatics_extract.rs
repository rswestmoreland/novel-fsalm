// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Pragmatics extractor.
//!
//! This module implements a rules-first, deterministic extractor for
//! [`crate::pragmatics_frame::PragmaticsFrameV1`].
//!
//! Design goals:
//! - CPU-only, integer-only.
//! - Bitwise deterministic given identical input text + config.
//! - Bounded memory: O(n) byte scan and O(t) token scan.
//! - No unsafe.

use crate::frame::Id64;
use crate::lexicon::{LemmaId, POS_VERB};
use crate::lexicon_expand_lookup::LexiconExpandLookupV1;
use crate::lexicon_neighborhoods::{lemma_pos_mask, LexiconCueNeighborhoodsV1};
use crate::pragmatics_frame::{
    IntentFlagsV1, PragmaticsFrameV1, PragmaticsFrameV1ValidateError, RhetoricModeV1,
    INTENT_FLAG_HAS_CODE, INTENT_FLAG_HAS_COMPARE_TARGETS, INTENT_FLAG_HAS_CONSTRAINTS,
    INTENT_FLAG_HAS_FOCUS_EXAMPLE, INTENT_FLAG_HAS_FOCUS_STEPS, INTENT_FLAG_HAS_FOCUS_SUMMARY,
    INTENT_FLAG_HAS_MATH, INTENT_FLAG_HAS_QUESTION, INTENT_FLAG_HAS_REQUEST,
    INTENT_FLAG_IS_COMPARE_REQUEST, INTENT_FLAG_IS_EXPLAIN_REQUEST, INTENT_FLAG_IS_FOLLOW_UP,
    INTENT_FLAG_IS_LOGIC_PUZZLE, INTENT_FLAG_IS_META_PROMPT, INTENT_FLAG_IS_PROBLEM_SOLVE,
    INTENT_FLAG_IS_RECOMMEND_REQUEST, INTENT_FLAG_IS_SUMMARIZE_REQUEST,
    INTENT_FLAG_SAFETY_SENSITIVE, PRAGMATICS_FRAME_V1_VERSION,
};
use crate::prompt_pack::PromptPack;
use crate::tokenizer::{term_id_from_token, TokenIter, TokenizerCfg};

use std::error::Error;
use std::fmt;

/// Configuration for [`extract_pragmatics_frame_v1`].
#[derive(Clone, Debug)]
pub struct PragmaticsExtractCfg<'a> {
    /// Tokenizer configuration used for cue matching.
    pub tokenizer_cfg: TokenizerCfg,

    /// Optional lexicon expansion view used for lemma-id matching.
    pub lexicon_view: Option<&'a LexiconExpandLookupV1>,

    /// Optional precomputed lexicon cue neighborhoods.
    pub lexicon_cues: Option<&'a LexiconCueNeighborhoodsV1>,

    /// Maximum lemma ids to consider per token when mapping a token to lemma ids.
    pub lemma_ids_cap_per_token: usize,
}

impl<'a> Default for PragmaticsExtractCfg<'a> {
    fn default() -> Self {
        PragmaticsExtractCfg {
            tokenizer_cfg: TokenizerCfg::default(),
            lexicon_view: None,
            lexicon_cues: None,
            lemma_ids_cap_per_token: 4,
        }
    }
}

/// Errors produced by the pragmatics extractor.
#[derive(Debug)]
pub enum PragmaticsExtractError {
    /// The produced frame failed [`PragmaticsFrameV1::validate`].
    Validate {
        /// Validation error.
        err: PragmaticsFrameV1ValidateError,
    },
}

impl fmt::Display for PragmaticsExtractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PragmaticsExtractError::Validate { err } => {
                write!(f, "pragmatics frame validate failed: {err}")
            }
        }
    }
}

impl Error for PragmaticsExtractError {}

#[derive(Clone, Copy, Debug, Default)]
struct PunctStats {
    exclamations: u16,
    questions: u16,
    ellipses: u16,
    repeat_punct_runs: u16,
    quotes: u16,
    ascii_only: u8,
}

fn sat_u16_add(acc: u16, add: u32) -> u16 {
    let a = u32::from(acc);
    let v = a.saturating_add(add);
    if v > u32::from(u16::MAX) {
        u16::MAX
    } else {
        v as u16
    }
}

fn clamp_u16_0_1000(v: u32) -> u16 {
    if v > 1000 {
        1000
    } else {
        v as u16
    }
}

fn clamp_u16_0_1000_i32(v: i32) -> u16 {
    if v <= 0 {
        0
    } else if v >= 1000 {
        1000
    } else {
        v as u16
    }
}

fn clamp_i16_m1000_1000(v: i32) -> i16 {
    if v > 1000 {
        1000
    } else if v < -1000 {
        -1000
    } else {
        v as i16
    }
}

fn is_all_caps_ascii_word(tok: &str) -> bool {
    let b = tok.as_bytes();
    if b.len() < 2 {
        return false;
    }
    for &x in b {
        if !(b'A'..=b'Z').contains(&x) {
            return false;
        }
    }
    true
}

fn contains_subslice(hay: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    if hay.len() < needle.len() {
        return false;
    }
    for i in 0..=(hay.len() - needle.len()) {
        if &hay[i..i + needle.len()] == needle {
            return true;
        }
    }
    false
}

fn scan_punct_stats(text: &str) -> PunctStats {
    let bytes = text.as_bytes();
    let mut st = PunctStats::default();
    st.ascii_only = 1;

    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        if b >= 0x80 {
            st.ascii_only = 0;
        }

        if b == b'"' || b == b'\'' {
            st.quotes = sat_u16_add(st.quotes, 1);
        }

        if b == b'.' {
            if i + 2 < bytes.len() && bytes[i + 1] == b'.' && bytes[i + 2] == b'.' {
                st.ellipses = sat_u16_add(st.ellipses, 1);
                i += 3;
                continue;
            }
        }

        if b == b'!' || b == b'?' {
            let ch = b;
            let mut j = i;
            while j < bytes.len() && bytes[j] == ch {
                j += 1;
            }
            let run_len = (j - i) as u32;
            if ch == b'!' {
                st.exclamations = sat_u16_add(st.exclamations, run_len);
            } else {
                st.questions = sat_u16_add(st.questions, run_len);
            }
            if run_len >= 2 {
                st.repeat_punct_runs = sat_u16_add(st.repeat_punct_runs, 1);
            }
            i = j;
            continue;
        }

        i += 1;
    }

    st
}

fn make_ids(cfg: TokenizerCfg, words: &[&'static str]) -> Vec<u64> {
    let mut v: Vec<u64> = Vec::with_capacity(words.len());
    for &w in words {
        v.push(term_id_from_token(w, cfg).0 .0);
    }
    v.sort_unstable();
    v.dedup();
    v
}

fn make_pairs(cfg: TokenizerCfg, pairs: &[(&'static str, &'static str)]) -> Vec<u128> {
    let mut v: Vec<u128> = Vec::with_capacity(pairs.len());
    for &(a, b) in pairs {
        let ia = term_id_from_token(a, cfg).0 .0;
        let ib = term_id_from_token(b, cfg).0 .0;
        let key = (u128::from(ia) << 64) | u128::from(ib);
        v.push(key);
    }
    v.sort_unstable();
    v.dedup();
    v
}

fn in_ids(ids: &[u64], x: u64) -> bool {
    ids.binary_search(&x).is_ok()
}

fn in_pairs(pairs: &[u128], a: u64, b: u64) -> bool {
    let key = (u128::from(a) << 64) | u128::from(b);
    pairs.binary_search(&key).is_ok()
}

fn lemma_in_sorted(ids: &[LemmaId], x: LemmaId) -> bool {
    let xv: u64 = (x.0).0;
    ids.binary_search_by(|a| ((a.0).0).cmp(&xv)).is_ok()
}

fn token_hits_lexicon_cues(
    view: &LexiconExpandLookupV1,
    cues: &LexiconCueNeighborhoodsV1,
    tok: &str,
    cap_lemma_ids: usize,
    scratch: &mut Vec<LemmaId>,
) -> (bool, bool) {
    if cap_lemma_ids == 0 {
        return (false, false);
    }
    let key = crate::lexicon::derive_lemma_key_id(tok);
    view.lemma_ids_for_key_into(key, cap_lemma_ids, scratch);
    let mut ps = false;
    let mut lp = false;
    for &lid in scratch.iter() {
        if !ps && lemma_in_sorted(&cues.problem_solve, lid) {
            ps = true;
        }
        if !lp && lemma_in_sorted(&cues.logic_puzzle, lid) {
            lp = true;
        }
        if ps && lp {
            break;
        }
    }
    (ps, lp)
}

fn scan_has_math(text: &str, tok_cfg: TokenizerCfg) -> bool {
    let bytes = text.as_bytes();
    let mut has_digit = false;
    let mut has_op = false;
    for &b in bytes {
        if (b'0'..=b'9').contains(&b) {
            has_digit = true;
        }
        if matches!(b, b'+' | b'-' | b'*' | b'/' | b'=' | b'^') {
            has_op = true;
        }
    }

    if has_digit && has_op {
        return true;
    }

    // Keyword fallback (token-based).
    let kw = make_ids(tok_cfg, &["sqrt", "sin", "cos", "tan", "log", "ln"]);
    for sp in TokenIter::new(text) {
        let tok = &text[sp.start..sp.end];
        let id = term_id_from_token(tok, tok_cfg).0 .0;
        if in_ids(&kw, id) {
            return true;
        }
    }

    false
}

fn scan_has_code(text: &str, tok_cfg: TokenizerCfg) -> bool {
    let bytes = text.as_bytes();
    if contains_subslice(bytes, b"```") {
        return true;
    }

    // Cheap raw cues.
    let mut has_braces = false;
    let mut has_semi = false;
    for &b in bytes {
        if b == b'{' || b == b'}' {
            has_braces = true;
        }
        if b == b';' {
            has_semi = true;
        }
    }
    if has_braces && has_semi {
        return true;
    }

    // Keyword fallback (token-based).
    let kw = make_ids(
        tok_cfg,
        &[
            "fn",
            "pub",
            "use",
            "let",
            "const",
            "class",
            "def",
            "import",
            "return",
            "println",
            "panic",
            "stacktrace",
            "exception",
        ],
    );

    for sp in TokenIter::new(text) {
        let tok = &text[sp.start..sp.end];
        let id = term_id_from_token(tok, tok_cfg).0 .0;
        if in_ids(&kw, id) {
            return true;
        }
    }

    false
}

/// Extract a [`PragmaticsFrameV1`] from a single message.
///
/// This function is rules-only in v1. It derives:
/// - punctuation and emphasis summaries via a single byte scan
/// - cue counts and intent flags via tokenization and stable TermIds
/// - coarse scores (temperature, politeness, etc.) via integer arithmetic
pub fn extract_pragmatics_frame_v1<'a>(
    source_id: Id64,
    msg_ix: u32,
    text: &str,
    cfg: &PragmaticsExtractCfg<'a>,
) -> Result<PragmaticsFrameV1, PragmaticsExtractError> {
    let bl_u32 = if (text.len() as u64) > (u32::MAX as u64) {
        u32::MAX
    } else {
        text.len() as u32
    };

    let punct = scan_punct_stats(text);

    // Cue ids. Keep the lists conservative and ASCII-only.
    let tok_cfg = cfg.tokenizer_cfg;

    let wh_words = make_ids(
        tok_cfg,
        &["what", "why", "how", "when", "where", "who", "which"],
    );

    let request_words = make_ids(tok_cfg, &["please"]);
    let request_pairs = make_pairs(
        tok_cfg,
        &[("can", "you"), ("could", "you"), ("would", "you")],
    );

    let constraints_words = make_ids(
        tok_cfg,
        &[
            "must", "should", "avoid", "never", "only", "require", "requires",
        ],
    );
    let constraints_pairs = make_pairs(tok_cfg, &[("do", "not")]);

    let meta_words = make_ids(
        tok_cfg,
        &["system", "assistant", "model", "prompt", "chatgpt", "gpt"],
    );

    let follow_first = make_ids(tok_cfg, &["and", "also", "so"]);
    let follow_pairs = make_pairs(tok_cfg, &[("what", "about"), ("how", "about")]);

    let hedge_words = make_ids(
        tok_cfg,
        &["maybe", "perhaps", "probably", "likely", "kinda", "sorta"],
    );
    let hedge_pairs = make_pairs(tok_cfg, &[("i", "think"), ("kind", "of")]);

    let intens_words = make_ids(tok_cfg, &["very", "extremely", "super", "really", "so"]);

    let profanity_words = make_ids(tok_cfg, &["damn", "shit", "fuck"]);
    let apology_words = make_ids(tok_cfg, &["sorry", "apologies"]);
    let gratitude_words = make_ids(tok_cfg, &["thanks", "thank", "appreciate"]);
    let insult_words = make_ids(tok_cfg, &["idiot", "stupid", "moron"]);

    let safety_words = make_ids(tok_cfg, &["suicide", "self-harm", "kill", "murder"]);

    let brainstorm_words = make_ids(tok_cfg, &["brainstorm", "ideas", "options"]);
    let brainstorm_pairs = make_pairs(tok_cfg, &[("what", "if")]);

    let debate_words = make_ids(tok_cfg, &["debate", "argue", "argument", "prove", "refute"]);

    let first_person_words = make_ids(tok_cfg, &["i", "me", "my", "mine"]);
    let negative_words = make_ids(
        tok_cfg,
        &["hate", "angry", "frustrated", "annoyed", "upset"],
    );
    let problem_words = make_ids(tok_cfg, &["no", "not", "never", "none", "missing", "empty"]);

    let compare_words = make_ids(tok_cfg, &["compare", "versus", "vs", "difference"]);
    let compare_pairs = make_pairs(tok_cfg, &[("better", "than"), ("compare", "with")]);
    let recommend_words = make_ids(tok_cfg, &["recommend", "recommended", "best"]);
    let recommend_pairs = make_pairs(tok_cfg, &[("which", "best"), ("what", "best")]);
    let summarize_words = make_ids(
        tok_cfg,
        &["summarize", "summary", "recap", "overview", "tldr"],
    );
    let summarize_pairs = make_pairs(tok_cfg, &[("sum", "up")]);
    let explain_words = make_ids(tok_cfg, &["explain", "walkthrough", "reasoning"]);
    let explain_pairs = make_pairs(
        tok_cfg,
        &[("walk", "through"), ("explain", "why"), ("explain", "how")],
    );

    let compare_target_words = make_ids(tok_cfg, &["and", "or", "between"]);
    let compare_target_explicit_words = make_ids(tok_cfg, &["vs", "versus", "between"]);
    let focus_summary_words = make_ids(tok_cfg, &["brief", "short", "overview", "recap", "tldr"]);
    let focus_summary_pairs = make_pairs(tok_cfg, &[("high", "level"), ("sum", "up")]);
    let focus_steps_words = make_ids(
        tok_cfg,
        &["steps", "step", "detailed", "detail", "walkthrough"],
    );
    let focus_steps_pairs = make_pairs(
        tok_cfg,
        &[("step", "by"), ("next", "steps"), ("walk", "through")],
    );
    let focus_example_words = make_ids(tok_cfg, &["example", "sample", "demo", "illustration"]);
    let focus_example_pairs = make_pairs(tok_cfg, &[("for", "example")]);
    let option_words = make_ids(tok_cfg, &["option"]);
    let token_a = term_id_from_token("a", tok_cfg).0 .0;
    let token_b = term_id_from_token("b", tok_cfg).0 .0;

    let imperative_first_words = make_ids(
        tok_cfg,
        &[
            "do",
            "make",
            "show",
            "tell",
            "give",
            "write",
            "explain",
            "help",
            "build",
            "implement",
            "fix",
        ],
    );

    // Token scan.
    let lex_view = cfg.lexicon_view;
    let lex_cues = cfg.lexicon_cues;
    let cap_lemma_ids = cfg.lemma_ids_cap_per_token;

    let mut caps_words: u16 = 0;

    let mut lex_problem_hits: u16 = 0;
    let mut lex_logic_hits: u16 = 0;
    let mut lemma_scratch: Vec<LemmaId> = Vec::new();
    let mut first_tok_text: Option<String> = None;

    let mut hedge_count: u16 = 0;
    let mut intensifier_count: u16 = 0;
    let mut profanity_count: u16 = 0;
    let mut apology_count: u16 = 0;
    let mut gratitude_count: u16 = 0;
    let mut insult_count: u16 = 0;

    let mut has_constraints = false;
    let mut has_request = false;
    let mut has_meta = false;
    let mut is_follow_up = false;
    let mut is_brainstorm = false;
    let mut is_debate = false;

    let mut first_person = false;
    let mut negative_cue = false;
    let mut problem_cue = false;
    let mut compare_cue = false;
    let mut compare_target_cue = false;
    let mut recommend_cue = false;
    let mut summarize_cue = false;
    let mut explain_cue = false;
    let mut focus_summary_cue = false;
    let mut focus_steps_cue = false;
    let mut focus_example_cue = false;
    let mut saw_compare_word = false;
    let mut saw_option_a = false;
    let mut saw_option_b = false;
    let mut wh_any = false;

    let mut first_id: Option<u64> = None;
    let mut second_id: Option<u64> = None;
    let mut prev_id: Option<u64> = None;

    let mut flags: IntentFlagsV1 = 0;

    for sp in TokenIter::new(text) {
        let tok = &text[sp.start..sp.end];
        if is_all_caps_ascii_word(tok) {
            caps_words = sat_u16_add(caps_words, 1);
        }

        let id = term_id_from_token(tok, tok_cfg).0 .0;

        if let (Some(view), Some(cues)) = (lex_view, lex_cues) {
            let (ps, lp) =
                token_hits_lexicon_cues(view, cues, tok, cap_lemma_ids, &mut lemma_scratch);
            if ps {
                lex_problem_hits = sat_u16_add(lex_problem_hits, 1);
            }
            if lp {
                lex_logic_hits = sat_u16_add(lex_logic_hits, 1);
            }
        }

        if first_id.is_none() {
            first_id = Some(id);
            first_tok_text = Some(tok.to_string());
        } else if second_id.is_none() {
            second_id = Some(id);
        }

        if in_ids(&meta_words, id) {
            has_meta = true;
        }

        if in_ids(&constraints_words, id) {
            has_constraints = true;
        }

        if in_ids(&request_words, id) {
            has_request = true;
        }

        if in_ids(&hedge_words, id) {
            hedge_count = sat_u16_add(hedge_count, 1);
        }

        if in_ids(&intens_words, id) {
            intensifier_count = sat_u16_add(intensifier_count, 1);
        }

        if in_ids(&profanity_words, id) {
            profanity_count = sat_u16_add(profanity_count, 1);
        }

        if in_ids(&apology_words, id) {
            apology_count = sat_u16_add(apology_count, 1);
        }

        if in_ids(&gratitude_words, id) {
            gratitude_count = sat_u16_add(gratitude_count, 1);
        }

        if in_ids(&insult_words, id) {
            insult_count = sat_u16_add(insult_count, 1);
        }

        if in_ids(&safety_words, id) {
            flags |= INTENT_FLAG_SAFETY_SENSITIVE;
        }

        if in_ids(&brainstorm_words, id) {
            is_brainstorm = true;
        }

        if in_ids(&debate_words, id) {
            is_debate = true;
        }

        if in_ids(&first_person_words, id) {
            first_person = true;
        }

        if in_ids(&negative_words, id) {
            negative_cue = true;
        }

        if in_ids(&problem_words, id) {
            problem_cue = true;
        }
        if in_ids(&compare_words, id) {
            compare_cue = true;
            saw_compare_word = true;
        }
        if in_ids(&compare_target_explicit_words, id) {
            compare_target_cue = true;
        }
        if saw_compare_word && in_ids(&compare_target_words, id) {
            compare_target_cue = true;
        }
        if let Some(p) = prev_id {
            if in_ids(&option_words, p) {
                if id == token_a {
                    saw_option_a = true;
                }
                if id == token_b {
                    saw_option_b = true;
                }
            }
        }
        if in_ids(&recommend_words, id) {
            recommend_cue = true;
        }
        if in_ids(&summarize_words, id) {
            summarize_cue = true;
            focus_summary_cue = true;
        }
        if in_ids(&explain_words, id) {
            explain_cue = true;
            focus_steps_cue = true;
        }
        if in_ids(&focus_summary_words, id) {
            focus_summary_cue = true;
        }
        if in_ids(&focus_steps_words, id) {
            focus_steps_cue = true;
        }
        if in_ids(&focus_example_words, id) {
            focus_example_cue = true;
        }

        if in_ids(&wh_words, id) {
            wh_any = true;
        }

        if let Some(p) = prev_id {
            if in_pairs(&hedge_pairs, p, id) {
                hedge_count = sat_u16_add(hedge_count, 1);
            }

            if in_pairs(&constraints_pairs, p, id) {
                has_constraints = true;
            }

            if in_pairs(&brainstorm_pairs, p, id) {
                is_brainstorm = true;
            }
            if in_pairs(&compare_pairs, p, id) {
                compare_cue = true;
                saw_compare_word = true;
            }
            if in_pairs(&recommend_pairs, p, id) {
                recommend_cue = true;
            }
            if in_pairs(&summarize_pairs, p, id) {
                summarize_cue = true;
                focus_summary_cue = true;
            }
            if in_pairs(&explain_pairs, p, id) {
                explain_cue = true;
                focus_steps_cue = true;
            }
            if in_pairs(&focus_summary_pairs, p, id) {
                focus_summary_cue = true;
            }
            if in_pairs(&focus_steps_pairs, p, id) {
                focus_steps_cue = true;
            }
            if in_pairs(&focus_example_pairs, p, id) {
                focus_example_cue = true;
            }
        }

        prev_id = Some(id);
    }

    // Request pairs at start.
    if let (Some(a), Some(b)) = (first_id, second_id) {
        if in_pairs(&request_pairs, a, b) {
            has_request = true;
        }
        if in_pairs(&follow_pairs, a, b) {
            is_follow_up = true;
        }
    }

    if saw_option_a && saw_option_b {
        compare_target_cue = true;
    }

    // Intent flags.
    // Treat WH-words as question cues when the message is already request-like or constraint-like.
    // This avoids requiring a trailing '?' for common forms like "please help me understand why ...".
    let first_is_wh = match first_id {
        Some(fid) => in_ids(&wh_words, fid),
        None => false,
    };

    if punct.questions != 0 || first_is_wh || (wh_any && (has_request || has_constraints)) {
        flags |= INTENT_FLAG_HAS_QUESTION;
    }

    if has_request {
        flags |= INTENT_FLAG_HAS_REQUEST;
    }

    // Imperative heuristic: first token is an imperative verb and there is no '?'.
    if punct.questions == 0 {
        if let Some(fid) = first_id {
            if in_ids(&imperative_first_words, fid) {
                flags |= INTENT_FLAG_HAS_REQUEST;
            }
        }

        // Lexicon-assisted imperative heuristic (optional): if the first token maps to a verb lemma,
        // treat it as a request. This is bounded (cap lemma ids per token) and deterministic.
        if (flags & (INTENT_FLAG_HAS_REQUEST | INTENT_FLAG_HAS_QUESTION)) == 0 {
            if let (Some(view), Some(ft)) = (lex_view, first_tok_text.as_deref()) {
                let key = crate::lexicon::derive_lemma_key_id(ft);
                view.lemma_ids_for_key_into(key, cap_lemma_ids, &mut lemma_scratch);
                let mut is_verb = false;
                for &lid in lemma_scratch.iter() {
                    if let Some(pm) = lemma_pos_mask(view, lid) {
                        if (pm & POS_VERB) != 0 {
                            is_verb = true;
                            break;
                        }
                    }
                }
                if is_verb {
                    flags |= INTENT_FLAG_HAS_REQUEST;
                }
            }
        }
    }

    if has_constraints {
        flags |= INTENT_FLAG_HAS_CONSTRAINTS;
    }

    // Lexicon-driven intent inference (optional).
    // These are conservative: lexicon hits must co-occur with request/question/constraints cues.
    if lex_view.is_some() && lex_cues.is_some() {
        let has_qr = (flags
            & (INTENT_FLAG_HAS_QUESTION | INTENT_FLAG_HAS_REQUEST | INTENT_FLAG_HAS_CONSTRAINTS))
            != 0;
        if lex_problem_hits != 0 && has_qr {
            flags |= INTENT_FLAG_IS_PROBLEM_SOLVE;
        }
        if lex_logic_hits != 0 {
            let has_struct =
                (flags & (INTENT_FLAG_HAS_CONSTRAINTS | INTENT_FLAG_HAS_QUESTION)) != 0;
            if has_struct || lex_logic_hits >= 2 {
                flags |= INTENT_FLAG_IS_LOGIC_PUZZLE;
            }
        }
    }

    // Conservative fallback: if the message is a request-like question and contains a negative cue,
    // treat it as generalized problem solving (troubleshooting / retrospection). This keeps behavior
    // useful even when the lexicon snapshot does not cover the key lemmas.
    if (flags & INTENT_FLAG_IS_PROBLEM_SOLVE) == 0 {
        let has_q = (flags & INTENT_FLAG_HAS_QUESTION) != 0;
        let has_r = (flags & INTENT_FLAG_HAS_REQUEST) != 0;
        if has_q && has_r && (problem_cue || negative_cue) {
            flags |= INTENT_FLAG_IS_PROBLEM_SOLVE;
        }
    }

    if scan_has_math(text, tok_cfg) {
        flags |= INTENT_FLAG_HAS_MATH;
    }

    if scan_has_code(text, tok_cfg) {
        flags |= INTENT_FLAG_HAS_CODE;
    }

    if has_meta {
        flags |= INTENT_FLAG_IS_META_PROMPT;
    }

    if !is_follow_up {
        if bl_u32 <= 80 && (flags & INTENT_FLAG_HAS_QUESTION) != 0 {
            if let Some(fid) = first_id {
                if in_ids(&follow_first, fid) {
                    is_follow_up = true;
                }
            }
        }
    }

    if is_follow_up {
        flags |= INTENT_FLAG_IS_FOLLOW_UP;
    }

    let has_qr = (flags
        & (INTENT_FLAG_HAS_QUESTION | INTENT_FLAG_HAS_REQUEST | INTENT_FLAG_HAS_CONSTRAINTS))
        != 0;
    if compare_cue && has_qr {
        flags |= INTENT_FLAG_IS_COMPARE_REQUEST;
    }
    if recommend_cue && has_qr {
        flags |= INTENT_FLAG_IS_RECOMMEND_REQUEST;
    }
    if summarize_cue && has_qr {
        flags |= INTENT_FLAG_IS_SUMMARIZE_REQUEST;
    }
    if explain_cue && has_qr {
        flags |= INTENT_FLAG_IS_EXPLAIN_REQUEST;
    }
    if compare_target_cue && has_qr {
        flags |= INTENT_FLAG_HAS_COMPARE_TARGETS;
    }
    if focus_summary_cue && has_qr {
        flags |= INTENT_FLAG_HAS_FOCUS_SUMMARY;
    }
    if focus_steps_cue && has_qr {
        flags |= INTENT_FLAG_HAS_FOCUS_STEPS;
    }
    if focus_example_cue && has_qr {
        flags |= INTENT_FLAG_HAS_FOCUS_EXAMPLE;
    }

    // Emphasis score.
    let emphasis_score = clamp_u16_0_1000(
        u32::from(punct.exclamations).saturating_mul(30)
            + u32::from(punct.questions).saturating_mul(20)
            + u32::from(punct.repeat_punct_runs).saturating_mul(200)
            + u32::from(caps_words).saturating_mul(120)
            + u32::from(punct.ellipses).saturating_mul(40),
    );

    // Coarse scores.
    let mut temperature = clamp_u16_0_1000(
        150 + u32::from(punct.repeat_punct_runs).saturating_mul(200)
            + u32::from(punct.exclamations).saturating_mul(25)
            + u32::from(profanity_count).saturating_mul(180)
            + u32::from(insult_count).saturating_mul(220)
            + u32::from(caps_words).saturating_mul(80),
    );

    let mut arousal = clamp_u16_0_1000(
        150 + u32::from(emphasis_score).saturating_div(2)
            + u32::from(intensifier_count).saturating_mul(50),
    );

    let politeness_i =
        500i32 + (i32::from(gratitude_count) * 200) + (i32::from(apology_count) * 160)
            - (i32::from(profanity_count) * 300)
            - (i32::from(insult_count) * 350);
    let mut politeness = clamp_u16_0_1000_i32(politeness_i);

    let formality_i = 500i32 + (i32::from(apology_count) * 80) + (i32::from(gratitude_count) * 50)
        - (i32::from(profanity_count) * 150)
        - (i32::from(insult_count) * 150)
        - (i32::from(punct.exclamations) * 5)
        - (i32::from(punct.repeat_punct_runs) * 20);
    let mut formality = clamp_u16_0_1000_i32(formality_i);

    let mut directness_i = 500i32 - (i32::from(hedge_count) * 150);
    if (flags & INTENT_FLAG_HAS_REQUEST) != 0 {
        directness_i += 200;
    }
    if (flags & INTENT_FLAG_HAS_CONSTRAINTS) != 0 {
        directness_i += 80;
    }
    if (flags & INTENT_FLAG_HAS_QUESTION) != 0 {
        directness_i -= 50;
    }
    let mut directness = clamp_u16_0_1000_i32(directness_i);

    let mut empathy_i = 100i32;
    if (flags & INTENT_FLAG_SAFETY_SENSITIVE) != 0 {
        empathy_i += 700;
    }
    empathy_i += i32::from(apology_count) * 200;
    if negative_cue {
        empathy_i += 200;
    }
    if profanity_count != 0 || insult_count != 0 {
        empathy_i += 150;
    }
    let mut empathy_need = clamp_u16_0_1000_i32(empathy_i);

    let valence_i = 0i32 + (i32::from(gratitude_count) * 200) + (i32::from(apology_count) * 100)
        - (i32::from(profanity_count) * 250)
        - (i32::from(insult_count) * 300)
        - if negative_cue { 150 } else { 0 }
        - if (flags & INTENT_FLAG_SAFETY_SENSITIVE) != 0 {
            200
        } else {
            0
        };
    let mut valence = clamp_i16_m1000_1000(valence_i);

    // Mode selection.
    let mut mode = RhetoricModeV1::Unknown;
    if (flags & INTENT_FLAG_HAS_QUESTION) != 0 {
        mode = RhetoricModeV1::Ask;
    } else if (flags & INTENT_FLAG_HAS_REQUEST) != 0 {
        mode = RhetoricModeV1::Command;
    } else if is_debate {
        mode = RhetoricModeV1::Debate;
    } else if is_brainstorm {
        mode = RhetoricModeV1::Brainstorm;
    } else if temperature >= 700
        && (profanity_count != 0 || insult_count != 0 || (first_person && negative_cue))
    {
        mode = RhetoricModeV1::Vent;
    }

    // Mode-based adjustments. Keep the adjustments small; this is a v1 heuristic.
    match mode {
        RhetoricModeV1::Ask => {
            temperature = clamp_u16_0_1000(u32::from(temperature).saturating_sub(50));
            arousal = clamp_u16_0_1000(u32::from(arousal).saturating_sub(50));
            directness = clamp_u16_0_1000_i32(i32::from(directness) - 50);
        }
        RhetoricModeV1::Command => {
            directness = clamp_u16_0_1000_i32(i32::from(directness) + 100);
        }
        RhetoricModeV1::Vent => {
            temperature = clamp_u16_0_1000(u32::from(temperature) + 100);
            politeness = clamp_u16_0_1000_i32(i32::from(politeness) - 150);
            empathy_need = clamp_u16_0_1000_i32(i32::from(empathy_need) + 150);
            valence = clamp_i16_m1000_1000(i32::from(valence) - 100);
        }
        RhetoricModeV1::Debate => {
            formality = clamp_u16_0_1000_i32(i32::from(formality) + 50);
        }
        RhetoricModeV1::Brainstorm => {
            directness = clamp_u16_0_1000_i32(i32::from(directness) - 25);
        }
        RhetoricModeV1::Story | RhetoricModeV1::Negotiation | RhetoricModeV1::Unknown => {}
    }

    if has_meta {
        formality = clamp_u16_0_1000_i32(i32::from(formality) + 50);
    }

    let frame = PragmaticsFrameV1 {
        version: PRAGMATICS_FRAME_V1_VERSION,
        source_id,
        msg_ix,
        byte_len: bl_u32,
        ascii_only: punct.ascii_only,
        temperature,
        valence,
        arousal,
        politeness,
        formality,
        directness,
        empathy_need,
        mode,
        flags,
        exclamations: punct.exclamations,
        questions: punct.questions,
        ellipses: punct.ellipses,
        caps_words,
        repeat_punct_runs: punct.repeat_punct_runs,
        quotes: punct.quotes,
        emphasis_score,
        hedge_count,
        intensifier_count,
        profanity_count,
        apology_count,
        gratitude_count,
        insult_count,
    };

    frame
        .validate()
        .map_err(|err| PragmaticsExtractError::Validate { err })?;

    Ok(frame)
}

/// Extract pragmatics frames for all messages in a [`PromptPack`].
///
/// The returned frames are ordered by message index.
pub fn extract_pragmatics_frames_for_prompt_pack_v1<'a>(
    source_id: Id64,
    pack: &PromptPack,
    cfg: &PragmaticsExtractCfg<'a>,
) -> Result<Vec<PragmaticsFrameV1>, PragmaticsExtractError> {
    let mut out = Vec::with_capacity(pack.messages.len());
    for (ix, msg) in pack.messages.iter().enumerate() {
        let ix_u32 = if (ix as u64) > (u32::MAX as u64) {
            u32::MAX
        } else {
            ix as u32
        };
        out.push(extract_pragmatics_frame_v1(
            source_id,
            ix_u32,
            &msg.content,
            cfg,
        )?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ex(text: &str) -> PragmaticsFrameV1 {
        extract_pragmatics_frame_v1(Id64(1), 0, text, &PragmaticsExtractCfg::default()).unwrap()
    }

    #[test]
    fn extract_sets_question_mode_and_flag() {
        let f = ex("What is this?");
        assert_eq!(f.questions, 1);
        assert_eq!(f.exclamations, 0);
        assert!((f.flags & INTENT_FLAG_HAS_QUESTION) != 0);
        assert_eq!(f.mode, RhetoricModeV1::Ask);
        assert!(f.validate().is_ok());
    }

    #[test]
    fn extract_detects_request_and_constraints() {
        let f = ex("Please avoid unsafe code.");
        assert!((f.flags & INTENT_FLAG_HAS_REQUEST) != 0);
        assert!((f.flags & INTENT_FLAG_HAS_CONSTRAINTS) != 0);
        assert_eq!(f.mode, RhetoricModeV1::Command);
        assert!(f.validate().is_ok());
    }

    #[test]
    fn extract_counts_caps_and_punct_runs() {
        let f = ex("THIS IS BAD!!!");
        assert_eq!(f.exclamations, 3);
        assert_eq!(f.repeat_punct_runs, 1);
        assert_eq!(f.caps_words, 3);
        assert!(f.emphasis_score > 0);
        assert!(f.validate().is_ok());
    }

    #[test]
    fn extract_counts_gratitude_and_apology() {
        let f = ex("Thanks, sorry.");
        assert_eq!(f.gratitude_count, 1);
        assert_eq!(f.apology_count, 1);
        assert!(f.politeness > 500);
        assert!(f.valence > 0);
        assert!(f.validate().is_ok());
    }

    #[test]
    fn extract_sets_problem_solve_and_logic_puzzle_flags_with_lexicon() {
        use crate::artifact::FsArtifactStore;
        use crate::lexicon::{LemmaRowV1, POS_NOUN};
        use crate::lexicon_expand_lookup::load_lexicon_expand_lookup_v1;
        use crate::lexicon_neighborhoods::{
            build_lexicon_cue_neighborhoods_v1, LexiconNeighborhoodCfgV1,
        };
        use crate::lexicon_segment::LexiconSegmentV1;
        use crate::lexicon_segment_store::put_lexicon_segment_v1;
        use crate::lexicon_snapshot::{LexiconSnapshotEntryV1, LexiconSnapshotV1};
        use crate::lexicon_snapshot_store::put_lexicon_snapshot_v1;

        use core::sync::atomic::{AtomicUsize, Ordering};

        static TMP_SEQ: AtomicUsize = AtomicUsize::new(0);

        fn tmp_dir(name: &str) -> std::path::PathBuf {
            use std::fs;
            let mut p = std::env::temp_dir();
            p.push("fsa_lm_tests");
            let pid = std::process::id();
            let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
            p.push(format!("{}_{}_{}", name, pid, seq));
            let _ = fs::remove_dir_all(&p);
            fs::create_dir_all(&p).unwrap();
            p
        }

        let root = tmp_dir("pragmatics_extract_lex");
        let store = FsArtifactStore::new(&root).unwrap();

        let l1 = LemmaRowV1::new("diagnose", POS_VERB, 0);
        let l2 = LemmaRowV1::new("logic", POS_NOUN, 0);
        let l3 = LemmaRowV1::new("puzzle", POS_NOUN, 0);
        let l4 = LemmaRowV1::new("constraint", POS_NOUN, 0);

        let seg = LexiconSegmentV1::build_from_rows(&[l1, l2, l3, l4], &[], &[], &[]).unwrap();
        let seg_hash = put_lexicon_segment_v1(&store, &seg).unwrap();

        let mut snap = LexiconSnapshotV1::new();
        snap.entries.push(LexiconSnapshotEntryV1 {
            lex_seg: seg_hash,
            lemma_count: seg.lemma_id.len() as u32,
            sense_count: seg.sense_id.len() as u32,
            rel_count: seg.rel_from_id.len() as u32,
            pron_count: seg.pron_lemma_id.len() as u32,
        });
        let snap_hash = put_lexicon_snapshot_v1(&store, &snap).unwrap();

        let view = load_lexicon_expand_lookup_v1(&store, &snap_hash)
            .unwrap()
            .unwrap();
        let cues = build_lexicon_cue_neighborhoods_v1(&view, &LexiconNeighborhoodCfgV1::new());

        let mut cfg = PragmaticsExtractCfg::default();
        cfg.lexicon_view = Some(&view);
        cfg.lexicon_cues = Some(&cues);

        let f1 = extract_pragmatics_frame_v1(Id64(1), 0, "Please diagnose why this fails.", &cfg)
            .unwrap();
        assert!((f1.flags & INTENT_FLAG_IS_PROBLEM_SOLVE) != 0);

        let f2 = extract_pragmatics_frame_v1(Id64(1), 0, "Logic puzzle: deduce the answer.", &cfg)
            .unwrap();
        assert!((f2.flags & INTENT_FLAG_IS_LOGIC_PUZZLE) != 0);
    }

    #[test]
    fn extract_sets_compare_recommend_summary_and_explain_flags() {
        let f_compare = ex("Please compare banana and apple for speed.");
        assert!((f_compare.flags & INTENT_FLAG_IS_COMPARE_REQUEST) != 0);

        let f_recommend = ex("What is the best option to recommend here?");
        assert!((f_recommend.flags & INTENT_FLAG_IS_RECOMMEND_REQUEST) != 0);

        let f_summary = ex("Please summarize the banana notes.");
        assert!((f_summary.flags & INTENT_FLAG_IS_SUMMARIZE_REQUEST) != 0);

        let f_explain = ex("Can you explain how the banana index works?");
        assert!((f_explain.flags & INTENT_FLAG_IS_EXPLAIN_REQUEST) != 0);
    }

    #[test]
    fn extract_sets_compare_target_and_focus_flags() {
        let f_compare = ex("Please compare option a and option b.");
        assert!((f_compare.flags & INTENT_FLAG_IS_COMPARE_REQUEST) != 0);
        assert!((f_compare.flags & INTENT_FLAG_HAS_COMPARE_TARGETS) != 0);

        let f_focus = ex("Can you explain this with a short high level summary and an example?");
        assert!((f_focus.flags & INTENT_FLAG_IS_EXPLAIN_REQUEST) != 0);
        assert!((f_focus.flags & INTENT_FLAG_HAS_FOCUS_SUMMARY) != 0);
        assert!((f_focus.flags & INTENT_FLAG_HAS_FOCUS_EXAMPLE) != 0);

        let f_steps = ex("Please explain this step by step.");
        assert!((f_steps.flags & INTENT_FLAG_HAS_FOCUS_STEPS) != 0);
    }

    #[test]
    fn extract_sets_safety_sensitive_flag() {
        let f = ex("suicide");
        assert!((f.flags & INTENT_FLAG_SAFETY_SENSITIVE) != 0);
        assert!(f.empathy_need >= 700);
        assert!(f.validate().is_ok());
    }
}
