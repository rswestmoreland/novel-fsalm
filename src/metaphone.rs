// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Metaphonetic preprocessor.
//!
//! The goal of this module is *not* to implement a full phonetics engine.
//! Instead, it provides a deterministic, allocation-light mapping from a token
//! to a compact "sounds-like" code, plus a stable 64-bit id.
//!
//! Design constraints:
//! - CPU-only, deterministic (bitwise stable).
//! - Minimal allocations: code generation uses stack buffers.
//! - Integer-only: ids and frequencies are integers; no floats.
//! - Conservative: v1 focuses on ASCII/English-like tokens and ignores
//! non-ASCII letters rather than attempting transliteration.
//!
//! The code here is inspired by classic Metaphone-style rules, but simplified.
//! This is intentional: it is an educational prototype and a building block for
//! fuzzy retrieval, not a linguistics project.

use crate::frame::{Id64, MetaCodeId};
use crate::tokenizer::{TokenIter, TokenizerCfg};
use blake3;

/// Configuration for metaphone code generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetaphoneCfg {
    /// Maximum number of bytes to read from the token when building a code.
    ///
    /// This limits CPU work on pathological tokens and keeps hashing bounded.
    pub max_token_bytes: usize,
    /// Maximum length of the output metaphone code.
    ///
    /// The code bytes are ASCII uppercase letters (and the digit '0' for TH).
    pub max_code_len: usize,
}

impl Default for MetaphoneCfg {
    fn default() -> Self {
        MetaphoneCfg {
            max_token_bytes: 64,
            max_code_len: 12,
        }
    }
}

/// Metaphone code bytes (ASCII uppercase), stored inline to avoid allocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetaCode {
    /// Code bytes (valid for indices 0..len).
    pub bytes: [u8; 12],
    /// Code length.
    pub len: u8,
}

impl MetaCode {
    /// View the code bytes as a slice.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..(self.len as usize)]
    }
}

/// A metaphone id + frequency pair.
///
/// This mirrors `TermFreq` but is separate to avoid coupling v1 retrieval rules.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MetaFreq {
    /// Metaphone code id.
    pub meta: MetaCodeId,
    /// Frequency count.
    pub tf: u32,
}

fn ascii_lower(b: u8) -> u8 {
    if (b'A'..=b'Z').contains(&b) {
        b + 32
    } else {
        b
    }
}

fn ascii_is_letter(b: u8) -> bool {
    (b'a'..=b'z').contains(&b) || (b'A'..=b'Z').contains(&b)
}

fn is_vowel(b: u8) -> bool {
    matches!(b, b'a' | b'e' | b'i' | b'o' | b'u' | b'y')
}

fn push_code(out: &mut [u8], out_len: &mut usize, max_len: usize, b: u8) {
    if *out_len >= max_len {
        return;
    }
    if *out_len != 0 && out[*out_len - 1] == b {
        return;
    }
    out[*out_len] = b;
    *out_len += 1;
}

/// Compute a metaphone-style code for `token`.
///
/// The returned code uses ASCII uppercase letters and is capped at `cfg.max_code_len`.
///
/// v1 behavior:
/// - Only ASCII letters are considered. Non-ASCII bytes are ignored.
/// - Vowels are emitted only at the start (as 'A').
/// - "TH" emits the digit '0' (traditional metaphone convention).
/// - The code is *not* intended to be reversible or linguistically perfect.
pub fn metaphone_code(token: &str, cfg: MetaphoneCfg) -> MetaCode {
    // Clean input: keep only ASCII letters, lowercase them, and cap input bytes.
    let mut clean = [0u8; 64];
    let mut n = 0usize;

    for &b in token.as_bytes() {
        if n >= cfg.max_token_bytes || n >= clean.len() {
            break;
        }
        if ascii_is_letter(b) {
            clean[n] = ascii_lower(b);
            n += 1;
        }
    }

    let mut out = [0u8; 12];
    let mut out_len = 0usize;
    let max_out = cfg.max_code_len.min(out.len());

    if n == 0 || max_out == 0 {
        return MetaCode { bytes: out, len: 0 };
    }

    // Start index after silent-letter patterns.
    let mut i = 0usize;

    // Initial patterns with silent first letter.
    if n >= 2 {
        let a = clean[0];
        let b = clean[1];
        if (a == b'k' && b == b'n')
            || (a == b'g' && b == b'n')
            || (a == b'p' && b == b'n')
            || (a == b'a' && b == b'e')
            || (a == b'w' && b == b'r')
        {
            i = 1;
        }
    }

    // Initial X -> S.
    if i == 0 && clean[0] == b'x' {
        clean[0] = b's';
    }

    while i < n && out_len < max_out {
        let c = clean[i];
        let prev = if i > 0 { Some(clean[i - 1]) } else { None };
        let next = if i + 1 < n { Some(clean[i + 1]) } else { None };
        let next2 = if i + 2 < n { Some(clean[i + 2]) } else { None };

        // Skip duplicate letters in input (except 'c' which participates in digraphs).
        if let Some(p) = prev {
            if c == p && c != b'c' {
                i += 1;
                continue;
            }
        }

        match c {
            b'a' | b'e' | b'i' | b'o' | b'u' | b'y' => {
                // Emit vowels only at the start.
                if i == 0 {
                    push_code(&mut out, &mut out_len, max_out, b'A');
                }
            }

            b'b' => {
                if !(i + 1 == n && prev == Some(b'm')) {
                    push_code(&mut out, &mut out_len, max_out, b'B');
                }
            }

            b'c' => {
                // SCH -> SK.
                if prev == Some(b's') && next == Some(b'h') {
                    push_code(&mut out, &mut out_len, max_out, b'K');
                    i += 1; // skip 'h'
                } else if next == Some(b'h') {
                    push_code(&mut out, &mut out_len, max_out, b'X');
                    i += 1; // skip 'h'
                } else if matches!(next, Some(b'e') | Some(b'i') | Some(b'y')) {
                    push_code(&mut out, &mut out_len, max_out, b'S');
                } else {
                    push_code(&mut out, &mut out_len, max_out, b'K');
                }
            }

            b'd' => {
                if next == Some(b'g') && matches!(next2, Some(b'e') | Some(b'i') | Some(b'y')) {
                    push_code(&mut out, &mut out_len, max_out, b'J');
                    i += 1; // skip 'g'
                } else {
                    push_code(&mut out, &mut out_len, max_out, b'T');
                }
            }

            b'f' => push_code(&mut out, &mut out_len, max_out, b'F'),
            b'j' => push_code(&mut out, &mut out_len, max_out, b'J'),
            b'l' => push_code(&mut out, &mut out_len, max_out, b'L'),
            b'm' => push_code(&mut out, &mut out_len, max_out, b'M'),
            b'n' => push_code(&mut out, &mut out_len, max_out, b'N'),
            b'r' => push_code(&mut out, &mut out_len, max_out, b'R'),

            b'g' => {
                if next == Some(b'h') {
                    // "GH" is tricky. In many words it is silent (night, though),
                    // but in some common "AUGH"/"OUGH" patterns it maps to F (laugh, tough).
                    //
                    // v1 rule:
                    // - If the pattern is A U G H or O U G H (case-insensitive after cleaning),
                    // emit F.
                    // - Otherwise, treat GH as silent.
                    if prev == Some(b'u') && i >= 2 {
                        let p2 = clean[i - 2];
                        if p2 == b'a' || p2 == b'o' {
                            push_code(&mut out, &mut out_len, max_out, b'F');
                        }
                    }
                    i += 1; // skip 'h'
                } else if next == Some(b'n') {
                    // -gn, -gne endings often drop g.
                    if i + 1 == n - 1 {
                        // end: "gn"
                    } else if i + 2 == n - 1 && next2 == Some(b'e') {
                        // end: "gne"
                    } else if matches!(next2, Some(b'e') | Some(b'i') | Some(b'y')) {
                        push_code(&mut out, &mut out_len, max_out, b'J');
                    } else {
                        push_code(&mut out, &mut out_len, max_out, b'K');
                    }
                    // always advance normally; do not skip 'n'
                } else if matches!(next, Some(b'e') | Some(b'i') | Some(b'y')) {
                    push_code(&mut out, &mut out_len, max_out, b'J');
                } else {
                    push_code(&mut out, &mut out_len, max_out, b'K');
                }
            }

            b'h' => {
                // Emit H only if it separates a consonant and a vowel.
                let prev_v = prev.map(is_vowel).unwrap_or(false);
                let next_v = next.map(is_vowel).unwrap_or(false);
                if !prev_v && next_v {
                    push_code(&mut out, &mut out_len, max_out, b'H');
                }
            }

            b'k' => {
                if prev != Some(b'c') {
                    push_code(&mut out, &mut out_len, max_out, b'K');
                }
            }

            b'p' => {
                if next == Some(b'h') {
                    push_code(&mut out, &mut out_len, max_out, b'F');
                    i += 1; // skip 'h'
                } else {
                    push_code(&mut out, &mut out_len, max_out, b'P');
                }
            }

            b'q' => push_code(&mut out, &mut out_len, max_out, b'K'),

            b's' => {
                if next == Some(b'h') {
                    push_code(&mut out, &mut out_len, max_out, b'X');
                    i += 1; // skip 'h'
                } else if next == Some(b'i') && matches!(next2, Some(b'o') | Some(b'a')) {
                    push_code(&mut out, &mut out_len, max_out, b'X');
                    i += 2;
                } else {
                    push_code(&mut out, &mut out_len, max_out, b'S');
                }
            }

            b't' => {
                if next == Some(b'h') {
                    push_code(&mut out, &mut out_len, max_out, b'0');
                    i += 1; // skip 'h'
                } else if next == Some(b'i') && matches!(next2, Some(b'o') | Some(b'a')) {
                    push_code(&mut out, &mut out_len, max_out, b'X');
                    i += 2;
                } else if next == Some(b'c') && next2 == Some(b'h') {
                    // "tch" -> "ch"
                } else {
                    push_code(&mut out, &mut out_len, max_out, b'T');
                }
            }

            b'v' => push_code(&mut out, &mut out_len, max_out, b'F'),

            b'w' => {
                if next.map(is_vowel).unwrap_or(false) {
                    push_code(&mut out, &mut out_len, max_out, b'W');
                }
            }

            b'x' => {
                push_code(&mut out, &mut out_len, max_out, b'K');
                push_code(&mut out, &mut out_len, max_out, b'S');
            }

            b'z' => push_code(&mut out, &mut out_len, max_out, b'S'),

            _ => {}
        }

        i += 1;
    }

    MetaCode {
        bytes: out,
        len: out_len as u8,
    }
}

/// Derive a stable metaphone id for `token`.
///
/// Returns `None` if the generated code is empty (e.g., token contains no ASCII letters).
pub fn meta_code_id_from_token(token: &str, cfg: MetaphoneCfg) -> Option<MetaCodeId> {
    let code = metaphone_code(token, cfg);
    if code.len == 0 {
        return None;
    }

    let mut h = blake3::Hasher::new();
    h.update(b"meta\0");
    h.update(code.as_bytes());
    let out = h.finalize();
    let bytes = out.as_bytes();
    let mut id8 = [0u8; 8];
    id8.copy_from_slice(&bytes[0..8]);
    Some(MetaCodeId(Id64(u64::from_le_bytes(id8))))
}

/// Compute metaphone frequencies from text.
///
/// This walks tokens from the tokenizer, computes a metaphone id for each token,
/// sorts ids, and counts runs to produce `MetaFreq`.
///
/// Complexity is O(n log n) in the number of tokens.
pub fn meta_freqs_from_text(
    text: &str,
    tok_cfg: TokenizerCfg,
    meta_cfg: MetaphoneCfg,
) -> Vec<MetaFreq> {
    let mut ids: Vec<u64> = Vec::new();

    for sp in TokenIter::new(text) {
        let tok = &text[sp.start..sp.end];
        // Apply the same byte limit as tokenizer for bounded work.
        let tok = if tok.len() > meta_cfg.max_token_bytes {
            &tok[..meta_cfg.max_token_bytes]
        } else {
            tok
        };
        if let Some(mid) = meta_code_id_from_token(tok, meta_cfg) {
            ids.push(mid.0 .0);
        }
    }

    // Note: tok_cfg is currently unused in v1 because TokenIter has no config.
    // It is included so future versions can share configuration across stages.
    let _ = tok_cfg;

    ids.sort_unstable();

    let mut out: Vec<MetaFreq> = Vec::new();
    if ids.is_empty() {
        return out;
    }

    let mut cur = ids[0];
    let mut cnt: u32 = 1;

    for &x in &ids[1..] {
        if x == cur {
            if cnt != u32::MAX {
                cnt += 1;
            }
        } else {
            out.push(MetaFreq {
                meta: MetaCodeId(Id64(cur)),
                tf: cnt,
            });
            cur = x;
            cnt = 1;
        }
    }
    out.push(MetaFreq {
        meta: MetaCodeId(Id64(cur)),
        tf: cnt,
    });

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(code: MetaCode) -> String {
        core::str::from_utf8(code.as_bytes()).unwrap().to_string()
    }

    #[test]
    fn metaphone_words_basic() {
        let cfg = MetaphoneCfg::default();

        assert_eq!(s(metaphone_code("Smith", cfg)), "SM0");
        assert_eq!(s(metaphone_code("Schmidt", cfg)), "SKMT");
        assert_eq!(s(metaphone_code("Knight", cfg)), "NT");
        assert_eq!(s(metaphone_code("Phone", cfg)), "FN");
        assert_eq!(s(metaphone_code("Xavier", cfg)), "SFR");
    }

    #[test]
    fn metaphone_ignores_non_ascii_bytes() {
        let cfg = MetaphoneCfg::default();
        // Non-ASCII bytes are ignored, so this behaves like "caf".
        assert_eq!(s(metaphone_code("café", cfg)), "KF");
    }

    #[test]
    fn meta_id_is_deterministic() {
        let cfg = MetaphoneCfg::default();
        let a = meta_code_id_from_token("Smith", cfg).unwrap();
        let b = meta_code_id_from_token("Smith", cfg).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn meta_id_is_case_insensitive_for_ascii() {
        let cfg = MetaphoneCfg::default();
        let a = meta_code_id_from_token("SMITH", cfg).unwrap();
        let b = meta_code_id_from_token("smith", cfg).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn meta_freqs_counts_runs() {
        let tok_cfg = TokenizerCfg::default();
        let meta_cfg = MetaphoneCfg::default();
        let v = meta_freqs_from_text("smith smith schmidt", tok_cfg, meta_cfg);
        assert_eq!(v.len(), 2);
        let sum: u32 = v.iter().map(|x| x.tf).sum();
        assert_eq!(sum, 3);
    }
}
