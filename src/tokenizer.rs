// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Tokenization and term-id strategy.
//!
//! Goals for v1:
//! - Deterministic results (bitwise stable given identical input).
//! - Minimal allocations: tokenization yields slices into the original string.
//! - Integer-only: term ids are derived via stable hashing; term frequencies are u32.
//!
//! This module is intentionally simple and conservative. It is designed to be
//! replaced or extended later without changing the segment storage format.

use crate::frame::{Id64, TermFreq, TermId};
use blake3;

/// Tokenizer configuration.
///
/// v1 normalizes tokens by ASCII-lowercasing bytes A-Z. Non-ASCII bytes are
/// passed through unchanged. This keeps hashing deterministic without requiring
/// Unicode case-fold allocations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TokenizerCfg {
    /// Maximum token byte length to consider. Longer tokens are truncated for hashing.
    ///
    /// This prevents pathological memory and CPU usage on very long inputs.
    pub max_token_bytes: usize,
}

impl Default for TokenizerCfg {
    fn default() -> Self {
        TokenizerCfg { max_token_bytes: 64 }
    }
}

/// A token produced by the tokenizer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TokenSpan {
    /// Start byte offset in the original string.
    pub start: usize,
    /// End byte offset (exclusive) in the original string.
    pub end: usize,
}

/// Deterministic token iterator.
///
/// This iterator walks UTF-8 safely using `char_indices`. It considers a token
/// to be a run of "token characters":
/// - alphanumeric characters (Unicode-aware)
/// - ASCII underscore '_' and hyphen '-'
/// - ASCII apostrophe '\''
///
/// Everything else is treated as a delimiter.
pub struct TokenIter<'a> {
    it: core::str::CharIndices<'a>,
    cur_start: Option<usize>,
    cur_end: usize,
}

impl<'a> TokenIter<'a> {
    /// Create a new token iterator over `s`.
    pub fn new(s: &'a str) -> TokenIter<'a> {
        TokenIter {
            it: s.char_indices(),
            cur_start: None,
            cur_end: 0,
        }
    }

    fn is_token_char(c: char) -> bool {
        if c.is_alphanumeric() {
            return true;
        }
        matches!(c, '_' | '-' | '\'')
    }
}

impl<'a> Iterator for TokenIter<'a> {
    type Item = TokenSpan;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.it.next() {
                Some((i, c)) => {
                    let next_end = i + c.len_utf8();
                    if Self::is_token_char(c) {
                        if self.cur_start.is_none() {
                            self.cur_start = Some(i);
                        }
                        self.cur_end = next_end;
                    } else if let Some(st) = self.cur_start.take() {
                        let end = self.cur_end;
                        return Some(TokenSpan { start: st, end });
                    }
                }
                None => {
                    if let Some(st) = self.cur_start.take() {
                        let end = self.cur_end;
                        return Some(TokenSpan { start: st, end });
                    }
                    return None;
                }
            }
        }
    }
}

/// Derive a stable term id from a token slice.
///
/// Strategy:
/// - Domain separation prefix: b"term\0"
/// - Hash: BLAKE3
/// - Normalization: ASCII lowercase (A-Z -> a-z), other bytes unchanged
/// - Truncate to `cfg.max_token_bytes` bytes for hashing
///
/// Output:
/// - TermId uses the first 8 bytes of the hash digest as little-endian u64.
pub fn term_id_from_token(token: &str, cfg: TokenizerCfg) -> TermId {
    let mut h = blake3::Hasher::new();
    h.update(b"term\0");

    let mut buf = [0u8; 64];
    let mut nbuf = 0usize;
    let mut used = 0usize;

    for b in token.as_bytes() {
        if used >= cfg.max_token_bytes {
            break;
        }
        used += 1;

        let nb = if (b'A'..=b'Z').contains(b) { b + 32 } else { *b };
        buf[nbuf] = nb;
        nbuf += 1;
        if nbuf == buf.len() {
            h.update(&buf);
            nbuf = 0;
        }
    }

    if nbuf != 0 {
        h.update(&buf[..nbuf]);
    }

    let out = h.finalize();
    let bytes = out.as_bytes();
    let mut id8 = [0u8; 8];
    id8.copy_from_slice(&bytes[0..8]);
    TermId(Id64(u64::from_le_bytes(id8)))
}

/// Extract term frequencies from text.
///
/// This function:
/// - tokenizes `text` into token spans
/// - derives a TermId per token
/// - sorts the ids and counts runs to produce TermFreq (tf is u32)
///
/// No hash maps are used in v1 to keep dependencies minimal and behavior
/// deterministic. Complexity is O(n log n) for n tokens in the input.
pub fn term_freqs_from_text(text: &str, cfg: TokenizerCfg) -> Vec<TermFreq> {
    let mut ids: Vec<u64> = Vec::new();
    for sp in TokenIter::new(text) {
        let tok = &text[sp.start..sp.end];
        let id = term_id_from_token(tok, cfg).0.0;
        ids.push(id);
    }

    ids.sort_unstable();

    let mut out: Vec<TermFreq> = Vec::new();
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
            out.push(TermFreq { term: TermId(Id64(cur)), tf: cnt });
            cur = x;
            cnt = 1;
        }
    }
    out.push(TermFreq { term: TermId(Id64(cur)), tf: cnt });

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_basic_spans() {
        let s = "Hello, world! a_b-c's 123.";
        let spans: Vec<TokenSpan> = TokenIter::new(s).collect();
        let toks: Vec<&str> = spans.iter().map(|t| &s[t.start..t.end]).collect();
        assert_eq!(toks, vec!["Hello", "world", "a_b-c's", "123"]);
    }

    #[test]
    fn tokenize_unicode_is_safe() {
        // Includes non-ASCII letters and an emoji delimiter.
        let s = "naive café 🍩 donut";
        let spans: Vec<TokenSpan> = TokenIter::new(s).collect();
        let toks: Vec<&str> = spans.iter().map(|t| &s[t.start..t.end]).collect();
        assert_eq!(toks, vec!["naive", "café", "donut"]);
    }

    #[test]
    fn term_id_is_deterministic() {
        let cfg = TokenizerCfg::default();
        let a = term_id_from_token("Hello", cfg);
        let b = term_id_from_token("Hello", cfg);
        assert_eq!(a, b);
    }

    #[test]
    fn term_id_ascii_lowercases() {
        let cfg = TokenizerCfg::default();
        let a = term_id_from_token("Hello", cfg);
        let b = term_id_from_token("hello", cfg);
        assert_eq!(a, b);
    }

    #[test]
    fn term_freqs_counts_runs() {
        let cfg = TokenizerCfg::default();
        let v = term_freqs_from_text("a a b a", cfg);
        assert_eq!(v.len(), 2);
        let tf_sum: u32 = v.iter().map(|x| x.tf).sum();
        assert_eq!(tf_sum, 4);
        assert!(v.iter().any(|x| x.tf == 3));
        assert!(v.iter().any(|x| x.tf == 1));
    }
}
