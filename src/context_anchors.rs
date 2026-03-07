// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Context anchors schema and builder.
//!
//! ContextAnchorsV1 is a small, replayable record of low-weight query terms
//! derived from recent conversation history.
//!
//! Goals:
//! - Improve conversational continuity for follow-up prompts.
//! - Keep determinism and bounded work.
//! - Prefer lexicon-backed content words when a LexiconSnapshot is available.
//!
//! Non-goals:
//! - Replace evidence-first retrieval.
//! - Add a new planning representation.

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::frame::{Id64, TermId};
use crate::hash::{blake3_hash, Hash32};
use crate::index_query::{QueryTerm, QueryTermsCfg};
use crate::lexicon::derive_lemma_key_id;
use crate::lexicon_expand_lookup::LexiconExpandLookupV1;
use crate::lexicon_neighborhoods::{lemma_pos_mask, pos_is_content};
use crate::prompt_pack::{Message, Role};
use crate::tokenizer::{term_id_from_token, TokenIter, TokenizerCfg};

/// ContextAnchorsV1 schema version.
pub const CONTEXT_ANCHORS_V1_VERSION: u32 = 1;

/// Maximum number of anchor terms allowed in v1.
pub const CONTEXT_ANCHORS_V1_MAX_TERMS: usize = 64;

/// Context anchors flags (v1).
pub type ContextAnchorsFlagsV1 = u32;

/// Context anchors used a lexicon snapshot for filtering.
pub const CA_FLAG_USED_LEXICON: ContextAnchorsFlagsV1 = 1u32 << 0;

/// Context anchors included assistant messages.
pub const CA_FLAG_INCLUDED_ASSISTANT: ContextAnchorsFlagsV1 = 1u32 << 1;

/// Mask of all known v1 flags.
pub const CA_FLAGS_V1_ALL: ContextAnchorsFlagsV1 =
    CA_FLAG_USED_LEXICON | CA_FLAG_INCLUDED_ASSISTANT;

/// One context anchor term (v1).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ContextAnchorTermV1 {
    /// Term id.
    pub term_id: Id64,
    /// Low-weight query term frequency.
    pub qtf: u16,
}

/// Context anchors record (v1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContextAnchorsV1 {
    /// Version.
    pub version: u32,
    /// PromptPack hash that produced this answer.
    pub prompt_id: Hash32,
    /// Index of the user message treated as the query in the PromptPack.
    pub query_msg_ix: u32,
    /// Flags.
    pub flags: ContextAnchorsFlagsV1,
    /// Hash of the source bytes used to compute anchors.
    pub source_hash: Hash32,
    /// Anchor terms (canonical order: term_id asc).
    pub terms: Vec<ContextAnchorTermV1>,
}

impl ContextAnchorsV1 {
    /// Validate schema invariants.
    pub fn validate(&self) -> Result<(), DecodeError> {
        if self.version != CONTEXT_ANCHORS_V1_VERSION {
            return Err(DecodeError::new("bad ContextAnchorsV1 version"));
        }
        if (self.flags & !CA_FLAGS_V1_ALL) != 0 {
            return Err(DecodeError::new("bad ContextAnchorsV1 flags"));
        }
        if self.terms.len() > CONTEXT_ANCHORS_V1_MAX_TERMS {
            return Err(DecodeError::new("too many ContextAnchorsV1 terms"));
        }
        if !self.is_canonical() {
            return Err(DecodeError::new("non-canonical ContextAnchorsV1"));
        }
        Ok(())
    }

    fn is_canonical(&self) -> bool {
        let mut prev: Option<u64> = None;
        for t in &self.terms {
            if t.qtf == 0 {
                return false;
            }
            let v = t.term_id.0;
            if let Some(p) = prev {
                if v <= p {
                    return false;
                }
            }
            prev = Some(v);
        }
        true
    }

    /// Encode to canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        if self.version != CONTEXT_ANCHORS_V1_VERSION {
            return Err(EncodeError::new("bad ContextAnchorsV1 version"));
        }
        if (self.flags & !CA_FLAGS_V1_ALL) != 0 {
            return Err(EncodeError::new("bad ContextAnchorsV1 flags"));
        }
        if self.terms.len() > CONTEXT_ANCHORS_V1_MAX_TERMS {
            return Err(EncodeError::new("too many ContextAnchorsV1 terms"));
        }
        if !self.is_canonical() {
            return Err(EncodeError::new("non-canonical ContextAnchorsV1"));
        }

        let mut w = ByteWriter::with_capacity(128 + self.terms.len() * 16);
        w.write_u32(self.version);
        w.write_raw(&self.prompt_id);
        w.write_u32(self.query_msg_ix);
        w.write_u32(self.flags);
        w.write_raw(&self.source_hash);
        w.write_u16(self.terms.len() as u16);
        for t in &self.terms {
            w.write_u64(t.term_id.0);
            w.write_u16(t.qtf);
        }
        Ok(w.into_bytes())
    }

    /// Decode from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let version = r.read_u32()?;
        if version != CONTEXT_ANCHORS_V1_VERSION {
            return Err(DecodeError::new("bad ContextAnchorsV1 version"));
        }
        let pid_b = r.read_fixed(32)?;
        let mut prompt_id = [0u8; 32];
        prompt_id.copy_from_slice(pid_b);
        let query_msg_ix = r.read_u32()?;
        let flags = r.read_u32()?;
        if (flags & !CA_FLAGS_V1_ALL) != 0 {
            return Err(DecodeError::new("bad ContextAnchorsV1 flags"));
        }
        let sh_b = r.read_fixed(32)?;
        let mut source_hash = [0u8; 32];
        source_hash.copy_from_slice(sh_b);
        let n = r.read_u16()? as usize;
        if n > CONTEXT_ANCHORS_V1_MAX_TERMS {
            return Err(DecodeError::new("too many ContextAnchorsV1 terms"));
        }
        let mut terms: Vec<ContextAnchorTermV1> = Vec::with_capacity(n);
        for _ in 0..n {
            let term_id = Id64(r.read_u64()?);
            let qtf = r.read_u16()?;
            terms.push(ContextAnchorTermV1 { term_id, qtf });
        }
        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }
        let out = ContextAnchorsV1 {
            version,
            prompt_id,
            query_msg_ix,
            flags,
            source_hash,
            terms,
        };
        out.validate()?;
        Ok(out)
    }
}

/// Builder configuration for ContextAnchorsV1.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContextAnchorsCfgV1 {
    /// Maximum messages to scan (counting backward from the query message).
    pub max_messages: usize,
    /// Maximum total UTF-8 bytes scanned across selected messages.
    pub max_total_bytes: usize,
    /// Maximum number of anchor terms emitted.
    pub max_terms: usize,
    /// Minimum token length (ASCII bytes) for anchors.
    pub min_token_len: usize,
    /// Whether to include assistant messages.
    pub include_assistant: bool,
}

impl ContextAnchorsCfgV1 {
    /// Conservative defaults.
    pub fn default_v1() -> Self {
        Self {
            max_messages: 6,
            max_total_bytes: 4096,
            max_terms: 16,
            min_token_len: 3,
            include_assistant: true,
        }
    }
}

/// Output of the context anchor builder.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContextAnchorsBuildV1 {
    /// The encoded anchor record.
    pub anchors: ContextAnchorsV1,
    /// Query terms to merge into retrieval (low weight).
    pub query_terms: Vec<QueryTerm>,
}

fn role_tag_u8(r: Role) -> u8 {
    match r {
        Role::System => 0,
        Role::User => 1,
        Role::Assistant => 2,
    }
}

fn lexicon_accept_token(
    lex: &LexiconExpandLookupV1,
    tok_lc: &str,
    lemma_buf: &mut Vec<crate::lexicon::LemmaId>,
) -> bool {
    let key = derive_lemma_key_id(tok_lc);
    lex.lemma_ids_for_key_into(key, 4, lemma_buf);
    if lemma_buf.is_empty() {
        return false;
    }

    // Accept if any lemma is a content part-of-speech.
    let mut pm: u32 = 0;
    for lid in lemma_buf.iter().copied() {
        if let Some(m) = lemma_pos_mask(lex, lid) {
            pm |= m;
        }
    }
    pos_is_content(pm)
}

fn is_all_hex_ascii(tok_lc: &str) -> bool {
    if tok_lc.is_empty() {
        return false;
    }
    for b in tok_lc.as_bytes() {
        if !matches!(*b, b'0'..=b'9' | b'a'..=b'f') {
            return false;
        }
    }
    true
}

fn accept_nonlex_token(tok_lc: &str, role_tag: u8) -> bool {
    // Allow digits and mixed alnum tokens even without lexicon (error codes, ids).
    // Avoid absorbing long hashes and meta tokens from assistant output.

    // Reject common hash lengths.
    if (tok_lc.len() == 64 || tok_lc.len() == 32) && is_all_hex_ascii(tok_lc) {
        return false;
    }
    // Bound non-lex tokens to avoid capturing long ids.
    if tok_lc.len() > 32 {
        return false;
    }

    let mut has_digit = false;
    let mut has_alpha = false;
    for b in tok_lc.as_bytes() {
        if b.is_ascii_digit() {
            has_digit = true;
        } else if b.is_ascii_alphabetic() {
            has_alpha = true;
        }
        if has_digit && has_alpha {
            break;
        }
    }

    // For assistant messages, be stricter: accept only digit-bearing tokens
    // (error codes like E0425, ports, small ids).
    if role_tag == 2 {
        return has_digit && tok_lc.len() <= 16;
    }

    // For user messages, allow either digit-bearing tokens or longer alpha tokens.
    has_digit || (has_alpha && tok_lc.len() >= 6)
}

/// Build context anchors from a PromptPack message list.
///
/// - `prompt_id` is the PromptPack hash used for the answer.
/// - `query_msg_ix` is the index of the user message treated as the query.
///
/// Returns None when no anchors are emitted.
pub fn build_context_anchors_v1(
    prompt_id: Hash32,
    query_msg_ix: usize,
    messages: &[Message],
    qcfg: &QueryTermsCfg,
    lex_opt: Option<&LexiconExpandLookupV1>,
    cfg: ContextAnchorsCfgV1,
) -> Option<ContextAnchorsBuildV1> {
    if query_msg_ix == 0 || messages.is_empty() {
        return None;
    }

    let max_terms = core::cmp::min(cfg.max_terms, CONTEXT_ANCHORS_V1_MAX_TERMS);
    if max_terms == 0 {
        return None;
    }

    // Select messages prior to the query message, scanning backward.
    let mut selected: Vec<(u8, &str)> = Vec::new();
    let mut bytes_total: usize = 0;
    let mut taken_msgs: usize = 0;

    let mut ix = query_msg_ix;
    while ix > 0 {
        ix -= 1;
        let m = &messages[ix];
        if m.role == Role::System {
            continue;
        }
        if m.role == Role::Assistant && !cfg.include_assistant {
            continue;
        }
        let b = m.content.as_bytes().len();
        if b == 0 {
            continue;
        }
        if bytes_total + b > cfg.max_total_bytes {
            break;
        }
        selected.push((role_tag_u8(m.role), &m.content));
        bytes_total += b;
        taken_msgs += 1;
        if taken_msgs >= cfg.max_messages {
            break;
        }
    }

    if selected.is_empty() {
        return None;
    }

    // Hash the selected source bytes in a stable order (oldest first).
    selected.reverse();
    let mut src: Vec<u8> = Vec::with_capacity(16 + bytes_total);
    src.extend_from_slice(b"ctx_anchors_v1\0");
    for (tag, s) in &selected {
        src.push(*tag);
        src.extend_from_slice(&(s.as_bytes().len() as u32).to_le_bytes());
        src.extend_from_slice(s.as_bytes());
    }
    let source_hash = blake3_hash(&src);

    // Score term ids from selected messages.
    // Deterministic scoring:
    // - More recent messages get a larger multiplier.
    // - Ties are broken by term id asc.
    let tok_cfg = TokenizerCfg {
        max_token_bytes: qcfg.tok_cfg.max_token_bytes,
    };

    let mut lemma_buf: Vec<crate::lexicon::LemmaId> = Vec::new();
    let mut scores: std::collections::BTreeMap<u64, u32> = std::collections::BTreeMap::new();

    for (j, (tag, s)) in selected.iter().enumerate() {
        // Newer messages weigh slightly more. Prefer user context over assistant
        // output to improve continuity across follow-up questions.
        let base_w: u32 = 1 + (j as u32);
        let role_mul: u32 = if *tag == 1 { 3 } else { 1 };
        let w: u32 = base_w.saturating_mul(role_mul);

        for sp in TokenIter::new(s) {
            let tok = &s[sp.start..sp.end];
            let tl = tok.to_ascii_lowercase();
            if tl.len() < cfg.min_token_len {
                continue;
            }

            let accept = if let Some(lex) = lex_opt {
                lexicon_accept_token(lex, &tl, &mut lemma_buf) || accept_nonlex_token(&tl, *tag)
            } else {
                accept_nonlex_token(&tl, *tag)
            };

            if !accept {
                continue;
            }

            let tid = term_id_from_token(&tl, tok_cfg);
            let k = (tid.0).0;
            let e = scores.entry(k).or_insert(0);
            *e = e.saturating_add(w);
        }
    }

    if scores.is_empty() {
        return None;
    }

    // Select top terms by score desc, then term id asc.
    let mut ranked: Vec<(u64, u32)> = scores.into_iter().collect();
    ranked.sort_by(|a, b| match b.1.cmp(&a.1) {
        core::cmp::Ordering::Equal => a.0.cmp(&b.0),
        o => o,
    });

    if ranked.len() > max_terms {
        ranked.truncate(max_terms);
    }

    // Canonicalize terms by term id asc in the stored artifact.
    let mut terms: Vec<ContextAnchorTermV1> = Vec::with_capacity(ranked.len());
    for (term_u64, _score) in ranked {
        terms.push(ContextAnchorTermV1 {
            term_id: Id64(term_u64),
            qtf: 1,
        });
    }
    terms.sort_by(|a, b| a.term_id.0.cmp(&b.term_id.0));
    terms.dedup_by(|a, b| a.term_id.0 == b.term_id.0);

    if terms.is_empty() {
        return None;
    }

    let mut flags: ContextAnchorsFlagsV1 = 0;
    if lex_opt.is_some() {
        flags |= CA_FLAG_USED_LEXICON;
    }
    if cfg.include_assistant {
        flags |= CA_FLAG_INCLUDED_ASSISTANT;
    }

    let anchors = ContextAnchorsV1 {
        version: CONTEXT_ANCHORS_V1_VERSION,
        prompt_id,
        query_msg_ix: query_msg_ix as u32,
        flags,
        source_hash,
        terms: terms.clone(),
    };

    // Query terms form.
    let mut qterms: Vec<QueryTerm> = Vec::with_capacity(terms.len());
    for t in &terms {
        qterms.push(QueryTerm {
            term: TermId(t.term_id),
            qtf: t.qtf as u32,
        });
    }

    Some(ContextAnchorsBuildV1 {
        anchors,
        query_terms: qterms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_anchors_round_trip_empty_rejected() {
        let a = ContextAnchorsV1 {
            version: CONTEXT_ANCHORS_V1_VERSION,
            prompt_id: [1u8; 32],
            query_msg_ix: 3,
            flags: 0,
            source_hash: [2u8; 32],
            terms: Vec::new(),
        };
        assert!(a.encode().is_ok(), "empty terms are allowed");
        let b = a.encode().unwrap();
        let dec = ContextAnchorsV1::decode(&b).unwrap();
        assert_eq!(dec.terms.len(), 0);
    }

    #[test]
    fn context_anchors_encode_decode() {
        let a = ContextAnchorsV1 {
            version: CONTEXT_ANCHORS_V1_VERSION,
            prompt_id: [3u8; 32],
            query_msg_ix: 1,
            flags: CA_FLAG_INCLUDED_ASSISTANT,
            source_hash: [4u8; 32],
            terms: vec![
                ContextAnchorTermV1 {
                    term_id: Id64(10),
                    qtf: 1,
                },
                ContextAnchorTermV1 {
                    term_id: Id64(20),
                    qtf: 2,
                },
            ],
        };
        let bytes = a.encode().unwrap();
        let dec = ContextAnchorsV1::decode(&bytes).unwrap();
        assert_eq!(a, dec);
    }

    #[test]
    fn builder_is_deterministic_for_same_inputs() {
        let msgs = vec![
            Message {
                role: Role::User,
                content: "banana banana".to_string(),
            },
            Message {
                role: Role::Assistant,
                content: "ok".to_string(),
            },
            Message {
                role: Role::User,
                content: "why?".to_string(),
            },
        ];
        let qcfg = QueryTermsCfg::new();
        let cfg = ContextAnchorsCfgV1::default_v1();
        let a = build_context_anchors_v1([9u8; 32], 2, &msgs, &qcfg, None, cfg).unwrap();
        let b = build_context_anchors_v1([9u8; 32], 2, &msgs, &qcfg, None, cfg).unwrap();
        assert_eq!(a.anchors.source_hash, b.anchors.source_hash);
        assert_eq!(a.anchors.terms, b.anchors.terms);
    }
}
