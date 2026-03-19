// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! ConversationPackV1: resumable, deterministic chat history artifact.
//!
//! This artifact records:
//! - ordered System/User/Assistant messages
//! - deterministic runtime knobs (seed, max output tokens)
//! - determinism-critical ids (merged snapshot + sig map, optional lexicon snapshot)
//! - the PromptLimits used to canonicalize and bound message history
//!
//! The binary format and canonicalization rules are defined in:
//! docs/CONVERSATION_PACK_V1.md

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::hash::Hash32;

/// ConversationPack version (v1).
pub const CONVERSATION_PACK_VERSION: u16 = 1;

/// Message role for a conversation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversationRole {
    /// System message (policy/instructions).
    System,
    /// User message.
    User,
    /// Assistant message.
    Assistant,
}

impl ConversationRole {
    fn to_u8(self) -> u8 {
        match self {
            ConversationRole::System => 0,
            ConversationRole::User => 1,
            ConversationRole::Assistant => 2,
        }
    }

    fn from_u8(v: u8) -> Result<Self, DecodeError> {
        match v {
            0 => Ok(ConversationRole::System),
            1 => Ok(ConversationRole::User),
            2 => Ok(ConversationRole::Assistant),
            _ => Err(DecodeError::new("invalid role")),
        }
    }
}

/// A single conversation message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversationMessage {
    /// Role.
    pub role: ConversationRole,
    /// UTF-8 content.
    pub content: String,
    /// Optional ReplayLog id for the assistant turn that produced this message.
    pub replay_id: Option<Hash32>,
}

/// Limits applied to conversation history.
///
/// These match the message-related subset of `PromptLimits`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConversationLimits {
    /// Maximum number of messages allowed.
    pub max_messages: u32,
    /// Maximum total UTF-8 bytes across all kept messages.
    pub max_total_message_bytes: u32,
    /// Maximum UTF-8 bytes allowed per message.
    pub max_message_bytes: u32,
    /// Whether to prioritize keeping System messages.
    pub keep_system: bool,
}

impl ConversationLimits {
    /// Default limits for v1.
    pub fn default_v1() -> Self {
        Self {
            max_messages: 64,
            max_total_message_bytes: 64 * 1024,
            max_message_bytes: 16 * 1024,
            keep_system: true,
        }
    }
}

/// Report describing canonicalization actions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversationCanonicalizeReport {
    /// Messages before canonicalization.
    pub messages_before: usize,
    /// Messages after canonicalization.
    pub messages_after: usize,
    /// Messages dropped due to limits.
    pub messages_dropped: usize,
    /// Messages whose content was truncated.
    pub messages_truncated: usize,
}

/// Saved presentation mode for resumed user or operator surfaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversationPresentationModeV1 {
    /// User-facing conversational surface.
    User,
    /// Operator-facing diagnostic surface.
    Operator,
}

impl ConversationPresentationModeV1 {
    /// Encodes the presentation mode as a stable trailer byte.
    pub fn to_u8(self) -> u8 {
        match self {
            Self::User => 0,
            Self::Operator => 1,
        }
    }

    /// Decodes a presentation mode from a stable trailer byte.
    pub fn from_u8(v: u8) -> Result<Self, DecodeError> {
        match v {
            0 => Ok(Self::User),
            1 => Ok(Self::Operator),
            _ => Err(DecodeError::new("invalid presentation mode")),
        }
    }

    /// Returns the stable lowercase text form used by CLI surfaces.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Operator => "operator",
        }
    }
}

/// Canonical, resumable conversation artifact (v1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversationPackV1 {
    /// Version.
    pub version: u16,
    /// Deterministic RNG seed.
    pub seed: u64,
    /// Maximum output tokens requested.
    pub max_output_tokens: u32,

    /// Determinism-critical ids.
    pub snapshot_id: Hash32,
    /// SigMap id matching `snapshot_id`.
    pub sig_map_id: Hash32,
    /// Optional lexicon snapshot id used for expansion.
    pub lexicon_snapshot_id: Option<Hash32>,
    /// Optional Markov model id used for bounded phrasing hints.
    pub markov_model_id: Option<Hash32>,
    /// Optional exemplar memory id used for advisory shaping.
    pub exemplar_memory_id: Option<Hash32>,
    /// Optional graph relevance id used for bounded graph expansion.
    pub graph_relevance_id: Option<Hash32>,
    /// Optional saved presentation mode for resumed operator or user surfaces.
    pub presentation_mode: Option<ConversationPresentationModeV1>,

    /// Limits used to canonicalize and bound message history.
    pub limits: ConversationLimits,
    /// Ordered messages.
    pub messages: Vec<ConversationMessage>,
}

fn utf8_prefix_boundary(s: &str, max_bytes: usize) -> usize {
    if s.len() <= max_bytes {
        return s.len();
    }
    let mut i = max_bytes;
    while i > 0 && !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

fn clamp_u32_to_usize(v: u32) -> usize {
    if (v as u64) > (usize::MAX as u64) {
        usize::MAX
    } else {
        v as usize
    }
}

fn read_hash32(r: &mut ByteReader<'_>) -> Result<Hash32, DecodeError> {
    let b = r.read_fixed(32)?;
    let mut out = [0u8; 32];
    out.copy_from_slice(b);
    Ok(out)
}

fn write_hash32(w: &mut ByteWriter, h: &Hash32) {
    w.write_raw(h);
}

fn read_bool_u8(r: &mut ByteReader<'_>) -> Result<bool, DecodeError> {
    match r.read_u8()? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(DecodeError::new("invalid bool")),
    }
}

fn write_bool_u8(w: &mut ByteWriter, v: bool) {
    w.write_u8(if v { 1 } else { 0 });
}

impl ConversationPackV1 {
    /// Create a new empty ConversationPackV1.
    pub fn new(
        seed: u64,
        max_output_tokens: u32,
        snapshot_id: Hash32,
        sig_map_id: Hash32,
        lexicon_snapshot_id: Option<Hash32>,
        limits: ConversationLimits,
    ) -> Self {
        Self {
            version: CONVERSATION_PACK_VERSION,
            seed,
            max_output_tokens,
            snapshot_id,
            sig_map_id,
            lexicon_snapshot_id,
            markov_model_id: None,
            exemplar_memory_id: None,
            graph_relevance_id: None,
            presentation_mode: None,
            limits,
            messages: Vec::new(),
        }
    }

    /// Canonicalize message history in-place under `self.limits`.
    pub fn canonicalize_in_place(&mut self) -> ConversationCanonicalizeReport {
        let messages_before = self.messages.len();

        // 1) Per-message truncation.
        let max_msg_bytes = clamp_u32_to_usize(self.limits.max_message_bytes);
        let mut messages_truncated = 0usize;
        for m in &mut self.messages {
            if m.content.len() > max_msg_bytes {
                let cut = utf8_prefix_boundary(&m.content, max_msg_bytes);
                if cut < m.content.len() {
                    m.content.truncate(cut);
                    messages_truncated += 1;
                }
            }
        }

        // 2) Message selection by count and total bytes.
        let max_messages = clamp_u32_to_usize(self.limits.max_messages);
        let max_total_bytes = clamp_u32_to_usize(self.limits.max_total_message_bytes);

        let mut total_bytes = 0usize;
        for m in &self.messages {
            total_bytes += m.content.len();
        }

        if self.messages.len() > max_messages || total_bytes > max_total_bytes {
            let old = core::mem::take(&mut self.messages);
            let n = old.len();

            let mut keep_idx: Vec<usize> = Vec::new();
            keep_idx.reserve(core::cmp::min(n, max_messages));

            if self.limits.keep_system {
                for (i, m) in old.iter().enumerate() {
                    if m.role == ConversationRole::System {
                        keep_idx.push(i);
                        if keep_idx.len() >= max_messages {
                            break;
                        }
                    }
                }
            }

            let remaining_slots = max_messages.saturating_sub(keep_idx.len());
            if remaining_slots > 0 {
                let mut taken = 0usize;
                for (i, m) in old.iter().enumerate().rev() {
                    if self.limits.keep_system && m.role == ConversationRole::System {
                        continue;
                    }
                    keep_idx.push(i);
                    taken += 1;
                    if taken >= remaining_slots {
                        break;
                    }
                }
            }

            keep_idx.sort_unstable();

            let mut keep_mask = vec![false; n];
            for &i in &keep_idx {
                keep_mask[i] = true;
            }

            let mut kept: Vec<ConversationMessage> = Vec::with_capacity(keep_idx.len());
            total_bytes = 0;
            for (i, m) in old.into_iter().enumerate() {
                if keep_mask[i] {
                    total_bytes += m.content.len();
                    kept.push(m);
                }
            }

            if total_bytes > max_total_bytes {
                // Prefer truncating the most recent kept message.
                if !kept.is_empty() {
                    let idx = kept.len() - 1;
                    let excess = total_bytes - max_total_bytes;
                    let cur = kept[idx].content.len();
                    let new_len = cur.saturating_sub(excess);
                    let cut = utf8_prefix_boundary(&kept[idx].content, new_len);
                    if cut < kept[idx].content.len() {
                        let delta = kept[idx].content.len() - cut;
                        kept[idx].content.truncate(cut);
                        total_bytes = total_bytes.saturating_sub(delta);
                        messages_truncated += 1;
                    }
                }

                // Drop oldest messages deterministically.
                let mut i = 0usize;
                while total_bytes > max_total_bytes && kept.len() > 1 && i < kept.len() {
                    let can_drop = if self.limits.keep_system {
                        kept[i].role != ConversationRole::System
                    } else {
                        true
                    };
                    if can_drop {
                        total_bytes -= kept[i].content.len();
                        kept.remove(i);
                        continue;
                    }
                    i += 1;
                }

                // If still too large, truncate the last remaining message.
                if total_bytes > max_total_bytes && !kept.is_empty() {
                    let idx = kept.len() - 1;
                    let cur = kept[idx].content.len();
                    let excess = total_bytes - max_total_bytes;
                    let new_len = cur.saturating_sub(excess);
                    let cut = utf8_prefix_boundary(&kept[idx].content, new_len);
                    if cut < kept[idx].content.len() {
                        let delta = kept[idx].content.len() - cut;
                        kept[idx].content.truncate(cut);
                        total_bytes = total_bytes.saturating_sub(delta);
                        messages_truncated += 1;
                    }
                }

                // Final fallback: allow dropping to reach budget.
                while total_bytes > max_total_bytes && !kept.is_empty() {
                    total_bytes -= kept[0].content.len();
                    kept.remove(0);
                }
            }

            self.messages = kept;
        }

        let messages_after = self.messages.len();
        let messages_dropped = messages_before.saturating_sub(messages_after);

        ConversationCanonicalizeReport {
            messages_before,
            messages_after,
            messages_dropped,
            messages_truncated,
        }
    }

    /// Encode to canonical bytes.
    ///
    /// Call `canonicalize_in_place` first to ensure this pack is canonical.
    pub fn encode_assuming_canonical(&self) -> Result<Vec<u8>, EncodeError> {
        let mut cap: usize = 2 + 8 + 4 + 32 + 32 + 1 + 4 + 4 + 4 + 1 + 4;
        if self.lexicon_snapshot_id.is_some() {
            cap += 32;
        }
        cap = cap.saturating_add(4);
        if self.markov_model_id.is_some() {
            cap = cap.saturating_add(32);
        }
        if self.exemplar_memory_id.is_some() {
            cap = cap.saturating_add(32);
        }
        if self.graph_relevance_id.is_some() {
            cap = cap.saturating_add(32);
        }
        if self.presentation_mode.is_some() {
            cap = cap.saturating_add(1);
        }
        for m in &self.messages {
            cap = cap.saturating_add(1 + 4 + m.content.len() + 1);
            if m.replay_id.is_some() {
                cap = cap.saturating_add(32);
            }
        }

        let mut w = ByteWriter::with_capacity(cap);
        w.write_u16(self.version);
        w.write_u64(self.seed);
        w.write_u32(self.max_output_tokens);

        write_hash32(&mut w, &self.snapshot_id);
        write_hash32(&mut w, &self.sig_map_id);

        match &self.lexicon_snapshot_id {
            Some(h) => {
                w.write_u8(1);
                write_hash32(&mut w, h);
            }
            None => {
                w.write_u8(0);
            }
        }

        w.write_u32(self.limits.max_message_bytes);
        w.write_u32(self.limits.max_total_message_bytes);
        w.write_u32(self.limits.max_messages);
        write_bool_u8(&mut w, self.limits.keep_system);

        if self.messages.len() > (u32::MAX as usize) {
            return Err(EncodeError::new("too many messages"));
        }
        w.write_u32(self.messages.len() as u32);

        for m in &self.messages {
            w.write_u8(m.role.to_u8());
            w.write_str(&m.content)?;
            match &m.replay_id {
                Some(h) => {
                    w.write_u8(1);
                    write_hash32(&mut w, h);
                }
                None => {
                    w.write_u8(0);
                }
            }
        }

        match &self.markov_model_id {
            Some(h) => {
                w.write_u8(1);
                write_hash32(&mut w, h);
            }
            None => {
                w.write_u8(0);
            }
        }
        match &self.exemplar_memory_id {
            Some(h) => {
                w.write_u8(1);
                write_hash32(&mut w, h);
            }
            None => {
                w.write_u8(0);
            }
        }
        match &self.graph_relevance_id {
            Some(h) => {
                w.write_u8(1);
                write_hash32(&mut w, h);
            }
            None => {
                w.write_u8(0);
            }
        }
        match self.presentation_mode {
            Some(v) => {
                w.write_u8(1);
                w.write_u8(v.to_u8());
            }
            None => {
                w.write_u8(0);
            }
        }

        Ok(w.into_bytes())
    }

    /// Decode from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let version = r.read_u16()?;
        if version != CONVERSATION_PACK_VERSION {
            return Err(DecodeError::new("unsupported ConversationPack version"));
        }

        let seed = r.read_u64()?;
        let max_output_tokens = r.read_u32()?;

        let snapshot_id = read_hash32(&mut r)?;
        let sig_map_id = read_hash32(&mut r)?;

        let has_lex = r.read_u8()?;
        let lexicon_snapshot_id = match has_lex {
            0 => None,
            1 => Some(read_hash32(&mut r)?),
            _ => return Err(DecodeError::new("invalid has_lexicon")),
        };

        let max_message_bytes = r.read_u32()?;
        let max_total_message_bytes = r.read_u32()?;
        let max_messages = r.read_u32()?;
        let keep_system = read_bool_u8(&mut r)?;

        let limits = ConversationLimits {
            max_messages,
            max_total_message_bytes,
            max_message_bytes,
            keep_system,
        };

        let msg_n = r.read_u32()? as usize;
        let mut messages = Vec::with_capacity(msg_n);
        let mut total_bytes = 0usize;
        let max_msg_bytes = clamp_u32_to_usize(max_message_bytes);

        for _ in 0..msg_n {
            let role = ConversationRole::from_u8(r.read_u8()?)?;
            let content = r.read_str_view()?.to_string();
            if content.len() > max_msg_bytes {
                return Err(DecodeError::new("message exceeds max_message_bytes"));
            }
            total_bytes = total_bytes.saturating_add(content.len());

            let has_replay = r.read_u8()?;
            let replay_id = match has_replay {
                0 => None,
                1 => Some(read_hash32(&mut r)?),
                _ => return Err(DecodeError::new("invalid has_replay")),
            };

            messages.push(ConversationMessage { role, content, replay_id });
        }

        let mut markov_model_id: Option<Hash32> = None;
        let mut exemplar_memory_id: Option<Hash32> = None;
        let mut graph_relevance_id: Option<Hash32> = None;
        let mut presentation_mode: Option<ConversationPresentationModeV1> = None;

        if r.remaining() != 0 {
            let has_markov = r.read_u8()?;
            markov_model_id = match has_markov {
                0 => None,
                1 => Some(read_hash32(&mut r)?),
                _ => return Err(DecodeError::new("invalid has_markov_model")),
            };

            let has_exemplar = r.read_u8()?;
            exemplar_memory_id = match has_exemplar {
                0 => None,
                1 => Some(read_hash32(&mut r)?),
                _ => return Err(DecodeError::new("invalid has_exemplar_memory")),
            };

            let has_graph = r.read_u8()?;
            graph_relevance_id = match has_graph {
                0 => None,
                1 => Some(read_hash32(&mut r)?),
                _ => return Err(DecodeError::new("invalid has_graph_relevance")),
            };

            if r.remaining() != 0 {
                let has_presentation = r.read_u8()?;
                presentation_mode = match has_presentation {
                    0 => None,
                    1 => Some(ConversationPresentationModeV1::from_u8(r.read_u8()?)?),
                    _ => return Err(DecodeError::new("invalid has_presentation_mode")),
                };
            }
        }

        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        // Validate recorded limits are respected by the encoded messages.
        if msg_n > clamp_u32_to_usize(max_messages) {
            return Err(DecodeError::new("message count exceeds max_messages"));
        }
        if total_bytes > clamp_u32_to_usize(max_total_message_bytes) {
            return Err(DecodeError::new("total message bytes exceed max_total_message_bytes"));
        }

        Ok(Self {
            version,
            seed,
            max_output_tokens,
            snapshot_id,
            sig_map_id,
            lexicon_snapshot_id,
            markov_model_id,
            exemplar_memory_id,
            graph_relevance_id,
            presentation_mode,
            limits,
            messages,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::blake3_hash;

    fn z32(tag: &[u8]) -> Hash32 {
        blake3_hash(tag)
    }

    fn encode_legacy_without_advisory(p: &ConversationPackV1) -> Vec<u8> {
        let mut w = ByteWriter::with_capacity(256);
        w.write_u16(p.version);
        w.write_u64(p.seed);
        w.write_u32(p.max_output_tokens);
        write_hash32(&mut w, &p.snapshot_id);
        write_hash32(&mut w, &p.sig_map_id);
        match &p.lexicon_snapshot_id {
            Some(h) => {
                w.write_u8(1);
                write_hash32(&mut w, h);
            }
            None => {
                w.write_u8(0);
            }
        }
        w.write_u32(p.limits.max_message_bytes);
        w.write_u32(p.limits.max_total_message_bytes);
        w.write_u32(p.limits.max_messages);
        write_bool_u8(&mut w, p.limits.keep_system);
        w.write_u32(p.messages.len() as u32);
        for m in &p.messages {
            w.write_u8(m.role.to_u8());
            w.write_str(&m.content).unwrap();
            match &m.replay_id {
                Some(h) => {
                    w.write_u8(1);
                    write_hash32(&mut w, h);
                }
                None => {
                    w.write_u8(0);
                }
            }
        }
        w.into_bytes()
    }

    fn encode_with_advisory_without_presentation(p: &ConversationPackV1) -> Vec<u8> {
        let mut w = ByteWriter::with_capacity(320);
        w.write_u16(p.version);
        w.write_u64(p.seed);
        w.write_u32(p.max_output_tokens);
        write_hash32(&mut w, &p.snapshot_id);
        write_hash32(&mut w, &p.sig_map_id);
        match &p.lexicon_snapshot_id {
            Some(h) => {
                w.write_u8(1);
                write_hash32(&mut w, h);
            }
            None => {
                w.write_u8(0);
            }
        }
        w.write_u32(p.limits.max_message_bytes);
        w.write_u32(p.limits.max_total_message_bytes);
        w.write_u32(p.limits.max_messages);
        write_bool_u8(&mut w, p.limits.keep_system);
        w.write_u32(p.messages.len() as u32);
        for m in &p.messages {
            w.write_u8(m.role.to_u8());
            w.write_str(&m.content).unwrap();
            match &m.replay_id {
                Some(h) => {
                    w.write_u8(1);
                    write_hash32(&mut w, h);
                }
                None => {
                    w.write_u8(0);
                }
            }
        }
        match &p.markov_model_id {
            Some(h) => {
                w.write_u8(1);
                write_hash32(&mut w, h);
            }
            None => w.write_u8(0),
        }
        match &p.exemplar_memory_id {
            Some(h) => {
                w.write_u8(1);
                write_hash32(&mut w, h);
            }
            None => w.write_u8(0),
        }
        match &p.graph_relevance_id {
            Some(h) => {
                w.write_u8(1);
                write_hash32(&mut w, h);
            }
            None => w.write_u8(0),
        }
        w.into_bytes()
    }

    #[test]
    fn conversation_pack_round_trip_and_stable_bytes() {
        let limits = ConversationLimits::default_v1();
        let mut p = ConversationPackV1::new(
            7,
            256,
            z32(b"snap"),
            z32(b"sig"),
            Some(z32(b"lex")),
            limits,
        );

        p.messages.push(ConversationMessage {
            role: ConversationRole::System,
            content: "system".to_string(),
            replay_id: None,
        });
        p.messages.push(ConversationMessage {
            role: ConversationRole::User,
            content: "hello".to_string(),
            replay_id: None,
        });
        p.messages.push(ConversationMessage {
            role: ConversationRole::Assistant,
            content: "reply".to_string(),
            replay_id: Some(z32(b"replay1")),
        });
        p.markov_model_id = Some(z32(b"markov"));
        p.exemplar_memory_id = Some(z32(b"exemplar"));
        p.graph_relevance_id = Some(z32(b"graph"));
        p.presentation_mode = Some(ConversationPresentationModeV1::Operator);

        p.canonicalize_in_place();
        let bytes1 = p.encode_assuming_canonical().unwrap();
        let dec = ConversationPackV1::decode(&bytes1).unwrap();

        // Re-encode after decode and ensure bytes are identical.
        let bytes2 = dec.encode_assuming_canonical().unwrap();
        assert_eq!(bytes1, bytes2);

        // Round-trip equality.
        assert_eq!(dec, p);
    }

    #[test]
    fn conversation_pack_canonicalization_truncates_by_limits() {
        let limits = ConversationLimits {
            max_messages: 2,
            max_total_message_bytes: 8,
            max_message_bytes: 8,
            keep_system: true,
        };

        let mut p = ConversationPackV1::new(1, 8, z32(b"snap"), z32(b"sig"), None, limits);
        p.messages.push(ConversationMessage {
            role: ConversationRole::System,
            content: "sys".to_string(),
            replay_id: None,
        });
        p.messages.push(ConversationMessage {
            role: ConversationRole::User,
            content: "123456789".to_string(),
            replay_id: None,
        });
        p.messages.push(ConversationMessage {
            role: ConversationRole::Assistant,
            content: "abcd".to_string(),
            replay_id: None,
        });

        let rep = p.canonicalize_in_place();
        assert_eq!(rep.messages_after, 2);
        assert!(p.messages.iter().all(|m| m.content.len() <= 8));

        let bytes = p.encode_assuming_canonical().unwrap();
        let dec = ConversationPackV1::decode(&bytes).unwrap();
        assert_eq!(dec, p);
    }


    #[test]
    fn conversation_pack_decode_advisory_trailer_without_presentation() {
        let limits = ConversationLimits::default_v1();
        let mut p = ConversationPackV1::new(9, 64, z32(b"snap3"), z32(b"sig3"), Some(z32(b"lex3")), limits);
        p.messages.push(ConversationMessage {
            role: ConversationRole::Assistant,
            content: "reply".to_string(),
            replay_id: Some(z32(b"replay3")),
        });
        p.markov_model_id = Some(z32(b"markov3"));
        p.exemplar_memory_id = Some(z32(b"exemplar3"));
        p.graph_relevance_id = Some(z32(b"graph3"));
        p.canonicalize_in_place();

        let bytes = encode_with_advisory_without_presentation(&p);
        let dec = ConversationPackV1::decode(&bytes).unwrap();
        assert_eq!(dec.markov_model_id, p.markov_model_id);
        assert_eq!(dec.exemplar_memory_id, p.exemplar_memory_id);
        assert_eq!(dec.graph_relevance_id, p.graph_relevance_id);
        assert_eq!(dec.presentation_mode, None);
        assert_eq!(dec.messages, p.messages);
    }

    #[test]
    fn conversation_pack_decode_zeroed_advisory_trailer_without_presentation() {
        let limits = ConversationLimits::default_v1();
        let mut p = ConversationPackV1::new(11, 48, z32(b"snap4"), z32(b"sig4"), None, limits);
        p.messages.push(ConversationMessage {
            role: ConversationRole::User,
            content: "hello".to_string(),
            replay_id: None,
        });
        p.canonicalize_in_place();

        let bytes = encode_with_advisory_without_presentation(&p);
        let dec = ConversationPackV1::decode(&bytes).unwrap();
        assert_eq!(dec.markov_model_id, None);
        assert_eq!(dec.exemplar_memory_id, None);
        assert_eq!(dec.graph_relevance_id, None);
        assert_eq!(dec.presentation_mode, None);
        assert_eq!(dec.messages, p.messages);
    }

    #[test]
    fn conversation_pack_decode_legacy_bytes_without_advisory_ids() {
        let limits = ConversationLimits::default_v1();
        let mut p = ConversationPackV1::new(3, 32, z32(b"snap2"), z32(b"sig2"), None, limits);
        p.messages.push(ConversationMessage {
            role: ConversationRole::User,
            content: "hello".to_string(),
            replay_id: None,
        });
        p.canonicalize_in_place();

        let bytes = encode_legacy_without_advisory(&p);
        let dec = ConversationPackV1::decode(&bytes).unwrap();
        assert_eq!(dec.markov_model_id, None);
        assert_eq!(dec.exemplar_memory_id, None);
        assert_eq!(dec.graph_relevance_id, None);
        assert_eq!(dec.presentation_mode, None);
        assert_eq!(dec.messages, p.messages);
    }
}
