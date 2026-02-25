// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! PromptPack: canonical, bounded request artifact for the runtime.
//!
//! PromptPack is the primary user-facing request artifact. It must be:
//! - canonical: identical logical content encodes to identical bytes
//! - deterministic: ordering rules are explicit (no map iteration dependence)
//! - stable: fields evolve via versioning
//!
//! scope:
//! - Define core PromptPack structures
//! - Implement canonical encode/decode (no serde)
//! - Add basic tests for round-trip and canonical ordering

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::hash::{Hash32};

/// PromptPack version (v1).
pub const PROMPT_PACK_VERSION: u16 = 1;

/// Message role for a chat prompt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// System message (policy/instructions).
    System,
    /// User message.
    User,
    /// Assistant message (prior assistant content).
    Assistant,
}

impl Role {
    fn to_u8(self) -> u8 {
        match self {
            Role::System => 0,
            Role::User => 1,
            Role::Assistant => 2,
        }
    }

    fn from_u8(v: u8) -> Result<Self, DecodeError> {
        match v {
            0 => Ok(Role::System),
            1 => Ok(Role::User),
            2 => Ok(Role::Assistant),
            _ => Err(DecodeError::new("invalid role")),
        }
    }
}

/// A single chat message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// Role.
    pub role: Role,
    /// UTF-8 content.
    pub content: String,
}

/// Determinism-critical IDs for reproducible runs.
///
/// These are hashes of the underlying immutable resources.
#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub struct PromptIds {
    /// Snapshot ID for IR memory (or zero hash if unused).
    pub snapshot_id: Hash32,
    /// Model weights ID (or zero hash if unused).
    pub weights_id: Hash32,
    /// Tokenizer ID (or zero hash if unused).
    pub tokenizer_id: Hash32,
}

/// A simple key/value constraint.
///
/// uses a generic form. Later stages will introduce typed constraints and
/// guarded decoding directives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConstraintKV {
    /// Constraint key (UTF-8).
    pub key: String,
    /// Constraint value (UTF-8).
    pub value: String,
}

/// Canonical PromptPack request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptPack {
    /// Version.
    pub version: u16,
    /// Deterministic RNG seed for decoding and any randomized tooling.
    pub seed: u64,
    /// Maximum output tokens requested (bounded by runtime).
    pub max_output_tokens: u32,
    /// Determinism-critical IDs.
    pub ids: PromptIds,
    /// Ordered chat messages.
    pub messages: Vec<Message>,
    /// Additional constraints (canonical encoding sorts by key then value).
    pub constraints: Vec<ConstraintKV>,

}

/// Limits applied to a PromptPack to bound size deterministically.
///
/// These limits are enforced by `PromptPack::canonicalize_in_place`.
/// All limits are byte- and count-based (UTF-8 bytes), not token-based.
///
/// Notes:
/// - Token-based limits require a tokenizer; that comes later.
/// - Byte limits are still useful for early CPU-first prototypes and for
/// deterministic truncation behavior that does not depend on tokenization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PromptLimits {
    /// Maximum number of messages allowed in the pack.
    pub max_messages: u32,
    /// Maximum total UTF-8 content bytes across all kept messages.
    pub max_total_message_bytes: u32,
    /// Maximum UTF-8 bytes allowed per message content.
    pub max_message_bytes: u32,
    /// Whether to prioritize keeping System messages.
    pub keep_system: bool,

    /// Maximum number of constraints allowed.
    pub max_constraints: u32,
    /// Maximum total UTF-8 bytes across all kept constraints (key+value bytes).
    pub max_total_constraint_bytes: u32,
}

impl PromptLimits {
    /// A conservative default for early prototypes.
    pub fn default_v1() -> Self {
        Self {
            max_messages: 64,
            max_total_message_bytes: 64 * 1024,
            max_message_bytes: 16 * 1024,
            keep_system: true,
            max_constraints: 64,
            max_total_constraint_bytes: 8 * 1024,
        }
    }
}

/// Report describing canonicalization actions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalizeReport {
    /// Messages before canonicalization.
    pub messages_before: usize,
    /// Messages after canonicalization.
    pub messages_after: usize,
    /// Messages dropped due to limits.
    pub messages_dropped: usize,
    /// Messages whose content was truncated.
    pub messages_truncated: usize,

    /// Constraints before canonicalization.
    pub constraints_before: usize,
    /// Constraints after canonicalization.
    pub constraints_after: usize,
    /// Constraints dropped due to limits.
    pub constraints_dropped: usize,
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


impl PromptPack {
    /// Create a new PromptPack with default version.
    pub fn new(seed: u64, max_output_tokens: u32, ids: PromptIds) -> Self {
        Self {
            version: PROMPT_PACK_VERSION,
            seed,
            max_output_tokens,
            ids,
            messages: Vec::new(),
            constraints: Vec::new(),
        }
    }

    /// Add a constraint key/value pair to this PromptPack.
    ///
    /// Constraints are later canonicalized (sorted and truncated) by
    /// `canonicalize_in_place`.
    pub fn add_constraint(&mut self, key: &str, value: &str) {
        self.constraints.push(ConstraintKV {
            key: key.to_string(),
            value: value.to_string(),
        });
    }


    /// Canonicalize the PromptPack in place under the given limits.
    ///
    /// Actions:
    /// - Truncates each message content to `max_message_bytes` (UTF-8 prefix).
    /// - Drops messages if `max_messages` or `max_total_message_bytes` are exceeded.
    /// If `keep_system` is true, System messages are kept preferentially.
    /// - Sorts constraints by (key asc, value asc) and truncates by count/bytes.
    ///
    /// This method is intended to be called before storing or hashing a PromptPack
    /// when a bounded, deterministic representation is required.
    pub fn canonicalize_in_place(&mut self, limits: PromptLimits) -> CanonicalizeReport {
        let messages_before = self.messages.len();
        let constraints_before = self.constraints.len();

        // 1) Per-message truncation.
        let max_msg_bytes = clamp_u32_to_usize(limits.max_message_bytes);
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
        let max_messages = clamp_u32_to_usize(limits.max_messages);
        let max_total_bytes = clamp_u32_to_usize(limits.max_total_message_bytes);

        let mut total_bytes = 0usize;
        for m in &self.messages {
            total_bytes += m.content.len();
        }

        if self.messages.len() > max_messages || total_bytes > max_total_bytes {
            let old = core::mem::take(&mut self.messages);
            let n = old.len();

            // Select indices deterministically.
            let mut keep_idx: Vec<usize> = Vec::new();
            keep_idx.reserve(core::cmp::min(n, max_messages));

            if limits.keep_system {
                for (i, m) in old.iter().enumerate() {
                    if m.role == Role::System {
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
                    if limits.keep_system && m.role == Role::System {
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

            let mut kept: Vec<Message> = Vec::with_capacity(keep_idx.len());
            total_bytes = 0;
            for (i, m) in old.into_iter().enumerate() {
                if keep_mask[i] {
                    total_bytes += m.content.len();
                    kept.push(m);
                }
            }

            if total_bytes > max_total_bytes {
                // Prefer truncating the most recent kept message to satisfy the total
                // byte budget while retaining message count.
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

                // If still too large, drop oldest messages deterministically.
                // Prefer to keep System messages if keep_system is enabled.
                let mut i = 0usize;
                while total_bytes > max_total_bytes && kept.len() > 1 && i < kept.len() {
                    let can_drop = if limits.keep_system {
                        kept[i].role != Role::System
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

                // If still too large (extreme case), truncate the last remaining message to fit.
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

                // Final deterministic fallback: if budget is extremely small, allow dropping
                // to reach the budget.
                while total_bytes > max_total_bytes && !kept.is_empty() {
                    total_bytes -= kept[0].content.len();
                    kept.remove(0);
                }
            }

            self.messages = kept;
        }

        let messages_after = self.messages.len();
        let messages_dropped = messages_before.saturating_sub(messages_after);

        // 3) Constraints canonicalization.
        self.constraints.sort_by(|a, b| match a.key.cmp(&b.key) {
            core::cmp::Ordering::Equal => a.value.cmp(&b.value),
            other => other,
        });

        let max_constraints = clamp_u32_to_usize(limits.max_constraints);
        if self.constraints.len() > max_constraints {
            self.constraints.truncate(max_constraints);
        }

        let max_cbytes = clamp_u32_to_usize(limits.max_total_constraint_bytes);
        let mut cbytes = 0usize;
        for c in &self.constraints {
            cbytes += c.key.len();
            cbytes += c.value.len();
        }
        if cbytes > max_cbytes {
            while cbytes > max_cbytes && !self.constraints.is_empty() {
                let last = self.constraints.pop().unwrap();
                cbytes = cbytes.saturating_sub(last.key.len() + last.value.len());
            }
        }

        let constraints_after = self.constraints.len();
        let constraints_dropped = constraints_before.saturating_sub(constraints_after);

        CanonicalizeReport {
            messages_before,
            messages_after,
            messages_dropped,
            messages_truncated,
            constraints_before,
            constraints_after,
            constraints_dropped,
        }
    }

    /// Encode assuming the PromptPack is already canonical (constraints sorted and truncated).
    ///
    /// This avoids cloning constraints for sorting, reducing allocations. Call
    /// `canonicalize_in_place` first if you need canonical ordering.
    pub fn encode_assuming_canonical(&self) -> Result<Vec<u8>, EncodeError> {
        let mut w = ByteWriter::with_capacity(512);
        w.write_u16(self.version);
        w.write_u64(self.seed);
        w.write_u32(self.max_output_tokens);

        w.write_raw(&self.ids.snapshot_id);
        w.write_raw(&self.ids.weights_id);
        w.write_raw(&self.ids.tokenizer_id);

        if self.messages.len() > (u32::MAX as usize) {
            return Err(EncodeError::new("too many messages"));
        }
        w.write_u32(self.messages.len() as u32);
        for m in &self.messages {
            w.write_u8(m.role.to_u8());
            w.write_str(&m.content)?;
        }

        if self.constraints.len() > (u32::MAX as usize) {
            return Err(EncodeError::new("too many constraints"));
        }
        w.write_u32(self.constraints.len() as u32);
        for c in &self.constraints {
            w.write_str(&c.key)?;
            w.write_str(&c.value)?;
        }

        Ok(w.into_bytes())
    }

    /// Encode to canonical bytes.
    ///
    /// This encoding is canonical even if `self.constraints` are not sorted.
    /// Constraints are cloned and sorted during encoding.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        let mut w = ByteWriter::with_capacity(512);
        w.write_u16(self.version);
        w.write_u64(self.seed);
        w.write_u32(self.max_output_tokens);

        w.write_raw(&self.ids.snapshot_id);
        w.write_raw(&self.ids.weights_id);
        w.write_raw(&self.ids.tokenizer_id);

        if self.messages.len() > (u32::MAX as usize) {
            return Err(EncodeError::new("too many messages"));
        }
        w.write_u32(self.messages.len() as u32);
        for m in &self.messages {
            w.write_u8(m.role.to_u8());
            w.write_str(&m.content)?;
        }

        // Canonical constraint ordering: key asc, value asc.
        let mut cvs = self.constraints.clone();
        cvs.sort_by(|a, b| match a.key.cmp(&b.key) {
            core::cmp::Ordering::Equal => a.value.cmp(&b.value),
            other => other,
        });

        if cvs.len() > (u32::MAX as usize) {
            return Err(EncodeError::new("too many constraints"));
        }
        w.write_u32(cvs.len() as u32);
        for c in &cvs {
            w.write_str(&c.key)?;
            w.write_str(&c.value)?;
        }

        Ok(w.into_bytes())
    }

    /// Decode from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let version = r.read_u16()?;
        if version != PROMPT_PACK_VERSION {
            return Err(DecodeError::new("unsupported PromptPack version"));
        }
        let seed = r.read_u64()?;
        let max_output_tokens = r.read_u32()?;

        let mut snapshot_id = [0u8; 32];
        snapshot_id.copy_from_slice(r.read_fixed(32)?);
        let mut weights_id = [0u8; 32];
        weights_id.copy_from_slice(r.read_fixed(32)?);
        let mut tokenizer_id = [0u8; 32];
        tokenizer_id.copy_from_slice(r.read_fixed(32)?);

        let msg_n = r.read_u32()? as usize;
        let mut messages = Vec::with_capacity(msg_n);
        for _ in 0..msg_n {
            let role = Role::from_u8(r.read_u8()?)?;
            let content = r.read_str_view()?.to_string();
            messages.push(Message { role, content });
        }

        let c_n = r.read_u32()? as usize;
        let mut constraints = Vec::with_capacity(c_n);
        for _ in 0..c_n {
            let key = r.read_str_view()?.to_string();
            let value = r.read_str_view()?.to_string();
            constraints.push(ConstraintKV { key, value });
        }

        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        Ok(Self {
            version,
            seed,
            max_output_tokens,
            ids: PromptIds { snapshot_id, weights_id, tokenizer_id },
            messages,
            constraints,
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

    #[test]
    fn prompt_pack_round_trip() {
        let ids = PromptIds {
            snapshot_id: z32(b"snap"),
            weights_id: z32(b"w"),
            tokenizer_id: z32(b"tok"),
        };

        let mut p = PromptPack::new(123, 256, ids);
        p.messages.push(Message { role: Role::System, content: "system".to_string() });
        p.messages.push(Message { role: Role::User, content: "hello".to_string() });
        p.constraints.push(ConstraintKV { key: "format".to_string(), value: "plain".to_string() });

        let enc = p.encode().unwrap();
        let dec = PromptPack::decode(&enc).unwrap();
        assert_eq!(dec, p);
    }

    #[test]
    fn prompt_pack_canonical_constraint_ordering() {
        let ids = PromptIds {
            snapshot_id: z32(b"snap"),
            weights_id: z32(b"w"),
            tokenizer_id: z32(b"tok"),
        };

        let mut a = PromptPack::new(1, 64, ids.clone());
        a.messages.push(Message { role: Role::User, content: "x".to_string() });
        a.constraints.push(ConstraintKV { key: "b".to_string(), value: "2".to_string() });
        a.constraints.push(ConstraintKV { key: "a".to_string(), value: "9".to_string() });

        let mut b = PromptPack::new(1, 64, ids);
        b.messages.push(Message { role: Role::User, content: "x".to_string() });
        // Insert constraints in opposite order.
        b.constraints.push(ConstraintKV { key: "a".to_string(), value: "9".to_string() });
        b.constraints.push(ConstraintKV { key: "b".to_string(), value: "2".to_string() });

        let ea = a.encode().unwrap();
        let eb = b.encode().unwrap();
        assert_eq!(ea, eb, "canonical encoding must sort constraints");
    }

    #[test]
    fn prompt_pack_rejects_bad_role() {
        let ids = PromptIds {
            snapshot_id: z32(b"snap2"),
            weights_id: z32(b"w2"),
            tokenizer_id: z32(b"tok2"),
        };

        // Build a pack with one message and then corrupt the role byte.
        let mut p = PromptPack::new(1, 64, ids);
        p.messages.push(Message { role: Role::User, content: "hi".to_string() });

        let mut enc = p.encode().unwrap();

        // Layout:
        // u16 ver (2)
        // u64 seed (8)
        // u32 max (4)
        // 3 * hash32 (96)
        // u32 msg_n (4) -> value 1
        // u8 role (1) -> corrupt this to 9
        let role_pos = 2 + 8 + 4 + 96 + 4;
        enc[role_pos] = 9;

        assert!(PromptPack::decode(&enc).is_err());
    }

    #[test]
    fn prompt_pack_truncation_prefers_system() {
        let ids = PromptIds {
            snapshot_id: z32(b"snapx"),
            weights_id: z32(b"wx"),
            tokenizer_id: z32(b"tokx"),
        };

        let mut p = PromptPack::new(1, 64, ids);
        p.messages.push(Message { role: Role::System, content: "SYS".to_string() });
        p.messages.push(Message { role: Role::User, content: "u1".to_string() });
        p.messages.push(Message { role: Role::Assistant, content: "a1".to_string() });
        p.messages.push(Message { role: Role::User, content: "u2".to_string() });
        p.messages.push(Message { role: Role::Assistant, content: "a2".to_string() });

        let limits = PromptLimits {
            max_messages: 3,
            max_total_message_bytes: 1024,
            max_message_bytes: 1024,
            keep_system: true,
            max_constraints: 64,
            max_total_constraint_bytes: 1024,
        };

        let rep = p.canonicalize_in_place(limits);
        assert_eq!(rep.messages_after, 3);
        assert_eq!(p.messages[0].role, Role::System);
        assert_eq!(p.messages[1].content, "u2");
        assert_eq!(p.messages[2].content, "a2");
    }

    #[test]
    fn prompt_pack_truncates_by_total_bytes() {
        let ids = PromptIds {
            snapshot_id: z32(b"snapb"),
            weights_id: z32(b"wb"),
            tokenizer_id: z32(b"tokb"),
        };

        let mut p = PromptPack::new(1, 64, ids);
        p.messages.push(Message { role: Role::User, content: "12345".to_string() });
        p.messages.push(Message { role: Role::User, content: "67890".to_string() });

        let limits = PromptLimits {
            max_messages: 2,
            max_total_message_bytes: 7,
            max_message_bytes: 1024,
            keep_system: false,
            max_constraints: 64,
            max_total_constraint_bytes: 1024,
        };

        let rep = p.canonicalize_in_place(limits);
        assert_eq!(rep.messages_after, 2);
        assert!(rep.messages_truncated >= 1);
        assert_eq!(p.messages[0].content, "12345");
        assert!(p.messages[1].content.len() <= 2);
    }

    #[test]
    fn encode_assuming_canonical_matches_encode_after_canonicalize() {
        let ids = PromptIds {
            snapshot_id: z32(b"snapc"),
            weights_id: z32(b"wc"),
            tokenizer_id: z32(b"tokc"),
        };

        let mut p = PromptPack::new(9, 7, ids);
        p.messages.push(Message { role: Role::User, content: "hi".to_string() });
        p.constraints.push(ConstraintKV { key: "b".to_string(), value: "2".to_string() });
        p.constraints.push(ConstraintKV { key: "a".to_string(), value: "9".to_string() });

        let limits = PromptLimits::default_v1();
        p.canonicalize_in_place(limits);

        let e1 = p.encode().unwrap();
        let e2 = p.encode_assuming_canonical().unwrap();
        assert_eq!(e1, e2);
    }

}
