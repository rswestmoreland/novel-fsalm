// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! PuzzleSketchArtifactV1 schema and codec.
//!
//! This artifact persists a bounded, deterministic PuzzleSketchV1 across turns.
//! It is intended to support conversational continuation for logic puzzles:
//! - a first turn yields a sketch and a clarifying question
//! - a later turn can merge a clarification reply into the prior sketch
//!
//! The artifact does not attempt to solve puzzles. It only persists the
//! intermediate sketch state in canonical form.

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::hash::{blake3_hash, Hash32};
use crate::puzzle_sketch_v1::{PuzzleShapeHintV1, PuzzleSketchV1};

/// PuzzleSketchArtifactV1 schema version.
pub const PUZZLE_SKETCH_ARTIFACT_V1_VERSION: u32 = 1;

/// Maximum number of variables stored in the artifact.
pub const PUZZLE_SKETCH_ARTIFACT_V1_MAX_VARS: usize = 16;

/// Maximum UTF-8 bytes allowed for a single variable name.
pub const PUZZLE_SKETCH_ARTIFACT_V1_MAX_VAR_BYTES: usize = 64;

/// Puzzle sketch flags (v1).
pub type PuzzleSketchFlagsV1 = u32;

/// Sketch was derived using a lexicon snapshot.
pub const PSA_FLAG_USED_LEXICON: PuzzleSketchFlagsV1 = 1u32 << 0;

/// Sketch was merged with a clarification reply.
pub const PSA_FLAG_MERGED: PuzzleSketchFlagsV1 = 1u32 << 1;

/// Sketch is pending additional details (clarification expected).
pub const PSA_FLAG_PENDING: PuzzleSketchFlagsV1 = 1u32 << 2;

/// Mask of all known v1 flags.
pub const PSA_FLAGS_V1_ALL: PuzzleSketchFlagsV1 = PSA_FLAG_USED_LEXICON | PSA_FLAG_MERGED | PSA_FLAG_PENDING;

fn shape_to_u8(s: PuzzleShapeHintV1) -> u8 {
    match s {
        PuzzleShapeHintV1::Unknown => 0,
        PuzzleShapeHintV1::Ordering => 1,
        PuzzleShapeHintV1::Matching => 2,
    }
}

fn shape_from_u8(v: u8) -> Result<PuzzleShapeHintV1, DecodeError> {
    match v {
        0 => Ok(PuzzleShapeHintV1::Unknown),
        1 => Ok(PuzzleShapeHintV1::Ordering),
        2 => Ok(PuzzleShapeHintV1::Matching),
        _ => Err(DecodeError::new("bad PuzzleShapeHintV1")),
    }
}

fn is_sorted_unique(vars: &[String]) -> bool {
    let mut prev: Option<&str> = None;
    for v in vars {
        if v.is_empty() {
            return false;
        }
        if let Some(p) = prev {
            if v.as_str() <= p {
                return false;
            }
        }
        prev = Some(v.as_str());
    }
    true
}

fn canonicalize_vars(mut vars: Vec<String>) -> Result<Vec<String>, EncodeError> {
    for v in &vars {
        if v.is_empty() {
            return Err(EncodeError::new("empty var name"));
        }
        if v.len() > PUZZLE_SKETCH_ARTIFACT_V1_MAX_VAR_BYTES {
            return Err(EncodeError::new("var name too long"));
        }
    }
    vars.sort();
    vars.dedup();
    if vars.len() > PUZZLE_SKETCH_ARTIFACT_V1_MAX_VARS {
        vars.truncate(PUZZLE_SKETCH_ARTIFACT_V1_MAX_VARS);
    }
    Ok(vars)
}

/// Canonical puzzle sketch artifact (v1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PuzzleSketchArtifactV1 {
    /// Version.
    pub version: u32,
    /// PromptPack id that produced this sketch.
    pub prompt_id: Hash32,
    /// Index of the user message treated as the query in the PromptPack.
    pub query_msg_ix: u32,
    /// Flags.
    pub flags: PuzzleSketchFlagsV1,
    /// Hash of the source bytes used to compute the sketch.
    pub source_hash: Hash32,

    /// True if this prompt likely intends a logic/constraint puzzle.
    pub is_logic_puzzle_likely: bool,
    /// Variable/entity name candidates (canonical order: lexical asc, unique).
    pub var_names: Vec<String>,
    /// Numeric domain range hint, inclusive (lo, hi).
    pub domain_range: Option<(i32, i32)>,
    /// True if the text contains explicit constraint operator signals.
    pub has_constraints: bool,
    /// Coarse shape hint.
    pub shape: PuzzleShapeHintV1,
}

impl PuzzleSketchArtifactV1 {
    /// Build a canonical artifact from a PuzzleSketchV1.
    pub fn from_sketch(
        prompt_id: Hash32,
        query_msg_ix: u32,
        used_lexicon: bool,
        merged: bool,
        pending: bool,
        source_hash: Hash32,
        sketch: &PuzzleSketchV1,
    ) -> Result<Self, EncodeError> {
        let mut flags: PuzzleSketchFlagsV1 = 0;
        if used_lexicon {
            flags |= PSA_FLAG_USED_LEXICON;
        }
        if merged {
            flags |= PSA_FLAG_MERGED;
        }
        if pending {
            flags |= PSA_FLAG_PENDING;
        }

        let mut domain_range = sketch.domain_range;
        if let Some((a, b)) = domain_range {
            if a <= b {
                domain_range = Some((a, b));
            } else {
                domain_range = Some((b, a));
            }
            if a == b {
                domain_range = None;
            }
        }

        let vars = canonicalize_vars(sketch.var_names.clone())?;

        let out = Self {
            version: PUZZLE_SKETCH_ARTIFACT_V1_VERSION,
            prompt_id,
            query_msg_ix,
            flags,
            source_hash,
            is_logic_puzzle_likely: sketch.is_logic_puzzle_likely,
            var_names: vars,
            domain_range,
            has_constraints: sketch.has_constraints,
            shape: sketch.shape,
        };
        out.validate().map_err(|_| EncodeError::new("validate failed"))?;
        Ok(out)
    }

    /// Validate schema invariants.
    pub fn validate(&self) -> Result<(), DecodeError> {
        if self.version != PUZZLE_SKETCH_ARTIFACT_V1_VERSION {
            return Err(DecodeError::new("bad PuzzleSketchArtifactV1 version"));
        }
        if (self.flags & !PSA_FLAGS_V1_ALL) != 0 {
            return Err(DecodeError::new("bad PuzzleSketchArtifactV1 flags"));
        }
        if self.var_names.len() > PUZZLE_SKETCH_ARTIFACT_V1_MAX_VARS {
            return Err(DecodeError::new("too many var names"));
        }
        for v in &self.var_names {
            if v.is_empty() {
                return Err(DecodeError::new("empty var name"));
            }
            if v.len() > PUZZLE_SKETCH_ARTIFACT_V1_MAX_VAR_BYTES {
                return Err(DecodeError::new("var name too long"));
            }
        }
        if !is_sorted_unique(&self.var_names) {
            return Err(DecodeError::new("non-canonical var names"));
        }
        if let Some((a, b)) = self.domain_range {
            if a > b {
                return Err(DecodeError::new("bad domain_range"));
            }
            if a == b {
                return Err(DecodeError::new("degenerate domain_range"));
            }
        }
        Ok(())
    }

    /// Encode to canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        self.validate().map_err(|_| EncodeError::new("validate failed"))?;

        let mut w = ByteWriter::with_capacity(256 + self.var_names.len() * 16);
        w.write_u32(self.version);
        w.write_raw(&self.prompt_id);
        w.write_u32(self.query_msg_ix);
        w.write_u32(self.flags);
        w.write_raw(&self.source_hash);

        w.write_u8(if self.is_logic_puzzle_likely { 1 } else { 0 });
        w.write_u16(self.var_names.len() as u16);
        for v in &self.var_names {
            w.write_u16(v.len() as u16);
            w.write_raw(v.as_bytes());
        }
        match self.domain_range {
            Some((a, b)) => {
                w.write_u8(1);
                w.write_i64(a as i64);
                w.write_i64(b as i64);
            }
            None => {
                w.write_u8(0);
            }
        }
        w.write_u8(if self.has_constraints { 1 } else { 0 });
        w.write_u8(shape_to_u8(self.shape));
        Ok(w.into_bytes())
    }

    /// Decode from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let version = r.read_u32()?;
        if version != PUZZLE_SKETCH_ARTIFACT_V1_VERSION {
            return Err(DecodeError::new("bad PuzzleSketchArtifactV1 version"));
        }
        let pid_b = r.read_fixed(32)?;
        let mut prompt_id = [0u8; 32];
        prompt_id.copy_from_slice(pid_b);
        let query_msg_ix = r.read_u32()?;
        let flags = r.read_u32()?;
        if (flags & !PSA_FLAGS_V1_ALL) != 0 {
            return Err(DecodeError::new("bad PuzzleSketchArtifactV1 flags"));
        }
        let sh_b = r.read_fixed(32)?;
        let mut source_hash = [0u8; 32];
        source_hash.copy_from_slice(sh_b);

        let is_logic = match r.read_u8()? {
            0 => false,
            1 => true,
            _ => return Err(DecodeError::new("bad is_logic_puzzle_likely")),
        };

        let n = r.read_u16()? as usize;
        if n > PUZZLE_SKETCH_ARTIFACT_V1_MAX_VARS {
            return Err(DecodeError::new("too many var names"));
        }
        let mut var_names: Vec<String> = Vec::with_capacity(n);
        for _ in 0..n {
            let ln = r.read_u16()? as usize;
            if ln == 0 {
                return Err(DecodeError::new("empty var name"));
            }
            if ln > PUZZLE_SKETCH_ARTIFACT_V1_MAX_VAR_BYTES {
                return Err(DecodeError::new("var name too long"));
            }
            let b = r.read_fixed(ln)?;
            let s = match core::str::from_utf8(b) {
                Ok(x) => x.to_string(),
                Err(_) => return Err(DecodeError::new("invalid utf8 var name")),
            };
            var_names.push(s);
        }

        let domain_present = r.read_u8()?;
        let domain_range = match domain_present {
            0 => None,
            1 => {
                let a64 = r.read_i64()?;
                let b64 = r.read_i64()?;
                if a64 < (i32::MIN as i64) || a64 > (i32::MAX as i64) {
                    return Err(DecodeError::new("domain_range out of range"));
                }
                if b64 < (i32::MIN as i64) || b64 > (i32::MAX as i64) {
                    return Err(DecodeError::new("domain_range out of range"));
                }
                Some((a64 as i32, b64 as i32))
            }
            _ => return Err(DecodeError::new("bad domain_range tag")),
        };

        let has_constraints = match r.read_u8()? {
            0 => false,
            1 => true,
            _ => return Err(DecodeError::new("bad has_constraints")),
        };
        let shape = shape_from_u8(r.read_u8()?)?;

        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        let out = Self {
            version,
            prompt_id,
            query_msg_ix,
            flags,
            source_hash,
            is_logic_puzzle_likely: is_logic,
            var_names,
            domain_range,
            has_constraints,
            shape,
        };
        out.validate()?;
        Ok(out)
    }
}

/// Compute a stable source hash for puzzle sketch inputs.
pub fn puzzle_sketch_source_hash_v1(text: &str) -> Hash32 {
    let mut b: Vec<u8> = Vec::with_capacity(16 + text.len());
    b.extend_from_slice(b"puzzle_sketch_source_v1\0");
    b.extend_from_slice(text.as_bytes());
    blake3_hash(&b)
}

/// Compute a stable source hash for a merged sketch (prior source + reply).
///
/// This chains the previous sketch source hash with the current reply text,
/// producing a deterministic lineage hash for cross-turn continuation.
pub fn puzzle_sketch_merged_source_hash_v1(prev_source_hash: &Hash32, reply_text: &str) -> Hash32 {
    let mut b: Vec<u8> = Vec::with_capacity(32 + 32 + 1 + reply_text.len());
    b.extend_from_slice(b"puzzle_sketch_merge_source_v1\0");
    b.extend_from_slice(prev_source_hash);
    b.push(0);
    b.extend_from_slice(reply_text.as_bytes());
    blake3_hash(&b)
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::puzzle_sketch_v1::{build_puzzle_sketch_v1, PuzzleSketchCfgV1};

    #[test]
    fn artifact_roundtrip_is_canonical() {
        let cfg = PuzzleSketchCfgV1::default();
        let sk = build_puzzle_sketch_v1("A,B,C are numbers 1..3. A != B.", None, None, cfg);
        let pid = blake3_hash(b"pid");
        let sh = puzzle_sketch_source_hash_v1("A,B,C are numbers 1..3. A != B.");
        let a = PuzzleSketchArtifactV1::from_sketch(pid, 3, false, false, true, sh, &sk).unwrap();
        let enc = a.encode().unwrap();
        let b = PuzzleSketchArtifactV1::decode(&enc).unwrap();
        assert_eq!(a, b);
        assert!(is_sorted_unique(&b.var_names));
    }

    #[test]
    fn merged_source_hash_is_deterministic() {
        let a = puzzle_sketch_source_hash_v1("A,B,C");
        let h1 = puzzle_sketch_merged_source_hash_v1(&a, "1..3");
        let h2 = puzzle_sketch_merged_source_hash_v1(&a, "1..3");
        assert_eq!(h1, h2);
        let h3 = puzzle_sketch_merged_source_hash_v1(&a, "ordering");
        assert_ne!(h1, h3);
    }

}
