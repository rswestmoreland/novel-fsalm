// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Bridge expansion budget contract.
//!
//! This module defines a small, canonical configuration type that captures
//! expansion budgets and per-channel weight multipliers. It is schema-only in
//! (no wiring into retrieval yet).
//!
//! The budget is intended to bound and stabilize query expansion across
//! multiple channels (LEX/META/ENT/GRAPH) while preserving deterministic
//! ordering and tie-breaking.

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};

/// ExpansionBudgetV1 schema version.
pub const EXPANSION_BUDGET_V1_VERSION: u32 = 1;

/// Maximum number of kind budget entries allowed in v1.
pub const EXPANSION_BUDGET_V1_MAX_KINDS: usize = 32;

/// Expansion channel kinds (v1).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
#[repr(u8)]
pub enum ExpansionKindV1 {
    /// Lexical expansions (morphology, synonym edges, etc.).
    Lex = 1,
    /// Metaphonetic expansions (metaphone codes).
    Meta = 2,
    /// Identity/alias expansions (canonical entity edges).
    Ent = 3,
    /// Graph adjacency expansions (future coprocessor).
    Graph = 4,
}

impl ExpansionKindV1 {
    fn from_u8(v: u8) -> Result<Self, DecodeError> {
        match v {
            1 => Ok(ExpansionKindV1::Lex),
            2 => Ok(ExpansionKindV1::Meta),
            3 => Ok(ExpansionKindV1::Ent),
            4 => Ok(ExpansionKindV1::Graph),
            _ => Err(DecodeError::new("bad ExpansionKindV1")),
        }
    }
}

/// Per-kind budget entry (v1).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExpansionKindBudgetV1 {
    /// Channel kind this entry applies to.
    pub kind: ExpansionKindV1,
    /// Maximum number of expansion items to select from this channel.
    pub max_total: u16,
    /// Maximum number of expansion items to take per base feature for this channel.
    pub max_per_base: u8,
    /// Weight multiplier in Q16 fixed-point (1.0 == 65536).
    ///
    /// The effective weight for ranking is:
    /// effective = clamp_u16((base_weight * weight_mul_q16) >> 16)
    ///
    /// does not define the base_weight table; see docs/BRIDGE_EXPANSION.md.
    pub weight_mul_q16: u32,
    /// Minimum effective weight required to keep a candidate (after multiplier).
    pub weight_floor: u16,
}

impl ExpansionKindBudgetV1 {
    /// Construct a new kind budget entry.
    pub fn new(
        kind: ExpansionKindV1,
        max_total: u16,
        max_per_base: u8,
        weight_mul_q16: u32,
        weight_floor: u16,
    ) -> Self {
        ExpansionKindBudgetV1 {
            kind,
            max_total,
            max_per_base,
            weight_mul_q16,
            weight_floor,
        }
    }
}

/// Bridge expansion budget configuration (v1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpansionBudgetV1 {
    /// Schema version (must be EXPANSION_BUDGET_V1_VERSION).
    pub version: u32,
    /// Maximum number of expansion items total (required + optional).
    pub max_expansions_total: u16,
    /// Maximum number of required expansion items total.
    pub max_required_total: u16,
    /// Maximum number of expansion items to select per base feature id.
    pub max_expansions_per_base: u8,
    /// Per-kind caps and weight multipliers.
    pub kinds: Vec<ExpansionKindBudgetV1>,
}

/// ExpansionBudgetV1 validation errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpansionBudgetError {
    /// version is not supported.
    BadVersion,
    /// max_expansions_total must be >= 1.
    MaxTotalZero,
    /// max_required_total must be <= max_expansions_total.
    RequiredExceedsTotal,
    /// Too many kind budget entries.
    TooManyKinds,
    /// Kind budgets must be in canonical order (kind asc) with no duplicates.
    NotCanonical,
}

impl core::fmt::Display for ExpansionBudgetError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ExpansionBudgetError::BadVersion => f.write_str("bad expansion budget version"),
            ExpansionBudgetError::MaxTotalZero => f.write_str("max_expansions_total must be >= 1"),
            ExpansionBudgetError::RequiredExceedsTotal => {
                f.write_str("max_required_total exceeds max_expansions_total")
            }
            ExpansionBudgetError::TooManyKinds => f.write_str("too many kind entries"),
            ExpansionBudgetError::NotCanonical => f.write_str("kind entries not canonical"),
        }
    }
}

impl std::error::Error for ExpansionBudgetError {}

impl ExpansionBudgetV1 {
    /// Default v1 budget matching docs/BRIDGE_EXPANSION.md suggested caps.
    pub fn default_v1() -> Self {
        ExpansionBudgetV1 {
            version: EXPANSION_BUDGET_V1_VERSION,
            max_expansions_total: 64,
            max_required_total: 24,
            max_expansions_per_base: 8,
            kinds: vec![
                // Note: canonical order is kind asc.
                ExpansionKindBudgetV1::new(ExpansionKindV1::Lex, 24, 8, 65536, 0),
                ExpansionKindBudgetV1::new(ExpansionKindV1::Meta, 16, 4, 65536, 0),
                ExpansionKindBudgetV1::new(ExpansionKindV1::Ent, 8, 4, 65536, 0),
                ExpansionKindBudgetV1::new(ExpansionKindV1::Graph, 8, 4, 65536, 0),
            ],
        }
    }

    /// Return true if `kinds` is sorted by kind asc and contains no duplicates.
    pub fn is_canonical(&self) -> bool {
        let mut prev: Option<ExpansionKindV1> = None;
        for kb in &self.kinds {
            if let Some(p) = prev {
                if kb.kind <= p {
                    return false;
                }
            }
            prev = Some(kb.kind);
        }
        true
    }

    /// Validate invariants.
    pub fn validate(&self) -> Result<(), ExpansionBudgetError> {
        if self.version != EXPANSION_BUDGET_V1_VERSION {
            return Err(ExpansionBudgetError::BadVersion);
        }
        if self.max_expansions_total == 0 {
            return Err(ExpansionBudgetError::MaxTotalZero);
        }
        if self.max_required_total > self.max_expansions_total {
            return Err(ExpansionBudgetError::RequiredExceedsTotal);
        }
        if self.kinds.len() > EXPANSION_BUDGET_V1_MAX_KINDS {
            return Err(ExpansionBudgetError::TooManyKinds);
        }
        if !self.is_canonical() {
            return Err(ExpansionBudgetError::NotCanonical);
        }
        Ok(())
    }

    /// Sort kind entries into canonical order.
    ///
    /// This does not remove duplicates. Use validate to enforce strict
    /// no-duplicate form.
    pub fn canonicalize_in_place(&mut self) {
        self.kinds.sort_by_key(|k| k.kind as u8);
    }

    /// Encode to canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        if self.version != EXPANSION_BUDGET_V1_VERSION {
            return Err(EncodeError::new("bad expansion budget version"));
        }
        if !self.is_canonical() {
            return Err(EncodeError::new("non-canonical kinds"));
        }
        if self.kinds.len() > EXPANSION_BUDGET_V1_MAX_KINDS {
            return Err(EncodeError::new("too many kinds"));
        }

        let cap = 32 + self.kinds.len() * 16;
        let mut w = ByteWriter::with_capacity(cap);
        w.write_u32(self.version);
        w.write_u16(self.max_expansions_total);
        w.write_u16(self.max_required_total);
        w.write_u8(self.max_expansions_per_base);
        w.write_u8(self.kinds.len() as u8);
        for kb in &self.kinds {
            w.write_u8(kb.kind as u8);
            w.write_u16(kb.max_total);
            w.write_u8(kb.max_per_base);
            w.write_u32(kb.weight_mul_q16);
            w.write_u16(kb.weight_floor);
        }
        Ok(w.into_bytes())
    }

    /// Decode from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let version = r.read_u32()?;
        if version != EXPANSION_BUDGET_V1_VERSION {
            return Err(DecodeError::new("bad expansion budget version"));
        }
        let max_expansions_total = r.read_u16()?;
        let max_required_total = r.read_u16()?;
        let max_expansions_per_base = r.read_u8()?;
        let kcount = r.read_u8()? as usize;
        if kcount > EXPANSION_BUDGET_V1_MAX_KINDS {
            return Err(DecodeError::new("too many kinds"));
        }
        let mut kinds: Vec<ExpansionKindBudgetV1> = Vec::with_capacity(kcount);
        for _ in 0..kcount {
            let kind = ExpansionKindV1::from_u8(r.read_u8()?)?;
            let max_total = r.read_u16()?;
            let max_per_base = r.read_u8()?;
            let weight_mul_q16 = r.read_u32()?;
            let weight_floor = r.read_u16()?;
            kinds.push(ExpansionKindBudgetV1::new(
                kind,
                max_total,
                max_per_base,
                weight_mul_q16,
                weight_floor,
            ));
        }
        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        kinds.sort_by_key(|k| k.kind as u8);
        for i in 1..kinds.len() {
            if kinds[i - 1].kind == kinds[i].kind {
                return Err(DecodeError::new("duplicate kind"));
            }
        }
        if max_expansions_total == 0 {
            return Err(DecodeError::new("max_expansions_total must be >= 1"));
        }
        if max_required_total > max_expansions_total {
            return Err(DecodeError::new(
                "max_required_total exceeds max_expansions_total",
            ));
        }

        Ok(ExpansionBudgetV1 {
            version,
            max_expansions_total,
            max_required_total,
            max_expansions_per_base,
            kinds,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expansion_budget_round_trip_and_canonical_order() {
        let mut b = ExpansionBudgetV1::default_v1();
        b.kinds.swap(0, 3);
        assert!(!b.is_canonical());
        b.canonicalize_in_place();
        assert!(b.is_canonical());
        b.validate().unwrap();

        let bytes = b.encode().unwrap();
        let dec = ExpansionBudgetV1::decode(&bytes).unwrap();
        assert_eq!(dec, b);
        assert!(dec.is_canonical());
    }

    #[test]
    fn expansion_budget_decode_rejects_duplicate_kind() {
        let mut w = ByteWriter::with_capacity(64);
        w.write_u32(EXPANSION_BUDGET_V1_VERSION);
        w.write_u16(10);
        w.write_u16(5);
        w.write_u8(2);
        w.write_u8(2);
        // two Lex entries
        w.write_u8(ExpansionKindV1::Lex as u8);
        w.write_u16(1);
        w.write_u8(1);
        w.write_u32(65536);
        w.write_u16(0);
        w.write_u8(ExpansionKindV1::Lex as u8);
        w.write_u16(1);
        w.write_u8(1);
        w.write_u32(65536);
        w.write_u16(0);
        let bytes = w.into_bytes();
        assert!(ExpansionBudgetV1::decode(&bytes).is_err());
    }
}
