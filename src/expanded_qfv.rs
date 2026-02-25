// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Expanded query feature vector schema.
//!
//! This module defines the canonical, deterministic representation of a query
//! feature vector after bridge expansion.
//!
//! is schema-only:
//! - Defines item records (kind, id, weight, origin).
//! - Defines a split required/optional representation.
//! - Provides canonical codec + validation helpers.
//!
//! No retrieval wiring is performed in.

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::expansion_budget::ExpansionKindV1;
use crate::frame::Id64;

/// ExpandedQfvV1 schema version.
pub const EXPANDED_QFV_V1_VERSION: u32 = 1;

/// Maximum number of items allowed across required + optional.
pub const EXPANDED_QFV_V1_MAX_ITEMS: usize = 4096;

fn kind_from_u8(v: u8) -> Result<ExpansionKindV1, DecodeError> {
    match v {
        1 => Ok(ExpansionKindV1::Lex),
        2 => Ok(ExpansionKindV1::Meta),
        3 => Ok(ExpansionKindV1::Ent),
        4 => Ok(ExpansionKindV1::Graph),
        _ => Err(DecodeError::new("bad ExpansionKindV1")),
    }
}

fn cmp_canon(a: &ExpandedQfvItemV1, b: &ExpandedQfvItemV1) -> core::cmp::Ordering {
    // Canonical order:
    // - weight desc
    // - kind asc
    // - id asc
    // Origin fields do not participate in canonical ordering.
    match b.weight.cmp(&a.weight) {
        core::cmp::Ordering::Equal => {}
        o => return o,
    }
    match (a.kind as u8).cmp(&(b.kind as u8)) {
        core::cmp::Ordering::Equal => {}
        o => return o,
    }
    (a.id.0).cmp(&(b.id.0))
}

fn key_u72(kind: ExpansionKindV1, id: Id64) -> (u8, u64) {
    (kind as u8, id.0)
}

fn seen_insert_key(seen: &mut Vec<(u8, u64)>, key: (u8, u64)) -> bool {
    match seen.binary_search(&key) {
        Ok(_) => false,
        Err(pos) => {
            seen.insert(pos, key);
            true
        }
    }
}

/// One expanded feature item (v1).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExpandedQfvItemV1 {
    /// Channel kind for this item.
    pub kind: ExpansionKindV1,
    /// Expanded feature id (domain depends on kind).
    pub id: Id64,
    /// Final weight (0..65535). Higher means stronger adjacency/equivalence.
    pub weight: u16,
    /// Origin base kind (what produced this expansion).
    pub origin_base_kind: ExpansionKindV1,
    /// Origin base id (domain depends on origin_base_kind).
    pub origin_base_id: Id64,
    /// Origin rule id (stable, rules-first id; 0 may be used for "original").
    pub origin_rule_id: u16,
}

impl ExpandedQfvItemV1 {
    /// Construct a new expanded item.
    pub fn new(
        kind: ExpansionKindV1,
        id: Id64,
        weight: u16,
        origin_base_kind: ExpansionKindV1,
        origin_base_id: Id64,
        origin_rule_id: u16,
    ) -> Self {
        ExpandedQfvItemV1 {
            kind,
            id,
            weight,
            origin_base_kind,
            origin_base_id,
            origin_rule_id,
        }
    }
}

/// Expanded query feature vector (v1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpandedQfvV1 {
    /// Schema version (must be EXPANDED_QFV_V1_VERSION).
    pub version: u32,
    /// Tie-control id used when later stages need stable tie-breaking.
    pub tie_control_id: Id64,
    /// Required anchor items (used for precision intersections).
    pub required: Vec<ExpandedQfvItemV1>,
    /// Optional expansion items (used for bounded recall boosts).
    pub optional: Vec<ExpandedQfvItemV1>,
}

/// ExpandedQfvV1 validation errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpandedQfvError {
    /// version is not supported.
    BadVersion,
    /// Too many items.
    TooManyItems,
    /// Required list is not in canonical order or contains duplicates.
    RequiredNotCanonical,
    /// Optional list is not in canonical order or contains duplicates.
    OptionalNotCanonical,
    /// Duplicate (kind,id) across required and optional.
    DuplicateAcrossPools,
}

impl core::fmt::Display for ExpandedQfvError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ExpandedQfvError::BadVersion => f.write_str("bad expanded qfv version"),
            ExpandedQfvError::TooManyItems => f.write_str("too many expanded qfv items"),
            ExpandedQfvError::RequiredNotCanonical => f.write_str("required items not canonical"),
            ExpandedQfvError::OptionalNotCanonical => f.write_str("optional items not canonical"),
            ExpandedQfvError::DuplicateAcrossPools => f.write_str("duplicate item across pools"),
        }
    }
}

impl std::error::Error for ExpandedQfvError {}

impl ExpandedQfvV1 {
    /// Return true if required/optional lists are strictly canonical.
    pub fn is_canonical(&self) -> bool {
        fn list_is_canon(xs: &[ExpandedQfvItemV1]) -> bool {
            for i in 1..xs.len() {
                if cmp_canon(&xs[i - 1], &xs[i]) != core::cmp::Ordering::Less {
                    return false;
                }
            }
            true
        }
        list_is_canon(&self.required) && list_is_canon(&self.optional)
    }

    /// Validate invariants.
    pub fn validate(&self) -> Result<(), ExpandedQfvError> {
        if self.version != EXPANDED_QFV_V1_VERSION {
            return Err(ExpandedQfvError::BadVersion);
        }
        let total = self.required.len().saturating_add(self.optional.len());
        if total > EXPANDED_QFV_V1_MAX_ITEMS {
            return Err(ExpandedQfvError::TooManyItems);
        }
        if !self.is_canonical() {
            let mut ok_req = true;
            for i in 1..self.required.len() {
                if cmp_canon(&self.required[i - 1], &self.required[i]) != core::cmp::Ordering::Less
                {
                    ok_req = false;
                    break;
                }
            }
            if !ok_req {
                return Err(ExpandedQfvError::RequiredNotCanonical);
            }
            return Err(ExpandedQfvError::OptionalNotCanonical);
        }

        // Enforce uniqueness across pools (keyed by (kind,id)).
        let mut seen: Vec<(u8, u64)> = Vec::with_capacity(total);
        for it in self.required.iter().chain(self.optional.iter()) {
            let key = key_u72(it.kind, it.id);
            if !seen_insert_key(&mut seen, key) {
                return Err(ExpandedQfvError::DuplicateAcrossPools);
            }
        }
        Ok(())
    }

    /// Encode to canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        if self.version != EXPANDED_QFV_V1_VERSION {
            return Err(EncodeError::new("bad expanded qfv version"));
        }
        let total = self.required.len().saturating_add(self.optional.len());
        if total > EXPANDED_QFV_V1_MAX_ITEMS {
            return Err(EncodeError::new("too many expanded qfv items"));
        }
        if !self.is_canonical() {
            return Err(EncodeError::new("non-canonical expanded qfv"));
        }

        // Check duplicates across pools.
        let mut seen: Vec<(u8, u64)> = Vec::with_capacity(total);
        for it in self.required.iter().chain(self.optional.iter()) {
            let key = key_u72(it.kind, it.id);
            if !seen_insert_key(&mut seen, key) {
                return Err(EncodeError::new("duplicate expanded qfv item"));
            }
        }

        let cap = 32 + total * 32;
        let mut w = ByteWriter::with_capacity(cap);
        w.write_u32(self.version);
        w.write_u64(self.tie_control_id.0);
        w.write_u32(self.required.len() as u32);
        w.write_u32(self.optional.len() as u32);
        for it in &self.required {
            w.write_u8(it.kind as u8);
            w.write_u64(it.id.0);
            w.write_u16(it.weight);
            w.write_u8(it.origin_base_kind as u8);
            w.write_u64(it.origin_base_id.0);
            w.write_u16(it.origin_rule_id);
        }
        for it in &self.optional {
            w.write_u8(it.kind as u8);
            w.write_u64(it.id.0);
            w.write_u16(it.weight);
            w.write_u8(it.origin_base_kind as u8);
            w.write_u64(it.origin_base_id.0);
            w.write_u16(it.origin_rule_id);
        }
        Ok(w.into_bytes())
    }

    /// Decode from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let version = r.read_u32()?;
        if version != EXPANDED_QFV_V1_VERSION {
            return Err(DecodeError::new("bad expanded qfv version"));
        }
        let tie_control_id = Id64(r.read_u64()?);
        let req_n = r.read_u32()? as usize;
        let opt_n = r.read_u32()? as usize;
        let total = req_n.saturating_add(opt_n);
        if total > EXPANDED_QFV_V1_MAX_ITEMS {
            return Err(DecodeError::new("too many expanded qfv items"));
        }

        let mut required: Vec<ExpandedQfvItemV1> = Vec::with_capacity(req_n);
        let mut optional: Vec<ExpandedQfvItemV1> = Vec::with_capacity(opt_n);

        for _ in 0..req_n {
            let kind = kind_from_u8(r.read_u8()?)?;
            let id = Id64(r.read_u64()?);
            let weight = r.read_u16()?;
            let origin_base_kind = kind_from_u8(r.read_u8()?)?;
            let origin_base_id = Id64(r.read_u64()?);
            let origin_rule_id = r.read_u16()?;
            required.push(ExpandedQfvItemV1::new(
                kind,
                id,
                weight,
                origin_base_kind,
                origin_base_id,
                origin_rule_id,
            ));
        }
        for _ in 0..opt_n {
            let kind = kind_from_u8(r.read_u8()?)?;
            let id = Id64(r.read_u64()?);
            let weight = r.read_u16()?;
            let origin_base_kind = kind_from_u8(r.read_u8()?)?;
            let origin_base_id = Id64(r.read_u64()?);
            let origin_rule_id = r.read_u16()?;
            optional.push(ExpandedQfvItemV1::new(
                kind,
                id,
                weight,
                origin_base_kind,
                origin_base_id,
                origin_rule_id,
            ));
        }
        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        // Enforce canonical order and uniqueness across pools.
        for i in 1..required.len() {
            if cmp_canon(&required[i - 1], &required[i]) != core::cmp::Ordering::Less {
                return Err(DecodeError::new("required not canonical"));
            }
        }
        for i in 1..optional.len() {
            if cmp_canon(&optional[i - 1], &optional[i]) != core::cmp::Ordering::Less {
                return Err(DecodeError::new("optional not canonical"));
            }
        }
        let mut seen: Vec<(u8, u64)> = Vec::with_capacity(total);
        for it in required.iter().chain(optional.iter()) {
            let key = key_u72(it.kind, it.id);
            if !seen_insert_key(&mut seen, key) {
                return Err(DecodeError::new("duplicate item"));
            }
        }

        Ok(ExpandedQfvV1 {
            version,
            tie_control_id,
            required,
            optional,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(kind: ExpansionKindV1, id: u64, w: u16) -> ExpandedQfvItemV1 {
        ExpandedQfvItemV1::new(kind, Id64(id), w, kind, Id64(id), 1)
    }

    #[test]
    fn expanded_qfv_round_trip() {
        let q = ExpandedQfvV1 {
            version: EXPANDED_QFV_V1_VERSION,
            tie_control_id: Id64(7),
            required: vec![
                item(ExpansionKindV1::Lex, 10, 50000),
                item(ExpansionKindV1::Meta, 3, 40000),
            ],
            optional: vec![item(ExpansionKindV1::Lex, 11, 20000)],
        };
        assert!(q.is_canonical());
        q.validate().unwrap();
        let bytes = q.encode().unwrap();
        let dec = ExpandedQfvV1::decode(&bytes).unwrap();
        assert_eq!(dec, q);
    }

    #[test]
    fn expanded_qfv_rejects_duplicate_across_pools() {
        let q = ExpandedQfvV1 {
            version: EXPANDED_QFV_V1_VERSION,
            tie_control_id: Id64(1),
            required: vec![item(ExpansionKindV1::Lex, 10, 50000)],
            optional: vec![item(ExpansionKindV1::Lex, 10, 20000)],
        };
        assert!(q.validate().is_err());
        assert!(q.encode().is_err());
    }

    #[test]
    fn expanded_qfv_decode_rejects_noncanonical_required() {
        let mut w = ByteWriter::with_capacity(128);
        w.write_u32(EXPANDED_QFV_V1_VERSION);
        w.write_u64(1);
        w.write_u32(2);
        w.write_u32(0);
        // required out of order: lower weight first
        // item A weight 100
        w.write_u8(ExpansionKindV1::Lex as u8);
        w.write_u64(1);
        w.write_u16(100);
        w.write_u8(ExpansionKindV1::Lex as u8);
        w.write_u64(1);
        w.write_u16(1);
        // item B weight 200
        w.write_u8(ExpansionKindV1::Lex as u8);
        w.write_u64(2);
        w.write_u16(200);
        w.write_u8(ExpansionKindV1::Lex as u8);
        w.write_u64(2);
        w.write_u16(1);
        let bytes = w.into_bytes();
        assert!(ExpandedQfvV1::decode(&bytes).is_err());
    }
}
