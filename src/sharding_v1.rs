// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Sharding helpers.
//!
//! This module defines the sharding mapping and small helpers.
//!
//! Design goals:
//! - Deterministic mapping from DocId to shard_id.
//! - Small and auditable.
//! - No filesystem dependencies in the mapping itself.

use crate::frame::DocId;
use crate::hash::blake3_hash;

/// Sharding mapping id for DocId -> shard_id.
pub const SHARD_MAPPING_DOC_ID_HASH32_V1: &str = "doc_id_hash32_v1";

/// Sharding configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShardCfgV1 {
    /// Total shard count.
    pub shard_count: u16,
    /// This shard id.
    pub shard_id: u16,
}

impl ShardCfgV1 {
    /// Validate basic invariants.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.shard_count == 0 {
            return Err("shard_count must be > 0");
        }
        if self.shard_id >= self.shard_count {
            return Err("shard_id out of range");
        }
        Ok(())
    }
}

/// Map a DocId to a shard_id using the v1 mapping.
///
/// Mapping:
/// - input bytes: little-endian u64 doc id
/// - hash: blake3_hash(bytes)
/// - shard: u64_le(hash[0..8]) % shard_count
pub fn shard_id_for_doc_id_hash32_v1(doc_id: DocId, shard_count: u16) -> u16 {
    // shard_count is validated by callers; keep this function total.
    if shard_count == 0 {
        return 0;
    }
    let b = (doc_id.0).0.to_le_bytes();
    let h = blake3_hash(&b);
    let mut x = [0u8; 8];
    x.copy_from_slice(&h[0..8]);
    let v = u64::from_le_bytes(x);
    (v % (shard_count as u64)) as u16
}
