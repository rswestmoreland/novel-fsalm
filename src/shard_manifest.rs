// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Sharded ingest manifest schema.
//!
//! ShardManifestV1 is a compact, canonically-encoded inventory of per-shard
//! outputs produced by a sharded ingest driver.
//!
//! Design goals:
//! - Deterministic, stable encoding (no serde).
//! - Sorted, canonical lists (shards by shard_id; outputs by tag).
//! - Small, auditable surface sufficient for reduce/merge.

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::hash::{hex32, Hash32};

/// ShardManifestV1 schema version.
pub const SHARD_MANIFEST_V1_VERSION: u32 = 1;

/// Max allowed length for mapping_id.
pub const SHARD_MANIFEST_V1_MAX_MAPPING_ID_BYTES: usize = 64;

/// Max allowed shard entries in a manifest.
pub const SHARD_MANIFEST_V1_MAX_SHARDS: usize = 4096;

/// Max allowed outputs per shard.
pub const SHARD_MANIFEST_V1_MAX_OUTPUTS_PER_SHARD: usize = 256;

/// Max allowed length for shard_root_rel.
pub const SHARD_MANIFEST_V1_MAX_ROOT_REL_BYTES: usize = 128;

/// Max allowed length for output tag.
pub const SHARD_MANIFEST_V1_MAX_OUTPUT_TAG_BYTES: usize = 64;

/// ShardManifestV1 validation errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardManifestError {
    /// Unsupported or mismatched schema version.
    BadVersion,
    /// Shard count is invalid.
    BadShardCount,
    /// Mapping id is invalid.
    BadMappingId,
    /// Too many shard entries.
    TooManyShards,
    /// Shard entry list is not sorted or contains duplicates.
    ShardsNotSorted,
    /// A shard id is out of range.
    ShardIdOutOfRange,
    /// Shard root rel path does not match shard id.
    BadShardRootRel,
    /// Too many outputs in a shard.
    TooManyOutputs,
    /// Output tags are not sorted or contain duplicates.
    OutputsNotSorted,
    /// Output tag is invalid.
    BadOutputTag,
}

impl core::fmt::Display for ShardManifestError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ShardManifestError::BadVersion => f.write_str("bad shard manifest version"),
            ShardManifestError::BadShardCount => f.write_str("bad shard count"),
            ShardManifestError::BadMappingId => f.write_str("bad mapping id"),
            ShardManifestError::TooManyShards => f.write_str("too many shards"),
            ShardManifestError::ShardsNotSorted => f.write_str("shard entries not sorted"),
            ShardManifestError::ShardIdOutOfRange => f.write_str("shard id out of range"),
            ShardManifestError::BadShardRootRel => f.write_str("bad shard root rel"),
            ShardManifestError::TooManyOutputs => f.write_str("too many outputs"),
            ShardManifestError::OutputsNotSorted => f.write_str("outputs not sorted"),
            ShardManifestError::BadOutputTag => f.write_str("bad output tag"),
        }
    }
}

/// One named shard output hash.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardOutputV1 {
    /// Output tag (ASCII identifier).
    pub tag: String,
    /// Output content hash.
    pub hash: Hash32,
}

impl ShardOutputV1 {
    /// Validate output tag.
    pub fn validate_tag(&self) -> Result<(), ShardManifestError> {
        if self.tag.is_empty() {
            return Err(ShardManifestError::BadOutputTag);
        }
        if self.tag.len() > SHARD_MANIFEST_V1_MAX_OUTPUT_TAG_BYTES {
            return Err(ShardManifestError::BadOutputTag);
        }
        if !self.tag.is_ascii() {
            return Err(ShardManifestError::BadOutputTag);
        }
        Ok(())
    }
}

/// One shard inventory entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardEntryV1 {
    /// Shard id.
    pub shard_id: u16,
    /// Shard root, relative to the primary root (e.g., "shards/000f").
    pub shard_root_rel: String,
    /// Named outputs produced by this shard.
    pub outputs: Vec<ShardOutputV1>,
}

impl ShardEntryV1 {
    fn expected_root_rel(shard_id: u16) -> String {
        format!("shards/{:04x}", shard_id)
    }

    fn validate_root_rel(&self) -> Result<(), ShardManifestError> {
        if self.shard_root_rel.len() > SHARD_MANIFEST_V1_MAX_ROOT_REL_BYTES {
            return Err(ShardManifestError::BadShardRootRel);
        }
        if !self.shard_root_rel.is_ascii() {
            return Err(ShardManifestError::BadShardRootRel);
        }
        if self.shard_root_rel != Self::expected_root_rel(self.shard_id) {
            return Err(ShardManifestError::BadShardRootRel);
        }
        Ok(())
    }

    fn validate_outputs_sorted(&self) -> Result<(), ShardManifestError> {
        if self.outputs.len() > SHARD_MANIFEST_V1_MAX_OUTPUTS_PER_SHARD {
            return Err(ShardManifestError::TooManyOutputs);
        }
        let mut prev: Option<&str> = None;
        for o in self.outputs.iter() {
            o.validate_tag()?;
            if let Some(p) = prev {
                if o.tag.as_str() <= p {
                    return Err(ShardManifestError::OutputsNotSorted);
                }
            }
            prev = Some(o.tag.as_str());
        }
        Ok(())
    }
}

/// Top-level sharded ingest manifest (v1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardManifestV1 {
    /// Schema version (must be SHARD_MANIFEST_V1_VERSION).
    pub version: u32,
    /// Total shard count (N).
    pub shard_count: u16,
    /// Short ASCII mapping id (e.g., "doc_id_hash32_v1").
    pub mapping_id: String,
    /// Shard entries, sorted ascending by shard_id.
    pub shards: Vec<ShardEntryV1>,
}

impl ShardManifestV1 {
    /// Validate canonical invariants.
    pub fn validate_canonical(&self) -> Result<(), ShardManifestError> {
        if self.version != SHARD_MANIFEST_V1_VERSION {
            return Err(ShardManifestError::BadVersion);
        }
        if self.shard_count == 0 {
            return Err(ShardManifestError::BadShardCount);
        }
        if (self.shard_count as usize) > SHARD_MANIFEST_V1_MAX_SHARDS {
            return Err(ShardManifestError::TooManyShards);
        }
        if self.mapping_id.is_empty() {
            return Err(ShardManifestError::BadMappingId);
        }
        if self.mapping_id.len() > SHARD_MANIFEST_V1_MAX_MAPPING_ID_BYTES {
            return Err(ShardManifestError::BadMappingId);
        }
        if !self.mapping_id.is_ascii() {
            return Err(ShardManifestError::BadMappingId);
        }
        if self.shards.len() > SHARD_MANIFEST_V1_MAX_SHARDS {
            return Err(ShardManifestError::TooManyShards);
        }

        let mut prev_id: Option<u16> = None;
        for s in self.shards.iter() {
            if let Some(p) = prev_id {
                if s.shard_id <= p {
                    return Err(ShardManifestError::ShardsNotSorted);
                }
            }
            prev_id = Some(s.shard_id);

            if s.shard_id >= self.shard_count {
                return Err(ShardManifestError::ShardIdOutOfRange);
            }
            s.validate_root_rel()?;
            s.validate_outputs_sorted()?;
        }

        Ok(())
    }

    /// Encode into canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        self.validate_canonical().map_err(|e| {
            EncodeError::new(match e {
                ShardManifestError::BadVersion => "bad shard manifest version",
                ShardManifestError::BadShardCount => "bad shard count",
                ShardManifestError::BadMappingId => "bad mapping id",
                ShardManifestError::TooManyShards => "too many shards",
                ShardManifestError::ShardsNotSorted => "shards not sorted",
                ShardManifestError::ShardIdOutOfRange => "shard id out of range",
                ShardManifestError::BadShardRootRel => "bad shard root rel",
                ShardManifestError::TooManyOutputs => "too many outputs",
                ShardManifestError::OutputsNotSorted => "outputs not sorted",
                ShardManifestError::BadOutputTag => "bad output tag",
            })
        })?;

        // Conservative capacity guess.
        let mut cap = 4 + 2 + 4 + self.mapping_id.len() + 2;
        for s in self.shards.iter() {
            cap += 2 + 4 + s.shard_root_rel.len() + 2;
            cap += s.outputs.len() * (4 + SHARD_MANIFEST_V1_MAX_OUTPUT_TAG_BYTES + 32);
        }
        let mut w = ByteWriter::with_capacity(cap);
        w.write_u32(self.version);
        w.write_u16(self.shard_count);
        w.write_str(&self.mapping_id)?;
        if self.shards.len() > (u16::MAX as usize) {
            return Err(EncodeError::new("too many shards"));
        }
        w.write_u16(self.shards.len() as u16);
        for s in self.shards.iter() {
            w.write_u16(s.shard_id);
            w.write_str(&s.shard_root_rel)?;
            if s.outputs.len() > (u16::MAX as usize) {
                return Err(EncodeError::new("too many outputs"));
            }
            w.write_u16(s.outputs.len() as u16);
            for o in s.outputs.iter() {
                w.write_str(&o.tag)?;
                w.write_raw(&o.hash);
            }
        }
        Ok(w.into_bytes())
    }

    /// Decode from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let version = r.read_u32()?;
        if version != SHARD_MANIFEST_V1_VERSION {
            return Err(DecodeError::new("bad shard manifest version"));
        }
        let shard_count = r.read_u16()?;
        let mapping_id = r.read_str_view()?;
        if mapping_id.is_empty() {
            return Err(DecodeError::new("bad mapping id"));
        }
        if mapping_id.len() > SHARD_MANIFEST_V1_MAX_MAPPING_ID_BYTES {
            return Err(DecodeError::new("bad mapping id"));
        }
        let n = r.read_u16()? as usize;
        if n > SHARD_MANIFEST_V1_MAX_SHARDS {
            return Err(DecodeError::new("too many shards"));
        }
        let mut shards: Vec<ShardEntryV1> = Vec::with_capacity(n);
        for _ in 0..n {
            let shard_id = r.read_u16()?;
            let shard_root_rel = r.read_str_view()?;
            if shard_root_rel.len() > SHARD_MANIFEST_V1_MAX_ROOT_REL_BYTES {
                return Err(DecodeError::new("bad shard root rel"));
            }
            let m = r.read_u16()? as usize;
            if m > SHARD_MANIFEST_V1_MAX_OUTPUTS_PER_SHARD {
                return Err(DecodeError::new("too many outputs"));
            }
            let mut outputs: Vec<ShardOutputV1> = Vec::with_capacity(m);
            for _ in 0..m {
                let tag = r.read_str_view()?;
                if tag.is_empty() {
                    return Err(DecodeError::new("bad output tag"));
                }
                if tag.len() > SHARD_MANIFEST_V1_MAX_OUTPUT_TAG_BYTES {
                    return Err(DecodeError::new("bad output tag"));
                }
                if !tag.is_ascii() {
                    return Err(DecodeError::new("bad output tag"));
                }
                let hb = r.read_fixed(32)?;
                let mut hash = [0u8; 32];
                hash.copy_from_slice(hb);
                outputs.push(ShardOutputV1 {
                    tag: tag.to_string(),
                    hash,
                });
            }
            shards.push(ShardEntryV1 {
                shard_id,
                shard_root_rel: shard_root_rel.to_string(),
                outputs,
            });
        }

        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        let man = ShardManifestV1 {
            version,
            shard_count,
            mapping_id: mapping_id.to_string(),
            shards,
        };
        man.validate_canonical()
            .map_err(|_| DecodeError::new("non-canonical shard manifest"))?;
        Ok(man)
    }
}

impl core::fmt::Display for ShardManifestV1 {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "shard_manifest_v1 shards={} mapping_id={} entries={} ",
            self.shard_count,
            self.mapping_id,
            self.shards.len()
        )?;
        for s in self.shards.iter() {
            write!(f, "shard={:04x} outputs={} ", s.shard_id, s.outputs.len())?;
        }
        Ok(())
    }
}

/// Helper to format a shard output list in stable text.
pub fn format_shard_outputs_v1(outputs: &[ShardOutputV1]) -> String {
    let mut out = String::new();
    for o in outputs.iter() {
        out.push_str(o.tag.as_str());
        out.push('=');
        out.push_str(hex32(&o.hash).as_str());
        out.push(' ');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(b: u8) -> Hash32 {
        let mut x = [0u8; 32];
        x[0] = b;
        x
    }

    #[test]
    fn shard_manifest_encode_decode_round_trip() {
        let man = ShardManifestV1 {
            version: SHARD_MANIFEST_V1_VERSION,
            shard_count: 4,
            mapping_id: "doc_id_hash32_v1".to_string(),
            shards: vec![
                ShardEntryV1 {
                    shard_id: 0,
                    shard_root_rel: "shards/0000".to_string(),
                    outputs: vec![
                        ShardOutputV1 {
                            tag: "index_snapshot".to_string(),
                            hash: h(1),
                        },
                        ShardOutputV1 {
                            tag: "sig_map".to_string(),
                            hash: h(2),
                        },
                    ],
                },
                ShardEntryV1 {
                    shard_id: 2,
                    shard_root_rel: "shards/0002".to_string(),
                    outputs: vec![ShardOutputV1 {
                        tag: "index_snapshot".to_string(),
                        hash: h(3),
                    }],
                },
            ],
        };

        let bytes = man.encode().unwrap();
        let got = ShardManifestV1::decode(&bytes).unwrap();
        assert_eq!(man, got);

        let bytes2 = got.encode().unwrap();
        assert_eq!(bytes, bytes2);
    }

    #[test]
    fn shard_manifest_rejects_unsorted_shards() {
        let man = ShardManifestV1 {
            version: SHARD_MANIFEST_V1_VERSION,
            shard_count: 4,
            mapping_id: "doc_id_hash32_v1".to_string(),
            shards: vec![
                ShardEntryV1 {
                    shard_id: 2,
                    shard_root_rel: "shards/0002".to_string(),
                    outputs: vec![],
                },
                ShardEntryV1 {
                    shard_id: 0,
                    shard_root_rel: "shards/0000".to_string(),
                    outputs: vec![],
                },
            ],
        };
        let e = man.validate_canonical().unwrap_err();
        assert_eq!(e, ShardManifestError::ShardsNotSorted);
    }

    #[test]
    fn shard_manifest_rejects_unsorted_outputs() {
        let man = ShardManifestV1 {
            version: SHARD_MANIFEST_V1_VERSION,
            shard_count: 2,
            mapping_id: "doc_id_hash32_v1".to_string(),
            shards: vec![ShardEntryV1 {
                shard_id: 0,
                shard_root_rel: "shards/0000".to_string(),
                outputs: vec![
                    ShardOutputV1 {
                        tag: "z".to_string(),
                        hash: h(1),
                    },
                    ShardOutputV1 {
                        tag: "a".to_string(),
                        hash: h(2),
                    },
                ],
            }],
        };
        let e = man.validate_canonical().unwrap_err();
        assert_eq!(e, ShardManifestError::OutputsNotSorted);
    }
}
