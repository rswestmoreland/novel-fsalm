// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Reduce manifest schema.
//!
//! ReduceManifestV1 is a canonically-encoded, content-addressed inventory of
//! merged outputs produced by reduce/merge.
//!
//! Design goals:
//! - Deterministic, stable encoding (no serde).
//! - Sorted, canonical lists (outputs sorted by tag).
//! - Small, auditable surface sufficient to replay and verify reduce results.

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::hash::Hash32;

/// ReduceManifestV1 schema version.
pub const REDUCE_MANIFEST_V1_VERSION: u32 = 1;

/// Max allowed length for mapping_id.
pub const REDUCE_MANIFEST_V1_MAX_MAPPING_ID_BYTES: usize = 64;

/// Max allowed outputs in a reduce manifest.
pub const REDUCE_MANIFEST_V1_MAX_OUTPUTS: usize = 256;

/// Max allowed length for output tag.
pub const REDUCE_MANIFEST_V1_MAX_OUTPUT_TAG_BYTES: usize = 64;

/// ReduceManifestV1 validation errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReduceManifestError {
    /// Unsupported or mismatched schema version.
    BadVersion,
    /// Shard count is invalid.
    BadShardCount,
    /// Mapping id is invalid.
    BadMappingId,
    /// Too many outputs.
    TooManyOutputs,
    /// Output tags are not sorted or contain duplicates.
    OutputsNotSorted,
    /// Output tag is invalid.
    BadOutputTag,
}

impl core::fmt::Display for ReduceManifestError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ReduceManifestError::BadVersion => f.write_str("bad reduce manifest version"),
            ReduceManifestError::BadShardCount => f.write_str("bad shard count"),
            ReduceManifestError::BadMappingId => f.write_str("bad mapping id"),
            ReduceManifestError::TooManyOutputs => f.write_str("too many outputs"),
            ReduceManifestError::OutputsNotSorted => f.write_str("outputs not sorted"),
            ReduceManifestError::BadOutputTag => f.write_str("bad output tag"),
        }
    }
}

/// One named reduce output hash.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReduceOutputV1 {
    /// Output tag (ASCII identifier).
    pub tag: String,
    /// Output content hash.
    pub hash: Hash32,
}

impl ReduceOutputV1 {
    /// Validate output tag.
    pub fn validate_tag(&self) -> Result<(), ReduceManifestError> {
        if self.tag.is_empty() {
            return Err(ReduceManifestError::BadOutputTag);
        }
        if self.tag.len() > REDUCE_MANIFEST_V1_MAX_OUTPUT_TAG_BYTES {
            return Err(ReduceManifestError::BadOutputTag);
        }
        if !self.tag.is_ascii() {
            return Err(ReduceManifestError::BadOutputTag);
        }
        Ok(())
    }
}

/// Top-level reduce manifest (v1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReduceManifestV1 {
    /// Schema version (must be REDUCE_MANIFEST_V1_VERSION).
    pub version: u32,
    /// Input shard manifest hash.
    pub shard_manifest: Hash32,
    /// Total shard count (N) from the input manifest.
    pub shard_count: u16,
    /// Short ASCII mapping id copied from the input manifest.
    pub mapping_id: String,
    /// Source id for merged index artifacts (u64 encoding of Id64).
    pub source_id_u64: u64,
    /// Total entries in merged IndexSnapshotV1.
    pub snapshot_entries: u32,
    /// Number of frame segment artifacts copied into the primary root.
    pub copied_frame_segs: u32,
    /// Number of index segment artifacts copied into the primary root.
    pub copied_index_segs: u32,
    /// Number of segment sig artifacts copied into the primary root.
    pub copied_segment_sigs: u32,
    /// Named outputs produced by this reduce step.
    ///
    /// The output list is sorted strictly ascending by tag.
    pub outputs: Vec<ReduceOutputV1>,
}

impl ReduceManifestV1 {
    /// Validate canonical invariants.
    pub fn validate_canonical(&self) -> Result<(), ReduceManifestError> {
        if self.version != REDUCE_MANIFEST_V1_VERSION {
            return Err(ReduceManifestError::BadVersion);
        }
        if self.shard_count == 0 {
            return Err(ReduceManifestError::BadShardCount);
        }
        if self.mapping_id.is_empty() {
            return Err(ReduceManifestError::BadMappingId);
        }
        if self.mapping_id.len() > REDUCE_MANIFEST_V1_MAX_MAPPING_ID_BYTES {
            return Err(ReduceManifestError::BadMappingId);
        }
        if !self.mapping_id.is_ascii() {
            return Err(ReduceManifestError::BadMappingId);
        }
        if self.outputs.len() > REDUCE_MANIFEST_V1_MAX_OUTPUTS {
            return Err(ReduceManifestError::TooManyOutputs);
        }
        let mut prev: Option<&str> = None;
        for o in self.outputs.iter() {
            o.validate_tag()?;
            if let Some(p) = prev {
                if o.tag.as_str() <= p {
                    return Err(ReduceManifestError::OutputsNotSorted);
                }
            }
            prev = Some(o.tag.as_str());
        }
        Ok(())
    }

    /// Encode into canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        self.validate_canonical().map_err(|e| {
            EncodeError::new(match e {
                ReduceManifestError::BadVersion => "bad reduce manifest version",
                ReduceManifestError::BadShardCount => "bad shard count",
                ReduceManifestError::BadMappingId => "bad mapping id",
                ReduceManifestError::TooManyOutputs => "too many outputs",
                ReduceManifestError::OutputsNotSorted => "outputs not sorted",
                ReduceManifestError::BadOutputTag => "bad output tag",
            })
        })?;

        let mut cap: usize = 4 + 32 + 2 + 4 + self.mapping_id.len() + 8 + 4 + 4 + 4 + 4 + 2;
        for o in self.outputs.iter() {
            cap += 4 + o.tag.len() + 32;
        }
        let mut w = ByteWriter::with_capacity(cap);
        w.write_u32(self.version);
        w.write_raw(&self.shard_manifest);
        w.write_u16(self.shard_count);
        w.write_str(&self.mapping_id)?;
        w.write_u64(self.source_id_u64);
        w.write_u32(self.snapshot_entries);
        w.write_u32(self.copied_frame_segs);
        w.write_u32(self.copied_index_segs);
        w.write_u32(self.copied_segment_sigs);

        if self.outputs.len() > (u16::MAX as usize) {
            return Err(EncodeError::new("too many outputs"));
        }
        w.write_u16(self.outputs.len() as u16);
        for o in self.outputs.iter() {
            w.write_str(&o.tag)?;
            w.write_raw(&o.hash);
        }
        Ok(w.into_bytes())
    }

    /// Decode from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<ReduceManifestV1, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let ver = r.read_u32()?;
        if ver != REDUCE_MANIFEST_V1_VERSION {
            return Err(DecodeError::new("bad reduce manifest version"));
        }
        let man_b = r.read_fixed(32)?;
        let mut shard_manifest: Hash32 = [0u8; 32];
        shard_manifest.copy_from_slice(man_b);

        let shard_count = r.read_u16()?;
        if shard_count == 0 {
            return Err(DecodeError::new("bad shard count"));
        }

        let mapping_id_view = r.read_str_view()?;
        if mapping_id_view.is_empty()
            || (mapping_id_view.len() > REDUCE_MANIFEST_V1_MAX_MAPPING_ID_BYTES)
            || !mapping_id_view.is_ascii()
        {
            return Err(DecodeError::new("bad mapping id"));
        }
        let mapping_id = mapping_id_view.to_string();

        let source_id_u64 = r.read_u64()?;
        let snapshot_entries = r.read_u32()?;
        let copied_frame_segs = r.read_u32()?;
        let copied_index_segs = r.read_u32()?;
        let copied_segment_sigs = r.read_u32()?;

        let n = r.read_u16()? as usize;
        if n > REDUCE_MANIFEST_V1_MAX_OUTPUTS {
            return Err(DecodeError::new("too many outputs"));
        }
        let mut outputs: Vec<ReduceOutputV1> = Vec::with_capacity(n);
        let mut prev: Option<String> = None;
        for _ in 0..n {
            let tag_view = r.read_str_view()?;
            if tag_view.is_empty()
                || (tag_view.len() > REDUCE_MANIFEST_V1_MAX_OUTPUT_TAG_BYTES)
                || !tag_view.is_ascii()
            {
                return Err(DecodeError::new("bad output tag"));
            }
            let tag = tag_view.to_string();
            if let Some(p) = &prev {
                if tag.as_str() <= p.as_str() {
                    return Err(DecodeError::new("outputs not sorted"));
                }
            }
            prev = Some(tag.clone());
            let h_b = r.read_fixed(32)?;
            let mut h: Hash32 = [0u8; 32];
            h.copy_from_slice(h_b);
            outputs.push(ReduceOutputV1 { tag, hash: h });
        }

        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        Ok(ReduceManifestV1 {
            version: ver,
            shard_manifest,
            shard_count,
            mapping_id,
            source_id_u64,
            snapshot_entries,
            copied_frame_segs,
            copied_index_segs,
            copied_segment_sigs,
            outputs,
        })
    }
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
    fn reduce_manifest_round_trip() {
        let m = ReduceManifestV1 {
            version: REDUCE_MANIFEST_V1_VERSION,
            shard_manifest: h(9),
            shard_count: 2,
            mapping_id: "doc_id_hash32_v1".to_string(),
            source_id_u64: 7,
            snapshot_entries: 3,
            copied_frame_segs: 3,
            copied_index_segs: 3,
            copied_segment_sigs: 3,
            outputs: vec![
                ReduceOutputV1 { tag: "index_sig_map_v1".to_string(), hash: h(2) },
                ReduceOutputV1 { tag: "index_snapshot_v1".to_string(), hash: h(1) },
            ],
        };
        let bytes = m.encode().unwrap();
        let got = ReduceManifestV1::decode(&bytes).unwrap();
        assert_eq!(m, got);
    }

    #[test]
    fn reduce_manifest_decode_rejects_unsorted_outputs() {
        // Write bytes with outputs out of order.
        let mut w = ByteWriter::with_capacity(256);
        w.write_u32(REDUCE_MANIFEST_V1_VERSION);
        w.write_raw(&h(9));
        w.write_u16(2);
        w.write_str("doc_id_hash32_v1").unwrap();
        w.write_u64(7);
        w.write_u32(3);
        w.write_u32(0);
        w.write_u32(0);
        w.write_u32(0);
        w.write_u16(2);
        // Intentionally unsorted: "b" then "a".
        w.write_str("b").unwrap();
        w.write_raw(&h(2));
        w.write_str("a").unwrap();
        w.write_raw(&h(1));
        let bytes = w.into_bytes();
        let err = ReduceManifestV1::decode(&bytes).unwrap_err();
        assert_eq!(err.to_string(), "outputs not sorted");
    }
}
