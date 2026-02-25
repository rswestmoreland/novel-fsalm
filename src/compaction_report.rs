// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! CompactionReportV1 schema and canonical codec.
//!
//! A CompactionReportV1 records a deterministic index-only compaction run.
//! It is intended to provide traceability for offline maintenance while
//! preserving replayability.
//!
//! This module is (schema + codec only).

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::hash::{blake3_hash, Hash32};

const COMPACTION_REPORT_V1_VERSION: u16 = 1;

/// Compaction configuration recorded in a report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionCfgV1 {
    /// Target encoded bytes per output IndexSegment.
    pub target_bytes_per_out_segment: u64,
    /// Maximum number of output segments.
    pub max_out_segments: u32,
    /// If true, the planner used the even-pack fallback.
    pub used_even_pack_fallback: bool,
    /// If true, no output snapshot or output segments were written.
    pub dry_run: bool,
}

impl CompactionCfgV1 {
    /// Compute a stable id for this configuration.
    pub fn cfg_id(&self) -> Hash32 {
        // Keep this encoding minimal and stable.
        let mut w = ByteWriter::with_capacity(8 + 4 + 1 + 1);
        w.write_u64(self.target_bytes_per_out_segment);
        w.write_u32(self.max_out_segments);
        w.write_u8(if self.used_even_pack_fallback { 1 } else { 0 });
        w.write_u8(if self.dry_run { 1 } else { 0 });
        let bytes = w.into_bytes();
        blake3_hash(&bytes)
    }
}

/// A planned output group (span into the sorted input segment list).
///
/// Groups are contiguous ranges into the `input_index_segments` list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionGroupV1 {
    /// Start index into `input_index_segments`.
    pub start_ix: u32,
    /// Length of this group.
    pub len: u32,
    /// Estimated total input bytes for this group.
    pub est_bytes_in: u64,
    /// Output segment hash, if produced.
    pub out_segment_id: Option<Hash32>,
    /// Encoded output bytes, if produced.
    pub out_bytes: u64,
}

/// Canonical compaction report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionReportV1 {
    /// Input snapshot id.
    pub input_snapshot_id: Hash32,
    /// Output snapshot id, if produced.
    pub output_snapshot_id: Option<Hash32>,
    /// Stable id of the compaction configuration.
    pub cfg_id: Hash32,
    /// Compaction configuration.
    pub cfg: CompactionCfgV1,
    /// Input IndexSegment hashes in canonical order (sorted ascending, unique).
    pub input_index_segments: Vec<Hash32>,
    /// Planned output groups.
    pub groups: Vec<CompactionGroupV1>,
    /// Output IndexSegment hashes in canonical order (sorted ascending, unique).
    pub output_index_segments: Vec<Hash32>,
    /// Total bytes of input segments used during planning.
    pub bytes_input_total: u64,
    /// Total bytes of output segments written.
    pub bytes_output_total: u64,
}

impl CompactionReportV1 {
    /// Encode the report as canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        self.validate_canonical()
            .map_err(|_| EncodeError::new("report not canonical"))?;

        let mut cap = 2 + 32 + 1 + 32 + 32 + (8 + 4 + 1 + 1) + 4;
        cap += 4 + self.input_index_segments.len() * 32;
        cap += 4 + self.groups.len() * (4 + 4 + 8 + 1 + 32 + 8);
        cap += 4 + self.output_index_segments.len() * 32;
        cap += 8 + 8;

        let mut w = ByteWriter::with_capacity(cap);
        w.write_u16(COMPACTION_REPORT_V1_VERSION);
        w.write_raw(&self.input_snapshot_id);
        match &self.output_snapshot_id {
            None => {
                w.write_u8(0);
            }
            Some(h) => {
                w.write_u8(1);
                w.write_raw(h);
            }
        }

        w.write_raw(&self.cfg_id);
        w.write_u64(self.cfg.target_bytes_per_out_segment);
        w.write_u32(self.cfg.max_out_segments);
        w.write_u8(if self.cfg.used_even_pack_fallback { 1 } else { 0 });
        w.write_u8(if self.cfg.dry_run { 1 } else { 0 });

        if self.input_index_segments.len() > (u32::MAX as usize) {
            return Err(EncodeError::new("too many input segments"));
        }
        w.write_u32(self.input_index_segments.len() as u32);
        for h in self.input_index_segments.iter() {
            w.write_raw(h);
        }

        if self.groups.len() > (u32::MAX as usize) {
            return Err(EncodeError::new("too many groups"));
        }
        w.write_u32(self.groups.len() as u32);
        for g in self.groups.iter() {
            w.write_u32(g.start_ix);
            w.write_u32(g.len);
            w.write_u64(g.est_bytes_in);
            match &g.out_segment_id {
                None => {
                    w.write_u8(0);
                }
                Some(h) => {
                    w.write_u8(1);
                    w.write_raw(h);
                }
            }
            w.write_u64(g.out_bytes);
        }

        if self.output_index_segments.len() > (u32::MAX as usize) {
            return Err(EncodeError::new("too many output segments"));
        }
        w.write_u32(self.output_index_segments.len() as u32);
        for h in self.output_index_segments.iter() {
            w.write_raw(h);
        }

        w.write_u64(self.bytes_input_total);
        w.write_u64(self.bytes_output_total);

        Ok(w.into_bytes())
    }

    /// Decode canonical bytes into a report.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let ver = r.read_u16()?;
        if ver != COMPACTION_REPORT_V1_VERSION {
            return Err(DecodeError::new("unsupported compaction report version"));
        }

        let input_snapshot_id = {
            let b = r.read_fixed(32)?;
            let mut h = [0u8; 32];
            h.copy_from_slice(b);
            h
        };

        let output_snapshot_id = match r.read_u8()? {
            0 => None,
            1 => {
                let b = r.read_fixed(32)?;
                let mut h = [0u8; 32];
                h.copy_from_slice(b);
                Some(h)
            }
            _ => return Err(DecodeError::new("invalid output snapshot tag")),
        };

        let cfg_id = {
            let b = r.read_fixed(32)?;
            let mut h = [0u8; 32];
            h.copy_from_slice(b);
            h
        };

        let target_bytes_per_out_segment = r.read_u64()?;
        let max_out_segments = r.read_u32()?;
        let used_even_pack_fallback = match r.read_u8()? {
            0 => false,
            1 => true,
            _ => return Err(DecodeError::new("invalid used_even_pack flag")),
        };
        let dry_run = match r.read_u8()? {
            0 => false,
            1 => true,
            _ => return Err(DecodeError::new("invalid dry_run flag")),
        };

        let cfg = CompactionCfgV1 {
            target_bytes_per_out_segment,
            max_out_segments,
            used_even_pack_fallback,
            dry_run,
        };

        let in_len = r.read_u32()? as usize;
        let mut input_index_segments = Vec::with_capacity(in_len);
        for _ in 0..in_len {
            let b = r.read_fixed(32)?;
            let mut h = [0u8; 32];
            h.copy_from_slice(b);
            input_index_segments.push(h);
        }

        let group_len = r.read_u32()? as usize;
        let mut groups = Vec::with_capacity(group_len);
        for _ in 0..group_len {
            let start_ix = r.read_u32()?;
            let len = r.read_u32()?;
            let est_bytes_in = r.read_u64()?;
            let out_segment_id = match r.read_u8()? {
                0 => None,
                1 => {
                    let b = r.read_fixed(32)?;
                    let mut h = [0u8; 32];
                    h.copy_from_slice(b);
                    Some(h)
                }
                _ => return Err(DecodeError::new("invalid out_segment tag")),
            };
            let out_bytes = r.read_u64()?;
            groups.push(CompactionGroupV1 {
                start_ix,
                len,
                est_bytes_in,
                out_segment_id,
                out_bytes,
            });
        }

        let out_len = r.read_u32()? as usize;
        let mut output_index_segments = Vec::with_capacity(out_len);
        for _ in 0..out_len {
            let b = r.read_fixed(32)?;
            let mut h = [0u8; 32];
            h.copy_from_slice(b);
            output_index_segments.push(h);
        }

        let bytes_input_total = r.read_u64()?;
        let bytes_output_total = r.read_u64()?;

        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        let rep = Self {
            input_snapshot_id,
            output_snapshot_id,
            cfg_id,
            cfg,
            input_index_segments,
            groups,
            output_index_segments,
            bytes_input_total,
            bytes_output_total,
        };

        rep.validate_canonical()
            .map_err(|_| DecodeError::new("report not canonical"))?;

        // Ensure cfg_id matches cfg.
        if rep.cfg_id != rep.cfg.cfg_id() {
            return Err(DecodeError::new("cfg_id mismatch"));
        }

        Ok(rep)
    }

    /// Validate canonical ordering and internal consistency.
    pub fn validate_canonical(&self) -> Result<(), &'static str> {
        if self.cfg_id != self.cfg.cfg_id() {
            return Err("cfg_id mismatch");
        }

        // Input segment list must be sorted ascending and unique.
        if !is_sorted_unique(&self.input_index_segments) {
            return Err("input_index_segments not sorted unique");
        }

        // Output segment list must be sorted ascending and unique.
        if !is_sorted_unique(&self.output_index_segments) {
            return Err("output_index_segments not sorted unique");
        }

        // If dry-run, there must not be an output snapshot id.
        if self.cfg.dry_run {
            if self.output_snapshot_id.is_some() {
                return Err("dry_run must not set output_snapshot_id");
            }
            if !self.output_index_segments.is_empty() {
                return Err("dry_run must not set output_index_segments");
            }
        }

        // Groups must be ordered and within bounds.
        let n_in = self.input_index_segments.len() as u32;
        let mut last_end: u32 = 0;
        let mut out_ids_present: u32 = 0;
        let mut bytes_in_sum: u64 = 0;
        let mut bytes_out_sum: u64 = 0;
        for (i, g) in self.groups.iter().enumerate() {
            if g.len == 0 {
                return Err("group len must be > 0");
            }
            if g.start_ix >= n_in {
                return Err("group start out of range");
            }
            let end = g
                .start_ix
                .checked_add(g.len)
                .ok_or("group end overflow")?;
            if end > n_in {
                return Err("group end out of range");
            }

            if i == 0 {
                if g.start_ix != 0 {
                    return Err("first group must start at 0");
                }
            } else {
                if g.start_ix != last_end {
                    return Err("groups must be contiguous");
                }
            }
            last_end = end;

            if g.out_segment_id.is_some() {
                out_ids_present += 1;
            }

            // If output list is present, out_bytes should be consistent.
            if g.out_segment_id.is_none() {
                if g.out_bytes != 0 {
                    return Err("missing out_segment_id must have out_bytes=0");
                }
            }

            bytes_in_sum = bytes_in_sum
                .checked_add(g.est_bytes_in)
                .ok_or("bytes_input_total overflow")?;
            bytes_out_sum = bytes_out_sum
                .checked_add(g.out_bytes)
                .ok_or("bytes_output_total overflow")?;
        }

        if n_in == 0 {
            if !self.groups.is_empty() {
                return Err("no input segments requires no groups");
            }
        } else {
            if self.groups.is_empty() {
                return Err("input segments requires at least one group");
            }
            if last_end != n_in {
                return Err("groups must cover all input segments");
            }
        }

        if self.bytes_input_total != bytes_in_sum {
            return Err("bytes_input_total mismatch");
        }
        if self.bytes_output_total != bytes_out_sum {
            return Err("bytes_output_total mismatch");
        }

        // If output segments exist, require an output snapshot id.
        if !self.output_index_segments.is_empty() {
            if self.output_snapshot_id.is_none() {
                return Err("output_index_segments requires output_snapshot_id");
            }
        }

        // If output snapshot exists, require group out_segment_id for each group.
        if self.output_snapshot_id.is_some() {
            if out_ids_present != (self.groups.len() as u32) {
                return Err("output snapshot requires out_segment_id for each group");
            }
            // Ensure each group out id is present in output_index_segments.
            for g in self.groups.iter() {
                let h = match &g.out_segment_id {
                    Some(h) => h,
                    None => return Err("missing out_segment_id"),
                };
                if !contains_hash(&self.output_index_segments, h) {
                    return Err("group out_segment_id missing from output_index_segments");
                }
            }
        } else {
            if out_ids_present != 0 {
                return Err("no output snapshot requires no out_segment_id");
            }
        }

        Ok(())
    }
}

fn is_sorted_unique(v: &[Hash32]) -> bool {
    if v.is_empty() {
        return true;
    }
    for i in 1..v.len() {
        if v[i - 1] >= v[i] {
            return false;
        }
    }
    true
}

fn contains_hash(v: &[Hash32], h: &Hash32) -> bool {
    // v is expected to be sorted, so binary search is stable and fast.
    v.binary_search(h).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(b: u8) -> Hash32 {
        let mut out = [0u8; 32];
        out[0] = b;
        out
    }

    #[test]
    fn cfg_id_is_stable() {
        let cfg = CompactionCfgV1 {
            target_bytes_per_out_segment: 64,
            max_out_segments: 8,
            used_even_pack_fallback: false,
            dry_run: true,
        };
        let id1 = cfg.cfg_id();
        let id2 = cfg.cfg_id();
        assert_eq!(id1, id2);
    }

    #[test]
    fn report_round_trip_dry_run() {
        let cfg = CompactionCfgV1 {
            target_bytes_per_out_segment: 64,
            max_out_segments: 8,
            used_even_pack_fallback: false,
            dry_run: true,
        };
        let mut input = vec![h(1), h(2), h(3)];
        input.sort();

        let rep = CompactionReportV1 {
            input_snapshot_id: h(9),
            output_snapshot_id: None,
            cfg_id: cfg.cfg_id(),
            cfg,
            input_index_segments: input,
            groups: vec![CompactionGroupV1 {
                start_ix: 0,
                len: 3,
                est_bytes_in: 123,
                out_segment_id: None,
                out_bytes: 0,
            }],
            output_index_segments: Vec::new(),
            bytes_input_total: 123,
            bytes_output_total: 0,
        };

        let bytes = rep.encode().unwrap();
        let dec = CompactionReportV1::decode(&bytes).unwrap();
        assert_eq!(rep, dec);
    }

    #[test]
    fn report_rejects_unsorted_inputs() {
        let cfg = CompactionCfgV1 {
            target_bytes_per_out_segment: 64,
            max_out_segments: 8,
            used_even_pack_fallback: false,
            dry_run: true,
        };

        let rep = CompactionReportV1 {
            input_snapshot_id: h(9),
            output_snapshot_id: None,
            cfg_id: cfg.cfg_id(),
            cfg,
            input_index_segments: vec![h(2), h(1)],
            groups: vec![CompactionGroupV1 {
                start_ix: 0,
                len: 2,
                est_bytes_in: 10,
                out_segment_id: None,
                out_bytes: 0,
            }],
            output_index_segments: Vec::new(),
            bytes_input_total: 10,
            bytes_output_total: 0,
        };

        assert!(rep.encode().is_err());
    }

    #[test]
    fn report_rejects_invalid_group_span() {
        let cfg = CompactionCfgV1 {
            target_bytes_per_out_segment: 64,
            max_out_segments: 8,
            used_even_pack_fallback: false,
            dry_run: true,
        };

        let rep = CompactionReportV1 {
            input_snapshot_id: h(9),
            output_snapshot_id: None,
            cfg_id: cfg.cfg_id(),
            cfg,
            input_index_segments: vec![h(1), h(2)],
            groups: vec![CompactionGroupV1 {
                start_ix: 1,
                len: 2,
                est_bytes_in: 10,
                out_segment_id: None,
                out_bytes: 0,
            }],
            output_index_segments: Vec::new(),
            bytes_input_total: 10,
            bytes_output_total: 0,
        };

        assert!(rep.encode().is_err());
    }

    #[test]
    fn report_round_trip_with_outputs() {
        let cfg = CompactionCfgV1 {
            target_bytes_per_out_segment: 64,
            max_out_segments: 8,
            used_even_pack_fallback: false,
            dry_run: false,
        };

        let input = vec![h(1), h(2), h(3)];
        let out_segs = vec![h(10), h(11)];

        let rep = CompactionReportV1 {
            input_snapshot_id: h(9),
            output_snapshot_id: Some(h(8)),
            cfg_id: cfg.cfg_id(),
            cfg,
            input_index_segments: input,
            groups: vec![
                CompactionGroupV1 {
                    start_ix: 0,
                    len: 2,
                    est_bytes_in: 100,
                    out_segment_id: Some(h(10)),
                    out_bytes: 80,
                },
                CompactionGroupV1 {
                    start_ix: 2,
                    len: 1,
                    est_bytes_in: 50,
                    out_segment_id: Some(h(11)),
                    out_bytes: 40,
                },
            ],
            output_index_segments: out_segs,
            bytes_input_total: 150,
            bytes_output_total: 120,
        };

        let bytes = rep.encode().unwrap();
        let dec = CompactionReportV1::decode(&bytes).unwrap();
        assert_eq!(rep, dec);
    }
}
