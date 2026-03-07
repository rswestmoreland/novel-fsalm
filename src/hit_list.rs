// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! HitListV1 schema and canonical codec.
//!
//! HitListV1 is a canonical artifact representing the ranked hit list
//! produced by query-time index lookup.
//!
//! Design goals (v1):
//! - Bitwise determinism (integer-only, stable ordering).
//! - Canonical encoding (stable bytes).
//! - Replay-friendly (content-addressed artifact).
//! - Defensive decode (bounded allocations).

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::hash::Hash32;
use core::cmp::Ordering;
use rustc_hash::FxHashSet;

const HIT_LIST_V1_VERSION: u16 = 1;

/// Hard cap on the number of hits permitted in a decoded HitListV1.
///
/// Query-time lookups are expected to return small `k` values.
/// This limit exists primarily to prevent runaway allocation when attempting
/// to decode arbitrary bytes.
pub const HIT_LIST_V1_MAX_HITS: u32 = 200_000;

/// A canonical ranked hit list produced by retrieval.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HitListV1 {
    /// Stable id for the query.
    pub query_id: Hash32,
    /// Stable id for the index snapshot used during retrieval.
    pub snapshot_id: Hash32,
    /// Optional control id used for seeded tie-breaking.
    ///
    /// When present, canonical ordering uses a deterministic seeded tiebreak
    /// that matches `index_query` ranking semantics.
    pub tie_control_id: Option<Hash32>,
    /// Hits in canonical order.
    pub hits: Vec<HitV1>,
}

/// A single ranked hit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HitV1 {
    /// Frame segment hash that owns the row.
    pub frame_seg: Hash32,
    /// Row index within that frame segment.
    pub row_ix: u32,
    /// Deterministic integer score (larger is better).
    pub score: u64,
}

impl HitListV1 {
    /// Encode as canonical bytes.
    ///
    /// This method canonicalizes a cloned copy of the hit list first.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        let mut tmp = self.clone();
        tmp.canonicalize_in_place()?;
        tmp.encode_assuming_canonical()
    }

    /// Encode as canonical bytes, assuming the list is already canonical.
    pub fn encode_assuming_canonical(&self) -> Result<Vec<u8>, EncodeError> {
        self.validate_canonical().map_err(|_| EncodeError::new("hit list not canonical"))?;

        let mut cap: usize = 2 + 32 + 32 + 1 + 4;
        if self.tie_control_id.is_some() {
            cap += 32;
        }
        cap += self.hits.len() * (32 + 4 + 8);

        let mut w = ByteWriter::with_capacity(cap);
        w.write_u16(HIT_LIST_V1_VERSION);
        w.write_raw(&self.query_id);
        w.write_raw(&self.snapshot_id);
        match &self.tie_control_id {
            None => w.write_u8(0),
            Some(h) => {
                w.write_u8(1);
                w.write_raw(h);
            }
        }

        if self.hits.len() > (u32::MAX as usize) {
            return Err(EncodeError::new("too many hits"));
        }
        w.write_u32(self.hits.len() as u32);
        for h in self.hits.iter() {
            w.write_raw(&h.frame_seg);
            w.write_u32(h.row_ix);
            w.write_u64(h.score);
        }
        Ok(w.into_bytes())
    }

    /// Decode canonical bytes into a HitListV1.
    pub fn decode(bytes: &[u8]) -> Result<HitListV1, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let ver = r.read_u16()?;
        if ver != HIT_LIST_V1_VERSION {
            return Err(DecodeError::new("bad hit list version"));
        }

        let mut query_id: Hash32 = [0u8; 32];
        let mut snapshot_id: Hash32 = [0u8; 32];
        query_id.copy_from_slice(r.read_fixed(32)?);
        snapshot_id.copy_from_slice(r.read_fixed(32)?);

        let tie_flag = r.read_u8()?;
        let tie_control_id: Option<Hash32> = match tie_flag {
            0 => None,
            1 => {
                let mut h: Hash32 = [0u8; 32];
                h.copy_from_slice(r.read_fixed(32)?);
                Some(h)
            }
            _ => return Err(DecodeError::new("bad tie flag")),
        };

        let n = r.read_u32()?;
        if n > HIT_LIST_V1_MAX_HITS {
            return Err(DecodeError::new("too many hits"));
        }
        let mut hits: Vec<HitV1> = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let mut seg: Hash32 = [0u8; 32];
            seg.copy_from_slice(r.read_fixed(32)?);
            let row_ix = r.read_u32()?;
            let score = r.read_u64()?;
            hits.push(HitV1 { frame_seg: seg, row_ix, score });
        }

        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        let out = HitListV1 { query_id, snapshot_id, tie_control_id, hits };
        out.validate_canonical().map_err(|_| DecodeError::new("hit list not canonical"))?;
        Ok(out)
    }

    /// Canonicalize this hit list in place.
    pub fn canonicalize_in_place(&mut self) -> Result<(), EncodeError> {
        if self.hits.len() > (HIT_LIST_V1_MAX_HITS as usize) {
            return Err(EncodeError::new("too many hits"));
        }
        let seed = self.tie_seed();
        self.hits.sort_by(|a, b| cmp_hit(a, b, seed));

        // Enforce uniqueness of (frame_seg, row_ix).
        let mut seen: FxHashSet<(Hash32, u32)> = FxHashSet::default();
        for h in self.hits.iter() {
            if !seen.insert((h.frame_seg, h.row_ix)) {
                return Err(EncodeError::new("duplicate hit"));
            }
        }
        Ok(())
    }

    fn tie_seed(&self) -> Option<u64> {
        self.tie_control_id.as_ref().map(seed64_from_control_id)
    }

    fn validate_canonical(&self) -> Result<(), ()> {
        if self.hits.len() > (HIT_LIST_V1_MAX_HITS as usize) {
            return Err(());
        }
        let seed = self.tie_seed();
        let mut seen: FxHashSet<(Hash32, u32)> = FxHashSet::default();

        for i in 0..self.hits.len() {
            let h = &self.hits[i];
            if !seen.insert((h.frame_seg, h.row_ix)) {
                return Err(());
            }
            if i > 0 {
                let prev = &self.hits[i - 1];
                if cmp_hit(prev, h, seed) == Ordering::Greater {
                    return Err(());
                }
            }
        }
        Ok(())
    }
}

fn seed64_from_control_id(control_id: &Hash32) -> u64 {
    u64::from_le_bytes([
        control_id[0],
        control_id[1],
        control_id[2],
        control_id[3],
        control_id[4],
        control_id[5],
        control_id[6],
        control_id[7],
    ])
}

fn mix64(mut z: u64) -> u64 {
    // splitmix64 finalizer (deterministic, integer-only)
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

fn tiebreak_key(seed: u64, frame_seg: &Hash32, row_ix: u32) -> u64 {
    let seg0 = u64::from_le_bytes([
        frame_seg[0],
        frame_seg[1],
        frame_seg[2],
        frame_seg[3],
        frame_seg[4],
        frame_seg[5],
        frame_seg[6],
        frame_seg[7],
    ]);
    let x = seed ^ seg0 ^ (row_ix as u64).wrapping_mul(0x9E3779B97F4A7C15);
    mix64(x.wrapping_add(0x9E3779B97F4A7C15))
}

fn cmp_hit(a: &HitV1, b: &HitV1, tie_seed: Option<u64>) -> Ordering {
    match b.score.cmp(&a.score) {
        Ordering::Equal => {
            match tie_seed {
                None => match a.frame_seg.cmp(&b.frame_seg) {
                    Ordering::Equal => a.row_ix.cmp(&b.row_ix),
                    x => x,
                },
                Some(seed) => {
                    let ka = tiebreak_key(seed, &a.frame_seg, a.row_ix);
                    let kb = tiebreak_key(seed, &b.frame_seg, b.row_ix);
                    match ka.cmp(&kb) {
                        Ordering::Equal => match a.frame_seg.cmp(&b.frame_seg) {
                            Ordering::Equal => a.row_ix.cmp(&b.row_ix),
                            x => x,
                        },
                        x => x,
                    }
                }
            }
        }
        x => x,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::blake3_hash;

    #[test]
    fn hit_list_roundtrip_is_stable() {
        let q = blake3_hash(b"q");
        let s = blake3_hash(b"snap");
        let a = blake3_hash(b"a");
        let b = blake3_hash(b"b");

        let hl = HitListV1 {
            query_id: q,
            snapshot_id: s,
            tie_control_id: None,
            hits: vec![
                HitV1 { frame_seg: b, row_ix: 2, score: 7 },
                HitV1 { frame_seg: a, row_ix: 1, score: 7 },
            ],
        };

        let enc1 = hl.encode().expect("encode");
        let dec = HitListV1::decode(&enc1).expect("decode");
        let enc2 = dec.encode_assuming_canonical().expect("encode2");
        assert_eq!(enc1, enc2);
    }

    #[test]
    fn hit_list_tie_control_affects_canonical_order_for_equal_scores() {
        let q = blake3_hash(b"q");
        let s = blake3_hash(b"snap");
        let ctrl = blake3_hash(b"ctrl");
        let a = blake3_hash(b"seg-a");
        let b = blake3_hash(b"seg-b");

        let seed = seed64_from_control_id(&ctrl);
        let ka = tiebreak_key(seed, &a, 1);
        let kb = tiebreak_key(seed, &b, 1);
        let expected_first = if ka <= kb { a } else { b };

        let mut hl = HitListV1 {
            query_id: q,
            snapshot_id: s,
            tie_control_id: Some(ctrl),
            hits: vec![
                HitV1 { frame_seg: a, row_ix: 1, score: 10 },
                HitV1 { frame_seg: b, row_ix: 1, score: 10 },
            ],
        };

        // Reverse to ensure canonicalize sorts.
        hl.hits.reverse();
        hl.canonicalize_in_place().expect("canonicalize");
        assert_eq!(hl.hits[0].frame_seg, expected_first);
    }

    #[test]
    fn decode_rejects_duplicate_hits() {
        let q = blake3_hash(b"q");
        let s = blake3_hash(b"snap");
        let a = blake3_hash(b"seg");
        let hl = HitListV1 {
            query_id: q,
            snapshot_id: s,
            tie_control_id: None,
            hits: vec![
                HitV1 { frame_seg: a, row_ix: 1, score: 1 },
                HitV1 { frame_seg: a, row_ix: 1, score: 1 },
            ],
        };

        // Encode is expected to fail because canonicalization enforces uniqueness.
        assert!(hl.encode().is_err());
    }
}
