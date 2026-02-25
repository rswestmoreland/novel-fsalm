// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! SegmentSig v1: per-index-artifact signature used for query-time gating.
//!
//! A segment signature is a small, deterministic summary that helps skip loading
//! and decoding segments that cannot match a query.
//!
//! v1 provides a Bloom filter over TermId values. This is designed to have:
//! - No false negatives (if a term was inserted, it must test true).
//! - Bounded size (bytes length is fixed per signature).
//! - Deterministic bytes (encode/decode is canonical).
//!
//! The signature is intended to be built for index artifacts referenced by
//! IndexSnapshotV1 entries (IndexSegmentV1 or IndexPackV1) and stored as its own
//! artifact. Later stages wire it into the query path.

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::frame::TermId;
use crate::hash::Hash32;

/// Domain separator for SegmentSig content addressing.
pub const DOMAIN_SEGMENT_SIG: &[u8] = b"sig\0";

const SEGMENT_SIG_MAGIC: [u8; 8] = *b"FSALMSIG";
const SEGMENT_SIG_VERSION: u16 = 1;

/// Segment signature v1.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentSigV1 {
    /// Hash of the index artifact bytes that this signature describes (IndexSegmentV1 or IndexPackV1).
    pub index_seg: Hash32,
    /// Bloom filter k (number of hash probes).
    pub bloom_k: u8,
    /// Bloom bitset bytes. Bit 0 is the low bit of byte 0.
    pub bloom_bits: Vec<u8>,
    /// Optional sketch payload (reserved for future). v1 typically uses empty.
    pub sketch: Vec<u8>,
}

/// Errors during signature build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SegmentSigBuildError {
    /// Bloom bits length must be non-zero.
    EmptyBloom,
    /// Bloom bits length is too large.
    BloomTooLarge,
    /// Bloom k must be non-zero.
    InvalidK,
}

impl core::fmt::Display for SegmentSigBuildError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            SegmentSigBuildError::EmptyBloom => f.write_str("empty bloom"),
            SegmentSigBuildError::BloomTooLarge => f.write_str("bloom too large"),
            SegmentSigBuildError::InvalidK => f.write_str("invalid bloom k"),
        }
    }
}

impl SegmentSigV1 {
    /// Build a v1 signature from the set of terms present in a segment.
    ///
    /// `bloom_bytes` controls the bitset size. `bloom_k` controls the number
    /// of probes per term.
    pub fn build(index_seg: Hash32, terms: &[TermId], bloom_bytes: usize, bloom_k: u8) -> Result<Self, SegmentSigBuildError> {
        if bloom_bytes == 0 {
            return Err(SegmentSigBuildError::EmptyBloom);
        }
        // Defensive cap: signatures should be small.
        if bloom_bytes > (16 * 1024 * 1024) {
            return Err(SegmentSigBuildError::BloomTooLarge);
        }
        if bloom_k == 0 {
            return Err(SegmentSigBuildError::InvalidK);
        }

        let mut bloom_bits: Vec<u8> = vec![0u8; bloom_bytes];
        let m_bits: u64 = (bloom_bytes as u64).saturating_mul(8);
        let use_mask = is_power_of_two_u64(m_bits);
        let mask = if use_mask { m_bits - 1 } else { 0 };

        for &t in terms {
            let x = (t.0).0;
            let h1 = mix64(x ^ 0xA0761D6478BD642F);
            let h2 = mix64(x ^ 0xE7037ED1A0B428DB) | 1;
            for i in 0..(bloom_k as u64) {
                let h = h1.wrapping_add(i.wrapping_mul(h2));
                let ix = if use_mask { h & mask } else { h % m_bits };
                set_bit(&mut bloom_bits, ix as u32);
            }
        }

        Ok(SegmentSigV1 {
            index_seg,
            bloom_k,
            bloom_bits,
            sketch: Vec::new(),
        })
    }

    /// Test whether the Bloom filter might contain the given term.
    pub fn might_contain_term(&self, term: TermId) -> bool {
        if self.bloom_bits.is_empty() || self.bloom_k == 0 {
            return false;
        }
        let m_bits: u64 = (self.bloom_bits.len() as u64).saturating_mul(8);
        if m_bits == 0 {
            return false;
        }
        let use_mask = is_power_of_two_u64(m_bits);
        let mask = if use_mask { m_bits - 1 } else { 0 };

        let x = (term.0).0;
        let h1 = mix64(x ^ 0xA0761D6478BD642F);
        let h2 = mix64(x ^ 0xE7037ED1A0B428DB) | 1;
        for i in 0..(self.bloom_k as u64) {
            let h = h1.wrapping_add(i.wrapping_mul(h2));
            let ix = if use_mask { h & mask } else { h % m_bits };
            if !test_bit(&self.bloom_bits, ix as u32) {
                return false;
            }
        }
        true
    }

    /// Encode into canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        if self.bloom_bits.is_empty() {
            return Err(EncodeError::new("empty bloom"));
        }
        if self.bloom_k == 0 {
            return Err(EncodeError::new("invalid bloom k"));
        }
        if self.bloom_bits.len() > (u32::MAX as usize) {
            return Err(EncodeError::new("bloom too large"));
        }
        if self.sketch.len() > (u32::MAX as usize) {
            return Err(EncodeError::new("sketch too large"));
        }

        let mut w = ByteWriter::with_capacity(64 + self.bloom_bits.len() + self.sketch.len());
        w.write_raw(&SEGMENT_SIG_MAGIC);
        w.write_u16(SEGMENT_SIG_VERSION);
        w.write_u16(0);
        w.write_raw(&self.index_seg);

        w.write_u8(self.bloom_k);
        w.write_u8(0);
        w.write_u8(0);
        w.write_u8(0);

        w.write_u32(self.bloom_bits.len() as u32);
        w.write_raw(&self.bloom_bits);

        w.write_u32(self.sketch.len() as u32);
        w.write_raw(&self.sketch);

        Ok(w.into_bytes())
    }

    /// Decode from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<SegmentSigV1, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let magic = r.read_fixed(8)?;
        if magic != SEGMENT_SIG_MAGIC {
            return Err(DecodeError::new("bad segment sig magic"));
        }
        let ver = r.read_u16()?;
        if ver != SEGMENT_SIG_VERSION {
            return Err(DecodeError::new("unsupported segment sig version"));
        }
        let _reserved = r.read_u16()?;

        let idx_b = r.read_fixed(32)?;
        let mut index_seg = [0u8; 32];
        index_seg.copy_from_slice(idx_b);

        let bloom_k = r.read_u8()?;
        // reserved padding
        let _p0 = r.read_u8()?;
        let _p1 = r.read_u8()?;
        let _p2 = r.read_u8()?;

        if bloom_k == 0 || bloom_k > 32 {
            return Err(DecodeError::new("invalid bloom k"));
        }

        let bloom_len = r.read_u32()? as usize;
        if bloom_len == 0 {
            return Err(DecodeError::new("empty bloom"));
        }
        if bloom_len > (16 * 1024 * 1024) {
            return Err(DecodeError::new("bloom too large"));
        }
        let bloom_view = r.read_fixed(bloom_len)?;
        let mut bloom_bits: Vec<u8> = vec![0u8; bloom_len];
        bloom_bits.copy_from_slice(bloom_view);

        let sketch_len = r.read_u32()? as usize;
        if sketch_len > (16 * 1024 * 1024) {
            return Err(DecodeError::new("sketch too large"));
        }
        let sketch_view = r.read_fixed(sketch_len)?;
        let mut sketch: Vec<u8> = vec![0u8; sketch_len];
        sketch.copy_from_slice(sketch_view);

        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        Ok(SegmentSigV1 { index_seg, bloom_k, bloom_bits, sketch })
    }
}

fn is_power_of_two_u64(v: u64) -> bool {
    v != 0 && (v & (v - 1)) == 0
}

fn set_bit(bits: &mut [u8], bit_ix: u32) {
    let byte_ix = (bit_ix >> 3) as usize;
    let bit = (bit_ix & 7) as u8;
    if byte_ix >= bits.len() {
        return;
    }
    bits[byte_ix] |= 1u8 << bit;
}

fn test_bit(bits: &[u8], bit_ix: u32) -> bool {
    let byte_ix = (bit_ix >> 3) as usize;
    let bit = (bit_ix & 7) as u8;
    if byte_ix >= bits.len() {
        return false;
    }
    (bits[byte_ix] & (1u8 << bit)) != 0
}

// Deterministic 64-bit mixer (SplitMix64-like).
fn mix64(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E3779B97F4A7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::Id64;

    fn h(b: u8) -> Hash32 {
        let mut x = [0u8; 32];
        x[0] = b;
        x
    }

    #[test]
    fn sig_round_trip() {
        let idx = h(7);
        let terms = [TermId(Id64(1)), TermId(Id64(2)), TermId(Id64(3))];
        let s = SegmentSigV1::build(idx, &terms, 128, 7).unwrap();
        let bytes = s.encode().unwrap();
        let s2 = SegmentSigV1::decode(&bytes).unwrap();
        assert_eq!(s2.index_seg, idx);
        assert_eq!(s2.bloom_k, 7);
        assert_eq!(s2.bloom_bits, s.bloom_bits);
        assert_eq!(s2.sketch.len(), 0);
    }

    #[test]
    fn sig_is_deterministic_under_term_order() {
        let idx = h(9);
        let a = [TermId(Id64(10)), TermId(Id64(11)), TermId(Id64(12)), TermId(Id64(13))];
        let b = [a[3], a[1], a[0], a[2]];
        let sa = SegmentSigV1::build(idx, &a, 256, 9).unwrap();
        let sb = SegmentSigV1::build(idx, &b, 256, 9).unwrap();
        assert_eq!(sa.encode().unwrap(), sb.encode().unwrap());
    }

    #[test]
    fn sig_has_no_false_negatives_for_inserted_terms() {
        let idx = h(1);
        let mut terms: Vec<TermId> = Vec::new();
        for i in 0..500u64 {
            terms.push(TermId(Id64(i.wrapping_mul(17).wrapping_add(3))));
        }
        let s = SegmentSigV1::build(idx, &terms, 1024, 7).unwrap();
        for &t in &terms {
            assert!(s.might_contain_term(t));
        }
    }

    #[test]
    fn decode_rejects_trailing_bytes() {
        let idx = h(2);
        let terms = [TermId(Id64(1))];
        let s = SegmentSigV1::build(idx, &terms, 64, 3).unwrap();
        let mut bytes = s.encode().unwrap();
        bytes.push(0);
        assert!(SegmentSigV1::decode(&bytes).is_err());
    }

    #[test]
    fn decode_rejects_bad_magic() {
        let idx = h(3);
        let terms = [TermId(Id64(1))];
        let s = SegmentSigV1::build(idx, &terms, 64, 3).unwrap();
        let mut bytes = s.encode().unwrap();
        bytes[0] = b'X';
        assert!(SegmentSigV1::decode(&bytes).is_err());
    }
}
