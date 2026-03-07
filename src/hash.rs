// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Content hashing and stable ID derivation.
//!
//! Hashes are used for:
//! - artifact addressing (content-addressed storage)
//! - request_id derivation
//! - snapshot/model/tokenizer IDs
//!
//! Use a stable, deterministic hash function. BLAKE3 is fast and widely used.

/// 32-byte hash.
pub type Hash32 = [u8; 32];

/// 16-byte request id.
pub type Id16 = [u8; 16];

/// Hash bytes with BLAKE3.
pub fn blake3_hash(bytes: &[u8]) -> Hash32 {
    let h = blake3::hash(bytes);
    *h.as_bytes()
}

/// Derive a 16-byte id from a domain separator and payload bytes.
pub fn derive_id16(domain: &[u8], payload: &[u8]) -> Id16 {
    let mut tmp = Vec::with_capacity(domain.len() + payload.len());
    tmp.extend_from_slice(domain);
    tmp.extend_from_slice(payload);
    let h = blake3_hash(&tmp);
    let mut out = [0u8; 16];
    out.copy_from_slice(&h[..16]);
    out
}

/// Render a hash as lowercase hex.
pub fn hex32(h: &Hash32) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = vec![0u8; 64];
    for (i, &b) in h.iter().enumerate() {
        out[i * 2] = HEX[(b >> 4) as usize];
        out[i * 2 + 1] = HEX[(b & 0x0F) as usize];
    }
    // Safety: ASCII hex
    String::from_utf8(out).unwrap()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(10 + (b - b'a')),
        b'A'..=b'F' => Some(10 + (b - b'A')),
        _ => None,
    }
}

/// Parse a 64-character hex string into a 32-byte hash.
///
/// Accepts lowercase or uppercase hex.
pub fn parse_hash32_hex(s: &str) -> Result<Hash32, String> {
    let bs = s.as_bytes();
    if bs.len() != 64 {
        return Err("hash hex must be 64 chars".to_string());
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        let hi = hex_val(bs[i * 2]).ok_or_else(|| "invalid hex".to_string())?;
        let lo = hex_val(bs[i * 2 + 1]).ok_or_else(|| "invalid hex".to_string())?;
        out[i] = (hi << 4) | lo;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_is_stable() {
        let h1 = blake3_hash(b"abc");
        let h2 = blake3_hash(b"abc");
        assert_eq!(h1, h2);
        assert_ne!(h1, blake3_hash(b"abd"));
    }

    #[test]
    fn derive_id16_is_stable() {
        let id1 = derive_id16(b"req", b"payload");
        let id2 = derive_id16(b"req", b"payload");
        assert_eq!(id1, id2);
        assert_ne!(id1, derive_id16(b"req", b"payload2"));
        assert_ne!(id1, derive_id16(b"other", b"payload"));
    }

    #[test]
    fn hex32_len() {
        let h = blake3_hash(b"x");
        let s = hex32(&h);
        assert_eq!(s.len(), 64);
        assert!(s
            .bytes()
            .all(|c| (b'0'..=b'9').contains(&c) || (b'a'..=b'f').contains(&c)));
    }

    #[test]
    fn parse_hash32_hex_round_trip() {
        let h = blake3_hash(b"hello");
        let s = hex32(&h);
        let p = parse_hash32_hex(&s).unwrap();
        assert_eq!(h, p);

        let su = s.to_uppercase();
        let p2 = parse_hash32_hex(&su).unwrap();
        assert_eq!(h, p2);
    }

    #[test]
    fn parse_hash32_hex_rejects_wrong_len() {
        assert!(parse_hash32_hex("").is_err());
        assert!(parse_hash32_hex("00").is_err());
        assert!(parse_hash32_hex(&"0".repeat(63)).is_err());
        assert!(parse_hash32_hex(&"0".repeat(65)).is_err());
    }
}
