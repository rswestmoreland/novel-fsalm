// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! TCP framed transport (baseline).
//!
//! provides:
//! - Length-delimited frames: u32 LE length prefix + payload bytes.
//! - A minimal artifact exchange protocol over TCP.
//!
//! This is intentionally small and avoids extra crates. It is not a full RPC system.
//! Later stages will define JobEnvelope and richer message types.

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::hash::Hash32;
use std::io::{Read, Write};
use std::net::TcpStream;

/// Maximum frame size accepted by default (16 MiB).
pub const DEFAULT_MAX_FRAME: u32 = 16 * 1024 * 1024;

/// Message kinds for the baseline artifact protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MsgKind {
    /// Put raw bytes, server returns hash.
    Put = 1,
    /// Get bytes by hash, server returns found flag and bytes.
    Get = 2,
}

impl MsgKind {
    fn to_u8(self) -> u8 {
        self as u8
    }

    fn from_u8(v: u8) -> Result<Self, DecodeError> {
        match v {
            1 => Ok(MsgKind::Put),
            2 => Ok(MsgKind::Get),
            _ => Err(DecodeError::new("invalid MsgKind")),
        }
    }
}

/// Write a single framed payload to a stream.
///
/// Frame format:
/// - u32 length (LE)
/// - payload bytes
pub fn write_frame(stream: &mut TcpStream, payload: &[u8]) -> std::io::Result<()> {
    let len = payload.len() as u32;
    stream.write_all(&len.to_le_bytes())?;
    stream.write_all(payload)?;
    Ok(())
}

/// Read a single framed payload from a stream.
///
/// `max_len` limits memory usage. If the incoming frame exceeds `max_len`,
/// this returns an error.
pub fn read_frame(stream: &mut TcpStream, max_len: u32) -> std::io::Result<Vec<u8>> {
    let mut len_b = [0u8; 4];
    stream.read_exact(&mut len_b)?;
    let len = u32::from_le_bytes(len_b);
    if len > max_len {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "frame too large"));
    }
    let mut buf = vec![0u8; len as usize];
    stream.read_exact(&mut buf)?;
    Ok(buf)
}

/// Encode a Put request payload.
/// Payload format:
/// - u8 kind = Put
/// - u32 byte_len
/// - bytes
pub fn encode_put_req(bytes: &[u8]) -> Result<Vec<u8>, EncodeError> {
    let mut w = ByteWriter::with_capacity(1 + 4 + bytes.len());
    w.write_u8(MsgKind::Put.to_u8());
    w.write_u32(bytes.len() as u32);
    w.write_raw(bytes);
    Ok(w.into_bytes())
}

/// Encode a Get request payload.
/// Payload format:
/// - u8 kind = Get
/// - 32 bytes hash
pub fn encode_get_req(hash: &Hash32) -> Result<Vec<u8>, EncodeError> {
    let mut w = ByteWriter::with_capacity(1 + 32);
    w.write_u8(MsgKind::Get.to_u8());
    w.write_raw(hash);
    Ok(w.into_bytes())
}

/// Decode a request payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// Put bytes request.
    Put(Vec<u8>),
    /// Get bytes request.
    Get(Hash32),
}

/// Decode an incoming request payload.
pub fn decode_request(payload: &[u8]) -> Result<Request, DecodeError> {
    let mut r = ByteReader::new(payload);
    let kind = MsgKind::from_u8(r.read_u8()?)?;
    match kind {
        MsgKind::Put => {
            let n = r.read_u32()? as usize;
            let b = r.read_fixed(n)?.to_vec();
            if r.remaining() != 0 {
                return Err(DecodeError::new("trailing bytes"));
            }
            Ok(Request::Put(b))
        }
        MsgKind::Get => {
            let mut h = [0u8; 32];
            h.copy_from_slice(r.read_fixed(32)?);
            if r.remaining() != 0 {
                return Err(DecodeError::new("trailing bytes"));
            }
            Ok(Request::Get(h))
        }
    }
}

/// Encode a Put response payload.
/// Payload format:
/// - 32 bytes hash
pub fn encode_put_resp(hash: &Hash32) -> Result<Vec<u8>, EncodeError> {
    let mut w = ByteWriter::with_capacity(32);
    w.write_raw(hash);
    Ok(w.into_bytes())
}

/// Encode a Get response payload.
/// Payload format:
/// - u8 found (0/1)
/// - u32 byte_len (if found=1)
/// - bytes (if found=1)
pub fn encode_get_resp(found: bool, bytes: &[u8]) -> Result<Vec<u8>, EncodeError> {
    if !found {
        let mut w = ByteWriter::with_capacity(1);
        w.write_u8(0);
        return Ok(w.into_bytes());
    }
    let mut w = ByteWriter::with_capacity(1 + 4 + bytes.len());
    w.write_u8(1);
    w.write_u32(bytes.len() as u32);
    w.write_raw(bytes);
    Ok(w.into_bytes())
}

/// Decode a Put response payload (hash).
pub fn decode_put_resp(payload: &[u8]) -> Result<Hash32, DecodeError> {
    let mut r = ByteReader::new(payload);
    let mut h = [0u8; 32];
    h.copy_from_slice(r.read_fixed(32)?);
    if r.remaining() != 0 {
        return Err(DecodeError::new("trailing bytes"));
    }
    Ok(h)
}

/// Decode a Get response payload.
pub fn decode_get_resp(payload: &[u8]) -> Result<Option<Vec<u8>>, DecodeError> {
    let mut r = ByteReader::new(payload);
    let found = r.read_u8()?;
    match found {
        0 => {
            if r.remaining() != 0 {
                return Err(DecodeError::new("trailing bytes"));
            }
            Ok(None)
        }
        1 => {
            let n = r.read_u32()? as usize;
            let b = r.read_fixed(n)?.to_vec();
            if r.remaining() != 0 {
                return Err(DecodeError::new("trailing bytes"));
            }
            Ok(Some(b))
        }
        _ => Err(DecodeError::new("invalid found flag")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::blake3_hash;

    #[test]
    fn codec_put_round_trip() {
        let bytes = b"abc";
        let p = encode_put_req(bytes).unwrap();
        let req = decode_request(&p).unwrap();
        assert_eq!(req, Request::Put(bytes.to_vec()));
    }

    #[test]
    fn codec_get_round_trip() {
        let h = blake3_hash(b"x");
        let p = encode_get_req(&h).unwrap();
        let req = decode_request(&p).unwrap();
        assert_eq!(req, Request::Get(h));
    }

    #[test]
    fn codec_put_resp_round_trip() {
        let h = blake3_hash(b"y");
        let p = encode_put_resp(&h).unwrap();
        let d = decode_put_resp(&p).unwrap();
        assert_eq!(d, h);
    }

    #[test]
    fn codec_get_resp_round_trip_found() {
        let bytes = b"zzz";
        let p = encode_get_resp(true, bytes).unwrap();
        let d = decode_get_resp(&p).unwrap().unwrap();
        assert_eq!(d, bytes);
    }

    #[test]
    fn codec_get_resp_round_trip_not_found() {
        let p = encode_get_resp(false, &[]).unwrap();
        let d = decode_get_resp(&p).unwrap();
        assert!(d.is_none());
    }
}
