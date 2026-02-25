// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Artifact Sync V1 protocol.
//!
//! This is a small, deterministic protocol for fetching content-addressed
//! artifacts over TCP using framed messages (see net.rs).
//!
//! The protocol supports:
//! - HELLO handshake (version + sizing caps)
//! - GET streaming (begin + chunks + end)
//! - ERR with ASCII message
//!
//! This is used by the CLI commands:
//! - serve-sync
//! - sync-reduce

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::hash::Hash32;

/// Protocol version.
pub const ARTIFACT_SYNC_V1_VERSION: u32 = 1;

/// Default maximum request frame size (bytes).
pub const DEFAULT_MAX_REQ_FRAME_BYTES: u32 = 64 * 1024;

/// Default maximum chunk size (bytes).
pub const DEFAULT_MAX_CHUNK_BYTES: u32 = 1024 * 1024;

/// Default maximum artifact bytes (bytes).
pub const DEFAULT_MAX_ARTIFACT_BYTES: u32 = 512 * 1024 * 1024;

/// Message kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMsgKind {
    /// Client hello handshake.
    Hello = 1,
    /// Client GET request.
    Get = 2,
    /// Server hello acknowledgment.
    HelloAck = 3,
    /// Server begins GET response.
    GetBegin = 4,
    /// Server sends one GET chunk.
    GetChunk = 5,
    /// Server ends GET response.
    GetEnd = 6,
    /// Server error (ASCII message).
    Err = 7,
}

impl SyncMsgKind {
    /// Parse a message kind byte.
    pub fn from_u8(v: u8) -> Result<Self, DecodeError> {
        match v {
            1 => Ok(SyncMsgKind::Hello),
            2 => Ok(SyncMsgKind::Get),
            3 => Ok(SyncMsgKind::HelloAck),
            4 => Ok(SyncMsgKind::GetBegin),
            5 => Ok(SyncMsgKind::GetChunk),
            6 => Ok(SyncMsgKind::GetEnd),
            7 => Ok(SyncMsgKind::Err),
            _ => Err(DecodeError::new("invalid sync msg kind")),
        }
    }
}

/// HELLO message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelloV1 {
    /// Protocol version (must be ARTIFACT_SYNC_V1_VERSION).
    pub version: u32,
    /// Client preferred max chunk size.
    pub max_chunk_bytes: u32,
    /// Client preferred max artifact bytes.
    pub max_artifact_bytes: u32,
}

/// HELLO_ACK message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelloAckV1 {
    /// Protocol version (ARTIFACT_SYNC_V1_VERSION).
    pub version: u32,
    /// Server max chunk size.
    pub max_chunk_bytes: u32,
    /// Server max artifact bytes.
    pub max_artifact_bytes: u32,
}

/// ERR message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrV1 {
    /// ASCII error message.
    pub msg: String,
}

/// GET request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetReqV1 {
    /// Requested artifact hash.
    pub hash: Hash32,
}

/// GET_BEGIN response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetBeginV1 {
    /// Whether the artifact was found.
    pub found: bool,
    /// Total length of the artifact in bytes (only valid if found=true).
    pub total_len: u32,
}

/// GET_CHUNK response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetChunkV1 {
    /// Chunk bytes.
    pub bytes: Vec<u8>,
}

/// Encode HELLO payload.
pub fn encode_hello_v1(h: &HelloV1) -> Result<Vec<u8>, EncodeError> {
    let mut w = ByteWriter::with_capacity(1 + 4 + 4 + 4);
    w.write_u8(SyncMsgKind::Hello as u8);
    w.write_u32(h.version);
    w.write_u32(h.max_chunk_bytes);
    w.write_u32(h.max_artifact_bytes);
    Ok(w.into_bytes())
}

/// Encode HELLO_ACK payload.
pub fn encode_hello_ack_v1(h: &HelloAckV1) -> Result<Vec<u8>, EncodeError> {
    let mut w = ByteWriter::with_capacity(1 + 4 + 4 + 4);
    w.write_u8(SyncMsgKind::HelloAck as u8);
    w.write_u32(h.version);
    w.write_u32(h.max_chunk_bytes);
    w.write_u32(h.max_artifact_bytes);
    Ok(w.into_bytes())
}

/// Encode GET request payload.
pub fn encode_get_req_v1(h: &Hash32) -> Result<Vec<u8>, EncodeError> {
    let mut w = ByteWriter::with_capacity(1 + 32);
    w.write_u8(SyncMsgKind::Get as u8);
    w.write_raw(h);
    Ok(w.into_bytes())
}

/// Encode GET_BEGIN response payload.
pub fn encode_get_begin_v1(found: bool, total_len: u32) -> Result<Vec<u8>, EncodeError> {
    let mut w = ByteWriter::with_capacity(1 + 1 + 4);
    w.write_u8(SyncMsgKind::GetBegin as u8);
    w.write_u8(if found { 1 } else { 0 });
    if found {
        w.write_u32(total_len);
    }
    Ok(w.into_bytes())
}

/// Encode GET_CHUNK response payload.
pub fn encode_get_chunk_v1(bytes: &[u8]) -> Result<Vec<u8>, EncodeError> {
    let mut w = ByteWriter::with_capacity(1 + 4 + bytes.len());
    w.write_u8(SyncMsgKind::GetChunk as u8);
    w.write_u32(bytes.len() as u32);
    w.write_raw(bytes);
    Ok(w.into_bytes())
}

/// Encode GET_END response payload.
pub fn encode_get_end_v1() -> Result<Vec<u8>, EncodeError> {
    let mut w = ByteWriter::with_capacity(1);
    w.write_u8(SyncMsgKind::GetEnd as u8);
    Ok(w.into_bytes())
}

/// Encode ERR payload.
pub fn encode_err_v1(msg: &str) -> Result<Vec<u8>, EncodeError> {
    if msg.is_empty() || !msg.is_ascii() {
        return Err(EncodeError::new("bad err msg"));
    }
    if msg.len() > (u16::MAX as usize) {
        return Err(EncodeError::new("err msg too long"));
    }
    let mut w = ByteWriter::with_capacity(1 + 2 + msg.len());
    w.write_u8(SyncMsgKind::Err as u8);
    w.write_u16(msg.len() as u16);
    w.write_raw(msg.as_bytes());
    Ok(w.into_bytes())
}

/// Decode HELLO payload.
pub fn decode_hello_v1(payload: &[u8]) -> Result<HelloV1, DecodeError> {
    let mut r = ByteReader::new(payload);
    let k = SyncMsgKind::from_u8(r.read_u8()?)?;
    if k != SyncMsgKind::Hello {
        return Err(DecodeError::new("expected hello"));
    }
    let ver = r.read_u32()?;
    let max_chunk_bytes = r.read_u32()?;
    let max_artifact_bytes = r.read_u32()?;
    if r.remaining() != 0 {
        return Err(DecodeError::new("trailing bytes"));
    }
    Ok(HelloV1 {
        version: ver,
        max_chunk_bytes,
        max_artifact_bytes,
    })
}

/// Decode HELLO_ACK payload.
pub fn decode_hello_ack_v1(payload: &[u8]) -> Result<HelloAckV1, DecodeError> {
    let mut r = ByteReader::new(payload);
    let k = SyncMsgKind::from_u8(r.read_u8()?)?;
    if k != SyncMsgKind::HelloAck {
        return Err(DecodeError::new("expected hello_ack"));
    }
    let ver = r.read_u32()?;
    let max_chunk_bytes = r.read_u32()?;
    let max_artifact_bytes = r.read_u32()?;
    if r.remaining() != 0 {
        return Err(DecodeError::new("trailing bytes"));
    }
    Ok(HelloAckV1 {
        version: ver,
        max_chunk_bytes,
        max_artifact_bytes,
    })
}

/// Decode GET request payload.
pub fn decode_get_req_v1(payload: &[u8]) -> Result<GetReqV1, DecodeError> {
    let mut r = ByteReader::new(payload);
    let k = SyncMsgKind::from_u8(r.read_u8()?)?;
    if k != SyncMsgKind::Get {
        return Err(DecodeError::new("expected get"));
    }
    let hb = r.read_fixed(32)?;
    let mut h = [0u8; 32];
    h.copy_from_slice(hb);
    if r.remaining() != 0 {
        return Err(DecodeError::new("trailing bytes"));
    }
    Ok(GetReqV1 { hash: h })
}

/// Decode GET_BEGIN payload.
pub fn decode_get_begin_v1(payload: &[u8]) -> Result<GetBeginV1, DecodeError> {
    let mut r = ByteReader::new(payload);
    let k = SyncMsgKind::from_u8(r.read_u8()?)?;
    if k != SyncMsgKind::GetBegin {
        return Err(DecodeError::new("expected get_begin"));
    }
    let found = r.read_u8()?;
    match found {
        0 => {
            if r.remaining() != 0 {
                return Err(DecodeError::new("trailing bytes"));
            }
            Ok(GetBeginV1 {
                found: false,
                total_len: 0,
            })
        }
        1 => {
            let total_len = r.read_u32()?;
            if r.remaining() != 0 {
                return Err(DecodeError::new("trailing bytes"));
            }
            Ok(GetBeginV1 {
                found: true,
                total_len,
            })
        }
        _ => Err(DecodeError::new("invalid found flag")),
    }
}

/// Decode GET_CHUNK payload.
pub fn decode_get_chunk_v1(payload: &[u8]) -> Result<GetChunkV1, DecodeError> {
    let mut r = ByteReader::new(payload);
    let k = SyncMsgKind::from_u8(r.read_u8()?)?;
    if k != SyncMsgKind::GetChunk {
        return Err(DecodeError::new("expected get_chunk"));
    }
    let n = r.read_u32()? as usize;
    let b = r.read_fixed(n)?.to_vec();
    if r.remaining() != 0 {
        return Err(DecodeError::new("trailing bytes"));
    }
    Ok(GetChunkV1 { bytes: b })
}

/// Decode GET_END payload.
pub fn decode_get_end_v1(payload: &[u8]) -> Result<(), DecodeError> {
    let mut r = ByteReader::new(payload);
    let k = SyncMsgKind::from_u8(r.read_u8()?)?;
    if k != SyncMsgKind::GetEnd {
        return Err(DecodeError::new("expected get_end"));
    }
    if r.remaining() != 0 {
        return Err(DecodeError::new("trailing bytes"));
    }
    Ok(())
}

/// Decode ERR payload.
pub fn decode_err_v1(payload: &[u8]) -> Result<ErrV1, DecodeError> {
    let mut r = ByteReader::new(payload);
    let k = SyncMsgKind::from_u8(r.read_u8()?)?;
    if k != SyncMsgKind::Err {
        return Err(DecodeError::new("expected err"));
    }
    let n = r.read_u16()? as usize;
    let b = r.read_fixed(n)?;
    if r.remaining() != 0 {
        return Err(DecodeError::new("trailing bytes"));
    }
    let s = core::str::from_utf8(b).map_err(|_| DecodeError::new("bad utf8"))?;
    if s.is_empty() || !s.is_ascii() {
        return Err(DecodeError::new("bad err msg"));
    }
    Ok(ErrV1 { msg: s.to_string() })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::blake3_hash;

    #[test]
    fn hello_round_trip() {
        let h = HelloV1 {
            version: ARTIFACT_SYNC_V1_VERSION,
            max_chunk_bytes: 123,
            max_artifact_bytes: 456,
        };
        let enc = encode_hello_v1(&h).unwrap();
        let dec = decode_hello_v1(&enc).unwrap();
        assert_eq!(h, dec);
    }

    #[test]
    fn hello_ack_round_trip() {
        let h = HelloAckV1 {
            version: ARTIFACT_SYNC_V1_VERSION,
            max_chunk_bytes: 1,
            max_artifact_bytes: 2,
        };
        let enc = encode_hello_ack_v1(&h).unwrap();
        let dec = decode_hello_ack_v1(&enc).unwrap();
        assert_eq!(h, dec);
    }

    #[test]
    fn get_req_round_trip() {
        let hh = blake3_hash(b"x");
        let enc = encode_get_req_v1(&hh).unwrap();
        let dec = decode_get_req_v1(&enc).unwrap();
        assert_eq!(dec.hash, hh);
    }

    #[test]
    fn get_begin_round_trip() {
        let enc = encode_get_begin_v1(true, 99).unwrap();
        let dec = decode_get_begin_v1(&enc).unwrap();
        assert!(dec.found);
        assert_eq!(dec.total_len, 99);
    }

    #[test]
    fn get_chunk_round_trip() {
        let enc = encode_get_chunk_v1(b"abc").unwrap();
        let dec = decode_get_chunk_v1(&enc).unwrap();
        assert_eq!(dec.bytes, b"abc");
    }

    #[test]
    fn err_round_trip() {
        let enc = encode_err_v1("bad").unwrap();
        let dec = decode_err_v1(&enc).unwrap();
        assert_eq!(dec.msg, "bad");
    }
}
