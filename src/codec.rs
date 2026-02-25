// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Canonical byte codec (little-endian, length-prefixed, stable).
//!
//! This module avoids serde to keep dependencies minimal and to guarantee a fully
//! controlled, deterministic encoding. All variable-length items use a u32 length
//! prefix followed by raw bytes. Strings are UTF-8 bytes (no NUL termination).
//!
//! Design notes:
//! - Prefer preallocation and single-pass writes.
//! - Avoid allocations during decode where possible (slice-based reading).

use core::fmt;

/// Encode error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodeError {
    msg: &'static str,
}

impl EncodeError {
    pub(crate) fn new(msg: &'static str) -> Self {
        Self { msg }
    }
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.msg)
    }
}

/// Decode error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeError {
    msg: &'static str,
}

impl DecodeError {
    pub(crate) fn new(msg: &'static str) -> Self {
        Self { msg }
    }
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.msg)
    }
}

/// Writer for canonical bytes.
#[derive(Debug, Clone)]
pub struct ByteWriter {
    buf: Vec<u8>,
}

impl ByteWriter {
    /// Create a new writer with an initial capacity.
    pub fn with_capacity(cap: usize) -> Self {
        Self { buf: Vec::with_capacity(cap) }
    }

    /// Access the encoded bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }

    /// Reserve additional capacity.
    pub fn reserve(&mut self, add: usize) {
        self.buf.reserve(add);
    }

    /// Write raw bytes (no length prefix).
    pub fn write_raw(&mut self, b: &[u8]) {
        self.buf.extend_from_slice(b);
    }

    /// Write u8.
    pub fn write_u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    /// Write u16 LE.
    pub fn write_u16(&mut self, v: u16) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    /// Write u32 LE.
    pub fn write_u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    /// Write u64 LE.
    pub fn write_u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    /// Write i64 LE.
    pub fn write_i64(&mut self, v: i64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    /// Write length-prefixed bytes: u32(len) + bytes.
    pub fn write_bytes(&mut self, b: &[u8]) -> Result<(), EncodeError> {
        if b.len() > (u32::MAX as usize) {
            return Err(EncodeError::new("bytes too large"));
        }
        self.write_u32(b.len() as u32);
        self.write_raw(b);
        Ok(())
    }

    /// Write a UTF-8 string as length-prefixed bytes.
    pub fn write_str(&mut self, s: &str) -> Result<(), EncodeError> {
        self.write_bytes(s.as_bytes())
    }
}

/// Reader for canonical bytes.
#[derive(Debug, Clone)]
pub struct ByteReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> ByteReader<'a> {
    /// Create a reader over a byte slice.
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    /// Current position.
    pub fn position(&self) -> usize {
        self.pos
    }

    /// Remaining bytes.
    pub fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.pos + n > self.buf.len() {
            return Err(DecodeError::new("unexpected EOF"));
        }
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    /// Read u8.
    pub fn read_u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    /// Read u16 LE.
    pub fn read_u16(&mut self) -> Result<u16, DecodeError> {
        let b = self.take(2)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }

    /// Read u32 LE.
    pub fn read_u32(&mut self) -> Result<u32, DecodeError> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// Read u64 LE.
    pub fn read_u64(&mut self) -> Result<u64, DecodeError> {
        let b = self.take(8)?;
        Ok(u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
    }

    /// Read i64 LE.
    pub fn read_i64(&mut self) -> Result<i64, DecodeError> {
        let b = self.take(8)?;
        Ok(i64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
    }

    /// Read exactly n bytes (no length prefix); returns a slice view.
    pub fn read_fixed(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        self.take(n)
    }

    /// Read length-prefixed bytes; returns a slice view (no alloc).
    pub fn read_bytes_view(&mut self) -> Result<&'a [u8], DecodeError> {
        let len = self.read_u32()? as usize;
        self.take(len)
    }

    /// Read length-prefixed UTF-8 bytes and validate UTF-8; returns &str view.
    pub fn read_str_view(&mut self) -> Result<&'a str, DecodeError> {
        let b = self.read_bytes_view()?;
        core::str::from_utf8(b).map_err(|_| DecodeError::new("invalid UTF-8"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_round_trip_primitives() {
        let mut w = ByteWriter::with_capacity(64);
        w.write_u8(7);
        w.write_u16(0x1234);
        w.write_u32(0x89ABCDEF);
        w.write_u64(0x0123456789ABCDEF);
        w.write_i64(-42);
        w.write_str("hello").unwrap();
        w.write_bytes(&[1, 2, 3, 4]).unwrap();

        let bytes = w.into_bytes();
        let mut r = ByteReader::new(&bytes);

        assert_eq!(r.read_u8().unwrap(), 7);
        assert_eq!(r.read_u16().unwrap(), 0x1234);
        assert_eq!(r.read_u32().unwrap(), 0x89ABCDEF);
        assert_eq!(r.read_u64().unwrap(), 0x0123456789ABCDEF);
        assert_eq!(r.read_i64().unwrap(), -42);
        assert_eq!(r.read_str_view().unwrap(), "hello");
        assert_eq!(r.read_bytes_view().unwrap(), &[1, 2, 3, 4]);
        assert_eq!(r.remaining(), 0);
    }

    #[test]
    fn codec_rejects_invalid_utf8() {
        let mut w = ByteWriter::with_capacity(16);
        // invalid utf8 sequence
        w.write_u32(2);
        w.write_raw(&[0xFF, 0xFF]);
        let bytes = w.into_bytes();
        let mut r = ByteReader::new(&bytes);
        assert!(r.read_str_view().is_err());
    }
}
