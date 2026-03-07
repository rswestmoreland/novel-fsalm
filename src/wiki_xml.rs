// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Wikipedia XML dump extractor.
//!
//! This module provides a minimal streaming parser for the Wikipedia
//! `pages-articles` XML dumps. The purpose is not to be a general XML parser.
//! It is a deterministic "source adapter" that extracts:
//! - <page> blocks
//! - <title>...</title>
//! - <ns>...</ns>
//! - <text...>...</text>
//!
//! The extracted text is entity-decoded (amp/lt/gt/quot/apos and numeric entities).
//! Unknown entities are preserved as literal text (e.g. "&foo;" remains "&foo;").
//!
//! Notes:
//! - The official Wikimedia dumps are commonly distributed as `.xml.bz2`.
//! does not implement bzip2 decompression (no extra crates).
//! Decompress externally, or use an uncompressed XML stream.
//! - This parser is designed to be allocation-aware and bounded:
//! text content is decoded and emitted in small chunks to a sink.

use std::io::{self, BufRead};

/// Config for the streaming Wikipedia XML adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WikiXmlCfg {
    /// If true, only ingest pages with <ns>0</ns>.
    pub filter_ns_main: bool,
    /// Maximum decoded UTF-8 bytes emitted per sink callback.
    pub emit_max_bytes: usize,
}

impl WikiXmlCfg {
    pub(crate) fn default_v1() -> Self {
        WikiXmlCfg {
            filter_ns_main: true,
            emit_max_bytes: 16 * 1024,
        }
    }
}

/// Errors from the Wikipedia XML adapter.
#[derive(Debug)]
pub(crate) enum WikiXmlError {
    /// I/O error while reading the XML stream.
    Io(io::Error),
    /// Parse error (deterministic string).
    Parse(&'static str),
}

impl core::fmt::Display for WikiXmlError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            WikiXmlError::Io(e) => write!(f, "io: {}", e),
            WikiXmlError::Parse(s) => write!(f, "parse: {}", s),
        }
    }
}

impl From<io::Error> for WikiXmlError {
    fn from(e: io::Error) -> Self {
        WikiXmlError::Io(e)
    }
}

/// Sink interface for streaming page extraction.
pub(crate) trait WikiXmlSink {
    /// Called when a new page is started (after ns filtering).
    fn on_page_start(&mut self, title: &str) -> Result<(), WikiXmlError>;
    /// Called with decoded text fragments for the current page.
    fn on_text_chunk(&mut self, chunk: &str) -> Result<(), WikiXmlError>;
    /// Called when the current page ends (after text is fully emitted).
    fn on_page_end(&mut self) -> Result<(), WikiXmlError>;
}

pub(crate) fn parse_wiki_xml<R: BufRead, S: WikiXmlSink>(
    mut reader: R,
    cfg: WikiXmlCfg,
    sink: &mut S,
    max_pages: Option<u64>,
) -> Result<(), WikiXmlError> {
    let mut sc = Scanner::new();
    let mut pages: u64 = 0;

    loop {
        // Seek next <page>
        if !sc.scan_to_tag(&mut reader, b"<page>")? {
            break;
        }

        // Extract <title> and <ns>. These are expected within <page>.
        let title_bytes = match sc.read_tag_content(&mut reader, b"<title>", b"</title>")? {
            Some(b) => b,
            None => return Err(WikiXmlError::Parse("page missing <title>")),
        };
        let title = xml_unescape_to_string(&title_bytes);

        let ns_bytes = sc.read_tag_content(&mut reader, b"<ns>", b"</ns>")?;
        let ns: u32 = match ns_bytes {
            Some(b) => parse_u32_ascii(&b).unwrap_or(0),
            None => 0,
        };

        if cfg.filter_ns_main && ns != 0 {
            // Skip this page quickly.
            sc.scan_to_tag(&mut reader, b"</page>")?;
            continue;
        }

        if let Some(maxp) = max_pages {
            if pages >= maxp {
                break;
            }
        }

        sink.on_page_start(&title)?;
        pages = pages.wrapping_add(1);

        // Seek <text...>
        if sc.scan_to_lt_text(&mut reader)? {
            // Consume attributes up to end of start tag.
            let self_closed = sc.scan_to_text_tag_end(&mut reader)?;
            if !self_closed {
                // Stream until </text>
                let mut dec = XmlTextDecoder::new();
                let mut emit = String::with_capacity(cfg.emit_max_bytes + 64);

                sc.stream_text_content(&mut reader, b"</text>", &mut dec, &mut emit, cfg.emit_max_bytes, sink)?;
                dec.finish(&mut emit)?;

                if !emit.is_empty() {
                    sink.on_text_chunk(&emit)?;
                    emit.clear();
                }
            }
        }

        // Seek end page.
        sc.scan_to_tag(&mut reader, b"</page>")?;
        sink.on_page_end()?;
    }

    Ok(())
}

fn parse_u32_ascii(bytes: &[u8]) -> Option<u32> {
    let mut v: u32 = 0;
    if bytes.is_empty() {
        return None;
    }
    for &b in bytes {
        if b < b'0' || b > b'9' {
            return None;
        }
        let d = (b - b'0') as u32;
        v = v.saturating_mul(10).saturating_add(d);
    }
    Some(v)
}

fn xml_unescape_to_string(bytes: &[u8]) -> String {
    // Titles are expected to be valid UTF-8 after unescaping.
    let mut dec = XmlTextDecoder::new();
    let mut out = String::new();
    let _ = dec.feed(bytes, &mut out);
    let _ = dec.finish(&mut out);
    out
}

/// Minimal buffered scanner with deterministic behavior.
///
/// This scanner does not implement a full XML tokenizer. It is a byte-scanning
/// adapter for a known dump format.
struct Scanner {
    buf: Vec<u8>,
    start: usize,
    eof: bool,
}

impl Scanner {
    fn new() -> Self {
        Scanner {
            buf: Vec::with_capacity(64 * 1024),
            start: 0,
            eof: false,
        }
    }

    fn slice(&self) -> &[u8] {
        &self.buf[self.start..]
    }

    fn compact_if_needed(&mut self) {
        if self.start == 0 {
            return;
        }
        if self.start < 64 * 1024 {
            return;
        }
        let len = self.buf.len() - self.start;
        self.buf.copy_within(self.start.., 0);
        self.buf.truncate(len);
        self.start = 0;
    }

    fn fill<R: BufRead>(&mut self, reader: &mut R) -> io::Result<bool> {
        if self.eof {
            return Ok(false);
        }
        let mut tmp = [0u8; 64 * 1024];
        let n = reader.read(&mut tmp)?;
        if n == 0 {
            self.eof = true;
            return Ok(false);
        }
        self.buf.extend_from_slice(&tmp[..n]);
        Ok(true)
    }

    fn find(h: &[u8], pat: &[u8]) -> Option<usize> {
        if pat.is_empty() {
            return Some(0);
        }
        if h.len() < pat.len() {
            return None;
        }
        let first = pat[0];
        let mut i = 0usize;
        while i + pat.len() <= h.len() {
            if h[i] == first {
                if &h[i..i + pat.len()] == pat {
                    return Some(i);
                }
            }
            i += 1;
        }
        None
    }

    fn scan_to_tag<R: BufRead>(&mut self, reader: &mut R, tag: &[u8]) -> Result<bool, WikiXmlError> {
        loop {
            if let Some(i) = Scanner::find(self.slice(), tag) {
                self.start += i + tag.len();
                self.compact_if_needed();
                return Ok(true);
            }
            if !self.fill(reader)? {
                return Ok(false);
            }
        }
    }

    fn read_tag_content<R: BufRead>(
        &mut self,
        reader: &mut R,
        open: &[u8],
        close: &[u8],
    ) -> Result<Option<Vec<u8>>, WikiXmlError> {
        if !self.scan_to_tag(reader, open)? {
            return Ok(None);
        }
        let mut out: Vec<u8> = Vec::new();
        loop {
            if let Some(i) = Scanner::find(self.slice(), close) {
                out.extend_from_slice(&self.slice()[..i]);
                self.start += i + close.len();
                self.compact_if_needed();
                return Ok(Some(out));
            }
            // Append current buffer and refill.
            out.extend_from_slice(self.slice());
            self.start = self.buf.len();
            self.compact_if_needed();
            if !self.fill(reader)? {
                return Err(WikiXmlError::Parse("unterminated tag content"));
            }
        }
    }

    fn scan_to_lt_text<R: BufRead>(&mut self, reader: &mut R) -> Result<bool, WikiXmlError> {
        // Find "<text"
        let tag = b"<text";
        loop {
            if let Some(i) = Scanner::find(self.slice(), tag) {
                self.start += i + tag.len();
                self.compact_if_needed();
                return Ok(true);
            }
            if !self.fill(reader)? {
                return Ok(false);
            }
        }
    }

    fn scan_to_text_tag_end<R: BufRead>(&mut self, reader: &mut R) -> Result<bool, WikiXmlError> {
        // We are positioned just after "<text". Find the next '>' and
        // detect if this is a self-closing tag (ends with "/>").
        loop {
            let s = self.slice();
            if let Some(i) = s.iter().position(|&b| b == b'>') {
                let self_closed = i > 0 && s[i - 1] == b'/';
                self.start += i + 1;
                self.compact_if_needed();
                return Ok(self_closed);
            }
            if !self.fill(reader)? {
                return Err(WikiXmlError::Parse("unterminated <text> start tag"));
            }
        }
    }

    fn stream_text_content<R: BufRead, S: WikiXmlSink>(
        &mut self,
        reader: &mut R,
        close: &[u8],
        dec: &mut XmlTextDecoder,
        emit: &mut String,
        emit_max: usize,
        sink: &mut S,
    ) -> Result<(), WikiXmlError> {
        loop {
            if let Some(i) = Scanner::find(self.slice(), close) {
                let raw = &self.slice()[..i];
                dec.feed(raw, emit)?;
                self.start += i + close.len();
                self.compact_if_needed();
                return Ok(());
            }

            // Process a safe prefix, keeping a small tail for close-tag overlap.
            let s = self.slice();
            if s.len() > close.len() {
                let keep = close.len() - 1;
                let take = s.len() - keep;
                let raw = &s[..take];
                dec.feed(raw, emit)?;
                if emit.len() >= emit_max {
                    sink.on_text_chunk(emit)?;
                    emit.clear();
                }
                self.start += take;
                self.compact_if_needed();
            }

            if !self.fill(reader)? {
                return Err(WikiXmlError::Parse("unterminated <text> content"));
            }
        }
    }
}

/// Streaming entity + UTF-8 decoder for XML character data.
///
/// This decoder is intentionally small. It supports:
/// - &amp; &lt; &gt; &quot; &apos;
/// - &#123; decimal entities
/// - &#x1f4a9; hex entities (case-insensitive)
///
/// Unknown entities are preserved literally.
struct XmlTextDecoder {
    ent: [u8; 32],
    ent_len: usize,
    utf8_pend: [u8; 4],
    utf8_len: usize,
}

impl XmlTextDecoder {
    fn new() -> Self {
        XmlTextDecoder {
            ent: [0u8; 32],
            ent_len: 0,
            utf8_pend: [0u8; 4],
            utf8_len: 0,
        }
    }

    fn finish(&mut self, out: &mut String) -> Result<(), WikiXmlError> {
        if self.ent_len > 0 {
            // Unterminated entity: preserve literally.
            let len = self.ent_len;
            if len > 0 {
                let mut tmp = [0u8; 32];
                tmp[..len].copy_from_slice(&self.ent[..len]);
                self.append_raw_bytes(out, &tmp[..len])?;
            }
            self.ent_len = 0;
        }
        if self.utf8_len > 0 {
            // Truncated UTF-8: replace deterministically.
            out.push('\u{FFFD}');
            self.utf8_len = 0;
        }
        Ok(())
    }

    fn feed(&mut self, bytes: &[u8], out: &mut String) -> Result<(), WikiXmlError> {
        let mut i = 0usize;
        while i < bytes.len() {
            if self.ent_len > 0 {
                // Continue collecting entity bytes until ';'.
                let rest = &bytes[i..];
                if let Some(pos) = rest.iter().position(|&b| b == b';') {
                    let take = pos + 1;
                    self.append_entity_bytes(&rest[..take], out)?;
                    i += take;
                    continue;
                } else {
                    self.append_entity_bytes(rest, out)?;
                    break;
                }
            } else {
                let rest = &bytes[i..];
                if let Some(pos) = rest.iter().position(|&b| b == b'&') {
                    // Emit raw prefix.
                    self.append_raw_bytes(out, &rest[..pos])?;
                    // Start entity capture with '&'.
                    self.ent_len = 0;
                    self.append_entity_bytes(b"&", out)?;
                    i += pos + 1;
                    continue;
                } else {
                    self.append_raw_bytes(out, rest)?;
                    break;
                }
            }
        }
        Ok(())
    }

    fn append_entity_bytes(&mut self, bytes: &[u8], out: &mut String) -> Result<(), WikiXmlError> {
        // If this is the leading '&', capture it.
        for &b in bytes {
            if self.ent_len < self.ent.len() {
                self.ent[self.ent_len] = b;
                self.ent_len += 1;
            } else {
                // Entity too long; preserve literally and reset.
                let len = self.ent_len;
                if len > 0 {
                    let mut tmp = [0u8; 32];
                    tmp[..len].copy_from_slice(&self.ent[..len]);
                    self.append_raw_bytes(out, &tmp[..len])?;
                }
                self.ent_len = 0;
                self.append_raw_bytes(out, &[b])?;
            }
            if b == b';' && self.ent_len > 0 {
                self.decode_entity(out)?;
                self.ent_len = 0;
            }
        }
        Ok(())
    }

    fn decode_entity(&mut self, out: &mut String) -> Result<(), WikiXmlError> {
        // ent includes '&'... ';'
        if self.ent_len < 3 {
            return Ok(());
        }
        let len = self.ent_len;
        let mut ent_buf = [0u8; 32];
        ent_buf[..len].copy_from_slice(&self.ent[..len]);
        let ent = &ent_buf[..len];
        if ent[0] != b'&' || ent[ent.len() - 1] != b';' {
            self.append_raw_bytes(out, ent)?;
            return Ok(());
        }
        let body = &ent[1..ent.len() - 1];

        // Named entities.
        match body {
            b"amp" => {
                out.push('&');
                return Ok(());
            }
            b"lt" => {
                out.push('<');
                return Ok(());
            }
            b"gt" => {
                out.push('>');
                return Ok(());
            }
            b"quot" => {
                out.push('"');
                return Ok(());
            }
            b"apos" => {
                out.push('\u{27}');
                return Ok(());
            }
            _ => {}
        }

        // Numeric entities.
        if !body.is_empty() && body[0] == b'#' {
            let (radix, digits) = if body.len() >= 3 && (body[1] == b'x' || body[1] == b'X') {
                (16u32, &body[2..])
            } else {
                (10u32, &body[1..])
            };
            if let Some(cp) = parse_u32_radix_ascii(digits, radix) {
                if let Some(ch) = core::char::from_u32(cp) {
                    out.push(ch);
                    return Ok(());
                }
            }
            // Invalid numeric entity: replace deterministically.
            out.push('\u{FFFD}');
            return Ok(());
        }

        // Unknown entity: preserve literally.
        self.append_raw_bytes(out, ent)?;
        Ok(())
    }

    fn append_raw_bytes(&mut self, out: &mut String, bytes: &[u8]) -> Result<(), WikiXmlError> {
        if bytes.is_empty() {
            return Ok(());
        }

        // If we have pending UTF-8, try to complete it with up to 4 bytes.
        if self.utf8_len > 0 {
            let need = 4usize.saturating_sub(self.utf8_len);
            let take = core::cmp::min(need, bytes.len());
            for j in 0..take {
                self.utf8_pend[self.utf8_len] = bytes[j];
                self.utf8_len += 1;
            }
            let tmp = &self.utf8_pend[..self.utf8_len];
            match core::str::from_utf8(tmp) {
                Ok(s) => {
                    out.push_str(s);
                    self.utf8_len = 0;
                    return self.append_raw_bytes(out, &bytes[take..]);
                }
                Err(e) => {
                    if e.error_len().is_none() {
                        // Still incomplete.
                        return Ok(());
                    }
                    // Invalid sequence.
                    out.push('\u{FFFD}');
                    self.utf8_len = 0;
                    return self.append_raw_bytes(out, &bytes[take..]);
                }
            }
        }

        // Fast path: common case is valid UTF-8.
        match core::str::from_utf8(bytes) {
            Ok(s) => {
                out.push_str(s);
                Ok(())
            }
            Err(e) => {
                let v = e.valid_up_to();
                if v > 0 {
                    let ok = core::str::from_utf8(&bytes[..v]).map_err(|_| WikiXmlError::Parse("utf8"))?;
                    out.push_str(ok);
                }
                match e.error_len() {
                    Some(bad) => {
                        out.push('\u{FFFD}');
                        let next = v.saturating_add(bad);
                        self.append_raw_bytes(out, &bytes[next..])
                    }
                    None => {
                        // Truncated sequence at end; store up to 3 bytes.
                        let rem = &bytes[v..];
                        let take = core::cmp::min(3, rem.len());
                        for j in 0..take {
                            self.utf8_pend[j] = rem[j];
                        }
                        self.utf8_len = take;
                        Ok(())
                    }
                }
            }
        }
    }
}

fn parse_u32_radix_ascii(bytes: &[u8], radix: u32) -> Option<u32> {
    if bytes.is_empty() {
        return None;
    }
    let mut v: u32 = 0;
    for &b in bytes {
        let d: u32 = match b {
            b'0'..=b'9' => (b - b'0') as u32,
            b'a'..=b'f' => (b - b'a' + 10) as u32,
            b'A'..=b'F' => (b - b'A' + 10) as u32,
            _ => return None,
        };
        if d >= radix {
            return None;
        }
        v = v.saturating_mul(radix).saturating_add(d);
    }
    Some(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xml_unescape_named_entities() {
        let s = xml_unescape_to_string(b"foo&amp;bar&lt;baz&gt;");
        assert_eq!(s, "foo&bar<baz>");
    }

    #[test]
    fn xml_unescape_numeric_entities() {
        let s = xml_unescape_to_string(b"X&#65;&#x42;Y");
        assert_eq!(s, "XABY");
    }

    #[test]
    fn xml_parser_extracts_single_page() {
        let xml = b"<mediawiki><page><title>Hello</title><ns>0</ns><revision><text xml:space=\"preserve\">hi</text></revision></page></mediawiki>";
        let rr = io::Cursor::new(&xml[..]);
        let mut got_title = String::new();
        let mut got_text = String::new();

        struct Sink<'a> {
            t: &'a mut String,
            x: &'a mut String,
        }
        impl<'a> WikiXmlSink for Sink<'a> {
            fn on_page_start(&mut self, title: &str) -> Result<(), WikiXmlError> {
                self.t.push_str(title);
                Ok(())
            }
            fn on_text_chunk(&mut self, chunk: &str) -> Result<(), WikiXmlError> {
                self.x.push_str(chunk);
                Ok(())
            }
            fn on_page_end(&mut self) -> Result<(), WikiXmlError> {
                Ok(())
            }
        }

        let mut sink = Sink {
            t: &mut got_title,
            x: &mut got_text,
        };
        parse_wiki_xml(io::BufReader::new(rr), WikiXmlCfg::default_v1(), &mut sink, None).unwrap();
        assert_eq!(got_title, "Hello");
        assert_eq!(got_text, "hi");
    }
}
