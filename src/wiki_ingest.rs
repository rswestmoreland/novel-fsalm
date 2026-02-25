// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Wikipedia TSV ingestion.
//!
//! This module wires a minimal "source adapter" for Wikipedia text into the
//! Novel FSA-LM cold storage layer (FrameSegmentV1 artifacts).
//!
//! Inputs:
//! - A UTF-8 TSV stream with one document per line:
//! <title>\t<text>\n
//! Outputs:
//! - One or more FrameSegmentV1 artifacts containing FrameRowV1 rows derived from
//! the text (token frequencies + metadata).
//! - A WikiIngestManifestV1 artifact listing the produced segment hashes and
//! basic counts. This manifest is the stable "commit hash" for the ingest run.
//!
//! Design goals:
//! - Deterministic: identical input bytes produce identical segment + manifest hashes.
//! - CPU-only and allocation-aware: stream input, reuse buffers, batch segments.
//! - Integer-only: ids and scoring use integer types only.
//!
//! Notes:
//! - This stage uses a simplified TSV adapter. A later stage (3b) will add a
//! streaming Wikipedia XML extractor that feeds the same ingestion pipeline.

use crate::artifact::ArtifactStore;
use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::frame::{derive_id64, DocId, FrameRowV1, Id64, SectionId, SourceId};
use crate::frame_segment::FrameSegmentV1;
use crate::frame_store::put_frame_segment_v1;
use crate::hash::Hash32;
use crate::tokenizer::{term_freqs_from_text, TokenizerCfg};
use crate::sharding_v1::{ShardCfgV1, shard_id_for_doc_id_hash32_v1};
use crate::wiki_xml::{parse_wiki_xml, WikiXmlCfg, WikiXmlError, WikiXmlSink};

use std::io::{self, BufRead};

/// Default source label for Wikipedia English text.
pub const WIKI_EN_SOURCE_LABEL: &str = "wikipedia/enwiki";

/// Configuration for Wikipedia TSV ingestion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WikiIngestCfg {
    /// Source id assigned to produced rows.
    pub source_id: SourceId,
    /// Tokenizer config for term extraction.
    pub tok_cfg: TokenizerCfg,
    /// FrameSegmentV1 chunk_rows parameter.
    pub chunk_rows: u32,
    /// Target maximum rows per produced segment.
    pub seg_rows: u32,
    /// Maximum bytes per row text chunk (UTF-8 safe boundary).
    pub row_max_bytes: usize,
    /// Optional cap on the number of documents (lines) to ingest.
    pub max_docs: Option<u64>,
}

impl WikiIngestCfg {
    /// Default v1 config (small segments, safe row chunking).
    pub fn default_v1() -> Self {
        let source_id = SourceId(derive_id64(b"src\0", WIKI_EN_SOURCE_LABEL.as_bytes()));
        WikiIngestCfg {
            source_id,
            tok_cfg: TokenizerCfg::default(),
            chunk_rows: 1024,
            seg_rows: 512,
            row_max_bytes: 8 * 1024,
            max_docs: None,
        }
    }
}

/// A manifest describing a completed Wikipedia ingest run.
///
/// The manifest is itself a canonical artifact, and can be used as a stable
/// pointer to the produced FrameSegment artifacts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WikiIngestManifestV1 {
    /// Manifest version (currently 1).
    pub version: u16,
    /// Source id for all rows in the ingest.
    pub source_id: SourceId,
    /// Segment build setting: chunk rows.
    pub chunk_rows: u32,
    /// Segment batching setting: max rows per segment.
    pub seg_rows: u32,
    /// Total documents ingested.
    pub docs_total: u64,
    /// Total rows produced (after chunking).
    pub rows_total: u64,
    /// FrameSegment artifact hashes in creation order.
    pub segments: Vec<Hash32>,
}

impl WikiIngestManifestV1 {
    /// Create an empty manifest for a given config.
    pub fn new(cfg: WikiIngestCfg) -> Self {
        WikiIngestManifestV1 {
            version: 1,
            source_id: cfg.source_id,
            chunk_rows: cfg.chunk_rows,
            seg_rows: cfg.seg_rows,
            docs_total: 0,
            rows_total: 0,
            segments: Vec::new(),
        }
    }

    /// Encode manifest into canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        // Fixed fields are small; reserve enough for segment hashes.
        let mut ww = ByteWriter::with_capacity(64 + (self.segments.len() * 32));
        ww.write_u16(self.version);
        ww.write_u64(self.source_id.0.0);
        ww.write_u32(self.chunk_rows);
        ww.write_u32(self.seg_rows);
        ww.write_u64(self.docs_total);
        ww.write_u64(self.rows_total);
        if self.segments.len() > (u32::MAX as usize) {
            return Err(EncodeError::new("too many segments"));
        }
        ww.write_u32(self.segments.len() as u32);
        for h in &self.segments {
            ww.write_raw(h);
        }
        Ok(ww.into_bytes())
    }

    /// Decode manifest from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut rr = ByteReader::new(bytes);
        let version = rr.read_u16()?;
        if version != 1 {
            return Err(DecodeError::new("unsupported manifest version"));
        }
        let src = rr.read_u64()?;
        let chunk_rows = rr.read_u32()?;
        let seg_rows = rr.read_u32()?;
        let docs_total = rr.read_u64()?;
        let rows_total = rr.read_u64()?;
        let n = rr.read_u32()? as usize;

        // Defensive bound: avoid pathological allocations on corrupt data.
        if n > 10_000_000 {
            return Err(DecodeError::new("segments length too large"));
        }

        let mut segments: Vec<Hash32> = Vec::with_capacity(n);
        for _ in 0..n {
            let b = rr.read_fixed(32)?;
            let mut h = [0u8; 32];
            h.copy_from_slice(b);
            segments.push(h);
        }

        if rr.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        Ok(WikiIngestManifestV1 {
            version,
            source_id: SourceId(Id64(src)),
            chunk_rows,
            seg_rows,
            docs_total,
            rows_total,
            segments,
        })
    }
}

/// Errors that can occur during ingest.
#[derive(Debug)]
pub enum WikiIngestError {
    /// I/O error while reading the input stream.
    Io(io::Error),
    /// FrameSegment build or decode error (string form).
    Segment(String),
    /// Wikipedia XML parse error (string form).
    Xml(String),
    /// Artifact store error.
    Store(crate::artifact::ArtifactError),
    /// Manifest encoding error.
    Encode(EncodeError),
}

impl core::fmt::Display for WikiIngestError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            WikiIngestError::Io(e) => write!(f, "io: {}", e),
            WikiIngestError::Segment(e) => write!(f, "segment: {}", e),
            WikiIngestError::Xml(e) => write!(f, "xml: {}", e),
            WikiIngestError::Store(e) => write!(f, "store: {}", e),
            WikiIngestError::Encode(e) => write!(f, "encode: {}", e),
        }
    }
}

impl From<io::Error> for WikiIngestError {
    fn from(e: io::Error) -> Self {
        WikiIngestError::Io(e)
    }
}

impl From<crate::artifact::ArtifactError> for WikiIngestError {
    fn from(e: crate::artifact::ArtifactError) -> Self {
        WikiIngestError::Store(e)
    }
}

fn trim_line_end(mut s: &str) -> &str {
    // Handle both "\n" and "\r\n" without allocation.
    if s.ends_with('\n') {
        s = &s[..s.len() - 1];
        if s.ends_with('\r') {
            s = &s[..s.len() - 1];
        }
    }
    s
}

fn utf8_prefix_boundary(s: &str, max_bytes: usize) -> usize {
    if s.len() <= max_bytes {
        return s.len();
    }
    let mut n = max_bytes;
    while n > 0 && !s.is_char_boundary(n) {
        n -= 1;
    }
    n
}

fn section_id_from(doc: DocId, chunk_index: u32) -> SectionId {
    let mut payload = [0u8; 12];
    payload[..8].copy_from_slice(&doc.0.0.to_le_bytes());
    payload[8..12].copy_from_slice(&chunk_index.to_le_bytes());
    SectionId(derive_id64(b"sec\0", &payload))
}

/// Ingest a TSV stream into FrameSegmentV1 artifacts and return a manifest.
///
/// This function is streaming-friendly: it reads line by line and batches rows
/// into segments of up to cfg.seg_rows.
pub fn ingest_wiki_tsv<R: BufRead, S: ArtifactStore>(
    store: &S,
    reader: R,
    cfg: WikiIngestCfg,
) -> Result<Hash32, WikiIngestError> {
    ingest_wiki_tsv_impl(store, reader, cfg, None)
}

/// Ingest a Wikipedia TSV stream into cold storage, filtering documents to a shard.
///
/// This function reads the full input stream but only stores rows for documents
/// whose DocId maps to the provided shard.
pub fn ingest_wiki_tsv_sharded<R: BufRead, S: ArtifactStore>(
    store: &S,
    reader: R,
    cfg: WikiIngestCfg,
    shard: ShardCfgV1,
) -> Result<Hash32, WikiIngestError> {
    if shard.validate().is_err() {
        return Err(WikiIngestError::Segment("bad shard cfg".to_string()));
    }
    ingest_wiki_tsv_impl(store, reader, cfg, Some(shard))
}

fn ingest_wiki_tsv_impl<R: BufRead, S: ArtifactStore>(
    store: &S,
    mut reader: R,
    cfg: WikiIngestCfg,
    shard: Option<ShardCfgV1>,
) -> Result<Hash32, WikiIngestError> {
    let mut manifest = WikiIngestManifestV1::new(cfg);
    let mut rows: Vec<FrameRowV1> = Vec::with_capacity(cfg.seg_rows as usize);

    let mut line = String::new();
    let mut docs_seen: u64 = 0;

    loop {
        line.clear();
        let n = reader.read_line(&mut line)?;
        if n == 0 {
            break;
        }
        if let Some(max_docs) = cfg.max_docs {
            if docs_seen >= max_docs {
                break;
            }
        }

        let ln = trim_line_end(&line);

        // Skip empty lines deterministically.
        if ln.is_empty() {
            continue;
        }

        let tab = match ln.find('\t') {
            Some(i) => i,
            None => {
                // Treat malformed lines as empty (deterministic and robust).
                continue;
            }
        };
        let (title, text0) = ln.split_at(tab);
        let text = &text0[1..]; // skip tab

        // Derive doc id from title bytes (as-is).
        let doc_id = DocId(derive_id64(b"doc\0", title.as_bytes()));

        if let Some(sc) = shard {
            let sid = shard_id_for_doc_id_hash32_v1(doc_id, sc.shard_count);
            if sid != sc.shard_id {
                continue;
            }
        }

        // Chunk the text into row_max_bytes pieces (UTF-8 safe).
        let mut chunk_index: u32 = 0;
        ingest_text_piece(store, cfg, &mut manifest, &mut rows, doc_id, &mut chunk_index, text)?;
        docs_seen += 1;
        manifest.docs_total = docs_seen;
    }

    if !rows.is_empty() {
        flush_rows(store, cfg, &mut manifest, &mut rows)?;
    }

    let bytes = manifest.encode().map_err(WikiIngestError::Encode)?;
    let mh = store.put(&bytes)?;
    Ok(mh)
}


/// Ingest a Wikipedia XML dump stream into cold storage.
///
/// This uses the streaming XML adapter. The XML must be uncompressed
/// UTF-8 (decompress `.xml.bz2` externally).
///
/// Pages are filtered to <ns>0</ns> by default (main namespace).
/// Ingest a Wikipedia XML dump stream into cold storage.
///
/// This uses the streaming XML adapter. The XML must be uncompressed
/// UTF-8 (decompress `.xml.bz2` externally).
///
/// Pages are filtered to <ns>0</ns> by default (main namespace).
pub fn ingest_wiki_xml<R: BufRead, S: ArtifactStore>(
    store: &S,
    reader: R,
    cfg: WikiIngestCfg,
) -> Result<Hash32, WikiIngestError> {
    ingest_wiki_xml_impl(store, reader, cfg, None)
}

/// Ingest a Wikipedia XML dump stream into cold storage, filtering documents to a shard.
pub fn ingest_wiki_xml_sharded<R: BufRead, S: ArtifactStore>(
    store: &S,
    reader: R,
    cfg: WikiIngestCfg,
    shard: ShardCfgV1,
) -> Result<Hash32, WikiIngestError> {
    if shard.validate().is_err() {
        return Err(WikiIngestError::Segment("bad shard cfg".to_string()));
    }
    ingest_wiki_xml_impl(store, reader, cfg, Some(shard))
}

fn ingest_wiki_xml_impl<R: BufRead, S: ArtifactStore>(
    store: &S,
    reader: R,
    cfg: WikiIngestCfg,
    shard: Option<ShardCfgV1>,
) -> Result<Hash32, WikiIngestError> {

    let mut manifest = WikiIngestManifestV1::new(cfg);
    let mut rows: Vec<FrameRowV1> = Vec::with_capacity(cfg.seg_rows as usize);

    struct Sink<'a, S: ArtifactStore> {
        store: &'a S,
        cfg: WikiIngestCfg,
        manifest: &'a mut WikiIngestManifestV1,
        rows: &'a mut Vec<FrameRowV1>,
        doc_id: DocId,
        chunk_index: u32,
        have_page: bool,
        shard: Option<ShardCfgV1>,
        sink_err: Option<WikiIngestError>,
    }

    impl<'a, S: ArtifactStore> WikiXmlSink for Sink<'a, S> {
        fn on_page_start(&mut self, title: &str) -> Result<(), WikiXmlError> {
            self.doc_id = DocId(derive_id64(b"doc\0", title.as_bytes()));
            self.chunk_index = 0;
            if let Some(sc) = self.shard {
                let sid = shard_id_for_doc_id_hash32_v1(self.doc_id, sc.shard_count);
                self.have_page = sid == sc.shard_id;
            } else {
                self.have_page = true;
            }
            Ok(())
        }

        fn on_text_chunk(&mut self, chunk: &str) -> Result<(), WikiXmlError> {
            if !self.have_page {
                return Ok(());
            }
            let mut idx = self.chunk_index;
            match ingest_text_piece(
                self.store,
                self.cfg,
                self.manifest,
                self.rows,
                self.doc_id,
                &mut idx,
                chunk,
            ) {
                Ok(()) => {
                    self.chunk_index = idx;
                    Ok(())
                }
                Err(e) => {
                    self.sink_err = Some(e);
                    Err(WikiXmlError::Parse("sink error"))
                }
            }
        }

        fn on_page_end(&mut self) -> Result<(), WikiXmlError> {
            if self.have_page {
                self.manifest.docs_total = self.manifest.docs_total.wrapping_add(1);
            }
            self.have_page = false;
            Ok(())
        }
    }

    let mut sink = Sink {
        store,
        cfg,
        manifest: &mut manifest,
        rows: &mut rows,
        doc_id: DocId(derive_id64(b"doc\0", b"")),
        chunk_index: 0,
        have_page: false,
        shard,
        sink_err: None,
    };

    let xml_cfg = WikiXmlCfg::default_v1();
    if let Err(e) = parse_wiki_xml(reader, xml_cfg, &mut sink, cfg.max_docs) {
        if let Some(se) = sink.sink_err.take() {
            return Err(se);
        }
        return Err(WikiIngestError::Xml(format!("{}", e)));
    }

    if !rows.is_empty() {
        flush_rows(store, cfg, &mut manifest, &mut rows)?;
    }

    let bytes = manifest.encode().map_err(WikiIngestError::Encode)?;
    let mh = store.put(&bytes)?;
    Ok(mh)

}




fn flush_rows<S: ArtifactStore>(
    store: &S,
    cfg: WikiIngestCfg,
    manifest: &mut WikiIngestManifestV1,
    rows: &mut Vec<FrameRowV1>,
) -> Result<(), WikiIngestError> {
    let seg = FrameSegmentV1::from_rows(rows.as_slice(), cfg.chunk_rows)
        .map_err(|e| WikiIngestError::Segment(e.to_string()))?;
    let sh = match put_frame_segment_v1(store, &seg) {
        Ok(h) => h,
        Err(e) => match e {
            crate::frame_store::FrameStoreError::Store(se) => return Err(WikiIngestError::Store(se)),
            crate::frame_store::FrameStoreError::Encode(ee) => return Err(WikiIngestError::Encode(ee)),
            crate::frame_store::FrameStoreError::Decode(de) => {
                return Err(WikiIngestError::Segment(de.to_string()))
            }
        },
    };
    manifest.segments.push(sh);
    rows.clear();
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
    use std::io::Cursor;
    use std::fs;

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push("novel_fsalm_tests");
        p.push(name);
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn wiki_manifest_round_trip() {
        let cfg = WikiIngestCfg::default_v1();
        let mut m = WikiIngestManifestV1::new(cfg);
        m.docs_total = 2;
        m.rows_total = 3;
        m.segments.push([7u8; 32]);
        m.segments.push([9u8; 32]);

        let b = m.encode().unwrap();
        let m2 = WikiIngestManifestV1::decode(&b).unwrap();
        assert_eq!(m, m2);
    }

    #[test]
    fn ingest_is_deterministic_for_same_input() {
        let dir = tmp_dir("ingest_is_deterministic_for_same_input");
        let store = FsArtifactStore::new(&dir).unwrap();

        let tsv = "Title1\tThis is a test document.\nTitle2\tAnother doc.\n";
        let cfg = WikiIngestCfg {
            seg_rows: 2,
            row_max_bytes: 16,
            ..WikiIngestCfg::default_v1()
        };

        let h1 = ingest_wiki_tsv(&store, Cursor::new(tsv.as_bytes()), cfg).unwrap();
        let h2 = ingest_wiki_tsv(&store, Cursor::new(tsv.as_bytes()), cfg).unwrap();
        assert_eq!(h1, h2);
    }
}fn ingest_text_piece<S: ArtifactStore>(
    store: &S,
    cfg: WikiIngestCfg,
    manifest: &mut WikiIngestManifestV1,
    rows: &mut Vec<FrameRowV1>,
    doc_id: DocId,
    chunk_index: &mut u32,
    text_piece: &str,
) -> Result<(), WikiIngestError> {
    let mut off: usize = 0;
    while off < text_piece.len() {
        let remain = &text_piece[off..];
        let take = utf8_prefix_boundary(remain, cfg.row_max_bytes);
        if take == 0 {
            break;
        }
        let chunk = &remain[..take];
        off += take;

        let mut row = FrameRowV1::new(doc_id, cfg.source_id);
        row.section_id = Some(section_id_from(doc_id, *chunk_index));

        row.terms = term_freqs_from_text(chunk, cfg.tok_cfg);
        row.recompute_doc_len();

        rows.push(row);
        manifest.rows_total += 1;
        *chunk_index = chunk_index.wrapping_add(1);

        if rows.len() >= (cfg.seg_rows as usize) {
            flush_rows(store, cfg, manifest, rows)?;
        }
    }
    Ok(())
}
