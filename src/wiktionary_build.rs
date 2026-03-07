// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Wiktionary XML ingest to lexicon artifacts.
//!
//! This module wires the Wiktionary wikitext scanner into the lexicon
//! persistence layer:
//! - read pages from the Wikimedia XML dump adapter
//! - extract English-only lexicon signals from wikitext
//! - map signals into lexicon rows
//! - partition rows into segments deterministically
//! - store LexiconSegment artifacts and build a LexiconSnapshot
//!
//! The ingest is deterministic for identical input bytes and configuration.

use crate::artifact::ArtifactStore;
use crate::hash::Hash32;
use crate::lexicon::{
    derive_lemma_id, LemmaRowV1, PronunciationRowV1, RelFromId, RelationEdgeRowV1, SenseRowV1,
    REL_ANTONYM, REL_COORDINATE_TERM, REL_DERIVED_TERM, REL_HOLONYM, REL_HYPERNYM, REL_HYPONYM,
    REL_MERONYM, REL_RELATED, REL_SYNONYM,
};
use crate::lexicon_segment::LexiconSegmentV1;
use crate::lexicon_segment_store::{put_lexicon_segment_v1, LexiconSegmentStoreError};
use crate::lexicon_segmenting::{
    segment_index_for_lemma_id_v1, LexiconRowsV1, LexiconSegmentationError,
};
use crate::lexicon_snapshot_builder::{
    build_lexicon_snapshot_v1_from_segments, LexiconSnapshotBuildError,
};
use crate::metaphone::{meta_code_id_from_token, MetaphoneCfg};
use crate::wiki_xml::{parse_wiki_xml, WikiXmlCfg, WikiXmlError, WikiXmlSink};
use crate::wiktionary_ingest::{
    parse_wiktionary_page_text, WiktionaryPageExtract, WiktionaryParseCfg,
};

use std::io::BufRead;

/// Result summary from Wiktionary ingest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WiktionaryIngestReportV1 {
    /// Stored lexicon segment hashes.
    pub segment_hashes: Vec<Hash32>,
    /// Stored lexicon snapshot hash.
    pub snapshot_hash: Hash32,

    /// Pages observed from the XML stream (after namespace filtering).
    pub pages_seen: u64,
    /// Pages that produced at least one retained signal.
    pub pages_kept: u64,

    /// Total lemma rows produced.
    pub lemmas_total: u64,
    /// Total sense rows produced.
    pub senses_total: u64,
    /// Total relation rows produced.
    pub rels_total: u64,
    /// Total pronunciation rows produced.
    pub prons_total: u64,
}

/// Errors that can occur during Wiktionary ingest.
#[derive(Debug)]
pub enum WiktionaryIngestError {
    /// Invalid segment count.
    InvalidSegmentCount,
    /// XML parse failure.
    Xml(String),
    /// Segment assignment failure.
    Segmentation(String),
    /// Segment build failure.
    SegmentBuild(String),
    /// Segment store failure.
    SegmentStore(String),
    /// Snapshot build failure.
    SnapshotBuild(String),
    /// No segments were produced.
    NoOutput,
}

impl core::fmt::Display for WiktionaryIngestError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            WiktionaryIngestError::InvalidSegmentCount => f.write_str("segment_count must be > 0"),
            WiktionaryIngestError::Xml(s) => write!(f, "xml: {}", s),
            WiktionaryIngestError::Segmentation(s) => write!(f, "segmentation: {}", s),
            WiktionaryIngestError::SegmentBuild(s) => write!(f, "segment build: {}", s),
            WiktionaryIngestError::SegmentStore(s) => write!(f, "segment store: {}", s),
            WiktionaryIngestError::SnapshotBuild(s) => write!(f, "snapshot: {}", s),
            WiktionaryIngestError::NoOutput => f.write_str("no lexicon output"),
        }
    }
}

impl std::error::Error for WiktionaryIngestError {}

fn map_xml_err(e: WikiXmlError) -> WiktionaryIngestError {
    WiktionaryIngestError::Xml(e.to_string())
}

fn map_seg_err(e: LexiconSegmentationError) -> WiktionaryIngestError {
    WiktionaryIngestError::Segmentation(e.to_string())
}

fn map_seg_store_err(e: LexiconSegmentStoreError) -> WiktionaryIngestError {
    WiktionaryIngestError::SegmentStore(e.to_string())
}

fn map_snap_err(e: LexiconSnapshotBuildError) -> WiktionaryIngestError {
    WiktionaryIngestError::SnapshotBuild(e.to_string())
}

fn rel_rows_from_extract(
    ex: &WiktionaryPageExtract,
    lemma_id: crate::lexicon::LemmaId,
) -> Vec<RelationEdgeRowV1> {
    let cap = ex.synonyms.len()
        + ex.antonyms.len()
        + ex.related_terms.len()
        + ex.hypernyms.len()
        + ex.hyponyms.len()
        + ex.derived_terms.len()
        + ex.coordinate_terms.len()
        + ex.holonyms.len()
        + ex.meronyms.len();
    let mut out: Vec<RelationEdgeRowV1> = Vec::with_capacity(cap);

    let from = RelFromId::Lemma(lemma_id);

    push_rel_list(&mut out, from, REL_SYNONYM, &ex.synonyms);
    push_rel_list(&mut out, from, REL_ANTONYM, &ex.antonyms);
    push_rel_list(&mut out, from, REL_RELATED, &ex.related_terms);
    push_rel_list(&mut out, from, REL_HYPERNYM, &ex.hypernyms);
    push_rel_list(&mut out, from, REL_HYPONYM, &ex.hyponyms);
    push_rel_list(&mut out, from, REL_DERIVED_TERM, &ex.derived_terms);
    push_rel_list(&mut out, from, REL_COORDINATE_TERM, &ex.coordinate_terms);
    push_rel_list(&mut out, from, REL_HOLONYM, &ex.holonyms);
    push_rel_list(&mut out, from, REL_MERONYM, &ex.meronyms);

    out
}

fn push_rel_list(
    out: &mut Vec<RelationEdgeRowV1>,
    from: RelFromId,
    rel: crate::lexicon::RelTypeId,
    xs: &[String],
) {
    // Dedup by derived lemma id for determinism.
    let mut seen: Vec<u64> = Vec::with_capacity(xs.len());
    for t in xs {
        let to = derive_lemma_id(t);
        let id = (to.0).0;
        if seen.iter().any(|&x| x == id) {
            continue;
        }
        seen.push(id);
        out.push(RelationEdgeRowV1::new(from, rel, to));
    }
}

fn prons_from_extract(
    title: &str,
    ex: &WiktionaryPageExtract,
    lemma_id: crate::lexicon::LemmaId,
) -> Vec<PronunciationRowV1> {
    let mut out: Vec<PronunciationRowV1> = Vec::with_capacity(ex.ipas.len());

    let meta_cfg = MetaphoneCfg::default();
    let base_meta = meta_code_id_from_token(title, meta_cfg);

    let mut seen_ipa: Vec<u64> = Vec::with_capacity(ex.ipas.len());
    for ipa in &ex.ipas {
        let ipa_id = (crate::lexicon::derive_text_id(ipa).0).0;
        if seen_ipa.iter().any(|&x| x == ipa_id) {
            continue;
        }
        seen_ipa.push(ipa_id);

        let mut meta_codes: Vec<crate::frame::MetaCodeId> = Vec::with_capacity(2);
        if let Some(m) = base_meta {
            meta_codes.push(m);
        }
        if let Some(m) = meta_code_id_from_token(ipa, meta_cfg) {
            meta_codes.push(m);
        }
        out.push(PronunciationRowV1::new(lemma_id, ipa, meta_codes, 0));
    }
    out
}

fn rows_from_extract(ex: WiktionaryPageExtract) -> LexiconRowsV1 {
    let lemma = LemmaRowV1::new(&ex.title, ex.pos_mask, 0);
    let lemma_id = lemma.lemma_id;

    let mut senses: Vec<SenseRowV1> = Vec::with_capacity(ex.senses.len());
    for (ix, gloss) in ex.senses.iter().enumerate() {
        // Ingest caps keep this within u16.
        let rank = ix as u16;
        senses.push(SenseRowV1::new(lemma_id, rank, gloss, 0));
    }

    let rels = rel_rows_from_extract(&ex, lemma_id);
    let prons = prons_from_extract(&ex.title, &ex, lemma_id);

    LexiconRowsV1 {
        lemmas: vec![lemma],
        senses,
        rels,
        prons,
    }
}

struct WiktionaryPageSink {
    parse_cfg: WiktionaryParseCfg,
    segment_count: usize,

    cur_title: String,
    cur_text: String,

    // Per-segment buckets.
    buckets: Vec<LexiconRowsV1>,

    last_err: Option<WiktionaryIngestError>,

    pages_seen: u64,
    pages_kept: u64,

    lemmas_total: u64,
    senses_total: u64,
    rels_total: u64,
    prons_total: u64,
}

impl WiktionaryPageSink {
    fn new(
        parse_cfg: WiktionaryParseCfg,
        segment_count: usize,
    ) -> Result<Self, WiktionaryIngestError> {
        if segment_count == 0 {
            return Err(WiktionaryIngestError::InvalidSegmentCount);
        }
        let buckets: Vec<LexiconRowsV1> =
            (0..segment_count).map(|_| LexiconRowsV1::empty()).collect();
        Ok(WiktionaryPageSink {
            parse_cfg,
            segment_count,
            cur_title: String::new(),
            cur_text: String::new(),
            buckets,
            last_err: None,
            pages_seen: 0,
            pages_kept: 0,
            lemmas_total: 0,
            senses_total: 0,
            rels_total: 0,
            prons_total: 0,
        })
    }

    fn push_rows(&mut self, rows: LexiconRowsV1) -> Result<(), WiktionaryIngestError> {
        if rows.lemmas.is_empty() {
            return Ok(());
        }
        let lemma_id = rows.lemmas[0].lemma_id;
        let ix =
            segment_index_for_lemma_id_v1(lemma_id, self.segment_count).map_err(map_seg_err)?;

        self.lemmas_total = self.lemmas_total.wrapping_add(rows.lemmas.len() as u64);
        self.senses_total = self.senses_total.wrapping_add(rows.senses.len() as u64);
        self.rels_total = self.rels_total.wrapping_add(rows.rels.len() as u64);
        self.prons_total = self.prons_total.wrapping_add(rows.prons.len() as u64);

        let l_len = rows.lemmas.len();
        let s_len = rows.senses.len();
        let r_len = rows.rels.len();
        let p_len = rows.prons.len();

        let b = &mut self.buckets[ix];
        b.lemmas.reserve(l_len);
        b.senses.reserve(s_len);
        b.rels.reserve(r_len);
        b.prons.reserve(p_len);

        b.lemmas.extend(rows.lemmas);
        b.senses.extend(rows.senses);
        b.rels.extend(rows.rels);
        b.prons.extend(rows.prons);
        Ok(())
    }
}

impl WikiXmlSink for WiktionaryPageSink {
    fn on_page_start(&mut self, title: &str) -> Result<(), WikiXmlError> {
        self.cur_title.clear();
        self.cur_title.push_str(title);
        self.cur_text.clear();
        self.pages_seen = self.pages_seen.wrapping_add(1);
        Ok(())
    }

    fn on_text_chunk(&mut self, chunk: &str) -> Result<(), WikiXmlError> {
        self.cur_text.push_str(chunk);
        Ok(())
    }

    fn on_page_end(&mut self) -> Result<(), WikiXmlError> {
        if let Some(ex) =
            parse_wiktionary_page_text(&self.cur_title, &self.cur_text, self.parse_cfg)
        {
            self.pages_kept = self.pages_kept.wrapping_add(1);
            let rows = rows_from_extract(ex);
            // Convert mapping/segmentation failures into XML errors to stop parsing.
            if let Err(e) = self.push_rows(rows) {
                self.last_err = Some(e);
                return Err(WikiXmlError::Parse("wiktionary row mapping failed"));
            }
        }
        Ok(())
    }
}

/// Ingest a Wiktionary XML dump into lexicon segments and a lexicon snapshot.
///
/// Input XML must be UTF-8. If the dump is `.xml.bz2`, decompress externally
/// or wrap the reader with a bzip2 decoder in the caller.
pub fn ingest_wiktionary_xml_to_lexicon_snapshot_v1<R: BufRead, S: ArtifactStore>(
    store: &S,
    reader: R,
    segment_count: usize,
    parse_cfg: WiktionaryParseCfg,
    max_pages: Option<u64>,
) -> Result<WiktionaryIngestReportV1, WiktionaryIngestError> {
    if segment_count == 0 {
        return Err(WiktionaryIngestError::InvalidSegmentCount);
    }

    let mut sink = WiktionaryPageSink::new(parse_cfg, segment_count)?;

    // Wiktionary pages are stored in ns=0 for main entries.
    let xml_cfg = WikiXmlCfg::default_v1();
    if let Err(e) = parse_wiki_xml(reader, xml_cfg, &mut sink, max_pages) {
        if let Some(le) = sink.last_err.take() {
            return Err(le);
        }
        return Err(map_xml_err(e));
    }

    let mut seg_hashes: Vec<Hash32> = Vec::with_capacity(segment_count);

    for b in &sink.buckets {
        if b.lemmas.is_empty() {
            continue;
        }
        let seg = LexiconSegmentV1::build_from_rows(&b.lemmas, &b.senses, &b.rels, &b.prons)
            .map_err(|e| WiktionaryIngestError::SegmentBuild(e.to_string()))?;
        let h = put_lexicon_segment_v1(store, &seg).map_err(map_seg_store_err)?;
        seg_hashes.push(h);
    }

    if seg_hashes.is_empty() {
        return Err(WiktionaryIngestError::NoOutput);
    }

    let (snap_hash, _snap) =
        build_lexicon_snapshot_v1_from_segments(store, &seg_hashes).map_err(map_snap_err)?;

    Ok(WiktionaryIngestReportV1 {
        segment_hashes: seg_hashes,
        snapshot_hash: snap_hash,
        pages_seen: sink.pages_seen,
        pages_kept: sink.pages_kept,
        lemmas_total: sink.lemmas_total,
        senses_total: sink.senses_total,
        rels_total: sink.rels_total,
        prons_total: sink.prons_total,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
    use std::fs;
    use std::io::Cursor;

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push("fsa_lm_tests");
        p.push(name);
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn mini_xml() -> String {
        // Minimal MediaWiki XML structure needed by the adapter.
        let s = r#"<mediawiki>
<page>
<title>Night</title>
<ns>0</ns>
<revision>
<text xml:space="preserve">==English==
===Noun===
# The period of darkness.
====Pronunciation====
* {{IPA|en|/naɪt/}}
====Synonyms====
* [[evening]]
</text>
</revision>
</page>
<page>
<title>NotEnglish</title>
<ns>0</ns>
<revision>
<text xml:space="preserve">==French==
===Noun===
# foo
</text>
</revision>
</page>
</mediawiki>"#;
        s.to_string()
    }

    #[test]
    fn wiktionary_xml_ingest_builds_segments_and_snapshot() {
        let dir = tmp_dir("wiktionary_xml_ingest_builds_segments_and_snapshot");
        let store = FsArtifactStore::new(&dir).unwrap();

        let xml = mini_xml();
        let rr = Cursor::new(xml.as_bytes());

        let rep = ingest_wiktionary_xml_to_lexicon_snapshot_v1(
            &store,
            rr,
            4,
            WiktionaryParseCfg::default_v1(),
            None,
        )
        .unwrap();

        assert!(rep.pages_seen >= 1);
        assert_eq!(rep.pages_kept, 1);
        assert_eq!(rep.lemmas_total, 1);
        assert_eq!(rep.senses_total, 1);
        assert_eq!(rep.rels_total, 1);
        assert_eq!(rep.prons_total, 1);
        assert!(!rep.segment_hashes.is_empty());

        // Snapshot should exist.
        let snap =
            crate::lexicon_snapshot_store::get_lexicon_snapshot_v1(&store, &rep.snapshot_hash)
                .unwrap()
                .unwrap();
        assert_eq!(snap.entries.len(), rep.segment_hashes.len());
    }
}
