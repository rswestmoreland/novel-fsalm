// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! LexiconSegment v1: on-disk, columnar lexicon rows.
//!
//! This module defines a deterministic, canonical byte format for lexicon data.
//! introduces the segment format only (no snapshot/manifest yet).
//!
//! Design goals:
//! - Deterministic, canonical bytes (bitwise stable within a build).
//! - Integer-only schema (ids and masks only).
//! - Minimal allocations during decode (fixed-size columns).
//! - Strict validation: non-canonical payloads are rejected on decode.
//!
//! High-level layout (canonical bytes):
//! - MAGIC[8] + version(u16) + reserved(u16)
//! - lemma_count(u32)
//! - sense_count(u32)
//! - rel_count(u32)
//! - pron_count(u32)
//! - meta_pool_count(u32)
//! - lemma columns (count entries each):
//! lemma_id(u64), lemma_key_id(u64), lemma_text_id(u64), pos_mask(u32), flags(u32)
//! - sense columns (count entries each):
//! sense_id(u64), lemma_id(u64), sense_rank(u16), gloss_text_id(u64), labels_mask(u32)
//! - relation columns (count entries each):
//! from_tag(u8), from_id(u64), rel_type_id(u16), to_lemma_id(u64)
//! - pronunciation columns (count entries each):
//! lemma_id(u64), ipa_text_id(u64), meta_off(u32), meta_len(u32), flags(u32)
//! - meta_pool(u64 * meta_pool_count)
//!
//! Canonical ordering rules (enforced on decode and by build_from_rows):
//! - Lemmas are sorted by lemma_id ascending and lemma_id must be unique.
//! - Senses are sorted by (lemma_id, sense_rank, sense_id) ascending.
//! - Relations are sorted by (from_tag, from_id, rel_type_id, to_lemma_id) ascending.
//! - Pronunciations are sorted by (lemma_id, ipa_text_id, flags, meta_codes) ascending,
//! where meta_codes are compared lexicographically by (len, codes...).
//! - Each pronunciation's meta_codes slice must be sorted and unique.
//! - meta_off/meta_len must index valid slices in meta_pool.

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::frame::{Id64, MetaCodeId};
use crate::lexicon::{
    LemmaId, LemmaKeyId, LemmaRowV1, PronunciationRowV1, RelFromId, RelTypeId, RelationEdgeRowV1,
    SenseId, SenseRowV1, TextId, LEXICON_SCHEMA_V1,
};

/// Domain separator for LexiconSegment content addressing.
pub const DOMAIN_LEXICON_SEGMENT: &[u8] = b"lexseg\0";

const LEXICON_SEGMENT_MAGIC: [u8; 8] = *b"FSALMLEX";
const LEXICON_SEGMENT_VERSION: u16 = 1;

/// LexiconSegment build error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LexiconBuildError {
    /// Too many rows for v1 (u32 overflow).
    TooManyRows,
    /// Lemma ids must be unique.
    DuplicateLemmaId,
    /// One or more input rows had an unsupported schema version.
    InvalidRowVersion,
    /// meta_codes are not sorted and unique.
    NonCanonicalMetaCodes,
    /// meta_pool would exceed u32 indexing.
    MetaPoolTooLarge,
}

impl core::fmt::Display for LexiconBuildError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            LexiconBuildError::TooManyRows => f.write_str("too many rows for v1"),
            LexiconBuildError::DuplicateLemmaId => f.write_str("duplicate lemma_id"),
            LexiconBuildError::InvalidRowVersion => f.write_str("invalid lexicon row version"),
            LexiconBuildError::NonCanonicalMetaCodes => f.write_str("meta_codes not canonical"),
            LexiconBuildError::MetaPoolTooLarge => f.write_str("meta_pool too large"),
        }
    }
}

/// LexiconSegment v1: a columnar lexicon segment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LexiconSegmentV1 {
    /// Lemma columns.
    pub lemma_id: Vec<LemmaId>,
    /// Lemma columns.
    pub lemma_key_id: Vec<LemmaKeyId>,
    /// Lemma columns.
    pub lemma_text_id: Vec<TextId>,
    /// Lemma columns.
    pub lemma_pos_mask: Vec<u32>,
    /// Lemma columns.
    pub lemma_flags: Vec<u32>,

    /// Sense columns.
    pub sense_id: Vec<SenseId>,
    /// Sense columns.
    pub sense_lemma_id: Vec<LemmaId>,
    /// Sense columns.
    pub sense_rank: Vec<u16>,
    /// Sense columns.
    pub sense_gloss_text_id: Vec<TextId>,
    /// Sense columns.
    pub sense_labels_mask: Vec<u32>,

    /// Relation columns.
    pub rel_from_tag: Vec<u8>,
    /// Relation columns (u64 payload of LemmaId or SenseId, tagged by rel_from_tag).
    pub rel_from_id: Vec<u64>,
    /// Relation columns.
    pub rel_type_id: Vec<RelTypeId>,
    /// Relation columns.
    pub rel_to_lemma_id: Vec<LemmaId>,

    /// Pronunciation columns.
    pub pron_lemma_id: Vec<LemmaId>,
    /// Pronunciation columns.
    pub pron_ipa_text_id: Vec<TextId>,
    /// Pronunciation columns: offsets into meta_pool.
    pub pron_meta_off: Vec<u32>,
    /// Pronunciation columns: lengths into meta_pool.
    pub pron_meta_len: Vec<u32>,
    /// Pronunciation columns.
    pub pron_flags: Vec<u32>,

    /// Metaphonetic code pool referenced by pron_meta_off/pron_meta_len.
    pub meta_pool: Vec<MetaCodeId>,
}

fn meta_codes_sorted_unique(xs: &[MetaCodeId]) -> bool {
    if xs.is_empty() {
        return true;
    }
    let mut prev = (xs[0].0).0;
    for m in &xs[1..] {
        let v = (m.0).0;
        if v <= prev {
            return false;
        }
        prev = v;
    }
    true
}

fn cmp_meta_list(a: &[MetaCodeId], b: &[MetaCodeId]) -> core::cmp::Ordering {
    match a.len().cmp(&b.len()) {
        core::cmp::Ordering::Equal => {}
        other => return other,
    }
    for (x, y) in a.iter().zip(b.iter()) {
        match ((x.0).0).cmp(&((y.0).0)) {
            core::cmp::Ordering::Equal => {}
            other => return other,
        }
    }
    core::cmp::Ordering::Equal
}

impl LexiconSegmentV1 {
    /// Build a canonical LexiconSegmentV1 from lexicon rows.
    ///
    /// This function sorts rows into canonical order and validates key invariants.
    pub fn build_from_rows(
        lemmas: &[LemmaRowV1],
        senses: &[SenseRowV1],
        rels: &[RelationEdgeRowV1],
        prons: &[PronunciationRowV1],
    ) -> Result<Self, LexiconBuildError> {
        if lemmas.len() > (u32::MAX as usize)
            || senses.len() > (u32::MAX as usize)
            || rels.len() > (u32::MAX as usize)
            || prons.len() > (u32::MAX as usize)
        {
            return Err(LexiconBuildError::TooManyRows);
        }

        // Canonicalize lemmas.
        let mut lem = lemmas.to_vec();
        for r in &lem {
            if r.version != LEXICON_SCHEMA_V1 {
                return Err(LexiconBuildError::InvalidRowVersion);
            }
        }
        lem.sort_by(|a, b| ((a.lemma_id.0).0).cmp(&((b.lemma_id.0).0)));
        for i in 1..lem.len() {
            if lem[i - 1].lemma_id == lem[i].lemma_id {
                return Err(LexiconBuildError::DuplicateLemmaId);
            }
        }

        // Canonicalize senses.
        let mut sen = senses.to_vec();
        for r in &sen {
            if r.version != LEXICON_SCHEMA_V1 {
                return Err(LexiconBuildError::InvalidRowVersion);
            }
        }
        sen.sort_by(|a, b| {
            let la = (a.lemma_id.0).0;
            let lb = (b.lemma_id.0).0;
            match la.cmp(&lb) {
                core::cmp::Ordering::Equal => match a.sense_rank.cmp(&b.sense_rank) {
                    core::cmp::Ordering::Equal => ((a.sense_id.0).0).cmp(&((b.sense_id.0).0)),
                    other => other,
                },
                other => other,
            }
        });

        // Canonicalize relations.
        let mut rel = rels.to_vec();
        for r in &rel {
            if r.version != LEXICON_SCHEMA_V1 {
                return Err(LexiconBuildError::InvalidRowVersion);
            }
        }
        rel.sort_by(|a, b| {
            let (ta, fa) = match a.from {
                RelFromId::Lemma(id) => (0u8, (id.0).0),
                RelFromId::Sense(id) => (1u8, (id.0).0),
            };
            let (tb, fb) = match b.from {
                RelFromId::Lemma(id) => (0u8, (id.0).0),
                RelFromId::Sense(id) => (1u8, (id.0).0),
            };
            match ta.cmp(&tb) {
                core::cmp::Ordering::Equal => match fa.cmp(&fb) {
                    core::cmp::Ordering::Equal => match a.rel_type_id.0.cmp(&b.rel_type_id.0) {
                        core::cmp::Ordering::Equal => {
                            ((a.to_lemma_id.0).0).cmp(&((b.to_lemma_id.0).0))
                        }
                        other => other,
                    },
                    other => other,
                },
                other => other,
            }
        });

        // Canonicalize pronunciations.
        let mut pro = prons.to_vec();
        for r in &pro {
            if r.version != LEXICON_SCHEMA_V1 {
                return Err(LexiconBuildError::InvalidRowVersion);
            }
            if !meta_codes_sorted_unique(&r.meta_codes) {
                return Err(LexiconBuildError::NonCanonicalMetaCodes);
            }
        }
        pro.sort_by(|a, b| {
            let la = (a.lemma_id.0).0;
            let lb = (b.lemma_id.0).0;
            match la.cmp(&lb) {
                core::cmp::Ordering::Equal => match ((a.ipa_text_id.0).0)
                    .cmp(&((b.ipa_text_id.0).0))
                {
                    core::cmp::Ordering::Equal => match a.flags.cmp(&b.flags) {
                        core::cmp::Ordering::Equal => cmp_meta_list(&a.meta_codes, &b.meta_codes),
                        other => other,
                    },
                    other => other,
                },
                other => other,
            }
        });

        // Estimate meta_pool size.
        let mut meta_total: u64 = 0;
        for r in &pro {
            meta_total = meta_total.saturating_add(r.meta_codes.len() as u64);
        }
        if meta_total > (u32::MAX as u64) {
            return Err(LexiconBuildError::MetaPoolTooLarge);
        }

        // Fill columns.
        let mut lemma_id = Vec::with_capacity(lem.len());
        let mut lemma_key_id = Vec::with_capacity(lem.len());
        let mut lemma_text_id = Vec::with_capacity(lem.len());
        let mut lemma_pos_mask = Vec::with_capacity(lem.len());
        let mut lemma_flags = Vec::with_capacity(lem.len());
        for r in &lem {
            lemma_id.push(r.lemma_id);
            lemma_key_id.push(r.lemma_key_id);
            lemma_text_id.push(r.lemma_text_id);
            lemma_pos_mask.push(r.pos_mask);
            lemma_flags.push(r.flags);
        }

        let mut sense_id = Vec::with_capacity(sen.len());
        let mut sense_lemma_id = Vec::with_capacity(sen.len());
        let mut sense_rank = Vec::with_capacity(sen.len());
        let mut sense_gloss_text_id = Vec::with_capacity(sen.len());
        let mut sense_labels_mask = Vec::with_capacity(sen.len());
        for r in &sen {
            sense_id.push(r.sense_id);
            sense_lemma_id.push(r.lemma_id);
            sense_rank.push(r.sense_rank);
            sense_gloss_text_id.push(r.gloss_text_id);
            sense_labels_mask.push(r.labels_mask);
        }

        let mut rel_from_tag = Vec::with_capacity(rel.len());
        let mut rel_from_id = Vec::with_capacity(rel.len());
        let mut rel_type_id = Vec::with_capacity(rel.len());
        let mut rel_to_lemma_id = Vec::with_capacity(rel.len());
        for r in &rel {
            match r.from {
                RelFromId::Lemma(id) => {
                    rel_from_tag.push(0u8);
                    rel_from_id.push((id.0).0);
                }
                RelFromId::Sense(id) => {
                    rel_from_tag.push(1u8);
                    rel_from_id.push((id.0).0);
                }
            }
            rel_type_id.push(r.rel_type_id);
            rel_to_lemma_id.push(r.to_lemma_id);
        }

        let mut pron_lemma_id = Vec::with_capacity(pro.len());
        let mut pron_ipa_text_id = Vec::with_capacity(pro.len());
        let mut pron_meta_off = Vec::with_capacity(pro.len());
        let mut pron_meta_len = Vec::with_capacity(pro.len());
        let mut pron_flags = Vec::with_capacity(pro.len());
        let mut meta_pool: Vec<MetaCodeId> = Vec::with_capacity(meta_total as usize);

        for r in &pro {
            pron_lemma_id.push(r.lemma_id);
            pron_ipa_text_id.push(r.ipa_text_id);
            let off = meta_pool.len();
            if off > (u32::MAX as usize) {
                return Err(LexiconBuildError::MetaPoolTooLarge);
            }
            if r.meta_codes.len() > (u32::MAX as usize) {
                return Err(LexiconBuildError::MetaPoolTooLarge);
            }
            pron_meta_off.push(off as u32);
            pron_meta_len.push(r.meta_codes.len() as u32);
            meta_pool.extend_from_slice(&r.meta_codes);
            pron_flags.push(r.flags);
        }
        if meta_pool.len() > (u32::MAX as usize) {
            return Err(LexiconBuildError::MetaPoolTooLarge);
        }

        let seg = Self {
            lemma_id,
            lemma_key_id,
            lemma_text_id,
            lemma_pos_mask,
            lemma_flags,
            sense_id,
            sense_lemma_id,
            sense_rank,
            sense_gloss_text_id,
            sense_labels_mask,
            rel_from_tag,
            rel_from_id,
            rel_type_id,
            rel_to_lemma_id,
            pron_lemma_id,
            pron_ipa_text_id,
            pron_meta_off,
            pron_meta_len,
            pron_flags,
            meta_pool,
        };

        // Validate internal canonical invariants to ensure build always yields encodable segments.
        seg.validate_canonical_encode()
            .map_err(|_| LexiconBuildError::NonCanonicalMetaCodes)?;
        Ok(seg)
    }

    /// Encode this LexiconSegmentV1 into canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        self.validate_canonical_encode()?;

        let lemma_count = self.lemma_id.len();
        let sense_count = self.sense_id.len();
        let rel_count = self.rel_from_tag.len();
        let pron_count = self.pron_lemma_id.len();
        let meta_pool_count = self.meta_pool.len();

        let mut cap = 64usize;
        cap = cap.saturating_add(lemma_count.saturating_mul(8 * 3 + 4 * 2));
        cap = cap.saturating_add(sense_count.saturating_mul(8 * 3 + 4 + 2));
        cap = cap.saturating_add(rel_count.saturating_mul(1 + 8 + 2 + 8));
        cap = cap.saturating_add(pron_count.saturating_mul(8 + 8 + 4 + 4 + 4));
        cap = cap.saturating_add(meta_pool_count.saturating_mul(8));
        let mut w = ByteWriter::with_capacity(cap);

        w.write_raw(&LEXICON_SEGMENT_MAGIC);
        w.write_u16(LEXICON_SEGMENT_VERSION);
        w.write_u16(0);

        if lemma_count > (u32::MAX as usize)
            || sense_count > (u32::MAX as usize)
            || rel_count > (u32::MAX as usize)
            || pron_count > (u32::MAX as usize)
            || meta_pool_count > (u32::MAX as usize)
        {
            return Err(EncodeError::new("count overflow"));
        }
        w.write_u32(lemma_count as u32);
        w.write_u32(sense_count as u32);
        w.write_u32(rel_count as u32);
        w.write_u32(pron_count as u32);
        w.write_u32(meta_pool_count as u32);

        // Lemma columns.
        for v in &self.lemma_id {
            w.write_u64((v.0).0);
        }
        for v in &self.lemma_key_id {
            w.write_u64((v.0).0);
        }
        for v in &self.lemma_text_id {
            w.write_u64((v.0).0);
        }
        for &v in &self.lemma_pos_mask {
            w.write_u32(v);
        }
        for &v in &self.lemma_flags {
            w.write_u32(v);
        }

        // Sense columns.
        for v in &self.sense_id {
            w.write_u64((v.0).0);
        }
        for v in &self.sense_lemma_id {
            w.write_u64((v.0).0);
        }
        for &v in &self.sense_rank {
            w.write_u16(v);
        }
        for v in &self.sense_gloss_text_id {
            w.write_u64((v.0).0);
        }
        for &v in &self.sense_labels_mask {
            w.write_u32(v);
        }

        // Relation columns.
        w.write_raw(&self.rel_from_tag);
        for &v in &self.rel_from_id {
            w.write_u64(v);
        }
        for v in &self.rel_type_id {
            w.write_u16(v.0);
        }
        for v in &self.rel_to_lemma_id {
            w.write_u64((v.0).0);
        }

        // Pronunciation columns.
        for v in &self.pron_lemma_id {
            w.write_u64((v.0).0);
        }
        for v in &self.pron_ipa_text_id {
            w.write_u64((v.0).0);
        }
        for &v in &self.pron_meta_off {
            w.write_u32(v);
        }
        for &v in &self.pron_meta_len {
            w.write_u32(v);
        }
        for &v in &self.pron_flags {
            w.write_u32(v);
        }

        for v in &self.meta_pool {
            w.write_u64((v.0).0);
        }

        Ok(w.into_bytes())
    }

    /// Decode a LexiconSegmentV1 from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let magic = r.read_fixed(8)?;
        if magic != LEXICON_SEGMENT_MAGIC {
            return Err(DecodeError::new("invalid magic"));
        }
        let ver = r.read_u16()?;
        if ver != LEXICON_SEGMENT_VERSION {
            return Err(DecodeError::new("unsupported lexicon segment version"));
        }
        let _reserved = r.read_u16()?;

        let lemma_count = r.read_u32()? as usize;
        let sense_count = r.read_u32()? as usize;
        let rel_count = r.read_u32()? as usize;
        let pron_count = r.read_u32()? as usize;
        let meta_pool_count = r.read_u32()? as usize;

        let mut lemma_id = Vec::with_capacity(lemma_count);
        let mut lemma_key_id = Vec::with_capacity(lemma_count);
        let mut lemma_text_id = Vec::with_capacity(lemma_count);
        let mut lemma_pos_mask = Vec::with_capacity(lemma_count);
        let mut lemma_flags = Vec::with_capacity(lemma_count);

        for _ in 0..lemma_count {
            lemma_id.push(LemmaId(Id64(r.read_u64()?)));
        }
        for _ in 0..lemma_count {
            lemma_key_id.push(LemmaKeyId(Id64(r.read_u64()?)));
        }
        for _ in 0..lemma_count {
            lemma_text_id.push(TextId(Id64(r.read_u64()?)));
        }
        for _ in 0..lemma_count {
            lemma_pos_mask.push(r.read_u32()?);
        }
        for _ in 0..lemma_count {
            lemma_flags.push(r.read_u32()?);
        }

        let mut sense_id = Vec::with_capacity(sense_count);
        let mut sense_lemma_id = Vec::with_capacity(sense_count);
        let mut sense_rank = Vec::with_capacity(sense_count);
        let mut sense_gloss_text_id = Vec::with_capacity(sense_count);
        let mut sense_labels_mask = Vec::with_capacity(sense_count);

        for _ in 0..sense_count {
            sense_id.push(SenseId(Id64(r.read_u64()?)));
        }
        for _ in 0..sense_count {
            sense_lemma_id.push(LemmaId(Id64(r.read_u64()?)));
        }
        for _ in 0..sense_count {
            sense_rank.push(r.read_u16()?);
        }
        for _ in 0..sense_count {
            sense_gloss_text_id.push(TextId(Id64(r.read_u64()?)));
        }
        for _ in 0..sense_count {
            sense_labels_mask.push(r.read_u32()?);
        }

        let rel_from_tag = r.read_fixed(rel_count)?.to_vec();
        let mut rel_from_id = Vec::with_capacity(rel_count);
        let mut rel_type_id = Vec::with_capacity(rel_count);
        let mut rel_to_lemma_id = Vec::with_capacity(rel_count);
        for _ in 0..rel_count {
            rel_from_id.push(r.read_u64()?);
        }
        for _ in 0..rel_count {
            rel_type_id.push(RelTypeId(r.read_u16()?));
        }
        for _ in 0..rel_count {
            rel_to_lemma_id.push(LemmaId(Id64(r.read_u64()?)));
        }

        let mut pron_lemma_id = Vec::with_capacity(pron_count);
        let mut pron_ipa_text_id = Vec::with_capacity(pron_count);
        let mut pron_meta_off = Vec::with_capacity(pron_count);
        let mut pron_meta_len = Vec::with_capacity(pron_count);
        let mut pron_flags = Vec::with_capacity(pron_count);
        for _ in 0..pron_count {
            pron_lemma_id.push(LemmaId(Id64(r.read_u64()?)));
        }
        for _ in 0..pron_count {
            pron_ipa_text_id.push(TextId(Id64(r.read_u64()?)));
        }
        for _ in 0..pron_count {
            pron_meta_off.push(r.read_u32()?);
        }
        for _ in 0..pron_count {
            pron_meta_len.push(r.read_u32()?);
        }
        for _ in 0..pron_count {
            pron_flags.push(r.read_u32()?);
        }

        let mut meta_pool = Vec::with_capacity(meta_pool_count);
        for _ in 0..meta_pool_count {
            meta_pool.push(MetaCodeId(Id64(r.read_u64()?)));
        }

        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }

        let seg = Self {
            lemma_id,
            lemma_key_id,
            lemma_text_id,
            lemma_pos_mask,
            lemma_flags,
            sense_id,
            sense_lemma_id,
            sense_rank,
            sense_gloss_text_id,
            sense_labels_mask,
            rel_from_tag,
            rel_from_id,
            rel_type_id,
            rel_to_lemma_id,
            pron_lemma_id,
            pron_ipa_text_id,
            pron_meta_off,
            pron_meta_len,
            pron_flags,
            meta_pool,
        };
        seg.validate_canonical_decode()?;
        Ok(seg)
    }

    fn validate_canonical_encode(&self) -> Result<(), EncodeError> {
        self.validate_counts()
            .map_err(|_| EncodeError::new("column length mismatch"))?;
        self.validate_lemmas()
            .map_err(|_| EncodeError::new("lemmas not canonical"))?;
        self.validate_senses()
            .map_err(|_| EncodeError::new("senses not canonical"))?;
        self.validate_relations()
            .map_err(|_| EncodeError::new("relations not canonical"))?;
        self.validate_prons()
            .map_err(|_| EncodeError::new("pronunciations not canonical"))?;
        Ok(())
    }

    fn validate_canonical_decode(&self) -> Result<(), DecodeError> {
        self.validate_counts()
            .map_err(|_| DecodeError::new("column length mismatch"))?;
        self.validate_lemmas()
            .map_err(|_| DecodeError::new("lemmas not canonical"))?;
        self.validate_senses()
            .map_err(|_| DecodeError::new("senses not canonical"))?;
        self.validate_relations()
            .map_err(|_| DecodeError::new("relations not canonical"))?;
        self.validate_prons()
            .map_err(|_| DecodeError::new("pronunciations not canonical"))?;
        Ok(())
    }

    fn validate_counts(&self) -> Result<(), ()> {
        let lc = self.lemma_id.len();
        if self.lemma_key_id.len() != lc
            || self.lemma_text_id.len() != lc
            || self.lemma_pos_mask.len() != lc
            || self.lemma_flags.len() != lc
        {
            return Err(());
        }

        let sc = self.sense_id.len();
        if self.sense_lemma_id.len() != sc
            || self.sense_rank.len() != sc
            || self.sense_gloss_text_id.len() != sc
            || self.sense_labels_mask.len() != sc
        {
            return Err(());
        }

        let rc = self.rel_from_tag.len();
        if self.rel_from_id.len() != rc
            || self.rel_type_id.len() != rc
            || self.rel_to_lemma_id.len() != rc
        {
            return Err(());
        }

        let pc = self.pron_lemma_id.len();
        if self.pron_ipa_text_id.len() != pc
            || self.pron_meta_off.len() != pc
            || self.pron_meta_len.len() != pc
            || self.pron_flags.len() != pc
        {
            return Err(());
        }
        Ok(())
    }

    fn validate_lemmas(&self) -> Result<(), ()> {
        // Sorted by lemma_id and unique.
        for i in 1..self.lemma_id.len() {
            let a = (self.lemma_id[i - 1].0).0;
            let b = (self.lemma_id[i].0).0;
            if a >= b {
                return Err(());
            }
        }
        Ok(())
    }

    fn validate_senses(&self) -> Result<(), ()> {
        for i in 1..self.sense_id.len() {
            let la = (self.sense_lemma_id[i - 1].0).0;
            let lb = (self.sense_lemma_id[i].0).0;
            let ra = self.sense_rank[i - 1];
            let rb = self.sense_rank[i];
            let sa = (self.sense_id[i - 1].0).0;
            let sb = (self.sense_id[i].0).0;
            let ok = if la < lb {
                true
            } else if la == lb {
                if ra < rb {
                    true
                } else if ra == rb {
                    sa <= sb
                } else {
                    false
                }
            } else {
                false
            };
            if !ok {
                return Err(());
            }
        }
        Ok(())
    }

    fn validate_relations(&self) -> Result<(), ()> {
        for i in 0..self.rel_from_tag.len() {
            let t = self.rel_from_tag[i];
            if t > 1 {
                return Err(());
            }
        }
        for i in 1..self.rel_from_tag.len() {
            let ta = self.rel_from_tag[i - 1];
            let tb = self.rel_from_tag[i];
            let fa = self.rel_from_id[i - 1];
            let fb = self.rel_from_id[i];
            let ra = self.rel_type_id[i - 1].0;
            let rb = self.rel_type_id[i].0;
            let toa = (self.rel_to_lemma_id[i - 1].0).0;
            let tob = (self.rel_to_lemma_id[i].0).0;
            let ok = if ta < tb {
                true
            } else if ta == tb {
                if fa < fb {
                    true
                } else if fa == fb {
                    if ra < rb {
                        true
                    } else if ra == rb {
                        toa <= tob
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            };
            if !ok {
                return Err(());
            }
        }
        Ok(())
    }

    fn validate_prons(&self) -> Result<(), ()> {
        // meta_off/meta_len bounds
        let pool_len = self.meta_pool.len();
        for i in 0..self.pron_lemma_id.len() {
            let off = self.pron_meta_off[i] as usize;
            let len = self.pron_meta_len[i] as usize;
            if off > pool_len || off.saturating_add(len) > pool_len {
                return Err(());
            }
            if !meta_codes_sorted_unique(&self.meta_pool[off..off + len]) {
                return Err(());
            }
        }

        // Canonical ordering by (lemma_id, ipa_text_id, flags, meta_codes).
        for i in 1..self.pron_lemma_id.len() {
            let la = (self.pron_lemma_id[i - 1].0).0;
            let lb = (self.pron_lemma_id[i].0).0;
            let ia = (self.pron_ipa_text_id[i - 1].0).0;
            let ib = (self.pron_ipa_text_id[i].0).0;
            let fa = self.pron_flags[i - 1];
            let fb = self.pron_flags[i];

            let off_a = self.pron_meta_off[i - 1] as usize;
            let len_a = self.pron_meta_len[i - 1] as usize;
            let off_b = self.pron_meta_off[i] as usize;
            let len_b = self.pron_meta_len[i] as usize;
            let ma = &self.meta_pool[off_a..off_a + len_a];
            let mb = &self.meta_pool[off_b..off_b + len_b];

            let ok = if la < lb {
                true
            } else if la == lb {
                if ia < ib {
                    true
                } else if ia == ib {
                    if fa < fb {
                        true
                    } else if fa == fb {
                        cmp_meta_list(ma, mb) != core::cmp::Ordering::Greater
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            };
            if !ok {
                return Err(());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexicon::{derive_lemma_id, POS_NOUN, REL_SYNONYM};

    fn mc(v: u64) -> MetaCodeId {
        MetaCodeId(Id64(v))
    }

    #[test]
    fn lexicon_segment_round_trip_encode_decode() {
        let l1 = LemmaRowV1::new("Night", POS_NOUN, 0);
        let l2 = LemmaRowV1::new("day", POS_NOUN, 0);
        let s1 = SenseRowV1::new(l1.lemma_id, 0, "The time of darkness.", 0);
        let s2 = SenseRowV1::new(l2.lemma_id, 0, "The period of light.", 0);
        let r1 = RelationEdgeRowV1::new(
            RelFromId::Lemma(l1.lemma_id),
            REL_SYNONYM,
            derive_lemma_id("evening"),
        );
        let p1 = PronunciationRowV1::new(l1.lemma_id, "nait", vec![mc(2), mc(1), mc(2)], 7);

        let seg = LexiconSegmentV1::build_from_rows(
            &[l1.clone(), l2.clone()],
            &[s1.clone(), s2.clone()],
            &[r1.clone()],
            &[p1.clone()],
        )
        .unwrap();
        let bytes = seg.encode().unwrap();
        let seg2 = LexiconSegmentV1::decode(&bytes).unwrap();
        assert_eq!(seg, seg2);
    }

    #[test]
    fn lexicon_segment_is_deterministic_across_input_order() {
        let l1 = LemmaRowV1::new("Night", POS_NOUN, 0);
        let l2 = LemmaRowV1::new("day", POS_NOUN, 0);
        let s1 = SenseRowV1::new(l1.lemma_id, 0, "The time of darkness.", 0);
        let s2 = SenseRowV1::new(l2.lemma_id, 0, "The period of light.", 0);
        let r1 = RelationEdgeRowV1::new(
            RelFromId::Lemma(l1.lemma_id),
            REL_SYNONYM,
            derive_lemma_id("evening"),
        );
        let p1 = PronunciationRowV1::new(l1.lemma_id, "nait", vec![mc(1), mc(3)], 0);
        let p2 = PronunciationRowV1::new(l1.lemma_id, "nait", vec![mc(1), mc(2)], 0);

        let a = LexiconSegmentV1::build_from_rows(
            &[l1.clone(), l2.clone()],
            &[s1.clone(), s2.clone()],
            &[r1.clone()],
            &[p1.clone(), p2.clone()],
        )
        .unwrap();
        let b = LexiconSegmentV1::build_from_rows(
            &[l2.clone(), l1.clone()],
            &[s2.clone(), s1.clone()],
            &[r1.clone()],
            &[p2.clone(), p1.clone()],
        )
        .unwrap();
        assert_eq!(a.encode().unwrap(), b.encode().unwrap());
    }

    #[test]
    fn lexicon_segment_decode_rejects_non_canonical_lemma_order() {
        let l1 = LemmaRowV1::new("aa", POS_NOUN, 0);
        let l2 = LemmaRowV1::new("bb", POS_NOUN, 0);
        let seg = LexiconSegmentV1::build_from_rows(&[l1, l2], &[], &[], &[]).unwrap();
        let mut bytes = seg.encode().unwrap();

        // Swap the first two lemma_id u64 values to break canonical order.
        // Header is 8 + 2 + 2 + 5*4 = 32 bytes.
        let off = 32usize;
        let a0 = bytes[off..off + 8].to_vec();
        let a1 = bytes[off + 8..off + 16].to_vec();
        bytes[off..off + 8].copy_from_slice(&a1);
        bytes[off + 8..off + 16].copy_from_slice(&a0);

        assert!(LexiconSegmentV1::decode(&bytes).is_err());
    }

    #[test]
    fn lexicon_segment_decode_rejects_pron_meta_out_of_bounds() {
        let lemma_id = derive_lemma_id("night");
        let p1 = PronunciationRowV1::new(lemma_id, "nait", vec![mc(1), mc(2)], 0);
        let seg = LexiconSegmentV1::build_from_rows(&[], &[], &[], &[p1]).unwrap();
        let mut bytes = seg.encode().unwrap();

        // Compute offset to pron_meta_off[0].
        // Header: 32 bytes.
        // Lemma columns: 0.
        // Sense columns: 0.
        // Relation columns: 0.
        // Pron columns before meta_off: lemma_id(u64) + ipa_text_id(u64) = 16 bytes.
        let pron_meta_off_pos = 32usize + 16usize;

        // Write an out-of-bounds offset (1) while meta_pool_count is 2.
        // The actual pool starts after pron columns, so any non-zero off breaks bounds.
        bytes[pron_meta_off_pos..pron_meta_off_pos + 4].copy_from_slice(&1u32.to_le_bytes());
        assert!(LexiconSegmentV1::decode(&bytes).is_err());
    }
}
