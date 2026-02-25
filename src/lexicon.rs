// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Lexicon schema v1 (Wiktionary, English-only).
//!
//! Novel FSA-LM treats "linguistics" as a first-class, disk-first dataset:
//! - word forms (lemmas), parts-of-speech, and senses (definitions)
//! - lexical relations (synonym, antonym, etc.)
//! - pronunciations (IPA) and metaphonetic codes for reflex-style matching
//!
//! This module defines the in-memory row shapes and stable ids for lexicon
//! ingestion. will define the columnar on-disk segment formats.
//!
//! Design constraints:
//! - deterministic ids and encoding
//! - integer-only, no floats
//! - minimal allocations (derive ids without allocating where possible)
//! - no extra crates beyond what the repo already uses

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::frame::{derive_id64, Id64, MetaCodeId};

/// Lexicon schema version (v1).
pub const LEXICON_SCHEMA_V1: u16 = 1;


/// Domain separator for exact lemma id derivation.
const DOMAIN_LEMMA_ID: &[u8] = b"lex\0lemma\0";

/// Domain separator for lemma-key id derivation (ASCII-lowercased).
const DOMAIN_LEMMA_KEY_ID: &[u8] = b"lex\0lemma_key\0";

/// Domain separator for sense id derivation.
const DOMAIN_SENSE_ID: &[u8] = b"lex\0sense\0";

/// Domain separator for text id derivation.
const DOMAIN_TEXT_ID: &[u8] = b"lex\0text\0";

/// Stable id for a lemma (exact case-preserving key).
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct LemmaId(pub Id64);

/// Stable id for lemma lookup keys (ASCII lowercased).
///
/// This exists to support deterministic matching from tokenized user input
/// (which is typically ASCII-lowercased) to lexicon lemmas, while still keeping
/// `LemmaId` case-preserving to avoid collisions (e.g., "US" vs "us").
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct LemmaKeyId(pub Id64);

/// Stable id for a sense (definition).
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SenseId(pub Id64);

/// Stable id for a text payload stored elsewhere (dictionary-coded later).
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TextId(pub Id64);

/// Relation type id (small integer).
///
/// keeps this as a compact id rather than an enum to allow extending
/// relation taxonomies without changing the binary format.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RelTypeId(pub u16);

/// Common relation type ids (initial set).
pub const REL_SYNONYM: RelTypeId = RelTypeId(1);
/// Common relation type ids (initial set).
pub const REL_ANTONYM: RelTypeId = RelTypeId(2);
/// Common relation type ids (initial set).
pub const REL_RELATED: RelTypeId = RelTypeId(3);
/// Common relation type ids (initial set).
pub const REL_HYPERNYM: RelTypeId = RelTypeId(4);
/// Common relation type ids (initial set).
pub const REL_HYPONYM: RelTypeId = RelTypeId(5);

/// Part-of-speech bitmask (initial set).
///
/// Values are not exhaustive; reserves the mask as an extensible field.
pub const POS_NOUN: u32 = 1 << 0;
/// Part-of-speech bitmask (initial set).
pub const POS_VERB: u32 = 1 << 1;
/// Part-of-speech bitmask (initial set).
pub const POS_ADJ: u32 = 1 << 2;
/// Part-of-speech bitmask (initial set).
pub const POS_ADV: u32 = 1 << 3;
/// Part-of-speech bitmask (initial set).
pub const POS_PROPER_NOUN: u32 = 1 << 4;

/// Lemma row (v1).
///
/// `lemma_text_id` refers to the original lemma text (case preserved).
/// `lemma_key_id` is a lookup key derived from ASCII-lowercased lemma text.
///
/// The text payloads themselves are stored outside of this schema,
/// typically via a dictionary-coded text table or content-addressed artifacts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LemmaRowV1 {
    /// Schema version for this row type.
    pub version: u16,
    /// Exact lemma id.
    pub lemma_id: LemmaId,
    /// Lookup key id (ASCII lowercased).
    pub lemma_key_id: LemmaKeyId,
    /// Text id for the lemma string.
    pub lemma_text_id: TextId,
    /// Part-of-speech mask.
    pub pos_mask: u32,
    /// Misc flags (reserved).
    pub flags: u32,
}

/// Sense row (v1).
///
/// A sense belongs to a lemma and may reference a gloss/definition text id.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SenseRowV1 {
    /// Schema version for this row type.
    pub version: u16,
    /// Sense id.
    pub sense_id: SenseId,
    /// Parent lemma id.
    pub lemma_id: LemmaId,
    /// Rank/ordering within the lemma (0..).
    pub sense_rank: u16,
    /// Text id for the gloss/definition.
    pub gloss_text_id: TextId,
    /// Label mask (reserved; e.g., archaic, slang).
    pub labels_mask: u32,
}

/// Relation "from" id.
///
/// A relation edge may originate at either a lemma or a specific sense.
/// This keeps the schema flexible for Wiktionary-style data, where some
/// relations apply at the word level and others apply at a sense level.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RelFromId {
    /// From a lemma node.
    Lemma(LemmaId),
    /// From a sense node.
    Sense(SenseId),
}

/// Relation edge row (v1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationEdgeRowV1 {
    /// Schema version for this row type.
    pub version: u16,
    /// Relation source node.
    pub from: RelFromId,
    /// Relation type id.
    pub rel_type_id: RelTypeId,
    /// Target lemma id.
    pub to_lemma_id: LemmaId,
}

/// Pronunciation row (v1).
///
/// `ipa_text_id` references an IPA string (stored outside the row).
/// `meta_codes` are metaphonetic codes computed from the lemma and/or IPA.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PronunciationRowV1 {
    /// Schema version for this row type.
    pub version: u16,
    /// Parent lemma id.
    pub lemma_id: LemmaId,
    /// Text id for IPA.
    pub ipa_text_id: TextId,
    /// Metaphonetic codes (must be sorted and unique for canonical encoding).
    pub meta_codes: Vec<MetaCodeId>,
    /// Misc flags (reserved).
    pub flags: u32,
}

/// Derive a stable TextId from a UTF-8 string.
///
/// The string bytes are used as-is (no normalization) to preserve exact payloads.
pub fn derive_text_id(text: &str) -> TextId {
    TextId(derive_id64(DOMAIN_TEXT_ID, text.as_bytes()))
}

/// Derive a stable LemmaId from the exact lemma text.
///
/// This is case-preserving and uses bytes as-is (after trimming whitespace).
pub fn derive_lemma_id(lemma: &str) -> LemmaId {
    let b = lemma.trim().as_bytes();
    LemmaId(derive_id64(DOMAIN_LEMMA_ID, b))
}

/// Derive a stable LemmaKeyId from the lemma text.
///
/// This lowercases ASCII A-Z without allocating. Non-ASCII bytes are passed
/// through unchanged.
pub fn derive_lemma_key_id(lemma: &str) -> LemmaKeyId {
    let s = lemma.trim().as_bytes();
    let mut hasher = blake3::Hasher::new();
    hasher.update(DOMAIN_LEMMA_KEY_ID);
    for &b in s {
        let lb = if b'A' <= b && b <= b'Z' { b + 32 } else { b };
        hasher.update(&[lb]);
    }
    let h = hasher.finalize();
    let bytes = h.as_bytes();
    let mut out = [0u8; 8];
    out.copy_from_slice(&bytes[..8]);
    LemmaKeyId(Id64(u64::from_le_bytes(out)))
}

/// Derive a stable SenseId from a parent lemma id and rank.
///
/// uses a simple, deterministic composition. Wiktionary ingestion can
/// still choose to override sense ids if needed (e.g., stable external keys).
pub fn derive_sense_id(lemma_id: LemmaId, sense_rank: u16) -> SenseId {
    let mut w = ByteWriter::with_capacity(16);
    w.write_u64(lemma_id.0 .0);
    w.write_u16(sense_rank);
    let bytes = w.into_bytes();
    SenseId(derive_id64(DOMAIN_SENSE_ID, &bytes))
}

/// Helper: check if MetaCodeId values are sorted and strictly increasing.
fn meta_codes_are_sorted_unique(xs: &[MetaCodeId]) -> bool {
    if xs.is_empty() {
        return true;
    }
    let mut prev = xs[0].0 .0;
    for m in &xs[1..] {
        let v = m.0 .0;
        if v <= prev {
            return false;
        }
        prev = v;
    }
    true
}

impl LemmaRowV1 {
    /// Create a new lemma row with schema version set.
    pub fn new(lemma: &str, pos_mask: u32, flags: u32) -> Self {
        let lemma_id = derive_lemma_id(lemma);
        let lemma_key_id = derive_lemma_key_id(lemma);
        let lemma_text_id = derive_text_id(lemma);
        Self {
            version: LEXICON_SCHEMA_V1,
            lemma_id,
            lemma_key_id,
            lemma_text_id,
            pos_mask,
            flags,
        }
    }

    /// Encode this row to canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        let mut w = ByteWriter::with_capacity(2 + 8 + 8 + 8 + 4 + 4);
        w.write_u16(self.version);
        w.write_u64(self.lemma_id.0 .0);
        w.write_u64(self.lemma_key_id.0 .0);
        w.write_u64(self.lemma_text_id.0 .0);
        w.write_u32(self.pos_mask);
        w.write_u32(self.flags);
        Ok(w.into_bytes())
    }

    /// Decode this row from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let version = r.read_u16()?;
        if version != LEXICON_SCHEMA_V1 {
            return Err(DecodeError::new("unsupported lemma row version"));
        }
        let lemma_id = LemmaId(Id64(r.read_u64()?));
        let lemma_key_id = LemmaKeyId(Id64(r.read_u64()?));
        let lemma_text_id = TextId(Id64(r.read_u64()?));
        let pos_mask = r.read_u32()?;
        let flags = r.read_u32()?;
        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }
        Ok(Self { version, lemma_id, lemma_key_id, lemma_text_id, pos_mask, flags })
    }
}

impl SenseRowV1 {
    /// Create a new sense row with schema version set.
    pub fn new(lemma_id: LemmaId, sense_rank: u16, gloss_text: &str, labels_mask: u32) -> Self {
        let gloss_text_id = derive_text_id(gloss_text);
        let sense_id = derive_sense_id(lemma_id, sense_rank);
        Self {
            version: LEXICON_SCHEMA_V1,
            sense_id,
            lemma_id,
            sense_rank,
            gloss_text_id,
            labels_mask,
        }
    }

    /// Encode this row to canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        let mut w = ByteWriter::with_capacity(2 + 8 + 8 + 2 + 8 + 4);
        w.write_u16(self.version);
        w.write_u64(self.sense_id.0 .0);
        w.write_u64(self.lemma_id.0 .0);
        w.write_u16(self.sense_rank);
        w.write_u64(self.gloss_text_id.0 .0);
        w.write_u32(self.labels_mask);
        Ok(w.into_bytes())
    }

    /// Decode this row from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let version = r.read_u16()?;
        if version != LEXICON_SCHEMA_V1 {
            return Err(DecodeError::new("unsupported sense row version"));
        }
        let sense_id = SenseId(Id64(r.read_u64()?));
        let lemma_id = LemmaId(Id64(r.read_u64()?));
        let sense_rank = r.read_u16()?;
        let gloss_text_id = TextId(Id64(r.read_u64()?));
        let labels_mask = r.read_u32()?;
        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }
        Ok(Self { version, sense_id, lemma_id, sense_rank, gloss_text_id, labels_mask })
    }
}

impl RelationEdgeRowV1 {
    /// Create a new relation edge row with schema version set.
    pub fn new(from: RelFromId, rel_type_id: RelTypeId, to_lemma_id: LemmaId) -> Self {
        Self { version: LEXICON_SCHEMA_V1, from, rel_type_id, to_lemma_id }
    }

    /// Encode this row to canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        let mut w = ByteWriter::with_capacity(2 + 1 + 8 + 2 + 8);
        w.write_u16(self.version);

        match self.from {
            RelFromId::Lemma(id) => {
                w.write_u8(0);
                w.write_u64(id.0 .0);
            }
            RelFromId::Sense(id) => {
                w.write_u8(1);
                w.write_u64(id.0 .0);
            }
        }

        w.write_u16(self.rel_type_id.0);
        w.write_u64(self.to_lemma_id.0 .0);
        Ok(w.into_bytes())
    }

    /// Decode this row from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let version = r.read_u16()?;
        if version != LEXICON_SCHEMA_V1 {
            return Err(DecodeError::new("unsupported relation edge row version"));
        }

        let tag = r.read_u8()?;
        let from_u = r.read_u64()?;
        let from = match tag {
            0 => RelFromId::Lemma(LemmaId(Id64(from_u))),
            1 => RelFromId::Sense(SenseId(Id64(from_u))),
            _ => return Err(DecodeError::new("invalid from tag")),
        };

        let rel_type_id = RelTypeId(r.read_u16()?);
        let to_lemma_id = LemmaId(Id64(r.read_u64()?));

        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }
        Ok(Self { version, from, rel_type_id, to_lemma_id })
    }
}

impl PronunciationRowV1 {
    /// Create a new pronunciation row with schema version set.
    pub fn new(lemma_id: LemmaId, ipa: &str, mut meta_codes: Vec<MetaCodeId>, flags: u32) -> Self {
        // Canonicalize here to support deterministic encoding without hidden allocs.
        meta_codes.sort_by_key(|m| m.0 .0);
        meta_codes.dedup_by_key(|m| m.0 .0);
        let ipa_text_id = derive_text_id(ipa);
        Self { version: LEXICON_SCHEMA_V1, lemma_id, ipa_text_id, meta_codes, flags }
    }

    /// Encode this row to canonical bytes.
    ///
    /// `meta_codes` must be sorted and unique.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        if self.meta_codes.len() > (u32::MAX as usize) {
            return Err(EncodeError::new("meta_codes too large"));
        }
        // Verify canonical ordering without allocating.
        if !meta_codes_are_sorted_unique(&self.meta_codes) {
            return Err(EncodeError::new("meta_codes must be sorted and unique"));
        }

        let mut w = ByteWriter::with_capacity(2 + 8 + 8 + 4 + (8 * self.meta_codes.len()) + 4);
        w.write_u16(self.version);
        w.write_u64(self.lemma_id.0 .0);
        w.write_u64(self.ipa_text_id.0 .0);
        w.write_u32(self.meta_codes.len() as u32);
        for m in &self.meta_codes {
            w.write_u64(m.0 .0);
        }
        w.write_u32(self.flags);
        Ok(w.into_bytes())
    }

    /// Decode this row from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let version = r.read_u16()?;
        if version != LEXICON_SCHEMA_V1 {
            return Err(DecodeError::new("unsupported pronunciation row version"));
        }
        let lemma_id = LemmaId(Id64(r.read_u64()?));
        let ipa_text_id = TextId(Id64(r.read_u64()?));
        let n = r.read_u32()? as usize;
        let mut meta_codes = Vec::with_capacity(n);
        for _ in 0..n {
            meta_codes.push(MetaCodeId(Id64(r.read_u64()?)));
        }
        let flags = r.read_u32()?;
        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }
        // Enforce canonical ordering on decode as well.
        if !meta_codes_are_sorted_unique(&meta_codes) {
            return Err(DecodeError::new("meta_codes not canonical"));
        }
        Ok(Self { version, lemma_id, ipa_text_id, meta_codes, flags })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lemma_id_is_case_preserving_but_key_is_ascii_lower() {
        let a = derive_lemma_id("US");
        let b = derive_lemma_id("us");
        assert_ne!(a, b);

        let ka = derive_lemma_key_id("US");
        let kb = derive_lemma_key_id("us");
        assert_eq!(ka, kb);
    }

    #[test]
    fn text_id_is_deterministic() {
        let t1 = derive_text_id("hello");
        let t2 = derive_text_id("hello");
        let t3 = derive_text_id("hello!");
        assert_eq!(t1, t2);
        assert_ne!(t1, t3);
    }

    #[test]
    fn lemma_row_round_trip() {
        let row = LemmaRowV1::new("Knight", POS_NOUN, 0);
        let b = row.encode().unwrap();
        let row2 = LemmaRowV1::decode(&b).unwrap();
        assert_eq!(row, row2);
    }

    #[test]
    fn sense_row_round_trip() {
        let lemma_id = derive_lemma_id("night");
        let row = SenseRowV1::new(lemma_id, 0, "The time of darkness.", 0);
        let b = row.encode().unwrap();
        let row2 = SenseRowV1::decode(&b).unwrap();
        assert_eq!(row, row2);
    }

    #[test]
    fn relation_row_round_trip() {
        let a = derive_lemma_id("big");
        let b = derive_lemma_id("large");
        let row = RelationEdgeRowV1::new(RelFromId::Lemma(a), REL_SYNONYM, b);
        let enc = row.encode().unwrap();
        let row2 = RelationEdgeRowV1::decode(&enc).unwrap();
        assert_eq!(row, row2);
    }

    #[test]
    fn pronunciation_row_rejects_non_canonical_meta_codes() {
        let lemma_id = derive_lemma_id("night");
        let ipa = "naɪt";
        let m1 = MetaCodeId(Id64(2));
        let m2 = MetaCodeId(Id64(1));
        let row = PronunciationRowV1 {
            version: LEXICON_SCHEMA_V1,
            lemma_id,
            ipa_text_id: derive_text_id(ipa),
            meta_codes: vec![m1, m2],
            flags: 0,
        };
        assert!(row.encode().is_err());
    }

    #[test]
    fn pronunciation_row_round_trip_with_canonical_meta_codes() {
        let lemma_id = derive_lemma_id("night");
        let ipa = "naɪt";
        let m1 = MetaCodeId(Id64(1));
        let m2 = MetaCodeId(Id64(2));
        let row = PronunciationRowV1::new(lemma_id, ipa, vec![m2, m1, m2], 0);
        let b = row.encode().unwrap();
        let row2 = PronunciationRowV1::decode(&b).unwrap();
        assert_eq!(row, row2);
    }
}
