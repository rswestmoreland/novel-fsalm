// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Knowledge frame schema.
//!
//! Frames are the disk-first, retrieval-friendly representation for "knowledge"
//! ingestion (e.g., Wikipedia). They are intended to be stored as columnar
//! segments (FrameSegment) in later stages.
//!
//! This module defines:
//! - stable integer identifier types (DocId, TermId, EntityId,...)
//! - fixed-point types for confidence/scoring
//! - a row-oriented builder view (FrameRowV1) that can be converted into
//! columnar storage later


/// A stable 64-bit identifier.
///
/// This is derived from a domain separator plus payload bytes, using BLAKE3 and
/// a fixed little-endian interpretation of the first 8 hash bytes. This keeps
/// the mapping deterministic across architectures.
///
/// Note: collisions are possible (64-bit space). Later stages may add collision
/// detection or 128-bit ids where needed.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Id64(pub u64);

/// Derive a stable 64-bit id from a domain separator and payload bytes.
pub fn derive_id64(domain: &[u8], payload: &[u8]) -> Id64 {
    // Avoid allocations by streaming both slices into the hasher.
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(payload);
    let h = hasher.finalize();
    let bytes = h.as_bytes();
    let mut b = [0u8; 8];
    b.copy_from_slice(&bytes[..8]);
    Id64(u64::from_le_bytes(b))
}

/// Document id (e.g., a Wikipedia page id or a stable derived id).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct DocId(pub Id64);

/// Source id (which corpus/source produced the row; e.g., wikipedia/enwiki).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct SourceId(pub Id64);

/// Section id within a document (optional, for finer-grained retrieval).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct SectionId(pub Id64);

/// Entity id (people/places/objects). Typically derived from normalized strings.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct EntityId(pub Id64);

/// Verb id (normalized verb or predicate label).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct VerbId(pub Id64);

/// Location id (may represent a geoplace, a page section, or a symbolic place).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct WhereId(pub Id64);

/// Term id used for retrieval (token or normalized term).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct TermId(pub Id64);

/// Metaphonetic code id (used for fuzzy matching by sound).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct MetaCodeId(pub Id64);

/// Signed polarity for a statement.
#[repr(i8)]
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum Polarity {
    /// Negative polarity (e.g., "X is not Y").
    Negative = -1,
    /// Neutral/unknown polarity.
    Neutral = 0,
    /// Positive polarity (affirmative).
    Positive = 1,
}

impl Polarity {
    /// Convert an i8 into a Polarity if it is in {-1,0,1}.
    pub fn from_i8(v: i8) -> Option<Self> {
        match v {
            -1 => Some(Polarity::Negative),
            0 => Some(Polarity::Neutral),
            1 => Some(Polarity::Positive),
            _ => None,
        }
    }

    /// Return the polarity as i8.
    pub fn as_i8(self) -> i8 {
        self as i8
    }
}

/// A fixed-point confidence value in Q16.16 format (integer-only).
///
/// One unit is 1/65536. Typical confidence values are in [0, 1] inclusive,
/// represented as [0, 65536].
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct ConfidenceQ16(pub u32);

impl ConfidenceQ16 {
    /// The fixed-point value representing 0.0.
    pub const ZERO: ConfidenceQ16 = ConfidenceQ16(0);

    /// The fixed-point value representing 1.0 (65536).
    pub const ONE: ConfidenceQ16 = ConfidenceQ16(1u32 << 16);

    /// Create from a numerator/denominator ratio, clamped to [0, ONE].
    ///
    /// If den == 0, returns ZERO.
    pub fn from_ratio(num: u32, den: u32) -> ConfidenceQ16 {
        if den == 0 {
            return ConfidenceQ16::ZERO;
        }
        let v = ((num as u64) << 16) / (den as u64);
        let v = if v > (ConfidenceQ16::ONE.0 as u64) {
            ConfidenceQ16::ONE.0
        } else {
            v as u32
        };
        ConfidenceQ16(v)
    }
}

/// A term + frequency pair for building retrieval columns.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct TermFreq {
    /// Term identifier.
    pub term: TermId,
    /// Term frequency within the row/document scope.
    pub tf: u32,
}

/// Row-oriented view of the knowledge frame schema.
///
/// This type is a staging representation for building segments. Later stages
/// will convert these into columnar segments to avoid per-row allocations.
///
/// Notes:
/// - variable-length fields (entity_ids, terms) are Vec-based in this stage.
/// - timestamps are represented as nanoseconds since Unix epoch in i128.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct FrameRowV1 {
    /// Document identifier.
    pub doc_id: DocId,
    /// Source/corpus identifier.
    pub source_id: SourceId,
    /// Timestamp in nanoseconds since Unix epoch (can be 0 if unknown).
    pub when_ns: i128,
    /// Optional section identifier for finer retrieval.
    pub section_id: Option<SectionId>,
    /// A symbolic location identifier (optional).
    pub where_id: Option<WhereId>,
    /// Primary subject entity (optional).
    pub who: Option<EntityId>,
    /// Primary object/topic entity (optional).
    pub what: Option<EntityId>,
    /// Verb/predicate identifier (optional).
    pub verb: Option<VerbId>,
    /// Polarity of the statement.
    pub polarity: Polarity,
    /// Confidence (Q16.16), used for scoring and filtering.
    pub confidence: ConfidenceQ16,
    /// Additional entities referenced by this row.
    pub entity_ids: Vec<EntityId>,
    /// Terms and their frequencies for retrieval.
    pub terms: Vec<TermFreq>,
    /// Document length proxy (sum of term frequencies).
    pub doc_len: u32,
}

impl FrameRowV1 {
    /// Create a new, empty FrameRowV1 with required ids and defaults.
    pub fn new(doc_id: DocId, source_id: SourceId) -> FrameRowV1 {
        FrameRowV1 {
            doc_id,
            source_id,
            when_ns: 0,
            section_id: None,
            where_id: None,
            who: None,
            what: None,
            verb: None,
            polarity: Polarity::Neutral,
            confidence: ConfidenceQ16::ONE,
            entity_ids: Vec::new(),
            terms: Vec::new(),
            doc_len: 0,
        }
    }

    /// Recompute doc_len as the sum of term frequencies.
    pub fn recompute_doc_len(&mut self) {
        let mut sum: u64 = 0;
        for t in &self.terms {
            sum += t.tf as u64;
        }
        self.doc_len = if sum > (u32::MAX as u64) { u32::MAX } else { sum as u32 };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derive_id64_is_stable() {
        let a = derive_id64(b"term", b"hello");
        let b = derive_id64(b"term", b"hello");
        assert_eq!(a, b);
        assert_ne!(a, derive_id64(b"term", b"hello2"));
        assert_ne!(a, derive_id64(b"other", b"hello"));
    }

    #[test]
    fn polarity_from_i8() {
        assert_eq!(Polarity::from_i8(-1), Some(Polarity::Negative));
        assert_eq!(Polarity::from_i8(0), Some(Polarity::Neutral));
        assert_eq!(Polarity::from_i8(1), Some(Polarity::Positive));
        assert_eq!(Polarity::from_i8(2), None);
    }

    #[test]
    fn confidence_ratio_clamps() {
        assert_eq!(ConfidenceQ16::from_ratio(0, 1), ConfidenceQ16::ZERO);
        assert_eq!(ConfidenceQ16::from_ratio(1, 1), ConfidenceQ16::ONE);
        // > 1 clamps.
        assert_eq!(ConfidenceQ16::from_ratio(2, 1), ConfidenceQ16::ONE);
        // den=0 -> 0
        assert_eq!(ConfidenceQ16::from_ratio(1, 0), ConfidenceQ16::ZERO);
    }

    #[test]
    fn frame_row_recompute_doc_len() {
        let doc = DocId(Id64(1));
        let src = SourceId(Id64(2));
        let mut row = FrameRowV1::new(doc, src);
        row.terms.push(TermFreq { term: TermId(Id64(10)), tf: 3 });
        row.terms.push(TermFreq { term: TermId(Id64(11)), tf: 5 });
        row.recompute_doc_len();
        assert_eq!(row.doc_len, 8);
    }
}
