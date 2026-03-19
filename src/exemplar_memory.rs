// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! ExemplarMemoryV1 schema and codec.
//!
//! Exemplar memory is a deterministic, structure-only artifact used to capture
//! repeatable answer-shape patterns from existing offline artifacts.
//!
//! v1 intentionally does not store truth facts, retrieved evidence, or free-form
//! text. It stores only compact advisory metadata for later runtime lookup.

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::frame::Id64;
use crate::hash::Hash32;

/// ExemplarMemoryV1 schema version.
pub const EXEMPLAR_MEMORY_V1_VERSION: u32 = 1;

/// Maximum number of rows allowed in v1.
pub const EXEMPLAR_MEMORY_V1_MAX_ROWS: usize = 256;

/// Maximum number of support refs allowed per row in v1.
pub const EXEMPLAR_MEMORY_V1_MAX_SUPPORT_REFS: usize = 16;

/// Artifact flags for ExemplarMemoryV1.
pub type ExemplarMemoryFlagsV1 = u32;

/// Artifact includes ReplayLog-derived support.
pub const EXMEM_FLAG_HAS_REPLAY_LOG: ExemplarMemoryFlagsV1 = 1u32 << 0;

/// Artifact includes PromptPack-derived support.
pub const EXMEM_FLAG_HAS_PROMPT_PACK: ExemplarMemoryFlagsV1 = 1u32 << 1;

/// Artifact includes GoldenPack-derived support.
pub const EXMEM_FLAG_HAS_GOLDEN_PACK: ExemplarMemoryFlagsV1 = 1u32 << 2;

/// Artifact includes GoldenPackConversation-derived support.
pub const EXMEM_FLAG_HAS_GOLDEN_PACK_CONVERSATION: ExemplarMemoryFlagsV1 = 1u32 << 3;

/// Artifact includes ConversationPack-derived support.
pub const EXMEM_FLAG_HAS_CONVERSATION_PACK: ExemplarMemoryFlagsV1 = 1u32 << 4;

/// Artifact includes MarkovTrace-derived support.
pub const EXMEM_FLAG_HAS_MARKOV_TRACE: ExemplarMemoryFlagsV1 = 1u32 << 5;

/// Mask of all known artifact flags in v1.
pub const EXMEM_FLAGS_V1_ALL: ExemplarMemoryFlagsV1 = EXMEM_FLAG_HAS_REPLAY_LOG
    | EXMEM_FLAG_HAS_PROMPT_PACK
    | EXMEM_FLAG_HAS_GOLDEN_PACK
    | EXMEM_FLAG_HAS_GOLDEN_PACK_CONVERSATION
    | EXMEM_FLAG_HAS_CONVERSATION_PACK
    | EXMEM_FLAG_HAS_MARKOV_TRACE;

/// Row flags for ExemplarMemoryV1.
pub type ExemplarRowFlagsV1 = u32;

/// Row shape includes a summary-first presentation.
pub const EXROW_FLAG_HAS_SUMMARY: ExemplarRowFlagsV1 = 1u32 << 0;

/// Row shape includes explicit step-by-step structure.
pub const EXROW_FLAG_HAS_STEPS: ExemplarRowFlagsV1 = 1u32 << 1;

/// Row shape includes comparison framing.
pub const EXROW_FLAG_HAS_COMPARISON: ExemplarRowFlagsV1 = 1u32 << 2;

/// Row shape includes a clarifier before the answer body.
pub const EXROW_FLAG_HAS_CLARIFIER: ExemplarRowFlagsV1 = 1u32 << 3;

/// Mask of all known row flags in v1.
pub const EXROW_FLAGS_V1_ALL: ExemplarRowFlagsV1 = EXROW_FLAG_HAS_SUMMARY
    | EXROW_FLAG_HAS_STEPS
    | EXROW_FLAG_HAS_COMPARISON
    | EXROW_FLAG_HAS_CLARIFIER;

/// Response mode captured by an exemplar row.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(u8)]
pub enum ExemplarResponseModeV1 {
    /// Default direct answer mode.
    Direct = 1,
    /// Comparative answer mode.
    Compare = 2,
    /// Recommendation answer mode.
    Recommend = 3,
    /// Concise summary mode.
    Summarize = 4,
    /// Explanatory mode.
    Explain = 5,
    /// Troubleshooting mode.
    Troubleshoot = 6,
    /// Clarifier-first mode.
    Clarify = 7,
    /// Continue/follow-up mode.
    Continue = 8,
}

impl ExemplarResponseModeV1 {
    fn from_u8(v: u8) -> Result<Self, DecodeError> {
        match v {
            1 => Ok(ExemplarResponseModeV1::Direct),
            2 => Ok(ExemplarResponseModeV1::Compare),
            3 => Ok(ExemplarResponseModeV1::Recommend),
            4 => Ok(ExemplarResponseModeV1::Summarize),
            5 => Ok(ExemplarResponseModeV1::Explain),
            6 => Ok(ExemplarResponseModeV1::Troubleshoot),
            7 => Ok(ExemplarResponseModeV1::Clarify),
            8 => Ok(ExemplarResponseModeV1::Continue),
            _ => Err(DecodeError::new("bad ExemplarResponseModeV1")),
        }
    }
}

/// Structure shape captured by an exemplar row.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(u8)]
pub enum ExemplarStructureKindV1 {
    /// One main answer followed by optional support.
    Direct = 1,
    /// Summary-first presentation.
    SummaryFirst = 2,
    /// Step-by-step presentation.
    Steps = 3,
    /// Comparison presentation.
    Comparison = 4,
    /// Recommendation with next-step framing.
    Recommendation = 5,
    /// Clarifier-first structure.
    Clarifier = 6,
}

impl ExemplarStructureKindV1 {
    fn from_u8(v: u8) -> Result<Self, DecodeError> {
        match v {
            1 => Ok(ExemplarStructureKindV1::Direct),
            2 => Ok(ExemplarStructureKindV1::SummaryFirst),
            3 => Ok(ExemplarStructureKindV1::Steps),
            4 => Ok(ExemplarStructureKindV1::Comparison),
            5 => Ok(ExemplarStructureKindV1::Recommendation),
            6 => Ok(ExemplarStructureKindV1::Clarifier),
            _ => Err(DecodeError::new("bad ExemplarStructureKindV1")),
        }
    }
}

/// Tone class captured by an exemplar row.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(u8)]
pub enum ExemplarToneKindV1 {
    /// Neutral/default tone.
    Neutral = 1,
    /// Supportive/helpful tone.
    Supportive = 2,
    /// Direct/concise tone.
    Direct = 3,
    /// Cautious/caveated tone.
    Cautious = 4,
}

impl ExemplarToneKindV1 {
    fn from_u8(v: u8) -> Result<Self, DecodeError> {
        match v {
            1 => Ok(ExemplarToneKindV1::Neutral),
            2 => Ok(ExemplarToneKindV1::Supportive),
            3 => Ok(ExemplarToneKindV1::Direct),
            4 => Ok(ExemplarToneKindV1::Cautious),
            _ => Err(DecodeError::new("bad ExemplarToneKindV1")),
        }
    }
}

/// Supported offline source families for exemplar support refs.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(u8)]
pub enum ExemplarSupportSourceKindV1 {
    /// ReplayLog support.
    ReplayLog = 1,
    /// PromptPack support.
    PromptPack = 2,
    /// GoldenPack support.
    GoldenPack = 3,
    /// GoldenPackConversation support.
    GoldenPackConversation = 4,
    /// ConversationPack support.
    ConversationPack = 5,
    /// MarkovTrace support.
    MarkovTrace = 6,
}

impl ExemplarSupportSourceKindV1 {
    fn from_u8(v: u8) -> Result<Self, DecodeError> {
        match v {
            1 => Ok(ExemplarSupportSourceKindV1::ReplayLog),
            2 => Ok(ExemplarSupportSourceKindV1::PromptPack),
            3 => Ok(ExemplarSupportSourceKindV1::GoldenPack),
            4 => Ok(ExemplarSupportSourceKindV1::GoldenPackConversation),
            5 => Ok(ExemplarSupportSourceKindV1::ConversationPack),
            6 => Ok(ExemplarSupportSourceKindV1::MarkovTrace),
            _ => Err(DecodeError::new("bad ExemplarSupportSourceKindV1")),
        }
    }
}

/// One bounded provenance reference for an exemplar row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExemplarSupportRefV1 {
    /// Source family.
    pub source_kind: ExemplarSupportSourceKindV1,
    /// Content hash of the source artifact.
    pub source_hash: Hash32,
    /// Stable item index within the source artifact.
    pub item_ix: u32,
}

impl ExemplarSupportRefV1 {
    /// Construct a support ref.
    pub fn new(
        source_kind: ExemplarSupportSourceKindV1,
        source_hash: Hash32,
        item_ix: u32,
    ) -> Self {
        Self {
            source_kind,
            source_hash,
            item_ix,
        }
    }
}

/// One structure-only exemplar row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExemplarRowV1 {
    /// Stable exemplar id used for deterministic ordering and lookup.
    pub exemplar_id: Id64,
    /// Response mode represented by this row.
    pub response_mode: ExemplarResponseModeV1,
    /// Structure shape represented by this row.
    pub structure_kind: ExemplarStructureKindV1,
    /// Tone class represented by this row.
    pub tone_kind: ExemplarToneKindV1,
    /// Structure-only row flags.
    pub flags: ExemplarRowFlagsV1,
    /// Total deduplicated support count before per-row capping.
    pub support_count: u32,
    /// Bounded canonical support refs kept with the row.
    pub support_refs: Vec<ExemplarSupportRefV1>,
}

/// Canonical exemplar memory artifact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExemplarMemoryV1 {
    /// Schema version.
    pub version: u32,
    /// Stable build id for the artifact.
    pub build_id: Hash32,
    /// Artifact-level source-family flags.
    pub flags: ExemplarMemoryFlagsV1,
    /// Canonical exemplar rows.
    pub rows: Vec<ExemplarRowV1>,
}

/// Validation errors for ExemplarMemoryV1.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExemplarMemoryError {
    /// Unsupported schema version.
    BadVersion,
    /// Artifact flags contain unknown bits.
    BadFlags,
    /// Too many rows.
    TooManyRows,
    /// Rows are not canonical or contain duplicate exemplar ids.
    RowsNotCanonical,
    /// Row flags contain unknown bits.
    BadRowFlags,
    /// Too many support refs on a row.
    TooManySupportRefs,
    /// Support refs are not canonical or contain duplicates.
    SupportRefsNotCanonical,
    /// support_count is invalid for a row.
    BadSupportCount,
    /// Artifact flags do not cover the source families referenced by rows.
    SourceFlagsMismatch,
}

impl core::fmt::Display for ExemplarMemoryError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ExemplarMemoryError::BadVersion => f.write_str("bad exemplar memory version"),
            ExemplarMemoryError::BadFlags => f.write_str("bad exemplar memory flags"),
            ExemplarMemoryError::TooManyRows => f.write_str("too many exemplar rows"),
            ExemplarMemoryError::RowsNotCanonical => f.write_str("exemplar rows not canonical"),
            ExemplarMemoryError::BadRowFlags => f.write_str("bad exemplar row flags"),
            ExemplarMemoryError::TooManySupportRefs => {
                f.write_str("too many exemplar support refs")
            }
            ExemplarMemoryError::SupportRefsNotCanonical => {
                f.write_str("exemplar support refs not canonical")
            }
            ExemplarMemoryError::BadSupportCount => f.write_str("bad exemplar support count"),
            ExemplarMemoryError::SourceFlagsMismatch => {
                f.write_str("exemplar source flags do not match support refs")
            }
        }
    }
}

impl std::error::Error for ExemplarMemoryError {}

fn write_hash32(w: &mut ByteWriter, h: &Hash32) {
    w.write_raw(h);
}

fn read_hash32(r: &mut ByteReader<'_>) -> Result<Hash32, DecodeError> {
    let b = r.read_fixed(32)?;
    let mut out = [0u8; 32];
    out.copy_from_slice(b);
    Ok(out)
}

fn source_kind_flag(kind: ExemplarSupportSourceKindV1) -> ExemplarMemoryFlagsV1 {
    match kind {
        ExemplarSupportSourceKindV1::ReplayLog => EXMEM_FLAG_HAS_REPLAY_LOG,
        ExemplarSupportSourceKindV1::PromptPack => EXMEM_FLAG_HAS_PROMPT_PACK,
        ExemplarSupportSourceKindV1::GoldenPack => EXMEM_FLAG_HAS_GOLDEN_PACK,
        ExemplarSupportSourceKindV1::GoldenPackConversation => {
            EXMEM_FLAG_HAS_GOLDEN_PACK_CONVERSATION
        }
        ExemplarSupportSourceKindV1::ConversationPack => EXMEM_FLAG_HAS_CONVERSATION_PACK,
        ExemplarSupportSourceKindV1::MarkovTrace => EXMEM_FLAG_HAS_MARKOV_TRACE,
    }
}

fn cmp_support_ref_canon(
    a: &ExemplarSupportRefV1,
    b: &ExemplarSupportRefV1,
) -> core::cmp::Ordering {
    match (a.source_kind as u8).cmp(&(b.source_kind as u8)) {
        core::cmp::Ordering::Equal => {}
        o => return o,
    }
    match a.source_hash.cmp(&b.source_hash) {
        core::cmp::Ordering::Equal => {}
        o => return o,
    }
    a.item_ix.cmp(&b.item_ix)
}

fn support_refs_are_canon(xs: &[ExemplarSupportRefV1]) -> bool {
    if xs.is_empty() {
        return true;
    }
    let mut prev = &xs[0];
    for x in xs.iter().skip(1) {
        match cmp_support_ref_canon(prev, x) {
            core::cmp::Ordering::Less => {}
            _ => return false,
        }
        prev = x;
    }
    true
}

fn cmp_row_canon(a: &ExemplarRowV1, b: &ExemplarRowV1) -> core::cmp::Ordering {
    match b.support_count.cmp(&a.support_count) {
        core::cmp::Ordering::Equal => {}
        o => return o,
    }
    match (a.response_mode as u8).cmp(&(b.response_mode as u8)) {
        core::cmp::Ordering::Equal => {}
        o => return o,
    }
    match (a.structure_kind as u8).cmp(&(b.structure_kind as u8)) {
        core::cmp::Ordering::Equal => {}
        o => return o,
    }
    match (a.tone_kind as u8).cmp(&(b.tone_kind as u8)) {
        core::cmp::Ordering::Equal => {}
        o => return o,
    }
    a.exemplar_id.0.cmp(&b.exemplar_id.0)
}

fn rows_are_canon(xs: &[ExemplarRowV1]) -> bool {
    if xs.is_empty() {
        return true;
    }
    let mut prev = &xs[0];
    for x in xs.iter().skip(1) {
        match cmp_row_canon(prev, x) {
            core::cmp::Ordering::Less => {}
            _ => return false,
        }
        prev = x;
    }
    true
}

impl ExemplarMemoryV1 {
    /// Validate invariants.
    pub fn validate(&self) -> Result<(), ExemplarMemoryError> {
        if self.version != EXEMPLAR_MEMORY_V1_VERSION {
            return Err(ExemplarMemoryError::BadVersion);
        }
        if (self.flags & !EXMEM_FLAGS_V1_ALL) != 0 {
            return Err(ExemplarMemoryError::BadFlags);
        }
        if self.rows.len() > EXEMPLAR_MEMORY_V1_MAX_ROWS {
            return Err(ExemplarMemoryError::TooManyRows);
        }
        if !rows_are_canon(&self.rows) {
            return Err(ExemplarMemoryError::RowsNotCanonical);
        }

        let mut seen_ids: Vec<u64> = Vec::with_capacity(self.rows.len());
        let mut required_flags = 0u32;
        for row in &self.rows {
            if (row.flags & !EXROW_FLAGS_V1_ALL) != 0 {
                return Err(ExemplarMemoryError::BadRowFlags);
            }
            if row.support_refs.len() > EXEMPLAR_MEMORY_V1_MAX_SUPPORT_REFS {
                return Err(ExemplarMemoryError::TooManySupportRefs);
            }
            if row.support_count == 0 || row.support_count < (row.support_refs.len() as u32) {
                return Err(ExemplarMemoryError::BadSupportCount);
            }
            if !support_refs_are_canon(&row.support_refs) {
                return Err(ExemplarMemoryError::SupportRefsNotCanonical);
            }
            match seen_ids.binary_search(&row.exemplar_id.0) {
                Ok(_) => return Err(ExemplarMemoryError::RowsNotCanonical),
                Err(pos) => seen_ids.insert(pos, row.exemplar_id.0),
            }
            for sr in &row.support_refs {
                required_flags |= source_kind_flag(sr.source_kind);
            }
        }
        if required_flags & !self.flags != 0 {
            return Err(ExemplarMemoryError::SourceFlagsMismatch);
        }
        Ok(())
    }

    /// Return true if the record is in canonical row/support-ref order.
    pub fn is_canonical(&self) -> bool {
        rows_are_canon(&self.rows)
            && self
                .rows
                .iter()
                .all(|r| support_refs_are_canon(&r.support_refs))
    }

    /// Encode to canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        self.validate().map_err(|e| match e {
            ExemplarMemoryError::BadVersion => EncodeError::new("bad exemplar memory version"),
            ExemplarMemoryError::BadFlags => EncodeError::new("bad exemplar memory flags"),
            ExemplarMemoryError::TooManyRows => EncodeError::new("too many exemplar rows"),
            ExemplarMemoryError::RowsNotCanonical => {
                EncodeError::new("exemplar rows not canonical")
            }
            ExemplarMemoryError::BadRowFlags => EncodeError::new("bad exemplar row flags"),
            ExemplarMemoryError::TooManySupportRefs => {
                EncodeError::new("too many exemplar support refs")
            }
            ExemplarMemoryError::SupportRefsNotCanonical => {
                EncodeError::new("exemplar support refs not canonical")
            }
            ExemplarMemoryError::BadSupportCount => EncodeError::new("bad exemplar support count"),
            ExemplarMemoryError::SourceFlagsMismatch => {
                EncodeError::new("exemplar source flags do not match support refs")
            }
        })?;

        let mut cap = 4 + 32 + 4 + 4;
        for row in &self.rows {
            cap += 8 + 1 + 1 + 1 + 1 + 4 + 4 + 4;
            cap += row.support_refs.len() * (1 + 3 + 32 + 4);
        }
        let mut w = ByteWriter::with_capacity(cap);
        w.write_u32(self.version);
        write_hash32(&mut w, &self.build_id);
        w.write_u32(self.flags);
        w.write_u32(self.rows.len() as u32);
        for row in &self.rows {
            w.write_u64(row.exemplar_id.0);
            w.write_u8(row.response_mode as u8);
            w.write_u8(row.structure_kind as u8);
            w.write_u8(row.tone_kind as u8);
            w.write_u8(row.support_refs.len() as u8);
            w.write_u32(row.flags);
            w.write_u32(row.support_count);
            w.write_u32(0);
            for sr in &row.support_refs {
                w.write_u8(sr.source_kind as u8);
                w.write_u8(0);
                w.write_u8(0);
                w.write_u8(0);
                write_hash32(&mut w, &sr.source_hash);
                w.write_u32(sr.item_ix);
            }
        }
        Ok(w.into_bytes())
    }

    /// Decode from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let version = r.read_u32()?;
        let build_id = read_hash32(&mut r)?;
        let flags = r.read_u32()?;
        let row_count = r.read_u32()? as usize;
        let mut rows = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            let exemplar_id = Id64(r.read_u64()?);
            let response_mode = ExemplarResponseModeV1::from_u8(r.read_u8()?)?;
            let structure_kind = ExemplarStructureKindV1::from_u8(r.read_u8()?)?;
            let tone_kind = ExemplarToneKindV1::from_u8(r.read_u8()?)?;
            let support_ref_count = r.read_u8()? as usize;
            let row_flags = r.read_u32()?;
            let support_count = r.read_u32()?;
            let _reserved = r.read_u32()?;
            let mut support_refs = Vec::with_capacity(support_ref_count);
            for _ in 0..support_ref_count {
                let source_kind = ExemplarSupportSourceKindV1::from_u8(r.read_u8()?)?;
                let _ = r.read_u8()?;
                let _ = r.read_u8()?;
                let _ = r.read_u8()?;
                let source_hash = read_hash32(&mut r)?;
                let item_ix = r.read_u32()?;
                support_refs.push(ExemplarSupportRefV1 {
                    source_kind,
                    source_hash,
                    item_ix,
                });
            }
            rows.push(ExemplarRowV1 {
                exemplar_id,
                response_mode,
                structure_kind,
                tone_kind,
                flags: row_flags,
                support_count,
                support_refs,
            });
        }
        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }
        let out = Self {
            version,
            build_id,
            flags,
            rows,
        };
        out.validate()
            .map_err(|_| DecodeError::new("invalid exemplar memory"))?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::blake3_hash;

    fn sample_row() -> ExemplarRowV1 {
        ExemplarRowV1 {
            exemplar_id: Id64(11),
            response_mode: ExemplarResponseModeV1::Explain,
            structure_kind: ExemplarStructureKindV1::Steps,
            tone_kind: ExemplarToneKindV1::Supportive,
            flags: EXROW_FLAG_HAS_STEPS,
            support_count: 2,
            support_refs: vec![
                ExemplarSupportRefV1::new(
                    ExemplarSupportSourceKindV1::ReplayLog,
                    blake3_hash(b"replay"),
                    0,
                ),
                ExemplarSupportRefV1::new(
                    ExemplarSupportSourceKindV1::MarkovTrace,
                    blake3_hash(b"trace"),
                    1,
                ),
            ],
        }
    }

    fn sample_memory() -> ExemplarMemoryV1 {
        ExemplarMemoryV1 {
            version: EXEMPLAR_MEMORY_V1_VERSION,
            build_id: blake3_hash(b"build"),
            flags: EXMEM_FLAG_HAS_REPLAY_LOG | EXMEM_FLAG_HAS_MARKOV_TRACE,
            rows: vec![sample_row()],
        }
    }

    #[test]
    fn exemplar_memory_roundtrip() {
        let m1 = sample_memory();
        let bytes = m1.encode().expect("encode");
        let m2 = ExemplarMemoryV1::decode(&bytes).expect("decode");
        assert_eq!(m1, m2);
    }

    #[test]
    fn reject_non_canonical_row_order() {
        let mut low = sample_row();
        low.exemplar_id = Id64(7);
        low.support_count = 1;
        let mut high = sample_row();
        high.exemplar_id = Id64(5);
        high.support_count = 3;
        let m = ExemplarMemoryV1 {
            version: EXEMPLAR_MEMORY_V1_VERSION,
            build_id: blake3_hash(b"build"),
            flags: EXMEM_FLAG_HAS_REPLAY_LOG | EXMEM_FLAG_HAS_MARKOV_TRACE,
            rows: vec![low, high],
        };
        assert_eq!(m.validate(), Err(ExemplarMemoryError::RowsNotCanonical));
    }

    #[test]
    fn reject_non_canonical_support_refs() {
        let mut row = sample_row();
        row.support_refs.swap(0, 1);
        let m = ExemplarMemoryV1 {
            version: EXEMPLAR_MEMORY_V1_VERSION,
            build_id: blake3_hash(b"build"),
            flags: EXMEM_FLAG_HAS_REPLAY_LOG | EXMEM_FLAG_HAS_MARKOV_TRACE,
            rows: vec![row],
        };
        assert_eq!(
            m.validate(),
            Err(ExemplarMemoryError::SupportRefsNotCanonical)
        );
    }

    #[test]
    fn reject_duplicate_exemplar_ids() {
        let a = sample_row();
        let mut b = sample_row();
        b.support_refs.truncate(1);
        b.support_count = 1;
        let m = ExemplarMemoryV1 {
            version: EXEMPLAR_MEMORY_V1_VERSION,
            build_id: blake3_hash(b"build"),
            flags: EXMEM_FLAG_HAS_REPLAY_LOG | EXMEM_FLAG_HAS_MARKOV_TRACE,
            rows: vec![a, b],
        };
        assert_eq!(m.validate(), Err(ExemplarMemoryError::RowsNotCanonical));
    }
}
