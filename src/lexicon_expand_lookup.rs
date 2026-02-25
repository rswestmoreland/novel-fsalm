// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Lexicon snapshot read helpers for query expansion.
//!
//! This module loads LexiconSnapshot + LexiconSegment artifacts and provides
//! small deterministic lookups intended for later query expansion rules.
//!
//! Returned lists are deterministic:
//! - lemma_ids_for_key returns lemma ids sorted ascending.
//! - related_lemmas_from_lemma / related_lemmas_from_sense return lemma ids
//! sorted ascending and unique.

use crate::artifact::ArtifactStore;
use crate::hash::Hash32;
use crate::lexicon::{LemmaId, LemmaKeyId, RelTypeId, SenseId};
use crate::lexicon_segment::LexiconSegmentV1;
use crate::lexicon_segment_store::{get_lexicon_segment_v1, LexiconSegmentStoreError};
use crate::lexicon_snapshot_store::{get_lexicon_snapshot_v1, LexiconSnapshotStoreError};

use std::sync::Arc;

/// Errors for building a LexiconExpandLookupV1.
#[derive(Debug)]
pub enum LexiconExpandLookupError {
    /// Failed to load or decode a LexiconSnapshot.
    Snapshot(LexiconSnapshotStoreError),
    /// Failed to load or decode a LexiconSegment.
    Segment(LexiconSegmentStoreError),
    /// Snapshot referenced a LexiconSegment that was not present in the store.
    MissingSegment(Hash32),
}

impl core::fmt::Display for LexiconExpandLookupError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            LexiconExpandLookupError::Snapshot(e) => write!(f, "snapshot: {}", e),
            LexiconExpandLookupError::Segment(e) => write!(f, "segment: {}", e),
            LexiconExpandLookupError::MissingSegment(h) => write!(f, "missing lexicon segment: {}", crate::hash::hex32(h)),
        }
    }
}

impl std::error::Error for LexiconExpandLookupError {}

/// A loaded lexicon segment reference for expansion lookups.
#[derive(Clone, Debug)]
pub struct LexiconSegRefV1 {
    /// LexiconSegment artifact hash.
    pub lex_seg: Hash32,
    /// Decoded segment bytes.
    pub seg: Arc<LexiconSegmentV1>,
}

/// In-memory lookup view for lexicon-driven expansion.
#[derive(Clone, Debug)]
pub struct LexiconExpandLookupV1 {
    /// LexiconSnapshot hash used to load the view.
    pub lex_snapshot: Hash32,
    /// Segments in snapshot order (lex_seg hash ascending by snapshot invariants).
    pub segments: Vec<LexiconSegRefV1>,

    // Sorted by (lemma_key_id, lemma_id) ascending.
    lemma_key_pairs: Vec<(LemmaKeyId, LemmaId)>,
}

impl LexiconExpandLookupV1 {
    /// Lookup lemma ids that match a lemma_key_id.
    pub fn lemma_ids_for_key(&self, key: LemmaKeyId, cap: usize) -> Vec<LemmaId> {
        if cap == 0 {
            return Vec::new();
        }
        let xs = &self.lemma_key_pairs;
        let mut lo: usize = 0;
        let mut hi: usize = xs.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            if xs[mid].0 < key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let mut out: Vec<LemmaId> = Vec::new();
        let mut i = lo;
        while i < xs.len() {
            if xs[i].0 != key {
                break;
            }
            out.push(xs[i].1);
            if out.len() >= cap {
                break;
            }
            i += 1;
        }
        out
    }

    /// Collect related lemma ids from a lemma-origin edge list.
    pub fn related_lemmas_from_lemma(&self, from: LemmaId, rel_type: RelTypeId, cap: usize) -> Vec<LemmaId> {
        if cap == 0 {
            return Vec::new();
        }
        let from_tag: u8 = 0;
        let from_id: u64 = (from.0).0;
        self.collect_related(from_tag, from_id, rel_type, cap)
    }

    /// Collect related lemma ids from a sense-origin edge list.
    pub fn related_lemmas_from_sense(&self, from: SenseId, rel_type: RelTypeId, cap: usize) -> Vec<LemmaId> {
        if cap == 0 {
            return Vec::new();
        }
        let from_tag: u8 = 1;
        let from_id: u64 = (from.0).0;
        self.collect_related(from_tag, from_id, rel_type, cap)
    }

    fn collect_related(&self, from_tag: u8, from_id: u64, rel_type: RelTypeId, cap: usize) -> Vec<LemmaId> {
        let raw_cap = cap.saturating_mul(8).max(cap);
        let mut tmp: Vec<LemmaId> = Vec::new();

        for s in &self.segments {
            if tmp.len() >= raw_cap {
                break;
            }
            collect_rel_to_lemmas(&s.seg, from_tag, from_id, rel_type, raw_cap - tmp.len(), &mut tmp);
        }

        tmp.sort_by(|a, b| ((a.0).0).cmp(&((b.0).0)));
        tmp.dedup();
        if tmp.len() > cap {
            tmp.truncate(cap);
        }
        tmp
    }
}

fn rel_key_at(seg: &LexiconSegmentV1, ix: usize) -> (u8, u64, u16, u64) {
    let tag = seg.rel_from_tag[ix];
    let from_id = seg.rel_from_id[ix];
    let rt = seg.rel_type_id[ix].0;
    let to = (seg.rel_to_lemma_id[ix].0).0;
    (tag, from_id, rt, to)
}

fn collect_rel_to_lemmas(
    seg: &LexiconSegmentV1,
    from_tag: u8,
    from_id: u64,
    rel_type: RelTypeId,
    cap: usize,
    out: &mut Vec<LemmaId>,
) {
    if cap == 0 {
        return;
    }
    let n = seg.rel_from_tag.len();
    if n == 0 {
        return;
    }
    let target = (from_tag, from_id, rel_type.0, 0u64);

    let mut lo: usize = 0;
    let mut hi: usize = n;
    while lo < hi {
        let mid = (lo + hi) / 2;
        let k = rel_key_at(seg, mid);
        if k < target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    let mut i = lo;
    while i < n {
        if out.len() >= cap {
            break;
        }
        if seg.rel_from_tag[i] != from_tag {
            break;
        }
        if seg.rel_from_id[i] != from_id {
            break;
        }
        if seg.rel_type_id[i] != rel_type {
            break;
        }
        out.push(seg.rel_to_lemma_id[i]);
        i += 1;
    }
}

/// Load lexicon artifacts and build an in-memory expansion lookup view.
///
/// Returns Ok(None) if the snapshot hash is not present in the store.
pub fn load_lexicon_expand_lookup_v1<S: ArtifactStore>(
    store: &S,
    snapshot_hash: &Hash32,
) -> Result<Option<LexiconExpandLookupV1>, LexiconExpandLookupError> {
    let snap_opt = get_lexicon_snapshot_v1(store, snapshot_hash)
        .map_err(LexiconExpandLookupError::Snapshot)?;
    let snap = match snap_opt {
        Some(s) => s,
        None => return Ok(None),
    };

    let mut segments: Vec<LexiconSegRefV1> = Vec::with_capacity(snap.entries.len());
    let mut lemma_key_pairs: Vec<(LemmaKeyId, LemmaId)> = Vec::new();

    for e in &snap.entries {
        let seg_opt = get_lexicon_segment_v1(store, &e.lex_seg)
            .map_err(LexiconExpandLookupError::Segment)?;
        let seg = match seg_opt {
            Some(s) => s,
            None => return Err(LexiconExpandLookupError::MissingSegment(e.lex_seg)),
        };
        if seg.lemma_id.len() != seg.lemma_key_id.len() {
            // This should be impossible if decode succeeded.
            return Err(LexiconExpandLookupError::Segment(LexiconSegmentStoreError::Decode(
                crate::codec::DecodeError::new("lemma column length mismatch"),
            )));
        }
        for i in 0..seg.lemma_id.len() {
            lemma_key_pairs.push((seg.lemma_key_id[i], seg.lemma_id[i]));
        }
        segments.push(LexiconSegRefV1 {
            lex_seg: e.lex_seg,
            seg: Arc::new(seg),
        });
    }

    lemma_key_pairs.sort_by(|a, b| match a.0.cmp(&b.0) {
        core::cmp::Ordering::Equal => a.1.cmp(&b.1),
        other => other,
    });

    Ok(Some(LexiconExpandLookupV1 {
        lex_snapshot: *snapshot_hash,
        segments,
        lemma_key_pairs,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::artifact::FsArtifactStore;
    use crate::frame::Id64;
    use crate::lexicon::{
        LemmaId, LemmaKeyId, LemmaRowV1, RelFromId, RelationEdgeRowV1, RelTypeId, SenseId,
        SenseRowV1, TextId, LEXICON_SCHEMA_V1, REL_RELATED, REL_SYNONYM,
    };
    use crate::lexicon_segment::LexiconSegmentV1;
    use crate::lexicon_segment_store::put_lexicon_segment_v1;
    use crate::lexicon_snapshot::{LexiconSnapshotEntryV1, LexiconSnapshotV1};
    use crate::lexicon_snapshot_store::put_lexicon_snapshot_v1;

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        use std::fs;
        let mut p = std::env::temp_dir();
        p.push("fsa_lm_tests");
        p.push(name);
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn build_sample_segment() -> LexiconSegmentV1 {
        let lemmas = vec![
            LemmaRowV1 {
                version: LEXICON_SCHEMA_V1,
                lemma_id: LemmaId(Id64(10)),
                lemma_key_id: LemmaKeyId(Id64(100)),
                lemma_text_id: TextId(Id64(1000)),
                pos_mask: 0,
                flags: 0,
            },
            LemmaRowV1 {
                version: LEXICON_SCHEMA_V1,
                lemma_id: LemmaId(Id64(11)),
                lemma_key_id: LemmaKeyId(Id64(101)),
                lemma_text_id: TextId(Id64(1001)),
                pos_mask: 0,
                flags: 0,
            },
            LemmaRowV1 {
                version: LEXICON_SCHEMA_V1,
                lemma_id: LemmaId(Id64(12)),
                lemma_key_id: LemmaKeyId(Id64(101)),
                lemma_text_id: TextId(Id64(1002)),
                pos_mask: 0,
                flags: 0,
            },
        ];

        let senses = vec![SenseRowV1 {
            version: LEXICON_SCHEMA_V1,
            sense_id: SenseId(Id64(200)),
            lemma_id: LemmaId(Id64(10)),
            sense_rank: 0,
            gloss_text_id: TextId(Id64(2000)),
            labels_mask: 0,
        }];

        let rels = vec![
            RelationEdgeRowV1 {
                version: LEXICON_SCHEMA_V1,
                from: RelFromId::Lemma(LemmaId(Id64(10))),
                rel_type_id: REL_SYNONYM,
                to_lemma_id: LemmaId(Id64(11)),
            },
            RelationEdgeRowV1 {
                version: LEXICON_SCHEMA_V1,
                from: RelFromId::Lemma(LemmaId(Id64(10))),
                rel_type_id: REL_SYNONYM,
                to_lemma_id: LemmaId(Id64(12)),
            },
            RelationEdgeRowV1 {
                version: LEXICON_SCHEMA_V1,
                from: RelFromId::Sense(SenseId(Id64(200))),
                rel_type_id: REL_RELATED,
                to_lemma_id: LemmaId(Id64(11)),
            },
        ];

        LexiconSegmentV1::build_from_rows(&lemmas, &senses, &rels, &[]).unwrap()
    }

    #[test]
    fn lookup_lemma_ids_for_key_is_deterministic_and_capped() {
        let dir = tmp_dir("lexicon_expand_lookup_key");
        let store = FsArtifactStore::new(&dir).unwrap();

        let seg = build_sample_segment();
        let seg_h = put_lexicon_segment_v1(&store, &seg).unwrap();

        let mut snap = LexiconSnapshotV1::new();
        snap.entries.push(LexiconSnapshotEntryV1 {
            lex_seg: seg_h,
            lemma_count: seg.lemma_id.len() as u32,
            sense_count: seg.sense_id.len() as u32,
            rel_count: seg.rel_from_tag.len() as u32,
            pron_count: seg.pron_lemma_id.len() as u32,
        });
        let snap_h = put_lexicon_snapshot_v1(&store, &snap).unwrap();

        let view = load_lexicon_expand_lookup_v1(&store, &snap_h).unwrap().unwrap();

        let got = view.lemma_ids_for_key(LemmaKeyId(Id64(101)), 10);
        assert_eq!(got, vec![LemmaId(Id64(11)), LemmaId(Id64(12))]);

        let got2 = view.lemma_ids_for_key(LemmaKeyId(Id64(101)), 1);
        assert_eq!(got2, vec![LemmaId(Id64(11))]);
    }

    #[test]
    fn lookup_related_lemmas_is_sorted_unique_and_capped() {
        let dir = tmp_dir("lexicon_expand_lookup_rel");
        let store = FsArtifactStore::new(&dir).unwrap();

        let seg = build_sample_segment();
        let seg_h = put_lexicon_segment_v1(&store, &seg).unwrap();

        let mut snap = LexiconSnapshotV1::new();
        snap.entries.push(LexiconSnapshotEntryV1 {
            lex_seg: seg_h,
            lemma_count: seg.lemma_id.len() as u32,
            sense_count: seg.sense_id.len() as u32,
            rel_count: seg.rel_from_tag.len() as u32,
            pron_count: seg.pron_lemma_id.len() as u32,
        });
        let snap_h = put_lexicon_snapshot_v1(&store, &snap).unwrap();

        let view = load_lexicon_expand_lookup_v1(&store, &snap_h).unwrap().unwrap();

        let got = view.related_lemmas_from_lemma(LemmaId(Id64(10)), REL_SYNONYM, 10);
        assert_eq!(got, vec![LemmaId(Id64(11)), LemmaId(Id64(12))]);

        let got2 = view.related_lemmas_from_sense(SenseId(Id64(200)), REL_RELATED, 10);
        assert_eq!(got2, vec![LemmaId(Id64(11))]);

        let got3 = view.related_lemmas_from_lemma(LemmaId(Id64(10)), RelTypeId(999), 10);
        assert!(got3.is_empty());

        let got4 = view.related_lemmas_from_lemma(LemmaId(Id64(10)), REL_SYNONYM, 1);
        assert_eq!(got4, vec![LemmaId(Id64(11))]);
    }
}
