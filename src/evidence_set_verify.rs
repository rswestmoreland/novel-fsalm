// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! EvidenceSet verifiers.
//!
//! These verifiers are intentionally small and rules-first:
//! - ensure each claim item has at least one evidence ref
//! - ensure each referenced (segment_id,row_ix) resolves to a real frame row
//! - ensure the referenced EvidenceBundle artifact exists
//!
//! The primary goal is to provide deterministic trace sanity checks for
//! evaluation and regression harnesses.

use crate::artifact::ArtifactStore;
use crate::evidence_artifact::{get_evidence_bundle_v1, EvidenceArtifactError};
use crate::evidence_set::EvidenceSetV1;
use crate::frame_store::{get_frame_segment_v1, FrameStoreError};
use crate::hash::Hash32;
use std::collections::HashMap;

/// Deterministic verifier errors.
#[derive(Debug)]
pub enum EvidenceSetVerifyError {
    /// EvidenceSet has zero items.
    EmptyItems,
    /// A claim item has zero evidence refs.
    EmptyEvidenceRefs {
        /// Claim id whose item has an empty evidence_refs list.
        claim_id: u32,
    },
    /// The referenced EvidenceBundle artifact is missing.
    EvidenceBundleNotFound {
        /// Evidence bundle hash referenced by the evidence set.
        evidence_bundle_id: Hash32,
    },
    /// Loading/decoding the referenced EvidenceBundle failed.
    EvidenceBundleLoad {
        /// Evidence bundle hash referenced by the evidence set.
        evidence_bundle_id: Hash32,
        /// Underlying artifact decode/store error.
        err: EvidenceArtifactError,
    },
    /// A referenced FrameSegment artifact is missing.
    FrameSegmentNotFound {
        /// Frame segment hash referenced by an evidence row ref.
        segment_id: Hash32,
    },
    /// Loading/decoding a referenced FrameSegment failed.
    FrameSegmentLoad {
        /// Frame segment hash referenced by an evidence row ref.
        segment_id: Hash32,
        /// Underlying frame segment decode/store error.
        err: FrameStoreError,
    },
    /// A referenced row index is out of range.
    FrameRowOutOfRange {
        /// Frame segment hash referenced by an evidence row ref.
        segment_id: Hash32,
        /// Row index referenced by an evidence row ref.
        row_ix: u32,
        /// Total number of rows in the referenced frame segment.
        row_count: u64,
    },
}

impl EvidenceSetVerifyError {
    /// Deterministic string code for this verifier error.
    pub fn code(&self) -> &'static str {
        match self {
            EvidenceSetVerifyError::EmptyItems => "V000",
            EvidenceSetVerifyError::EmptyEvidenceRefs { .. } => "V001",
            EvidenceSetVerifyError::EvidenceBundleNotFound { .. } => "V010",
            EvidenceSetVerifyError::EvidenceBundleLoad { .. } => "V011",
            EvidenceSetVerifyError::FrameSegmentNotFound { .. } => "V020",
            EvidenceSetVerifyError::FrameSegmentLoad { .. } => "V021",
            EvidenceSetVerifyError::FrameRowOutOfRange { .. } => "V022",
        }
    }
}

impl core::fmt::Display for EvidenceSetVerifyError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            EvidenceSetVerifyError::EmptyItems => write!(f, "{} evidence_set.items is empty", self.code()),
            EvidenceSetVerifyError::EmptyEvidenceRefs { claim_id } => {
                write!(f, "{} claim {} has empty evidence_refs", self.code(), claim_id)
            }
            EvidenceSetVerifyError::EvidenceBundleNotFound { evidence_bundle_id } => {
                write!(f, "{} evidence bundle not found: {}", self.code(), crate::hash::hex32(evidence_bundle_id))
            }
            EvidenceSetVerifyError::EvidenceBundleLoad { evidence_bundle_id, err } => {
                write!(f, "{} evidence bundle load failed {}: {}", self.code(), crate::hash::hex32(evidence_bundle_id), err)
            }
            EvidenceSetVerifyError::FrameSegmentNotFound { segment_id } => {
                write!(f, "{} frame segment not found: {}", self.code(), crate::hash::hex32(segment_id))
            }
            EvidenceSetVerifyError::FrameSegmentLoad { segment_id, err } => {
                write!(f, "{} frame segment load failed {}: {}", self.code(), crate::hash::hex32(segment_id), err)
            }
            EvidenceSetVerifyError::FrameRowOutOfRange { segment_id, row_ix, row_count } => {
                write!(f, "{} frame row out of range {} row_ix={} row_count={}", self.code(), crate::hash::hex32(segment_id), row_ix, row_count)
            }
        }
    }
}

impl std::error::Error for EvidenceSetVerifyError {}

/// Verify an EvidenceSetV1 against the artifact store.
///
/// Checks:
/// - items is non-empty
/// - each item has at least one evidence ref
/// - referenced EvidenceBundle exists and decodes
/// - each referenced (segment_id,row_ix) is in range for that segment
pub fn verify_evidence_set_v1<S: ArtifactStore>(
    store: &S,
    set: &EvidenceSetV1,
) -> Result<(), EvidenceSetVerifyError> {
    if set.items.is_empty() {
        return Err(EvidenceSetVerifyError::EmptyItems);
    }

    match get_evidence_bundle_v1(store, &set.evidence_bundle_id) {
        Ok(Some(_)) => {}
        Ok(None) => {
            return Err(EvidenceSetVerifyError::EvidenceBundleNotFound {
                evidence_bundle_id: set.evidence_bundle_id,
            })
        }
        Err(e) => {
            return Err(EvidenceSetVerifyError::EvidenceBundleLoad {
                evidence_bundle_id: set.evidence_bundle_id,
                err: e,
            })
        }
    }

    let mut seg_rows: HashMap<Hash32, u64> = HashMap::new();

    for it in set.items.iter() {
        if it.evidence_refs.is_empty() {
            return Err(EvidenceSetVerifyError::EmptyEvidenceRefs { claim_id: it.claim_id });
        }

        for r in it.evidence_refs.iter() {
            let row_count = if let Some(rc) = seg_rows.get(&r.segment_id) {
                *rc
            } else {
                match get_frame_segment_v1(store, &r.segment_id) {
                    Ok(Some(seg)) => {
                        let rc = seg.row_count();
                        seg_rows.insert(r.segment_id, rc);
                        rc
                    }
                    Ok(None) => {
                        return Err(EvidenceSetVerifyError::FrameSegmentNotFound {
                            segment_id: r.segment_id,
                        })
                    }
                    Err(e) => {
                        return Err(EvidenceSetVerifyError::FrameSegmentLoad {
                            segment_id: r.segment_id,
                            err: e,
                        })
                    }
                }
            };

            if (r.row_ix as u64) >= row_count {
                return Err(EvidenceSetVerifyError::FrameRowOutOfRange {
                    segment_id: r.segment_id,
                    row_ix: r.row_ix,
                    row_count,
                });
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
    use crate::evidence_set::{EvidenceRowRefV1, EvidenceSetItemV1};
    use crate::frame::{DocId, FrameRowV1, Id64, SourceId};
    use crate::frame_segment::FrameSegmentV1;
    use crate::frame_store::put_frame_segment_v1;
    use crate::evidence_artifact::put_evidence_bundle_v1;
    use crate::evidence_bundle::{EvidenceBundleV1, EvidenceItemDataV1, EvidenceItemV1, EvidenceLimitsV1, FrameRowRefV1};
    use std::sync::atomic::{AtomicUsize, Ordering};

    static TMP_DIR_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        let n = TMP_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
        let suffix = format!(
            "fsa_lm_evset_verify_{}_{}_{}",
            name,
            std::process::id(),
            n
        );

        let mut bases: Vec<std::path::PathBuf> = Vec::new();
        bases.push(std::env::temp_dir());
        if let Ok(cwd) = std::env::current_dir() {
            bases.push(cwd.join("target").join("fsa_lm_test_tmp"));
        }

        let mut last_err: Option<std::io::Error> = None;
        for base in bases {
            let p = base.join(&suffix);
            let _ = std::fs::remove_dir_all(&p);
            match std::fs::create_dir_all(&p) {
                Ok(()) => return p,
                Err(e) => last_err = Some(e),
            }
        }

        panic!(
            "tmp_dir create_dir_all failed for all candidates: {:?}",
            last_err
        );
    }

    fn build_min_store_with_one_row() -> (FsArtifactStore, Hash32, Hash32) {
        let root = tmp_dir("one_row");
        let store = FsArtifactStore::new(root.join("store")).unwrap();

        let mut row = FrameRowV1::new(DocId(Id64(1)), SourceId(Id64(1)));
        row.recompute_doc_len();
        let seg = FrameSegmentV1::from_rows(&[row], 1024).unwrap();
        let seg_id = put_frame_segment_v1(&store, &seg).unwrap();

        let limits = EvidenceLimitsV1 {
            segments_touched: 1,
            max_items: 1,
            max_bytes: 4096,
        };
        let mut bundle = EvidenceBundleV1::new([0u8; 32], [0u8; 32], limits, 0);
        bundle.items.push(EvidenceItemV1 {
            score: 1,
            data: EvidenceItemDataV1::Frame(FrameRowRefV1 {
                segment_id: seg_id,
                row_ix: 0,
                sketch: None,
            }),
        });
        bundle.canonicalize_in_place().unwrap();
        let bundle_id = put_evidence_bundle_v1(&store, &bundle).unwrap();

        (store, seg_id, bundle_id)
    }

    #[test]
    fn verify_rejects_empty_evidence_refs() {
        let (store, _seg_id, bundle_id) = build_min_store_with_one_row();
        let set = EvidenceSetV1 {
            version: 1,
            evidence_bundle_id: bundle_id,
            items: vec![EvidenceSetItemV1 {
                claim_id: 1,
                claim_text: "x".to_string(),
                evidence_refs: vec![],
            }],
        };
        let got = verify_evidence_set_v1(&store, &set);
        assert!(matches!(got, Err(EvidenceSetVerifyError::EmptyEvidenceRefs { .. })));
    }

    #[test]
    fn verify_rejects_out_of_range_row() {
        let (store, seg_id, bundle_id) = build_min_store_with_one_row();
        let set = EvidenceSetV1 {
            version: 1,
            evidence_bundle_id: bundle_id,
            items: vec![EvidenceSetItemV1 {
                claim_id: 1,
                claim_text: "x".to_string(),
                evidence_refs: vec![EvidenceRowRefV1 {
                    segment_id: seg_id,
                    row_ix: 9,
                    score: 1,
                }],
            }],
        };
        let got = verify_evidence_set_v1(&store, &set);
        assert!(matches!(got, Err(EvidenceSetVerifyError::FrameRowOutOfRange { .. })));
    }

    #[test]
    fn verify_accepts_minimal_ok_set() {
        let (store, seg_id, bundle_id) = build_min_store_with_one_row();
        let set = EvidenceSetV1 {
            version: 1,
            evidence_bundle_id: bundle_id,
            items: vec![EvidenceSetItemV1 {
                claim_id: 1,
                claim_text: "x".to_string(),
                evidence_refs: vec![EvidenceRowRefV1 {
                    segment_id: seg_id,
                    row_ix: 0,
                    score: 1,
                }],
            }],
        };
        verify_evidence_set_v1(&store, &set).unwrap();
    }
}
