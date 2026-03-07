// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! CompactionReport artifact helpers.
//!
//! These helpers store and load CompactionReportV1 artifacts using the
//! content-addressed ArtifactStore.

use crate::artifact::{ArtifactError, ArtifactStore};
use crate::codec::{DecodeError, EncodeError};
use crate::compaction_report::CompactionReportV1;
use crate::hash::Hash32;

/// Store a CompactionReportV1 as an artifact.
pub fn put_compaction_report_v1<S: ArtifactStore>(
    store: &S,
    report: &CompactionReportV1,
) -> Result<Hash32, CompactionReportArtifactError> {
    let bytes = report.encode().map_err(CompactionReportArtifactError::Encode)?;
    store.put(&bytes).map_err(CompactionReportArtifactError::Store)
}

/// Load and decode a CompactionReportV1 artifact by hash.
///
/// Returns None if the artifact is not present in the store.
pub fn get_compaction_report_v1<S: ArtifactStore>(
    store: &S,
    hash: &Hash32,
) -> Result<Option<CompactionReportV1>, CompactionReportArtifactError> {
    let bytes_opt = store.get(hash).map_err(CompactionReportArtifactError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let rep = CompactionReportV1::decode(&bytes).map_err(CompactionReportArtifactError::Decode)?;
    Ok(Some(rep))
}

/// Errors for CompactionReport artifact helpers.
#[derive(Debug)]
pub enum CompactionReportArtifactError {
    /// Report encoding error.
    Encode(EncodeError),
    /// Report decoding error.
    Decode(DecodeError),
    /// Artifact store error.
    Store(ArtifactError),
}

impl core::fmt::Display for CompactionReportArtifactError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            CompactionReportArtifactError::Encode(e) => write!(f, "encode: {}", e),
            CompactionReportArtifactError::Decode(e) => write!(f, "decode: {}", e),
            CompactionReportArtifactError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl std::error::Error for CompactionReportArtifactError {}
