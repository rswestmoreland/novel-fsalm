// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Scale demo orchestration.
//!
//! Track C wires end-to-end "scale demos" to exercise the full pipeline
//! on deterministic synthetic data.
//!
//! scope: generate-only. This stage produces a stable workload hash and a
//! small summary report. Subsequent updates add ingestion, indexing, querying,
//! and answering.

use crate::artifact::{ArtifactError, ArtifactStore};
use crate::codec::{DecodeError, EncodeError};
use crate::frame::{derive_id64, SourceId};
use crate::frame_store::{get_frame_segment_v1, FrameStoreError};
use crate::hash::{blake3_hash, hex32, Hash32};
use crate::index_segment::{IndexBuildError, IndexSegmentV1};
use crate::index_sig_map::IndexSigMapV1;
use crate::index_sig_map_store::{put_index_sig_map_v1, IndexSigMapStoreError};
use crate::index_snapshot::{IndexSnapshotEntryV1, IndexSnapshotV1};
use crate::index_snapshot_store::{put_index_snapshot_v1, IndexSnapshotStoreError};
use crate::index_store::{put_index_segment_v1, IndexStoreError};
use crate::prompt_artifact::{put_prompt_pack, PromptArtifactError};
use crate::prompt_pack::{Message, PromptIds, PromptLimits, PromptPack, Role};
use crate::segment_sig::{SegmentSigBuildError, SegmentSigV1};
use crate::segment_sig_store::{put_segment_sig_v1, SegmentSigStoreError};
use crate::wiki_ingest::{ingest_wiki_tsv, WikiIngestCfg, WikiIngestError, WikiIngestManifestV1};
use crate::workload_gen::{
    generate_workload_v1, workload_hash_v1, WorkloadCfgV1, WorkloadGenError, WorkloadV1,
};

use crate::cache::{Cache2Q, CacheCfgV1};
use crate::evidence_artifact::{put_evidence_bundle_v1, EvidenceArtifactError};
use crate::evidence_builder::{
    build_evidence_bundle_v1_from_hits_cached, EvidenceBuildCfgV1, EvidenceBuildError,
};
use crate::evidence_bundle::EvidenceLimitsV1;
use crate::frame_segment::FrameSegmentV1;
use crate::index_query::{
    query_terms_from_text, search_snapshot_gated, IndexQueryError, QueryTermsCfg, SearchCfg,
};
use crate::scale_report::{
    HashListSummaryV1, ScaleDemoScaleReportV1, SCALE_DEMO_SCALE_REPORT_V1_VERSION,
};

use std::sync::Arc;

use std::env;
use std::io::{BufReader, Cursor};

/// Scale demo module version.
pub const SCALE_DEMO_V1_VERSION: u32 = 1;

/// Scale demo config.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScaleDemoCfgV1 {
    /// Schema version. Must equal `SCALE_DEMO_V1_VERSION`.
    pub version: u32,
    /// Underlying deterministic workload generator config.
    pub workload: WorkloadCfgV1,
}

impl Default for ScaleDemoCfgV1 {
    fn default() -> Self {
        Self {
            version: SCALE_DEMO_V1_VERSION,
            workload: WorkloadCfgV1::default(),
        }
    }
}

/// Config validation errors.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScaleDemoCfgError {
    /// Version mismatch.
    VersionMismatch,
    /// Workload config invalid.
    BadWorkloadCfg,
}

impl core::fmt::Display for ScaleDemoCfgError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ScaleDemoCfgError::VersionMismatch => f.write_str("scale demo cfg version mismatch"),
            ScaleDemoCfgError::BadWorkloadCfg => f.write_str("bad workload cfg"),
        }
    }
}

impl ScaleDemoCfgV1 {
    /// Validate config invariants.
    pub fn validate(&self) -> Result<(), ScaleDemoCfgError> {
        if self.version != SCALE_DEMO_V1_VERSION {
            return Err(ScaleDemoCfgError::VersionMismatch);
        }
        if self.workload.validate().is_err() {
            return Err(ScaleDemoCfgError::BadWorkloadCfg);
        }
        Ok(())
    }
}

/// Scale demo orchestration errors (generate-only path).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScaleDemoError {
    /// Config invalid.
    BadCfg(ScaleDemoCfgError),
    /// Workload generation failed.
    WorkloadGen(WorkloadGenError),
}

impl core::fmt::Display for ScaleDemoError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ScaleDemoError::BadCfg(e) => {
                f.write_str("bad scale demo cfg: ")?;
                core::fmt::Display::fmt(e, f)
            }
            ScaleDemoError::WorkloadGen(e) => {
                f.write_str("workload generation failed: ")?;
                core::fmt::Display::fmt(e, f)
            }
        }
    }
}

impl From<ScaleDemoCfgError> for ScaleDemoError {
    fn from(e: ScaleDemoCfgError) -> Self {
        ScaleDemoError::BadCfg(e)
    }
}

impl From<WorkloadGenError> for ScaleDemoError {
    fn from(e: WorkloadGenError) -> Self {
        ScaleDemoError::WorkloadGen(e)
    }
}

/// Scale demo generate-only report.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScaleDemoReportV1 {
    /// Schema version.
    pub version: u32,
    /// Deterministic workload hash.
    pub workload_hash: Hash32,
    /// Total number of docs.
    pub doc_count: u32,
    /// Total number of queries.
    pub query_count: u32,
    /// Optional tie pair enabled.
    pub tie_pair: bool,
    /// Seed used.
    pub seed: u64,
}

impl core::fmt::Display for ScaleDemoReportV1 {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "scale_demo_v1 workload_hash={} docs={} queries={} tie_pair={} seed={}",
            hex32(&self.workload_hash),
            self.doc_count,
            self.query_count,
            if self.tie_pair { 1 } else { 0 },
            self.seed
        )
    }
}

fn generate_workload_and_hash(cfg: ScaleDemoCfgV1) -> Result<(WorkloadV1, Hash32), ScaleDemoError> {
    cfg.validate()?;
    let w = generate_workload_v1(cfg.workload)?;
    let h = workload_hash_v1(&w);
    Ok((w, h))
}

/// Generate the scale demo report ().
pub fn run_scale_demo_generate_only_v1(
    cfg: ScaleDemoCfgV1,
) -> Result<ScaleDemoReportV1, ScaleDemoError> {
    let (w, h) = generate_workload_and_hash(cfg)?;
    Ok(ScaleDemoReportV1 {
        version: SCALE_DEMO_V1_VERSION,
        workload_hash: h,
        doc_count: w.docs.len() as u32,
        query_count: w.queries.len() as u32,
        tie_pair: cfg.workload.include_tie_pair != 0,
        seed: cfg.workload.seed,
    })
}

/// Frames ingest report from a scale demo workload ().
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScaleDemoFramesReportV1 {
    /// Schema version.
    pub version: u32,
    /// Hash of the stored ingest manifest.
    pub frame_manifest_hash: Hash32,
    /// Total docs ingested (from manifest).
    pub docs_total: u64,
    /// Total rows emitted (from manifest).
    pub rows_total: u64,
    /// Total segments stored (from manifest).
    pub segments_total: u32,
}

impl core::fmt::Display for ScaleDemoFramesReportV1 {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "scale_demo_frames_v1 manifest={} docs={} rows={} segments={}",
            hex32(&self.frame_manifest_hash),
            self.docs_total,
            self.rows_total,
            self.segments_total
        )
    }
}

/// Scale demo errors for generation + frame ingestion ().
#[derive(Debug)]
pub enum ScaleDemoIngestError {
    /// Generate-only path failed.
    Scale(ScaleDemoError),
    /// Ingest pipeline failed.
    Ingest(WikiIngestError),
    /// Store access failed.
    Store(ArtifactError),
    /// Manifest decode failed.
    Decode(DecodeError),
    /// Manifest expected but missing.
    MissingManifest,
}

impl core::fmt::Display for ScaleDemoIngestError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ScaleDemoIngestError::Scale(e) => {
                f.write_str("scale demo failed: ")?;
                core::fmt::Display::fmt(e, f)
            }
            ScaleDemoIngestError::Ingest(e) => {
                f.write_str("ingest failed: ")?;
                core::fmt::Display::fmt(e, f)
            }
            ScaleDemoIngestError::Store(e) => {
                f.write_str("store failed: ")?;
                core::fmt::Display::fmt(e, f)
            }
            ScaleDemoIngestError::Decode(e) => {
                f.write_str("decode failed: ")?;
                core::fmt::Display::fmt(e, f)
            }
            ScaleDemoIngestError::MissingManifest => f.write_str("manifest missing from store"),
        }
    }
}

impl From<ScaleDemoError> for ScaleDemoIngestError {
    fn from(e: ScaleDemoError) -> Self {
        ScaleDemoIngestError::Scale(e)
    }
}

impl From<WikiIngestError> for ScaleDemoIngestError {
    fn from(e: WikiIngestError) -> Self {
        ScaleDemoIngestError::Ingest(e)
    }
}

impl From<ArtifactError> for ScaleDemoIngestError {
    fn from(e: ArtifactError) -> Self {
        ScaleDemoIngestError::Store(e)
    }
}

impl From<DecodeError> for ScaleDemoIngestError {
    fn from(e: DecodeError) -> Self {
        ScaleDemoIngestError::Decode(e)
    }
}

/// Errors for manifest-driven helpers ().
#[derive(Debug)]
pub enum ScaleDemoManifestError {
    /// Store access failed.
    Store(ArtifactError),
    /// Manifest decode failed.
    Decode(DecodeError),
    /// Manifest expected but missing.
    MissingManifest,
}

impl core::fmt::Display for ScaleDemoManifestError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ScaleDemoManifestError::Store(e) => {
                f.write_str("store failed: ")?;
                core::fmt::Display::fmt(e, f)
            }
            ScaleDemoManifestError::Decode(e) => {
                f.write_str("decode failed: ")?;
                core::fmt::Display::fmt(e, f)
            }
            ScaleDemoManifestError::MissingManifest => f.write_str("manifest missing from store"),
        }
    }
}

impl From<ArtifactError> for ScaleDemoManifestError {
    fn from(e: ArtifactError) -> Self {
        ScaleDemoManifestError::Store(e)
    }
}

impl From<DecodeError> for ScaleDemoManifestError {
    fn from(e: DecodeError) -> Self {
        ScaleDemoManifestError::Decode(e)
    }
}

/// Load a Wiki ingest manifest from the store ().
pub fn load_wiki_ingest_manifest_v1<S: ArtifactStore>(
    store: &S,
    manifest_hash: &Hash32,
) -> Result<WikiIngestManifestV1, ScaleDemoManifestError> {
    let man_bytes_opt = store.get(manifest_hash)?;
    let man_bytes = match man_bytes_opt {
        Some(b) => b,
        None => return Err(ScaleDemoManifestError::MissingManifest),
    };
    Ok(WikiIngestManifestV1::decode(&man_bytes)?)
}

/// Collect the FrameSegment hashes referenced by a Wiki ingest manifest ().
pub fn collect_frame_segments_from_manifest_v1<S: ArtifactStore>(
    store: &S,
    manifest_hash: &Hash32,
) -> Result<Vec<Hash32>, ScaleDemoManifestError> {
    let man = load_wiki_ingest_manifest_v1(store, manifest_hash)?;
    Ok(man.segments)
}

// Index building from ingested frame manifests ().

const SCALE_DEMO_INDEX_V1_VERSION: u32 = 1;
const SCALE_DEMO_INDEX_BLOOM_BYTES: usize = 4096;
const SCALE_DEMO_INDEX_BLOOM_K: u8 = 6;

/// Report produced by building an index snapshot from an ingest manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScaleDemoIndexReportV1 {
    /// Schema version. Must equal `SCALE_DEMO_INDEX_V1_VERSION`.
    pub version: u32,
    /// Input ingest manifest hash.
    pub frame_manifest_hash: Hash32,
    /// Output IndexSnapshotV1 artifact hash.
    pub index_snapshot_hash: Hash32,
    /// Output IndexSigMapV1 artifact hash.
    pub index_sig_map_hash: Hash32,
    /// Total number of frame segments indexed.
    pub segments_total: u32,
}

impl core::fmt::Display for ScaleDemoIndexReportV1 {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "scale_demo_index_v1 manifest={} index_snapshot={} index_sig_map={} segments={}",
            hex32(&self.frame_manifest_hash),
            hex32(&self.index_snapshot_hash),
            hex32(&self.index_sig_map_hash),
            self.segments_total
        )
    }
}

/// Errors while building index artifacts for scale demos.
#[derive(Debug)]
pub enum ScaleDemoIndexError {
    /// Manifest helper error.
    Manifest(ScaleDemoManifestError),
    /// FrameSegment store helper error.
    FrameStore(FrameStoreError),
    /// A referenced FrameSegment artifact is missing.
    MissingFrameSegment(Hash32),
    /// Index build error.
    IndexBuild(IndexBuildError),
    /// IndexSegment store helper error.
    IndexStore(IndexStoreError),
    /// Segment signature build error.
    SegmentSigBuild(SegmentSigBuildError),
    /// Segment signature store helper error.
    SegmentSigStore(SegmentSigStoreError),
    /// IndexSigMap store helper error.
    IndexSigMapStore(IndexSigMapStoreError),
    /// Index snapshot store helper error.
    IndexSnapshotStore(IndexSnapshotStoreError),
}

impl core::fmt::Display for ScaleDemoIndexError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ScaleDemoIndexError::Manifest(e) => write!(f, "manifest: {}", e),
            ScaleDemoIndexError::FrameStore(e) => write!(f, "frame store: {}", e),
            ScaleDemoIndexError::MissingFrameSegment(h) => {
                write!(f, "missing frame segment: {}", hex32(h))
            }
            ScaleDemoIndexError::IndexBuild(e) => write!(f, "index build: {}", e),
            ScaleDemoIndexError::IndexStore(e) => write!(f, "index store: {}", e),
            ScaleDemoIndexError::SegmentSigBuild(e) => write!(f, "segment sig build: {}", e),
            ScaleDemoIndexError::SegmentSigStore(e) => write!(f, "segment sig store: {}", e),
            ScaleDemoIndexError::IndexSigMapStore(e) => write!(f, "index sig map store: {}", e),
            ScaleDemoIndexError::IndexSnapshotStore(e) => write!(f, "index snapshot store: {}", e),
        }
    }
}

impl std::error::Error for ScaleDemoIndexError {}

impl From<ScaleDemoManifestError> for ScaleDemoIndexError {
    fn from(e: ScaleDemoManifestError) -> Self {
        ScaleDemoIndexError::Manifest(e)
    }
}

impl From<FrameStoreError> for ScaleDemoIndexError {
    fn from(e: FrameStoreError) -> Self {
        ScaleDemoIndexError::FrameStore(e)
    }
}

impl From<IndexStoreError> for ScaleDemoIndexError {
    fn from(e: IndexStoreError) -> Self {
        ScaleDemoIndexError::IndexStore(e)
    }
}

impl From<SegmentSigStoreError> for ScaleDemoIndexError {
    fn from(e: SegmentSigStoreError) -> Self {
        ScaleDemoIndexError::SegmentSigStore(e)
    }
}

impl From<IndexSigMapStoreError> for ScaleDemoIndexError {
    fn from(e: IndexSigMapStoreError) -> Self {
        ScaleDemoIndexError::IndexSigMapStore(e)
    }
}

impl From<IndexSnapshotStoreError> for ScaleDemoIndexError {
    fn from(e: IndexSnapshotStoreError) -> Self {
        ScaleDemoIndexError::IndexSnapshotStore(e)
    }
}

/// Build IndexSegmentV1 + IndexSnapshotV1 + SegmentSigV1 + IndexSigMapV1 from a frame ingest manifest.
///
/// The output artifacts are content-addressed. Calling this function multiple times with the
/// same inputs should yield the same hashes and be idempotent.
pub fn run_scale_demo_build_index_from_manifest_v1<S: ArtifactStore>(
    store: &S,
    manifest_hash: &Hash32,
) -> Result<ScaleDemoIndexReportV1, ScaleDemoIndexError> {
    let man = load_wiki_ingest_manifest_v1(store, manifest_hash)?;
    let segs = man.segments;

    let mut entries: Vec<IndexSnapshotEntryV1> = Vec::with_capacity(segs.len());
    let mut sig_map = IndexSigMapV1::new(man.source_id);

    for frame_hash in &segs {
        let frame_opt = get_frame_segment_v1(store, frame_hash)?;
        let frame = match frame_opt {
            Some(s) => s,
            None => return Err(ScaleDemoIndexError::MissingFrameSegment(*frame_hash)),
        };

        let idx = IndexSegmentV1::build_from_segment(*frame_hash, &frame)
            .map_err(ScaleDemoIndexError::IndexBuild)?;
        let idx_hash = put_index_segment_v1(store, &idx)?;

        let terms: Vec<crate::frame::TermId> = idx.terms.iter().map(|e| e.term).collect();
        let sig = SegmentSigV1::build(
            idx_hash,
            &terms,
            SCALE_DEMO_INDEX_BLOOM_BYTES,
            SCALE_DEMO_INDEX_BLOOM_K,
        )
        .map_err(ScaleDemoIndexError::SegmentSigBuild)?;
        let sig_hash = put_segment_sig_v1(store, &sig)?;
        sig_map.push(idx_hash, sig_hash);

        entries.push(IndexSnapshotEntryV1 {
            frame_seg: *frame_hash,
            index_seg: idx_hash,
            row_count: idx.row_count,
            term_count: idx.terms.len() as u32,
            postings_bytes: idx.postings.len() as u32,
        });
    }

    let snap = IndexSnapshotV1 {
        version: 1,
        source_id: man.source_id,
        entries,
    };
    let snap_hash = put_index_snapshot_v1(store, &snap)?;
    let sig_map_hash = put_index_sig_map_v1(store, &sig_map)?;

    Ok(ScaleDemoIndexReportV1 {
        version: SCALE_DEMO_INDEX_V1_VERSION,
        frame_manifest_hash: *manifest_hash,
        index_snapshot_hash: snap_hash,
        index_sig_map_hash: sig_map_hash,
        segments_total: segs.len() as u32,
    })
}

/// PromptPack report version (scale demo).
pub const SCALE_DEMO_PROMPTS_V1_VERSION: u32 = 1;

/// Default max output tokens used when generating PromptPacks for scale demos.
pub const SCALE_DEMO_PROMPT_MAX_OUTPUT_TOKENS: u32 = 256;

/// Scale demo prompts report ().
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScaleDemoPromptsReportV1 {
    /// Schema version.
    pub version: u32,
    /// Deterministic workload hash.
    pub workload_hash: Hash32,
    /// Total number of queries.
    pub query_count: u32,
    /// Max output tokens embedded in each PromptPack.
    pub max_output_tokens: u32,
    /// PromptPack artifact hashes, in ascending query_id order.
    pub prompt_hashes: Vec<Hash32>,
}

impl core::fmt::Display for ScaleDemoPromptsReportV1 {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        if self.prompt_hashes.is_empty() {
            write!(
                f,
                "scale_demo_prompts_v1 workload_hash={} prompts=0 max_out={}",
                hex32(&self.workload_hash),
                self.max_output_tokens
            )
        } else {
            let first = self.prompt_hashes.first().unwrap();
            let last = self.prompt_hashes.last().unwrap();
            write!(
                f,
                "scale_demo_prompts_v1 workload_hash={} prompts={} max_out={} first={} last={}",
                hex32(&self.workload_hash),
                self.prompt_hashes.len(),
                self.max_output_tokens,
                hex32(first),
                hex32(last)
            )
        }
    }
}

/// Errors for prompt generation + persistence.
#[derive(Debug)]
pub enum ScaleDemoPromptsError {
    /// Scale demo config or workload generation failed.
    Scale(ScaleDemoError),
    /// PromptPack artifact store operation failed.
    Prompt(PromptArtifactError),
}

impl core::fmt::Display for ScaleDemoPromptsError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ScaleDemoPromptsError::Scale(e) => core::fmt::Display::fmt(e, f),
            ScaleDemoPromptsError::Prompt(e) => {
                f.write_str("prompt artifact: ")?;
                core::fmt::Display::fmt(e, f)
            }
        }
    }
}

impl From<ScaleDemoError> for ScaleDemoPromptsError {
    fn from(value: ScaleDemoError) -> Self {
        ScaleDemoPromptsError::Scale(value)
    }
}

impl From<PromptArtifactError> for ScaleDemoPromptsError {
    fn from(value: PromptArtifactError) -> Self {
        ScaleDemoPromptsError::Prompt(value)
    }
}

/// Generate and store one PromptPack per query.
///
/// Each PromptPack contains:
/// - a single User message with the query text
/// - a deterministic seed derived from (workload_seed + query_id)
/// - a stable constraint `scale_demo_query_id`
///
/// PromptPack ids are set to zero hashes for now; later stages may wire in real
/// snapshot ids.
pub fn run_scale_demo_generate_and_store_prompts_v1<S: ArtifactStore>(
    store: &S,
    cfg: ScaleDemoCfgV1,
) -> Result<ScaleDemoPromptsReportV1, ScaleDemoPromptsError> {
    let (w, h) = generate_workload_and_hash(cfg)?;

    let limits = PromptLimits::default_v1();
    let ids = PromptIds {
        snapshot_id: [0u8; 32],
        weights_id: [0u8; 32],
        tokenizer_id: [0u8; 32],
    };

    let mut hashes: Vec<Hash32> = Vec::with_capacity(w.queries.len());
    for q in &w.queries {
        let seed = cfg.workload.seed.wrapping_add(q.query_id as u64);
        let mut pack = PromptPack::new(seed, SCALE_DEMO_PROMPT_MAX_OUTPUT_TOKENS, ids);

        pack.messages.push(Message {
            role: Role::User,
            content: q.text.clone(),
        });

        pack.add_constraint("scale_demo_query_id", &q.query_id.to_string());

        let ph = put_prompt_pack(store, &mut pack, limits)?;
        hashes.push(ph);
    }

    Ok(ScaleDemoPromptsReportV1 {
        version: SCALE_DEMO_PROMPTS_V1_VERSION,
        workload_hash: h,
        query_count: w.queries.len() as u32,
        max_output_tokens: SCALE_DEMO_PROMPT_MAX_OUTPUT_TOKENS,
        prompt_hashes: hashes,
    })
}

fn push_u32_dec(out: &mut Vec<u8>, mut v: u32) {
    if v == 0 {
        out.push(b'0');
        return;
    }
    let mut tmp = [0u8; 10];
    let mut i = tmp.len();
    while v > 0 {
        let d = (v % 10) as u8;
        v /= 10;
        i -= 1;
        tmp[i] = b'0' + d;
    }
    out.extend_from_slice(&tmp[i..]);
}

fn workload_to_wiki_tsv_bytes(w: &WorkloadV1) -> Vec<u8> {
    // Format: title<TAB>text<NEWLINE>
    // Title is deterministic and unique per doc_id.
    let mut out = Vec::new();
    for d in &w.docs {
        out.extend_from_slice(b"doc_");
        push_u32_dec(&mut out, d.doc_id);
        out.push(b'\t');
        out.extend_from_slice(d.text.as_bytes());
        out.push(b'\n');
    }
    out
}

/// Generate workload, report, and persist docs as FrameSegments using the existing ingest pipeline ().
///
/// Returns the generate-only report plus a frames ingest report (manifest hash + counts).
pub fn run_scale_demo_generate_and_ingest_frames_v1<S: ArtifactStore>(
    store: &S,
    cfg: ScaleDemoCfgV1,
) -> Result<(ScaleDemoReportV1, ScaleDemoFramesReportV1), ScaleDemoIngestError> {
    let (w, h) = generate_workload_and_hash(cfg).map_err(ScaleDemoIngestError::Scale)?;

    let report = ScaleDemoReportV1 {
        version: SCALE_DEMO_V1_VERSION,
        workload_hash: h,
        doc_count: w.docs.len() as u32,
        query_count: w.queries.len() as u32,
        tie_pair: cfg.workload.include_tie_pair != 0,
        seed: cfg.workload.seed,
    };

    let mut icfg = WikiIngestCfg::default_v1();
    icfg.source_id = SourceId(derive_id64(b"source\0", b"scale_demo/workload_v1"));
    icfg.max_docs = Some(w.docs.len() as u64);

    let bytes = workload_to_wiki_tsv_bytes(&w);
    let rdr = BufReader::new(Cursor::new(bytes));

    let manifest_hash = ingest_wiki_tsv(store, rdr, icfg)?;

    let man_bytes_opt = store.get(&manifest_hash)?;
    let man_bytes = match man_bytes_opt {
        Some(b) => b,
        None => return Err(ScaleDemoIngestError::MissingManifest),
    };
    let man = WikiIngestManifestV1::decode(&man_bytes)?;

    let frames = ScaleDemoFramesReportV1 {
        version: 1,
        frame_manifest_hash: manifest_hash,
        docs_total: man.docs_total,
        rows_total: man.rows_total,
        segments_total: man.segments.len() as u32,
    };

    Ok((report, frames))
}

/// Evidence report version (scale demo).
pub const SCALE_DEMO_EVIDENCE_V1_VERSION: u32 = 1;

/// Default top-k used when building scale demo evidence bundles.
pub const SCALE_DEMO_EVIDENCE_K: usize = 16;

/// Default maximum encoded bytes per EvidenceBundle artifact (scale demo).
pub const SCALE_DEMO_EVIDENCE_MAX_BYTES: u32 = 64 * 1024;

/// Default byte budget for the in-process FrameSegment cache while building evidence.
pub const SCALE_DEMO_EVIDENCE_FRAME_CACHE_BYTES: u64 = 8 * 1024 * 1024;

fn env_u64(name: &str) -> Option<u64> {
    match env::var(name) {
        Ok(v) => v.parse::<u64>().ok(),
        Err(_) => None,
    }
}

fn scale_demo_evidence_k() -> usize {
    if let Some(v) = env_u64("FSA_LM_SCALE_DEMO_EVIDENCE_K") {
        if v == 0 {
            return SCALE_DEMO_EVIDENCE_K;
        }
        let cap = if v > 4096 { 4096 } else { v };
        return cap as usize;
    }
    SCALE_DEMO_EVIDENCE_K
}

fn scale_demo_evidence_max_bytes() -> u32 {
    if let Some(v) = env_u64("FSA_LM_SCALE_DEMO_EVIDENCE_MAX_BYTES") {
        if v == 0 {
            return SCALE_DEMO_EVIDENCE_MAX_BYTES;
        }
        let cap = if v > (1024 * 1024) { 1024 * 1024 } else { v };
        return cap as u32;
    }
    SCALE_DEMO_EVIDENCE_MAX_BYTES
}

fn scale_demo_evidence_frame_cache_bytes() -> u64 {
    env_u64("FSA_LM_SCALE_DEMO_EVIDENCE_FRAME_CACHE_BYTES")
        .unwrap_or(SCALE_DEMO_EVIDENCE_FRAME_CACHE_BYTES)
}

/// Scale demo evidence report ().
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScaleDemoEvidenceReportV1 {
    /// Schema version.
    pub version: u32,
    /// Deterministic workload hash.
    pub workload_hash: Hash32,
    /// IndexSnapshotV1 artifact hash used for retrieval.
    pub index_snapshot_hash: Hash32,
    /// IndexSigMapV1 artifact hash used for signature gating.
    pub index_sig_map_hash: Hash32,
    /// Total number of queries.
    pub query_count: u32,
    /// Top-k used for retrieval per query.
    pub k: u32,
    /// Evidence bundle max_bytes limit.
    pub max_bytes: u32,
    /// EvidenceBundle artifact hashes, in ascending query_id order.
    pub evidence_hashes: Vec<Hash32>,
}

impl core::fmt::Display for ScaleDemoEvidenceReportV1 {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        if self.evidence_hashes.is_empty() {
            write!(
                f,
                "scale_demo_evidence_v1 workload_hash={} evidence=0 snapshot={} sig_map={} k={} max_bytes={}",
                hex32(&self.workload_hash),
                hex32(&self.index_snapshot_hash),
                hex32(&self.index_sig_map_hash),
                self.k,
                self.max_bytes
            )
        } else {
            let first = self.evidence_hashes.first().unwrap();
            let last = self.evidence_hashes.last().unwrap();
            write!(
                f,
                "scale_demo_evidence_v1 workload_hash={} evidence={} first={} last={} snapshot={} sig_map={} k={} max_bytes={}",
                hex32(&self.workload_hash),
                self.evidence_hashes.len(),
                hex32(first),
                hex32(last),
                hex32(&self.index_snapshot_hash),
                hex32(&self.index_sig_map_hash),
                self.k,
                self.max_bytes
            )
        }
    }
}

/// Errors while building evidence bundles for scale demos.
#[derive(Debug)]
pub enum ScaleDemoEvidenceError {
    /// Underlying scale demo error.
    ScaleDemo(ScaleDemoError),
    /// Index query error.
    IndexQuery(IndexQueryError),
    /// Evidence build error.
    EvidenceBuild(EvidenceBuildError),
    /// Evidence artifact store error.
    EvidenceArtifact(EvidenceArtifactError),
}

impl core::fmt::Display for ScaleDemoEvidenceError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ScaleDemoEvidenceError::ScaleDemo(e) => write!(f, "scale demo: {}", e),
            ScaleDemoEvidenceError::IndexQuery(e) => write!(f, "index query: {:?}", e),
            ScaleDemoEvidenceError::EvidenceBuild(e) => write!(f, "evidence build: {}", e),
            ScaleDemoEvidenceError::EvidenceArtifact(e) => write!(f, "evidence artifact: {}", e),
        }
    }
}

impl std::error::Error for ScaleDemoEvidenceError {}

impl From<ScaleDemoError> for ScaleDemoEvidenceError {
    fn from(e: ScaleDemoError) -> Self {
        ScaleDemoEvidenceError::ScaleDemo(e)
    }
}

impl From<IndexQueryError> for ScaleDemoEvidenceError {
    fn from(e: IndexQueryError) -> Self {
        ScaleDemoEvidenceError::IndexQuery(e)
    }
}

impl From<EvidenceBuildError> for ScaleDemoEvidenceError {
    fn from(e: EvidenceBuildError) -> Self {
        ScaleDemoEvidenceError::EvidenceBuild(e)
    }
}

impl From<EvidenceArtifactError> for ScaleDemoEvidenceError {
    fn from(e: EvidenceArtifactError) -> Self {
        ScaleDemoEvidenceError::EvidenceArtifact(e)
    }
}

/// Build and store EvidenceBundle artifacts per query ().
///
/// Inputs:
/// - `cfg` controls deterministic query generation.
/// - `index_snapshot_hash` and `index_sig_map_hash` must refer to the artifacts
/// produced by /.
///
/// Behavior:
/// - Derives query terms from each query text.
/// - Runs signature-gated retrieval against the snapshot.
/// - Builds a canonical EvidenceBundleV1 and stores it as a content-addressed artifact.
/// - Returns a stable list of evidence hashes in ascending query_id order.
pub fn run_scale_demo_build_evidence_bundles_v1<S: ArtifactStore>(
    store: &S,
    cfg: ScaleDemoCfgV1,
    index_snapshot_hash: &Hash32,
    index_sig_map_hash: &Hash32,
) -> Result<ScaleDemoEvidenceReportV1, ScaleDemoEvidenceError> {
    let (w, w_hash) = generate_workload_and_hash(cfg)?;

    let mut qcfg = QueryTermsCfg::new();
    qcfg.include_metaphone = false;

    let mut scfg = SearchCfg::new();
    scfg.k = scale_demo_evidence_k();

    let max_bytes = scale_demo_evidence_max_bytes();

    let mut frame_cache: Cache2Q<Hash32, Arc<FrameSegmentV1>> =
        Cache2Q::new(CacheCfgV1::new(scale_demo_evidence_frame_cache_bytes()));

    let bcfg = EvidenceBuildCfgV1::new();

    let mut hashes: Vec<Hash32> = Vec::with_capacity(w.queries.len());
    for q in &w.queries {
        let qterms = query_terms_from_text(&q.text, &qcfg);
        let (hits, _gate_stats) = search_snapshot_gated(
            store,
            index_snapshot_hash,
            index_sig_map_hash,
            &qterms,
            &scfg,
        )?;

        // Derive a stable query_id for this evidence bundle.
        let mut qid_bytes: Vec<u8> = Vec::new();
        qid_bytes.extend_from_slice(b"scale_demo_evidence_v1\0");
        qid_bytes.extend_from_slice(&cfg.workload.seed.to_le_bytes());
        qid_bytes.extend_from_slice(&q.query_id.to_le_bytes());
        qid_bytes.extend_from_slice(index_snapshot_hash);
        qid_bytes.extend_from_slice(index_sig_map_hash);
        qid_bytes.extend_from_slice(&(scfg.k as u32).to_le_bytes());
        qid_bytes.extend_from_slice(&max_bytes.to_le_bytes());
        qid_bytes.extend_from_slice(&(qcfg.max_terms as u32).to_le_bytes());
        qid_bytes.extend_from_slice(q.text.as_bytes());
        let query_id = blake3_hash(&qid_bytes);

        let limits = EvidenceLimitsV1 {
            segments_touched: 0,
            max_items: hits.len() as u32,
            max_bytes: max_bytes,
        };

        let score_model_id: u32 = 1;
        let bundle = build_evidence_bundle_v1_from_hits_cached(
            store,
            &mut frame_cache,
            query_id,
            *index_snapshot_hash,
            limits,
            score_model_id,
            &hits,
            &bcfg,
        )?;

        let h = put_evidence_bundle_v1(store, &bundle)?;
        hashes.push(h);
    }

    Ok(ScaleDemoEvidenceReportV1 {
        version: SCALE_DEMO_EVIDENCE_V1_VERSION,
        workload_hash: w_hash,
        index_snapshot_hash: *index_snapshot_hash,
        index_sig_map_hash: *index_sig_map_hash,
        query_count: w.queries.len() as u32,
        k: scfg.k as u32,
        max_bytes: max_bytes,
        evidence_hashes: hashes,
    })
}

/// Scale demo answers report version.
pub const SCALE_DEMO_ANSWERS_V1_VERSION: u32 = 3;

/// Scale demo answers report (v3).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScaleDemoAnswersReportV1 {
    /// Schema version (must equal 3).
    pub version: u32,
    /// Workload hash.
    pub workload_hash: Hash32,
    /// Index snapshot hash used for evidence.
    pub index_snapshot_hash: Hash32,
    /// Index signature map hash used for gated search.
    pub index_sig_map_hash: Hash32,
    /// Number of queries.
    pub query_count: u32,
    /// Planner max_plan_items used during answer planning.
    pub planner_max_plan_items: u32,
    /// Realizer max evidence items rendered.
    pub realizer_max_evidence_items: u16,
    /// Realizer max terms per row.
    pub realizer_max_terms_per_row: u16,
    /// Realizer load frame rows flag.
    pub realizer_load_frame_rows: u8,
    /// Answer output artifact hashes, one per query, in ascending query_id order.
    pub answer_hashes: Vec<Hash32>,

    /// PlannerHints artifact hashes, one per query, in ascending query_id order.
    pub planner_hints_hashes: Vec<Hash32>,

    /// Forecast artifact hashes, one per query, in ascending query_id order.
    pub forecast_hashes: Vec<Hash32>,

    /// MarkovTrace artifact hashes, one per query, in ascending query_id order.
    pub markov_trace_hashes: Vec<Hash32>,
}

impl core::fmt::Display for ScaleDemoAnswersReportV1 {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let answers_n = self.answer_hashes.len();
        let ph_n = self.planner_hints_hashes.len();
        let fc_n = self.forecast_hashes.len();
        let mt_n = self.markov_trace_hashes.len();

        let (ans_first, ans_last) = if answers_n == 0 {
            ([0u8; 32], [0u8; 32])
        } else {
            (
                *self.answer_hashes.first().unwrap(),
                *self.answer_hashes.last().unwrap(),
            )
        };
        let (ph_first, ph_last) = if ph_n == 0 {
            ([0u8; 32], [0u8; 32])
        } else {
            (
                *self.planner_hints_hashes.first().unwrap(),
                *self.planner_hints_hashes.last().unwrap(),
            )
        };
        let (fc_first, fc_last) = if fc_n == 0 {
            ([0u8; 32], [0u8; 32])
        } else {
            (
                *self.forecast_hashes.first().unwrap(),
                *self.forecast_hashes.last().unwrap(),
            )
        };

        let (mt_first, mt_last) = if mt_n == 0 {
            ([0u8; 32], [0u8; 32])
        } else {
            (
                *self.markov_trace_hashes.first().unwrap(),
                *self.markov_trace_hashes.last().unwrap(),
            )
        };

        write!(
            f,
            "scale_demo_answers_v3 workload_hash={} answers={} answers_first={} answers_last={} planner_hints={} planner_hints_first={} planner_hints_last={} forecasts={} forecasts_first={} forecasts_last={} markov_traces={} markov_traces_first={} markov_traces_last={} snapshot={} sig_map={} plan_items={} max_evidence_items={} max_terms_per_row={} load_rows={}",
            hex32(&self.workload_hash),
            answers_n,
            hex32(&ans_first),
            hex32(&ans_last),
            ph_n,
            hex32(&ph_first),
            hex32(&ph_last),
            fc_n,
            hex32(&fc_first),
            hex32(&fc_last),
            mt_n,
            hex32(&mt_first),
            hex32(&mt_last),
            hex32(&self.index_snapshot_hash),
            hex32(&self.index_sig_map_hash),
            self.planner_max_plan_items,
            self.realizer_max_evidence_items,
            self.realizer_max_terms_per_row,
            self.realizer_load_frame_rows
        )
    }
}
/// Scale demo answer stage errors.
#[derive(Debug)]
pub enum ScaleDemoAnswerError {
    /// Evidence report is invalid.
    BadEvidenceReport,
    /// Evidence bundle artifact could not be loaded.
    EvidenceArtifact(EvidenceArtifactError),
    /// Evidence bundle artifact missing from store.
    MissingEvidenceBundle,

    /// PlannerHints artifact store error.
    PlannerHintsArtifact(crate::planner_hints_artifact::PlannerHintsArtifactError),

    /// Forecast artifact store error.
    ForecastArtifact(crate::forecast_artifact::ForecastArtifactError),

    /// MarkovTrace artifact store error.
    MarkovTraceArtifact(crate::markov_trace_artifact::MarkovTraceArtifactError),
    /// Evidence bundle could not be encoded canonically.
    EvidenceEncode(EncodeError),
    /// Planner failed.
    Planner(crate::planner_v1::PlannerV1Error),
    /// Realizer failed.
    Realizer(crate::realizer_v1::RealizerV1Error),
    /// Artifact store error.
    Store(ArtifactError),
}

impl core::fmt::Display for ScaleDemoAnswerError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ScaleDemoAnswerError::BadEvidenceReport => f.write_str("bad evidence report"),
            ScaleDemoAnswerError::EvidenceArtifact(e) => write!(f, "evidence artifact: {}", e),
            ScaleDemoAnswerError::MissingEvidenceBundle => f.write_str("missing evidence bundle"),
            ScaleDemoAnswerError::PlannerHintsArtifact(e) => write!(f, "planner_hints: {}", e),
            ScaleDemoAnswerError::ForecastArtifact(e) => write!(f, "forecast: {}", e),
            ScaleDemoAnswerError::MarkovTraceArtifact(e) => write!(f, "markov_trace: {}", e),
            ScaleDemoAnswerError::EvidenceEncode(e) => write!(f, "evidence encode: {}", e),
            ScaleDemoAnswerError::Planner(e) => write!(f, "planner: {}", e),
            ScaleDemoAnswerError::Realizer(e) => write!(f, "realizer: {}", e),
            ScaleDemoAnswerError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl std::error::Error for ScaleDemoAnswerError {}

/// Build and store deterministic answer outputs per query.
///
/// This stage:
/// - Loads each EvidenceBundleV1 referenced by the evidence report.
/// - Runs planner v1 to produce an AnswerPlanV1.
/// - Runs realizer v1 to produce a deterministic text output.
/// - Stores the output bytes as a content-addressed artifact and records the hash.
pub fn run_scale_demo_build_answers_v1<S: ArtifactStore>(
    store: &S,
    evidence_report: &ScaleDemoEvidenceReportV1,
) -> Result<ScaleDemoAnswersReportV1, ScaleDemoAnswerError> {
    run_scale_demo_build_answers_v1_impl(store, evidence_report, None)
}

/// Build answers per query using an explicit RealizerDirectivesV1.
///
/// This exists to let golden-pack exercise the directives path without
/// changing the default scale-demo CLI behavior.
pub fn run_scale_demo_build_answers_v1_with_directives<S: ArtifactStore>(
    store: &S,
    evidence_report: &ScaleDemoEvidenceReportV1,
    directives: &crate::realizer_directives::RealizerDirectivesV1,
) -> Result<ScaleDemoAnswersReportV1, ScaleDemoAnswerError> {
    run_scale_demo_build_answers_v1_impl(store, evidence_report, Some(directives))
}

fn run_scale_demo_build_answers_v1_impl<S: ArtifactStore>(
    store: &S,
    evidence_report: &ScaleDemoEvidenceReportV1,
    directives: Option<&crate::realizer_directives::RealizerDirectivesV1>,
) -> Result<ScaleDemoAnswersReportV1, ScaleDemoAnswerError> {
    if evidence_report.version != SCALE_DEMO_EVIDENCE_V1_VERSION {
        return Err(ScaleDemoAnswerError::BadEvidenceReport);
    }
    if evidence_report.evidence_hashes.len() != (evidence_report.query_count as usize) {
        return Err(ScaleDemoAnswerError::BadEvidenceReport);
    }

    let planner_cfg = crate::planner_v1::PlannerCfgV1::default_v1();
    let realizer_cfg = crate::realizer_v1::RealizerCfgV1::new();

    let mut answer_hashes: Vec<Hash32> = Vec::with_capacity(evidence_report.evidence_hashes.len());

    let mut planner_hints_hashes: Vec<Hash32> =
        Vec::with_capacity(evidence_report.evidence_hashes.len());
    let mut forecast_hashes: Vec<Hash32> =
        Vec::with_capacity(evidence_report.evidence_hashes.len());
    let mut markov_trace_hashes: Vec<Hash32> =
        Vec::with_capacity(evidence_report.evidence_hashes.len());

    for evh in evidence_report.evidence_hashes.iter() {
        let bundle_opt = crate::evidence_artifact::get_evidence_bundle_v1(store, evh)
            .map_err(ScaleDemoAnswerError::EvidenceArtifact)?;
        let bundle = match bundle_opt {
            Some(b) => b,
            None => return Err(ScaleDemoAnswerError::MissingEvidenceBundle),
        };

        let eb_bytes = bundle
            .encode()
            .map_err(ScaleDemoAnswerError::EvidenceEncode)?;
        let eb_id = blake3_hash(&eb_bytes);
        let pout = crate::planner_v1::plan_from_evidence_bundle_v1_with_guidance(
            &bundle,
            eb_id,
            &planner_cfg,
            None,
        )
        .map_err(ScaleDemoAnswerError::Planner)?;
        let plan = pout.plan;
        let hints = pout.hints;
        let forecast = pout.forecast;

        let hints_hash = crate::planner_hints_artifact::put_planner_hints_v1(store, &hints)
            .map_err(ScaleDemoAnswerError::PlannerHintsArtifact)?;
        let forecast_hash = crate::forecast_artifact::put_forecast_v1(store, &forecast)
            .map_err(ScaleDemoAnswerError::ForecastArtifact)?;
        planner_hints_hashes.push(hints_hash);
        forecast_hashes.push(forecast_hash);
        let qr = crate::quality_gate_v1::realize_with_quality_gate_v1(
            store,
            &bundle,
            &plan,
            &realizer_cfg,
            directives,
            None,
            &hints,
            &forecast,
        )
        .map_err(ScaleDemoAnswerError::Realizer)?;

        let text = qr.text;
        let did_append_q = qr.did_append_question;

        //: Use the quality gate token builder so scale-demo MarkovTrace
        // matches the answer CLI (including any wired surface-template ids).
        let mt_tokens: Vec<crate::markov_model::MarkovTokenV1> =
            crate::quality_gate_v1::build_markov_trace_tokens_v1(&plan, &qr.markov, did_append_q);

        let trace = crate::markov_trace::MarkovTraceV1 {
            version: crate::markov_trace::MARKOV_TRACE_V1_VERSION,
            query_id: bundle.query_id,
            tokens: mt_tokens,
        };
        let trace_hash = crate::markov_trace_artifact::put_markov_trace_v1(store, &trace)
            .map_err(ScaleDemoAnswerError::MarkovTraceArtifact)?;
        markov_trace_hashes.push(trace_hash);

        let ah = store
            .put(text.as_bytes())
            .map_err(ScaleDemoAnswerError::Store)?;
        answer_hashes.push(ah);
    }

    Ok(ScaleDemoAnswersReportV1 {
        version: SCALE_DEMO_ANSWERS_V1_VERSION,
        workload_hash: evidence_report.workload_hash,
        index_snapshot_hash: evidence_report.index_snapshot_hash,
        index_sig_map_hash: evidence_report.index_sig_map_hash,
        query_count: evidence_report.query_count,
        planner_max_plan_items: planner_cfg.max_plan_items,
        realizer_max_evidence_items: realizer_cfg.max_evidence_items,
        realizer_max_terms_per_row: realizer_cfg.max_terms_per_row,
        realizer_load_frame_rows: if realizer_cfg.load_frame_rows { 1 } else { 0 },
        answer_hashes,
        planner_hints_hashes,
        forecast_hashes,
        markov_trace_hashes,
    })
}

/// Errors while assembling a ScaleDemoScaleReportV1.
#[derive(Debug)]
pub enum ScaleDemoScaleReportError {
    /// Workload hash mismatch across stage reports.
    WorkloadHashMismatch,
    /// Query count mismatch across stage reports.
    QueryCountMismatch,
    /// Index report does not match frames report.
    IndexManifestMismatch,
    /// Evidence stage requires an index report.
    EvidenceRequiresIndex,
    /// Evidence stage uses a different index than the index report.
    EvidenceIndexMismatch,
    /// Answers stage requires an evidence report.
    AnswersRequiresEvidence,
    /// Answers stage uses a different index than the evidence report.
    AnswersIndexMismatch,

    /// Guidance list lengths do not match the answers list length.
    GuidanceCountMismatch,
}

impl core::fmt::Display for ScaleDemoScaleReportError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ScaleDemoScaleReportError::WorkloadHashMismatch => {
                f.write_str("workload hash mismatch")
            }
            ScaleDemoScaleReportError::QueryCountMismatch => f.write_str("query count mismatch"),
            ScaleDemoScaleReportError::IndexManifestMismatch => {
                f.write_str("index manifest mismatch")
            }
            ScaleDemoScaleReportError::EvidenceRequiresIndex => {
                f.write_str("evidence requires index")
            }
            ScaleDemoScaleReportError::EvidenceIndexMismatch => {
                f.write_str("evidence index mismatch")
            }
            ScaleDemoScaleReportError::AnswersRequiresEvidence => {
                f.write_str("answers requires evidence")
            }
            ScaleDemoScaleReportError::AnswersIndexMismatch => {
                f.write_str("answers index mismatch")
            }
            ScaleDemoScaleReportError::GuidanceCountMismatch => {
                f.write_str("guidance count mismatch")
            }
        }
    }
}

impl std::error::Error for ScaleDemoScaleReportError {}

/// Build a compact, deterministic ScaleDemoScaleReportV1 from stage reports.
///
/// The scale report is a canonically-encoded summary artifact intended for
/// large runs. It summarizes per-query hash lists via list hashes and
/// first/last samples.
pub fn build_scale_demo_scale_report_v1(
    gen_report: &ScaleDemoReportV1,
    frames_report: &ScaleDemoFramesReportV1,
    index_report: Option<&ScaleDemoIndexReportV1>,
    prompts_report: Option<&ScaleDemoPromptsReportV1>,
    evidence_report: Option<&ScaleDemoEvidenceReportV1>,
    answers_report: Option<&ScaleDemoAnswersReportV1>,
) -> Result<ScaleDemoScaleReportV1, ScaleDemoScaleReportError> {
    if let Some(pr) = prompts_report {
        if pr.workload_hash != gen_report.workload_hash {
            return Err(ScaleDemoScaleReportError::WorkloadHashMismatch);
        }
        if pr.query_count != gen_report.query_count {
            return Err(ScaleDemoScaleReportError::QueryCountMismatch);
        }
    }

    if let Some(ix) = index_report {
        if ix.frame_manifest_hash != frames_report.frame_manifest_hash {
            return Err(ScaleDemoScaleReportError::IndexManifestMismatch);
        }
    }

    if let Some(er) = evidence_report {
        if er.workload_hash != gen_report.workload_hash {
            return Err(ScaleDemoScaleReportError::WorkloadHashMismatch);
        }
        if er.query_count != gen_report.query_count {
            return Err(ScaleDemoScaleReportError::QueryCountMismatch);
        }
        let ix = match index_report {
            Some(v) => v,
            None => return Err(ScaleDemoScaleReportError::EvidenceRequiresIndex),
        };
        if er.index_snapshot_hash != ix.index_snapshot_hash
            || er.index_sig_map_hash != ix.index_sig_map_hash
        {
            return Err(ScaleDemoScaleReportError::EvidenceIndexMismatch);
        }
    }

    if let Some(ar) = answers_report {
        if ar.workload_hash != gen_report.workload_hash {
            return Err(ScaleDemoScaleReportError::WorkloadHashMismatch);
        }
        if ar.query_count != gen_report.query_count {
            return Err(ScaleDemoScaleReportError::QueryCountMismatch);
        }
        let er = match evidence_report {
            Some(v) => v,
            None => return Err(ScaleDemoScaleReportError::AnswersRequiresEvidence),
        };
        if ar.index_snapshot_hash != er.index_snapshot_hash
            || ar.index_sig_map_hash != er.index_sig_map_hash
        {
            return Err(ScaleDemoScaleReportError::AnswersIndexMismatch);
        }

        if ar.planner_hints_hashes.len() != ar.answer_hashes.len() {
            return Err(ScaleDemoScaleReportError::GuidanceCountMismatch);
        }
        if ar.forecast_hashes.len() != ar.answer_hashes.len() {
            return Err(ScaleDemoScaleReportError::GuidanceCountMismatch);
        }
        if ar.markov_trace_hashes.len() != ar.answer_hashes.len() {
            return Err(ScaleDemoScaleReportError::GuidanceCountMismatch);
        }
    }

    let (has_index, index_snapshot_hash, index_sig_map_hash, index_segments_total) =
        match index_report {
            Some(ix) => (
                1u8,
                ix.index_snapshot_hash,
                ix.index_sig_map_hash,
                ix.segments_total,
            ),
            None => (0u8, [0u8; 32], [0u8; 32], 0u32),
        };

    let (has_prompts, prompts_max_output_tokens, prompts_summary) = match prompts_report {
        Some(pr) => (
            1u8,
            pr.max_output_tokens,
            HashListSummaryV1::from_list("prompts", &pr.prompt_hashes),
        ),
        None => (0u8, 0u32, HashListSummaryV1::empty()),
    };

    let (has_evidence, evidence_k, evidence_max_bytes, evidence_summary) = match evidence_report {
        Some(er) => (
            1u8,
            er.k,
            er.max_bytes,
            HashListSummaryV1::from_list("evidence", &er.evidence_hashes),
        ),
        None => (0u8, 0u32, 0u32, HashListSummaryV1::empty()),
    };

    let (
        has_answers,
        planner_max_plan_items,
        realizer_max_evidence_items,
        realizer_max_terms_per_row,
        realizer_load_frame_rows,
        answers_summary,
        planner_hints_summary,
        forecasts_summary,
        markov_traces_summary,
    ) = match answers_report {
        Some(ar) => (
            1u8,
            ar.planner_max_plan_items,
            ar.realizer_max_evidence_items,
            ar.realizer_max_terms_per_row,
            ar.realizer_load_frame_rows,
            HashListSummaryV1::from_list("answers", &ar.answer_hashes),
            HashListSummaryV1::from_list("planner_hints", &ar.planner_hints_hashes),
            HashListSummaryV1::from_list("forecasts", &ar.forecast_hashes),
            HashListSummaryV1::from_list("markov_traces", &ar.markov_trace_hashes),
        ),
        None => (
            0u8,
            0u32,
            0u16,
            0u16,
            0u8,
            HashListSummaryV1::empty(),
            HashListSummaryV1::empty(),
            HashListSummaryV1::empty(),
            HashListSummaryV1::empty(),
        ),
    };

    let rep = ScaleDemoScaleReportV1 {
        version: SCALE_DEMO_SCALE_REPORT_V1_VERSION,
        workload_hash: gen_report.workload_hash,
        doc_count: gen_report.doc_count,
        query_count: gen_report.query_count,
        tie_pair: if gen_report.tie_pair { 1 } else { 0 },
        seed: gen_report.seed,
        frame_manifest_hash: frames_report.frame_manifest_hash,
        docs_total: frames_report.docs_total,
        rows_total: frames_report.rows_total,
        frame_segments_total: frames_report.segments_total,
        has_index,
        index_snapshot_hash,
        index_sig_map_hash,
        index_segments_total,
        has_prompts,
        prompts_max_output_tokens,
        prompts: prompts_summary,
        has_evidence,
        evidence_k,
        evidence_max_bytes,
        evidence: evidence_summary,
        has_answers,
        planner_max_plan_items,
        realizer_max_evidence_items,
        realizer_max_terms_per_row,
        realizer_load_frame_rows,
        answers: answers_summary,
        planner_hints: planner_hints_summary,
        forecasts: forecasts_summary,
        markov_traces: markov_traces_summary,
    };

    // Defensive canonical validation (should always pass).
    rep.validate_canonical()
        .map_err(|_| ScaleDemoScaleReportError::WorkloadHashMismatch)?;

    Ok(rep)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push("fsa_lm_tests");
        p.push(name);
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn generate_only_is_deterministic() {
        let mut cfg = ScaleDemoCfgV1::default();
        cfg.workload.seed = 42;
        cfg.workload.doc_count = 16;
        cfg.workload.query_count = 8;
        cfg.workload.min_tokens_per_doc = 3;
        cfg.workload.max_tokens_per_doc = 6;
        cfg.workload.vocab_size = 32;
        cfg.workload.query_tokens = 3;
        cfg.workload.include_tie_pair = 1;

        let r1 = run_scale_demo_generate_only_v1(cfg).unwrap();
        let r2 = run_scale_demo_generate_only_v1(cfg).unwrap();
        assert_eq!(r1, r2);
    }

    #[test]
    fn generate_and_ingest_frames_is_deterministic() {
        let dir = tmp_dir("scale_demo_generate_and_ingest_frames_is_deterministic");
        let store = FsArtifactStore::new(&dir).unwrap();

        let mut cfg = ScaleDemoCfgV1::default();
        cfg.workload.seed = 7;
        cfg.workload.doc_count = 12;
        cfg.workload.query_count = 4;
        cfg.workload.min_tokens_per_doc = 3;
        cfg.workload.max_tokens_per_doc = 6;
        cfg.workload.vocab_size = 32;
        cfg.workload.query_tokens = 3;
        cfg.workload.include_tie_pair = 1;

        let (r1, f1) = run_scale_demo_generate_and_ingest_frames_v1(&store, cfg).unwrap();
        let (r2, f2) = run_scale_demo_generate_and_ingest_frames_v1(&store, cfg).unwrap();
        assert_eq!(r1, r2);
        assert_eq!(f1, f2);

        assert_eq!(f1.docs_total, cfg.workload.doc_count as u64);
        assert!(f1.rows_total >= f1.docs_total);
        assert!(f1.segments_total >= 1);

        let p = store.path_for(&f1.frame_manifest_hash);
        assert!(p.exists());
    }

    #[test]
    fn manifest_helpers_load_and_collect_segments() {
        let dir = tmp_dir("scale_demo_manifest_helpers_load_and_collect_segments");
        let store = FsArtifactStore::new(&dir).unwrap();

        let mut cfg = ScaleDemoCfgV1::default();
        cfg.workload.seed = 9;
        cfg.workload.doc_count = 20;
        cfg.workload.query_count = 5;
        cfg.workload.min_tokens_per_doc = 3;
        cfg.workload.max_tokens_per_doc = 6;
        cfg.workload.vocab_size = 32;
        cfg.workload.query_tokens = 3;
        cfg.workload.include_tie_pair = 1;

        let (_r, frames) = run_scale_demo_generate_and_ingest_frames_v1(&store, cfg).unwrap();
        let man = load_wiki_ingest_manifest_v1(&store, &frames.frame_manifest_hash).unwrap();
        let segs =
            collect_frame_segments_from_manifest_v1(&store, &frames.frame_manifest_hash).unwrap();

        assert_eq!(segs, man.segments);
        assert_eq!(segs.len() as u32, frames.segments_total);
        assert_eq!(man.docs_total, frames.docs_total);
        assert_eq!(man.rows_total, frames.rows_total);
    }

    #[test]
    fn manifest_helpers_missing_manifest_fails() {
        let dir = tmp_dir("scale_demo_manifest_helpers_missing_manifest_fails");
        let store = FsArtifactStore::new(&dir).unwrap();
        let missing: Hash32 = [0u8; 32];
        let err = collect_frame_segments_from_manifest_v1(&store, &missing).unwrap_err();
        match err {
            ScaleDemoManifestError::MissingManifest => {}
            _ => panic!("expected MissingManifest"),
        }
    }
    #[test]
    fn generate_and_store_prompts_is_deterministic() {
        let dir = tmp_dir("scale_demo_generate_and_store_prompts_is_deterministic");
        let store = FsArtifactStore::new(&dir).unwrap();

        let mut cfg = ScaleDemoCfgV1::default();
        cfg.workload.seed = 11;
        cfg.workload.doc_count = 50;
        cfg.workload.query_count = 9;
        cfg.workload.min_tokens_per_doc = 3;
        cfg.workload.max_tokens_per_doc = 7;
        cfg.workload.vocab_size = 64;
        cfg.workload.query_tokens = 4;
        cfg.workload.include_tie_pair = 1;

        let r1 = run_scale_demo_generate_and_store_prompts_v1(&store, cfg).unwrap();
        let r2 = run_scale_demo_generate_and_store_prompts_v1(&store, cfg).unwrap();
        assert_eq!(r1, r2);
        assert_eq!(r1.query_count, cfg.workload.query_count);
        assert_eq!(r1.prompt_hashes.len() as u32, cfg.workload.query_count);

        let w = generate_workload_v1(cfg.workload).unwrap();
        assert_eq!(w.queries.len() as u32, cfg.workload.query_count);

        for (i, q) in w.queries.iter().enumerate() {
            let h = r1.prompt_hashes[i];
            let pack = crate::prompt_artifact::get_prompt_pack(&store, &h)
                .unwrap()
                .unwrap();
            assert_eq!(pack.max_output_tokens, SCALE_DEMO_PROMPT_MAX_OUTPUT_TOKENS);
            assert_eq!(pack.seed, cfg.workload.seed.wrapping_add(q.query_id as u64));
            assert_eq!(pack.messages.len(), 1);
            assert_eq!(pack.messages[0].role, Role::User);
            assert_eq!(pack.messages[0].content, q.text);

            let mut found = false;
            for c in &pack.constraints {
                if c.key == "scale_demo_query_id" {
                    assert_eq!(c.value, q.query_id.to_string());
                    found = true;
                }
            }
            assert!(found);
        }
    }

    #[test]
    fn build_evidence_bundles_is_deterministic() {
        let dir = tmp_dir("scale_demo_build_evidence_bundles_is_deterministic");
        let store = FsArtifactStore::new(&dir).unwrap();

        let mut cfg = ScaleDemoCfgV1::default();
        cfg.workload.seed = 7;
        cfg.workload.doc_count = 200;
        cfg.workload.query_count = 9;
        cfg.workload.min_tokens_per_doc = 5;
        cfg.workload.max_tokens_per_doc = 18;
        cfg.workload.query_tokens = 3;
        cfg.workload.include_tie_pair = 1;

        let (_r, frames) = run_scale_demo_generate_and_ingest_frames_v1(&store, cfg).unwrap();
        let idx = run_scale_demo_build_index_from_manifest_v1(&store, &frames.frame_manifest_hash)
            .unwrap();

        let e1 = run_scale_demo_build_evidence_bundles_v1(
            &store,
            cfg,
            &idx.index_snapshot_hash,
            &idx.index_sig_map_hash,
        )
        .unwrap();
        let e2 = run_scale_demo_build_evidence_bundles_v1(
            &store,
            cfg,
            &idx.index_snapshot_hash,
            &idx.index_sig_map_hash,
        )
        .unwrap();

        assert_eq!(e1, e2);
        assert_eq!(e1.query_count, cfg.workload.query_count);
        assert_eq!(e1.evidence_hashes.len() as u32, cfg.workload.query_count);

        for h in &e1.evidence_hashes {
            let got = crate::evidence_artifact::get_evidence_bundle_v1(&store, h)
                .unwrap()
                .unwrap();
            assert_eq!(got.snapshot_id, idx.index_snapshot_hash);
        }
    }

    #[test]
    fn build_answers_is_deterministic() {
        let dir = tmp_dir("scale_demo_build_answers_is_deterministic");
        let store = FsArtifactStore::new(&dir).unwrap();

        let mut cfg = ScaleDemoCfgV1::default();
        cfg.workload.seed = 9;
        cfg.workload.doc_count = 200;
        cfg.workload.query_count = 9;
        cfg.workload.min_tokens_per_doc = 5;
        cfg.workload.max_tokens_per_doc = 18;
        cfg.workload.query_tokens = 3;
        cfg.workload.include_tie_pair = 1;

        let (_r, frames) = run_scale_demo_generate_and_ingest_frames_v1(&store, cfg).unwrap();
        let idx = run_scale_demo_build_index_from_manifest_v1(&store, &frames.frame_manifest_hash)
            .unwrap();

        let e = run_scale_demo_build_evidence_bundles_v1(
            &store,
            cfg,
            &idx.index_snapshot_hash,
            &idx.index_sig_map_hash,
        )
        .unwrap();

        let a1 = run_scale_demo_build_answers_v1(&store, &e).unwrap();
        let a2 = run_scale_demo_build_answers_v1(&store, &e).unwrap();

        assert_eq!(a1, a2);
        assert_eq!(a1.query_count, cfg.workload.query_count);
        assert_eq!(a1.answer_hashes.len() as u32, cfg.workload.query_count);

        for h in a1.answer_hashes.iter() {
            let bytes = store.get(h).unwrap().unwrap();
            assert!(bytes.starts_with(b"Answer v1\n"));
        }
    }

    #[test]
    fn build_index_from_manifest_is_deterministic() {
        let dir = tmp_dir("scale_demo_build_index_from_manifest_is_deterministic");
        let store = FsArtifactStore::new(&dir).unwrap();

        let mut cfg = ScaleDemoCfgV1::default();
        cfg.workload.doc_count = 400;
        cfg.workload.query_count = 10;
        cfg.workload.min_tokens_per_doc = 6;
        cfg.workload.max_tokens_per_doc = 20;
        cfg.workload.query_tokens = 3;
        cfg.workload.include_tie_pair = 1;

        let (_r, frames) = run_scale_demo_generate_and_ingest_frames_v1(&store, cfg).unwrap();

        let a = run_scale_demo_build_index_from_manifest_v1(&store, &frames.frame_manifest_hash)
            .unwrap();
        let b = run_scale_demo_build_index_from_manifest_v1(&store, &frames.frame_manifest_hash)
            .unwrap();
        assert_eq!(a, b);
        assert_eq!(a.segments_total, frames.segments_total);

        // Snapshot entries should match the manifest segments (sorted).
        let man = load_wiki_ingest_manifest_v1(&store, &frames.frame_manifest_hash).unwrap();
        let mut want = man.segments.clone();
        want.sort();

        let snap =
            crate::index_snapshot_store::get_index_snapshot_v1(&store, &a.index_snapshot_hash)
                .unwrap()
                .unwrap();
        assert_eq!(snap.entries.len(), want.len());

        let mut got: Vec<Hash32> = snap.entries.iter().map(|e| e.frame_seg).collect();
        got.sort();
        assert_eq!(got, want);

        // Sig map should resolve each index segment to a segment signature.
        let map = crate::index_sig_map_store::get_index_sig_map_v1(&store, &a.index_sig_map_hash)
            .unwrap()
            .unwrap();
        for e in &snap.entries {
            let sig_hash = map.lookup_sig(&e.index_seg).expect("sig present");
            let sig = crate::segment_sig_store::get_segment_sig_v1(&store, &sig_hash)
                .unwrap()
                .unwrap();
            assert_eq!(sig.index_seg, e.index_seg);

            let idx = crate::index_store::get_index_segment_v1(&store, &e.index_seg)
                .unwrap()
                .unwrap();
            assert_eq!(idx.seg_hash, e.frame_seg);
        }
    }
}
