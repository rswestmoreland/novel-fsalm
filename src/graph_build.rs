// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Offline graph relevance builder helpers.
//!
//! This module keeps graph work offline and bounded. The builder:
//! - prepares a deterministic input plan over supported source families
//! - mines conservative adjacency candidates from selected decoded artifacts
//! - finalizes a canonical `GraphRelevanceV1` artifact
//!
//! Mining remains intentionally conservative in this step:
//! - `FrameSegmentV1` contributes direct 1-hop co-occurrence edges from stored
//!   terms/entities/verbs
//! - `ReplayLog`, `PromptPack`, and `ConversationPack` are accepted as future
//!   source families but do not emit rows yet in v1
//! - no retrieval activation happens here

use crate::conversation_pack::ConversationPackV1;
use crate::frame::{EntityId, Id64, VerbId};
use crate::frame_segment::FrameSegmentV1;
use crate::graph_relevance::{
    GraphNodeKindV1, GraphRelevanceEdgeFlagsV1, GraphRelevanceEdgeV1, GraphRelevanceError,
    GraphRelevanceFlagsV1, GraphRelevanceRowV1, GraphRelevanceV1,
    GRAPH_RELEVANCE_EDGE_FLAGS_V1_ALL, GRAPH_RELEVANCE_V1_MAX_EDGES_PER_ROW,
    GRAPH_RELEVANCE_V1_MAX_ROWS, GRAPH_RELEVANCE_V1_VERSION, GREDGE_FLAG_SYMMETRIC,
    GR_FLAG_HAS_ENTITY_ROWS, GR_FLAG_HAS_TERM_ROWS, GR_FLAG_HAS_VERB_ROWS,
};
use crate::hash::Hash32;
use crate::prompt_pack::PromptPack;
use crate::replay::ReplayLog;
use std::collections::BTreeMap;

/// Maximum number of input artifacts accepted by the v1 graph builder.
pub const GRAPH_BUILD_V1_MAX_INPUTS: usize = 512;

const GRAPH_BUILD_V1_DEFAULT_TERM_TERM_WEIGHT_Q16: u16 = 4096;
const GRAPH_BUILD_V1_DEFAULT_TERM_ENTITY_WEIGHT_Q16: u16 = 8192;
const GRAPH_BUILD_V1_DEFAULT_TERM_VERB_WEIGHT_Q16: u16 = 6144;
const GRAPH_BUILD_V1_DEFAULT_ENTITY_VERB_WEIGHT_Q16: u16 = 10240;

/// Deterministic builder config for graph relevance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GraphBuildConfigV1 {
    /// Maximum number of input artifacts to consider overall.
    pub max_inputs_total: u32,
    /// Maximum number of input artifacts to keep per source kind.
    pub max_inputs_per_source_kind: u32,
    /// Maximum number of seed rows to keep in the finalized graph artifact.
    pub max_rows: u32,
    /// Maximum number of edges to keep per seed row.
    pub max_edges_per_row: u8,
    /// Maximum number of unique terms to mine from one frame row.
    pub max_terms_per_frame_row: u8,
    /// Maximum number of unique entities to mine from one frame row.
    pub max_entities_per_frame_row: u8,
}

impl GraphBuildConfigV1 {
    /// Default deterministic config for v1.
    pub fn default_v1() -> Self {
        Self {
            max_inputs_total: 256,
            max_inputs_per_source_kind: 64,
            max_rows: GRAPH_RELEVANCE_V1_MAX_ROWS as u32,
            max_edges_per_row: GRAPH_RELEVANCE_V1_MAX_EDGES_PER_ROW as u8,
            max_terms_per_frame_row: 8,
            max_entities_per_frame_row: 4,
        }
    }

    /// Validate builder caps.
    pub fn validate(&self) -> Result<(), GraphBuildError> {
        if self.max_inputs_total == 0
            || self.max_inputs_total as usize > GRAPH_BUILD_V1_MAX_INPUTS
            || self.max_inputs_per_source_kind == 0
            || self.max_rows == 0
            || self.max_rows as usize > GRAPH_RELEVANCE_V1_MAX_ROWS
            || self.max_edges_per_row == 0
            || self.max_edges_per_row as usize > GRAPH_RELEVANCE_V1_MAX_EDGES_PER_ROW
            || self.max_terms_per_frame_row == 0
            || self.max_entities_per_frame_row == 0
        {
            return Err(GraphBuildError::BadConfig);
        }
        Ok(())
    }
}

/// One input source family accepted by the builder.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(u8)]
pub enum GraphBuildSourceKindV1 {
    /// A frame segment built from ingested corpus rows.
    FrameSegment = 1,
    /// A replay log accepted for future source-family coverage.
    ReplayLog = 2,
    /// A prompt pack accepted for future source-family coverage.
    PromptPack = 3,
    /// A conversation pack accepted for future source-family coverage.
    ConversationPack = 4,
}

/// One input artifact accepted by the graph builder.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GraphBuildInputV1 {
    /// Input source family.
    pub source_kind: GraphBuildSourceKindV1,
    /// Content hash of the source artifact.
    pub source_hash: Hash32,
}

impl GraphBuildInputV1 {
    /// Construct a builder input.
    pub fn new(source_kind: GraphBuildSourceKindV1, source_hash: Hash32) -> Self {
        Self {
            source_kind,
            source_hash,
        }
    }
}

/// Prepared deterministic build plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphBuildPlanV1 {
    /// Stable build id for the future graph artifact.
    pub build_id: Hash32,
    /// Canonical deduplicated and capped input inventory.
    pub inputs: Vec<GraphBuildInputV1>,
    /// Final row cap to apply when the artifact is finalized.
    pub max_rows: u32,
    /// Final edge cap to apply per row when the artifact is finalized.
    pub max_edges_per_row: u8,
    /// Per-frame-row term cap used during mining.
    pub max_terms_per_frame_row: u8,
    /// Per-frame-row entity cap used during mining.
    pub max_entities_per_frame_row: u8,
}

/// Deterministic report for build planning.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GraphBuildReportV1 {
    /// Number of inputs provided by the caller.
    pub inputs_seen: u32,
    /// Number of inputs kept after dedup and capping.
    pub inputs_kept: u32,
    /// Number of duplicate inputs dropped.
    pub inputs_deduped: u32,
    /// Number of inputs dropped by caps.
    pub inputs_dropped_by_cap: u32,
}

/// Borrowed decoded source artifact used by the mining step.
#[derive(Clone, Copy, Debug)]
pub enum GraphSourceArtifactV1<'a> {
    /// FrameSegment source.
    FrameSegment {
        /// Content hash of the artifact.
        source_hash: Hash32,
        /// Decoded artifact.
        artifact: &'a FrameSegmentV1,
    },
    /// ReplayLog source.
    ReplayLog {
        /// Content hash of the artifact.
        source_hash: Hash32,
        /// Decoded artifact.
        artifact: &'a ReplayLog,
    },
    /// PromptPack source.
    PromptPack {
        /// Content hash of the artifact.
        source_hash: Hash32,
        /// Decoded artifact.
        artifact: &'a PromptPack,
    },
    /// ConversationPack source.
    ConversationPack {
        /// Content hash of the artifact.
        source_hash: Hash32,
        /// Decoded artifact.
        artifact: &'a ConversationPackV1,
    },
}

impl<'a> GraphSourceArtifactV1<'a> {
    /// Return the canonical build input for this source artifact.
    pub fn build_input(&self) -> GraphBuildInputV1 {
        match self {
            GraphSourceArtifactV1::FrameSegment {
                source_hash,
                artifact,
            } => {
                let _ = artifact.chunk_rows;
                GraphBuildInputV1::new(GraphBuildSourceKindV1::FrameSegment, *source_hash)
            }
            GraphSourceArtifactV1::ReplayLog {
                source_hash,
                artifact,
            } => {
                let _ = artifact.version;
                GraphBuildInputV1::new(GraphBuildSourceKindV1::ReplayLog, *source_hash)
            }
            GraphSourceArtifactV1::PromptPack {
                source_hash,
                artifact,
            } => {
                let _ = artifact.version;
                GraphBuildInputV1::new(GraphBuildSourceKindV1::PromptPack, *source_hash)
            }
            GraphSourceArtifactV1::ConversationPack {
                source_hash,
                artifact,
            } => {
                let _ = artifact.version;
                GraphBuildInputV1::new(GraphBuildSourceKindV1::ConversationPack, *source_hash)
            }
        }
    }
}

/// Errors produced by graph build helpers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphBuildError {
    /// Builder config is invalid.
    BadConfig,
    /// Too many inputs were supplied.
    TooManyInputs,
    /// Finalized rows exceed the configured row cap.
    TooManyRows,
    /// A row exceeds the configured edge cap.
    TooManyEdges,
    /// Finalized artifact failed validation.
    InvalidOutput,
}

impl core::fmt::Display for GraphBuildError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            GraphBuildError::BadConfig => f.write_str("bad graph build config"),
            GraphBuildError::TooManyInputs => f.write_str("too many graph build inputs"),
            GraphBuildError::TooManyRows => f.write_str("too many graph relevance rows"),
            GraphBuildError::TooManyEdges => f.write_str("too many graph relevance edges"),
            GraphBuildError::InvalidOutput => f.write_str("invalid graph relevance output"),
        }
    }
}

impl std::error::Error for GraphBuildError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct NodeKeyV1 {
    kind: GraphNodeKindV1,
    id: Id64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct EdgeAggV1 {
    weight_q16_sum: u32,
    flags: GraphRelevanceEdgeFlagsV1,
}

fn cmp_edge_canon(a: &GraphRelevanceEdgeV1, b: &GraphRelevanceEdgeV1) -> core::cmp::Ordering {
    match b.weight_q16.cmp(&a.weight_q16) {
        core::cmp::Ordering::Equal => {}
        o => return o,
    }
    match (a.target_kind as u8).cmp(&(b.target_kind as u8)) {
        core::cmp::Ordering::Equal => {}
        o => return o,
    }
    match a.target_id.0.cmp(&b.target_id.0) {
        core::cmp::Ordering::Equal => {}
        o => return o,
    }
    match a.hop_count.cmp(&b.hop_count) {
        core::cmp::Ordering::Equal => {}
        o => return o,
    }
    a.flags.cmp(&b.flags)
}

fn cmp_row_canon(a: &GraphRelevanceRowV1, b: &GraphRelevanceRowV1) -> core::cmp::Ordering {
    match (a.seed_kind as u8).cmp(&(b.seed_kind as u8)) {
        core::cmp::Ordering::Equal => {}
        o => return o,
    }
    a.seed_id.0.cmp(&b.seed_id.0)
}

/// Prepare a deterministic build plan from supported input artifacts.
///
/// Inputs are canonicalized by `(source_kind, source_hash)`, deduplicated, and
/// capped in that order. No source artifact decoding happens in this step.
pub fn prepare_graph_build_plan_v1(
    build_id: Hash32,
    mut inputs: Vec<GraphBuildInputV1>,
    cfg: &GraphBuildConfigV1,
) -> Result<(GraphBuildPlanV1, GraphBuildReportV1), GraphBuildError> {
    cfg.validate()?;
    if inputs.len() > GRAPH_BUILD_V1_MAX_INPUTS {
        return Err(GraphBuildError::TooManyInputs);
    }

    let inputs_seen = if inputs.len() > u32::MAX as usize {
        u32::MAX
    } else {
        inputs.len() as u32
    };
    inputs.sort_unstable();

    let mut deduped: Vec<GraphBuildInputV1> = Vec::with_capacity(inputs.len());
    let mut deduped_count = 0u32;
    let mut prev: Option<GraphBuildInputV1> = None;
    for item in inputs {
        if prev == Some(item) {
            deduped_count = deduped_count.saturating_add(1);
            continue;
        }
        prev = Some(item);
        deduped.push(item);
    }

    let mut kept: Vec<GraphBuildInputV1> = Vec::with_capacity(deduped.len());
    let mut dropped_by_cap = 0u32;
    let mut current_kind: Option<GraphBuildSourceKindV1> = None;
    let mut kept_for_kind = 0u32;
    for item in deduped {
        if kept.len() >= cfg.max_inputs_total as usize {
            dropped_by_cap = dropped_by_cap.saturating_add(1);
            continue;
        }
        if current_kind != Some(item.source_kind) {
            current_kind = Some(item.source_kind);
            kept_for_kind = 0;
        }
        if kept_for_kind >= cfg.max_inputs_per_source_kind {
            dropped_by_cap = dropped_by_cap.saturating_add(1);
            continue;
        }
        kept_for_kind = kept_for_kind.saturating_add(1);
        kept.push(item);
    }

    Ok((
        GraphBuildPlanV1 {
            build_id,
            inputs: kept.clone(),
            max_rows: cfg.max_rows,
            max_edges_per_row: cfg.max_edges_per_row,
            max_terms_per_frame_row: cfg.max_terms_per_frame_row,
            max_entities_per_frame_row: cfg.max_entities_per_frame_row,
        },
        GraphBuildReportV1 {
            inputs_seen,
            inputs_kept: kept_count(&kept),
            inputs_deduped: deduped_count,
            inputs_dropped_by_cap: dropped_by_cap,
        },
    ))
}

fn kept_count(xs: &[GraphBuildInputV1]) -> u32 {
    if xs.len() > u32::MAX as usize {
        u32::MAX
    } else {
        xs.len() as u32
    }
}

fn collect_unique_terms(ids: &[crate::frame::TermFreq], cap: usize) -> Vec<Id64> {
    let mut out: Vec<Id64> = ids.iter().map(|tf| tf.term.0).collect();
    out.sort_unstable_by_key(|id| id.0);
    out.dedup_by_key(|id| id.0);
    if out.len() > cap {
        out.truncate(cap);
    }
    out
}

fn collect_unique_entities(
    who: Option<EntityId>,
    what: Option<EntityId>,
    ids: &[EntityId],
    cap: usize,
) -> Vec<Id64> {
    let mut out: Vec<Id64> = Vec::with_capacity(ids.len().saturating_add(2));
    if let Some(v) = who {
        out.push(v.0);
    }
    if let Some(v) = what {
        out.push(v.0);
    }
    for v in ids {
        out.push(v.0);
    }
    out.sort_unstable_by_key(|id| id.0);
    out.dedup_by_key(|id| id.0);
    if out.len() > cap {
        out.truncate(cap);
    }
    out
}

fn add_edge_agg(
    rows: &mut BTreeMap<NodeKeyV1, BTreeMap<NodeKeyV1, EdgeAggV1>>,
    seed: NodeKeyV1,
    target: NodeKeyV1,
    weight_q16: u16,
    flags: GraphRelevanceEdgeFlagsV1,
) {
    if seed == target {
        return;
    }
    let row = rows.entry(seed).or_default();
    let entry = row.entry(target).or_insert(EdgeAggV1 {
        weight_q16_sum: 0,
        flags: 0,
    });
    entry.weight_q16_sum = entry.weight_q16_sum.saturating_add(weight_q16 as u32);
    entry.flags |= flags;
    entry.flags &= GRAPH_RELEVANCE_EDGE_FLAGS_V1_ALL;
}

fn add_symmetric_edge_agg(
    rows: &mut BTreeMap<NodeKeyV1, BTreeMap<NodeKeyV1, EdgeAggV1>>,
    a: NodeKeyV1,
    b: NodeKeyV1,
    weight_q16: u16,
) {
    let flags = GREDGE_FLAG_SYMMETRIC;
    add_edge_agg(rows, a, b, weight_q16, flags);
    add_edge_agg(rows, b, a, weight_q16, flags);
}

fn mine_frame_segment_into_aggs(
    rows: &mut BTreeMap<NodeKeyV1, BTreeMap<NodeKeyV1, EdgeAggV1>>,
    segment: &FrameSegmentV1,
    cfg: &GraphBuildPlanV1,
) {
    let row_count = segment.row_count();
    let row_cap = if row_count > u32::MAX as u64 {
        u32::MAX
    } else {
        row_count as u32
    };
    for row_ix in 0..row_cap {
        let row = match segment.get_row(row_ix) {
            Some(v) => v,
            None => continue,
        };
        let terms = collect_unique_terms(&row.terms, cfg.max_terms_per_frame_row as usize);
        let entities = collect_unique_entities(
            row.who,
            row.what,
            &row.entity_ids,
            cfg.max_entities_per_frame_row as usize,
        );
        let verb: Option<Id64> = row.verb.map(|v: VerbId| v.0);

        for i in 0..terms.len() {
            for j in (i + 1)..terms.len() {
                add_symmetric_edge_agg(
                    rows,
                    NodeKeyV1 {
                        kind: GraphNodeKindV1::Term,
                        id: terms[i],
                    },
                    NodeKeyV1 {
                        kind: GraphNodeKindV1::Term,
                        id: terms[j],
                    },
                    GRAPH_BUILD_V1_DEFAULT_TERM_TERM_WEIGHT_Q16,
                );
            }
        }

        for term_id in &terms {
            let term_node = NodeKeyV1 {
                kind: GraphNodeKindV1::Term,
                id: *term_id,
            };
            for entity_id in &entities {
                add_symmetric_edge_agg(
                    rows,
                    term_node,
                    NodeKeyV1 {
                        kind: GraphNodeKindV1::Entity,
                        id: *entity_id,
                    },
                    GRAPH_BUILD_V1_DEFAULT_TERM_ENTITY_WEIGHT_Q16,
                );
            }
            if let Some(verb_id) = verb {
                add_symmetric_edge_agg(
                    rows,
                    term_node,
                    NodeKeyV1 {
                        kind: GraphNodeKindV1::Verb,
                        id: verb_id,
                    },
                    GRAPH_BUILD_V1_DEFAULT_TERM_VERB_WEIGHT_Q16,
                );
            }
        }

        if let Some(verb_id) = verb {
            let verb_node = NodeKeyV1 {
                kind: GraphNodeKindV1::Verb,
                id: verb_id,
            };
            for entity_id in &entities {
                add_symmetric_edge_agg(
                    rows,
                    NodeKeyV1 {
                        kind: GraphNodeKindV1::Entity,
                        id: *entity_id,
                    },
                    verb_node,
                    GRAPH_BUILD_V1_DEFAULT_ENTITY_VERB_WEIGHT_Q16,
                );
            }
        }
    }
}

/// Mine conservative graph rows from selected decoded source artifacts.
///
/// Only inputs present in `plan.inputs` are considered. Sources that are not in
/// the plan or that do not currently contribute graph rows are ignored.
pub fn mine_graph_rows_from_sources_v1<'a>(
    plan: &GraphBuildPlanV1,
    sources: &[GraphSourceArtifactV1<'a>],
) -> Result<Vec<GraphRelevanceRowV1>, GraphBuildError> {
    let mut planned: BTreeMap<GraphBuildInputV1, ()> = BTreeMap::new();
    for item in &plan.inputs {
        planned.insert(*item, ());
    }

    let mut seen: BTreeMap<GraphBuildInputV1, ()> = BTreeMap::new();
    let mut row_aggs: BTreeMap<NodeKeyV1, BTreeMap<NodeKeyV1, EdgeAggV1>> = BTreeMap::new();

    for source in sources {
        let input = source.build_input();
        if !planned.contains_key(&input) {
            continue;
        }
        if seen.insert(input, ()).is_some() {
            continue;
        }
        match source {
            GraphSourceArtifactV1::FrameSegment { artifact, .. } => {
                mine_frame_segment_into_aggs(&mut row_aggs, artifact, plan);
            }
            GraphSourceArtifactV1::ReplayLog { .. }
            | GraphSourceArtifactV1::PromptPack { .. }
            | GraphSourceArtifactV1::ConversationPack { .. } => {}
        }
    }

    let mut rows: Vec<GraphRelevanceRowV1> = Vec::new();
    for (seed, targets) in row_aggs {
        let mut edges: Vec<GraphRelevanceEdgeV1> = Vec::with_capacity(targets.len());
        for (target, agg) in targets {
            if agg.weight_q16_sum == 0 {
                continue;
            }
            let weight_q16 = if agg.weight_q16_sum > u16::MAX as u32 {
                u16::MAX
            } else {
                agg.weight_q16_sum as u16
            };
            edges.push(GraphRelevanceEdgeV1::new(
                target.kind,
                target.id,
                weight_q16,
                1,
                agg.flags & GRAPH_RELEVANCE_EDGE_FLAGS_V1_ALL,
            ));
        }
        edges.sort_by(cmp_edge_canon);
        if edges.len() > plan.max_edges_per_row as usize {
            edges.truncate(plan.max_edges_per_row as usize);
        }
        if edges.is_empty() {
            continue;
        }
        rows.push(GraphRelevanceRowV1 {
            seed_kind: seed.kind,
            seed_id: seed.id,
            edges,
        });
    }

    rows.sort_by(cmp_row_canon);
    if rows.len() > plan.max_rows as usize {
        rows.truncate(plan.max_rows as usize);
    }
    Ok(rows)
}

fn derive_artifact_flags(rows: &[GraphRelevanceRowV1]) -> GraphRelevanceFlagsV1 {
    let mut flags = 0u32;
    for row in rows {
        match row.seed_kind {
            GraphNodeKindV1::Term => flags |= GR_FLAG_HAS_TERM_ROWS,
            GraphNodeKindV1::Entity => flags |= GR_FLAG_HAS_ENTITY_ROWS,
            GraphNodeKindV1::Verb => flags |= GR_FLAG_HAS_VERB_ROWS,
        }
    }
    flags
}

/// Finalize a canonical `GraphRelevanceV1` artifact from mined rows.
pub fn finalize_graph_relevance_v1(
    plan: &GraphBuildPlanV1,
    mut rows: Vec<GraphRelevanceRowV1>,
) -> Result<GraphRelevanceV1, GraphBuildError> {
    if rows.len() > plan.max_rows as usize {
        return Err(GraphBuildError::TooManyRows);
    }
    for row in &rows {
        if row.edges.len() > plan.max_edges_per_row as usize {
            return Err(GraphBuildError::TooManyEdges);
        }
    }
    rows.sort_by(cmp_row_canon);
    for row in &mut rows {
        row.edges.sort_by(cmp_edge_canon);
    }
    let graph = GraphRelevanceV1 {
        version: GRAPH_RELEVANCE_V1_VERSION,
        build_id: plan.build_id,
        flags: derive_artifact_flags(&rows),
        rows,
    };
    match graph.validate() {
        Ok(()) => Ok(graph),
        Err(GraphRelevanceError::TooManyRows) => Err(GraphBuildError::TooManyRows),
        Err(GraphRelevanceError::TooManyEdges) => Err(GraphBuildError::TooManyEdges),
        Err(_) => Err(GraphBuildError::InvalidOutput),
    }
}

/// Return an empty canonical graph artifact for a prepared plan.
pub fn empty_graph_relevance_v1(plan: &GraphBuildPlanV1) -> GraphRelevanceV1 {
    GraphRelevanceV1 {
        version: GRAPH_RELEVANCE_V1_VERSION,
        build_id: plan.build_id,
        flags: 0,
        rows: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{DocId, FrameRowV1, SourceId, TermFreq, TermId};
    use crate::frame_segment::FrameSegmentV1;
    use crate::hash::blake3_hash;

    fn build_id(label: &str) -> Hash32 {
        blake3_hash(label.as_bytes())
    }

    fn term_id(token: &str) -> Id64 {
        crate::tokenizer::term_id_from_token(token, crate::tokenizer::TokenizerCfg::default()).0
    }

    fn entity_id(token: &str) -> Id64 {
        crate::frame::derive_id64(b"entity", token.as_bytes())
    }

    fn verb_id(token: &str) -> Id64 {
        crate::frame::derive_id64(b"verb", token.as_bytes())
    }

    fn frame_input(label: &str) -> GraphBuildInputV1 {
        GraphBuildInputV1::new(GraphBuildSourceKindV1::FrameSegment, build_id(label))
    }

    fn prompt_input(label: &str) -> GraphBuildInputV1 {
        GraphBuildInputV1::new(GraphBuildSourceKindV1::PromptPack, build_id(label))
    }

    #[test]
    fn build_plan_dedups_and_caps_inputs() {
        let cfg = GraphBuildConfigV1 {
            max_inputs_total: 3,
            max_inputs_per_source_kind: 2,
            ..GraphBuildConfigV1::default_v1()
        };
        let inputs = vec![
            prompt_input("prompt-a"),
            frame_input("seg-b"),
            frame_input("seg-a"),
            frame_input("seg-a"),
            frame_input("seg-c"),
        ];
        let (plan, report) =
            prepare_graph_build_plan_v1(build_id("graph-plan"), inputs, &cfg).expect("plan");
        assert_eq!(report.inputs_seen, 5);
        assert_eq!(report.inputs_kept, 3);
        assert_eq!(report.inputs_deduped, 1);
        assert_eq!(report.inputs_dropped_by_cap, 1);
        assert_eq!(
            plan.inputs,
            vec![
                frame_input("seg-a"),
                frame_input("seg-b"),
                prompt_input("prompt-a")
            ]
        );
    }

    #[test]
    fn empty_build_is_canonical() {
        let cfg = GraphBuildConfigV1::default_v1();
        let (plan, _) =
            prepare_graph_build_plan_v1(build_id("empty"), Vec::new(), &cfg).expect("plan");
        let graph = empty_graph_relevance_v1(&plan);
        assert!(graph.rows.is_empty());
        assert_eq!(graph.flags, 0);
        assert!(graph.validate().is_ok());
    }

    #[test]
    fn mine_rows_from_frame_segment_is_canonical() {
        let mut r0 = FrameRowV1::new(DocId(Id64(1)), SourceId(Id64(10)));
        r0.who = Some(EntityId(entity_id("alice")));
        r0.verb = Some(VerbId(verb_id("likes")));
        r0.entity_ids.push(EntityId(entity_id("banana-split")));
        r0.terms.push(TermFreq {
            term: TermId(term_id("banana")),
            tf: 2,
        });
        r0.terms.push(TermFreq {
            term: TermId(term_id("split")),
            tf: 1,
        });
        r0.recompute_doc_len();

        let mut r1 = FrameRowV1::new(DocId(Id64(2)), SourceId(Id64(10)));
        r1.what = Some(EntityId(entity_id("dessert")));
        r1.verb = Some(VerbId(verb_id("likes")));
        r1.terms.push(TermFreq {
            term: TermId(term_id("banana")),
            tf: 1,
        });
        r1.terms.push(TermFreq {
            term: TermId(term_id("dessert")),
            tf: 1,
        });
        r1.recompute_doc_len();

        let seg = FrameSegmentV1::from_rows(&[r0, r1], 2).expect("segment");
        let seg_hash = build_id("frame-seg");
        let cfg = GraphBuildConfigV1::default_v1();
        let (plan, _) = prepare_graph_build_plan_v1(
            build_id("graph-build"),
            vec![GraphBuildInputV1::new(
                GraphBuildSourceKindV1::FrameSegment,
                seg_hash,
            )],
            &cfg,
        )
        .expect("plan");
        let rows = mine_graph_rows_from_sources_v1(
            &plan,
            &[GraphSourceArtifactV1::FrameSegment {
                source_hash: seg_hash,
                artifact: &seg,
            }],
        )
        .expect("mine");
        let graph = finalize_graph_relevance_v1(&plan, rows).expect("finalize");
        assert!(graph.validate().is_ok());
        assert_eq!(
            graph.flags,
            GR_FLAG_HAS_TERM_ROWS | GR_FLAG_HAS_ENTITY_ROWS | GR_FLAG_HAS_VERB_ROWS
        );

        let banana_row = graph
            .rows
            .iter()
            .find(|r| r.seed_kind == GraphNodeKindV1::Term && r.seed_id == term_id("banana"))
            .expect("banana row");
        assert_eq!(banana_row.edges[0].target_kind, GraphNodeKindV1::Verb);
        assert_eq!(banana_row.edges[0].target_id, verb_id("likes"));
        assert_eq!(
            banana_row.edges[0].weight_q16,
            GRAPH_BUILD_V1_DEFAULT_TERM_VERB_WEIGHT_Q16 * 2
        );
        assert_eq!(banana_row.edges[0].hop_count, 1);
        assert_eq!(banana_row.edges[0].flags, GREDGE_FLAG_SYMMETRIC);
        assert!(banana_row
            .edges
            .iter()
            .any(|e| e.target_kind == GraphNodeKindV1::Entity));
        assert!(banana_row
            .edges
            .iter()
            .any(|e| e.target_kind == GraphNodeKindV1::Term));
    }

    #[test]
    fn mine_rows_ignores_unplanned_and_opaque_sources() {
        let mut r0 = FrameRowV1::new(DocId(Id64(1)), SourceId(Id64(10)));
        r0.terms.push(TermFreq {
            term: TermId(term_id("alpha")),
            tf: 1,
        });
        r0.terms.push(TermFreq {
            term: TermId(term_id("beta")),
            tf: 1,
        });
        r0.recompute_doc_len();
        let seg = FrameSegmentV1::from_rows(&[r0], 1).expect("segment");
        let seg_hash = build_id("kept-seg");
        let other_hash = build_id("other-seg");
        let prompt = PromptPack::new(
            7,
            64,
            crate::prompt_pack::PromptIds {
                snapshot_id: [0u8; 32],
                weights_id: [0u8; 32],
                tokenizer_id: [0u8; 32],
            },
        );
        let cfg = GraphBuildConfigV1::default_v1();
        let (plan, _) = prepare_graph_build_plan_v1(
            build_id("graph-build-2"),
            vec![GraphBuildInputV1::new(
                GraphBuildSourceKindV1::FrameSegment,
                seg_hash,
            )],
            &cfg,
        )
        .expect("plan");
        let rows = mine_graph_rows_from_sources_v1(
            &plan,
            &[
                GraphSourceArtifactV1::PromptPack {
                    source_hash: build_id("opaque-prompt"),
                    artifact: &prompt,
                },
                GraphSourceArtifactV1::FrameSegment {
                    source_hash: other_hash,
                    artifact: &seg,
                },
                GraphSourceArtifactV1::FrameSegment {
                    source_hash: seg_hash,
                    artifact: &seg,
                },
            ],
        )
        .expect("mine");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].seed_kind, GraphNodeKindV1::Term);
        assert_eq!(rows[1].seed_kind, GraphNodeKindV1::Term);
    }
}
