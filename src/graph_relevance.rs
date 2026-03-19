// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! GraphRelevanceV1 schema and codec.
//!
//! Graph relevance is a deterministic, bounded artifact used to hold offline
//! adjacency hints for later bridge-expansion work.
//!
//! v1 is contract-only:
//! - compact seed rows keyed by a stable node id
//! - bounded target edges with fixed-point weights
//! - canonical ordering and validation
//! - no retrieval activation in this module

use crate::codec::{ByteReader, ByteWriter, DecodeError, EncodeError};
use crate::frame::{Id64};
use crate::hash::Hash32;

/// GraphRelevanceV1 schema version.
pub const GRAPH_RELEVANCE_V1_VERSION: u32 = 1;

/// Maximum number of seed rows allowed in v1.
pub const GRAPH_RELEVANCE_V1_MAX_ROWS: usize = 1024;

/// Maximum number of edges allowed per seed row in v1.
pub const GRAPH_RELEVANCE_V1_MAX_EDGES_PER_ROW: usize = 32;

/// Artifact flags for GraphRelevanceV1.
pub type GraphRelevanceFlagsV1 = u32;

/// Artifact includes term-seed rows.
pub const GR_FLAG_HAS_TERM_ROWS: GraphRelevanceFlagsV1 = 1u32 << 0;

/// Artifact includes entity-seed rows.
pub const GR_FLAG_HAS_ENTITY_ROWS: GraphRelevanceFlagsV1 = 1u32 << 1;

/// Artifact includes verb-seed rows.
pub const GR_FLAG_HAS_VERB_ROWS: GraphRelevanceFlagsV1 = 1u32 << 2;

/// Mask of all known artifact flags in v1.
pub const GRAPH_RELEVANCE_FLAGS_V1_ALL: GraphRelevanceFlagsV1 =
    GR_FLAG_HAS_TERM_ROWS | GR_FLAG_HAS_ENTITY_ROWS | GR_FLAG_HAS_VERB_ROWS;

/// Edge flags for GraphRelevanceEdgeV1.
pub type GraphRelevanceEdgeFlagsV1 = u8;

/// Edge was derived through a multi-hop walk rather than a direct relation.
pub const GREDGE_FLAG_MULTI_HOP: GraphRelevanceEdgeFlagsV1 = 1u8 << 0;

/// Edge is known to be symmetric in the offline graph source.
pub const GREDGE_FLAG_SYMMETRIC: GraphRelevanceEdgeFlagsV1 = 1u8 << 1;

/// Mask of all known edge flags in v1.
pub const GRAPH_RELEVANCE_EDGE_FLAGS_V1_ALL: GraphRelevanceEdgeFlagsV1 =
    GREDGE_FLAG_MULTI_HOP | GREDGE_FLAG_SYMMETRIC;

/// Node kind used by GraphRelevanceV1 rows and edges.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(u8)]
pub enum GraphNodeKindV1 {
    /// Seed/target is a retrieval term id.
    Term = 1,
    /// Seed/target is an entity id.
    Entity = 2,
    /// Seed/target is a verb/predicate id.
    Verb = 3,
}

impl GraphNodeKindV1 {
    fn from_u8(v: u8) -> Result<Self, DecodeError> {
        match v {
            1 => Ok(GraphNodeKindV1::Term),
            2 => Ok(GraphNodeKindV1::Entity),
            3 => Ok(GraphNodeKindV1::Verb),
            _ => Err(DecodeError::new("bad GraphNodeKindV1")),
        }
    }
}

/// One bounded graph edge for a seed row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GraphRelevanceEdgeV1 {
    /// Target node kind.
    pub target_kind: GraphNodeKindV1,
    /// Target node id.
    pub target_id: Id64,
    /// Fixed-point weight in Q0.16 form (0..65535).
    pub weight_q16: u16,
    /// Hop count used by the offline walk/mining path.
    pub hop_count: u8,
    /// Edge flags.
    pub flags: GraphRelevanceEdgeFlagsV1,
}

impl GraphRelevanceEdgeV1 {
    /// Construct a graph relevance edge.
    pub fn new(
        target_kind: GraphNodeKindV1,
        target_id: Id64,
        weight_q16: u16,
        hop_count: u8,
        flags: GraphRelevanceEdgeFlagsV1,
    ) -> Self {
        Self {
            target_kind,
            target_id,
            weight_q16,
            hop_count,
            flags,
        }
    }
}

/// One seed row in GraphRelevanceV1.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphRelevanceRowV1 {
    /// Seed node kind.
    pub seed_kind: GraphNodeKindV1,
    /// Seed node id.
    pub seed_id: Id64,
    /// Canonical bounded target edges.
    pub edges: Vec<GraphRelevanceEdgeV1>,
}

/// Canonical graph relevance artifact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphRelevanceV1 {
    /// Schema version.
    pub version: u32,
    /// Stable build id for the artifact.
    pub build_id: Hash32,
    /// Artifact-level row-kind flags.
    pub flags: GraphRelevanceFlagsV1,
    /// Canonical seed rows.
    pub rows: Vec<GraphRelevanceRowV1>,
}

/// GraphRelevanceV1 validation errors.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphRelevanceError {
    /// Unsupported version.
    BadVersion,
    /// Unknown artifact flags were present.
    BadFlags,
    /// Too many rows were provided.
    TooManyRows,
    /// A row exceeded the per-row edge cap.
    TooManyEdges,
    /// Row order is not canonical or seed rows are duplicated.
    RowsNotCanonical,
    /// Edge order is not canonical or a row contains duplicate targets.
    EdgesNotCanonical,
    /// Unknown edge flags were present.
    BadEdgeFlags,
    /// An edge had zero weight.
    ZeroEdgeWeight,
    /// An edge had hop_count zero.
    ZeroHopCount,
}

impl core::fmt::Display for GraphRelevanceError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            GraphRelevanceError::BadVersion => f.write_str("bad graph relevance version"),
            GraphRelevanceError::BadFlags => f.write_str("bad graph relevance flags"),
            GraphRelevanceError::TooManyRows => f.write_str("too many graph relevance rows"),
            GraphRelevanceError::TooManyEdges => f.write_str("too many graph relevance edges"),
            GraphRelevanceError::RowsNotCanonical => {
                f.write_str("graph relevance rows not canonical")
            }
            GraphRelevanceError::EdgesNotCanonical => {
                f.write_str("graph relevance edges not canonical")
            }
            GraphRelevanceError::BadEdgeFlags => f.write_str("bad graph relevance edge flags"),
            GraphRelevanceError::ZeroEdgeWeight => f.write_str("graph relevance edge has zero weight"),
            GraphRelevanceError::ZeroHopCount => f.write_str("graph relevance edge has zero hop count"),
        }
    }
}

impl std::error::Error for GraphRelevanceError {}

fn cmp_row_canon(a: &GraphRelevanceRowV1, b: &GraphRelevanceRowV1) -> core::cmp::Ordering {
    match (a.seed_kind as u8).cmp(&(b.seed_kind as u8)) {
        core::cmp::Ordering::Equal => {}
        o => return o,
    }
    a.seed_id.0.cmp(&b.seed_id.0)
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

impl GraphRelevanceV1 {
    /// Return true if all rows and edges are strictly canonical.
    pub fn is_canonical(&self) -> bool {
        for i in 1..self.rows.len() {
            if cmp_row_canon(&self.rows[i - 1], &self.rows[i]) != core::cmp::Ordering::Less {
                return false;
            }
        }
        for row in &self.rows {
            for i in 1..row.edges.len() {
                if cmp_edge_canon(&row.edges[i - 1], &row.edges[i]) != core::cmp::Ordering::Less {
                    return false;
                }
            }
        }
        true
    }

    /// Validate invariants.
    pub fn validate(&self) -> Result<(), GraphRelevanceError> {
        if self.version != GRAPH_RELEVANCE_V1_VERSION {
            return Err(GraphRelevanceError::BadVersion);
        }
        if (self.flags & !GRAPH_RELEVANCE_FLAGS_V1_ALL) != 0 {
            return Err(GraphRelevanceError::BadFlags);
        }
        if self.rows.len() > GRAPH_RELEVANCE_V1_MAX_ROWS {
            return Err(GraphRelevanceError::TooManyRows);
        }
        if !self.is_canonical() {
            let mut rows_ok = true;
            for i in 1..self.rows.len() {
                if cmp_row_canon(&self.rows[i - 1], &self.rows[i]) != core::cmp::Ordering::Less {
                    rows_ok = false;
                    break;
                }
            }
            if !rows_ok {
                return Err(GraphRelevanceError::RowsNotCanonical);
            }
            return Err(GraphRelevanceError::EdgesNotCanonical);
        }
        for row in &self.rows {
            if row.edges.len() > GRAPH_RELEVANCE_V1_MAX_EDGES_PER_ROW {
                return Err(GraphRelevanceError::TooManyEdges);
            }
            let mut seen_targets: Vec<(u8, u64)> = Vec::with_capacity(row.edges.len());
            for edge in &row.edges {
                if edge.weight_q16 == 0 {
                    return Err(GraphRelevanceError::ZeroEdgeWeight);
                }
                if edge.hop_count == 0 {
                    return Err(GraphRelevanceError::ZeroHopCount);
                }
                if (edge.flags & !GRAPH_RELEVANCE_EDGE_FLAGS_V1_ALL) != 0 {
                    return Err(GraphRelevanceError::BadEdgeFlags);
                }
                let key = (edge.target_kind as u8, edge.target_id.0);
                match seen_targets.binary_search(&key) {
                    Ok(_) => return Err(GraphRelevanceError::EdgesNotCanonical),
                    Err(pos) => seen_targets.insert(pos, key),
                }
            }
        }
        Ok(())
    }

    /// Encode to canonical bytes.
    pub fn encode(&self) -> Result<Vec<u8>, EncodeError> {
        self.validate().map_err(|e| match e {
            GraphRelevanceError::BadVersion => EncodeError::new("bad graph relevance version"),
            GraphRelevanceError::BadFlags => EncodeError::new("bad graph relevance flags"),
            GraphRelevanceError::TooManyRows => EncodeError::new("too many graph relevance rows"),
            GraphRelevanceError::TooManyEdges => EncodeError::new("too many graph relevance edges"),
            GraphRelevanceError::RowsNotCanonical => EncodeError::new("graph relevance rows not canonical"),
            GraphRelevanceError::EdgesNotCanonical => EncodeError::new("graph relevance edges not canonical"),
            GraphRelevanceError::BadEdgeFlags => EncodeError::new("bad graph relevance edge flags"),
            GraphRelevanceError::ZeroEdgeWeight => EncodeError::new("graph relevance edge has zero weight"),
            GraphRelevanceError::ZeroHopCount => EncodeError::new("graph relevance edge has zero hop count"),
        })?;

        let mut cap = 48usize;
        for row in &self.rows {
            cap = cap.saturating_add(24 + row.edges.len().saturating_mul(24));
        }
        let mut w = ByteWriter::with_capacity(cap);
        w.write_u32(self.version);
        w.write_raw(&self.build_id);
        w.write_u32(self.flags);
        w.write_u32(self.rows.len() as u32);
        for row in &self.rows {
            w.write_u8(row.seed_kind as u8);
            w.write_u64(row.seed_id.0);
            w.write_u32(row.edges.len() as u32);
            for edge in &row.edges {
                w.write_u8(edge.target_kind as u8);
                w.write_u64(edge.target_id.0);
                w.write_u16(edge.weight_q16);
                w.write_u8(edge.hop_count);
                w.write_u8(edge.flags);
            }
        }
        Ok(w.into_bytes())
    }

    /// Decode from canonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = ByteReader::new(bytes);
        let version = r.read_u32()?;
        if version != GRAPH_RELEVANCE_V1_VERSION {
            return Err(DecodeError::new("bad graph relevance version"));
        }
        let build_id_b = r.read_fixed(32)?;
        let mut build_id = [0u8; 32];
        build_id.copy_from_slice(build_id_b);
        let flags = r.read_u32()?;
        let rows_n = r.read_u32()? as usize;
        if rows_n > GRAPH_RELEVANCE_V1_MAX_ROWS {
            return Err(DecodeError::new("too many graph relevance rows"));
        }
        let mut rows = Vec::with_capacity(rows_n);
        for _ in 0..rows_n {
            let seed_kind = GraphNodeKindV1::from_u8(r.read_u8()?)?;
            let seed_id = Id64(r.read_u64()?);
            let edges_n = r.read_u32()? as usize;
            if edges_n > GRAPH_RELEVANCE_V1_MAX_EDGES_PER_ROW {
                return Err(DecodeError::new("too many graph relevance edges"));
            }
            let mut edges = Vec::with_capacity(edges_n);
            for _ in 0..edges_n {
                let target_kind = GraphNodeKindV1::from_u8(r.read_u8()?)?;
                let target_id = Id64(r.read_u64()?);
                let weight_q16 = r.read_u16()?;
                let hop_count = r.read_u8()?;
                let flags2 = r.read_u8()?;
                edges.push(GraphRelevanceEdgeV1 {
                    target_kind,
                    target_id,
                    weight_q16,
                    hop_count,
                    flags: flags2,
                });
            }
            rows.push(GraphRelevanceRowV1 {
                seed_kind,
                seed_id,
                edges,
            });
        }
        if r.remaining() != 0 {
            return Err(DecodeError::new("trailing bytes"));
        }
        let out = GraphRelevanceV1 {
            version,
            build_id,
            flags,
            rows,
        };
        out.validate().map_err(|e| match e {
            GraphRelevanceError::BadVersion => DecodeError::new("bad graph relevance version"),
            GraphRelevanceError::BadFlags => DecodeError::new("bad graph relevance flags"),
            GraphRelevanceError::TooManyRows => DecodeError::new("too many graph relevance rows"),
            GraphRelevanceError::TooManyEdges => DecodeError::new("too many graph relevance edges"),
            GraphRelevanceError::RowsNotCanonical => DecodeError::new("graph relevance rows not canonical"),
            GraphRelevanceError::EdgesNotCanonical => DecodeError::new("graph relevance edges not canonical"),
            GraphRelevanceError::BadEdgeFlags => DecodeError::new("bad graph relevance edge flags"),
            GraphRelevanceError::ZeroEdgeWeight => DecodeError::new("graph relevance edge has zero weight"),
            GraphRelevanceError::ZeroHopCount => DecodeError::new("graph relevance edge has zero hop count"),
        })?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::blake3_hash;

    fn sample() -> GraphRelevanceV1 {
        GraphRelevanceV1 {
            version: GRAPH_RELEVANCE_V1_VERSION,
            build_id: blake3_hash(b"graph-build"),
            flags: GR_FLAG_HAS_TERM_ROWS | GR_FLAG_HAS_ENTITY_ROWS,
            rows: vec![
                GraphRelevanceRowV1 {
                    seed_kind: GraphNodeKindV1::Term,
                    seed_id: Id64(10),
                    edges: vec![
                        GraphRelevanceEdgeV1::new(
                            GraphNodeKindV1::Term,
                            Id64(11),
                            60000,
                            1,
                            GREDGE_FLAG_SYMMETRIC,
                        ),
                        GraphRelevanceEdgeV1::new(
                            GraphNodeKindV1::Entity,
                            Id64(12),
                            50000,
                            2,
                            GREDGE_FLAG_MULTI_HOP,
                        ),
                    ],
                },
                GraphRelevanceRowV1 {
                    seed_kind: GraphNodeKindV1::Entity,
                    seed_id: Id64(20),
                    edges: vec![GraphRelevanceEdgeV1::new(
                        GraphNodeKindV1::Term,
                        Id64(21),
                        45000,
                        1,
                        0,
                    )],
                },
            ],
        }
    }

    #[test]
    fn graph_relevance_roundtrip() {
        let g = sample();
        let bytes = g.encode().expect("encode");
        let got = GraphRelevanceV1::decode(&bytes).expect("decode");
        assert_eq!(got, g);
    }

    #[test]
    fn reject_non_canonical_rows() {
        let mut g = sample();
        g.rows.swap(0, 1);
        assert_eq!(g.validate(), Err(GraphRelevanceError::RowsNotCanonical));
    }

    #[test]
    fn reject_non_canonical_edges() {
        let mut g = sample();
        g.rows[0].edges.swap(0, 1);
        assert_eq!(g.validate(), Err(GraphRelevanceError::EdgesNotCanonical));
    }

    #[test]
    fn reject_zero_weight() {
        let mut g = sample();
        g.rows[0].edges[1].weight_q16 = 0;
        assert_eq!(g.validate(), Err(GraphRelevanceError::ZeroEdgeWeight));
    }
}
