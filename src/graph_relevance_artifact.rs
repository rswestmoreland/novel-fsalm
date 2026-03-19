// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! GraphRelevanceV1 artifact helpers.

use crate::artifact::{ArtifactError, ArtifactStore};
use crate::codec::{DecodeError, EncodeError};
use crate::graph_relevance::GraphRelevanceV1;
use crate::hash::Hash32;
use core::fmt;

/// Errors while storing or loading a GraphRelevanceV1 artifact.
#[derive(Debug)]
pub enum GraphRelevanceArtifactError {
    /// Graph relevance could not be encoded.
    Encode(EncodeError),
    /// Graph relevance could not be decoded.
    Decode(DecodeError),
    /// Artifact store error.
    Store(ArtifactError),
}

impl fmt::Display for GraphRelevanceArtifactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GraphRelevanceArtifactError::Encode(e) => write!(f, "encode: {}", e),
            GraphRelevanceArtifactError::Decode(e) => write!(f, "decode: {}", e),
            GraphRelevanceArtifactError::Store(e) => write!(f, "store: {}", e),
        }
    }
}

impl std::error::Error for GraphRelevanceArtifactError {}

/// Store GraphRelevanceV1 as a content-addressed artifact.
pub fn put_graph_relevance_v1<S: ArtifactStore>(
    store: &S,
    graph_relevance: &GraphRelevanceV1,
) -> Result<Hash32, GraphRelevanceArtifactError> {
    let bytes = graph_relevance
        .encode()
        .map_err(GraphRelevanceArtifactError::Encode)?;
    store.put(&bytes).map_err(GraphRelevanceArtifactError::Store)
}

/// Load GraphRelevanceV1 by content hash.
///
/// Returns None if the artifact is not present in the store.
pub fn get_graph_relevance_v1<S: ArtifactStore>(
    store: &S,
    hash: &Hash32,
) -> Result<Option<GraphRelevanceV1>, GraphRelevanceArtifactError> {
    let bytes_opt = store.get(hash).map_err(GraphRelevanceArtifactError::Store)?;
    let bytes = match bytes_opt {
        Some(b) => b,
        None => return Ok(None),
    };
    let graph_relevance =
        GraphRelevanceV1::decode(&bytes).map_err(GraphRelevanceArtifactError::Decode)?;
    Ok(Some(graph_relevance))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::FsArtifactStore;
    use crate::graph_relevance::{
        GraphNodeKindV1, GraphRelevanceEdgeV1, GraphRelevanceRowV1, GraphRelevanceV1,
        GREDGE_FLAG_SYMMETRIC, GRAPH_RELEVANCE_V1_VERSION, GR_FLAG_HAS_TERM_ROWS,
    };
    use crate::frame::Id64;
    use crate::hash::blake3_hash;

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        let mut base = std::env::temp_dir();
        base.push("fsa_lm_tests");
        base.push(name);
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).expect("tmp create_dir_all");
        base
    }

    #[test]
    fn graph_relevance_artifact_roundtrip() {
        let root = tmp_dir("graph_relevance_artifact_roundtrip");
        let store = FsArtifactStore::new(&root).unwrap();
        let graph = GraphRelevanceV1 {
            version: GRAPH_RELEVANCE_V1_VERSION,
            build_id: blake3_hash(b"graph-build"),
            flags: GR_FLAG_HAS_TERM_ROWS,
            rows: vec![GraphRelevanceRowV1 {
                seed_kind: GraphNodeKindV1::Term,
                seed_id: Id64(1),
                edges: vec![GraphRelevanceEdgeV1::new(
                    GraphNodeKindV1::Term,
                    Id64(2),
                    61000,
                    1,
                    GREDGE_FLAG_SYMMETRIC,
                )],
            }],
        };
        let hash = put_graph_relevance_v1(&store, &graph).expect("put");
        let got = get_graph_relevance_v1(&store, &hash).expect("get").unwrap();
        assert_eq!(got, graph);
        let _ = std::fs::remove_dir_all(&root);
    }
}
