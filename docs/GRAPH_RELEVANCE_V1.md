Graph Relevance v1
==================

Purpose
-------
GraphRelevanceV1 is a deterministic, bounded artifact for offline graph-adjacency
hints that may later feed the existing bridge-expansion lane.

This contract is advisory only.
It does not replace lexical retrieval, evidence selection, or planner truth.

When answer retrieval uses `--expand` with `--graph-relevance` and the artifact
yields bounded term candidates, operator answer output includes one `graph_trace ...`
inspect line summarizing the candidate reasons. This line is read-only and does
not add facts or bypass lexical evidence precedence.

v1 scope
--------
- compact seed rows keyed by stable ids
- bounded weighted target edges
- canonical ordering and validation
- content-addressed artifact storage
- offline builder scaffolding and conservative mining helpers
- bounded term-to-term retrieval enrichment is active when answer retrieval uses `--expand` with `--graph-relevance`
- no second graph system

Schema
------
Artifact: `GraphRelevanceV1`
- `version: u32`
- `build_id: Hash32`
- `flags: u32`
- `rows: Vec<GraphRelevanceRowV1>`

Row: `GraphRelevanceRowV1`
- `seed_kind: GraphNodeKindV1`
- `seed_id: Id64`
- `edges: Vec<GraphRelevanceEdgeV1>`

Edge: `GraphRelevanceEdgeV1`
- `target_kind: GraphNodeKindV1`
- `target_id: Id64`
- `weight_q16: u16`
- `hop_count: u8`
- `flags: u8`

Node kinds
----------
- `Term`
- `Entity`
- `Verb`

Artifact flags
--------------
- `GR_FLAG_HAS_TERM_ROWS`
- `GR_FLAG_HAS_ENTITY_ROWS`
- `GR_FLAG_HAS_VERB_ROWS`

Edge flags
----------
- `GREDGE_FLAG_MULTI_HOP`
- `GREDGE_FLAG_SYMMETRIC`

Canonical ordering
------------------
Rows sort by:
- `seed_kind` ascending
- `seed_id` ascending

Edges within a row sort by:
- `weight_q16` descending
- `target_kind` ascending
- `target_id` ascending
- `hop_count` ascending
- `flags` ascending

Validation rules
----------------
- version must be `1`
- row count must be <= `1024`
- per-row edge count must be <= `32`
- rows must be strictly canonical
- edges must be strictly canonical
- edge weights must be non-zero
- hop count must be non-zero
- unknown artifact or edge flags are rejected

Storage
-------
Use `src/graph_relevance_artifact.rs` to store/load the artifact by content hash.

Offline builder helpers
-----------------------
Use `src/graph_build.rs` to:
- prepare a deterministic input plan over supported source families
- mine conservative 1-hop graph rows from `FrameSegmentV1`
- finalize a canonical `GraphRelevanceV1` artifact

Current mining coverage
-----------------------
This checkpoint mines only from `FrameSegmentV1` and uses stored row fields:
- terms -> term rows and term-target edges
- entities -> entity rows and entity-target edges
- verbs -> verb rows and verb-target edges

Current row mining is conservative and direct only:
- term <-> term co-occurrence from the same frame row
- term <-> entity co-occurrence from the same frame row
- term <-> verb co-occurrence from the same frame row
- entity <-> verb co-occurrence from the same frame row

ReplayLog, PromptPack, and ConversationPack are accepted as future source
families in the builder inventory, but do not emit rows yet in v1.

Current status
--------------
The graph relevance stack now includes:
- artifact contract and store helpers
- offline builder helpers and CLI wiring
- conservative automatic mining from `FrameSegmentV1`
- bounded term-to-term retrieval enrichment on the existing bridge-expansion lane

Operators do not need to define relationships manually. The current graph
artifact is derived automatically from stored frame-row content.

CLI builder command
- `build-graph-relevance`
- `answer --expand --graph-relevance <hash32hex>`
