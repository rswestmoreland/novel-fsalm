Sharded Ingest V1
===========================

Purpose
-------
Sharded ingest partitions ingestion output into N independent shard stores.
Each shard store is a normal artifact store root (content-addressed by hash).

Goals:
- Deterministic partitioning: identical inputs and config yield identical shard
 placement and identical shard outputs.
- Isolation: a shard run writes only inside its shard root.
- Auditability: a top-level manifest records what was produced per shard.
- Foundation for: deterministic reduce/merge across shards.

Non-goals
--------------------
- Parallel execution (can be added later; determinism first).
- Cross-shard retrieval in a single command.
- Network sync/replication.

Shard store layout
------------------
Given a primary store root:

 <root>/shards/<shard_id_hex4>/

Where:
- shard_id_hex4 is a 4-hex-digit, zero-padded shard id (e.g., 0000, 0001, 000f).
- each shard directory is an independent artifact store root.

Examples:
- root/shards/0000/
- root/shards/0001/
- root/shards/000f/

Shard count and id
------------------
- shard_count: N (u16), N >= 1
- shard_id: K (u16), 0 <= K < N

CLI uses:
- --shards N
- --shard-id K

Deterministic shard mapping
---------------------------
Each ingestion "unit" (typically a document or a frame row) is assigned to a
single shard id K.

Mapping function (v1):

1) Choose a deterministic shard key for the unit.
 - For wiki ingest v1: shard_key_u64 = doc_id (DocId)
 - For other ingestors: shard_key_u64 must be documented per source adapter.

2) Compute:
 - h = blake3_hash(encode_u64_le(shard_key_u64)) -> Hash32
 - x = u64_from_first_8_bytes_le(h)
 - shard_id = (x % shard_count) as u16

Notes:
- The mapping does not depend on iteration order, wall clock, or randomness.
- The mapping is stable across platforms.

Isolation rule
--------------
When running an ingest command with --shards N --shard-id K:
- It MUST write artifacts only inside <root>/shards/<K>.
- It MUST NOT write artifacts into other shard roots.
- It MAY write a small log line to stdout/stderr.

Top-level manifest
------------------
After producing shard outputs, the system writes a ShardManifestV1 artifact
into the primary root (not a shard root). The manifest is a deterministic
inventory of shard outputs and is required for reduce/merge.

ShardManifestV1 schema (implemented):
- version: u32 (1)
- shard_count: u16
- mapping_id: a short ASCII tag identifying the mapping (e.g., "doc_id_hash32_v1")
- shards: list of shard entries in ascending order by shard_id
- per_shard entry:
 - shard_id: u16
 - shard_root_rel: string (e.g., "shards/000f")
 - outputs: list of (tag, hash32) pairs, sorted by tag
 (exact tag set is defined when wiring ingest/index for shards)

Determinism invariants
----------------------
- With identical input bytes and identical flags, shard assignment is identical.
- Shard outputs are content-addressed; stable bytes imply stable hashes.
- ShardManifestV1 canonical encoding is stable and sorted.
- The shard list is always sorted ascending by shard_id.

Implemented CLI surface 
----------------------------
 introduces per-shard ingest flags and two convenience drivers.

Per-shard ingest:
- ingest-wiki... --shards N --shard-id K
- ingest-wiki-xml... --shards N --shard-id K

Convenience drivers (sequential):
- ingest-wiki-sharded... --shards N
- ingest-wiki-xml-sharded... --shards N

Driver output:
- The driver writes a ShardManifestV1 artifact into the primary root and prints
 its hash to stdout.
- If --out-file is provided, the driver writes the manifest hash hex plus a
 trailing newline to that file.

ShardManifest output tags :
- wiki_ingest_manifest_v1: per-shard WikiIngestManifestV1 artifact hash.

Next :
- build-index can be run per shard by setting --root <root>/shards/<K>.
- build-index-sharded runs build-index across all shards sequentially and writes
 an updated ShardManifestV1. If --manifest <hash32hex> is provided, the command
 preserves existing non-index outputs and replaces index outputs.

ShardManifest output tags :
- index_snapshot_v1: per-shard IndexSnapshotV1 artifact hash (if any segments).
- index_sig_map_v1: per-shard IndexSigMapV1 artifact hash (if any segments).

Examples 
--------------
The repo includes a small, deterministic demo that runs sharded TSV ingest and
sharded build-index.

- examples/demo_cmd_sharded_ingest.bat
- examples/demo_cmd_sharded_ingest.sh

Per-shard query snippet
-----------------------
 is per-shard only.
To run a query against a single shard store, first build an index snapshot
inside that shard root, then run query-index with the snapshot and sig-map.

Example (shard 0000):

- build-index --root <root>/shards/0000
 - stdout prints the snapshot hash
 - stderr prints index_sig_map=<hash>

- query-index --root <root>/shards/0000 --snapshot <snapshot_hash> --sig-map <sig_map_hash> --text "..."

Notes:
- build-index-sharded updates the top-level ShardManifestV1 inventory for 
- query-index requires an explicit snapshot hash; does not include a
 manifest-inspection CLI.

 (implemented)
----------------------
 adds deterministic reduce/merge across shard outputs to produce a
single global view in the primary root. Use reduce-index to merge per-shard
index outputs and copy referenced artifacts into the primary root.

See:
- docs/SHARDED_REDUCE_V1.md
- docs/ARTIFACT_SYNC_V1.md
