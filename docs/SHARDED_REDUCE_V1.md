Sharded Reduce V1
===========================

Purpose
-------
 adds a deterministic reduce/merge step that combines per-shard outputs
into a single global view in the primary store root.

 produces:
- N independent shard stores at <root>/shards/<shard_id_hex4>/
- a top-level ShardManifestV1 stored in the primary root

 consumes ShardManifestV1 and produces:
- a merged IndexSnapshotV1 and IndexSigMapV1 stored in the primary root
- a ReduceManifestV1 stored in the primary root
- a deterministic copy of referenced artifacts into the primary root so that
 existing commands (query-index, build-evidence, answer) can run against the
 primary root without needing multi-store logic

Goals
-----
- Deterministic merge: stable bytes and stable hashes for merged outputs.
- Deterministic copy: the primary root contains all artifacts referenced by the
 merged snapshot and sig map.
- Compatibility: existing CLI commands operate on the primary root using the
 merged snapshot id and merged sig map id.
- Auditability: ReduceManifestV1 records inputs, outputs, and copy counts.

Non-goals
--------------------
- True parallel reduce (can be added later; determinism first).
- Network replication/sync.
 For replication of reduced roots, see docs/ARTIFACT_SYNC_V1.md.
- Multi-store query/evidence that reads directly from shard roots.


Inputs
------
ShardManifestV1 in the primary root records per-shard output hashes.
For index reduce, each shard may provide:
- index_snapshot_v1
- index_sig_map_v1

Empty shards are permitted. Shards that omit these tags are skipped.

Reduce strategy (Index reduce)
------------------------------
The reduce-index step:
1) Loads ShardManifestV1 from the primary root.
2) For each shard in ascending shard_id order:
 - loads that shard's IndexSnapshotV1 and IndexSigMapV1 (if present)
 - validates that source_id matches across all participating shards
3) Merges:
 - IndexSnapshotV1 entries: concatenated then canonicalized
 - IndexSigMapV1 entries: concatenated then canonicalized
 Duplicate index_seg across shards is an error.
4) Builds a deterministic set of referenced artifact hashes and copies bytes
 into the primary root:
 - frame segments referenced by the merged snapshot
 - index segments referenced by the merged snapshot
 - segment sig artifacts referenced by the merged sig map
 Copy plan is deterministic: grouped by artifact kind (frame, index, sig),
 sorted by hash within each group, and deduplicated across groups. Copy scan
 order is deterministic: shard_id ascending; within each shard, hashes are
 visited in copy plan order.
5) Stores merged IndexSnapshotV1 and merged IndexSigMapV1 in the primary root.
6) Stores ReduceManifestV1 in the primary root.

Determinism invariants
----------------------
- Merge order is fixed: shard_id ascending.
- Snapshot entries are canonicalized by (frame_seg, index_seg).
- Sig map entries are canonicalized by (index_seg).
- Copy set is deduplicated and processed in fixed group order (frame, index, sig).
- Copy scan order is fixed: shard_id ascending; within each shard scan,
 hashes are visited in copy plan order.
- The resulting merged artifacts have stable bytes and stable hashes.

Performance notes 
----------------------
 is a hot-path cleanup focused on reduce-index performance, with no
behavior or determinism changes:
- Pre-size shard and entry vectors to reduce reallocations.
- Avoid cloning merged entry vectors when storing merged artifacts.
- Use sort_unstable for canonicalization (dedup output is unchanged).

Performance notes 
----------------------
 improves copy-stage I/O locality and reduces failed opens when
searching for referenced artifacts across shard stores:
- Build a deterministic, globally deduplicated copy plan (frame, index, sig).
- Scan shard stores in shard_id order using path existence checks before
 opening/reading bytes, then copy and verify hashes exactly once.
- This changes only the internal copy mechanics; merged artifact bytes and
 hashes remain unchanged.

CLI surface
----------------------
Command:

 reduce-index --root <dir> --manifest <hash32hex> [--out-file <path>]

Behavior:
- Reads ShardManifestV1 from <root>.
- Produces:
 - merged index_snapshot_v1 hash
 - merged index_sig_map_v1 hash
 - reduce_manifest_v1 hash
- Prints three hashes to stdout (one per line):
 1) ReduceManifestV1 hash
 2) merged IndexSnapshotV1 hash
 3) merged IndexSigMapV1 hash
- If --out-file is provided, writes the same three lines to that file.

After reduce-index, existing commands can run on the primary root:

 query-index --root <root> --snapshot <merged_snapshot> --sig-map <merged_sig_map> --text "..."

 build-evidence --root <root> --snapshot <merged_snapshot> --sig-map <merged_sig_map> --text "..." --k <n>...

 answer --root <root>... --snapshot <merged_snapshot> --sig-map <merged_sig_map>...

Examples
-------------------
A small end-to-end demo is provided under examples:

- examples/demo_cmd_reduce_index.bat
- examples/demo_cmd_reduce_index.sh

The demo runs:
- ingest-wiki-sharded
- build-index-sharded
- reduce-index
- a global query-index on the primary root using the merged snapshot ids

Notes
-----
- chooses deterministic copy to the primary root for compatibility.
 A future stage may introduce a composite store or on-demand fetch.
