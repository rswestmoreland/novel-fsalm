Compaction v1 (Index-only)
=========================

Goal
----
Provide deterministic, offline maintenance for laptop-scale operation by reducing the number of
IndexSegment artifacts referenced by an IndexSnapshot, without changing FrameSegments.

This stage targets interactive use on a resource-constrained laptop (CPU-only) while remaining
compatible with future multi-host clustering.

Scope (v1)
----------
- Index-only compaction: bundles IndexSegments and rewrites IndexSnapshot.
- FrameSegments are not modified; existing EvidenceBundle frame refs remain valid.
- Output is a new IndexSnapshotV1 plus one or more new IndexPackV1 artifacts.


Non-goals (v1)
--------------
- No FrameSegment merges or row remapping.
- No Lexicon compaction.
- No in-place mutation of existing artifacts. Compaction always writes new artifacts.
- No time-based eviction or TTL. No nondeterministic scheduling.

Determinism invariants
----------------------
- Inputs to compaction are explicit: snapshot hash, compaction cfg, and the referenced
 IndexSegment hashes.
- Segment processing order is stable: sort segment hashes ascending (byte order) before any merge.
- Within merged outputs, all ordering rules are canonical and stable (no HashMap iteration order).
- Outputs are canonical encodings: the output artifact hash is a function of the canonical bytes.

Compaction modes
----------------
Index-only compaction (v1)
- Bundle IndexSegments into IndexPack artifacts and rewrite the IndexSnapshot to reference fewer index artifacts.
- FrameSegments are unchanged, so existing evidence row refs (frame_seg,row_ix) remain valid.

Frame+Index compaction (future)
- Merge FrameSegments and/or LexiconSegments.
- Requires a row remap table so older evidence can be translated or treated as version-tied.


Merge planning policy (laptop default)
------------------------------------
For an actively used laptop-scale system, the default plan is to produce multiple medium-sized
output IndexPacks rather than a single monolithic pack. This reduces cold-start latency
spikes and works better with bounded warm caches.

Defaults
- target_bytes_per_out_segment: 64 MiB
- max_out_segments: 8

Rationale
- Avoid a single giant decoded segment that does not fit comfortably in warm caches.
- Reduce worst-case decode and scan time when caches are cold.
- Keep a natural sharding boundary for future multi-host compaction and query prefetch.

Deterministic planner algorithm (v1)
1) Load the input IndexSnapshot and collect its IndexSegment hashes.
2) Sort input segment hashes ascending (byte order).
3) Estimate each input segment size as its encoded artifact byte length.
4) Sweep in sorted order, packing input segments into an output group until adding the next
 segment would exceed target_bytes_per_out_segment.
5) If the plan would exceed max_out_segments, fall back to an even-pack plan:
 - Split the sorted segment list into exactly max_out_segments contiguous groups by count.
 - No reordering beyond the initial sort.

The plan is recorded in the CompactionReportV1 artifact.

IndexPack merge semantics (v1)
------------------------------
In, v1 compaction uses **IndexPackV1**, a bundle artifact that contains multiple
canonical IndexSegmentV1 blobs.

This preserves query behavior exactly because:
- Each bundled IndexSegmentV1 is byte-for-byte the same as the original.
- Each segment keeps its original (frame_seg_hash,row_ix) posting coordinate system.
- Query-time scoring still uses the same per-segment statistics.

The compaction win for v1 is reducing artifact count and disk lookups (fewer files to open,
fewer hashes to fetch). A future v2 may implement true postings-level merges, but v1 keeps
the format and scoring stable.

Snapshot rewrite
----------------
Compaction produces a new IndexSnapshotV1 that references the output IndexPacks.
- The output segment list is ordered canonically (sorted by segment hash ascending).
- The snapshot continues to reference the same FrameSegment set as before.

Artifacts
---------
Compaction writes new artifacts only:
- One or more IndexPackV1 artifacts (bundled segments).
- One IndexSnapshotV1 artifact referencing the bundled segments.
- One CompactionReportV1 artifact describing inputs, cfg, and outputs.
 - schema: src/compaction_report.rs
 - artifact helper: src/compaction_report_artifact.rs

CLI
---------------
 adds a small `compact-index` command that wraps the library API:
- `compact_index_snapshot_v1(store, snapshot_id, cfg)`

Command
- compact-index --root <dir> --snapshot <hash32hex> [--target-bytes <n>] [--max-out-segments <n>] [--dry-run] [--verbose]

Defaults
- --target-bytes 67108864 (64 MiB)
- --max-out-segments 8

Output
- Without --dry-run: prints the new snapshot hash (64 hex chars) to stdout for scripting.
 A report hash and a small summary are printed to stderr.
- With --dry-run: prints a human-readable plan to stdout and writes no artifacts.

Verification and tests
----------------------
- unit tests: fixtures validating planning determinism and query equivalence.
- E2E equivalence test:
 - build index snapshot
 - query and build evidence before compaction
 - compact index
 - query and build evidence after compaction
 - assert identical SearchHits (refs + scores) and evidence items
 - test file: tests/e2e_compact_index_equivalence_smoke.rs

Interaction with caches
-----------------------
Compaction produces new artifact hashes. Warm caches naturally treat these as cold keys.
No invalidation of existing caches is required.

Note: IndexPackV1 is decoded at query time when encountered. The current v1 cached query path
does not insert pack blobs into the IndexSegment cache (because the cache is keyed only by
artifact hash and a pack hash can correspond to multiple frame segments). Pack decode is
cached per query call.

Cluster readiness (future)
-------------------------
The planner groups segments, which is a natural unit of parallel work.
Future cluster mode can distribute group merges across hosts and deterministically reduce
outputs by sorted hash order.
