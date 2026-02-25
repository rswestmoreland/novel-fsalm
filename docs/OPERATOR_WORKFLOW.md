 operator guide (shard -> reduce -> replicate -> query/answer)
====================================================================

This guide describes the recommended workflow for operators:
- Sharded ingest
- Deterministic reduce merge
- Manifest-driven artifact replication over TCP
- Query and answer on the replicated root

The workflow is deterministic: artifact identities are content hashes, manifests
are stable-ordered, and replication is driven by ReduceManifestV1 outputs.

Quickstart scripts
------------------
The /examples directory contains copy/paste-friendly scripts that run the
full workflow on a tiny deterministic TSV fixture:

Windows (cmd.exe):
- examples/demo_cmd_reduce_index.bat
 Sharded ingest + build-index-sharded + reduce-index + query snippet.
- examples/demo_cmd_sync_reduce.bat
 Runs on a source root, serves sync, sync-reduce into a fresh
 destination root, then runs a query snippet on the destination root.

Linux/WSL:
- examples/demo_cmd_reduce_index.sh
- examples/demo_cmd_sync_reduce.sh

Most scripts accept environment variable overrides (ROOT, SRC_ROOT, DST_ROOT,
SHARDS, PORT, RW_TIMEOUT_MS, KEEP_TMP, EXE).

Manual workflow (CLI)
---------------------
The following is a minimal manual outline. Replace paths and counts as needed.

1) Sharded ingest (build ShardManifestV1)
 ingest-wiki-sharded --root <root> --dump <dump.tsv> --shards <n>... [--out-file <path>]

 Output: a ShardManifestV1 hash (written to --out-file if provided).

2) Build per-shard index snapshots (ShardManifestV1 -> ShardManifestV1)
 build-index-sharded --root <root> --shards <n> --manifest <ingest_manifest_hash>... [--out-file <path>]

 Output: a ShardManifestV1 hash for the index stage.

3) Reduce merge to a primary root (ShardManifestV1 -> ReduceManifestV1 + merged ids)
 reduce-index --root <root> --manifest <index_manifest_hash> [--out-file <path>]

 Output (three lines, when --out-file is provided):
 - ReduceManifestV1 hash
 - merged IndexSnapshotV1 hash
 - merged IndexSigMapV1 hash

4) Serve artifacts for replication (source side)
 serve-sync --root <src_root> --addr <ip:port> [--rw_timeout_ms <n>]

5) Replicate reduce outputs into a fresh root (destination side)
 sync-reduce --root <dst_root> --addr <ip:port> --reduce-manifest <reduce_manifest_hash> [--rw_timeout_ms <n>] [--out-file <path>]

 Output: a single stats line (written to --out-file if provided), for example:
 sync_stats needed_total=<n> already_present=<n> fetched=<n> bytes_fetched=<n>

6) Query on the replicated root
 query-index --root <dst_root> --snapshot <merged_snapshot_hash> --sig-map <merged_sig_map_hash> --text "<query>" --k <n> [--cache-stats]

7) Prompt and answer on the replicated root
 prompt --root <dst_root> --text "<prompt text>"...
 answer --root <dst_root> --prompt <prompt_hash> --snapshot <merged_snapshot_hash> --sig-map <merged_sig_map_hash>... [--cache-stats]

Common failure modes
--------------------
Connection refused / cannot connect
- Ensure serve-sync is running on the source and that the --addr matches.
- Check the port is available and not blocked by a firewall.
- On Windows, use 127.0.0.1:<port> for local runs.

Timeouts
- sync-reduce and serve-sync accept --rw_timeout_ms.
- If running over slow storage or a constrained environment, increase the timeout.
 Setting --rw_timeout_ms 0 disables timeouts.

"proto: reduce missing index_snapshot_v1"
- This indicates the ReduceManifestV1 was produced by an older build that used
 different output tags.
- Fix: re-run reduce-index (or run-phase6) using the current build to produce
 a new ReduceManifestV1.

Stale roots / leftover artifacts
- The scripts default to deleting temporary roots unless KEEP_TMP=1.
- For manual runs, remove the root directory to start from a clean state.

Debug bundle export
-------------------------------
When reporting issues, export a debug bundle from the relevant root:

 export-debug-bundle --root <dir> --out <path.zip> [--include-hash <hash32hex>...]

By default the bundle contains metadata (build info, small root files, artifact
index samples) but does NOT include raw artifact bytes unless requested via
--include-hash.

Regression locks
----------------
 adds two operator-focused regression tests:

- tests/operator_workflow_golden_pack_v1.rs
 End-to-end: run-phase6 -> serve-sync/sync-reduce -> query-index -> answer

- tests/sync_resilience_regressions_v1.rs
 Sync resilience matrix: timeout, disconnect, already-present fast path, and
 batch overlap repeatability.

Use tools/check_warnings.(bat|sh) to enforce warning-zero builds via
cargo test --all-targets with -Dwarnings.
