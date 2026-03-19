Operator workflow
===================

This guide describes the current recommended operator workflow for:
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

Lexicon ingest (Wiktionary) demos:
Windows (cmd.exe):
- examples/demo_cmd_ingest_wiktionary_xml.bat
- examples/demo_cmd_workflow_with_lexicon.bat

Linux/WSL:
- examples/demo_cmd_ingest_wiktionary_xml.sh
- examples/demo_cmd_workflow_with_lexicon.sh

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

Optional: lexicon ingest for query expansion
--------------------------------------------
Query expansion uses lexicon artifacts (LexiconSegmentV1 + LexiconSnapshotV1).
These artifacts are not part of ReduceManifestV1, so sync-reduce does not copy
them. To use query expansion on a given root, ingest Wiktionary into that same
root (or otherwise ensure the lexicon artifacts exist in that root).

Lexicon artifacts are not included in ReduceManifestV1 closures. If you replicate
a reduced index to a fresh root, you must also replicate the lexicon closure.

Use sync-lexicon to replicate a LexiconSnapshotV1 plus its referenced
LexiconSegmentV1 artifacts over artifact sync. See docs/LEXICON_SYNC_V1.md.

Example:
- Source: fsa_lm serve-sync --root <src_root> --addr <ip:port>
- Destination: fsa_lm sync-lexicon --root <dst_root> --addr <ip:port> --lexicon-snapshot <hash32hex>

 Ingest Wiktionary and store lexicon artifacts:
 ingest-wiktionary-xml --root <root> (--xml <path> | --xml-bz2 <path>) --segments <n> [--max_pages <n>] [--out-file <path>]

 Output:
 - zero or more lines: segment=<hash32hex>
 - one line: lexicon_snapshot=<hash32hex>

 Validate the snapshot:
 validate-lexicon-snapshot --root <root> --snapshot <lexicon_snapshot_hash>

7) Prompt and answer on the replicated root
 prompt --root <dst_root> "<prompt text>"
 answer --root <dst_root> --prompt <prompt_hash> --snapshot <merged_snapshot_hash> --sig-map <merged_sig_map_hash>... [--cache-stats]

 With query expansion enabled:
 answer --root <dst_root> --prompt <prompt_hash> --snapshot <merged_snapshot_hash> --sig-map <merged_sig_map_hash> --expand --lexicon-snapshot <lexicon_snapshot_hash>...


Conversation workflow (evidence-first with optional advisory artifacts)
---------------------------------------------------------------------
After reduce merge (and optional lexicon replication), operators can build the
conversation-improvement artifacts offline and then opt into them during
answering.

Recommended order:

1) Build bounded Markov phrasing model from replay logs
 build-markov-model --root <root> --replay <hash32hex> [--replay <hash32hex>...] [--out-file <path>]

2) Build exemplar memory from existing artifacts
 build-exemplar-memory --root <root> --prompt <hash32hex> [--conversation-pack <hash32hex>...] [--markov-trace <hash32hex>...] [--out-file <path>]

3) Build graph relevance from stored frame segments
 build-graph-relevance --root <root> --frame-segment <hash32hex> [--frame-segment <hash32hex>...] [--out-file <path>]

4) Answer with optional advisory artifacts
 answer --root <root> --prompt <prompt_hash> --snapshot <merged_snapshot_hash> --sig-map <merged_sig_map_hash>    --expand --lexicon-snapshot <lexicon_snapshot_hash> --graph-relevance <graph_relevance_hash>    --markov-model <markov_model_hash> --exemplar-memory <exemplar_memory_hash>

Default user surface vs operator surface
----------------------------------------
Normal `answer`, `ask`, and `chat` usage defaults to the user-facing surface.
That default hides inspect-only lines and keeps the output conversational.

Use `--presentation operator` when you want the inspect surface for debugging,
release audit work, or workflow diffs. Saved `ask` and `chat` sessions also keep
this presentation choice so resumed operator workflows stay operator-facing until
changed.

Answer inspect lines
--------------------
When the optional conversation artifacts are enabled, answer output may include
stable inspect lines near the top of the operator answer surface:

- `directives ...`
  Final realizer directives used for the answer.
- `routing_trace ...`
  Top planner hint, top forecast intent, key flags, and final tone/style.
- `graph_trace ...`
  Present only when bounded graph candidates are active; summarizes seed and
  candidate counts plus up to 2 candidate reasons.
- `exemplar_match ...`
  Present only when one exemplar row is selected; summarizes the matched row,
  score, support_count, and bounded reason flags.

These lines are read-only diagnostics. They do not replace retrieval, evidence,
or grounded plan refs. They are intended for the operator surface, not the
normal default user surface.

Conversation stack guardrails
-----------------------------
- Lexical retrieval remains primary.
- Graph relevance only enriches candidates; it is not a second fact source.
- Exemplar memory shapes presentation only; it is not evidence.
- Markov phrasing only selects among fixed approved surface templates.
- The evidence bundle remains authoritative for truth and for grounded plan refs.

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
- Fix: re-run reduce-index (or run-workflow) using the current build to produce
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
 End-to-end: run-workflow -> serve-sync/sync-reduce -> query-index -> answer

- tests/sync_resilience_regressions_v1.rs
 Sync resilience matrix: timeout, disconnect, already-present fast path, and
 batch overlap repeatability.

Use tools/check_warnings.(bat|sh) to enforce warning-zero builds via
cargo test --all-targets with -Dwarnings.
