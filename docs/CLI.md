CLI
==============

Purpose
-------
The CLI is the primary developer interface:
- create and inspect artifacts
- build PromptPack artifacts from text
- decode ReplayLog artifacts
- provide a baseline workflow before the full pipeline exists

Note:
- The CLI usage string printed by `fsa_lm` (run with no args or -h) is the authoritative
 reference for exact flags. This document is a human summary and is kept in sync
 as part of 

Core commands (introduced in )
----------------------------------------
- fsa_lm hash [--file <path>]
 Compute BLAKE3 hash of bytes from a file or stdin.

- fsa_lm put [--root <dir>] [--file <path>]
 Store bytes as an artifact in the filesystem store. Prints hash hex.

- fsa_lm get [--root <dir>] <hash_hex>
 Fetch artifact bytes and write to stdout.

- fsa_lm prompt [--root <dir>] [--seed <u64>] [--max_tokens <u32>] [--role <role>] <text>
 Build a PromptPack with a single message. Stores the PromptPack bytes as an artifact.
 Prints the PromptPack hash hex.

- fsa_lm replay-decode [--root <dir>] <hash_hex>
 Load a ReplayLog artifact by hash and print a human-readable summary.

- fsa_lm serve [--root <dir>] [--addr <ip:port>]
 Start a TCP server exposing Put/Get artifact operations using framed binary messages.

- fsa_lm serve-sync [--root <dir>] [--addr <ip:port>] [--max_chunk_kb <n>] [--max_artifact_mb <n>] [--rw_timeout_ms <n>]
 Start an Artifact Sync V1 server exposing streaming GET for verified replication.
 See docs/ARTIFACT_SYNC_V1.md.

- fsa_lm send-put [--addr <ip:port>] [--file <path>]
 Send a Put request to a server and print the returned hash.

- fsa_lm send-get [--addr <ip:port>] <hash_hex>
 Send a Get request to a server and write returned bytes to stdout.

- fsa_lm sync-reduce --root <dir> --addr <ip:port> --reduce-manifest <hash32hex> [--out-file <path>] [--max_chunk_kb <n>] [--max_artifact_mb <n>] [--rw_timeout_ms <n>]
 Replicate a ReduceManifestV1 and all referenced artifacts into the local root.
 This is intended to replicate a reduced primary root to another machine.
 See docs/ARTIFACT_SYNC_V1.md.

 Output:
 - One line with sync stats:
 needed_total=<n> already_present=<n> fetched=<n> bytes_fetched=<n>

 Notes:
 - --rw_timeout_ms sets the socket read/write timeout (default 30000 ms). Set to 0 to disable.

- fsa_lm sync-reduce-batch --root <dir> --addr <ip:port> --reduce-manifests <path> [--out-file <path>] [--max_chunk_kb <n>] [--max_artifact_mb <n>] [--rw_timeout_ms <n>]
 Replicate multiple ReduceManifestV1 closures in one TCP session.
 The file given by --reduce-manifests contains one 64-hex hash per line.
 Blank lines and lines starting with '#' are ignored.

 Output:
 - First line global sync stats for the union closure:
 needed_total=<n> already_present=<n> fetched=<n> bytes_fetched=<n> manifests=<n>
 - Then one line per manifest (in input order):
 manifest=<hash32hex> needed_total=<n>

Notes
-----
- This is a prototype CLI with manual argument parsing (no clap).
- Determinism is defined by artifact bytes and hashes, not by console output formatting.

Replay commands
--------------

- replay-new [--root <dir>]
 - Create an empty ReplayLog artifact and print its hash.

- replay-add-prompt [--root <dir>] <replay_hash_hex> <prompt_hash_hex> [--name <step_name>]
 - Load ReplayLog by hash, append a prompt step that outputs the PromptPack hash,
 store the new ReplayLog as an artifact, and print the new hash.
 - The log is immutable; each append produces a new hash.


Frame and ingestion commands
---------------------------

- frame-seg-demo [--root <dir>] [--text <text>] [--chunk_rows <u32>]
 Build a single FrameSegmentV1 from demo text and store it as an artifact.
 Prints the segment hash.

- frame-seg-show [--root <dir>] <segment_hash_hex>
 Load a FrameSegmentV1 artifact and print a small summary.

- ingest-wiki --dump <path> [--root <dir>] [--seg_mb <u32>] [--row_kb <u32>] [--chunk_rows <u32>] [--max_docs <u64>]
 Ingest a Wikipedia TSV dump (title + text) into FrameSegmentV1 artifacts.
 Prints the manifest hash.
 See docs/INGEST_WIKI.md for details.

Sharded ingest
-----------------------
These flags and commands are introduced in 

Per-shard ingest:
- ingest-wiki --dump <path> [--root <dir>] [--seg_mb <u32>] [--row_kb <u32>] [--chunk_rows <u32>] [--max_docs <u64>] [--shards <n> --shard-id <k>]
 Add --shards <n> --shard-id <k> to run TSV ingest for a single shard. Writes only to <root>/shards/<k_hex4>/.

- ingest-wiki-xml (--xml <path> | --xml-bz2 <path>) [--root <dir>] [--seg_mb <u32>] [--row_kb <u32>] [--chunk_rows <u32>] [--max_docs <u64>] [--shards <n> --shard-id <k>]
 Add --shards <n> --shard-id <k> to run XML ingest for a single shard. Writes only to <root>/shards/<k_hex4>/.

Convenience drivers (sequential):
- ingest-wiki-sharded --dump <path> --shards <n> [--root <dir>] [--seg_mb <u32>] [--row_kb <u32>] [--chunk_rows <u32>] [--max_docs <u64>] [--out-file <path>]
 Runs shard-id 0..n-1 and writes a ShardManifestV1 into the primary root.

- ingest-wiki-xml-sharded (--xml <path> | --xml-bz2 <path>) --shards <n> [--root <dir>] [--seg_mb <u32>] [--row_kb <u32>] [--chunk_rows <u32>] [--max_docs <u64>] [--out-file <path>]
 Runs shard-id 0..n-1 and writes a ShardManifestV1 into the primary root.

See docs/SHARDED_INGEST_V1.md.

Sharded reduce
-------------------------
 adds a deterministic reduce/merge step that produces a global view in
the primary root.

- reduce-index --root <dir> --manifest <hash32hex> [--out-file <path>]
 Merge per-shard index outputs referenced by ShardManifestV1 and copy
 referenced artifacts into the primary root.

 Output:
 - Prints three hashes to stdout (one per line):
 1) ReduceManifestV1 hash
 2) merged IndexSnapshotV1 hash
 3) merged IndexSigMapV1 hash
 - If --out-file is set, writes the same three lines to that file.

See docs/SHARDED_REDUCE_V1.md.

Example scripts:
- examples/demo_cmd_reduce_index.bat
- examples/demo_cmd_reduce_index.sh

Orchestrator
-------------------------
 adds a sequential "one command" driver for the pipeline.
It runs sharded ingest , sharded index build , and reduce-index 
in one process.

- run-phase6 --root <dir> --dump <path> --shards <n> [--seg_mb <u32>] [--row_kb <u32>] [--chunk_rows <u32>] [--max_docs <u64>] [--out-file <path>] [--sync-addr <ip:port> --sync-root <dir>] [--max_chunk_kb <n>] [--max_artifact_mb <n>] [--rw_timeout_ms <n>]
 Build a reduced primary root from a TSV dump.

 Output (stdout and --out-file):
 - key=value lines:
 shard_manifest_ingest=<hash32hex>
 shard_manifest_index=<hash32hex>
 reduce_manifest=<hash32hex>
 merged_snapshot=<hash32hex>
 merged_sig_map=<hash32hex>

 - If --sync-addr and --sync-root are set, one additional line is appended:
 sync_stats needed_total=<n> already_present=<n> fetched=<n> bytes_fetched=<n>

 Optional sync client step (assumes serve-sync is already running elsewhere):
 - add: --sync-addr <ip:port> --sync-root <dir> [--max_chunk_kb <n>] [--max_artifact_mb <n>] [--rw_timeout_ms <n>]

See docs/SHARDED_INGEST_V1.md, docs/SHARDED_REDUCE_V1.md, and docs/ARTIFACT_SYNC_V1.md.

Debug bundle
------------------------
 adds a small debug bundle exporter for sharing metadata and manifests.
By default it does NOT include raw artifact bytes.

- export-debug-bundle --root <dir> --out <path> [--include-hash <hash32hex>...]
 Write a zip archive containing:
 - INFO.txt (build + environment summary)
 - ROOT_LISTING.txt (top-level listing)
 - ARTIFACT_INDEX.txt (artifact counts + samples)
 - root_files/*.txt (small root text files, size-capped)
 - artifacts/*.bin (only for hashes passed via --include-hash)

 Output:
 - Prints the output path to stdout.

Artifact sync
-----------------------
 adds deterministic replication of reduced stores over TCP.

Example scripts:
- examples/demo_cmd_sync_reduce.bat
- examples/demo_cmd_sync_reduce.sh

- ingest-wiki-xml: ingest Wikipedia XML dump (docs/INGEST_WIKI_XML.md)


`ingest-wiki-xml` accepts either `--xml <path>` or `--xml-bz2 <path>`.

Lexicon commands
----------------

- build-lexicon-snapshot --root <dir> --segment <hash32hex> [--segment <hash32hex>...] [--out-file <path>]
 Build a LexiconSnapshotV1 manifest from a list of LexiconSegmentV1 artifact hashes.

 Behavior:
 - Each `--segment` hash must exist and decode as LexiconSegmentV1.
 - The snapshot is stored as an artifact and its hash is printed to stdout.
 - If `--out-file` is set, the canonical snapshot bytes are also written to that path.

 Notes:
 - (Wiktionary ingest) will produce LexiconSegment artifacts and call this command.
 - LexiconSnapshot is a small deterministic inventory; it does not build postings.

- validate-lexicon-snapshot --root <dir> --snapshot <hash32hex>
 Validate a LexiconSnapshotV1 inventory.

 Behavior:
 - Loads the LexiconSnapshotV1 by hash.
 - Verifies that each referenced segment exists and decodes.
 - Enforces the ownership rule: term owners must be disjoint across segments.
 - Prints a stable summary on success; returns non-zero on failure.


Pragmatics commands
------------------

- build-pragmatics --root <dir> --prompt <hash32hex> [--source-id <u64>] [--tok-max-bytes <n>] [--out-file <path>]
 Build PragmaticsFrameV1 artifacts for each message in a PromptPack.

 Behavior:
 - Loads the PromptPack by hash from the artifact store.
 - Runs the rules-first extractor.
 - Stores one PragmaticsFrameV1 per message in message order (msg_ix).
 - Prints each frame hash (one per line) to stdout.
 - If --out-file is set, writes the same list of hashes to that file.

 Notes:
 - source_id defaults to 1 if not provided.
 - tok-max-bytes controls token truncation for cue matching (default: 64).

Index and evidence commands
---------------------------

- build-index [--root <dir>]
 Scan the artifact store for FrameSegmentV1 artifacts, build IndexSegmentV1 artifacts,
 and write an IndexSnapshotV1 artifact. Prints the snapshot hash hex.

 Notes:
 - v1 requires a single source_id per snapshot (mixed sources are rejected).
 - This is a prototype scan; later stages will use manifests and explicit segment lists.

- build-index-sharded --shards <n> [--root <dir>] [--manifest <hash32hex>] [--out-file <path>]
 Run build-index across all shards sequentially (root/shards/<id>) and write a new
 ShardManifestV1 to the primary root.

 Notes:
 - If --manifest is provided, non-index outputs are preserved and the index outputs
 (index_snapshot_v1, index_sig_map_v1) are replaced.
 - Shards with no FrameSegmentV1 artifacts produce no index outputs.



- query-index --root <dir> --snapshot <hash32hex> [--sig-map <hash32hex>] --text <string> [--k <n>] [--meta] [--cache-stats]
 Query an IndexSnapshotV1 for a text string and print ranked hits.

 Artifacts:
 - Stores a query-id blob (raw bytes) and a HitListV1 artifact.
 - Emits a ReplayLog step "retrieve-v1" whose outputs include the HitList hash.

 Output format (one hit per line):
 - score<TAB>frame_segment_hash<TAB>row_ix

 Flags:
 - --sig-map enables signature gating (skip closure) when paired with a compatible IndexSigMapV1.
 - --meta enables metaphone expansion for query terms.
 - --cache-stats prints cache statistics to stderr after the command completes.

- build-evidence --root <dir> --snapshot <hash32hex> [--sig-map <hash32hex>] --text <string> [--k <n>] [--meta] [--max_items <n>] [--max_bytes <n>] [--no_sketch] [--no_verify] [--score_model <id>] [--verbose] [--cache-stats]
 Query an IndexSnapshotV1 and build an EvidenceBundleV1 artifact from the top hits.
 Prints the EvidenceBundle hash hex.

 Flags:
 - --max_items caps the number of evidence items stored in the bundle (default: k).
 - --max_bytes sets a strict byte budget for the bundle including sketches (default: 65536).
 Use 0 to disable the byte budget.
 - --no_sketch disables row sketches.
 - --no_verify disables segment/row verification (sketching still requires loading rows).
 - --score_model sets score_model_id in the bundle (default: 0).
 - --verbose prints a brief summary to stderr.
 - --cache-stats prints cache statistics to stderr after the command completes.


Answer command
--------------

- answer --root <dir> --prompt <hash32hex> --snapshot <hash32hex> [--sig-map <hash32hex>] [--pragmatics <hash32hex>...] [--k <n>] [--meta] [--max_terms <n>] [--no_ties] [--expand --lexicon-snapshot <hash32hex>] [--plan_items <n>] [--verify-trace <0|1>] [--markov-model <hash32hex>] [--markov-max-choices <n>] [--out-file <path>]
 Run the full evidence-first answering loop.

 Behavior (high level):
 - Retrieves hits from the snapshot (optionally signature-gated with --sig-map).
 - Builds an EvidenceBundleV1 from top hits.
 - Plans an answer from evidence (optionally guided by lexicon expansion).
 - Realizes an answer with optional quality-gate features (directives, hints/forecast, Markov opener).

 Notes:
 - For a detailed pipeline overview, see docs/ANSWERING_LOOP.md.
 - Use --out-file to write the full output for scripting.

- compact-index --root <dir> --snapshot <hash32hex> [--target-bytes <n>] [--max-out-segments <n>] [--dry-run] [--verbose]
 Run deterministic index compaction for a snapshot. Compaction writes new artifacts only.

 Behavior:
 - Without --dry-run: writes one or more IndexPackV1 artifacts, writes a new IndexSnapshotV1 that
 references those packs, writes a CompactionReportV1, and prints the new snapshot hash to stdout.
 - With --dry-run: prints a human-readable plan and does not write any artifacts.

 Defaults (laptop):
 - --target-bytes 67108864 (64 MiB)
 - --max-out-segments 8

 Notes:
 - A report hash is printed to stderr when artifacts are written.
 - Use --verbose to print group breakdown to stderr (non-dry-run) or stdout (dry-run).

Cache tuning
------------
For `--cache-stats` and cache sizing knobs, see docs/CACHES_V1.md.

Scale demo command (Track C)
----------------------------

- scale-demo [--seed <u64>] [--docs <n>] [--queries <n>] [--min_doc_tokens <n>] [--max_doc_tokens <n>] [--vocab <n>] [--query_tokens <n>] [--tie_pair <0|1>] [--ingest <0|1>] [--build_index <0|1>] [--prompts <0|1>] [--evidence <0|1>] [--answer <0|1>] [--root <dir>] [--out-file <path>]

 Purpose:
 - Run a deterministic synthetic workload through ingest, indexing, prompt pack generation, evidence building, and answering.

 Stage flags:
 - --ingest 1 writes FrameSegment artifacts and an ingest manifest.
 - --build_index 1 builds IndexSnapshot + SegmentSig + IndexSigMap (requires --ingest 1).
 - --prompts 1 stores one PromptPack per query (requires --ingest 1).
 - --evidence 1 stores one EvidenceBundleV1 per query (requires --build_index 1).
 - --answer 1 stores one realized answer per query plus PlannerHints, Forecast, and MarkovTrace artifacts (requires --evidence 1).

 Output:
 - Prints one line per enabled stage, followed by a final line beginning with `scale_demo_scale_report_v3` when --ingest 1.
 - The answers stage line begins with `scale_demo_answers_v3` and includes planner_hints, forecasts, and markov_traces counts/hashes.
 - Use --out-file to write the full output to a file.

 See:
 - docs/SCALE_DEMO.md
 - docs/SCALE_DEMO_RUNBOOK.md
 - docs/SCALE_DEMO_MEMORY.md
 - docs/SCALE_DEMO_REGRESSION_PACK.md
 - docs/SCALE_REPORT_V1.md

 Examples:
 - examples/demo_cmd_scale_demo_full_loop.bat
 - examples/demo_cmd_scale_demo_full_loop.sh

Markov training command
-------------------------------

- build-markov-model --root <dir> --replay <hash32hex> [--replay <hash32hex>...] [--replay-file <path>] [--max-replays <n>] [--max-traces <n>] [--order <n>] [--max-next <n>] [--max-states <n>] [--out-file <path>]

 Purpose:
 - Offline build of a MarkovModelV1 artifact from MarkovTraceV1 artifacts referenced by replay logs.

 Behavior:
 - Collects replay logs from --replay/--replay-file, then sorts and dedups replay hashes (deterministic order).
 - Loads each ReplayLog, collects all markov-trace-v1 step outputs, sorts and dedups trace hashes, and trains a bounded model deterministically.
 - If --max-replays or --max-traces is set (>0), truncates the sorted unique lists to the first N items.
 - Stores one MarkovModelV1 artifact and prints a single summary line beginning with `markov_model_v1`.
 - If --out-file is set, writes the same summary line to that path.

 Notes:
 - This is a rules-first offline trainer; it does not run online during answering.
 - The corpus_hash binds the training config and the set of trace hashes (sorted, unique, post-truncation).
 - The printed summary line includes stable list hashes (replay_list_hash, trace_list_hash) and first/last for reproducibility.

- inspect-markov-model --root <dir> --model <hash32hex> [--top-states <n>] [--top-next <n>] [--out-file <path>]

 Purpose:
 - Inspect a stored MarkovModelV1 artifact and print a stable summary line.

 Behavior:
 - Loads the MarkovModelV1 from the artifact store by hash and validates invariants.
 - Prints one summary line beginning with `markov_model_inspect_v1`.
 - If --top-states > 0, prints up to N additional `markov_model_state_v1` lines ranked by outgoing count.
 - If --top-next > 0, includes up to N next entries per printed state (already canonical).
 - If --out-file is set, writes the same lines to that path.

Golden pack command
-----------------------------

- golden-pack [--root <dir>] [--expect <hash32hex>] [--out-file <path>]

 Purpose:
 - Run a small deterministic end-to-end workload in-process and emit a single-line report.

 Output:
 - Prints one line beginning with `golden_pack_report_v1`.

 Locking:
 - Provide `--expect <hash32hex>` or set `FSA_LM_GOLDEN_PACK_V1_REPORT_HEX` to make the command
 fail if the report hash changes.

 See:
 - docs/GOLDEN_PACK_V1.md

Golden pack turn-pairs command
-------------------------------------------

- golden-pack-turn-pairs [--root <dir>] [--expect <hash32hex>] [--out-file <path>]

 Purpose:
 - Run a deterministic two-turn answer workload that covers Markov opener
 surface-template selection (preface variant 0 vs 1).

 Output:
 - Prints one line beginning with `golden_pack_turn_pairs_report_v1`.

 Locking:
 - Provide `--expect <hash32hex>` or set `FSA_LM_GOLDEN_PACK_TURN_PAIRS_V1_REPORT_HEX` to make
 the command fail if the report hash changes.

 See:
 - docs/GOLDEN_PACK_TURN_PAIRS_V1.md

Golden pack conversation command
--------------------------------------------

- golden-pack-conversation [--root <dir>] [--expect <hash32hex>] [--out-file <path>]

 Purpose:
 - Run the scale-demo golden pack and the turn-pairs golden pack and emit a
 single bundled report.

 Output:
 - Prints one line beginning with `golden_pack_conversation_report_v1`.

 Locking:
 - Provide `--expect <hash32hex>` or set `FSA_LM_GOLDEN_PACK_CONVERSATION_V1_REPORT_HEX` to make
 the command fail if the report hash changes.

 See:
 - docs/GOLDEN_PACK_CONVERSATION_V1.md
