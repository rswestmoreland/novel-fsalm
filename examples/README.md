Examples policy
===============
As Novel grows, keep small, copy/paste-friendly examples under /examples.

- Prefer Windows cmd (.bat) examples when possible.
- Each example should be deterministic and should not require network access.
- Examples must avoid huge datasets; use tiny demo inputs.

Retrieval design examples will be added during (segment signatures and postings index).


----------
- demo_cmd_build_evidence.bat: ingest tiny wiki XML, build index, build evidence bundle
- demo_cmd_build_evidence.sh: same demo for bash


----------
- demo_cmd_scale_demo_full_loop.bat: scale-demo full loop, run twice, compare scale report line
- demo_cmd_scale_demo_full_loop.sh: same demo for bash


----------
- demo_cmd_compact_index.bat: ingest tiny wiki XML, build index, compact index, query before/after
- demo_cmd_compact_index.sh: same demo for bash



----------
- demo_cmd_sharded_ingest.bat: sharded TSV ingest + build-index-sharded + per-shard query snippet
- demo_cmd_sharded_ingest.sh: same demo for bash


----------
- demo_cmd_reduce_index.bat: sharded TSV ingest + build-index-sharded + reduce-index + global query snippet
- demo_cmd_reduce_index.sh: same demo for bash


----------
- demo_cmd_sync_reduce.bat: start serve-sync, sync-reduce into fresh root, global query on destination
- demo_cmd_sync_reduce.sh: same demo for bash


-----------
- run-phase6: one-command sequential driver for (sharded ingest + build-index-sharded + reduce-index).
 See docs/CLI.md for arguments and output format.


 knobs
------------
All scripts accept environment variable overrides so you can adjust paths and ports without editing the files.

Common knobs:
- SHARDS: shard count for sharded ingest/build-index-sharded (default 4).
- KEEP_TMP: 0 deletes temp roots at start, 1 keeps existing roots (default 0).
- EXE: path to the built fsa_lm executable (defaults to./target/debug/fsa_lm or target\debug\fsa_lm.exe).

stage-specific knobs:
-: ROOT (default./_tmp_sharded_ingest or./_tmp_reduce_index).
-: SRC_ROOT, DST_ROOT, PORT, RW_TIMEOUT_MS (default 30000; 0 disables).

Notes:
- The scripts build the debug executable if it does not exist. To avoid rebuilds, run: cargo build --bin fsa_lm



----------
- demo_cmd_build_lexicon_snapshot.bat: build a LexiconSnapshot from LexiconSegment hashes
- demo_cmd_build_lexicon_snapshot.sh: same demo for bash


--------
- demo_cmd_build_markov_model.bat: build a MarkovModelV1 offline from replay logs
- demo_cmd_build_markov_model.sh: same demo for bash
- demo_cmd_inspect_markov_model.bat: inspect a stored MarkovModelV1 and print a stable summary
- demo_cmd_inspect_markov_model.sh: same demo for bash


----------
- demo_cmd_golden_pack_turn_pairs_v1.bat: run golden-pack-turn-pairs twice, compare report lines
- demo_cmd_golden_pack_turn_pairs_v1.sh: same demo for bash
- demo_cmd_golden_pack_conversation_v1.bat: run golden-pack-conversation twice, compare report lines
- demo_cmd_golden_pack_conversation_v1.sh: same demo for bash
