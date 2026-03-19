Examples
========

These scripts are small, copy/paste-friendly demos under `examples/`.

Goals
-----

- Deterministic (no network access; tiny local fixtures).
- "Just works" workflow for end users:
  - `load-wikipedia`
  - `load-wiktionary`
  - `show-workspace`
  - `ask`
  - `chat` (with `--session-file` + `--autosave`)
- Operator/advanced demos are allowed, but should be clearly labeled and kept stable.

Quickstart scripts (recommended)
-------------------------------

- `demo_cmd_workflow_with_lexicon.(bat|sh)`
  - Loads a tiny TSV + the tiny Wiktionary fixture into one root.
  - Shows workspace defaults.
  - Runs `ask` with and without `--expand`.

- `demo_cmd_compare_presentation.(bat|sh)`
  - Loads a tiny TSV into one root.
  - Runs the same `ask` prompt once in default user mode and once in operator mode.
  - Useful for smoke-checking the conversational surface before a release.

- `demo_cmd_build_markov_model.(bat|sh)`
  - Creates a small chat session (`--session-file` + `--autosave`).
  - Extracts assistant replay ids from the ConversationPack.
  - Builds `MarkovModelV1` from replay logs.

- `demo_cmd_inspect_markov_model.(bat|sh)`
  - Inspects the Markov model produced by the script above.

Wikipedia loading
----------------

- `demo_cmd_ingest_wiki.(bat|sh)`
  - Loads a tiny TSV via `load-wikipedia`.

- `demo_cmd_ingest_wiki_xml.(bat|sh)`
  - Loads `examples/wiki_tiny.xml` via `load-wikipedia`.

- `demo_cmd_ingest_wiki_xml_bz2.(bat|sh)`
  - Loads `examples/wiki_tiny.xml.bz2` via `load-wikipedia`.

Wiktionary loading
-----------------

- `demo_cmd_ingest_wiktionary_xml.(bat|sh)`
  - Loads the tiny Wiktionary fixture (plain and bz2) via `load-wiktionary`.

- `demo_cmd_build_lexicon_snapshot.(bat|sh)`
  - Loads a tiny Wiktionary fixture and validates the resulting `LexiconSnapshotV1`.

Operator/advanced demos
-----------------------

These scripts may use lower-level subcommands and artifact hashes.

- `demo_cmd_build_evidence.(bat|sh)`
  - Loads a tiny Wikipedia fixture, then builds an `EvidenceBundleV1`.

- `demo_cmd_compact_index.(bat|sh)`
  - Loads a tiny Wikipedia fixture, compacts the index snapshot, and queries before/after.

- `demo_cmd_scale_demo_full_loop.(bat|sh)`
  - Runs `scale-demo` twice and compares a stable report line.

- `demo_cmd_sharded_ingest.(bat|sh)`
  - Runs `load-wikipedia` with `--shards`, then queries using workspace defaults.

- `demo_cmd_reduce_index.(bat|sh)`
  - Similar to the sharded ingest demo, with a global query snippet.

- `demo_cmd_sync_reduce.(bat|sh)`
  - Starts `serve-sync` and uses `sync-reduce` to replicate reduce outputs.

- `demo_cmd_sync_lexicon.(bat|sh)`
  - Starts `serve-sync` and uses `sync-lexicon` to replicate a lexicon snapshot.

- `demo_cmd_golden_pack_v1.(bat|sh)`, `demo_cmd_golden_pack_turn_pairs_v1.(bat|sh)`, `demo_cmd_golden_pack_conversation_v1.(bat|sh)`
  - Determinism checks for golden-pack reports.

Knobs
-----

Many scripts accept environment variable overrides so you can adjust paths and ports without editing the files.

Common knobs:
- `ROOT`: root directory for artifacts (varies by script).
- `SHARDS`: shard count for `load-wikipedia` (default 4 in most scripts).
- `KEEP_TMP`: 0 deletes temp roots at start, 1 keeps existing roots (default 0).
- `EXE`: path to the built `fsa_lm` executable.

Notes:
- Most scripts build the debug executable if it does not exist. To avoid rebuilds, run:
  - `cargo build --bin fsa_lm`
