# Novel FSA-LM

Novel FSA-LM ("Novel") is a CPU-first, deterministic, evidence-first conversational language model prototype designed to run on consumer hardware.

Instead of relying on a large end-to-end neural model, Novel stores compact claims and facts in immutable, content-addressed artifacts (columnar frames plus lexicon segments), then performs deterministic retrieval to assemble small evidence bundles. A separate planning and realization pipeline produces answers grounded in evidence, defaulting to "unknown" or requesting clarification when support is insufficient.

Novel borrows ideas from neural-era systems (retrieval-augmented answering, structured planning/realization, and prompt packing), but keeps the implementation reproducible and auditable. Everything is driven by deterministic policies and immutable artifacts rather than large opaque weights.

## What makes Novel work

Novel borrows proven ideas from modern language systems and classical NLP, but implements them with reproducible, inspectable artifacts:

- Evidence-first retrieval and answering (retrieval-augmented generation style, but deterministic and auditable via evidence bundles).
- Columnar frame memory for compact factual storage and stable scoring.
- Lexicon segments and snapshots (Wiktionary-derived) to support deterministic query expansion.
- Deterministic retrieval policies (stable ordering, canonical bytes, signature gating and skip strategies).
- Planner and realizer pipeline (structured planning, then controlled realization rather than freeform generation).
- Pragmatics and hints as deterministic control signals (tone and formatting directives; Markov/PPM style hinting for continuity).

## Key properties
- CPU-first, deterministic behavior (stable ordering; canonical bytes everywhere)
- Evidence-first answers (structured evidence bundles, not long scraped passages)
- Disk-first artifacts (content-addressed storage; replayable pipelines)
- Minimal dependencies and no unsafe Rust

## Default user experience

Novel now defaults to a conversational user-facing surface in normal `ask`, `chat`, and `answer` flows.

- `--presentation user` is the default. It hides raw operator diagnostics such as `Answer v1`, `query_id=...`, `routing_trace ...`, `graph_trace ...`, and `exemplar_match ...`.
- `--presentation operator` keeps the inspect-friendly surface for debugging, evaluation, and workflow audits.
- Workspace defaults are applied automatically when matching flags are omitted, including `default_k`, `default_expand`, and `default_meta`.
- When the workspace configures `markov_model`, `exemplar_memory`, or `graph_relevance`, those bounded advisory artifacts are auto-used in normal runtime flow.
- Resumed conversations restore sticky advisory artifact ids and the selected presentation mode so a saved user or operator workflow stays consistent across runs.

## HOW TO: load Wikipedia and Wiktionary, then run prompts

This guide is the "just works" path: you load datasets once, then use `ask` or `chat` without managing artifact hashes.

Novel stores all data in a local artifact root directory (`--root <dir>`). The `load-*` commands write defaults into `<root>/workspace_v1.txt`, and `ask`/`chat` automatically use those defaults.

### 1) Build the CLI

```bash
cargo build --release
./target/release/fsa_lm -h
```

### 2) Choose an artifact root

Pick a directory to store artifacts (frames, indexes, lexicon segments, snapshots, replay logs):

```bash
mkdir -p ./store
```

All commands below use `--root ./store`.

### 3) Load Wikipedia and build the merged index

Use ONE of these input forms:

- TSV dump (`title<TAB>text`, one document per line):

```bash
./target/release/fsa_lm load-wikipedia --root ./store --dump ./enwiki.tsv --shards 8
```

- Wikipedia XML or XML.bz2:

```bash
./target/release/fsa_lm load-wikipedia --root ./store --xml-bz2 ./enwiki.xml.bz2 --shards 8
```

This produces a merged index snapshot and signature map and writes them into `./store/workspace_v1.txt`.

You can verify what is configured:

```bash
./target/release/fsa_lm show-workspace --root ./store
```

### 4) (Optional) Load Wiktionary for query expansion

Wiktionary provides a deterministic lexicon snapshot used for query expansion (synonyms, related terms, IPA, etc).

```bash
./target/release/fsa_lm load-wiktionary --root ./store --xml-bz2 ./enwiktionary.xml.bz2 --segments 16
```

This writes `lexicon_snapshot=...` into `./store/workspace_v1.txt`.

If you answer from a different root (for example after index replication), the lexicon artifacts must also exist in that root.
Use `sync-lexicon` to replicate a LexiconSnapshotV1 plus its referenced LexiconSegmentV1 artifacts over artifact sync.
See `docs/LEXICON_SYNC_V1.md`.

### 5) Ask a single question

`ask` creates a prompt internally and runs the full evidence-first answering pipeline.

```bash
./target/release/fsa_lm ask --root ./store "What is Ada Lovelace known for?"
```

Enable lexicon expansion (uses `lexicon_snapshot` from the workspace when present):

```bash
./target/release/fsa_lm ask --root ./store --expand "Tell me about bananas"
```

Inspect the same answer in operator mode:

```bash
./target/release/fsa_lm ask --root ./store --presentation operator "What is Ada Lovelace known for?"
```

Helpful knobs:

- `--k <n>` retrieval depth
- `--meta` enables metaphone-based expansion of query terms
- `--max_tokens <n>` caps realization length
- `--presentation operator` shows the inspect-friendly surface without changing grounding

Workspace default behavior:

- `ask`, `chat`, and `answer` use workspace defaults for snapshot, sig map, and lexicon artifacts when matching flags are omitted.
- `default_k`, `default_expand`, and `default_meta` are also applied from the workspace when their flags are omitted.
- If the workspace sets `markov_model`, `exemplar_memory`, or `graph_relevance`, those advisory artifacts are auto-used when matching flags are omitted.

### 6) Chat (interactive)

`chat` reads one prompt per line and keeps a bounded history for the session.

```bash
./target/release/fsa_lm chat --root ./store
```

With lexicon expansion:

```bash
./target/release/fsa_lm chat --root ./store --expand
```

Inspect the same workflow in operator mode:

```bash
./target/release/fsa_lm chat --root ./store --presentation operator
```

If you save and resume a session, Novel restores sticky advisory ids and the selected presentation mode so resumed conversations keep the same runtime behavior.

Chat commands:

- `/help` show help
- `/reset` clear history
- `/exit` or `/quit` exit

### Example chat session (simulated surface shape)

This is an illustrative user-mode session. The exact wording, evidence ids, and source lines depend on the loaded artifacts, but the default surface is conversational rather than report-like. Compare, recommend, summarize, and explain prompts keep light structure when it helps. Troubleshooting and procedure-heavy prompts still keep explicit `Steps`. Clarifiers use a shorter `Quick question: ...` style when more information is required before answering.

```text
$ ./target/release/fsa_lm chat --root ./store --expand
> What is a banana?
A banana is an edible fruit produced by several kinds of large herbaceous plants in the genus Musa.

Sources
[E0] ...

> What about plantains?
Plantains are a type of banana that are usually starchier and are often cooked before eating.

Sources
[E0] ...

> /exit
$
```

To compare the same prompt in default user mode and operator mode, run:

```bash
./target/release/fsa_lm ask --root ./store "What is a banana?"
./target/release/fsa_lm ask --root ./store --presentation operator "What is a banana?"
```

Tip: for a quick smoke test, see `examples/README.md`, especially `demo_cmd_compare_presentation.(bat|sh)` and `demo_cmd_workflow_with_lexicon.(bat|sh)`.

## Operator workflow

For the end-to-end distributed workflow (shard ingest -> reduce -> replicate -> query and answer), see:
- `docs/OPERATOR_WORKFLOW.md`

## Docs
Start here:
- `docs/INDEX.md`
- `docs/CLI.md`
- `docs/WORKSPACE_V1.md`

For advanced usage (explicit artifact handles, index inspection, replay decoding), see `docs/CLI.md`.

Implementation contracts:
- `docs/ARTIFACTS.md`
- `docs/FRAMES.md`
- `docs/LEXICON.md`
- `docs/WIKTIONARY_INGEST_V1.md`
- `docs/SHARDED_INGEST_V1.md`
- `docs/SHARDED_REDUCE_V1.md`
- `docs/ARTIFACT_SYNC_V1.md`

## Development policy
- ASCII-only comments and docs
- Warning-free builds (see `tools/check_warnings.*`)
- Deterministic ordering (do not depend on hash map iteration order)
- Add tests for every new behavior

## License
Apache-2.0. See `LICENSE` and `NOTICE`.

## Contributing
See `CONTRIBUTING.md`.

## Security
See `SECURITY.md`.

## Changelog
See `CHANGELOG.md`.

## Releasing
See `docs/RELEASING.md`.

## Contact
- dev@rswestmore.land
