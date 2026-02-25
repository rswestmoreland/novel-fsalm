# Novel FSA-LM

Novel FSA-LM ("Novel") is a CPU-first, deterministic, evidence-first conversational LM prototype designed to run on consumer hardware.

Instead of relying on a large end-to-end neural model, Novel stores compact **claims and facts** in immutable, content-addressed artifacts (columnar frames + lexicon segments), then performs deterministic retrieval to assemble small **evidence bundles**. A separate planning/realization pipeline produces answers that remain grounded in evidence, defaulting to "unknown/clarify" when support is insufficient.

## Key properties
- CPU-first, deterministic behavior (stable ordering; canonical bytes everywhere)
- Evidence-first answers (structured evidence bundles, not long scraped passages)
- Disk-first artifacts (content-addressed storage; replayable pipelines)
- Minimal dependencies and no unsafe Rust

## Quick start
Build and run the CLI:

```bash
cargo build
./target/debug/fsa_lm -h
```

Common entry points:
- Build prompt artifacts: `prompt`
- Ingest and build indexes: `ingest-wiki*`, `build-index*`, `reduce-index`
- Replicate artifacts over TCP: `serve-sync`, `sync-reduce`, `sync-reduce-batch`
- Query and answer: `query-index`, `build-evidence`, `answer`

## Operator workflow
For the end-to-end distributed workflow (shard ingest -> reduce -> replicate -> query/answer), see:
- `docs/OPERATOR_WORKFLOW.md`

## Docs
Start here:
- `docs/INDEX.md`
- `docs/CLI.md`

Implementation contracts:
- `docs/ARTIFACTS.md`
- `docs/FRAMES.md`
- `docs/LEXICON.md`
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
