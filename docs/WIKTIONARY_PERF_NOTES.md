# Wiktionary ingest performance notes

This document captures safe performance optimization guidance for the Wiktionary
ingest pipeline while preserving determinism and output stability.

## Scope

These notes apply to:

- The deterministic Wiktionary wikitext scanner and page extraction
- Lexicon row construction for lemma/sense/relation/pronunciation data
- Segment and snapshot writing for lexicon artifacts

The intent is to reduce allocations and CPU where reasonable without changing
outputs for fixed fixtures.

## Definition of "no semantic change"

For this repository, "no semantic change" means:

- For a fixed input fixture set, the produced lexicon artifacts are identical:
  - Lexicon segment bytes are identical
  - Lexicon snapshot bytes are identical
  - Any printed artifact ids/hashes are identical
- The extracted rows (lemmas, senses, relations, pronunciations) are identical
  in content and in deterministic ordering.

If an optimization would change any of the above, it is not "no semantic change"
and should be treated as a contract change.

## Safety locks

Use determinism and fixture-based tests as the guardrail for any optimization:

- `tests/wiktionary_ingest_expand_e2e.rs` locks determinism by ingesting the same
  fixture into multiple roots and asserting the resulting snapshot hash matches.
- `examples/wiktionary_tiny.xml` (and its `.bz2`) provide a small operator
  fixture for quick validation.

If a change breaks a lock, revert or rework the optimization. Do not update
fixture expectations as part of a performance-only change.

## Allowed optimization techniques

The following techniques are considered safe when they do not change outputs:

- Reuse scratch buffers and vectors:
  - Keep `Vec` capacity and `clear()` between pages/sections
  - Avoid repeated allocation of temporary `Vec<String>` and `String`
- Prefer slices over owned strings:
  - Use `&str` or byte indexing during scanning and parsing
  - Allocate a `String` only at the boundary where it must be stored
- Avoid repeated trimming allocations:
  - Use index-based trimming (start/end offsets) during parsing
- Reduce churn in builders:
  - Pre-size vectors using conservative caps or observed counts
  - Avoid per-item cloning when a borrow or shared reference is sufficient
- Minimize repeated work:
  - Avoid scanning the same line multiple times for different patterns when a
    single pass can extract all needed fields

## Techniques to avoid

The following changes are high risk for determinism or output stability and
should not be used in performance-only work:

- Introducing nondeterministic iteration order (for example, iterating a map
  without a stable ordering rule)
- Changing canonical encoding or field ordering of any artifact
- Altering caps, filters, allowlists, or normalization rules
- Adding parallelism or background work to ingestion paths
- Switching hashing strategies that influence ordering or ids

## Notes on measurement

Keep measurement tooling lightweight:

- Prefer counting and coarse timing around the scanner and build stages.
- Avoid new heavy dependencies unless clearly justified.

Any measurement output should be optional and must not change default CLI output.
