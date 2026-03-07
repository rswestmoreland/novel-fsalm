Roadmap
=======

This document describes the architectural direction of Novel FSA-LM at a high level.
For the authoritative rolling checklist and internal progress tracking, see `docs/MASTER_PLAN.md`.

Near term
---------
- Keep the end-to-end operator workflow stable and reproducible (shard ingest -> reduce -> replicate -> query and answer).
- Maintain determinism and evidence grounding as the default behavior.
- Keep automated CI green on Windows and Linux (tests for all targets; warnings treated as errors).
- Continue improving operator ergonomics and docs consistency.
- Extend lexicon workflows:
  - Add replication support for lexicon artifacts (segments and snapshot) alongside existing index replication.
  - Broaden Wiktionary extraction coverage while preserving deterministic caps and stable ordering.
  - Performance pass on Wiktionary ingest and segment building (streaming throughput and allocation trimming) without semantic changes.

Decoding direction
-----------------
- **Configuration A (current):** Orchestrator -> draft -> rewrite -> verifier.
- **Configuration B (later):** Guarded decoding driven by verifier directives (TokenGuard/SpanGuard style constraints).

Future directions
-----------------
- More retrieval policies and skip strategies beyond signature gating.
- Additional coprocessors/reflexes (math/logic/pragmatics) as deterministic control-signal tracks.
- Larger-scale ingest and operator tooling refinements while preserving reproducibility.
