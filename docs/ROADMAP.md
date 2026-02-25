Roadmap
=======

This document describes the architectural direction of Novel FSA-LM at a high level.
For the authoritative rolling checklist and internal progress tracking, see `docs/MASTER_PLAN.md`.

Near term
---------
- Keep the end-to-end operator workflow stable and reproducible (shard ingest -> reduce -> replicate -> query/answer).
- Maintain determinism and evidence grounding as the default behavior.
- Keep automated CI green on Windows and Linux (tests for all targets; warnings treated as errors).
- Improve release hygiene (CI, packaging, docs consistency).

Decoding direction
-----------------
- **Config 2 (current):** Orchestrator -> draft -> rewrite -> verifier.
- **Config 3 (later):** Guarded decoding driven by verifier directives (TokenGuard/SpanGuard style constraints).

Future directions
-----------------
- More retrieval policies and skip strategies beyond signature gating.
- Additional coprocessors/reflexes (math/logic/pragmatics) as deterministic control-signal tracks.
- Larger-scale ingest and operator tooling refinements while preserving reproducibility.