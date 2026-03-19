Release notes
=============

This file provides a human-readable summary of notable releases.
For the authoritative per-commit list, see CHANGELOG.md.

Current release: 0.1.3
----------------------

Notable changes:
- Conversational user mode is now the default presentation surface for `ask`, `chat`, and `answer`.
- Raw inspect lines such as `Answer v1`, `query_id=...`, `routing_trace ...`, `graph_trace ...`, and `exemplar_match ...` stay hidden unless `--presentation operator` is selected.
- Workspace scalar defaults (`default_k`, `default_expand`, and `default_meta`) are now applied automatically when matching flags are omitted.
- Workspace advisory defaults (`markov_model`, `exemplar_memory`, and `graph_relevance`) are auto-used in normal runtime flow when configured.
- Conversation resume keeps sticky advisory ids plus the selected presentation mode so user and operator workflows stay consistent across runs.
- README, release notes, release audit notes, and example scripts now document and demonstrate the user-vs-operator surface split.

Previous release: 0.1.2
-----------------------

Notable changes:
- "Just works" user flow: load-wikipedia/load-wiktionary write workspace defaults so ask/chat can run without manual hash plumbing.
- Deterministic conversation persistence: ConversationPackV1 via --session-file/--resume, with bounded history and replayable artifacts.
- Deterministic conversation continuity: ContextAnchorsV1 integrates prior-turn anchors into retrieval in a bounded, auditable way.
- Improved control signals: PragmaticsFrameV1, PlannerHintsV1, ForecastV1, and RealizerDirectivesV1 are wired through the answering loop.
- Logic puzzle support (structured): a deterministic finite-domain solver can emit ProofArtifactV1, attach ProofRef in EvidenceBundleV1, and record proof-artifact-v1 in ReplayLog.

Operator notes:
- Release readiness checklist: docs/RELEASE_AUDIT.md
- How to cut a release: docs/RELEASING.md
- Warning-free builds policy: docs/WARNING_ZERO.md
