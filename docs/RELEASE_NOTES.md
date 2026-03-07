Release notes
=============

This file provides a human-readable summary of notable releases.
For the authoritative per-commit list, see CHANGELOG.md.

Current release: 0.1.2
----------------------

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
