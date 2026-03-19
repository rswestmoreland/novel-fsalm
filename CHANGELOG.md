# Changelog

This file tracks user-visible changes.

## Unreleased

- No user-visible changes recorded yet.

## 0.1.3
### Added
- Exemplar guidance support via `ExemplarMemoryV1`, including exemplar-memory build/runtime flows and bounded answer shaping for tone, structure, and clarifier style.
- Graph-assisted query expansion via `GraphRelevanceV1`, including graph-relevance build/runtime flows that enrich expansion candidates without outranking lexical evidence.
- Acceptance and release-audit docs for the new default user-vs-operator presentation split, plus comparison smoke scripts for both surfaces.

### Changed
- Default presentation now targets a conversational user-facing surface for `ask`, `chat`, and `answer`, while `--presentation operator` preserves the inspect-friendly workflow.
- Raw operator diagnostics such as `Answer v1`, `query_id=...`, `routing_trace ...`, `graph_trace ...`, and `exemplar_match ...` are hidden by default and shown only in operator mode.
- Workspace scalar defaults (`default_k`, `default_expand`, and `default_meta`) now apply automatically when their flags are omitted.
- Workspace advisory defaults (`markov_model`, `exemplar_memory`, and `graph_relevance`) now auto-apply in normal runtime flow when configured.
- Conversation resume now restores sticky advisory artifact ids plus the selected presentation mode so saved user and operator workflows stay consistent.
- README, release notes, release audit notes, and example scripts now describe and demonstrate the user-vs-operator surface split.

## 0.1.2
### Added
- Lexicon-assisted pragmatics signals for problem solving and logic puzzles, feeding planner hints and forecast clarifiers.
- Conversation continuity via ContextAnchorsV1:
  - bounded context anchors derived from recent turns
  - stored as artifacts and recorded in replay logs for auditability
  - used as low-weight retrieval anchors so follow-up questions can stay grounded.
- Deterministic logic puzzle support with auditable proof evidence:
  - optional structured `[puzzle]...[/puzzle]` input format
  - ProofArtifactV1 stored as a content-addressed artifact
  - ProofRef attached to EvidenceBundleV1 and recorded via `proof-artifact-v1` replay step
  - concise proof solution line emitted in steps-oriented outputs.

### Changed
- Reasoning and conversation documentation refreshed:
  - `docs/REASONING_FLOW.md` updated to match current wiring and now uses Mermaid diagrams for GitHub readability.
  - `docs/INDEX.md` expanded with a curated operator and maintenance section.

### Fixed
- CLI help and docs updated to mention logic puzzle behavior (clarifiers, optional structured block) without requiring a DSL.

## 0.1.1
### Added
- "Just works" end-user commands:
  - `load-wikipedia` (TSV, XML, or XML.bz2) runs ingest + index build + deterministic reduce/merge and writes workspace defaults.
  - `load-wiktionary` (XML or XML.bz2) builds a LexiconSnapshotV1 and writes it to workspace defaults.
  - `show-workspace` prints the active defaults in `workspace_v1.txt`.
- Session and conversation persistence:
  - ConversationPackV1 artifact with canonical codec and artifact helpers.
  - `chat --resume <hash>` to resume a prior conversation pack.
  - `chat --session-file <path>` and `chat --autosave` for file-based persistence.
  - `ask --session-file <path>` for non-interactive continuation.
- Markov chat-history context:
  - In-session tail (bounded) used to derive continuity hints.
  - Resume-aware history reconstruction from assistant replay ids stored in ConversationPack.

### Changed
- README and docs updated to prefer `load-wikipedia`, `load-wiktionary`, `ask`, `chat`, and `show-workspace` for the primary user flow.
- Wiktionary extraction coverage expanded with deterministic caps (POS and relation headings, target forms, and sense cleanup).
- Performance pass on Wiktionary ingest and related pipelines (allocation trimming and pre-sizing) without semantic changes.
- Optional `load-wiktionary --stats` output (default output remains stable).

### Fixed
- Example scripts updated to use the "just works" commands and avoid manual hash plumbing.
- CLI help text and error messages hardened for workspace and session-file usage.
- Tests expanded to cover in-session Markov history behavior and CLI help output.

## 0.1.0
- Initial public release.
- Deterministic, disk-first artifacts (frames, lexicon segments, index snapshots, evidence bundles).
- Sharded ingest, deterministic reduce merge, and manifest-driven replication over TCP.
- Operator tooling and workflow docs, plus regression locks for end-to-end operator flows.
