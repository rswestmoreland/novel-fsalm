FSA-LM Master Plan and Checklist
================================

This file is the authoritative rolling checklist for the prototype. It is intended
to prevent loss of design intent across checkpoints.

Core goals
----------
- CPU-only, consumer laptop target (e.g., 16 GB RAM).
- Deterministic, bitwise-stable outputs given identical artifacts and config.
- Minimal crates; prefer code we own; no unsafe.
- Integer-only or fixed-point math (avoid floats).
- Grounded answers: prefer retrieval + evidence over hallucination.
- Scales beyond RAM: disk-first segments with hot/warm/cold tiers.

Status key
----------
- DONE: implemented and passing tests
- PENDING: planned, not yet implemented

Repo policy and licensing
------------------------
- License: Apache-2.0 (see LICENSE and NOTICE)
- Copyright: Richard S. Westmoreland <dev@rswestmore.land>
- Code of Ethics: CODE_OF_ETHICS.md (guidance; does not modify the license)
- Planned safety gate: docs/SAFETY_REFLEX_V1.md (Asimov-inspired rules-first reflex)


Phase 0 - Foundations (artifacts, determinism, interface)
--------------------------------------------------------
DONE
- 0a: Scaffold crate + core primitives (hash/codec/determinism/artifacts)
- 0b: ReplayLog codec + artifact storage
- 0c: CLI + TCP skeleton (put/get, prompt, replay decode) + tests

PENDING
- 0d: Wire protocol versioning (capabilities, explicit errors)
- 0e: Optional REST facade (later, not required for core)
- 0f: Apply SPDX headers and copyright line across repo files

Phase 1 - Prompts as first-class artifacts and replay linkage
-------------------------------------------------------------
DONE
- 1a: PromptPack data model + canonical encoding/decoding + tests
- 1b: PromptLimits + canonicalize_in_place + truncation tests
- 1c: PromptPack artifact helpers + CLI prompt command + tests
- 1d: ReplayLog prompt linkage conventions + CLI replay-new/replay-add-prompt
      + example script under /examples

PENDING
- 1e: Replay step expansion (timestamps, scores, traces, error codes)
- 1f: Human-readable dump commands (PromptPack and ReplayLog pretty print)

Phase 2 - Data models for knowledge and linguistics (disk-first)
---------------------------------------------------------------
Goal: define what ingestion produces and how it is stored for retrieval.

Frame rows are stored as columnar segments.
Columns (initial set, can expand):
- doc_id, source_id, when, where, who, what
- entity_ids[] (hashed ids), verb, polarity, confidence (fixed-point)
- term_id[] and term_tf[] for retrieval, doc_len, section_id (optional)

DONE (Phase 2)
- 2a: Frame schema v1 (knowledge frames, columnar)
- 2b: FrameSegment file format v1 (chunked, columnar, tests)
- 2c: Tokenizer and term-id strategy (deterministic, integer-only)
- 2d: Metaphonetic preprocessor v1 (MetaCodeId, docs, tests)
- 2e: FrameStore (persist FrameSegmentV1 via artifact store)
- 2f: Lexicon schema v1 (Wiktionary row schema + deterministic ids + tests)

PENDING (Phase 2)
- 2g: LexiconSegment and LexiconSnapshot formats
  - 2g1 DONE: LexiconSegmentV1 codec + canonical ordering + unit tests + docs
    - code: src/lexicon_segment.rs
    - docs: docs/LEXICON_SEGMENT_V1.md
  - 2g2 DONE: LexiconSnapshotV1 (manifest + build CLI)
    - 2g2a DONE: LexiconSnapshotV1 schema + codec + unit tests + docs
      - code: src/lexicon_snapshot.rs
      - docs: docs/LEXICON_SNAPSHOT_V1.md
    - 2g2b DONE: LexiconSegment and LexiconSnapshot store helpers
    - 2g2c DONE: build-lexicon-snapshot CLI + examples
  - 2g3 PENDING: Lexicon retrieval helpers (lookup by LemmaId, MetaCodeId postings, etc.)

- 2h: Lexicon segmenting and merge policy
  - 2h0 DONE: Contract doc for segment boundaries, owner lemma rules, and snapshot merge rules
    - docs/LEXICON_SEGMENTING_AND_MERGE_POLICY.md
  - 2h1 DONE: Segment builder (partition rows into N LexiconSegmentV1 artifacts)
    - Deterministic partition function and sense-owner validation
    - Unit tests covering owner lemma mapping, missing sense refs, and determinism
  - 2h2 DONE: Snapshot validation for disjoint lemma ownership
    - 2h2a DONE: Disjoint owner validator + unit/integration tests
    - 2h2b DONE: validate-lexicon-snapshot CLI command
    - 2h2c DONE: CLI unit tests (OK + conflict)


DONE (Pragmatics coprocessor)
- 2p: PragmaticsFrameV1
  - 2p0 DONE: Schema + rules-first algorithm doc
    - docs/PRAGMATICS_FRAME_V1.md
  - 2p1a DONE: PragmaticsFrameV1 schema v1 (types + flags + validate + unit tests)
  - 2p1b DONE: Pragmatics extractor v1 (PromptPack -> PragmaticsFrameV1) + unit tests
  - 2p2 DONE: Codec + store helpers + build-pragmatics CLI + tests
  - 2p3 DONE: Retrieval integration as a control-signal track (tie-break only; evidence stable)
  - 2p4 DONE: Lexicon cue neighborhoods utility (seed lemma keys -> bounded neighborhood ids)
  - 2p5 DONE: Lexicon-driven intent flags (problem_solve, logic_puzzle) + clarify triggers

Phase 3 - Ingestion pipelines + indexing + hot/warm/cold storage tiers
---------------------------------------------------------------------
Goal: ingest large corpora (Wikipedia + Wiktionary) without fitting in RAM.

DONE (Wikipedia ingest)
- 3a: Ingest CLI and source adapters (DONE)
  - ingest-wiki --dump <path> --root <dir> [--seg_mb N] [--row_kb N] [--chunk_rows N] [--max_docs N]
  - Input v1: UTF-8 TSV (title + text). See docs/INGEST_WIKI.md.
- 3b: Streaming Wikipedia XML ingest (DONE)
  - Supports raw .xml and .xml.bz2 (streaming).

PENDING (Wiktionary ingest)
- WKT0 DONE: Ingest contract doc and doc alignment
  - docs/WIKTIONARY_INGEST_V1.md
  - docs/LEXICON_QUERY_EXPANSION.md (answer CLI expand flags documented)
- WKT1 DONE: Deterministic English-section wikitext scanner
  - POS detection, sense extraction, relation extraction, IPA extraction
  - src/wiktionary_ingest.rs (scanner + unit tests)
- WKT2 DONE: Build lexicon rows and segments (full rows) and write LexiconSnapshotV1
  - src/wiktionary_build.rs (XML adapter wiring + row mapping + segment + snapshot)
- WKT3 DONE: ingest-wiktionary-xml CLI + docs/CLI.md wiring + example scripts
  - ingest-wiktionary-xml (CLI)
  - examples/demo_cmd_ingest_wiktionary_xml.bat
  - examples/demo_cmd_ingest_wiktionary_xml.sh
- WKT4 DONE: E2E integration test and determinism lock (ingest -> snapshot -> validate -> answer --expand)
  - tests/wiktionary_ingest_expand_e2e.rs
- WKT5 DONE: Operator workflow doc update (Wikipedia + Wiktionary + prompt + answer)

- 3c: Index build v1 (DONE core pieces)
  - Term stats + segment postings metadata + optional bloom filters.
  - Deterministic scoring and tie-breaking.
  - 3c1: IndexSegmentV1 format + tests (DONE)
  - 3c2: IndexSnapshotV1 build-index CLI + tests (DONE)
  - 3c3: IndexQuery (BM25-ish fixed-point) query-index CLI + tests (DONE)
- 3c4: Evidence bundles (two-pass retrieval) (DONE)
  - 3c4a: FrameSegment random row access (get_row) + tests (DONE)
  - 3c4b: EvidenceBundleV1 schema + codec + tests (DONE)
  - 3c4c: Evidence builder (pass1 + pass2 merge + row sketches) (DONE)
  - 3c4d: CLI build-evidence + examples scripts (DONE)
  - 3c4e: E2E smoke test (ingest -> index -> query -> evidence) (DONE; tests/e2e_ingest_index_query_evidence_smoke.rs)
- 3d: Hot/Warm/Cold storage policy v1
  - 3d0 DONE: cache and storage-tier contracts doc (docs/CACHES_V1.md)
  - 3d1 DONE: deterministic Cache2Q primitive (bytes-bounded) + unit tests (src/cache.rs)
  - 3d2 read-through caching:
    - 3d2a DONE: FrameSegment cached load (frame_store get_frame_segment_v1_cached) + test
    - 3d2b DONE: IndexSegment cached load (index_store get_index_segment_v1_cached) + test
    - 3d2c DONE: IndexSnapshot cached load (index_snapshot_store get_index_snapshot_v1_cached) + test
  - 3d3 DONE: cache stats exposure (opt-in CLI) + tuning notes (docs/CACHES_V1.md) + unit smoke test
  - Cold: immutable segments + snapshots on disk
  - Warm: decoded artifact caches in-process (bounded and evicting)
  - Hot: per-operation scratch state (query-local) only
- 3e: Offline maintenance / compaction
  - 3e0 DONE: contracts doc (docs/COMPACTION_V1.md)
  - 3e1 DONE: CompactionReportV1 schema + codec + tests (src/compaction_report.rs)
  - 3e2 DONE: Index compaction (IndexPackV1 bundling) + deterministic merge planning + tests
    - code: src/index_pack.rs, src/index_compaction.rs
    - query support: src/index_query.rs loads IndexPackV1 when encountered
    - default planning: target_bytes_per_out_segment=64 MiB, max_out_segments=8
  - 3e3 DONE: CLI compact-index + examples scripts
    - command: fsa_lm compact-index --root <dir> --snapshot <hash32hex> [--target-bytes N] [--max-out-segments N] [--dry-run] [--verbose]
    - examples: examples/demo_cmd_compact_index.(bat|sh)
  - 3e4 DONE: E2E equivalence test (pre vs post compaction)
    - tests/e2e_compact_index_equivalence_smoke.rs
- 3f: Wikipedia scale target
  - Demonstrate ingest of English Wikipedia pages-articles dump.

PENDING (Wiktionary)
- 3g: ingest-wiktionary (English-only)
  - Produce LexiconSegments + LexiconManifest artifacts.
- 3h: build-lexicon-snapshot
  - Produce LexiconSnapshot (lemma/sense/relations/pronunciations/meta index).
- 3i: Unified KnowledgeSnapshot
  - References IndexSnapshot (Wikipedia knowledge) + LexiconSnapshot (Wiktionary).

Phase 3 - Retrieval and memory selection (disk-first)
-----------------------------------------------------
Goal: select small structured evidence for reasoning (not text retrieval).

Design baseline:
- docs/RETRIEVAL_PIPELINE.md (D + C/B hybrid, MRS scoring, formats)

PENDING (Phase 3)
- 3a0: Bridge expansion policy v1
  - 3a0a DONE: ExpansionBudgetV1 schema + docs
    - code: src/expansion_budget.rs
    - docs: docs/EXPANSION_BUDGET_V1.md (also referenced by docs/BRIDGE_EXPANSION.md)
  - 3a0b DONE: Expansion candidate schema (ExpandedQfvV1) + codec + tests
    - code: src/expanded_qfv.rs
    - docs: docs/EXPANDED_QFV_V1.md
  - 3a0c DONE: Bridge expansion builder (candidate merge + budget fill) + tests
    - code: src/expansion_builder.rs
  - 3a0d DONE: RetrievalPolicy wired to bridge expansion via src/bridge_expansion.rs (lex morphology candidates + ExpansionBudgetV1 + build_expanded_qfv_v1)

- 3r1 DONE: SegmentSigV1 artifact format (Bloom gating) + docs + unit tests
  - code: src/segment_sig.rs
  - docs: docs/SEGMENT_SIG_V1.md
  - note: wiring into build-index/compact-index/query-index/build-evidence is complete (3r2/3r3)

- 3r2 DONE: Produce SegmentSig alongside index artifacts (IndexSegment/IndexPack) and carry through compaction
  - 3r2a DONE: SegmentSig store helpers + IndexSigMapV1 schema+codec+store
  - 3r2b DONE: build-index emits SegmentSig artifacts + IndexSigMap sidecar
  - 3r2c DONE: compact-index carries IndexSigMap (or regenerates) and preserves equivalence
- 3r3 DONE: Query-time gating (signature-first skip) + query stats + equivalence tests
  - 3r3a1 DONE: Core gating helper + deterministic unit tests
  - 3r3a2 DONE: query-index and build-evidence gating behind --sig-map
  - 3r3a3 DONE: EvidenceBundle equivalence test (gated vs ungated)
  - 3r3b DONE: Runbook/docs updates for signature gating (docs/RETRIEVAL_PIPELINE.md)

- 3a PENDING: Chunk signatures (optional finer gating)
  - ChunkSig optional for finer gating (future)
- 3b: Postings index v1 (Option C)
  - TermId -> postings(RowIx, tf) blocks
  - MetaCodeId -> postings(LemmaIx or SenseIx) blocks
  - Dictionary-coded dict + postings byte area + block index tables
- 3c: Memory Relevance Score (MRS) v1
- 3c1: Retrieval diversity refinements v1 (docs/TWO_PASS_RETRIEVAL.md)
  - 3c1a DONE: deterministic dedupe + diversity caps
    - Implemented in `src/retrieval_policy.rs` refine stage.
    - Config: `RetrievalPolicyCfgV1.max_hits_per_frame_seg` and `max_hits_per_doc`.
    - Unit tests cover per-segment/per-doc caps and tie behavior.
  - 3c1b DONE: novelty scoring + deterministic tie-break integration
    - Added `RetrievalPolicyCfgV1.novelty_mode` and re-ranking in `src/retrieval_policy.rs`.
    - Novelty is inverse-frequency over DocId and/or FrameSegment (secondary key after score).
  - 3c1c DONE: gated equivalence + scale-demo sanity invariants
    - Added unit test ensuring gated vs ungated search produces identical results
      even when diversity caps and novelty re-ranking are enabled.
    - Added unit test ensuring `search_snapshot_gated` matches `search_snapshot`
      for the same snapshot when signatures are complete.
- 3d: EvidenceSet + synthesis hooks
  - structured evidence emission (no long text)
  - answer trace (claims -> evidence ids) stored in replay artifacts
Phase 4 - Retrieval + grounding for chat
---------------------------------------

PENDING (Phase 4x)

- 4x: Synapse training (post-ingested state) (docs/SYNAPSE_TRAINING.md)
  - Export deterministic feature rows from replay/retrieval artifacts
  - Targets: correctness buckets, pass2_gain, per-evidence usefulness labels
  - Offline-only training; Rust inference hooks optional later

DONE
- 4a DONE: Answer loop v1 (PromptPack -> retrieval -> EvidenceBundle -> AnswerPlan -> rendered text)
  - 4a0 DONE: Review-only entry (no changes)
  - 4a1 DONE: RetrievalPolicyCfgV1 types (src/retrieval_policy.rs)
  - 4a2 DONE: apply_retrieval_policy_v1 wrapper + stats + tests
  - 4a3 DONE: AnswerPlanV1 schema types (src/answer_plan.rs)
  - 4a4 DONE: Planner v1 (EvidenceBundleV1 -> AnswerPlanV1) + tests
  - 4a5 DONE: Realizer v1 + CLI answer + smoke test
  - 4a6 DONE: Docs closeout (docs/ANSWERING_LOOP.md and docs/REASONING_FLOW.md)

- 4b DONE: Lexicon query expansion (bounded, deterministic)
  - 4b1 DONE: Query expansion types (src/query_expansion.rs)
  - 4b2 DONE: Lexicon expand lookup helpers (src/lexicon_expand_lookup.rs)
  - 4b3 DONE: Expansion rules + membership filter + unit tests
  - 4b4 DONE: Retrieval policy integration (apply_retrieval_policy_from_text_v1) + tests
  - 4b5 DONE: Docs closeout (docs/LEXICON_QUERY_EXPANSION.md, docs/ANSWERING_LOOP.md)
  - 4b6 DONE: answer CLI toggle for query expansion

DONE
- 4c: Scale demo/runbook (deterministic end-to-end pipeline)
  - 4c1 DONE: Deterministic workload generator (src/workload_gen.rs) + snapshot tests
  - 4c2 DONE: Scale demo CLI (generate -> ingest -> index -> prompts -> evidence -> answer) + runbook
    - 4c2a DONE: scale-demo generate-only report (src/scale_demo.rs + CLI)
    - 4c2b DONE: scale-demo ingest wiring (write FrameSegment artifacts + manifest)
    - 4c2c DONE: scale-demo index wiring (build IndexSnapshot + SegmentSig + IndexSigMap)
      - 4c2c1 DONE: manifest helpers (load manifest + collect FrameSegment hashes)
      - 4c2c2 DONE: build IndexSnapshot from manifest FrameSegments
      - 4c2c3 DONE: CLI wiring (scale-demo --ingest 1 --build_index 1)
    - 4c2d DONE: scale-demo prompts/evidence/answer loop
      - 4c2d1 DONE: generate + store PromptPacks per query
      - 4c2d2 DONE: build + store EvidenceBundle artifacts per query (gated search)
      - 4c2d3 DONE: run planner+realizer per query, store/print stable output hashes
      - 4c2d4 DONE: CLI flags to drive full loop (prompts/evidence/answer)
    - 4c2e DONE: stable scale report artifact (counts/hashes; deterministic formatting)
    - 4c2f DONE: runbook closeout (docs/SCALE_DEMO_RUNBOOK.md, docs/CLI.md)
  - 4c3 DONE: Memory caps guidance + fixed-seed regression pack
    - docs/SCALE_DEMO_MEMORY.md
    - docs/SCALE_DEMO_REGRESSION_PACK.md
    - tests/scale_demo_regression_pack_v1.rs
  - 4c4 DONE: Track C docs closeout (docs/INDEX.md + scale demo examples)
- 4d: Grounded synthesis upgrades (claims/verifiers/rewrite contract)

Phase 5 - Dataset ledger + evaluation harness (ReplayLog as training log)
-------------------------------------------------------------------------
DONE
- 5a: Replay step conventions extended
  - ingest-wiki, ingest-wiktionary, index-build, lexicon-build, retrieve, answer
  - Steps reference artifact hashes for inputs/outputs.
  - 5a1 DONE: docs/REPLAY_STEP_CONVENTIONS.md + src/replay_steps.rs constants
  - 5a2 DONE: build-evidence CLI emits ReplayLog (build-evidence-v1) with query-id blob input
  - 5a3a DONE: HitListV1 schema + canonical codec + docs
  - 5a3b DONE: query-index emits ReplayLog (retrieve-v1) and stores HitList artifacts
- 5b: Golden tests and regression harness
  - Fixed prompts -> fixed retrieved evidence -> fixed outputs (bitwise).
  - DONE: `golden-pack` CLI + GoldenPackReportV1 artifact + docs/GOLDEN_PACK_V1.md
  - DONE: examples/demo_cmd_golden_pack_v1.(bat|sh)

Phase 6 - Parallel and distributed mode
---------------------------------------
PENDING (Phase 6)
- 6a: Sharded ingestion (deterministic partitioning)
  - 6a1 DONE: Sharded ingest contract doc + CLI stub
    - docs/SHARDED_INGEST_V1.md
  - 6a2 DONE: ShardManifestV1 schema + codec + artifact helpers
    - src/shard_manifest.rs
    - src/shard_manifest_artifact.rs
  - 6a3 DONE: Ingest sharding wiring + ShardManifestV1 writer (wiki TSV/XML)
    - ingest-wiki and ingest-wiki-xml accept --shards N --shard-id K
    - ingest-wiki-sharded and ingest-wiki-xml-sharded drivers write ShardManifestV1
    - wiki_ingest_manifest_v1 output tag recorded per shard
  - 6a4 DONE: Per-shard index snapshots + validation + retrieval compatibility (build-index-sharded)
  - 6a5 DONE: Examples/runbook updates
    - examples/demo_cmd_sharded_ingest.bat
    - examples/demo_cmd_sharded_ingest.sh

- 6c: Artifact replication improvements over TCP (content-addressed sync)
  - 6c1 DONE: Protocol and invariants doc
    - docs/ARTIFACT_SYNC_V1.md
  - 6c2 DONE: Artifact Sync V1 protocol module (streaming GET)
    - src/artifact_sync_v1.rs
    - src/artifact_sync.rs (server handler)
  - 6c3 DONE: Manifest-driven client sync (ReduceManifestV1)
    - fsa_lm serve-sync
    - fsa_lm sync-reduce
  - 6c4 DONE: Core tests (loopback sync)
    - src/artifact_sync.rs tests
  - 6c5 DONE: Examples/runbook updates
    - examples/demo_cmd_sync_reduce.bat/.sh
  - 6c6 DONE: Sync hardening (timeouts, disconnect resilience, bounded temp writes)
  - 6c7 DONE: Resume-friendly fast path (skip already-present artifacts) + stable stats output
  - 6c8 DONE: Batch sync (multiple reduce manifests in one session)

- 6x: Operator UX (post Phase 6 core)
  - 6x1 DONE: Script UX hardening for Phase 6 demos (build once, env overrides, robust Windows quoting)
  - 6x2 DONE: One-command Phase 6 orchestrator CLI (sequential, deterministic)
  - 6x3 DONE: Debug bundle exporter (hashes + manifests + environment; no raw content by default)

- 6b: Deterministic reduce/merge (Phase 6b)
  - 6b1 DONE: Reduce contracts doc
    - docs/SHARDED_REDUCE_V1.md
  - 6b2 DONE: ReduceManifestV1 schema + codec + artifact helpers
    - src/reduce_manifest.rs
    - src/reduce_manifest_artifact.rs
  - 6b3 DONE: reduce-index CLI (merge IndexSnapshot/IndexSigMap + deterministic copy)
  - 6b4 DONE: Integration tests (reduce determinism, empty-shard, preexisting artifacts, build-evidence/answer on merged ids)
  - 6b5 DONE: Examples/runbook updates for reduce-index
    - examples/demo_cmd_reduce_index.bat
    - examples/demo_cmd_reduce_index.sh
  - 6b6 DONE: Reduce-index perf audit (pre-sizing, avoid clones; no semantic changes)
  - 6b7 DONE: Reduce-index copy I/O locality improvements
  - 6b8 DONE: Scale regression lock (bigger synthetic E2E)
    - Locked by CLI-level test: src/bin/fsa_lm.rs :: sharded_ingest_cli_tests::cmd_phase6b_scale_regression_lock_e2e

Phase 7 - Fluent and interactive conversation layer
---------------------------------------------------
Goal: improve interactive conversation quality without giving up evidence-first
determinism. These features are designed to be replay-friendly and fully
auditable via artifacts and ReplayLog steps.

7a - Realizer directives
~~~~~~~~~~~~~~~~~~~~~~~~
DONE
- 7a1: RealizerDirectivesV1 contract (schema + canonical codec + docs + tests)
- 7a2: Derive RealizerDirectivesV1 from PragmaticsFrameV1 (deterministic rules)
- 7a3: Realizer integration: apply directives (tone/style/format/limits)
- 7a4: ReplayLog linkage + golden-pack coverage

7b - Planner hints and forecast
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
DONE
- 7b1 DONE: PlannerHintsV1 contract (schema + canonical codec + docs + tests)
- 7b2 DONE: ForecastV1 contract (top-k predicted next user intents/questions; deterministic)
- 7b3 DONE: Planner integration (emit hints/forecast; tie rules and caps)
- 7b4 DONE: Answer + scale-demo integration (clarifying question policy; replay + golden-pack)

Notes:
- ReplayLog adds stable steps: planner-hints-v1 and forecast-v1.
- answer-v1 step inputs include RealizerDirectives (optional), PlannerHints, and Forecast hashes.
- Scale demo answer stage prints `scale_demo_answers_v3` and the scale report line prints `scale_demo_scale_report_v3`.

7c - Conversation quality hints (Markov/PPM)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
IN PROGRESS
- 7c1a DONE: MarkovHintsV1 contract (schema + canonical codec + docs + tests)
- 7c1b1 DONE: MarkovModelV1 contract (schema + canonical codec + docs + tests)
- 7c1b2 DONE: Markov model training contract (inputs, caps, deterministic tie rules)
- 7c2a DONE: Markov trace + offline trainer scaffold (schema + artifacts + training + hint derivation)
- 7c2b DONE: Wire trace into replay artifacts + build offline trainer over replay logs
- 7c2c DONE: CLI: build-markov-model + inspect-markov-model
- 7c2c1 DONE: build-markov-model command (train MarkovModelV1 from replay logs)
- 7c2c2 DONE: deterministic input canonicalization + stable list summaries + caps (max-replays/max-traces)
- 7c2c3 DONE: inspect-markov-model command (load + validate + stable summary + optional top-states)
- 7c3a1 DONE: Docs alignment for Markov realizer hook (opener)
- 7c3a2 DONE: Realizer surface-form selection hook (opener only; MarkovHints advisory; opt-in API)
- 7c3a3 DONE: Unit tests for Markov opener selection hook (variants + invalid hints ignored)
- 7c3b1 DONE: Lock 7c3b contracts (Option B token policy + selection pipeline)
- 7c3b2 DONE: Load MarkovModelV1 and derive MarkovHintsV1 (opener only; bounded; deterministic)
- 7c3b3 DONE: Emit surface template choice ids in MarkovTraceV1 when templates are used (preface:* first)
- 7c3b4 DONE: Wire answer/scale-demo to use MarkovHintsV1 via the opt-in realizer API (no new claims)
- 7c3c DONE: Golden-pack turn-pairs runner + CLI + report artifact (stable hash verification)
- 7c4: Golden-pack expansions and regression locks

7d - Quality gate consolidation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
DONE
- 7d1 DONE: Consolidate quality-gate wiring (directives + hints/forecast + Markov selection)
- 7d2 DONE: Conversation golden pack bundle (scale-demo + turn-pairs) and regression locks

Notes:
- Introduces src/quality_gate_v1.rs to centralize post-planning integration:
  pragmatics -> directives, optional markov hints derivation, bounded question
  append, and MarkovTrace token construction.
- cmd_answer and scale_demo answer stage both use the shared helpers so surface
  template ids and structural tokens remain consistent across pipelines.
- No new claims: this layer only selects among fixed surface templates and
  appends bounded clarifying questions.

7d2 notes:
- Adds a bundled conversation golden pack report (GoldenPackConversationReportV1)
  and `golden-pack-conversation` CLI command.
- The report embeds the sub-reports for the scale-demo golden pack and the
  turn-pairs golden pack and validates their hashes.
- Adds a determinism test for the bundled pack over two independent runs.

7e - Conversational retrieval continuity (context anchors)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
DONE
- 7e1 DONE: ContextAnchorsV1 artifact (schema + canonical codec + store helpers)
  - Replay step: context-anchors-v1
  - Docs: docs/CONTEXT_ANCHORS_V1.md
  - Tests: unit + integration coverage
- 7e2 DONE: Answer/chat wiring to derive bounded anchor terms from prior turns
  - Lexicon-assisted when available (content POS preference), conservative fallback otherwise
  - Anchors are merged into retrieval within query-term caps (never dominate the current prompt)
  - ReplayLog links anchors hash into answer-v1 inputs for auditability
- 7e3 DONE: Session-file coverage
  - ask and chat both persist anchors deterministically when a prior turn exists
  - Integration tests cover in-session behavior and session-file flows

7f - Logic puzzles (sketch persistence + proof evidence)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
DONE
- 7f1 DONE: PuzzleSketchV1 (free text) + clarifier selection
  - No mandatory DSL; the system prefers one best clarifying question when incomplete
  - Optional structured [puzzle]...[/puzzle] block supported for reproducible inputs
- 7f2 DONE: Pending puzzle persistence across turns
  - PuzzleSketchArtifactV1 stored content-addressed
  - Replay step: puzzle-sketch-v1
  - Deterministic merge of short clarification replies (vars/domain/shape)
- 7f3 DONE: Deterministic solver + proof artifacts
  - ProofArtifactV1 stored content-addressed
  - Replay step: proof-artifact-v1
  - EvidenceBundle ProofRef is active and attached when a proof is produced
- 7f4 DONE: User-facing rendering + docs
  - Realizer emits a short Proof solution line in steps-oriented outputs
  - Docs: docs/LOGIC_SOLVER_V1.md and EvidenceBundle notes
  - Tests: replay step presence, artifact decode, and hash stability


Phase 8 - Consolidation and hardening (recommended)
--------------------------------------------------
Goal: no new features; tighten consistency, operator UX, and regression locks.

8a DONE: Docs + CLI consistency sweep
- Ensure docs/CLI.md matches the CLI usage() string (flags and output formats).
- Cross-link Phase 6 docs: sharded ingest -> reduce -> artifact sync.

8b DONE: Warning-zero + lint hygiene
- Add tools/check_warnings.sh and tools/check_warnings.bat to run cargo test --all-targets with -Dwarnings.
- Add docs/WARNING_ZERO.md with policy and usage.
- Remove remaining warnings in dev and test builds (no behavior changes).

8c DONE: Phase 6 operator E2E golden pack
- Added an operator-style end-to-end regression pack that executes:
  run-workflow -> serve-sync/sync-reduce -> query-index -> answer.
- Locks hashes + key stats lines (not raw content).
- Test: tests/operator_workflow_golden_pack_v1.rs
- Optional lock env var: FSA_LM_REGRESSION_OPERATOR_WORKFLOW_PACK_V1_REPORT_HEX

8d DONE: Sync resilience regression consolidation
- Added CLI-level regression tests for:
  - server stall timeout
  - mid-stream disconnect during GET
  - already-present fast path on repeat sync
  - batch overlap correctness and repeatability
- Test: tests/sync_resilience_regressions_v1.rs

8e DONE: Release readiness snapshot
- Added a Phase 6 operator guide: docs/OPERATOR_WORKFLOW.md
  - Minimal steps: shard -> reduce -> replicate -> query/answer
  - Common failure modes + debug bundle export guidance



Examples policy
---------------
Whenever a new user-visible CLI workflow is added, include at least one small
cmd.exe script under /examples showing an end-to-end run.

Hot / warm / cold notes
-----------------------
- Cold: immutable artifact store (segments and snapshots) on disk.
- Warm: OS page cache and small dictionaries; later consider explicit mmap.
- Hot: bounded in-process caches (query results, term postings, lemma lookups).
  Eviction must be deterministic (use a monotonic counter, not wall clock).

Phase X - Optional coprocessors (reasoning augmentation)
--------------------------------------------------------
Goal: augment Novel's deterministic "thinking" without turning it into a GPU-first neural LLM.
These components are optional and must preserve:
- CPU-first operation (consumer laptop)
- Integer-only / fixed-point arithmetic (no floats)
- Bitwise determinism (stable order, stable reductions)
- Bounded memory and explicit caps
- Evidence-first synthesis (no raw text dumping)

Design principle:
- Coprocessors are *advisory*. They propose features, evidence candidates, or reasoning steps.
- The core evidence + synthesis contracts remain authoritative:
  - Retrieval returns structured EvidenceSet (frames + lexicon rows), not paragraphs.
  - Synthesis produces paraphrased claims with uncertainty.

PENDING (Phase X)
- X1: Markov/PPM predictor work is covered by Phase 7c (conversation quality hints)
- X2: Graph relevance (random walks / PPR-lite) over entity/verb relations
  - Use fixed-point probabilities and stable iteration order.
  - Outputs are candidate entity expansions and multi-hop evidence hints.
- X3: HMM/CRF-lite intent + slot model (integer weights)
  - Improve deterministic intent classification and shallow parsing for frame extraction.
  - Strict caps on states/features; deterministic Viterbi.
- X4: kNN / exemplar patch memory (deterministic nearest neighbors)
  - Store compact hashes of prior QFV -> AnswerTrace patterns.
  - Retrieve similar traces to stabilize behavior and reduce re-derivation.
- X5: Compression-as-learning (MDL-inspired)
  - Grammar/phrase induction (Sequitur-like) to discover reusable patterns.
  - Used to improve tokenizer/lexicon/frame extraction heuristics offline.

Phase Y - Optional tiny transformer modules (strictly bounded)
-------------------------------------------------------------
Goal: allow a small quantized transformer as a plug-in helper while preserving determinism.

Constraints (non-negotiable):
- Inference only, CPU-only.
- Integer-only kernels (int8/int16/int32 or fixed-point).
- Deterministic scheduling and reduction order (single-thread or deterministic parallel).
- Fixed weights; fixed tokenizer; stable model file hashing.
- Model is optional and must not be required for correctness.

PENDING (Phase Y)
- Y1: Quantized inference kernel scaffold
  - Minimal ops needed (matmul, attention, activation, normalization variant).
  - No unsafe code; no fast-math; strict test vectors for bitwise stability.
- Y2: Tiny encoder for additional gating/ranking features
  - Produces compact feature ids or sketch bits to assist D-gating and MRS scoring.
  - Evidence selection remains deterministic with stable tie-breakers.
- Y3: Tiny surface realizer for fluency (claims -> text)
  - Consumes AnswerTrace/claims and renders conversational text.
  - Must obey anti-plagiarism constraints (no verbatim long excerpts).
  - Must support a "disabled" mode with a deterministic template renderer.



Release quality
---------------
- DONE: SPDX headers across src/ and tests/.
- DONE: GitHub Actions CI for Windows and Linux (tests for all targets; warnings treated as errors).
- DONE: Repo hygiene docs (CONTRIBUTING.md, SECURITY.md, CHANGELOG.md, .gitignore).
- DONE: Versioning and release procedure docs (docs/RELEASING.md).
- DONE: Release readiness audit checklist (docs/RELEASE_AUDIT.md).

Release quality follow-ups (optional)
-------------------------------------
- Lexicon replication support (lexicon segments and lexicon snapshots) in addition to index replication.
  - A0 DONE: docs/LEXICON_SYNC_V1.md contract.
  - A1 DONE: sync-lexicon client command.
  - A2 DONE: integration tests and resilience coverage.
  - A3 DONE: examples and operator docs updates.

- Broader Wiktionary extraction coverage (more POS headers and templates) while keeping deterministic caps and stable ordering.
- Performance pass on Wiktionary ingest and segment building (streaming throughput and allocation trimming) without semantic changes.

