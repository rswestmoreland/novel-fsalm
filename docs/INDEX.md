Docs index
==========

This file is a navigation aid. For stage status, see docs/MASTER_PLAN.md.

Start here
----------
- README.md: project overview and guardrails
- docs/NOVEL.md: name and scope
- docs/MASTER_PLAN.md: authoritative stage checklist
- docs/ROADMAP.md: short progress view
- docs/RELEASING.md: how to cut a release
- docs/RELEASE_AUDIT.md: release readiness checklist

Core architecture
-----------------
- docs/DETERMINISM.md: what "deterministic" means in this repo
- docs/FRAMES.md: frame model and schemas
- docs/TOKENIZER.md: tokenization
- docs/METAPHONE.md: metaphonetics
- docs/LEXICON.md: lexicon model and snapshots
- docs/RETRIEVAL_PIPELINE.md: retrieval and evidence construction (high level)
- docs/TWO_PASS_RETRIEVAL.md: deterministic two-pass scoring overview
- docs/REASONING_FLOW.md: end-to-end reasoning diagram
- docs/SYNAPSE_TRAINING.md: training export and inference hooks

Artifacts and storage
---------------------
- docs/ARTIFACTS.md: content-addressed store layout
- docs/FRAME_STORE.md: frame storage overview
- docs/INDEX_SNAPSHOT_V1.md: IndexSnapshot schema
- docs/INDEX_SEGMENT_V1.md: IndexSegment schema
- docs/INDEX_SIG_MAP_V1.md: IndexSigMap schema
- docs/SEGMENT_SIG_V1.md: SegmentSig schema (gating)
- docs/EVIDENCE_BUNDLE_V1.md: EvidenceBundle schema
- docs/EVIDENCE_BUILDER_V1.md: Evidence builder details
- docs/SCALE_REPORT_V1.md: Scale demo scale report artifact

Answering
---------
- docs/ANSWERING_LOOP.md: Planner/Realizer loop
- docs/REALIZER_DIRECTIVES_V1.md: realization control directives schema
- docs/PLANNER_HINTS_V1.md: planner hint schema
- docs/FORECAST_V1.md: forecast schema
- docs/MARKOV_HINTS_V1.md: Markov/PPM hint schema
- docs/MARKOV_MODEL_V1.md: Markov/PPM model schema
- docs/MARKOV_TRACE_V1.md: Markov/PPM choice trace schema
- docs/MARKOV_TRAINING_CONTRACT_V1.md: Markov training rules and caps contract
- docs/LEXICON_QUERY_EXPANSION.md: bounded query expansion rules
- docs/BRIDGE_EXPANSION.md: bridge expansion notes
- docs/EXPANSION_BUDGET_V1.md: bridge expansion budget contract

CLI
---
- docs/CLI.md: command reference
- docs/REPLAY.md: ReplayLog and replayable workflows
- docs/REPLAY_STEP_CONVENTIONS.md: step name conventions and required hash sets

Scale demo (Track C)
--------------------
Scale demo is the deterministic end-to-end health check.

- docs/SCALE_DEMO.md: stage overview
- docs/SCALE_DEMO_RUNBOOK.md: how to run and what to compare
- docs/SCALE_DEMO_MEMORY.md: memory caps and sizing guidance
- docs/SCALE_DEMO_REGRESSION_PACK.md: fixed-seed regression pack contract

Examples
--------
- examples/README.md
- examples/demo_cmd_scale_demo_full_loop.bat
- examples/demo_cmd_scale_demo_full_loop.sh
