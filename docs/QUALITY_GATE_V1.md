# Quality Gate V1

This document describes the "quality gate" consolidation layer.

The quality gate is a deterministic post-planning integration layer that
combines guidance signals and surface-form selection without introducing new
claims.

## Goals

- Single, shared place to wire:
 - PragmaticsFrameV1 -> RealizerDirectivesV1
 - (Optional) MarkovModelV1 -> MarkovHintsV1 (surface template selection only)
 - PlannerHintsV1 + ForecastV1 -> bounded clarifying question append
 - Realizer Markov events -> MarkovTraceV1 token stream
- Keep behavior deterministic and replay-friendly.
- Ensure answer CLI and scale-demo answer stage produce consistent MarkovTrace
 token streams when surface-template sites are wired.

## Non-goals

- No evidence selection changes.
- No new claims.
- No unbounded generation.

## Module

Implementation lives in:

- src/quality_gate_v1.rs

Key helpers:

- derive_directives_opt(p: Option<&PragmaticsFrameV1>) -> Option<RealizerDirectivesV1>
- derive_markov_hints_opener_preface_opt(...)
 - Derives MarkovHintsV1 for the opener/preface site only.
 - Filtered to the fixed preface:<tone>:{0|1} choice ids.
- realize_with_quality_gate_v1(...)
 - Runs the realizer with directives + optional Markov hints.
 - Applies bounded clarifying question append (max_questions).
 - Returns text + observed Markov events.
- build_markov_trace_tokens_v1(...)
 - Emits opener preface choice id first (if present).
 - Emits structural plan_item:* tokens for AnswerPlan item kinds.
 - Emits append:clarify_question if a question was appended.

## Determinism rules

- All choices are derived from canonical artifacts or stable, bounded rules.
- Markov selection is advisory only and selects among fixed surface templates.
- MarkovTraceV1 uses Id64 values derived by derive_id64("markov_choice_v1", label).
- No wall-clock time, RNG, or non-deterministic iteration ordering.

## ReplayLog linkage

In the answer CLI:

- planner-hints-v1 and forecast-v1 are recorded as stable steps.
- markov-hints-v1 is recorded when enabled via --markov-model.
- markov-trace-v1 is recorded with inputs including answer text hash.
