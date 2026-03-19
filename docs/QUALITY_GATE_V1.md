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
- No exemplar row may override evidence-grounded truth.

## Module

Implementation lives in:

- src/quality_gate_v1.rs

Key helpers:

- derive_directives_opt(p: Option<&PragmaticsFrameV1>) -> Option<RealizerDirectivesV1>
- answer path may optionally merge one runtime exemplar advisory before storing
  final directives and deriving Markov hints.
- operator answer output now includes one bounded `routing_trace ...` inspect line
  summarizing the selected planner/forecast route.
- when graph expansion is active and produces bounded term candidates, the
  operator answer path emits one bounded `graph_trace ...` inspect line.
- when a runtime exemplar row is matched, the operator answer path emits one bounded
  `exemplar_match ...` inspect line in the final answer text.
- these inspect lines are diagnostics only; they do not change evidence
  selection, grounded refs, or truth.
- the default user-facing answer surface hides these inspect lines and keeps the
  conversational rendering separate from operator diagnostics.
- derive_markov_hints_surface_choices_opt(...)
 - Derives MarkovHintsV1 for the currently wired opener, transition, closer, and clarifier-intro sites.
 - Filtered to the fixed approved choice ids for those sites.
- derive_markov_hints_opener_preface_opt(...)
 - Retained as an opener-only compatibility helper for focused tests and callers.
- realize_with_quality_gate_v1(...)
 - Runs the realizer with directives + optional Markov hints.
 - Uses fixed conversational preface templates with deterministic ids.
 - Applies bounded clarifying question append (max_questions).
 - Clarifying output uses a fixed lead-in plus one labeled question.
 - In non-debug output, Default and Concise styles use softer plan section labels
   (for example: Main answer, Supporting points, Things to keep in mind).
 - Checklist and StepByStep styles keep the more explicit structural labels.
 - Returns text + observed Markov events.
- build_markov_trace_tokens_v1(...)
 - Emits opener preface choice id first (if present).
 - Emits the wired details-heading transition choice before the first Bullet
   placeholder token for that group (if present).
 - Emits the wired caveat-heading closer choice before the first Caveat
   placeholder token for that group (if present).
 - Emits structural plan_item:* tokens for AnswerPlan item kinds.
 - Emits other:clarifier_intro:<variant> before append:clarify_question when a clarifying question intro was emitted.
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


## Inspect line order

When present, the answer path emits inspect lines in a stable top-of-answer
order:

1. `directives ...` (if directives exist)
2. `routing_trace ...`
3. `graph_trace ...` (only when bounded graph candidates are active)
4. `exemplar_match ...` (only when an exemplar row is matched)

This ordering is intended to keep operator output stable and easy to diff.
The user-facing surface does not show these lines unless `--presentation operator`
is selected.

