PlannerHintsV1
==============

PlannerHintsV1 is a replayable, deterministic advisory record used to steer the
Planner and Realizer toward more interactive, fluent conversations while
preserving Novel's evidence-first guarantees.

This document defines the schema and canonical codec and records the current
pipeline wiring used by the answer path. PlannerHintsV1 is already generated
deterministically, stored as an artifact, and consumed by planning and quality
gate logic.

Design goals
------------

- Deterministic: stable ordering, stable tie-break ids, and canonical encoding.
- Replayable: can be stored as an artifact and referenced from ReplayLog.
- Bounded: hard caps on list sizes and per-string byte lengths.
- Rules-first: rationale is numeric-coded; no free-form reasoning text.

Non-goals
---------

- This schema does not grant the system permission to invent facts.
- Hints may influence structure, tone, or whether to ask a clarifying question,
 but they must not override evidence constraints.

Schema
------

Types live in `src/planner_hints.rs`.

PlannerHintsV1
--------------

- `version: u32` must equal `PLANNER_HINTS_V1_VERSION`.
- `query_id: Hash32` hash of the user query bytes.
- `flags: u32` bitflags:
 - `PH_FLAG_PREFER_CLARIFY`
 - `PH_FLAG_PREFER_DIRECT`
 - `PH_FLAG_PREFER_STEPS`
 - `PH_FLAG_PREFER_CAVEATS`
 - Unknown bits are rejected.
- `hints: Vec<PlannerHintItemV1>` ranked hint items.
- `followups: Vec<PlannerFollowupV1>` ranked followup suggestions.

PlannerHintItemV1
-----------------

- `kind: PlannerHintKindV1` (u8)
- `hint_id: Id64` stable tie-break id
- `score: i64` signed rank score
- `rationale_code: u16` rules-first rationale id

PlannerFollowupV1
-----------------

- `followup_id: Id64` stable tie-break id
- `score: i64` signed rank score
- `text: String` UTF-8 followup prompt (capped)
- `rationale_code: u16` rules-first rationale id

Hard caps (v1)
--------------

- Max hints: `PLANNER_HINTS_V1_MAX_HINTS` (64)
- Max followups: `PLANNER_HINTS_V1_MAX_FOLLOWUPS` (32)
- Max followup text bytes: `PLANNER_HINTS_V1_MAX_TEXT_BYTES` (512)

Canonical ordering and uniqueness
--------------------------------

To keep hashes stable across platforms, the encoded lists must be canonical.

Hint canonical order:

1) `score` descending
2) `kind` ascending
3) `hint_id` ascending

Uniqueness: `(kind, hint_id)` must be unique.

Followup canonical order:

1) `score` descending
2) `followup_id` ascending

Uniqueness: `followup_id` must be unique.

Canonical encoding
------------------

Encoding uses the shared byte codec utilities.

Layout:

1) `u32 version`
2) `32 bytes query_id`
3) `u32 flags`
4) `u8 hints_n`
5) `u8 followups_n`
6) `hints_n` repeated:
 - `u8 kind`
 - `u64 hint_id`
 - `i64 score`
 - `u16 rationale_code`
7) `followups_n` repeated:
 - `u64 followup_id`
 - `i64 score`
 - `str text` (codec length + bytes)
 - `u16 rationale_code`

Decoding rejects trailing bytes and rejects non-canonical lists.

Integration notes
-----------------

PlannerHintsV1 is intended to be produced by a deterministic hint generator that
reads:

- PragmaticsFrame (tone/tact/emphasis)
- Query text
- EvidenceBundle / EvidenceSet metadata
- Optional forecast modules (Markov-lite, bounded)

The Planner may use hints to decide whether to:

- ask one clarifying question,
- choose a more structured AnswerPlan,
- select RealizerDirectives defaults.

Even with hints, the planner must continue to obey evidence constraints.


## builder notes (planner integration)

PlannerHintsV1 is derived deterministically from:

- EvidenceBundleV1 (item count, and whether evidence spans multiple segments)
- PragmaticsFrameV1 flags when available (question/request/constraints/code/math/follow-up/safety)

Rules (high level):

- Prefer clarify when evidence is empty or weak (0 items), or when we see a follow-up question with low evidence.
- Prefer steps when we see constraints, code, or math.
- Prefer steps when pragmatics indicates problem-solving or a logic puzzle.
- Prefer caveats when the conversation is safety-sensitive or evidence is empty.
- Emit SummaryFirst when we have enough evidence (>= 3 items), or when Pragmatics explicitly requests a summary and evidence is non-empty.
- Emit Compare when evidence spans multiple segments, or when Pragmatics explicitly requests a comparison.
- Prefer Steps for explain-style requests without changing evidence selection.

Ordering, dedupe, and caps follow the canonical rules described above (score desc; tie-break kind asc then id asc).


Clarifying append behavior (v1):

- The quality gate appends at most one clarifying question when `PH_FLAG_PREFER_CLARIFY` is set and directives allow questions.
- The question text comes from the top-ranked `ForecastV1.questions[0]` entry.
- For problem-solving and logic puzzles, the forecast should prioritize specific disambiguation questions (expected vs actual, minimal reproduction, variables/domains).
- For explicit compare targets, the planner should prefer criteria-focused compare followups over generic option-selection prompts.
- For summary-first, step-by-step, or example-led response-focus cues, the planner should keep the same evidence path but ask more specific clarifiers about answer shape.
