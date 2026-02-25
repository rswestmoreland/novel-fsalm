RealizerDirectivesV1 (schema v1)
=============================

Purpose
-------
Novel's core loop is evidence-first:

 PromptPack -> retrieval -> EvidenceBundle -> AnswerPlan -> realized text

PragmaticsFrameV1 captures tone/tact/emphasis signals extracted from input text.
RealizerDirectivesV1 is the downstream, realization-focused control plane.

Key properties:
- Deterministic and canonical encoding.
- Advisory: it constrains rendering and style, but does not change evidence selection.
- Small and replay-friendly.

 scope
---------------
Contract-only:
- Schema types + enums + flags.
- Canonical byte codec + validation.
- Unit tests for codec invariants.

 adds a deterministic derivation function from PragmaticsFrameV1.
 integrates directives into the Realizer and Answer CLI.

Schema
------

Fields
~~~~~~

RealizerDirectivesV1:

- version: u16
- tone: ToneV1 (u8)
- style: StyleV1 (u8)
- reserved: u16 (must be 0 in v1)
- format_flags: u32 (FormatFlagsV1)
- max_softeners: u8
- max_preface_sentences: u8
- max_hedges: u8
- max_questions: u8
- rationale_count: u16
- rationale_codes: rationale_count * u16

Enums
~~~~~

ToneV1 (u8)
- 0: Neutral
- 1: Supportive
- 2: Direct
- 3: Cautious

StyleV1 (u8)
- 0: Default
- 1: Concise
- 2: StepByStep
- 3: Checklist
- 4: Debug

Format flags
~~~~~~~~~~~~

FormatFlagsV1 is a u32 bitset.
Only the following bits are defined in v1; unknown bits must be zero.

- bit 0: BULLETS
- bit 1: NUMBERED
- bit 2: INCLUDE_SUMMARY
- bit 3: INCLUDE_NEXT_STEPS
- bit 4: INCLUDE_RISKS
- bit 5: INCLUDE_ASSUMPTIONS

Validation rules
----------------
- version must equal 1.
- reserved must equal 0.
- format_flags must not contain unknown bits.
- rationale_count must be <= 64.
- rationale_codes must be strictly increasing (sorted, no duplicates).

Canonical encoding
------------------
Encoding is little-endian for all integer fields.
No trailing bytes are allowed.

Rationale codes
--------------
Rationale codes are compact, stable integers that explain why a directive
was selected (for traceability and regression testing).

The mapping from codes to meaning is intentionally not locked in v1. The
only requirement is that codes are deterministic and stable within a build.

Future work
-----------
Derivation rules v1
-------------------------------
RealizerDirectivesV1 can be derived from PragmaticsFrameV1 using a rules-first
mapping implemented in `src/realizer_directives.rs`:

`derive_realizer_directives_v1(p: &PragmaticsFrameV1) -> RealizerDirectivesV1`

The derivation is deterministic, integer-only, and must not change evidence.

Derived fields
~~~~~~~~~~~~~~

Tone selection (priority order)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
1) If `SAFETY_SENSITIVE` is set: `Cautious`
2) Else if `empathy_need >= 650`: `Supportive`
3) Else if `mode == Vent`: `Supportive`
4) Else if `directness >= 700` and `politeness <= 350`: `Direct`
5) Else if `arousal >= 650` and `directness >= 600`: `Direct`
6) Else: `Neutral`

Style selection (priority order)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
1) If `HAS_CODE` is set: `Debug`
2) Else if `HAS_MATH` is set: `StepByStep`
3) Else if `HAS_CONSTRAINTS` is set or `mode == Command`: `Checklist`
4) Else if `mode == Brainstorm`: `StepByStep`
5) Else: `Default`

Format flags (advisory)
^^^^^^^^^^^^^^^^^^^^^^^
- If `byte_len >= 300`: `INCLUDE_SUMMARY`
- If `HAS_REQUEST` is set or `mode == Command`: `INCLUDE_NEXT_STEPS`
- If `SAFETY_SENSITIVE` is set: `INCLUDE_RISKS`
- If question is present (flag or '?' count) and `HAS_CONSTRAINTS` is not set:
 `INCLUDE_ASSUMPTIONS`

Additionally, style provides a default list formatting preference:
- `Checklist`: NUMBERED if `mode == Command` or `HAS_REQUEST`, else BULLETS
- `StepByStep`: NUMBERED
- `Debug`: BULLETS

Limits
^^^^^^
These are small caps used by the Realizer to avoid excessive hedging or
over-prefacing. The v1 derivation sets them as a function of `tone` and whether
the input looks like a question without constraints.

Rationale codes
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Rationale codes are stable integers explaining which rules fired.

Codes introduced in:
- 1 SAFETY_SENSITIVE
- 2 EMPATHY_HIGH (empathy_need >= 650)
- 3 VENT_MODE
- 4 DIRECTNESS_HIGH (directness >= 700 with low politeness)
- 10 HAS_CODE
- 11 HAS_MATH
- 12 HAS_CONSTRAINTS
- 13 HAS_REQUEST
- 14 HAS_QUESTION (flag or '?' count)
- 15 LONG_INPUT (byte_len >= 300)
- 16 LOW_POLITENESS (politeness <= 350)
- 17 HIGH_AROUSAL (arousal >= 650)

Canonical form: `rationale_codes` must be strictly increasing with no
duplicates.

Future work
-----------
- Store directives as a first-class artifact and reference them in ReplayLog.
- Add Realizer consumption of directives (format, tone, structure caps).
