ForecastV1
==========

ForecastV1 is a replayable, deterministic prediction record for what the user
may ask next.

It is advisory and must not change evidence-first contracts.

Use cases
---------
- Planner/realizer can use forecast items to choose clarifying questions,
 surface next-step prompts, or adjust interaction flow.
- ReplayLog can store a forecast artifact for auditing and regression testing.

This is a schema-only contract.

Schema (v1)
-----------

Fields:
- version: u32 (must be 1)
- query_id: Hash32 (32 bytes)
- flags: u32 (ForecastFlagsV1)
- horizon_turns: u8 (1..=4; v1 typically uses 1)
- n_intents: u8 (<= 32)
- n_questions: u8 (<= 16)
- reserved: u8 (0)
- intents: n_intents * ForecastIntentV1
- questions: n_questions * ForecastQuestionV1

ForecastIntentV1:
- kind: u8 (ForecastIntentKindV1)
- intent_id: u64 (stable id for tie-breaking)
- score: i64 (rank score)
- rationale_code: u16 (rules-first code; 0 allowed)

ForecastQuestionV1:
- question_id: u64 (stable id for tie-breaking)
- score: i64
- text: str (u16 length prefix + UTF-8 bytes)
- rationale_code: u16

Caps:
- intents.len <= 32
- questions.len <= 16
- question text bytes <= 512

Flags
-----

ForecastFlagsV1 is a u32 bitset. Unknown bits are rejected by validation and
decoder.

Known bits (v1):
- 1<<0 FC_FLAG_HAS_PRAGMATICS: input included PragmaticsFrameV1
- 1<<1 FC_FLAG_HAS_HISTORY: input included prior conversation context
- 1<<2 FC_FLAG_USED_MARKOV: Markov/PPM style hints were used
- 1<<3 FC_FLAG_USED_LEXICON: lexicon context was used

Canonical order
---------------

ForecastV1 is canonical if it satisfies:

Intents (ForecastIntentV1 list):
- Sorted by score descending
- Then kind ascending
- Then intent_id ascending
- No duplicate (kind, intent_id)

Questions (ForecastQuestionV1 list):
- Sorted by score descending
- Then question_id ascending
- No duplicate question_id

Canonical encoding is required for artifacts and ReplayLog references.

Determinism notes
-----------------

- ForecastV1 stores predictions, not generation randomness.
- If a future builder uses counts/Markov, it must be bounded and deterministic
 (stable ordering, stable tie-breakers).
- Forecast must not introduce new claims; it can only affect interaction style
 (for example, whether to ask a clarification question).


## builder notes (planner integration)

ForecastV1 is derived deterministically from:

- PlannerHintsV1 flags and hint kinds
- PragmaticsFrameV1 flags when available

Rules (high level):

- Always include a small set of base intents (Example, MoreDetail).
- Add Clarify when PlannerHints prefers clarify.
- Add Compare when PlannerHints includes Compare.
- Add NextSteps when Pragmatics indicates a request.
- Add Implementation and VerifyOrTroubleshoot when Pragmatics indicates code (or when PlannerHints prefers steps).
- Add Risks when Pragmatics indicates safety-sensitive or when PlannerHints prefers caveats.

Questions are similarly template-driven and capped (max 4) and ordered canonically (score desc; tie-break id asc).

Horizon is currently 1 turn (top-of-next-message forecast).
