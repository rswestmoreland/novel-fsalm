MarkovHintsV1
=============

MarkovHintsV1 is a replayable, deterministic advisory record used to guide
surface-form selection (openers/transitions/closers) without changing Novel's
evidence-first guarantees.

This stage defines the schema and canonical codec. The current quality
path wires a bounded subset of approved opener, transition, closer, and clarifier-intro template ids.

Design goals
------------

- Deterministic: stable ordering, stable tie-break ids, and canonical encoding.
- Replayable: can be stored as an artifact and referenced from ReplayLog.
- Bounded: hard caps on list sizes.
- Advisory: affects only phrasing choices among already-allowed templates.

Non-goals
---------

- MarkovHintsV1 does not grant permission to invent facts.
- MarkovHintsV1 must not introduce new claims beyond what the Planner/Realizer
 would otherwise emit from evidence.

Schema
------

Types live in `src/markov_hints.rs`.

MarkovHintsV1
-------------

- `version: u32` must equal `MARKOV_HINTS_V1_VERSION`.
- `query_id: Hash32` hash of the user query bytes.
- `flags: u32` bitflags:
 - `MH_FLAG_HAS_HISTORY`
 - `MH_FLAG_HAS_PRAGMATICS`
 - `MH_FLAG_USED_PPM`
 - `MH_FLAG_USED_LEXICON`
 - Unknown bits are rejected.
- `order_n: u8` Markov order (n-gram length). Must be 1..=6 in v1.
- `state_id: Id64` stable state id for deterministic tie-breaking.
- `model_hash: Hash32` hash of the Markov model artifact used to compute hints.
- `context_hash: Hash32` hash of the context token stream used to compute hints.
- `choices: Vec<MarkovChoiceV1>` ranked surface-form choices.

MarkovChoiceV1
--------------

- `kind: MarkovChoiceKindV1` (u8)
- `choice_id: Id64` stable tie-break id
- `score: i64` signed rank score
- `rationale_code: u16` rules-first rationale id

MarkovChoiceKindV1
------------------

- `Opener` (1)
- `Transition` (2)
- `Closer` (3)
- `Other` (4)

Hard caps (v1)
--------------

- Max choices: `MARKOV_HINTS_V1_MAX_CHOICES` (32)
- Max order: `MARKOV_HINTS_V1_MAX_ORDER_N` (6)

Canonical ordering and uniqueness
--------------------------------

To keep hashes stable across platforms, encoded lists must be canonical.

Choice canonical order:

1) `score` descending
2) `kind` ascending
3) `choice_id` ascending

Uniqueness: `(kind, choice_id)` must be unique.

Canonical encoding
------------------

Encoding uses the shared byte codec utilities.

Layout:

1) `u32 version`
2) `32 bytes query_id`
3) `u32 flags`
4) `u8 order_n`
5) `u8 choices_n`
6) `u8 reserved0` (0)
7) `u8 reserved1` (0)
8) `u64 state_id`
9) `32 bytes model_hash`
10) `32 bytes context_hash`
11) `choices_n` repeated:
 - `u8 kind`
 - `u64 choice_id`
 - `i64 score`
 - `u16 rationale_code`

Decoding rejects trailing bytes and rejects non-canonical lists.

Integration notes
-----------------

MarkovHintsV1 is intended to be produced by a deterministic hint generator that
is trained offline from ReplayLog outputs (realized token streams), optionally
anchored by lexicon segments and pragmatics flags.

The training output is stored as a MarkovModelV1 artifact (docs/MARKOV_MODEL_V1.md).

Training and corpus hashing rules are specified in docs/MARKOV_TRAINING_CONTRACT_V1.md.

The realizer may use MarkovHintsV1 only to choose between pre-approved surface
forms (templates) that do not add claims.
