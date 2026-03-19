MarkovModelV1
=============

MarkovModelV1 is a replayable, deterministic Markov/PPM model artifact used to
produce MarkovHintsV1. It stores bounded n-gram transition counts over
surface-form tokens (choice kind + choice id).

This document defines the artifact schema and canonical codec. The current
runtime can build, store, inspect, and apply MarkovModelV1 artifacts, while
this doc stays focused on the stable on-disk contract.

Training rules and deterministic pruning are specified in
docs/MARKOV_TRAINING_CONTRACT_V1.md.

Design goals
------------

- Deterministic: stable ordering, stable tie-break tokens, canonical encoding.
- Replayable: can be stored as an artifact and referenced from MarkovHintsV1.
- Bounded: hard caps on the number of states and next entries per state.
- Advisory: affects only phrasing choices among already-allowed templates.

Non-goals
---------

- MarkovModelV1 does not grant permission to invent facts.
- MarkovModelV1 does not define every training or runtime policy detail
 (ReplayLog selection, token extraction budgets, or higher-level wiring).

Schema
------

Types live in `src/markov_model.rs`.

MarkovTokenV1
-------------

A token is the pair:

- `kind: MarkovChoiceKindV1` (u8)
- `choice_id: Id64` (u64)

MarkovStateV1
-------------

One state is:

- `context: Vec<MarkovTokenV1>`
 - Length may be 0 (unconditional distribution).
 - Length must be `< order_n_max`.
- `escape_count: u32`
 - Reserved for PPM-style estimators.
- `next: Vec<MarkovNextV1>`
 - Bounded list of next-token counts.

MarkovNextV1
------------

- `token: MarkovTokenV1`
- `count: u32`

MarkovModelV1
-------------

- `version: u32` must equal `MARKOV_MODEL_V1_VERSION`.
- `order_n_max: u8` must be 1..=6 in v1.
- `max_next_per_state: u8` must be 1..=64 in v1.
- `total_transitions: u64` total observed transitions used to build the model.
- `corpus_hash: Hash32` hash of the training corpus (e.g., a ReplayLog set).
- `states: Vec<MarkovStateV1>`

Hard caps (v1)
--------------

- Max order: `MARKOV_MODEL_V1_MAX_ORDER_N` (6)
- Max states: `MARKOV_MODEL_V1_MAX_STATES` (200000)
- Max next entries per state: `MARKOV_MODEL_V1_MAX_NEXT_PER_STATE` (64)

Canonical ordering
------------------

State list canonical order:

1) `context.len` descending (higher-order contexts first)
2) `context` lexicographic ascending by tokens
 - token order: `kind` ascending then `choice_id` ascending

Duplicate contexts are rejected.

Next list canonical order (within each state):

1) `count` descending
2) `token` ascending

Uniqueness: `(token.kind, token.choice_id)` must be unique within a state's
`next` list.

Canonical encoding
------------------

Encoding uses the shared byte codec utilities.

Header layout:

1) `u32 version`
2) `u8 order_n_max`
3) `u8 max_next_per_state`
4) `u8 reserved0` (0)
5) `u8 reserved1` (0)
6) `u32 states_n`
7) `u64 total_transitions`
8) `32 bytes corpus_hash`

Then `states_n` repeated states:

1) `u8 context_n`
2) `u8 next_n`
3) `u16 reserved` (0)
4) `context_n` repeated tokens:
 - `u8 kind`
 - `u64 choice_id`
5) `u32 escape_count`
6) `next_n` repeated next entries:
 - `u8 kind`
 - `u64 choice_id`
 - `u32 count`

Decoding rejects trailing bytes and rejects non-canonical lists.
