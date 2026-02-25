Markov Training Contract V1
==========================

This document defines the deterministic training contract used to build a
MarkovModelV1 artifact (docs/MARKOV_MODEL_V1.md) and to derive MarkovHintsV1
(docs/MARKOV_HINTS_V1.md).

This stage is contract-only: it specifies inputs, counting rules,
canonicalization, and bounded pruning. Implementation is handled in 

Goals
-----

- Deterministic across platforms and runs.
- Replayable: training inputs are addressable artifacts (ReplayLog sets).
- Bounded: outputs respect MarkovModelV1 hard caps.
- Advisory: training learns only surface-form selection behavior.

Non-goals
---------

- This contract does not define new claims or knowledge.
- This contract does not require probabilistic RNG sampling.
- This contract does not define a free form generator.

Inputs
------

The training input is a list of ReplayLog artifacts (or a GoldenPack report that
enumerates ReplayLog hashes). Each ReplayLog is assumed to contain the artifacts
needed to reconstruct a surface-form choice stream for a turn.

Minimum required per turn
-------------------------

The training pipeline requires a deterministic sequence of surface-form choices
for the answer text. Each choice event yields one MarkovTokenV1:

- kind: MarkovChoiceKindV1 (Opener/Transition/Closer/Other)
- choice_id: Id64 stable id for the surface-form template

 provides MarkovTraceV1 as the concrete replayable record for this
stream (docs/MARKOV_TRACE_V1.md).

Early traces may include structural placeholder tokens (plan_item:*). Starting
in (Option B), when a realizer surface-template site is wired, the
trace MUST record the actual template choice id used at that site (for example
preface:* for the opener preface line).

Choice id derivation
--------------------

Surface-form templates MUST have stable identifiers. The canonical derivation is:

- choice_id = derive_id64(b"markov_choice_v1", template_bytes)

where template_bytes is the ASCII template name plus any explicit version
suffix needed to avoid collisions (for example "opener.neutral.v1").

No random ids are allowed.

Corpus hash
-----------

MarkovModelV1 stores corpus_hash: Hash32. It MUST be computed as follows:

1) Collect the list of MarkovTraceV1 hashes used for training.
 - The source-of-truth input is a list of ReplayLog hashes (or a GoldenPack report).
 - Each ReplayLog contributes zero or more markov-trace-v1 step outputs.
2) Canonicalize the trace list: sort ascending by bytes, then dedupe.
3) If an optional trace cap is applied, truncate the canonical list to the first N.
4) Compute:

 corpus_hash = blake3("markov_corpus_v1" || encode(cfg) || traces...)

where:

- encode(cfg) is a canonical byte encoding of the training configuration
 fields listed in this document (order and caps).
- traces... is the concatenation of the 32-byte trace hashes in canonical order.

This ensures the same trace set and config yields the same corpus_hash.


Context hash
------------

MarkovHintsV1 stores context_hash: Hash32. It MUST be computed from the input
context token stream as:

context_hash = blake3("markov_context_v1" || encode(tokens))

where encode(tokens) is the concatenation of (kind_u8, choice_id_u64_le) for
each token in order.

Training configuration (v1)
---------------------------

The training builder MUST be configured with bounded parameters:

- order_n_max: u8 in 1..=6
- max_next_per_state: u8 in 1..=64
- max_states: u32 in 1..=MARKOV_MODEL_V1_MAX_STATES

The builder MAY also accept optional filters, but they MUST be deterministic and
included in encode(cfg) if they affect output.

Counting rules
--------------

Let an answer choice stream be tokens t[0..m).

For each position i in 0..m:

- Let next = t[i].
- For each context length k in 0..=min(i, order_n_max - 1):
 - Let ctx = t[i-k.. i] (the preceding k tokens).
 - Increment count(ctx, next) by 1.

Notes:

- The empty context k=0 is always included (unconditional distribution).
- This is an n-gram count model suitable for PPM-style backoff.
- escape_count is reserved in v1 and MUST be set to 0 by the builder.

total_transitions
-----------------

MarkovModelV1.total_transitions MUST equal the total number of observed next
tokens across all streams:

total_transitions = sum_over_streams(stream_len)

This value is independent of order_n_max.

Deterministic state and next pruning
-----------------------------------

After counting, the builder converts internal maps to canonical vectors and
enforces hard caps.

Next pruning (per state)
------------------------

If a state has more than max_next_per_state next entries:

1) Sort next entries by canonical order:
 - count descending
 - token.kind ascending
 - token.choice_id ascending
2) Keep the first max_next_per_state entries.

State pruning (global)
----------------------

If there are more than max_states states, the builder MUST prune deterministically.

Define each state weight:

- weight = sum(next.count)

Then rank states by:

1) weight descending
2) context.len descending
3) context lexicographic ascending by tokens

Keep the first max_states states.

After pruning, the final state list MUST be sorted into the canonical order
defined by MarkovModelV1 (docs/MARKOV_MODEL_V1.md).

Merging multiple sources
------------------------

If the training run merges multiple ReplayLog sources:

- Counts for identical (context, next) pairs MUST be summed.
- The merged result MUST be identical to training on the concatenated stream
 set under the same config.

Implementation notes for determinism
-----------------------------------

- HashMap iteration order must not affect output.
- All intermediate key sets must be materialized into vectors and sorted using
 the canonical comparisons.
- Any filtering that depends on byte content must use normalized bytes (ASCII
 template names, stable ids) and be included in the config hash.

Deriving MarkovHintsV1 (high level)
----------------------------------

MarkovHintsV1 is derived from:

- the trained MarkovModelV1
- an input context token stream for the current conversation

The hint generator MUST be deterministic and bounded:

- It may use PPM-style backoff from longer contexts to shorter contexts.
- It must produce at most MARKOV_HINTS_V1_MAX_CHOICES ranked choices.
- It must assign stable choice_id and stable state_id for tie-breaking.
- It must not introduce new claims; only select among already-allowed surface
 forms.

CLI integration
---------------

 provides two CLI commands for this contract:

- build-markov-model: trains a bounded MarkovModelV1 from replay logs.
- inspect-markov-model: loads and validates a MarkovModelV1 and prints a stable summary.

See docs/CLI.md for full flags.

Realizer integration hooks
-------------------------

A first realizer integration hook (surface-form selection only) is described in
`docs/MARKOV_REALIZER_HOOKS_V1.md`.

