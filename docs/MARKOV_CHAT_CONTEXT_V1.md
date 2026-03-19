# Markov chat context (v1)

This document specifies how Novel derives Markov hint context tokens from
conversation history for interactive sessions.

The purpose is to let MarkovHintsV1 be conditioned on prior observed
surface/structure choice tokens (MarkovTraceV1) while preserving:

- determinism (stable inputs -> stable outputs)
- bounded memory and CPU
- evidence-first guarantees (only phrasing selection among pre-approved forms)

This contract is intentionally narrow: it defines only the history-to-context
mapping and the context hashing rules.


Summary
-------

When a Markov model is supplied (for example via `chat --markov-model <hash>`),
Novel builds a bounded context token stream from prior assistant turns and uses
that stream as `context_tokens` when deriving MarkovHintsV1.

The context stream is derived from MarkovTraceV1 tokens that were emitted while
rendering previous assistant replies.


Inputs
------

- MarkovTraceV1 (docs/MARKOV_TRACE_V1.md)
 - Per assistant turn, an ordered token stream of (kind, choice_id).
- MarkovHintsV1 (docs/MARKOV_HINTS_V1.md)
 - Uses `context_hash` and flags to record whether history was present.


Context token source
--------------------

Only prior assistant turns contribute context.

- For each assistant reply, take the MarkovTraceV1 token stream observed while
  rendering that reply.
- User messages do not contribute Markov context tokens.
- The current turn does not contribute tokens to its own context.

Token order is preserved:

- Concatenate prior assistant-turn token streams in chronological order.
- Do not sort or deduplicate tokens.


Bounded tail rule
-----------------

To keep the context bounded and stable, only the last N tokens are used.

v1 parameter:

- MARKOV_CHAT_CONTEXT_MAX_TOKENS = 64

Algorithm:

1) Let `all` be the concatenation of prior assistant trace tokens.
2) If `len(all) <= N`, context_tokens = all.
3) Else context_tokens = the last N tokens of all.

This tail selection must be deterministic and must not depend on timing.


Context hashing
--------------

MarkovHintsV1 stores `context_hash`, the blake3 hash of the context token
stream bytes.

To ensure stability across platforms, v1 uses a fixed byte layout:

- u16 token_count (little-endian)
- repeated token_count times:
  - u8 kind
  - u64 choice_id (little-endian)

This is the same token layout used inside MarkovTraceV1, but without the trace
header fields.

Rules:

- token_count must equal `context_tokens.len()`.
- When the context is empty, token_count is 0 and no token bytes follow.


Flags
-----

When deriving MarkovHintsV1:

- If `context_tokens` is non-empty, set MH_FLAG_HAS_HISTORY.
- If `context_tokens` is empty, MH_FLAG_HAS_HISTORY must be unset.


Interaction with chat sessions
------------------------------

In an interactive `chat` session, the context tail is updated after each
assistant reply:

- Append that turn's MarkovTraceV1 tokens to the rolling buffer.
- Truncate to the last N tokens.

This produces a stable, bounded history signal that can influence phrasing
selection in later turns.


Resume integration
------------------

When ConversationPackV1 is used to resume sessions across runs, the same
contract applies. The context tail is reconstructed from saved assistant turns
by loading their recorded ReplayLog outputs and extracting the referenced
MarkovTraceV1 artifacts when replay ids are present.


Related: context anchors for retrieval
-------------------------------------

Markov chat context is used only for phrasing selection (surface forms).

Separately, Novel derives low-weight context anchors from recent conversation
message text and merges them into the retrieval query term list. This improves
follow-up retrieval continuity when the user omits key terms from prior turns.

See docs/CONTEXT_ANCHORS_V1.md.
