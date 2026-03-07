# Context anchors (v1)

Context anchors are a bounded, deterministic set of low-weight query terms
computed from recent conversation history.

They are used to improve retrieval continuity for follow-up prompts that omit
key nouns or identifiers from earlier turns.

This feature is evidence-first:

- Context anchors only influence which evidence rows are retrieved.
- They do not add facts or claims.
- They are capped and low-weight so the current user prompt remains dominant.


When anchors are computed
-------------------------

Anchors are computed when the prompt contains prior conversation messages.

Input:

- PromptPack messages (System/User/Assistant)
- The query message index (the last User message used as the query)
- Optional LexiconSnapshot (when available)

Anchors are derived from messages that occur before the query message.


Lexicon preference
------------------

When a LexiconSnapshot is available, anchor term selection prefers lexicon-backed
content words:

- Token -> lemma key -> lemma ids
- Keep a token if any matching lemma has a content part-of-speech
  (noun/verb/adj/adv/proper)

When a token is not lexicon-backed, a conservative fallback accepts:

- mixed alphanumeric tokens (ids, error codes)
- longer alphabetic tokens (to avoid stopwords)


Bounds
------

v1 uses conservative caps:

- max_messages: 6 (scanned backward from the query message)
- max_total_bytes: 4096
- max_terms: 16
- qtf: 1 (low weight)

These caps are intended to keep work bounded and avoid overwhelming the current
prompt.


Artifact and replay
-------------------

Context anchors are recorded as a replayable artifact:

- ContextAnchorsV1

The answer replay log records a step:

- name: `context-anchors-v1`
- inputs: prompt hash (and lexicon snapshot hash when available)
- outputs: ContextAnchorsV1 hash

This makes anchor derivation auditable and stable across platforms.
