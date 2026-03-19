Chat flow acceptance
====================

Purpose
-------
This document defines the regression surface for conversation-shaping work.
It is intended to lock behavior before changes to routing, phrasing, graph
expansion, or exemplar guidance are introduced.

Scope for this acceptance layer
--------------------------------
The acceptance layer covers the following behavioral guarantees:

- multi-turn continuity for ask/chat session workflows
- pronoun follow-up handling through existing conversation state
- one-best-clarifier behavior
- stable repeated outputs for the same input and context
- evidence preservation under conversational shaping

Principles
----------
- Evidence-first remains authoritative.
- The acceptance harness is black-box where possible: it should exercise the
  user-facing CLI rather than re-testing internal helper functions.
- Repeated runs with the same workspace, prompt, and conversation state must
  produce the same output bytes after line-ending normalization.
- Conversation improvements may change phrasing only where the tests are
  intentionally updated to reflect a new deterministic output.
- No acceptance test may permit unsupported claims.

Current test coverage
---------------------
Existing tests already cover key parts of the conversation path:

- tests/chat_cli.rs
  - basic chat execution using workspace defaults
- tests/conversation_pack_chat_resume_cli.rs
  - save and resume behavior through ConversationPackV1
- tests/conversation_pack_chat_session_file_cli.rs
  - chat session file persistence
- tests/conversation_pack_ask_session_file_cli.rs
  - ask session file persistence
- tests/context_anchors_in_session_cli.rs
  - retrieval continuity in chat sessions via ContextAnchorsV1
- tests/context_anchors_in_session_ask_cli.rs
  - retrieval continuity in ask sessions via ContextAnchorsV1
- tests/markov_chat_context_in_session_cli.rs
  - Markov chat history context in interactive sessions
- tests/markov_chat_context_resume_cli.rs
  - Markov chat history context across resume flows
- tests/puzzle_free_text_clarify_cli.rs
  - conversational clarifier behavior for free-text logic prompts
- tests/puzzle_parse_failure_clarify_cli.rs
  - clarifier fallback when logic parsing fails

Focused acceptance additions
----------------------------
The dedicated chat-flow acceptance file adds black-box checks for:

- repeated-output stability for the same ask input and workspace
- one-best-clarifier behavior for a conversational logic prompt
- pronoun follow-up continuity in ask sessions, verified by preserved evidence output and
  preserved subject anchors in conversation-state artifacts
- integrated answer-path regression covering routing, bounded graph enrichment,
  exemplar guidance, and Markov phrasing while re-locking evidence lines and grounded
  plan refs

What this document does not do
------------------------------
- It does not define a second renderer.
- It does not define a second planner.
- It does not replace deeper artifact-level tests for replay, anchors, or
  Markov traces.
- It does not permit style-only layers to override grounding.

Update rule
-----------
When a conversation-flow change is intentional, update this document and the
associated CLI regression tests in the same change set.
