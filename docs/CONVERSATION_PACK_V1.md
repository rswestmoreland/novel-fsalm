ConversationPackV1
==================

Overview
--------
ConversationPackV1 is a resumable chat history artifact.
It stores an ordered list of System/User/Assistant messages plus the key
deterministic knobs needed to continue a conversation across runs.

This artifact is immutable and content-addressed like all other artifacts in
this repo. To update a conversation, write a new ConversationPackV1 and update
an external pointer (for example, a small session file).

Goals
-----
- End-user UX: allow chat sessions that can be resumed without manual hash
  wiring.
- Determinism: stable bytes and hashes for the same conversation state.
- Auditability: optionally record per-assistant replay ids.
- Bounded memory: apply deterministic truncation and message caps.

Non-goals
---------
- No wall-clock timestamps.
- No probabilistic sampling metadata.
- No network identity or per-user profiles.

Binary format (v1)
------------------
All integers are little-endian.

Header:
- u16 version (must be 1)
- u64 seed
- u32 max_output_tokens
- Hash32 snapshot_id
- Hash32 sig_map_id
- u8 has_lexicon (0 or 1)
- if has_lexicon == 1: Hash32 lexicon_snapshot_id

PromptLimits (recorded to make truncation stable across releases):
- u32 max_message_bytes
- u32 max_total_message_bytes
- u32 max_messages
- u8 keep_system (0 or 1)

Messages:
- u32 messages_len
- Message repeated

Optional advisory trailer:
- Legacy v1 packs may end immediately after the message list.
- Newer v1 packs may append these sticky advisory ids:
  - u8 has_markov_model (0 or 1)
  - if has_markov_model == 1: Hash32 markov_model_id
  - u8 has_exemplar_memory (0 or 1)
  - if has_exemplar_memory == 1: Hash32 exemplar_memory_id
  - u8 has_graph_relevance (0 or 1)
  - if has_graph_relevance == 1: Hash32 graph_relevance_id
  - u8 has_presentation_mode (0 or 1)
  - if has_presentation_mode == 1: u8 presentation_mode
    - 0: user
    - 1: operator

Message encoding
----------------
Each Message is:
- u8 role
  - 0: system
  - 1: user
  - 2: assistant
- str content (length-prefixed UTF-8)
- u8 has_replay (0 or 1)
- if has_replay == 1: Hash32 replay_id

Notes:
- replay_id is intended to reference a ReplayLog artifact for the assistant
  turn that produced this message.
- has_replay may be 0 for System and User messages.
- When a saved ConversationPackV1 is used to resume `ask` or `chat`, the stored
  sticky advisory ids and stored `presentation_mode` are reused automatically
  unless the caller passes newer explicit overrides on the command line.

Canonicalization
----------------
ConversationPackV1 MUST be canonical before encoding.

Rules:
- Messages remain in given order.
- Message content is truncated to a UTF-8 prefix at a char boundary so that
  content_len_bytes <= max_message_bytes.
- If message count or total message bytes exceed limits:
  - If keep_system is true, keep System messages preferentially.
  - Fill remaining slots with the most recent non-system messages.
  - If still over total bytes, truncate the most recent kept message first.
  - If still over, drop oldest non-system messages next.

The rules match the PromptLimits truncation behavior used for PromptPack.
Decoders should validate self-consistency (for example, that the encoded
messages do not exceed recorded limits).

How it is used
--------------
A typical resumable chat loop:
- Load an existing ConversationPackV1 (or start empty).
- Append the next user message.
- Construct a PromptPack from the conversation messages.
- Run the normal answering pipeline.
- Append the assistant message (optionally with replay_id).
- Canonicalize using the recorded PromptLimits.
- Encode and store a new ConversationPackV1.

Session pointer file (v1)
-------------------------
For end-user usability, a small ASCII session file can store the current
ConversationPack id.

Location (suggested):
- <root>/chat_session.txt

Format:
- key=value pairs, same parsing rules as workspace_v1.txt

Key:
- conversation_pack=<hex>

Writers should update the file atomically (write temp then rename).

Determinism notes
-----------------
- ConversationPackV1 hashing depends only on canonical bytes.
- snapshot_id, sig_map_id, and lexicon_snapshot_id are recorded so resuming a
  conversation can be deterministic even if the workspace defaults change.
- markov_model_id, exemplar_memory_id, and graph_relevance_id may also be
  recorded so resumed conversations can preserve the same advisory artifact
  selections across runs.
- presentation_mode may also be recorded so a saved operator workflow or the
  default user-facing workflow can be resumed consistently across runs.
- If a caller chooses to override these ids at runtime, that override must be
  explicit and should be recorded in the next saved ConversationPack.
- Decoders must accept both legacy v1 packs with no advisory trailer and newer
  v1 packs that include the advisory trailer. The presentation field is also optional
  within that trailer for backward compatibility with earlier saved packs.
