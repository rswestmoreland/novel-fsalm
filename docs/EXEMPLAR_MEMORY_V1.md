ExemplarMemory v1
=================

Purpose
-------
ExemplarMemoryV1 is a deterministic, structure-only artifact for conversation
improvement work.

It stores compact answer-shape patterns that can later be used only as
advisory guidance for:
- response structure choice
- clarifier shape
- next-step phrasing bias
- recommendation-versus-comparison presentation style

It does not store:
- truth facts
- evidence rows
- retrieved passages
- free-form generated text
- any signal that can override the evidence bundle

Scope of v1
-----------
This stage locks the artifact contract and builder scaffold only.

Included now:
- canonical schema and codec
- content-addressed artifact helpers
- supported offline source-family inventory
- deterministic builder plan scaffolding
- deterministic caps for inputs, rows, and support refs
- conservative offline mining from:
  - PromptPack last-user-message request shape
  - ConversationPack last-user-message request shape
  - MarkovTrace bounded surface-shape tokens

Not included yet:
- builder CLI command
- runtime lookup
- planner or realizer integration
- inspect CLI wiring

Top-level fields
----------------
- version
- build_id
- flags
- rows

Artifact flags
--------------
Artifact flags indicate which source families contributed support to any kept
row.

Known v1 flags:
- HAS_REPLAY_LOG
- HAS_PROMPT_PACK
- HAS_GOLDEN_PACK
- HAS_GOLDEN_PACK_CONVERSATION
- HAS_CONVERSATION_PACK
- HAS_MARKOV_TRACE

Unknown flags are invalid.

Row fields
----------
Each row stores:
- exemplar_id
- response_mode
- structure_kind
- tone_kind
- flags
- support_count
- support_refs

Row flags are structure-only and may indicate features such as:
- summary-first shape
- steps shape
- comparison framing
- clarifier-first shape

Source families
---------------
Allowed support source kinds in v1:
- ReplayLog
- PromptPack
- GoldenPack
- GoldenPackConversation
- ConversationPack
- MarkovTrace

Support refs
------------
Each support ref stores:
- source_kind
- source_hash
- item_ix

`item_ix` is a stable item index inside the source artifact. It is not a score.

Canonical ordering
------------------
Rows must be sorted by:
1. support_count descending
2. response_mode ascending
3. structure_kind ascending
4. tone_kind ascending
5. exemplar_id ascending

Support refs inside each row must be sorted by:
1. source_kind ascending
2. source_hash ascending
3. item_ix ascending

Duplicates are rejected for:
- exemplar_id across rows
- identical support refs within a row

Validation rules
----------------
- version must match the v1 constant
- unknown artifact flags are rejected
- unknown row flags are rejected
- row count must be within the v1 cap
- support ref count per row must be within the v1 cap
- support_count must be non-zero for every row
- support_count must be at least the number of kept support_refs
- artifact flags must cover every source family referenced by support refs

Builder scaffold contract
-------------------------
The current offline builder provides:
- deterministic input inventory planning
- canonical sorting by `(source_kind, source_hash)`
- exact-duplicate removal
- per-source-kind input caps
- global input caps
- conservative mining from selected decoded artifacts
- final row and support-ref caps

Current mining behavior:
- PromptPack: mine one row from the last user message
- ConversationPack: mine one row from the last user message, with follow-up
  conversations biased toward `Continue`
- MarkovTrace: mine one row from bounded opener/clarifier/summary tokens
- ReplayLog, GoldenPack, GoldenPackConversation: accepted as supported inputs
  but currently inventory-only in this step

Current outputs:
- a prepared build plan
- mined canonical exemplar rows from caller-supplied decoded artifacts
- an empty canonical ExemplarMemoryV1 artifact, or
- a finalized ExemplarMemoryV1 from caller-supplied rows

CLI build command
-----------------
The offline builder is now wired to the CLI command:
- `build-exemplar-memory`

Supported input flags:
- `--replay <hash32hex>`
- `--prompt <hash32hex>`
- `--golden-pack <hash32hex>`
- `--golden-pack-conversation <hash32hex>`
- `--conversation-pack <hash32hex>`
- `--markov-trace <hash32hex>`

Builder caps exposed by the CLI:
- `--max-inputs-total <n>`
- `--max-inputs-per-source-kind <n>`
- `--max-rows <n>`
- `--max-support-refs-per-row <n>`

Output:
- one stable summary line beginning with `exemplar_memory_v1`
- stored ExemplarMemoryV1 artifact hash in `exemplar_hash=<hash32hex>`

Precedence and safety
---------------------
Exemplar memory is advisory only.

It must not:
- introduce unsupported claims
- replace lexical retrieval
- replace the evidence bundle
- act as an alternate factual memory system

Current runtime use in this checkpoint is intentionally narrow:
- answer-time lookup is opt-in via `--exemplar-memory <hash32hex>`
- empty exemplar artifacts fall back cleanly
- matched exemplar rows may shape tone, fixed presentation style, and bounded
  plan-item structure (for example Bullet -> Step, Bullet -> Summary, or
  Step -> Bullet when a comparison exemplar is matched)
- operator answer output now includes one bounded `exemplar_match ...` inspect line when a
  row is matched, including exemplar id, selected mode/structure/tone, score,
  support count, and stable match reasons
- runtime exemplar use must not change retrieved evidence rows or claim content

The evidence bundle remains authoritative.
