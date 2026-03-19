Reasoning and conversation maturity
==================================

Purpose
-------
This note records how the current repo wires together retrieval, planning, and
conversation continuity. It also distinguishes live behavior from the smaller
set of remaining maturity gaps.

Novel is evidence-first and deterministic. "Reasoning" is treated as a product
of:
- better query understanding and expansion (lexicon-first when available)
- stable evidence selection
- structured planning (AnswerPlanV1)
- bounded verification and surface-form guidance (quality gate)

This document is descriptive, not a checklist. The authoritative checklist
remains docs/MASTER_PLAN.md.


Entry points (end user)
----------------------
The primary end-user commands are implemented in src/bin/fsa_lm.rs:
- ask: single prompt -> full reply; optional conversation continuation
- chat: interactive loop; optional resume + session file persistence
- load-wikipedia / load-wiktionary: populate workspace defaults
- show-workspace: display workspace defaults

The ask and chat commands run the same core answering path by constructing a
PromptPack and then invoking the internal answer runner.


Pragmatics (tone, tact, intent signals)
--------------------------------------
Pragmatics is implemented as a rules-first extractor:
- schema: src/pragmatics_frame.rs (PragmaticsFrameV1)
- extraction: src/pragmatics_extract.rs
- storage helpers: src/pragmatics_frame_store.rs

CLI wiring:
- build-pragmatics (src/bin/fsa_lm.rs) loads a PromptPack and emits one or more
  PragmaticsFrameV1 artifacts.
- answer/ask/chat accept one or more --pragmatics <hash> values. The last id is
  used as the active PragmaticsFrameV1 for planning and realization.

Downstream use:
- src/quality_gate_v1.rs derives RealizerDirectivesV1 from PragmaticsFrameV1 via
  derive_directives_opt(...).


Planner guidance (PlannerHintsV1 and ForecastV1)
-----------------------------------------------
Planner guidance is produced and stored as replayable artifacts.

Implementation:
- src/planner_v1.rs
  - plan_from_evidence_bundle_v1_with_guidance(...) returns:
    - AnswerPlanV1
    - PlannerHintsV1
    - ForecastV1
- artifacts:
  - src/planner_hints_artifact.rs
  - src/forecast_artifact.rs

CLI wiring:
- the internal answer runner stores PlannerHintsV1 and ForecastV1 for each
  answer. These hashes are recorded in ReplayLog steps.

Behavioral effect:
- PlannerHintsV1 influences AnswerPlan item kinds (Bullet vs Step) and whether
  the plan is summary-first or clarify-first.
- src/quality_gate_v1.rs can append a bounded clarifying question using
  PlannerHintsV1 + ForecastV1.

Current state:
- docs/PLANNER_HINTS_V1.md now matches the implemented answer path: the current
  CLI flow stores and uses PlannerHintsV1 and ForecastV1.


Bridge expansion and lexicon-powered query expansion
---------------------------------------------------
Query expansion is implemented as a bounded, deterministic transformation of a
query feature vector.

Key modules:
- src/retrieval_policy.rs: apply_retrieval_policy_from_text_v1(...)
- src/expanded_qfv.rs: ExpandedQfvV1
- src/bridge_expansion.rs, src/expansion_builder.rs, src/expansion_budget.rs
- src/lexicon_expand_lookup.rs

CLI wiring:
- answer/ask/chat accept --expand.
- when --expand is enabled, a LexiconSnapshot id is required (via
  --lexicon-snapshot or workspace defaults).
- workspace_v1.txt can also enable expansion through default_expand=1 or a
  configured graph_relevance artifact.

Notes:
- Expansion today is primarily about recall and "same meaning, different
  surface form" alignment.
- The lexicon is not yet used as a first-class signal for higher-level intent
  shaping. Conversation continuity for retrieval is handled by
  ContextAnchorsV1, not by lexicon neighborhoods alone.


Markov integration (surface-form guidance only)
----------------------------------------------
Markov is used only to select among fixed surface templates. It does not create
new claims.

Key modules:
- src/markov_model.rs, src/markov_runtime.rs
- src/markov_hints.rs, src/markov_trace.rs
- src/quality_gate_v1.rs

Wiring:
- MarkovHintsV1 is derived when a Markov model is active, whether it arrives
  from an explicit CLI flag, a workspace default, or a resumed conversation
  pack sticky id. The derivation path is
  derive_markov_hints_opener_preface_opt(...).
- MarkovTraceV1 is emitted for each answer by quality gate helpers and stored as
  an artifact. The trace is referenced from ReplayLog.

Chat history context:
- ask and chat rebuild a bounded Markov context tail from prior assistant turns
  when continuing a conversation.
- The rebuild uses replay_id values stored in ConversationPackV1 and loads the
  prior MarkovTraceV1 artifacts referenced from ReplayLog.

Current state:
- docs/MARKOV_CHAT_CONTEXT_V1.md now matches the implemented ask/chat flow: the
  current CLI path reconstructs Markov context across runs when replay ids are
  present.


ConversationPackV1 (resumable chat history)
------------------------------------------
ConversationPackV1 is the persistent conversation state artifact.

Key modules:
- schema: src/conversation_pack.rs
- artifact helpers: src/conversation_pack_artifact.rs

Wiring:
- ask
  - --session-file stores a ConversationPack id in a small key=value file
  - --conversation resumes directly from a ConversationPack hash
- chat
  - --resume resumes directly from a ConversationPack hash
  - --session-file with --autosave updates the session file after each turn
  - /save writes a new ConversationPack and prints conversation_pack=<hash>

ConversationPackV1 can store a replay_id for assistant turns. This enables:
- Markov context reconstruction across runs
- deterministic audit of how a reply was produced (ReplayLog linkage)

ConversationPackV1 can also store sticky runtime choices for:
- markov_model_id
- exemplar_memory_id
- graph_relevance_id
- presentation_mode

ask and chat restore those saved values on resume unless the caller provides a
newer explicit CLI override.


EvidenceBundleV1 live kinds and remaining growth area
--------------------------------------------------
EvidenceBundleV1 is the canonical output of retrieval (src/evidence_bundle.rs).
The current answer path can now include:

- EvidenceItemDataV1::Frame(FrameRowRefV1)
  - the normal grounded retrieval result from index snapshots

- EvidenceItemDataV1::Proof(ProofRefV1)
  - used when the deterministic puzzle and logic flow produces a Proof artifact
  - attached as evidence-first output rather than free-form reasoning text

One growth area still remains:

- EvidenceItemDataV1::Lexicon(LexiconRowRefV1)
  - reserved for retrieval paths that return lexicon rows directly as evidence
  - the current default answering flow still uses frame rows as its primary
    evidence surface

These evidence kinds remain references to canonical artifacts, not free-form
reasoning text.


Remaining maturity gap worth exploiting next
-------------------------------------------
The repo already contains most of the right extension points. The main gap that
still materially limits "conversation with reasoning" today is:

- Lexicon-first intent shaping
  - PragmaticsFrameV1 has intent flags and cue-count machinery, but cue selection
    is not yet driven by lexicon neighborhoods derived from Wiktionary.

