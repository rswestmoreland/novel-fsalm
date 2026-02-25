Reasoning flow model (text)
==========================

Overview
--------
Novel separates memory selection from synthesis.

- Retrieval selects small structured evidence (frames + lexicon rows), not paragraphs.
- Pragmatics (tone/tact) produces control signals that shape how the answer is rendered.
- Synthesis turns evidence into claims and renders a conversational answer.
- Deterministic reflexes (math/logic) verify or compute when needed.

Flow diagram
------------

+--------------------------------------------------------------+
| INPUT |
| PromptPack (conversation turns + constraints + limits) |
+-------------------------------+------------------------------+
 |
 v
+--------------------------------------------------------------+
| 1) NORMALIZE + FEATURE EXTRACT |
| - Tokenize -> TermIds (deterministic) |
| - Metaphone -> MetaCodeIds (deterministic) |
| - Detect numbers -> BigInt / Rational tokens |
| - Intent classify (definition/factoid/compare/math/...) |
| - Build Query Feature Vector (QFV): |
| Q_terms, Q_meta, Q_intent, Q_required, Q_optional |
+-------------------------------+------------------------------+
 |
 v
+--------------------------------------------------------------+
| 2) BRIDGE EXPANSION (bounded, deterministic) |
| - Lexicon expansion: morphology, synonyms, sense variants |
| - Entity identity expansion: aliases/canonical ids |
| - Metaphonetic expansion: phonetic codes + neighbors |
| - Graph hints (future): related entities/verbs |
| Output: ExpandedQFV with strict budgets and integer weights |
| See: docs/BRIDGE_EXPANSION.md |
+-------------------------------+------------------------------+
 |
 v
+--------------------------------------------------------------+
| 3) MEMORY PLANNING (deterministic planner) |
| - Decide evidence types needed (Frames vs Lexicon vs Proof) |
| - Choose required anchors for exact match |
| - Choose optional expansions for recall |
| - Set budgets: segments_touched, postings_reads, topK |
+-------------------------------+------------------------------+
 |
 v
+--------------------------------------------------------------+
| 4) MEMORY GATING (D) |
| - Bloom reject on TermId/MetaCodeId |
| - Sketch overlap score |
| - Keep top S segments/chunks (stable sort + ties) |
+-------------------------------+------------------------------+
 |
 v
+--------------------------------------------------------------+
| 5) EXACT CANDIDATE SELECTION (C, later B) |
| - Fetch postings for required anchors |
| - Intersect by smallest df first (stable order) |
| - Score candidates with MRS (integer-only) |
| - Apply diversity caps |
| Output: SearchHits (row refs + scores); EvidenceBuilder -> EvidenceBundleV1 |
+-------------------------------+------------------------------+
 |
 v
+--------------------------------------------------------------+
| 6) CLAIM SYNTHESIS (reasoning over evidence) |
| - Normalize evidence into propositions (claim candidates) |
| - Merge duplicates (hash-based) |
| - Detect gaps and contradictions |
| - Assign claim confidence (fixed-point) |
| Output: ClaimSet + AnswerTrace (optional) |
+-------------------------------+------------------------------+
 |
 v
+--------------------------------------------------------------+
| 7) VERIFIERS / REFLEXES |
| - Math: BigInt/rational evaluator |
| - Logic: small proof/consistency rules |
| - Update or reject claims deterministically |
+-------------------------------+------------------------------+
 |
 v
+--------------------------------------------------------------+
| 8) ANSWER RENDERING (conversational synthesis) |
| - Select top claims supporting the intent |
| - Paraphrase into natural language (no copying) |
| - Include uncertainty / ask clarifying question if weak |
| Output: AnswerDraft (final text) |
+--------------------------------------------------------------+
 |
 v
+--------------------------------------------------------------+
| 9) REPLAY + MEMORY UPDATE (deterministic) |
| - Store AnswerTrace + evidence ids (no big text) |
| - Update hot context sets (recent terms/entities) |
| - Optional: update coprocessor stats (future) |
+--------------------------------------------------------------+

Notes
-----
- The key to "different words, same answer" is Step 2 (Bridge Expansion) plus
 iterative retrieval passes with strict caps.
- Novel remains "thinking-oriented" because evidence is structured and synthesis
 is claim-based, with verifiers for correctness.

Implementation status
-------------------------------
 implements a minimal answer loop suitable for developer smoke tests:

- Query term extraction: src/tokenizer.rs (QueryTermsCfg and query_terms_from_text)
- Retrieval policy wrapper: src/retrieval_policy.rs (apply_retrieval_policy_v1)
- Evidence bundle construction: src/evidence_builder.rs (build_evidence_bundle_v1_from_hits)
- Planning: src/answer_plan.rs and src/planner_v1.rs
- Rendering: src/realizer_v1.rs
- CLI entrypoint: fsa_lm answer (src/bin/fsa_lm.rs)

This loop is intentionally conservative:
- Bridge expansion (Step 2) is not yet wired into the answer command beyond the optional --meta path.
- Claim synthesis/verifiers (Steps 6-7) are placeholders for future stages.
- Answer rendering (Step 8) is developer-oriented and includes an evidence appendix.

See: docs/ANSWERING_LOOP.md
