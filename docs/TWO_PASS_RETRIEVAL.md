Two-pass retrieval policy (v1)
==============================

Purpose
-------
Novel can afford an extra bounded retrieval pass because the default CPU-first
path is designed to be fast. The second pass is not for "more recall" in a
search-engine sense. It is for coverage, balance, and counterchecks that
improve the robustness of answers.

This policy defines:
- Pass 1: precision / anchor proof
- Pass 2: coverage / rounding / counterchecks
- Coverage vector and novelty filtering
- Deterministic caps and tie-breaking

Core principles
---------------
- Evidence-first: retrieval emits structured evidence (frames + lexicon rows), not paragraphs.
- Deterministic: same input -> same plan, same evidence ordering, same answer given same corpus.
- Bounded: explicit caps for segments/postings/evidence per pass.
- Not a search engine: Pass 2 only runs to improve claim coverage and reliability.

Definitions
-----------
- EvidenceSet: structured evidence items (frame row refs, lexicon refs, proof results).
- ClaimSet: candidate claims derived from evidence.
- Coverage vector: a compact summary of gaps/weaknesses in ClaimSet that the system can target.

Pass 1: precision / anchor proof
--------------------------------
Goal:
- Determine a correct core answer supported by strong evidence anchors.

Inputs:
- Required anchors from ExpandedQFV:
 - base required terms
 - ENT identity expansions with weight >= 50000
 - morphology expansions with weight >= 45000

Actions:
- D-gate segments/chunks using required anchors.
- Exact candidate selection with postings intersection on required anchors.
- Score with MRS (integer-only) and apply diversity caps.
- Emit EvidenceSet_1 (top K1) and derive ClaimSet_1.

Stop condition:
- If ClaimSet_1 meets evidence threshold to answer at all, proceed to Pass 2
 (if enabled) or answer immediately (if disabled).

Pass 2: coverage / rounding / counterchecks
-------------------------------------------
Goal:
- Improve answer robustness by filling gaps, balancing sources, and detecting contradictions.

Inputs:
- ClaimSet_1 and EvidenceSet_1
- Optional expansions from ExpandedQFV:
 - synonyms, metaphone neighbors, hyper/hypo, graph hints (future)

Driven by a coverage vector:
- missing slots: who/what/when/where in key claims
- weak slots: low confidence, single-source support, low specificity
- missing comparisons: asked A vs B but only A supported
- missing lexicon grounding: unknown key terms not defined
- contradiction risk: polarity or time disagreement among evidence

Actions:
- Plan targeted retrieval tasks based on the coverage vector:
 - If missing term definitions: pull lexicon sense/relations evidence
 - If single-source: retrieve additional evidence from different doc_id buckets
 - If contradiction risk: search for opposing polarity frames and time variants
 - If missing comparison: retrieve evidence for the other side using anchors

Novelty filtering (critical):
- Evidence in Pass 2 must add value. Discard items that:
 - do not introduce new claim features (entity/verb/time) OR
 - do not increase confidence OR
 - are redundant with existing evidence from the same doc_id/lemma_id

Outputs:
- EvidenceSet_2 (bounded additions only)
- ClaimSet_2 = merge(ClaimSet_1, derive(EvidenceSet_2))
- Answer uses ClaimSet_2 with updated uncertainty.

Deterministic caps (recommended defaults)
-----------------------------------------
Per pass:
- segments_touched_pass1 <= S1
- segments_touched_pass2 <= S2 (S2 can be slightly higher than S1)
- postings_reads_pass1 <= R1
- postings_reads_pass2 <= R2
- evidence_items_pass1 <= K1
- evidence_items_pass2_new <= K2_new
- max_per_doc_id <= Dcap
- max_per_lemma_id <= Lcap

Implementation note (current)
-----------------------------
The current codebase implements diversity caps as a post-search refine stage in
`src/retrieval_policy.rs`.

Signature gating equivalence:
- The gated search path (SegmentSigV1 + IndexSigMapV1) must be behaviorally
 equivalent to the ungated path for the same snapshot.
- Unit tests assert gated vs ungated equivalence even when the refine stage
 enables diversity caps and novelty re-ranking.

Supported caps:
- per FrameSegment: `RetrievalPolicyCfgV1.max_hits_per_frame_seg`
- per DocId: `RetrievalPolicyCfgV1.max_hits_per_doc`

Supported novelty scoring:
- `RetrievalPolicyCfgV1.novelty_mode` provides inverse-frequency novelty signals
 over DocId and/or FrameSegment (secondary key after score).

Lemma-based caps and lemma novelty are deferred.

Example defaults (tunable):
- S1=8, S2=12
- R1=32, R2=48
- K1=32, K2_new=24
- Dcap=3, Lcap=3

Tie-breaking:
- stable sort by (score desc, evidence_type, stable_id asc)
- stable planner ordering for targeted tasks

Coprocessor integration
-----------------------
Coprocessors may participate only as advisory modules:
- Graph relevance: propose candidates to fill coverage gaps.
- kNN patch memory: suggest common missing pieces for similar question traces.
- Markov/PPM: assist phrasing and structure only (not factual claims).

Coprocessors must not override evidence thresholds.

Configuration knobs (v1)
------------------------
- two_pass_mode: off | on_strict | on_default
 - off: Pass 1 only
 - on_strict: Pass 2 runs only if coverage vector indicates gaps/risks
 - on_default: Pass 2 runs even if strong, but bounded by novelty filter
- pass2_objectives: bitflags
 - definitions, diversity, contradictions, comparisons

Why this improves robustness
----------------------------
- Coverage-driven retrieval makes answers more complete without broadening into uncontrolled recall.
- Diversity caps reduce the risk of overfitting to one source.
- Contradiction checks reduce hallucination and improve calibrated uncertainty.
