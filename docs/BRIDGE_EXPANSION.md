Bridge expansion policy (v1)
============================

Purpose
-------
Bridge expansion is how Novel combines adjacent and related information even when
the surface words differ. It expands a Query Feature Vector (QFV) using deterministic
rules and strict budgets.

This is not embeddings-based semantic search. Expansion is a bounded, rule-driven
mechanism that produces additional feature ids that help retrieval and reasoning.

Design goals
------------
- Deterministic: same input -> same expansions and ordering.
- Integer-only: weights and scoring use u16/u32/u64 (no floats).
- Bounded: strict per-channel and global caps.
- Evidence-first: expansions influence evidence selection, not raw passage retrieval.
- Not a search engine: expansions are advisory and constrained; synthesis remains claim-based.

Implementation status
--------------------------------
- DONE: ExpansionBudgetV1 schema + codec + docs.
- DONE: ExpandedQfvV1 candidate schema + codec + tests.
- DONE: Expansion builder (rank + dedup + budget fill) + tests.
- DONE: RetrievalPolicy integrated via src/bridge_expansion.rs and apply_retrieval_policy_from_text_v1.

Inputs and outputs
------------------
Input:
- PromptPack and hot context (recent terms/entities)
- Tokenizer output (TermIds)
- Metaphone output (MetaCodeIds)
- Lexicon mappings and relations (Wiktionary-derived)
- Optional entity alias table (later)

Output:
- ExpandedQFV:
 - required anchors (strict)
 - optional expansions (for gating/scoring)
 - per-item weights and origin traces (optional debug)

Data model
----------
Expansion item:
- kind: LEX | META | ENT | GRAPH
- id: TermId | MetaCodeId | EntityId
- weight: u16 (0..65535)
- origin: (base_id, rule_id)
- rank_key: (weight desc, kind_order, id asc)

Dedup rule:
- Keyed by (kind, id)
- Keep the max weight (deterministic)

Expansion channels
------------------
LEX (lexical):
- morphology: lemma variants (plural/singular, tense)
- synonym edges (sense-aware if available)
- hypernym/hyponym edges (coarser adjacency)

META (metaphonetic):
- metaphone codes for tokens
- neighbor codes (very bounded)

ENT (identity):
- alias/abbreviation/canonical identity edges only
- No "related entity" expansion in v1

GRAPH (adjacency from GraphRelevanceV1, bounded offline artifact):
- 1-hop and 2-hop neighbors on entity/verb graph
- Strictly advisory and bounded

Weights (integer-only)
----------------------
Weights represent confidence that an expansion is equivalent or strongly adjacent.

Base weights (suggested):
- ENT identity alias: 60000
- LEX morphology variant: 48000
- LEX synonym (same sense): 36000
- LEX synonym (unknown sense): 24000
- LEX hyper/hyponym: 16000
- META metaphone exact code: 28000
- META neighbor code: 16000
- GRAPH 1-hop neighbor: 12000
- GRAPH 2-hop neighbor: 6000

Adjustments:
- If base feature is required: +8000 (cap)
- If feature is common (low idf bucket): subtract common_penalty
- If sense is low confidence: subtract sense_penalty
- If token looks like proper noun: META +4000

All weights are clamped to [0..65535].

Budgets and caps (v1)
---------------------
The canonical configuration schema for these caps is in:
- docs/EXPANSION_BUDGET_V1.md (code: src/expansion_budget.rs)

The canonical expanded feature vector schema is in:
- docs/EXPANDED_QFV_V1.md (code: src/expanded_qfv.rs)

Global caps:
- MAX_EXPANSIONS_TOTAL = 64
- MAX_EXPANSIONS_PER_BASE = 8
- MAX_REQUIRED_EXPANSIONS_TOTAL = 24

Per-channel caps:
- MAX_ENT = 8
- MAX_LEX = 24
- MAX_META = 16
- MAX_GRAPH = 8

Per-base caps:
- morphology: <= 2
- synonyms: <= 3
- hyper/hypo: <= 2
- metaphone: <= 2 codes + 2 neighbors

Deterministic selection algorithm
---------------------------------
Step A: Generate candidates
- Apply rules in a fixed order:
 1) ENT identity (if entity)
 2) LEX morphology
 3) LEX synonyms (sense-aware if available)
 4) LEX hyper/hyponym
 5) META codes and neighbors
 6) GRAPH neighbors (future)

Step B: Partition by required vs optional origin
- If origin base is required -> required pool
- Else -> optional pool

Step C: Stable rank
- Stable sort each pool by (weight desc, kind_order, id asc)

Step D: Fill budgets
1) Take up to MAX_REQUIRED_EXPANSIONS_TOTAL from required pool,
 enforcing per-channel and per-base caps.
2) Fill remaining up to MAX_EXPANSIONS_TOTAL from optional pool,
 enforcing caps the same way.

Planner integration
-------------------
Expansions do not automatically become "required terms".

Recommended v1 behavior:
- Required anchors:
 - base required terms
 - ENT expansions with weight >= 50000
 - morphology expansions with weight >= 45000
- Optional expansions:
 - everything else (synonyms, metaphone neighbors, graph hints)

Two-pass retrieval pattern (bounded)
------------------------------------
Pass 1 (precision):
- Intersect postings using required anchors only.
- Score with MRS and apply diversity caps.
- If evidence meets threshold, stop.

Pass 2 (recall, still bounded):
- Use optional expansions for D-gating and MRS boosts.
- Keep a small core anchor set for exact intersection.
- Maintain strict evidence caps and thresholds.

Why this is not a search engine
-------------------------------
- Expansion yields ids and small weights, not passages.
- Retrieval emits structured EvidenceSet (frames + lexicon rows) by default.
- Synthesis is claim-based and paraphrased, with uncertainty on weak support.
