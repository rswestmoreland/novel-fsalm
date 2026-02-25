Lexicon query expansion
=================================

Purpose
-------
 adds a bounded, deterministic query expansion stage that can be used
when building query terms.

Goal:
- Improve recall by adding a small number of additional TermId values.
- Keep behavior stable and reproducible (no randomization, no floating point).
- Stay bounded: explicit caps on depth and term counts.

This stage is "lexicon-backed" in the sense that candidate surface forms are
only emitted if the lexicon confirms the lemma_key_id exists.

What expansion does in v1
-------------------------
The v1 implementation is intentionally conservative:

- Original terms:
 - Derived from query text tokens using the tokenizer TermId function.
 - Depth 0, reason Original.

- Variant terms (depth 1):
 - Derived from deterministic morphology-like rules (ASCII-only transforms).
 - Emitted only if the candidate surface form is present in the lexicon
 (membership filter).
 - Reason Variant.

Not implemented in v1:
- Relation-derived expansions (synonyms, related terms).
 These are deferred until we have a stable surface-form mapping for target
 lemmas (a text table).

Key types
---------
Module: src/query_expansion.rs

- QueryExpansionCfgV1
 - max_depth: u8
 - v1 only emits depth 1 variants when max_depth >= 1.
 - max_new_terms: u16
 - cap on additional terms beyond the original set.
 - max_total_terms: u16
 - cap on total terms (original + new).
 - allow_relations_mask: u32
 - reserved for future relation expansions; unused in v1.

- ExpandedTermV1
 - term_id: TermId
 - depth: u8
 - reason_code: u16 (QueryExpansionReasonV1)

Determinism rules
-----------------
The expansion stage must preserve determinism:

- Tokenization order is irrelevant to output ordering.
- Duplicates are removed using stable sorting.
- Final output is canonicalized (sorted by TermId; stable unique).
- All caps are enforced deterministically.

Membership filter
-----------------
Expansion candidates are only emitted if the lexicon lookup view reports at
least one lemma id for the candidate lemma key.

- Lemma key id derivation is ASCII-lowercase for stable matching.
- Lookup is performed via LexiconExpandLookupV1.

Wiring and usage
----------------
Track is wired as a library feature through the retrieval policy wrapper:

- RetrievalPolicyCfgV1 has a flag:
 - enable_query_expansion: u8 (0 or 1)

- apply_retrieval_policy_from_text_v1(...) in src/retrieval_policy.rs:
 - Derives base QueryTerm values from query_text.
 - If enable_query_expansion == 1:
 - Requires lexicon_snapshot_hash_opt (LexiconSnapshotV1 hash).
 - Loads LexiconExpandLookupV1 from the lexicon snapshot.
 - Runs expand_query_terms_v1(...) to obtain candidates.
 - Adds new unique TermId values as QueryTerm { qtf: 1 }.
 - Canonicalizes query terms.
 - Applies apply_retrieval_policy_v1(...) and returns hits + stats.

Important:
- The answer CLI currently uses apply_retrieval_policy_v1 on pre-derived terms.
 It does not expose a query expansion toggle yet.

Stats
-----
RetrievalPolicyStatsV1 includes counters used by the expansion wrapper:

- query_terms_original: number of unique base terms
- query_terms_expanded_new: number of new unique terms added by expansion

Tests
-----
- Unit tests for expansion rules live in src/query_expansion.rs.
- Retrieval policy tests cover expansion integration in src/retrieval_policy.rs.

Future work
-----------
Likely next steps (discussion first):
- Relation-derived expansions using lexicon relations once a stable
 surface-form mapping for target lemmas exists.
- Optional weighting for expanded terms (qtf) while preserving determinism.
- Expose an expansion toggle and lexicon snapshot hash in the answer CLI.
