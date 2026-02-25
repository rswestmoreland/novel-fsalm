# Index Query v1 (INDEX_QUERY_V1)

This document describes the query-time lookup and scoring logic used by Novel FSA-LM.

Scope (v1)
- Input: IndexSnapshotV1 hash + query text
- Output: ranked row addresses (frame segment hash + row index)
- Determinism: bitwise (no randomized maps, no floating point)

## Inputs

Query text is converted to canonical query terms:

1) Token terms
- TokenizerCfg controls max token bytes (truncation is applied before hashing)
- Each token maps to a TermId (domain separated by the tokenizer)

2) Optional metaphone terms
- If enabled, MetaphoneCfg produces metaphone codes from tokens
- Each code maps to a MetaCodeId (domain separated by metaphone)
- Metaphone ids are cast to TermId and appended as additional query terms

Canonicalization:
- all query terms are sorted by term id ascending
- duplicates are merged (qtf sums, saturating)
- the list is truncated to max_terms

## Lookup

Given IndexSnapshotV1, we scan its entries in order (entries are canonicalized
by (frame_seg, index_seg) ascending):

For each entry:
- load index artifact by hash
 - if it is an IndexSegmentV1 blob, use it directly
 - if it is an IndexPackV1 blob, locate the inner IndexSegmentV1 for this entry's frame_seg
- for each query term:
 - binary search term in the term dictionary
 - if found:
 - compute IDF-like ratio based on segment row_count and term df
 - iterate postings (row_ix, tf) pairs
 - add contribution to row score

## Scoring (integer-only)

v1 uses a very simple deterministic score:

idf_scaled = ((N + 1) << IDF_SHIFT) / (df + 1)

contribution = qtf * tf * idf_scaled

score(row) = sum(contribution)

Notes:
- IDF_SHIFT defaults to 8 in code for safety.
- This is not BM25. It is a cheap IDF ratio that works without floats.
- Future stages can add doc length normalization and field-aware boosts.

## Ranking

Hits are sorted by:
1) score descending
2) frame_seg ascending
3) row_ix ascending

This produces a stable total ordering.

## Performance notes

Dense scoring path:
- if row_count <= dense_row_threshold (default 200k)
- allocate Vec<u64> of length row_count
- track touched rows to avoid scanning the full array

Sparse scoring path:
- deterministic sorted Vec<(row_ix, score)>
- O(m log m) inserts, where m is number of matched postings

The dense path is faster when row_count is moderate.

