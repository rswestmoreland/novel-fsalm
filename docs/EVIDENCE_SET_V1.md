EvidenceSetV1
=============

Overview
--------
EvidenceSetV1 is a compact, canonical artifact schema that maps claims (or output
spans) to evidence row references.

The intent is to make answers auditable and to enable future evaluation and
training stages by recording which evidence rows support which claims.

 introduces the schema and codec only. Wiring into the answer pipeline
and ReplayLog happens in future updates.

Binary format (v1)
------------------
All integers are little-endian.

Header:
- u16 version (must be 1)
- Hash32 evidence_bundle_id
- u32 items_len

Items:
Each item is:
- u32 claim_id
- str claim_text (u32 byte length + UTF-8 bytes)
- u32 evidence_refs_len
- evidence_refs repeated

EvidenceRowRefV1:
- Hash32 segment_id
- u32 row_ix
- i64 score

Canonical ordering
------------------
Canonical ordering is required for content-addressed stability.

- items MUST be sorted by claim_id ascending
- claim_id values MUST be unique

For each item:
- evidence_refs MUST be sorted by (segment_id bytes, row_ix) ascending
- (segment_id, row_ix) pairs MUST be unique

Notes
-----
- score is carried forward from retrieval for convenience. In v1 it is always
 present to avoid per-entry tag bytes. Ordering ignores score.
- claim_text is stored as UTF-8 bytes. No additional normalization is applied
 in the codec.

Verifiers
---------------------
The EvidenceSet codec enforces canonical ordering, but it does not verify
artifact existence or referential integrity.

 adds small, deterministic verifiers in `src/evidence_set_verify.rs`:
- items must be non-empty
- each item must have at least one evidence ref
- the referenced EvidenceBundle artifact must exist
- each referenced (segment_id,row_ix) must be in range for that FrameSegment

These verifiers are used by the answer CLI when `--verify-trace 1` is set.

Deterministic verifier error codes
---------------------------------
Verifier failures include a stable short code for regression harnesses:
- V000: evidence_set.items is empty
- V001: claim item has empty evidence_refs
- V010: evidence bundle not found
- V011: evidence bundle load/decode failed
- V020: frame segment not found
- V021: frame segment load/decode failed
- V022: referenced row_ix is out of range
