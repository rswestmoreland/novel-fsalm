EvidenceBundleV1
================

Overview
--------
EvidenceBundleV1 is the canonical retrieval output artifact.
It is designed to support evidence-first synthesis with deterministic replay.

Goals:
- Deterministic bytes and hashes (canonical ordering enforced)
- Integer-only / fixed-width fields where possible
- Optional row sketches for gating and overlap scoring
- Minimal dependencies (no serde)

Binary format (v1)
------------------
All integers are little-endian.

Header:
- u16 version (must be 1)
- Hash32 query_id
- Hash32 snapshot_id
- limits:
 - u32 segments_touched
 - u32 max_items
 - u32 max_bytes
- u32 score_model_id
- u32 items_len

Items:
Each item is:
- i64 score
- u8 kind
- kind payload

Kinds (u8)
----------
- 0: FrameRowRef
- 1: LexiconRowRef (reserved)
- 2: ProofRef (reserved)

Canonical ordering
------------------
Items MUST be sorted by:
1) score descending
2) kind ascending
3) stable_id ascending

stable_id is defined per kind:
- FrameRowRef: (segment_id bytes, row_ix)
- LexiconRowRef: (segment_id bytes, row_ix)
- ProofRef: (proof_id bytes)

FrameRowRef payload (kind=0)
----------------------------
- Hash32 segment_id
- u32 row_ix
- u8 has_sketch (0 or 1)
- if has_sketch == 1: FrameRowSketchV1

LexiconRowRef payload (kind=1, reserved)
---------------------------------------
- Hash32 segment_id
- u32 row_ix

ProofRef payload (kind=2, reserved)
----------------------------------
- Hash32 proof_id

FrameRowSketchV1 payload
------------------------
Sketches are optional and intended for memory gating and overlap scoring.

- u32 entity_ids_len
- u32 entity_id repeated
- u32 meta_codes_len
- u32 meta_code repeated
- u32 terms_len
- (u32 term_id, u32 tf) repeated

FrameRowSketchV1 canonical rules
--------------------------------
- entity_ids: sorted ascending, unique
- meta_codes: sorted ascending, unique
- terms: sorted by (tf descending, term_id ascending)
- tf must be > 0
- term_id must be unique within the sketch

Notes
-----
- max_items and max_bytes are recorded with the bundle. Builders should enforce
 these limits; decoders validate self-consistency.
