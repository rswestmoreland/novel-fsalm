Evidence Builder v1
===================

Overview
--------
Evidence building converts retrieval output (row references + scores) into a
canonical EvidenceBundleV1 artifact.

In v1, evidence building is intentionally simple and deterministic:
- It does not create new evidence kinds.
- It does not perform secondary retrieval.
- It optionally attaches small row sketches for downstream reasoning.

API surface
-----------
Code: src/evidence_builder.rs

Primary entrypoint:
- build_evidence_bundle_v1_from_hits(store, query_id, snapshot_id, limits,
 score_model_id, hits, cfg) -> EvidenceBundleV1

Inputs
------
- hits: a ranked list of SearchHit { frame_seg, row_ix, score }.
- limits: EvidenceLimitsV1 { segments_touched, max_items, max_bytes }.
- cfg:
 - verify_refs: if true, ensure referenced frame segments exist and row_ix is in range.
 - sketch.enable: if true, attach FrameRowSketchV1 when budget allows.
 - sketch.max_terms / sketch.max_entities: caps for sketch payload.

Two-pass process
----------------
Pass 1: normalize, dedup, rank, cap
- Convert scores to i64 (clamped).
- Deduplicate by (segment_id, row_ix), keeping the highest score.
- Canonicalize item order using EvidenceBundleV1 rules.
- Apply max_items (truncate after ranking).

Pass 2: optional sketches under a strict byte budget
- Optionally attach FrameRowSketchV1 to frame items.
- Sketches are attached in canonical item order (highest score first).
- Each sketch is only attached if it would keep the estimated encoded bundle size
 within limits.max_bytes.

Sketch payload
--------------
FrameRowSketchV1 contains three compact fields:
- terms: Vec<TermTfV1> sorted by (tf desc, term_id asc)
- entity_ids: sorted unique u32 ids
- meta_codes: reserved; empty in v1

Compact id derivation
---------------------
Row sketches use u32 ids for compactness.

Term ids and entity ids are stable u64 values in the frame schema. For sketches,
we derive a compact u32 signature by XOR-folding the high and low 32-bit halves:

 sig_u32 = (id_u64 as u32) ^ ((id_u64 >> 32) as u32)

This is deterministic and mixes all 64 bits, but it is not collision-free.
Sketch ids are hints for downstream reasoning, not authoritative identifiers.

Determinism notes
-----------------
- No hash maps are used for deduplication; sorting drives canonical selection.
- Budgets are applied in canonical order.
- If max_bytes is zero, sketches are attached for all items (still capped by
 max_terms/max_entities).
