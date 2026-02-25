HitListV1
=========

Overview
--------
HitListV1 is the canonical retrieval "hit list" artifact.
It represents the ranked row addresses returned by query-time index lookup.

Goals:
- Deterministic bytes and hashes (canonical ordering enforced)
- Integer-only / fixed-width fields
- Replay-friendly (content-addressed artifact)
- Defensive decode (bounded allocations)

Binary format (v1)
------------------
All integers are little-endian.

Header:
- u16 version (must be 1)
- Hash32 query_id
- Hash32 snapshot_id
- u8 tie_flag (0 or 1)
- if tie_flag == 1: Hash32 tie_control_id
- u32 hits_len

Hit entries:
Each hit is:
- Hash32 frame_seg
- u32 row_ix
- u64 score

Canonical ordering
------------------
Hits MUST be sorted by:
1) score descending
2) tie-break key (ascending) when tie_flag == 1
3) frame_seg bytes ascending
4) row_ix ascending

When tie_flag == 0, rule (2) is omitted.

Tie-break key (v1)
------------------
When tie_flag == 1, the tie-break key matches the retrieval tie-break used by
`index_query` control integration:

- seed64 = first 8 bytes of tie_control_id interpreted as u64 LE
- seg0 = first 8 bytes of frame_seg interpreted as u64 LE
- x = seed64 XOR seg0 XOR (row_ix as u64 * 0x9E3779B97F4A7C15)
- key = splitmix64_finalizer(x + 0x9E3779B97F4A7C15)

The splitmix64 finalizer is:
- z = (z XOR (z >> 30)) * 0xBF58476D1CE4E5B9
- z = (z XOR (z >> 27)) * 0x94D049BB133111EB
- z = z XOR (z >> 31)

Uniqueness
----------
The pair (frame_seg, row_ix) MUST be unique within the hit list.

Bounds
------
Decoders enforce a hard cap on hits_len to prevent runaway allocations.
Current cap: 200,000 hits.
