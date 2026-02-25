FrameStore
====================

Goal
----
Persist `FrameSegmentV1` (columnar frame segments) to disk using the existing
content-addressed artifact store.

This provides the "cold storage" tier:
- segments are immutable and addressed by hash
- segments can be streamed from disk later (future stages)
- indexes can be rebuilt deterministically from stored segments

What is stored
--------------
- `FrameSegmentV1` encoded bytes (see `src/frame_segment.rs`).
- The artifact address is the BLAKE3 hash of those bytes (Hash32).

API
---
Library helpers (generic over `ArtifactStore`):
- `put_frame_segment_v1(store, &seg) -> Hash32`
- `get_frame_segment_v1(store, &hash) -> Option<FrameSegmentV1>`

Notes
-----
- Segment encoding is canonical. If the same rows and chunking parameters are
 used, the same bytes and hash are produced.
- This module does not introduce a new on-disk layout beyond the artifact store.
 The artifact store already provides sharded paths and best-effort atomic writes.

Future work
-----------
- Warm index: segment metadata and per-segment term dictionaries.
- Hot index: small in-memory caches (recent segments, frequently used ids).
- Streaming decoders to avoid allocating full segment bytes on read.
- Network fetch: retrieve segments by hash from remote nodes.
