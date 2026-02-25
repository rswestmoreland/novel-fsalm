IndexSigMap v1
===============

Goal
----
IndexSigMap is a small sidecar manifest that maps an index artifact hash
(IndexSegmentV1 or IndexPackV1, as referenced by IndexSnapshotV1 entries) to
the SegmentSig artifact hash that summarizes that index artifact.

This keeps IndexSnapshotV1 stable while enabling query-time gating:
- Build-time: produce SegmentSig artifacts and a map for the snapshot.
- Query-time: given an index artifact hash, load its SegmentSig hash and consult the
 signature before decoding the (larger) index bytes.

Artifact identity
-----------------
IndexSigMap is stored as a content-addressed artifact (Hash32 of canonical bytes).

Canonical byte layout
---------------------
All integers are little-endian.

- MAGIC[8] = "FSALMISM"
- version(u16) = 1
- reserved(u16) = 0
- source_id(u64)
- entry_count(u32)
- entries[entry_count]:
 - index_seg_hash[32]
 - sig_hash[32]

Encoding rules
--------------
- Entries are sorted by index_seg_hash ascending (bytewise).
- Each index_seg_hash must appear at most once.
- reserved is always 0.

Decode rules
------------
- Reject wrong MAGIC or unsupported version.
- Reject trailing bytes.
- Enforce sorted order and uniqueness of index_seg_hash.
- Defensive caps prevent pathological allocations on corrupt inputs.

Implementation
--------------
- Schema + codec: src/index_sig_map.rs
- Store helpers (put/get/cached): src/index_sig_map_store.rs
