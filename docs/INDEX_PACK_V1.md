# IndexPack v1 (INDEX_PACK_V1)

IndexPackV1 is a compaction artifact that bundles multiple IndexSegmentV1 blobs
into a single content-addressed artifact.

Why it exists
- IndexSnapshotV1 references index artifacts by hash.
- On a laptop, loading thousands of small IndexSegment artifacts can be IO-bound.
- IndexPack reduces the number of index artifacts to load while preserving exact
 query semantics.

Important: IndexPack does not change scoring or ranking. It stores the canonical
IndexSegmentV1 bytes for each frame segment.

## Wire format

All integers are little-endian.

Header
- magic[8] = b"FSALMIPK"
- version u16 = 1
- reserved u16 = 0
- source_id u64
- n_entries u32

Entries (repeated n_entries)
- frame_seg_hash[32]
- index_bytes_len u32
- index_bytes[index_bytes_len]

## Canonicalization rules

Encoding enforces:
- entries are sorted by frame_seg_hash ascending
- frame_seg_hash values are unique
- each entry's index_bytes MUST decode as IndexSegmentV1
- decoded IndexSegmentV1.seg_hash MUST equal frame_seg_hash
- decoded IndexSegmentV1.source_id MUST equal the pack source_id

Decoding also enforces sorted order and includes bounds checks to avoid
pathological allocations.

## Query-time behavior

IndexSnapshotV1 entries still contain (frame_seg, index_seg) pairs.
After compaction, index_seg may refer to:
- a regular IndexSegmentV1 artifact hash, or
- an IndexPackV1 artifact hash

At query time:
- if index_seg bytes start with the IndexPack magic, decode IndexPackV1
- locate the inner IndexSegmentV1 bytes using frame_seg
- use that inner index bytes as if it had been loaded directly

See docs/INDEX_QUERY_V1.md and docs/COMPACTION_V1.md.
