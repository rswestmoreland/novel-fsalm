# IndexSnapshot v1 (manifest)

IndexSnapshot is a small manifest artifact that links FrameSegment hashes to their
corresponding IndexSegment hashes.

This is used by Novel's retrieval layer to quickly discover which segments are
indexed, without scanning or decoding the full set of artifacts.

It is not an externally visible "search index". It is an internal acceleration
manifest.

## Canonical byte layout

All integers are little-endian.

- version(u16) = 1
- source_id(u64)
- entry_count(u32)
- entries (entry_count entries), sorted by (frame_seg, index_seg) ascending:
 - frame_seg_hash[32]
 - index_seg_hash[32]
 - row_count(u32)
 - term_count(u32)
 - postings_bytes(u32)

- entries (entry_count entries), sorted by (frame_seg, index_seg) ascending:
non-canonical byte sequences from being treated as valid.

## Determinism

Encoding canonicalizes entries by sorting before writing. If two snapshots contain
the same set of entries, their encoded bytes are identical regardless of insertion
order.

## Relationship to IndexSegment

- IndexSegment v1 (docs/INDEX_SEGMENT_V1.md) is the per-segment postings index.
- IndexSnapshot v1 is the inventory/manifest tying segments to their indexes.

## CLI usage

See `fsa_lm build-index` for building index segments and emitting an IndexSnapshot
artifact hash.
