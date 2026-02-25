# LexiconSnapshot v1 (manifest)

LexiconSnapshot is a small manifest artifact that lists the LexiconSegment
artifacts that make up a lexicon view (initially Wiktionary, English-only).

It is used by Novel's retrieval layer to discover which lexicon segments exist
without scanning or decoding the full set of artifacts.

This v1 snapshot only links segment hashes and basic per-segment row counts.
Higher-level indexes (meta-code postings, adjacency lists, text tables) are
introduced in later stages.

## Canonical byte layout

All integers are little-endian.

- version(u16) = 1
- entry_count(u32)
- entries (entry_count entries), sorted by lex_seg_hash ascending:
 - lex_seg_hash[32]
 - lemma_count(u32)
 - sense_count(u32)
 - rel_count(u32)
 - pron_count(u32)

Decoding rejects non-canonical encodings:
- entries must be strictly sorted by lex_seg_hash
- duplicate lex_seg_hash values are rejected
- trailing bytes are rejected

## Determinism

Encoding canonicalizes entries by sorting before writing. If two snapshots contain
the same set of entries, their encoded bytes are identical regardless of
insertion order.

## Relationship to LexiconSegment

- LexiconSegment v1 (docs/LEXICON_SEGMENT_V1.md) stores the columnar lexicon rows.
- LexiconSnapshot v1 is the inventory/manifest tying together a set of segments.

## CLI usage

The CLI command `build-lexicon-snapshot` builds and stores a LexiconSnapshotV1
manifest from a list of LexiconSegment hashes.

See docs/CLI.md for the full command line.
