LexiconSegment v1
=================

Purpose
-------
LexiconSegmentV1 is the on-disk, columnar storage format for lexicon rows
(lemmas, senses, relation edges, and pronunciations).

The segment stores only ids and masks. Text payloads (lemma strings, glosses,
IPA) are referenced by TextId and will be stored via separate artifacts in a
later stage (dictionary coding / text tables).

This format is implemented in src/lexicon_segment.rs and is produced by ingest-wiktionary-xml and build-lexicon-snapshot.

Design goals
------------
- Deterministic, canonical bytes (bitwise stable within a build).
- Integer-only schema (u64 ids, u32 masks, u16 ranks).
- Strict validation on decode: non-canonical payloads are rejected.

Byte layout (canonical)
----------------------
All integers are little-endian.

Header:
- MAGIC[8] = "FSALMLEX"
- version(u16) = 1
- reserved(u16) = 0
- lemma_count(u32)
- sense_count(u32)
- rel_count(u32)
- pron_count(u32)
- meta_pool_count(u32)

Lemma columns (each length = lemma_count):
- lemma_id(u64)
- lemma_key_id(u64)
- lemma_text_id(u64)
- pos_mask(u32)
- flags(u32)

Sense columns (each length = sense_count):
- sense_id(u64)
- lemma_id(u64)
- sense_rank(u16)
- gloss_text_id(u64)
- labels_mask(u32)

Relation columns (each length = rel_count):
- from_tag(u8) (0 = LemmaId, 1 = SenseId)
- from_id(u64) (payload determined by from_tag)
- rel_type_id(u16)
- to_lemma_id(u64)

Pronunciation columns (each length = pron_count):
- lemma_id(u64)
- ipa_text_id(u64)
- meta_off(u32) (offset into meta_pool)
- meta_len(u32) (length into meta_pool)
- flags(u32)

Meta pool:
- meta_pool(u64 * meta_pool_count) (MetaCodeId payloads)

Canonical ordering rules
------------------------
The segment must be in canonical order (enforced on decode and by the builder):
- Lemmas: sorted by lemma_id ascending; lemma_id must be unique.
- Senses: sorted by (lemma_id, sense_rank, sense_id) ascending.
- Relations: sorted by (from_tag, from_id, rel_type_id, to_lemma_id) ascending.
- Pronunciations: sorted by (lemma_id, ipa_text_id, flags, meta_codes) ascending,
 where meta_codes are compared lexicographically by (len, codes...).
- Each pronunciation slice in meta_pool (meta_off/meta_len) must be sorted and
 unique, and must be in-bounds.

Source of truth
---------------
- src/lexicon_segment.rs
