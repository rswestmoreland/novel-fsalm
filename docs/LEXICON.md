Lexicon Schema v1
============================

Purpose
-------
Novel FSA-LM separates "knowledge" from "linguistics".

- Knowledge (Wikipedia, text corpora):
 stored as `FrameRowV1` inside `FrameSegmentV1`,
 persisted via the artifact store.

- Linguistics (Wiktionary, English-only):
 stored as lexicon rows (this stage) and later as lexicon segments/snapshots
 so it can be queried without loading everything into RAM.

What counts as "linguistics" here
--------------------------------
- Lemmas (word forms), parts-of-speech, and senses (definitions)
- Lexical relations (synonyms, antonyms, hypernyms, etc.)
- Pronunciations (IPA strings) and metaphonetic codes for reflex matching

Core ideas
----------
1) Disk-first:
 The lexicon will be stored in immutable, content-addressed segments.
 This stage only defines the row schema and stable ids.

2) Deterministic ids:
 - `LemmaId` is derived from exact lemma bytes (case-preserving).
 - `LemmaKeyId` is derived from ASCII-lowercased lemma bytes to support
 deterministic lookup from tokenized user input without colliding `LemmaId`.

3) External text payloads:
 Rows reference `TextId` values instead of embedding large strings.
 will define dictionary-coded text tables and/or text artifacts.

Row types (v1)
--------------
Defined in `src/lexicon.rs`:

- `LemmaRowV1`:
 - lemma_id, lemma_key_id, lemma_text_id, pos_mask, flags

- `SenseRowV1`:
 - sense_id, lemma_id, sense_rank, gloss_text_id, labels_mask

- `RelationEdgeRowV1`:
 - from (lemma or sense), rel_type_id, to_lemma_id

- `PronunciationRowV1`:
 - lemma_id, ipa_text_id, meta_codes[], flags
 - `meta_codes` must be sorted and unique for canonical encoding.

Part-of-speech and relation ids
-------------------------------
- POS uses a u32 bitmask (initial constants in `lexicon.rs`).
- Relation type ids are compact u16 values (initial constants in `lexicon.rs`).

Future work
----------------------
- LexiconSegment: columnar storage for lemma/sense/relation/pronunciation rows.
- LexiconSnapshot: a deterministic view over a set of segments plus indexes.
- Metaphonetic index: meta_code_id -> postings (lemma ids).
- Text tables: dictionary-coded storage for lemma/gloss/IPA strings.

Future retrieval
---------------
- MetaphoneticIndex: MetaCodeId -> lemma postings (planned )
- Relations adjacency lists and sense gloss text are stored disk-first; synthesis uses ids.
