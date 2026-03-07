Wiktionary ingest v1
==================

Purpose
-------
Provide a deterministic ingest path from a Wiktionary dump (English-only) into
lexicon artifacts:

- LexiconSegmentV1 (immutable, columnar segments)
- LexiconSnapshotV1 (a manifest of segment hashes)

This enables lexicon-backed query expansion in the answering loop via:

answer ... --expand --lexicon-snapshot <hash32hex>

Scope
-----
This contract covers "full rows" for English entries:

- Lemmas: lemma id, lookup key id, lemma text id, part-of-speech mask, flags
- Senses: ranked gloss rows (definition lines)
- Relations: lemma-level relation edges (synonym/antonym/related/hypernym/hyponym)
- Pronunciations: IPA text id and metaphonetic codes

This contract does not require a text table/dictionary artifact yet. TextId values
are derived from the exact UTF-8 bytes and stored in the segment rows.

Inputs
------
Supported dump formats:

- Wiktionary XML: --xml <path>
- Wiktionary XML.bz2: --xml-bz2 <path>

The ingest reads pages using the existing streaming XML adapter used by the wiki
XML ingest path. Only page title and page text are required.

Proposed CLI surface
--------------------
Implemented CLI:

ingest-wiktionary-xml --root <dir> (--xml <path> | --xml-bz2 <path>) --segments <n> [--max_pages <n>] [--out-file <path>]

Behavior:
- Produces LexiconSegmentV1 artifacts and a LexiconSnapshotV1 artifact.
- Prints stable hash lines for each segment and the final snapshot.
- If --out-file is set, writes the same hash lines to that file.

Determinism and bounds
----------------------
The ingest must be deterministic and bounded:

- Stable parsing rules with explicit allowlists (sections, headers, templates).
- Stable caps on per-page work.
- Stable ordering of emitted rows (segment builder enforces canonical ordering).
- Unknown or out-of-scope patterns are ignored, not guessed.

Recommended v1 caps (can be adjusted later via an explicit contract update):
- max_page_text_bytes: 131072
- max_senses_per_lemma: 16
- max_relations_per_lemma_per_type: 32
- max_pronunciations_per_lemma: 8
- max_ipa_bytes: 96

English section selection
-------------------------
Only the ==English== section is processed.

Rules:
- Headings are detected using wikitext heading markers.
- The English section name match is exact after trimming ASCII whitespace.
- If no English section exists, the page is skipped.

Part-of-speech mapping
----------------------
Within the English section, only the following POS headers contribute to pos_mask:

- Noun -> POS_NOUN
- Verb -> POS_VERB
- Adjective -> POS_ADJ
- Adverb -> POS_ADV
- Proper noun -> POS_PROPER_NOUN
- Pronoun -> POS_PRONOUN
- Determiner -> POS_DETERMINER
- Preposition -> POS_PREPOSITION
- Conjunction -> POS_CONJUNCTION
- Interjection -> POS_INTERJECTION
- Numeral -> POS_NUMERAL
- Particle -> POS_PARTICLE

All other POS headers are ignored.

Row mapping and ids
-------------------
Ids are derived using existing stable functions in src/lexicon.rs:

- lemma_id: derive_lemma_id(title)
- lemma_key_id: derive_lemma_key_id(title)
- lemma_text_id: derive_text_id(title)

Text ids for gloss and IPA are derived from the exact extracted strings:

- gloss_text_id: derive_text_id(gloss_text)
- ipa_text_id: derive_text_id(ipa_text)

Senses
------
Senses are extracted from definition lines within each supported POS section.

Definition line rule (v1):
- A sense line is a line whose first non-space character is '#'.
- Lines starting with '#:' are treated as examples and ignored.
- The gloss text is the remainder of the line after leading '#' markers, trimmed.
- If the gloss text begins with one or more leading {{lb|en|...}} label templates, those templates are stripped and the result is trimmed again.

Ranking:
- sense_rank starts at 1 and increments in appearance order within the lemma.
- Only the first max_senses_per_lemma senses are kept.

sense_id is derived from (lemma_id, sense_rank) via derive_sense_id.

Relations
---------
Relations are extracted from the following subsection headings (under English,
and typically within a POS section):

- Synonyms -> REL_SYNONYM
- Antonyms -> REL_ANTONYM
- Related terms -> REL_RELATED
- Hypernyms -> REL_HYPERNYM
- Hyponyms -> REL_HYPONYM
- Derived terms -> REL_DERIVED_TERM
- Coordinate terms -> REL_COORDINATE_TERM
- Holonyms -> REL_HOLONYM
- Meronyms -> REL_MERONYM

Target extraction rule (v1):
- Accept wikilinks of the form [[target]] and [[target|label]] (also written as [[word|display]]; use the target, before any '|').
- Accept {{l|en|target}} and {{m|en|target}} templates (take the first target param).
- Targets are trimmed; empty targets are ignored.

Row mapping:
- from_id: RelFromId::Lemma(lemma_id) (lemma-level only in v1)
- rel_type_id: mapped from the subsection heading
- to_lemma_id: derive_lemma_id(target)

Pronunciations
--------------
Pronunciations are extracted from a Pronunciation subsection under English.

IPA extraction rule (v1):
- Accept {{IPA|en|/ipa/}} and {{IPA|en|ipa}} forms:
  - language must be 'en'
  - the first IPA payload parameter is used
- Ignore other pronunciation templates in v1.

Pronunciation row mapping:
- lemma_id: parent lemma id
- ipa_text_id: derive_text_id(ipa_string)
- meta_codes: metaphone codes derived from the lemma text tokens:
  - tokenize the lemma text
  - compute meta_code_id_from_token for each token
  - sort and unique the ids

Segmenting and snapshot build
-----------------------------
The ingest builds N segments (where N = --segments) using the existing deterministic
segmenting policy and produces a LexiconSnapshotV1 listing those segment hashes.

Validation:
- The produced snapshot should pass validate-lexicon-snapshot.

Tiny fixtures
-------------
The repository includes a minimal Wiktionary XML fixture for operator demos and regression tests:

- examples/wiktionary_tiny.xml (and examples/wiktionary_tiny.xml.bz2)

This tiny fixture is intended to cover the explicit allowlists and extraction forms in this contract,
including:

- POS headers listed in Part-of-speech mapping
- Relation headings listed in Relations
- Link/target forms: [[target]], [[target|label]] ([[word|display]]), {{l|en|target}}, {{m|en|target}}
- Sense label cleanup: leading {{lb|en|...}} on definition lines

The ingest must remain deterministic when this fixture is ingested multiple times into fresh roots.

See also
--------
- docs/LEXICON.md
- docs/LEXICON_SEGMENT_V1.md
- docs/LEXICON_SNAPSHOT_V1.md
- docs/LEXICON_QUERY_EXPANSION.md
