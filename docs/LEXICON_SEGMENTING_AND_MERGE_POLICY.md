# Lexicon segmenting and merge policy (v0)

This document defines the cross-segment rules for Novel's lexicon artifacts.
It complements:

- docs/LEXICON_SEGMENT_V1.md (single segment canonical encoding)
- docs/LEXICON_SNAPSHOT_V1.md (manifest of segments)

Novel stores lexicon data in LexiconSegment artifacts and ties them together
with a LexiconSnapshot manifest. The snapshot is only an inventory in v1; the
rules in this document ensure segments can be discovered, validated, and merged
into a coherent view deterministically.

Terminology
-----------

- Lemma: a headword entry, identified by LemmaId.
- Sense: a meaning of a lemma, identified by SenseId, and always owned by a
 parent lemma.
- Pronunciation: an IPA row for a lemma.
- Relation edge: an edge with a source node (lemma or sense), a relation type,
 and a target lemma.

- Owner lemma: the LemmaId that owns a row for partitioning purposes.
- Segment: a LexiconSegmentV1 artifact containing many lemma/sense/relation/
 pronunciation rows.
- Snapshot: a LexiconSnapshotV1 manifest that lists segment hashes.

Goals
-----

- Deterministic partitioning: given the same input rows and the same partition
 parameters, segment boundaries are identical.
- Disjoint ownership: within a snapshot, each LemmaId is owned by exactly one
 segment.
- Simple validation: it is possible to validate snapshot coherence without
 loading all segments fully into memory.

Non-goals (v0)
--------------

- High performance lexicon lookup indexes. This document only covers
  deterministic ownership, segmenting, validation, and merge policy.
- Supporting overlapping lemmas across segments inside a single snapshot.
 (v0 treats overlaps as an error.)

Owner lemma rules
-----------------

Rows map to an owner lemma as follows:

- LemmaRowV1: owner = lemma_id
- SenseRowV1: owner = lemma_id
- PronunciationRowV1: owner = lemma_id
- RelationEdgeRowV1:
 - from = RelFromId::Lemma(LemmaId): owner = that lemma
 - from = RelFromId::Sense(SenseId): owner = parent lemma of that sense

Note the last case requires a deterministic sense_id -> lemma_id mapping.
In v0, that mapping is derived from the SenseRowV1 rows present in the same
segment build input.

Partitioning (segmenting) policy
--------------------------------

Input
- A set of lexicon rows (lemmas, senses, relations, pronunciations).
- A partition count N (number of output segments).

Output
- N LexiconSegmentV1 artifacts, each containing a disjoint set of owner lemmas.

Partition function
- Compute owner lemma id value as a u64 (LemmaId wraps Id64(u64)).
- Compute seg_ix as:

 seg_ix = mix64(owner_lemma_u64) % N

Where mix64 is a fixed, deterministic mixing function (e.g., SplitMix64 finalizer
or the existing mix helper used elsewhere in the repo). The goal is to spread
lemmas across segments evenly while keeping the mapping deterministic.

Determinism requirements
- N must be explicit (a CLI/config parameter). Changing N changes segment
 boundaries; that is expected.
- The partition function must be stable across platforms and releases.
 Changing it is a format change and must be versioned.

Row assignment steps (recommended)
1) Build a SenseOwner map: SenseId -> LemmaId from all SenseRowV1 rows.
 - Reject duplicates (same SenseId mapped to two different lemmas).
2) For each row, compute owner lemma using the rules above.
 - For relation rows with from=Sense(sense_id), the sense_id must exist in the
 SenseOwner map.
3) Assign the row to seg_ix via the partition function.

Cross-row invariants
- All rows with the same owner lemma MUST land in the same segment.
- A segment MUST contain all rows required to interpret itself:
 - If the segment contains a relation row with from=Sense(sense_id), it must
 also contain the SenseRowV1 for that sense_id (so the owner mapping exists).

Validation and failure behavior
- If a relation row references a sense_id that is not present in the SenseOwner
 map, the segment build fails.
- If a segment contains a SenseId twice with different lemma owners, the segment
 build fails.

Snapshot merge policy (v0)
--------------------------

LexiconSnapshotV1 is a manifest of segments. In v0, it is also the unit of
coherence: a snapshot is valid only if its segments are disjoint by owner lemma.

Rules
- A snapshot MUST NOT contain overlapping LemmaIds across segments.
- Overlap detection should be deterministic and must not depend on hash table
 iteration order.

Practical validation strategy
- During snapshot build (or via a separate validate command), decode each
 candidate LexiconSegmentV1 header/columns enough to enumerate lemma_id values.
- Build a sorted list of lemma_id values per segment.
- Merge the lists (k-way merge) and reject duplicates.

If full lemma enumeration becomes too expensive for a larger lexicon, a later
version can add a per-segment lemma range summary or a compact signature.

Validation CLI
--------------

To validate a LexiconSnapshotV1 for disjoint owner-lemma coverage:

 fsa_lm validate-lexicon-snapshot --root <dir> --snapshot <hash32hex>

This command loads the snapshot and its referenced LexiconSegmentV1 artifacts and
fails if any LemmaId is present in more than one segment.

Reserved extension note
-----------------------

V0 keeps overlapping segments out of scope. If a later version introduces
incremental-update layers or multi-source overlays, that behavior MUST stay
explicit and deterministic. One possible policy would be:

- A snapshot becomes an ordered list of segment layers.
- For any owner lemma, the first layer that defines it wins.
- All rows for that lemma are taken from the winning segment only.

That would require new manifest fields and a separately versioned contract.

Notes on relation edges
-----------------------

Relation edges are owned by the "from" side. This keeps all outbound relations
for a lemma (or a specific sense) collocated with that lemma's segment.

The target lemma (to_lemma_id) may live in a different segment; that is allowed.
It is the responsibility of later retrieval layers to decide whether to follow
cross-segment links based on budgets.

Implementation status
---------------------

The current runtime implements the row segmenter in `src/lexicon_segmenting.rs`:
- `LexiconRowsV1` holds row-form bundles.
- `segment_lexicon_rows_v1(rows, segment_count)` partitions rows by owner lemma.
- Failures match the v0 contract (unknown SenseId in relation owner, SenseId owner mismatch, invalid segment_count).
It also implements snapshot validation for disjoint lemma ownership in
`src/lexicon_snapshot_validate.rs`:
- `validate_lexicon_snapshot_v1_disjoint_owners(store, snapshot_hash)` performs a
 deterministic duplicate check via sorted lemma lists.
The CLI exposes the validator via:
- `fsa_lm validate-lexicon-snapshot --root <dir> --snapshot <hash32hex>`
