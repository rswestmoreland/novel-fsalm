Retrieval pipeline
==================

Purpose
-------
Novel FSA-LM is not a search engine. Retrieval is a *memory subsystem* that selects
small, structured evidence for reasoning and synthesis. The system avoids copying
large text spans. Evidence is represented as compact typed rows (frames + lexicon),
referenced by stable ids and hashes.

This document captures the current retrieval design:

- deterministic IndexSegmentV1 lookup over columnar postings
- optional SegmentSigV1 gating via IndexSigMapV1
- integer-only memory relevance scoring
- deterministic query planning and tie-breaking
- EvidenceBundleV1 assembly and evidence-first synthesis contracts

Implementation status note
--------------------------
The current codebase implements exact IndexSegmentV1 lookup for `query-index`,
optional deterministic SegmentSigV1 gating when the caller provides an
`IndexSigMapV1` hash (for example via `--sig-map`), and EvidenceBundleV1
assembly for the answer path. Block-level top-k skipping remains a reserved
extension.

Control note
------------
RetrievalControlV1 is an optional control record derived from the pragmatics track.
When the caller provides a control record to the query path:
- Equal-score hit ordering uses a control-derived tie-break key (deterministic).
- Truncation becomes tie-inclusive: all hits tied at the cutoff score are retained.

This prevents evidence selection from depending on style/control signals when scores
tie, while still allowing deterministic presentation order for tied candidates.
EvidenceBundleV1 canonicalization remains stable and independent of control.

E2E check:
- tests/e2e_retrieval_control_evidence_equivalence_smoke.rs asserts that evidence bundle
 bytes remain identical across different control seeds on a forced tie group.

stage notes:
- adds the SegmentSigV1 artifact format (Bloom over TermId, sketch
 reserved) (docs/SEGMENT_SIG_V1.md).
- adds SegmentSig store helpers plus IndexSigMapV1
 (docs/INDEX_SIG_MAP_V1.md).
- wires build-index and compact-index to emit SegmentSigV1 artifacts
 and an IndexSigMapV1 sidecar for each produced IndexSnapshotV1.
- adds a small core gating helper (src/retrieval_gating.rs) with
 unit tests.
- wires gating into query-index and build-evidence behind an
 explicit IndexSigMapV1 hash, and adds an integration test that
 verifies a gated skip avoids loading an unrelated index artifact.


Signature gating runbook
------------------------
Signature gating is optional and explicit. It is enabled only when the caller
provides an IndexSigMapV1 hash (CLI flag: --sig-map).

Prerequisites:
- An IndexSnapshotV1 hash (snapshot id)
- An IndexSigMapV1 hash (sig-map id)

How to obtain snapshot id and sig-map id
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The build-index and compact-index commands print the snapshot id to stdout and
print the associated sig-map id to stderr in the form:

 index_sig_map=<hash32hex>

Example (bash-like):

 fsa_lm build-index --root./artifacts 1>snapshot.txt 2>build.log
 snapshot=$(cat snapshot.txt)
 sig_map=$(grep '^index_sig_map=' build.log | cut -d= -f2)

Querying with and without gating
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Ungated query (baseline behavior):

 fsa_lm query-index --root./artifacts --snapshot $snapshot --text "login failed" --k 10

Gated query (signature-first skip):

 fsa_lm query-index --root./artifacts --snapshot $snapshot --sig-map $sig_map --text "login failed" --k 10

The gated query prints a one-line summary to stderr:

 gate.entries_total=... entries_decoded=... entries_skipped_sig=... entries_missing_sig=... query_terms_total=... bloom_probes_total=...

Fields:
- entries_total: number of snapshot entries scanned.
- entries_decoded: number of IndexSegmentV1 artifacts decoded (store.get + decode).
- entries_skipped_sig: entries skipped because SegmentSigV1 proved none of the
 query terms can exist in that index artifact.
- entries_missing_sig: entries forced down the decode path because the snapshot
 entry lacked a signature mapping, or the referenced signature artifact could
 not be loaded/decoded.
- query_terms_total: number of distinct query terms used for gating.
- bloom_probes_total: total bloom probes performed (implementation-dependent;
 useful as a rough measure of gating work).

build-evidence supports the same gating flag
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
build-evidence accepts --sig-map and prints the same gate.* summary line when
gating is enabled:

 fsa_lm build-evidence --root./artifacts --snapshot $snapshot --sig-map $sig_map --text "login failed" --k 10

Notes:
- Gating is conservative: it only skips an entry when it can prove the query
 cannot match.
- When signatures are missing, the query path falls back to the decode path.
- For performance measurements, combine --sig-map with --cache-stats to see
 cache hit/miss behavior alongside gating counters.


Terminology
-----------
- Segment: immutable columnar container (FrameSegmentV1). Stored as an artifact by hash.
- Chunk: a subrange of rows inside a segment (fixed chunk_rows).
- RowIx: 0-based row ordinal within a segment (u32). This is used for postings.
- DocId: stable 64-bit id for an external document identity (u64).
- TermId: stable id for a token (derived from bytes; see tokenizer).
- MetaCodeId: stable id for metaphonetic code (see metaphone).

The D + C/B hybrid
------------------
Retrieval is a three-stage pipeline:

Stage 0 (hot): Conversation state
 - PromptPack and replay log define the request.
 - Hot working set is small and deterministic (recent turns, recent entities/terms).

Stage 1 (warm gate): D = Segment/Chunk gating
 - For each segment (or chunk within a segment), consult:
 - Bloom filter over TermId and MetaCodeId
 - Sketch fingerprint (fixed-size bitset or minhash-like signature)
 - Reject segments/chunks that cannot match.
 - Rank remaining candidates by approximate overlap score.
 - Touch only the top S segments/chunks (cap).

Stage 2 (exact): C or B = Postings-based candidate selection
 - For each required TermId / MetaCodeId, read postings lists within the candidate segments.
 - Intersect postings lists in deterministic order (smallest df first).
 - Option C stores postings as two columns: row_deltas and tfs.
 - Option B extends this with per-block max score bounds (BlockMax/WAND) for top-k.

Stage 3 (synthesis input): Evidence packing
 - Convert matched row references to compact Evidence items.
 - Enforce strict caps on evidence count and bytes.

Evidence contract
-----------------
Retrieval returns structured evidence only. It does not return long text.

EvidenceBundleV1 fields (v1):
 - query_id: Hash32 (hash of query features + planner config)
 - snapshot_id: Hash32 (corpus snapshot ids)
 - limits: caps used (segments_touched, evidence_items, evidence_bytes)
 - score_model_id: u32
 - items[]: stable sorted by (score desc, type, stable_id)

Evidence item types:
 - Frame evidence: FrameRowV1 or a compact reference to a row within a segment.
 - Lexicon evidence: Lemma/Sense/Relation/Pronunciation rows (ids + small flags).
 - ProofStep/MathResult: deterministic arithmetic results (int/bigint), when applicable.

Anti-plagiarism guardrails:
 - Default MaxQuoteBytes = 0 (no raw quotes returned).
 - If enabled for debugging, quotes are capped and always source-linked.
 - The synthesis layer must never emit verbatim evidence text unless explicitly requested.

Synthesis contract
------------------
The thinking layer consumes EvidenceBundleV1 and produces:
 - AnswerDraft: the final chat text plus uncertainty.
 - AnswerTrace (optional but recommended): claims[] with evidence references.

Hallucination avoidance policy (deterministic):
 - If the request is factual and evidence support < threshold:
 - ask for clarification, or say unknown.

Deterministic query planner
---------------------------
Planner input: PromptPack + hot context + configs.

Planner output: a fixed DAG of operations.
Determinism requirements:
 - Stable tokenization and id derivation.
 - Stable sorting and tie-breakers.
 - Bounded buffers, no time-based randomness.

Query Feature Vector (QFV):
 - Q_terms: TermIds from tokenizer (plus dedup)
 - Q_meta: MetaCodeIds from metaphone
 - Q_entities: optional entity ids if present
 - intent: small enum (definition, factoid, comparison, math, etc.)
 - required vs optional: deterministic heuristic

Planning steps (v1):
 1) Gate segments/chunks with Bloom + sketch (D).
 2) Within top candidates, fetch postings for required terms/meta (C).
 3) Intersect postings lists by ascending df (tie by id).
 4) Score candidate rows with MRS (below).
 5) Apply diversity caps (max per doc_id / lemma_id).
 6) Emit EvidenceBundleV1 (top K, stable).

Diversity caps (current implementation)
------------------------------------
The retrieval policy layer can apply deterministic diversity caps to the ranked hit list.

Caps are configured on `RetrievalPolicyCfgV1`:

 - `max_hits_per_frame_seg` (0 disables): cap hits per FrameSegment.
 - `max_hits_per_doc` (0 disables): cap hits per DocId (requires loading FrameSegments).

Novelty scoring (current implementation)
---------------------------------------
The retrieval policy layer can optionally re-rank the candidate hit list using a
stable novelty signal as a secondary key after score.

Novelty is inverse-frequency over the full candidate list:
 novelty = 65535 / freq

Configured on `RetrievalPolicyCfgV1`:
 - `novelty_mode`:
 0 = off
 1 = DocId inverse frequency
 2 = FrameSegment inverse frequency
 3 = DocId + FrameSegment inverse frequency

Novelty re-ranking never overrides the primary score ordering. It only changes
ordering within equal-score groups (or among oversampled candidates when the
policy layer truncates to `max_hits`).

When enabled, we first dedupe hits by exact `(frame_seg, row_ix)` identity, then scan hits in
rank order enforcing caps.

`include_ties_at_cutoff=1` semantics are preserved: if the engine returned extra hits due to a
tie at the cutoff score, the refine stage may return more than `max_hits` (but only from the
cutoff tie group).

When `include_ties_at_cutoff=0` and caps are enabled, the search engine deterministically
oversamples candidates (`k * 8`, capped at 1024) so that the refine stage can fill `max_hits`
after filtering.

Memory Relevance Score (MRS) - integer only
------------------------------------------
MRS is not a document ranker. It is a lightweight score to select evidence.

All components are integers, combined as a weighted sum.

 MRS =
 Wm * MatchScore
 + Ws * SpecificityScore
 + Wr * RecencyScore
 + Wc * ConfidenceScore
 + Wk * ContextOverlapScore
 - Wp * PenaltyScore

MatchScore (ints):
 - hits_terms: count of matched TermIds
 - hits_meta: count of matched MetaCodeIds
 - hits_entities: count of matched entity ids
 - MatchScore = A*hits_entities + B*hits_terms + C*hits_meta

SpecificityScore:
 - sum(idf_u16(feature)) over matched features (cap)
 - idf_u16 is precomputed at ingest time using the bucket scheme below.

RecencyScore:
 - bonus if source_id is part of the active conversation session
 - otherwise 0 (Wikipedia snapshot is stable)

ConfidenceScore:
 - use existing fixed-point confidence (0..10000)
 - map into score scale by integer multiply/divide

ContextOverlapScore:
 - overlap with hot context term/entity sets (small bonus)

PenaltyScore:
 - penalize huge doc_len buckets and low confidence

Tie-breaking:
 - stable sort by (MRS desc, evidence_type, stable_id asc).

IDF bucket scheme (integer only, ingest-time)
---------------------------------------------
Goal: approximate log-based rarity without floats at query time.

Inputs:
 - N: total docs/rows in the indexed universe (u64)
 - df: document frequency for a feature (u64), df >= 1

Outputs:
 - idf_u16: a non-negative rarity weight in [0..65535]

v1 algorithm (fast, deterministic):
 1) Compute ratio in fixed-point:
 ratio_q = ((N + 1) << Q) / (df + 1)
 where Q = 20 (fixed-point bits).
 2) Compute log2(ratio_q) in fixed-point using integer operations:
 - Let k = ilog2(ratio_q) (0..)
 - Normalize m = ratio_q >> k, so m in [1<<Q, 2<<Q)
 - Take top B bits of mantissa: mant = (m - (1<<Q)) >> (Q - B)
 where B = 8 (256 buckets for fractional part).
 - log2_q = k*(1<<B) + mant
 3) Map to u16:
 idf_u16 = clamp_u16(log2_q * SCALE)
 where SCALE is chosen to fit in u16 (example: SCALE=8).

Notes:
 - This is computed offline during ingest, not per query.
 - For very common features, idf_u16 tends toward 0.
 - For rare features, idf_u16 increases and saturates at 65535.
 - Using ilog2 and a coarse mantissa avoids floats and keeps cost low.

Postings block format (Option C, lean, streaming-friendly)
---------------------------------------------------------
Goal: fast intersections and top-k, without bloating storage.

Key choice: postings reference RowIx (u32) within a segment, not DocId (u64).
This reduces storage and improves cache locality. DocId is retrieved later from
the frame columns for final evidence emission.

For each segment, maintain an index artifact (planned ):
 - TermDict: TermId -> (postings_offset, postings_len, df, optional idf_u16)
 - MetaDict: MetaCodeId -> (offset, len, df, optional idf_u16)
 - Postings data area: compressed blocks

Blocked postings store two parallel columns:
 - row_deltas stream: delta-encoded RowIx
 - tf stream: term frequencies aligned to rows

v1 encoding:
 - Block size: 128 entries (tunable).
 - For each block, store a small BlockIndex entry:
 last_row_ix: u32 (absolute RowIx of last entry in block)
 off_rows: u32 (byte offset into row_deltas stream)
 off_tfs: u32 (byte offset into tf stream)
 Optional later:
 block_max_score: u16
 block_max_tf: u16

 - row_deltas stream:
 first entry stored as absolute RowIx varint
 subsequent entries stored as delta varint (>= 1)
 - tf stream:
 tf stored as varint (u32) or as u8 with escape if tf > 255

Varint v1: standard 7-bit continuation (unsigned):
 - bytes: low 7 bits payload, high bit indicates continuation
 - deterministic encoding (no alternate forms)

Storage bloat control:
 - Keep block index minimal (3 u32 per block in v1).
 - Choose block size to amortize metadata.
 - Rely on D gating so only a small number of segments are accessed.

BlockMax/WAND extension (Option B, later)
----------------------------------------
To accelerate top-k:
 - Add block_max_score per postings block.
 - Query uses deterministic WAND to skip blocks that cannot exceed current threshold.
 - Still stable with fixed ordering and tie-break rules.

What makes this "LLM memory" and not "search"
---------------------------------------------
- Retrieval returns structured evidence rows, not text passages.
- Scoring is tuned for reasoning usefulness, not click-through ranking.
- Synthesis produces paraphrased claims with uncertainty, not excerpts.
- Diversity caps prevent overfitting to a single document.

Implementation notes
--------------------
- All algorithms are integer-only and deterministic.
- Expensive computations (df/idf) happen at ingest time.
- Query-time operations are bounded by explicit caps.
- Distributed mode: segments and postings artifacts are content-addressed and can be
 fetched by hash. Query planning remains deterministic across nodes.

Tests
-----
- Integration E2E smoke: tests/e2e_ingest_index_query_evidence_smoke.rs
 - Exercises: ingest_wiki_tsv -> build IndexSnapshotV1 -> search_snapshot -> build EvidenceBundleV1
 - Validates reference correctness and deterministic artifact hashes.
