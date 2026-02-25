# IndexSegment v1 (IDX)

IndexSegment is a per-FrameSegment postings index. It is the core building block of Novel's
CPU-friendly retrieval. Each IndexSegment covers exactly one FrameSegment (same seg_hash).

Goals:
- Deterministic canonical bytes (bitwise stable within a build).
- Integer-only encoding and scoring primitives (no floats).
- Query-time access without expanding the full postings list unless requested.

## v1 invariants

- All rows in the source FrameSegment must share the same source_id (enforced at build time).
- Term dictionary entries are sorted by term id ascending.
- For a given term, postings are sorted by row index ascending.

## Canonical byte layout

All integers are little-endian.

- MAGIC[8] = "FSALMIDX"
- version(u16) = 1
- reserved(u16) = 0
- seg_hash[32]
- source_id(u64)
- row_count(u32)
- term_count(u32)
- term dictionary entries (term_count entries):
 - term_id(u64)
 - postings_off(u32)
 - postings_len(u32)
 - df(u32)
 - tf_sum(u32)
- postings_total_len(u32)
- postings_blob[postings_total_len]

### Postings encoding (per term)

The postings blob contains per-term payloads concatenated in dictionary order. Each per-term payload
is a sequence of pairs:

- (row_delta_varint_u32, tf_varint_u32)

Row indices are absolute row positions in the FrameSegment, in range 0..row_count.

row_delta uses a +1 encoding:
- first row_delta = row_ix + 1
- subsequent row_delta = (row_ix - prev_row_ix) + 1

This allows rejecting delta=0 while still representing row_ix=0.

Varint format is standard 7-bit little-endian continuation encoding.

## Why a separate IndexSegment?

FrameSegment is a columnar store optimized for sequential scans, compression, and deterministic
serialization. IndexSegment adds a postings-oriented view optimized for:
- term -> candidate rows (fast narrowing)
- df / tf_sum (quick scoring estimates)
- minimal decode work for single-term probes

IndexSegment is not a search engine index. It is an internal acceleration structure used to
support reasoning over structured frames and conversational context.
