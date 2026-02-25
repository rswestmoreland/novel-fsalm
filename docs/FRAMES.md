Frames and Columnar Context Memory
=============================================

Overview
--------
FSA-LM stores "context memory" and ingested knowledge as *frames* that can be
retrieved deterministically. Frames are designed to be stored as columnar
segments on disk, so the full corpus does not need to fit in RAM.

The high-level idea is to store context memory as columnar frames, with fields
similar to:

- who, what, when, where
- entity_ids
- verb, polarity
- confidence
- source_id
- term_ids and term frequencies (for retrieval)

This module currently defines the schema types and stable integer identifiers.
Segment formats and indexing are planned in later stages.

Row view vs columnar storage
----------------------------
In, `FrameRowV1` is a row-oriented staging structure for building
segments. Columnar storage (FrameSegment) will be introduced in 

Integer-only
------------
Confidence and scoring use integer-only or fixed-point representations. No
floats are used in the frame schema.

Stable ids
----------
Identifiers are derived deterministically using a domain separator and payload
bytes, hashed with BLAKE3 and interpreted as fixed little-endian integers.

Variable-length columns
-----------------------
Some fields are naturally variable-length (entity_ids, term lists). In segment
storage these will be represented via offset arrays and packed value buffers to
avoid per-row allocations.

FrameSegment v1
---------------
Frame rows are stored on disk as columnar segments with a chunked layout.

High-level layout (v1):
- Header (magic + version + chunk_rows + num_chunks)
- For each chunk:
 - rows_in_chunk
 - length-prefixed column blobs (fixed-width arrays + bitmaps + pools)

Columns per row (knowledge frames):
- doc_id (u64)
- source_id (u64)
- when_ns (i128)
- section_id (optional: bitmap + values)
- where_id (optional: bitmap + values)
- who/what (optional: bitmap + values)
- verb (optional: bitmap + values)
- polarity (i8 stored as u8)
- confidence (u32 Q16.16)
- doc_len (u32)
- entity_ids (offset/len + pool of u64)
- terms (offset/len + pools of term_id u64 and tf u32)

The v1 encoding uses fixed-width little-endian arrays for fast access.
Later stages may add compression (delta/varint, dictionary coding) while
keeping the logical schema stable.

Metaphonetic ids (MetaCodeId)
-----------------------------
In addition to TermId-based retrieval, Novel FSA-LM supports metaphonetic codes
(MetaCodeId) for sound-alike matching. These are derived from tokens using the
metaphone preprocessor (docs/METAPHONE.md).

Retrieval
---------
Frames are not retrieved as raw text. Retrieval selects FrameRow evidence using segment signatures and postings indexes (see docs/RETRIEVAL_PIPELINE.md).
