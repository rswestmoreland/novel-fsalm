# Wikipedia ingestion

Goal
----
Ingest a disk-first Wikipedia text dump into Novel's cold storage as immutable FrameSegment artifacts.

Input format (v1 TSV)
---------------------
Novel uses a simple, streaming TSV adapter:

- UTF-8, one document per line
- Columns:
 1) title
 2) text

Example line (tabs shown as "\t"):

 Ada Lovelace\tAugusta Ada King, Countess of Lovelace (1815-1852)...

For Wikipedia XML input, use ingest-wiki-xml/ingest-wiki-xml-sharded or the higher-level load-wikipedia wrapper.

Output artifacts
----------------
The ingest produces:

1) One or more FrameSegmentV1 artifacts
 - Each segment contains many FrameRowV1 rows (columnar encoding).
 - Rows are created by chunking each document text into fixed-size UTF-8 safe pieces, then extracting term frequencies.

2) One WikiIngestManifestV1 artifact
 - Lists produced FrameSegment hashes in creation order.
 - Records basic counts and segment build parameters.

The manifest hash is the stable "commit id" for this ingest run.

Determinism
-----------
Given identical input bytes and identical cfg parameters, the produced segment hashes and manifest hash are deterministic.

Chunking strategy
-----------------
To reduce memory and improve retrieval granularity, each document is split into text chunks:

- Each chunk is at most `row_max_bytes` bytes.
- Boundaries are UTF-8 safe (`str::is_char_boundary`).
- Each chunk becomes one FrameRowV1 row.
- `section_id` is derived deterministically from (doc_id, chunk_index).

CLI usage
---------
See docs/CLI.md for the full CLI reference. The key command is:

 fsa_lm ingest-wiki --root <db_root> --dump <path_to_tsv>

Common flags:

- --seg_mb N: segment target size in MB (approx, used to derive seg_rows)
- --row_kb N: row chunk size in KB (used to derive row_max_bytes and seg_rows)
- --chunk_rows N: FrameSegment chunk_rows parameter (default 1024)
- --max_docs N: cap documents ingested (for smoke tests)

Notes
-----
- This command writes cold FrameSegment artifacts and a manifest. Index snapshots are built separately (build-index) or via load-wikipedia.
- Retrieval uses IndexSnapshot/IndexSigMap built from ingested segments (see docs/RETRIEVAL_PIPELINE.md).
