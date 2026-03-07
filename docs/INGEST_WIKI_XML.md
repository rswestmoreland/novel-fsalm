Wikipedia XML ingestion
==================================

Novel includes a TSV adapter for ingesting (title, text) records into cold storage.

It also includes a streaming Wikipedia XML extractor that feeds the same ingest pipeline.

Input format
------------
The Wikimedia "pages-articles" dump is commonly distributed as `.xml.bz2`.
The CLI supports .xml.bz2 via the bzip2 crate in streaming mode (memory stays bounded).


What is extracted
-----------------
From each `<page>` block (after filtering), we extract:

- `<title>...</title>` (XML entities decoded)
- `<ns>...</ns>` (namespace)
- `<text...>...</text>` (revision text; entities decoded)

Namespace filtering
-------------------
By default, only <ns>0</ns> pages are ingested (main namespace).

Determinism
-----------
- Identical XML bytes produce identical extracted text bytes (after entity decode).
- doc_id is derived from the unescaped title bytes:

```text
derive_id64("doc\0", title_bytes)
```

CLI
---
Use:

 fsa_lm ingest-wiki-xml --xml <path> [--root <dir>] [--seg_mb <u32>] [--row_kb <u32>] [--chunk_rows <u32>] [--max_docs <u64>]

The output is the manifest artifact hash (hex32), which points to the produced
FrameSegment artifacts.

Examples
--------
See:

 examples/demo_cmd_ingest_wiki_xml.bat
 examples/wiki_tiny.xml


Bzip2 streaming support
----------------------
`ingest-wiki-xml` supports `.xml.bz2` via `--xml-bz2 <path>`.
The decoder is streaming (File -> BzDecoder -> BufReader), so memory stays bounded.
