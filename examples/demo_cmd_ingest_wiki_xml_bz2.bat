@echo off
setlocal

REM Demo: ingest the tiny XML fixture in either plain or.bz2 form.
REM Requires: cargo

set ROOT=.\demo_db_xml_bz2
set XML=.\examples\wiki_tiny.xml
set XMLBZ2=.\examples\wiki_tiny.xml.bz2

echo Running: ingest-wiki-xml (plain XML)...
cargo run --bin fsa_lm -- ingest-wiki-xml --root %ROOT% --xml %XML% --seg_mb 1 --row_kb 1 --chunk_rows 64 --max_docs 10

echo Running: ingest-wiki-xml (bz2)...
cargo run --bin fsa_lm -- ingest-wiki-xml --root %ROOT% --xml-bz2 %XMLBZ2% --seg_mb 1 --row_kb 1 --chunk_rows 64 --max_docs 10

endlocal
