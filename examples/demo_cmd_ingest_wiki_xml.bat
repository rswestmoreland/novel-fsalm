\
@echo off
setlocal
REM demo: ingest a tiny Wikipedia XML file into a demo artifact store.
set ROOT=.\demo_db_xml
set XML=.\examples\wiki_tiny.xml

if exist %ROOT% rmdir /s /q %ROOT%
mkdir %ROOT%

cargo run --bin fsa_lm -- ingest-wiki-xml --root %ROOT% --xml %XML% --seg_mb 1 --row_kb 1 --chunk_rows 64 --max_docs 10
endlocal
