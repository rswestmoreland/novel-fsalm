@echo off
REM Novel FSA-LM demo: ingest a tiny Wikipedia TSV file.
REM This script is intended to run from the repo root on Windows.

set ROOT=.\demo_db
set DUMP=.\examples\wiki_tiny.tsv

if exist %ROOT% (
 rmdir /S /Q %ROOT%
)
mkdir %ROOT%

REM Create a tiny TSV with two "documents".
echo Ada Lovelace\tAda Lovelace was an English mathematician and writer.> %DUMP%
echo Alan Turing\tAlan Turing was a pioneering computer scientist.>> %DUMP%

REM Ingest using small segment/row sizing for the demo.
cargo run --bin fsa_lm -- ingest-wiki --root %ROOT% --dump %DUMP% --seg_mb 1 --row_kb 1 --chunk_rows 64 --max_docs 10

echo.
echo Done. The CLI printed the manifest hash for this ingest run.
echo The artifact store is in %ROOT%.
