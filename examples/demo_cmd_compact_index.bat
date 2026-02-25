@echo off
setlocal
REM demo: ingest tiny wiki XML, build an index snapshot, compact it, then query before/after.

set ROOT=./demo_db_compact
set XML=./examples/wiki_tiny.xml

if exist %ROOT% rmdir /s /q %ROOT%
mkdir %ROOT%

REM Ingest a tiny XML file into FrameSegment artifacts.
cargo run --bin fsa_lm -- ingest-wiki-xml --root %ROOT% --xml %XML% --seg_mb 1 --row_kb 1 --chunk_rows 64 --max_docs 10

REM Build an IndexSnapshotV1 and capture its hash.
for /f %%i in ('cargo run --bin fsa_lm -- build-index --root %ROOT%') do set SNAP=%%i
echo Snapshot(before): %SNAP%

set Q=banana bread recipe

echo Query(before):
cargo run --bin fsa_lm -- query-index --root %ROOT% --snapshot %SNAP% --text "%Q%" --k 5

echo Plan(dry-run):
cargo run --bin fsa_lm -- compact-index --root %ROOT% --snapshot %SNAP% --target-bytes 1 --max-out-segments 1 --dry-run --verbose

REM Compact the snapshot. The command prints the new snapshot hash to stdout.
for /f %%i in ('cargo run --bin fsa_lm -- compact-index --root %ROOT% --snapshot %SNAP% --target-bytes 1 --max-out-segments 1 --verbose') do set OUT=%%i
echo Snapshot(after): %OUT%

echo Query(after):
cargo run --bin fsa_lm -- query-index --root %ROOT% --snapshot %OUT% --text "%Q%" --k 5

endlocal