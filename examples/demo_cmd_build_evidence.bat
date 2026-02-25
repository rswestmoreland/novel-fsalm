\
@echo off
setlocal
REM demo: ingest tiny wiki XML, build index snapshot, then build an EvidenceBundle.

set ROOT=./demo_db_evidence
set XML=./examples/wiki_tiny.xml

if exist %ROOT% rmdir /s /q %ROOT%
mkdir %ROOT%

REM Ingest a tiny XML file into FrameSegment artifacts.
cargo run --bin fsa_lm -- ingest-wiki-xml --root %ROOT% --xml %XML% --seg_mb 1 --row_kb 1 --chunk_rows 64 --max_docs 10

REM Build an IndexSnapshotV1 and capture its hash.
for /f %%i in ('cargo run --bin fsa_lm -- build-index --root %ROOT%') do set SNAP=%%i

echo Snapshot: %SNAP%

REM Build an EvidenceBundleV1 artifact and capture its hash.
set Q=banana bread recipe
for /f %%i in ('cargo run --bin fsa_lm -- build-evidence --root %ROOT% --snapshot %SNAP% --text "%Q%" --k 5 --max_items 5 --max_bytes 65536 --verbose') do set EV=%%i

echo EvidenceBundle: %EV%
endlocal
