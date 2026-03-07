@echo off
setlocal enabledelayedexpansion
REM Novel FSA-LM demo: load a tiny Wikipedia XML fixture, compact the index, then query before/after.
REM
REM This script avoids manual hash plumbing by reading snapshot ids from show-workspace.

set "ROOT=./demo_db_compact"
set "XML=./examples/wiki_tiny.xml"
set "EXE=target\debug\fsa_lm.exe"

if exist "%ROOT%" rmdir /s /q "%ROOT%"
mkdir "%ROOT%" || exit /b 1

if not exist "%EXE%" (
 echo Building %EXE%...
 cargo build --quiet --bin fsa_lm
 if errorlevel 1 exit /b 1
)

echo.
echo Loading Wikipedia (writes workspace defaults)...
"%EXE%" load-wikipedia --root "%ROOT%" --xml "%XML%" --shards 1 --seg_mb 1 --row_kb 1 --chunk_rows 64 --max_docs 10
if errorlevel 1 exit /b 1

set "WS_OUT=%ROOT%\workspace_out.txt"
"%EXE%" show-workspace --root "%ROOT%" > "%WS_OUT%"
if errorlevel 1 exit /b 1

set "SNAP="
set "SIG="
for /f "usebackq tokens=1,2 delims==" %%A in ("%WS_OUT%") do (
 if "%%A"=="merged_snapshot" set "SNAP=%%B"
 if "%%A"=="merged_sig_map" set "SIG=%%B"
)

if not defined SNAP (
 echo Failed to resolve merged_snapshot from workspace
 exit /b 1
)
if "%SNAP%"=="MISSING" (
 echo Failed to resolve merged_snapshot from workspace
 exit /b 1
)

echo Snapshot(before): %SNAP%

set "Q=banana bread recipe"

echo.
echo Query(before):
"%EXE%" query-index --root "%ROOT%" --snapshot %SNAP% --sig-map %SIG% --text "%Q%" --k 5
if errorlevel 1 exit /b 1

echo.
echo Plan(dry-run):
"%EXE%" compact-index --root "%ROOT%" --snapshot %SNAP% --target-bytes 1 --max-out-segments 1 --dry-run --verbose
if errorlevel 1 exit /b 1

REM Compact the snapshot. The command prints the new snapshot hash to stdout.
echo.
echo Compacting...
for /f %%i in ('"%EXE%" compact-index --root "%ROOT%" --snapshot %SNAP% --target-bytes 1 --max-out-segments 1 --verbose') do set OUT=%%i
if "%OUT%"=="" (
 echo Failed to capture compacted snapshot hash
 exit /b 1
)

echo Snapshot(after): %OUT%

echo.
echo Query(after):
"%EXE%" query-index --root "%ROOT%" --snapshot %OUT% --text "%Q%" --k 5
if errorlevel 1 exit /b 1

endlocal
