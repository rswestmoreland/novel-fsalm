@echo off
setlocal enabledelayedexpansion
REM Novel FSA-LM demo: load a tiny Wikipedia XML fixture, then build an EvidenceBundle.
REM
REM This script avoids manual hash plumbing by reading snapshot ids from show-workspace.

set "ROOT=./demo_db_evidence"
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
if not defined SIG (
 echo Failed to resolve merged_sig_map from workspace
 exit /b 1
)
if "%SIG%"=="MISSING" (
 echo Failed to resolve merged_sig_map from workspace
 exit /b 1
)

echo Snapshot: %SNAP%
echo SigMap:   %SIG%

echo.
echo Building EvidenceBundle...
set "Q=banana bread recipe"
"%EXE%" build-evidence --root "%ROOT%" --snapshot %SNAP% --sig-map %SIG% --text "%Q%" --k 5 --max_items 5 --max_bytes 65536 --verbose
endlocal
