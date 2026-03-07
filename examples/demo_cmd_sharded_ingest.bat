@echo off
REM Novel FSA-LM demo: load-wikipedia with --shards, then query using workspace defaults.
REM
REM load-wikipedia performs ingest + build-index + reduce into a single root.
REM
REM Override knobs via environment variables before running:
REM set ROOT=... (default .\_tmp_sharded_ingest)
REM set SHARDS=... (default 4)
REM set KEEP_TMP=0|1 (default 0)
REM set EXE=... (optional; default target\debug\fsa_lm.exe)

setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
pushd "%SCRIPT_DIR%\.." >nul || (echo Failed to cd to repo root.& exit /b 1)

if not defined ROOT set "ROOT=.\_tmp_sharded_ingest"
if not defined SHARDS set "SHARDS=4"
if not defined KEEP_TMP set "KEEP_TMP=0"
if not defined EXE set "EXE=target\debug\fsa_lm.exe"

if not exist "%EXE%" (
 echo Building %EXE%...
 cargo build --quiet --bin fsa_lm
 if errorlevel 1 goto:fail
)

set "DUMP=%ROOT%\wiki_tiny.tsv"
set "WS_OUT=%ROOT%\workspace_out.txt"

if "%KEEP_TMP%"=="0" (
 if exist "%ROOT%" rmdir /S /Q "%ROOT%"
)
mkdir "%ROOT%" 2>nul

powershell -NoProfile -Command "$t=[char]9; $lines=@('Ada Lovelace'+$t+'Ada Lovelace was an English mathematician and writer.','Alan Turing'+$t+'Alan Turing was a pioneering computer scientist.','Grace Hopper'+$t+'Grace Hopper helped popularize compilers.','Claude Shannon'+$t+'Claude Shannon founded information theory.'); Set-Content -Encoding Ascii -Path '%DUMP%' -Value $lines" >nul
if not exist "%DUMP%" (
 echo Failed to create dump file: %DUMP%
 goto:fail
)

echo.
echo Loading Wikipedia (writes workspace defaults)...
"%EXE%" load-wikipedia --root "%ROOT%" --dump "%DUMP%" --shards %SHARDS% --seg_mb 1 --row_kb 1 --chunk_rows 64 --max_docs 100
if errorlevel 1 goto:fail

echo.
echo Workspace:
"%EXE%" show-workspace --root "%ROOT%" > "%WS_OUT%"
if errorlevel 1 goto:fail

type "%WS_OUT%"

set "MERGED_SNAP="
set "MERGED_SIG="
for /f "usebackq tokens=1,2 delims==" %%A in ("%WS_OUT%") do (
 if "%%A"=="merged_snapshot" set "MERGED_SNAP=%%B"
 if "%%A"=="merged_sig_map" set "MERGED_SIG=%%B"
)

if not defined MERGED_SNAP (
 echo Failed to resolve merged_snapshot from workspace
 goto:fail
)
if "%MERGED_SNAP%"=="MISSING" (
 echo Failed to resolve merged_snapshot from workspace
 goto:fail
)
if not defined MERGED_SIG (
 echo Failed to resolve merged_sig_map from workspace
 goto:fail
)
if "%MERGED_SIG%"=="MISSING" (
 echo Failed to resolve merged_sig_map from workspace
 goto:fail
)

echo.
echo Query snippet (uses workspace snapshot ids)...
"%EXE%" query-index --root "%ROOT%" --snapshot %MERGED_SNAP% --sig-map %MERGED_SIG% --text "Ada Lovelace" --k 5
if errorlevel 1 goto:fail

echo.
echo Done.
echo Artifact store root: %ROOT%

popd >nul
endlocal
exit /b 0

:fail
popd >nul
endlocal
echo.
echo Script failed.
exit /b 1
