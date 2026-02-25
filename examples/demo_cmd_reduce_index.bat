@echo off
REM Novel FSA-LM demo: sharded ingest + build-index-sharded + reduce-index + global query snippet.
REM
REM Override knobs via environment variables before running:
REM set ROOT=... (default.\_tmp_reduce_index)
REM set SHARDS=... (default 4)
REM set KEEP_TMP=0|1 (default 0)
REM set EXE=... (optional; default target\debug\fsa_lm.exe)

setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
pushd "%SCRIPT_DIR%\.." >nul || (echo Failed to cd to repo root.& exit /b 1)

if not defined ROOT set "ROOT=.\_tmp_reduce_index"
if not defined SHARDS set "SHARDS=4"
if not defined KEEP_TMP set "KEEP_TMP=0"
if not defined EXE set "EXE=target\debug\fsa_lm.exe"

if not exist "%EXE%" (
 echo Building %EXE%...
 cargo build --quiet --bin fsa_lm
 if errorlevel 1 goto:fail
)

set "DUMP=%ROOT%\wiki_tiny.tsv"
set "OUT1=%ROOT%\manifest_ingest.txt"
set "OUT2=%ROOT%\manifest_index.txt"
set "OUT3=%ROOT%\reduce_out.txt"

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
echo Running sharded ingest...
"%EXE%" ingest-wiki-sharded --root "%ROOT%" --dump "%DUMP%" --shards %SHARDS% --seg_mb 1 --row_kb 1 --chunk_rows 64 --max_docs 100 --out-file "%OUT1%"
if errorlevel 1 goto:fail
for /f "usebackq delims=" %%A in ("%OUT1%") do set "MANIFEST1=%%A"
echo Ingest ShardManifestV1: %MANIFEST1%

echo.
echo Running sharded build-index...
"%EXE%" build-index-sharded --root "%ROOT%" --shards %SHARDS% --manifest %MANIFEST1% --out-file "%OUT2%"
if errorlevel 1 goto:fail
for /f "usebackq delims=" %%A in ("%OUT2%") do set "MANIFEST2=%%A"
echo Index ShardManifestV1: %MANIFEST2%

echo.
echo Running reduce-index (global merge into primary root)...
"%EXE%" reduce-index --root "%ROOT%" --manifest %MANIFEST2% --out-file "%OUT3%"
if errorlevel 1 goto:fail

set I=0
for /f "usebackq delims=" %%A in ("%OUT3%") do (
 set /a I+=1
 if !I!==1 set "REDUCE_MAN=%%A"
 if !I!==2 set "MERGED_SNAP=%%A"
 if !I!==3 set "MERGED_SIG=%%A"
)

echo ReduceManifestV1: %REDUCE_MAN%
echo Merged IndexSnapshotV1: %MERGED_SNAP%
echo Merged IndexSigMapV1: %MERGED_SIG%

echo.
echo Global query snippet (primary root)...
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
