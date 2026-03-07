@echo off
REM Novel FSA-LM demo: load Wikipedia TSV + Wiktionary XML into one root and ask.
REM
REM This script uses the end-user workflow:
REM   load-wikipedia -> load-wiktionary -> show-workspace -> ask
REM
REM Override knobs via environment variables before running:
REM set ROOT=... (default .\_tmp_workflow_with_lexicon)
REM set SHARDS=... (default 4)
REM set SEGMENTS=... (default 4)
REM set KEEP_TMP=0|1 (default 0)
REM set EXE=... (optional; default target\debug\fsa_lm.exe)

setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
pushd "%SCRIPT_DIR%\.." >nul || (echo Failed to cd to repo root.& exit /b 1)

if not defined ROOT set "ROOT=.\_tmp_workflow_with_lexicon"
if not defined SHARDS set "SHARDS=4"
if not defined SEGMENTS set "SEGMENTS=4"
if not defined KEEP_TMP set "KEEP_TMP=0"
if not defined EXE set "EXE=target\debug\fsa_lm.exe"

if not exist "%EXE%" (
 echo Building %EXE%...
 cargo build --quiet --bin fsa_lm
 if errorlevel 1 goto:fail
)

if "%KEEP_TMP%"=="0" (
 if exist "%ROOT%" rmdir /S /Q "%ROOT%"
)
mkdir "%ROOT%" 2>nul

set "DUMP=%ROOT%\wiki_tiny.tsv"

powershell -NoProfile -Command "$t=[char]9; $lines=@('Night'+$t+'Night is the period of darkness.','Evening'+$t+'Evening is the time near sunset.'); Set-Content -Encoding Ascii -Path '%DUMP%' -Value $lines" >nul
if not exist "%DUMP%" (
 echo Failed to create dump file: %DUMP%
 goto:fail
)

echo.
echo Loading Wikipedia (writes workspace defaults)...
"%EXE%" load-wikipedia --root "%ROOT%" --dump "%DUMP%" --shards %SHARDS% --seg_mb 1 --row_kb 1 --chunk_rows 64 --max_docs 100
if errorlevel 1 goto:fail

echo.
echo Loading Wiktionary (writes lexicon_snapshot into workspace defaults)...
"%EXE%" load-wiktionary --root "%ROOT%" --xml "%SCRIPT_DIR%wiktionary_tiny.xml" --segments %SEGMENTS% --max_pages 100
if errorlevel 1 goto:fail

echo.
echo Workspace:
"%EXE%" show-workspace --root "%ROOT%"
if errorlevel 1 goto:fail

echo.
echo Ask without query expansion...
"%EXE%" ask --root "%ROOT%" --k 20 "Tell me about nights."
if errorlevel 1 goto:fail

echo.
echo Ask with query expansion enabled...
"%EXE%" ask --root "%ROOT%" --k 20 --expand "Tell me about nights."
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
