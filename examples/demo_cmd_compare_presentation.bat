@echo off
REM Novel FSA-LM demo: compare the default user surface and operator surface.
REM
REM This script uses a tiny local TSV fixture so the same prompt can be shown in:
REM   1) default user mode
REM   2) operator mode
REM
REM Override knobs via environment variables before running:
REM set ROOT=... (default .\_tmp_compare_presentation)
REM set SHARDS=... (default 4)
REM set KEEP_TMP=0|1 (default 0)
REM set EXE=... (optional; default target\debug\fsa_lm.exe)
REM set PROMPT=... (optional; default What is night?)

setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
pushd "%SCRIPT_DIR%\.." >nul || (echo Failed to cd to repo root.& exit /b 1)

if not defined ROOT set "ROOT=.\_tmp_compare_presentation"
if not defined SHARDS set "SHARDS=4"
if not defined KEEP_TMP set "KEEP_TMP=0"
if not defined EXE set "EXE=target\debug\fsa_lm.exe"
if not defined PROMPT set "PROMPT=What is night?"

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

powershell -NoProfile -Command "$t=[char]9; $lines=@('Night'+$t+'Night is the period of darkness between sunset and sunrise.','Evening'+$t+'Evening is the period near the end of the day.'); Set-Content -Encoding Ascii -Path '%DUMP%' -Value $lines" >nul
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
"%EXE%" show-workspace --root "%ROOT%"
if errorlevel 1 goto:fail

echo.
echo Same prompt in default user mode:
"%EXE%" ask --root "%ROOT%" --k 20 "%PROMPT%"
if errorlevel 1 goto:fail

echo.
echo Same prompt in operator mode:
"%EXE%" ask --root "%ROOT%" --k 20 --presentation operator "%PROMPT%"
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
