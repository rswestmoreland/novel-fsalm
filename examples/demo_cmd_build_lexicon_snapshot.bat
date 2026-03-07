@echo off
setlocal enabledelayedexpansion
REM Novel FSA-LM demo: load a tiny Wiktionary fixture and validate the resulting LexiconSnapshot.
REM
REM For end users, load-wiktionary is the preferred way to build a lexicon snapshot.
REM This script also runs validate-lexicon-snapshot to confirm the snapshot is readable.

set "ROOT=./demo_db_lexicon_snapshot"
set "XML=./examples/wiktionary_tiny.xml"
set "EXE=target\debug\fsa_lm.exe"
if not defined SEGMENTS set "SEGMENTS=4"

if exist "%ROOT%" rmdir /s /q "%ROOT%"
mkdir "%ROOT%" || exit /b 1

if not exist "%EXE%" (
 echo Building %EXE%...
 cargo build --quiet --bin fsa_lm
 if errorlevel 1 exit /b 1
)

echo.
echo Loading Wiktionary (writes lexicon_snapshot into workspace defaults)...
"%EXE%" load-wiktionary --root "%ROOT%" --xml "%XML%" --segments %SEGMENTS% --max_pages 10
if errorlevel 1 exit /b 1

set "WS_OUT=%ROOT%\workspace_out.txt"
"%EXE%" show-workspace --root "%ROOT%" > "%WS_OUT%"
if errorlevel 1 exit /b 1

type "%WS_OUT%"

set "LEX="
for /f "usebackq tokens=1,2 delims==" %%A in ("%WS_OUT%") do (
 if "%%A"=="lexicon_snapshot" set "LEX=%%B"
)

if not defined LEX (
 echo Failed to resolve lexicon_snapshot from workspace
 exit /b 1
)
if "%LEX%"=="MISSING" (
 echo Failed to resolve lexicon_snapshot from workspace
 exit /b 1
)

echo.
echo Validating LexiconSnapshotV1 %LEX%...
"%EXE%" validate-lexicon-snapshot --root "%ROOT%" --snapshot %LEX%
if errorlevel 1 exit /b 1

endlocal
