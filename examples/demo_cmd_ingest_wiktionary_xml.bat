@echo off
setlocal enabledelayedexpansion

REM Novel FSA-LM demo: load the tiny Wiktionary XML fixture in either plain or .bz2 form.
REM
REM This uses the end-user command load-wiktionary, which writes lexicon_snapshot into
REM workspace defaults.

set "ROOT=./demo_db_wiktionary"
set "XML=./examples/wiktionary_tiny.xml"
set "XMLBZ2=./examples/wiktionary_tiny.xml.bz2"
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
echo Running: load-wiktionary (plain XML)...
"%EXE%" load-wiktionary --root "%ROOT%" --xml "%XML%" --segments %SEGMENTS% --max_pages 10
if errorlevel 1 exit /b 1
"%EXE%" show-workspace --root "%ROOT%"
if errorlevel 1 exit /b 1

echo.
echo Running: load-wiktionary (bz2)...
"%EXE%" load-wiktionary --root "%ROOT%" --xml-bz2 "%XMLBZ2%" --segments %SEGMENTS% --max_pages 10
if errorlevel 1 exit /b 1
"%EXE%" show-workspace --root "%ROOT%"
if errorlevel 1 exit /b 1

endlocal
