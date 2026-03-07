@echo off
setlocal
REM Novel FSA-LM demo: load the tiny Wikipedia XML.bz2 fixture.
REM
REM This uses the end-user command load-wikipedia, which builds workspace defaults.

set "ROOT=./demo_db_xml_bz2"
set "XMLBZ2=./examples/wiki_tiny.xml.bz2"
set "EXE=target\debug\fsa_lm.exe"

if exist "%ROOT%" rmdir /s /q "%ROOT%"
mkdir "%ROOT%" || exit /b 1

if not exist "%EXE%" (
 echo Building %EXE%...
 cargo build --quiet --bin fsa_lm
 if errorlevel 1 exit /b 1
)

echo.
echo Loading Wikipedia XML.bz2 (writes workspace defaults)...
"%EXE%" load-wikipedia --root "%ROOT%" --xml-bz2 "%XMLBZ2%" --shards 1 --seg_mb 1 --row_kb 1 --chunk_rows 64 --max_docs 10
if errorlevel 1 exit /b 1

echo.
echo Workspace:
"%EXE%" show-workspace --root "%ROOT%"
if errorlevel 1 exit /b 1

endlocal
