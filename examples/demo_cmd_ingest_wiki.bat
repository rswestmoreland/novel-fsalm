@echo off
REM Novel FSA-LM demo: load a tiny Wikipedia TSV file.
REM
REM This uses the end-user command load-wikipedia, which builds workspace defaults.

setlocal

set "ROOT=.\demo_db"
set "DUMP=.\examples\wiki_tiny.tsv"
set "EXE=target\debug\fsa_lm.exe"

if exist "%ROOT%" rmdir /S /Q "%ROOT%"
mkdir "%ROOT%" || exit /b 1

if not exist "%EXE%" (
 echo Building %EXE%...
 cargo build --quiet --bin fsa_lm
 if errorlevel 1 exit /b 1
)

REM Create a tiny TSV with two "documents".
powershell -NoProfile -Command "$t=[char]9; $lines=@('Ada Lovelace'+$t+'Ada Lovelace was an English mathematician and writer.','Alan Turing'+$t+'Alan Turing was a pioneering computer scientist.'); Set-Content -Encoding Ascii -Path '%DUMP%' -Value $lines" >nul

echo.
echo Loading Wikipedia TSV (writes workspace defaults)...
"%EXE%" load-wikipedia --root "%ROOT%" --dump "%DUMP%" --shards 1 --seg_mb 1 --row_kb 1 --chunk_rows 64 --max_docs 10
if errorlevel 1 exit /b 1

echo.
echo Workspace:
"%EXE%" show-workspace --root "%ROOT%"
if errorlevel 1 exit /b 1

echo.
echo Ask:
"%EXE%" ask --root "%ROOT%" "Tell me about Ada Lovelace."
if errorlevel 1 exit /b 1

echo.
echo Done. Artifact store root: %ROOT%
endlocal
