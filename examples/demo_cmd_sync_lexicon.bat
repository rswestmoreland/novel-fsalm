@echo off
REM Novel FSA-LM demo: replicate lexicon artifacts over TCP (LexiconSnapshotV1 + LexiconSegmentV1 closure).
REM
REM This uses load-wiktionary to generate the LexiconSnapshot deterministically.
REM
REM Override knobs via environment variables before running:
REM set SRC_ROOT=... (default .\_tmp_sync_lexicon_src)
REM set DST_ROOT=... (default .\_tmp_sync_lexicon_dst)
REM set PORT=... (default 47778)
REM set RW_TIMEOUT_MS=... (default 30000; 0 disables)
REM set KEEP_TMP=0|1 (default 0)
REM set EXE=... (optional; default target\debug\fsa_lm.exe)

setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
pushd "%SCRIPT_DIR%\.." >nul || (echo Failed to cd to repo root.& exit /b 1)

if not defined SRC_ROOT set "SRC_ROOT=.\_tmp_sync_lexicon_src"
if not defined DST_ROOT set "DST_ROOT=.\_tmp_sync_lexicon_dst"
if not defined PORT set "PORT=47778"
if not defined RW_TIMEOUT_MS set "RW_TIMEOUT_MS=30000"
if not defined KEEP_TMP set "KEEP_TMP=0"
if not defined EXE set "EXE=target\debug\fsa_lm.exe"

if not exist "%EXE%" (
 echo Building %EXE%...
 cargo build --quiet --bin fsa_lm
 if errorlevel 1 goto:fail
)
for %%I in ("%EXE%") do set "EXE_ABS=%%~fI"

set "WKT_XML=%SCRIPT_DIR%wiktionary_tiny.xml"
set "LOAD_OUT=%SRC_ROOT%\load_wiktionary_out.txt"
set "SERVER_LOG=%SRC_ROOT%\server.log"
set "PID_FILE=%SRC_ROOT%\server_pid.txt"
set "SYNC_OUT=%DST_ROOT%\sync_lexicon_out.txt"

if "%KEEP_TMP%"=="0" (
 if exist "%SRC_ROOT%" rmdir /S /Q "%SRC_ROOT%"
 if exist "%DST_ROOT%" rmdir /S /Q "%DST_ROOT%"
)
mkdir "%SRC_ROOT%" 2>nul
mkdir "%DST_ROOT%" 2>nul

if not exist "%WKT_XML%" (
 echo Missing fixture: %WKT_XML%
 goto:fail
)

echo.
echo Loading Wiktionary fixture into source root...
"%EXE%" load-wiktionary --root "%SRC_ROOT%" --xml "%WKT_XML%" --segments 4 --max_pages 100 --out-file "%LOAD_OUT%" >nul
if errorlevel 1 goto:fail

set "LEX_SNAP="
for /f "tokens=2 delims==" %%A in ('findstr /b "lexicon_snapshot=" "%LOAD_OUT%"') do set "LEX_SNAP=%%A"

if not defined LEX_SNAP (
 echo Failed to parse lexicon_snapshot from: %LOAD_OUT%
 type "%LOAD_OUT%"
 goto:fail
)

echo LexiconSnapshotV1: %LEX_SNAP%

set "ADDR=127.0.0.1:%PORT%"

echo.
echo Starting sync server (source) at %ADDR%...

powershell -NoProfile -Command "$p=Start-Process -PassThru -WindowStyle Hidden -FilePath '%EXE_ABS%' -ArgumentList @('serve-sync','--root','%SRC_ROOT%','--addr','%ADDR%','--rw_timeout_ms','%RW_TIMEOUT_MS%') -RedirectStandardOutput '%SERVER_LOG%' -RedirectStandardError '%SERVER_LOG%'; $p.Id" > "%PID_FILE%"
if errorlevel 1 goto:fail
set /p SERVER_PID=<"%PID_FILE%"

timeout /t 1 >nul

echo.
echo Syncing lexicon artifacts into destination root...
"%EXE%" sync-lexicon --root "%DST_ROOT%" --addr "%ADDR%" --lexicon-snapshot %LEX_SNAP% --rw_timeout_ms %RW_TIMEOUT_MS% --out-file "%SYNC_OUT%"
if errorlevel 1 goto:fail_stop

echo.
echo Validating snapshot in destination root...
"%EXE%" validate-lexicon-snapshot --root "%DST_ROOT%" --snapshot %LEX_SNAP%
if errorlevel 1 goto:fail_stop

echo.
echo Stopping sync server...
powershell -NoProfile -Command "Stop-Process -Id %SERVER_PID% -Force" >nul 2>nul

echo.
echo Done.
echo Source root: %SRC_ROOT%
echo Destination root: %DST_ROOT%

popd >nul
endlocal
exit /b 0

:fail_stop
echo.
echo Stopping sync server...
powershell -NoProfile -Command "Stop-Process -Id %SERVER_PID% -Force" >nul 2>nul

:fail
echo.
echo FAILED.
popd >nul
endlocal
exit /b 1
