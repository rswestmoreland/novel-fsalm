@echo off
REM Novel FSA-LM demo: artifact sync over TCP driven by ReduceManifestV1.
REM
REM Override knobs via environment variables before running:
REM set SRC_ROOT=... (default.\_tmp_sync_src)
REM set DST_ROOT=... (default.\_tmp_sync_dst)
REM set SHARDS=... (default 4)
REM set PORT=... (default 47777)
REM set RW_TIMEOUT_MS=... (default 30000; 0 disables)
REM set KEEP_TMP=0|1 (default 0)
REM set EXE=... (optional; default target\debug\fsa_lm.exe)

setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
pushd "%SCRIPT_DIR%\.." >nul || (echo Failed to cd to repo root.& exit /b 1)

if not defined SRC_ROOT set "SRC_ROOT=.\_tmp_sync_src"
if not defined DST_ROOT set "DST_ROOT=.\_tmp_sync_dst"
if not defined SHARDS set "SHARDS=4"
if not defined PORT set "PORT=47777"
if not defined RW_TIMEOUT_MS set "RW_TIMEOUT_MS=30000"
if not defined KEEP_TMP set "KEEP_TMP=0"
if not defined EXE set "EXE=target\debug\fsa_lm.exe"

if not exist "%EXE%" (
 echo Building %EXE%...
 cargo build --quiet --bin fsa_lm
 if errorlevel 1 goto:fail
)
for %%I in ("%EXE%") do set "EXE_ABS=%%~fI"

set "DUMP=%SRC_ROOT%\wiki_tiny.tsv"
set "OUT1=%SRC_ROOT%\manifest_ingest.txt"
set "OUT2=%SRC_ROOT%\manifest_index.txt"
set "OUT3=%SRC_ROOT%\reduce_out.txt"
set "SERVER_LOG=%SRC_ROOT%\server.log"
set "PID_FILE=%SRC_ROOT%\server_pid.txt"
set "SYNC_OUT=%DST_ROOT%\sync_out.txt"

if "%KEEP_TMP%"=="0" (
 if exist "%SRC_ROOT%" rmdir /S /Q "%SRC_ROOT%"
 if exist "%DST_ROOT%" rmdir /S /Q "%DST_ROOT%"
)
mkdir "%SRC_ROOT%" 2>nul
mkdir "%DST_ROOT%" 2>nul

powershell -NoProfile -Command "$t=[char]9; $lines=@('Ada Lovelace'+$t+'Ada Lovelace was an English mathematician and writer.','Alan Turing'+$t+'Alan Turing was a pioneering computer scientist.','Grace Hopper'+$t+'Grace Hopper helped popularize compilers.','Claude Shannon'+$t+'Claude Shannon founded information theory.'); Set-Content -Encoding Ascii -Path '%DUMP%' -Value $lines" >nul
if not exist "%DUMP%" (
 echo Failed to create dump file: %DUMP%
 goto:fail
)

echo.
echo Running sharded ingest (source)...
"%EXE%" ingest-wiki-sharded --root "%SRC_ROOT%" --dump "%DUMP%" --shards %SHARDS% --seg_mb 1 --row_kb 1 --chunk_rows 64 --max_docs 100 --out-file "%OUT1%"
if errorlevel 1 goto:fail
for /f "usebackq delims=" %%A in ("%OUT1%") do set "MANIFEST1=%%A"
echo Ingest ShardManifestV1: %MANIFEST1%

echo.
echo Running sharded build-index (source)...
"%EXE%" build-index-sharded --root "%SRC_ROOT%" --shards %SHARDS% --manifest %MANIFEST1% --out-file "%OUT2%"
if errorlevel 1 goto:fail
for /f "usebackq delims=" %%A in ("%OUT2%") do set "MANIFEST2=%%A"
echo Index ShardManifestV1: %MANIFEST2%

echo.
echo Running reduce-index (source)...
"%EXE%" reduce-index --root "%SRC_ROOT%" --manifest %MANIFEST2% --out-file "%OUT3%"
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

set "ADDR=127.0.0.1:%PORT%"

echo.
echo Starting sync server (source) at %ADDR%...

powershell -NoProfile -Command "$p=Start-Process -PassThru -WindowStyle Hidden -FilePath '%EXE_ABS%' -ArgumentList @('serve-sync','--root','%SRC_ROOT%','--addr','%ADDR%','--rw_timeout_ms','%RW_TIMEOUT_MS%') -RedirectStandardOutput '%SERVER_LOG%' -RedirectStandardError '%SERVER_LOG%'; $p.Id" > "%PID_FILE%"
if errorlevel 1 goto:fail
set /p SERVER_PID=<"%PID_FILE%"

REM Give the server a moment to bind.
timeout /t 1 >nul

echo.
echo Syncing reduce outputs into destination root...
"%EXE%" sync-reduce --root "%DST_ROOT%" --addr "%ADDR%" --reduce-manifest %REDUCE_MAN% --rw_timeout_ms %RW_TIMEOUT_MS% --out-file "%SYNC_OUT%"
if errorlevel 1 goto:fail_stop

echo Sync stats:
type "%SYNC_OUT%"

echo.
echo Stopping sync server...
powershell -NoProfile -Command "Stop-Process -Id %SERVER_PID% -Force" >nul 2>nul

echo.
echo Global query snippet (destination root)...
"%EXE%" query-index --root "%DST_ROOT%" --snapshot %MERGED_SNAP% --sig-map %MERGED_SIG% --text "Ada Lovelace" --k 5
if errorlevel 1 goto:fail

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
popd >nul
endlocal
echo.
echo Script failed.
exit /b 1
