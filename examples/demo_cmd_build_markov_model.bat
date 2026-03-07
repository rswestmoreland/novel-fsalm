@echo off
setlocal enableextensions enabledelayedexpansion

REM Novel FSA-LM demo: build a MarkovModelV1 from replay logs.
REM
REM This script is self-contained:
REM - load-wikipedia + load-wiktionary into a fresh root (workspace defaults)
REM - run a short non-interactive chat with --session-file + --autosave
REM - extract assistant replay ids from the ConversationPack
REM - build MarkovModelV1 from those replay logs

set "SCRIPT_DIR=%~dp0"
pushd "%SCRIPT_DIR%\.." >nul || (echo Failed to cd to repo root.& exit /b 1)

set "ROOT=%SCRIPT_DIR%..\_tmp_markov_model"
set "EXE=target\debug\fsa_lm.exe"

if exist "%ROOT%" rmdir /s /q "%ROOT%"
mkdir "%ROOT%" || goto:fail

if not exist "%EXE%" (
 echo Building %EXE%...
 cargo build --quiet --bin fsa_lm
 if errorlevel 1 goto:fail
)

set "XML_WIKI=examples\wiki_tiny.xml"
set "XML_WIKT=examples\wiktionary_tiny.xml"
set "SESSION_FILE=%ROOT%\session.txt"
set "CONV_OUT=%ROOT%\conversation.txt"
set "REPLAY_FILE=%ROOT%\replays_markov.txt"
set "CHAT_IN=%ROOT%\chat_in.txt"

echo.
echo Loading Wikipedia (writes workspace defaults)...
"%EXE%" load-wikipedia --root "%ROOT%" --xml "%XML_WIKI%" --shards 1 --seg_mb 1 --row_kb 1 --chunk_rows 64 --max_docs 25
if errorlevel 1 goto:fail

echo.
echo Loading Wiktionary (writes lexicon_snapshot into workspace defaults)...
"%EXE%" load-wiktionary --root "%ROOT%" --xml "%XML_WIKT%" --segments 2 --max_pages 50
if errorlevel 1 goto:fail

echo.
echo Creating a short chat session (writes %SESSION_FILE%)...
(
  echo Hello
  echo Tell me about Night.
  echo Tell me about Evening.
  echo /save
  echo /exit
) > "%CHAT_IN%"

type "%CHAT_IN%" | "%EXE%" chat --root "%ROOT%" --session-file "%SESSION_FILE%" --autosave --k 10 --expand
if errorlevel 1 goto:fail

if not exist "%SESSION_FILE%" (
 echo Missing session file after chat: %SESSION_FILE%
 goto:fail
)

set "CONV_HASH="
for /f "usebackq delims=" %%A in ("%SESSION_FILE%") do (
 set "CONV_HASH=%%A"
 goto:conv_done
)
:conv_done

if "%CONV_HASH%"=="" (
 echo Empty conversation hash in session file: %SESSION_FILE%
 goto:fail
)

echo ConversationPack: %CONV_HASH%

echo.
echo Extracting replay ids from ConversationPack...
"%EXE%" show-conversation --root "%ROOT%" %CONV_HASH% > "%CONV_OUT%"
if errorlevel 1 goto:fail

powershell -NoProfile -Command "Get-Content '%CONV_OUT%' | Where-Object { $_ -like 'msg.*.replay_id=*' } | ForEach-Object { $_.Split('=')[1] } | Where-Object { $_ -ne 'NONE' } | Sort-Object -Unique | Set-Content -Encoding Ascii -Path '%REPLAY_FILE%'" >nul

if not exist "%REPLAY_FILE%" (
 echo Missing replay file: %REPLAY_FILE%
 goto:fail
)
for %%F in ("%REPLAY_FILE%") do if %%~zF LSS 1 (
 echo Replay file is empty: %REPLAY_FILE%
 goto:fail
)

echo.
echo Building Markov model...
"%EXE%" build-markov-model --root "%ROOT%" --replay-file "%REPLAY_FILE%" --max-replays 1024 --max-traces 50000 --order 3 --max-next 8 --max-states 8192 --out-file "%ROOT%\markov_model.txt"
if errorlevel 1 goto:fail

echo.
type "%ROOT%\markov_model.txt"

popd >nul
endlocal
exit /b 0

:fail
popd >nul
endlocal
echo.
echo Script failed.
exit /b 1
