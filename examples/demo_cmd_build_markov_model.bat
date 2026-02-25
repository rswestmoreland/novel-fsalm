@echo off
setlocal enableextensions enabledelayedexpansion

REM: build a MarkovModelV1 from replay logs.
REM
REM Prereq:
REM - Create a text file containing one ReplayLog hash per line.
REM Example: examples\replays_markov.txt
REM
REM The command will canonicalize inputs deterministically:
REM - replay hashes: sort + dedup (+ optional --max-replays truncation)
REM - trace hashes: sort + dedup (+ optional --max-traces truncation)

set ROOT=%~dp0..\_tmp_markov_model
if exist "%ROOT%" rmdir /s /q "%ROOT%"
mkdir "%ROOT%" || exit /b 1

set REPLAY_FILE=%~dp0replays_markov.txt

echo Building Markov model...
cargo run --quiet --release --bin fsa_lm -- build-markov-model --root "%ROOT%" --replay-file "%REPLAY_FILE%" --max-replays 1024 --max-traces 50000 --order 3 --max-next 8 --max-states 8192 --out-file "%ROOT%\markov_model.txt" || exit /b 1

echo.
type "%ROOT%\markov_model.txt"
