@echo off
setlocal enableextensions enabledelayedexpansion

REM: inspect a stored MarkovModelV1.
REM
REM Prereq:
REM - Run examples\demo_cmd_build_markov_model.bat to produce:
REM %ROOT%\markov_model.txt

set ROOT=%~dp0..\_tmp_markov_model

if not exist "%ROOT%\markov_model.txt" (
 echo Missing %ROOT%\markov_model.txt. Run demo_cmd_build_markov_model.bat first.
 exit /b 1
)

REM Extract model_hash from the single-line summary.
set MODEL_HASH=
for /f "usebackq tokens=2 delims==" %%A in (`findstr /c:"model_hash=" "%ROOT%\markov_model.txt"`) do set TMP=%%A
for /f "tokens=1 delims= " %%B in ("%TMP%") do set MODEL_HASH=%%B

if "%MODEL_HASH%"=="" (
 echo Failed to extract model_hash from %ROOT%\markov_model.txt
 exit /b 1
)

echo Inspecting Markov model %MODEL_HASH%...
cargo run --quiet --release --bin fsa_lm -- inspect-markov-model --root "%ROOT%" --model "%MODEL_HASH%" --top-states 5 --top-next 5 --out-file "%ROOT%\markov_model_inspect.txt" || exit /b 1

echo.
type "%ROOT%\markov_model_inspect.txt"
