@echo off
REM demo: PromptPack artifact + ReplayLog chain (cmd.exe).
REM Assumes you run this from the repo root where Cargo.toml is.

setlocal

set FSA_LM_STORE=%CD%\store
if not exist "%FSA_LM_STORE%" mkdir "%FSA_LM_STORE%"

echo Creating a PromptPack artifact...
for /f %%H in ('cargo run --quiet --bin fsa_lm -- prompt --seed 1 --max_tokens 64 --role user "Compute 17*19 exactly."') do set PROMPT_HASH=%%H
echo PromptPack hash: %PROMPT_HASH%

echo Creating a new ReplayLog...
for /f %%R in ('cargo run --quiet --bin fsa_lm -- replay-new') do set REPLAY_HASH=%%R
echo ReplayLog hash: %REPLAY_HASH%

echo Appending prompt step to ReplayLog...
for /f %%N in ('cargo run --quiet --bin fsa_lm -- replay-add-prompt %REPLAY_HASH% %PROMPT_HASH% --name prompt') do set REPLAY_HASH2=%%N
echo New ReplayLog hash: %REPLAY_HASH2%

echo Decoding ReplayLog...
cargo run --quiet --bin fsa_lm -- replay-decode %REPLAY_HASH2%

endlocal
