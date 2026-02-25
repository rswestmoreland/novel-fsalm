@echo off
REM demo: FrameSegment artifact persistence (cmd.exe).
REM Assumes you run this from the repo root where Cargo.toml is.

setlocal

set FSA_LM_STORE=%CD%\store
if not exist "%FSA_LM_STORE%" mkdir "%FSA_LM_STORE%"

echo Creating a FrameSegment artifact...
for /f %%H in ('cargo run --quiet --bin fsa_lm -- frame-seg-demo --root "%FSA_LM_STORE%" --chunk_rows 1 "Night falls. Knights ride."') do set SEG=%%H

echo Segment hash: %SEG%
echo.

echo Loading segment summary...
cargo run --quiet --bin fsa_lm -- frame-seg-show --root "%FSA_LM_STORE%" %SEG%

endlocal
