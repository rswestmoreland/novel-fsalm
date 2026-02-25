@echo off
setlocal enableextensions enabledelayedexpansion

REM: golden pack conversation v1 (bundles golden-pack + turn-pairs)

set ROOT=%~dp0..\_tmp_golden_pack_conversation_v1
if exist "%ROOT%" rmdir /s /q "%ROOT%"
mkdir "%ROOT%" || exit /b 1

echo Running golden-pack-conversation...
cargo run --quiet --release --bin fsa_lm -- golden-pack-conversation --root "%ROOT%" || exit /b 1

echo.
echo Run again and compare report hashes...

set ROOT2=%~dp0..\_tmp_golden_pack_conversation_v1_2
if exist "%ROOT2%" rmdir /s /q "%ROOT2%"
mkdir "%ROOT2%" || exit /b 1

for /f "usebackq delims=" %%L in (`cargo run --quiet --release --bin fsa_lm -- golden-pack-conversation --root "%ROOT%"`) do set LINE1=%%L
for /f "usebackq delims=" %%L in (`cargo run --quiet --release --bin fsa_lm -- golden-pack-conversation --root "%ROOT2%"`) do set LINE2=%%L

echo First: %LINE1%
echo Second: %LINE2%

echo.
echo If the two lines match, the run is deterministic.
