@echo off
setlocal enabledelayedexpansion

REM Enforce warning-free builds across all targets.
REM This is intended for local verification and CI.

if defined RUSTFLAGS (
  set "RUSTFLAGS=%RUSTFLAGS% -Dwarnings"
) else (
  set "RUSTFLAGS=-Dwarnings"
)

cargo test --all-targets
if errorlevel 1 exit /b 1

exit /b 0
