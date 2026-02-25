Warning-free builds
===================

This repo treats compiler warnings as build failures.

Why
---
Warnings tend to accumulate and hide real issues. Keeping the tree warning-free makes it easier to maintain determinism and correctness.

How to run locally
------------------

Windows:

- `tools\check_warnings.bat`

Linux/macOS/WSL:

- `tools/check_warnings.sh`

What it does
------------
The scripts run:

- `cargo test --all-targets`

with `RUSTFLAGS=-Dwarnings` so any warning becomes a hard failure.

CI note
-------
The GitHub Actions workflow runs the same policy (tests for all targets with warnings treated as errors).
