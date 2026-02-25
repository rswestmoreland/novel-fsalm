#!/usr/bin/env bash
set -euo pipefail

# Enforce warning-free builds across all targets.
# This is intended for local verification and CI.

export RUSTFLAGS="${RUSTFLAGS:-} -Dwarnings"

cargo test --all-targets
