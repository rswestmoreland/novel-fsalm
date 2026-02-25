#!/usr/bin/env bash
set -euo pipefail

#: golden pack v1

ROOT="$(cd "$(dirname "$0")" && pwd)/../_tmp_golden_pack_v1"
ROOT2="$(cd "$(dirname "$0")" && pwd)/../_tmp_golden_pack_v1_2"

rm -rf "$ROOT" "$ROOT2"
mkdir -p "$ROOT" "$ROOT2"

echo "Running golden-pack..."
cargo run --quiet --release --bin fsa_lm -- golden-pack --root "$ROOT"

echo
echo "Run again and compare report hashes..."
LINE1=$(cargo run --quiet --release --bin fsa_lm -- golden-pack --root "$ROOT")
LINE2=$(cargo run --quiet --release --bin fsa_lm -- golden-pack --root "$ROOT2")

echo "First: $LINE1"
echo "Second: $LINE2"

echo
echo "If the two lines match, the run is deterministic."
