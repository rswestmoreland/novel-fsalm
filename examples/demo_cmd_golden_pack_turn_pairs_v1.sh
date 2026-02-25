#!/usr/bin/env bash
set -euo pipefail

#: golden pack turn-pairs v1

ROOT="$(cd "$(dirname "$0")" && pwd)/../_tmp_golden_turn_pairs_v1"
rm -rf "$ROOT"
mkdir -p "$ROOT"

echo "Running golden-pack-turn-pairs..."
cargo run --quiet --release --bin fsa_lm -- golden-pack-turn-pairs --root "$ROOT"

echo
echo "Run again and compare report hashes..."

ROOT2="$(cd "$(dirname "$0")" && pwd)/../_tmp_golden_turn_pairs_v1_2"
rm -rf "$ROOT2"
mkdir -p "$ROOT2"

LINE1=$(cargo run --quiet --release --bin fsa_lm -- golden-pack-turn-pairs --root "$ROOT")
LINE2=$(cargo run --quiet --release --bin fsa_lm -- golden-pack-turn-pairs --root "$ROOT2")

echo "First: $LINE1"
echo "Second: $LINE2"

echo
echo "If the two lines match, the run is deterministic."
