#!/usr/bin/env bash
set -euo pipefail

#: golden pack conversation v1 (bundles golden-pack + turn-pairs)

ROOT="$(cd "$(dirname "$0")" && pwd)/../_tmp_golden_pack_conversation_v1"
ROOT2="$(cd "$(dirname "$0")" && pwd)/../_tmp_golden_pack_conversation_v1_2"

rm -rf "$ROOT" "$ROOT2"
mkdir -p "$ROOT" "$ROOT2"

echo "Running golden-pack-conversation..."
cargo run --quiet --release --bin fsa_lm -- golden-pack-conversation --root "$ROOT"

echo
echo "Run again and compare report hashes..."

LINE1=$(cargo run --quiet --release --bin fsa_lm -- golden-pack-conversation --root "$ROOT")
LINE2=$(cargo run --quiet --release --bin fsa_lm -- golden-pack-conversation --root "$ROOT2")

echo "First: $LINE1"
echo "Second: $LINE2"
echo
echo "If the two lines match, the run is deterministic."
