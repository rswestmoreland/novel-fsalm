#!/usr/bin/env bash
set -euo pipefail

#: build a MarkovModelV1 from replay logs.
#
# Prereq:
# - Create a text file containing one ReplayLog hash per line.
# Example: examples/replays_markov.txt
#
# The command will canonicalize inputs deterministically:
# - replay hashes: sort + dedup (+ optional --max-replays truncation)
# - trace hashes: sort + dedup (+ optional --max-traces truncation)

ROOT="$(cd "$(dirname "$0")" && pwd)/../_tmp_markov_model"
rm -rf "$ROOT"
mkdir -p "$ROOT"

REPLAY_FILE="$(cd "$(dirname "$0")" && pwd)/replays_markov.txt"

echo "Building Markov model..."
cargo run --quiet --release --bin fsa_lm -- build-markov-model --root "$ROOT" --replay-file "$REPLAY_FILE" --max-replays 1024 --max-traces 50000 --order 3 --max-next 8 --max-states 8192 --out-file "$ROOT/markov_model.txt"

echo
cat "$ROOT/markov_model.txt"
