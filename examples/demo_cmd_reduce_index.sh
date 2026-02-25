#!/usr/bin/env bash
set -euo pipefail

# Novel FSA-LM demo: sharded ingest + build-index-sharded + reduce-index + global query snippet.
#
# Override knobs via env vars:
# ROOT=... (default./_tmp_reduce_index)
# SHARDS=... (default 4)
# KEEP_TMP=0|1 (default 0)
# EXE=... (optional; default./target/debug/fsa_lm)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

ROOT="${ROOT:-./_tmp_reduce_index}"
SHARDS="${SHARDS:-4}"
KEEP_TMP="${KEEP_TMP:-0}"
EXE="${EXE:-$REPO_ROOT/target/debug/fsa_lm}"

if [[ ! -x "$EXE" ]]; then
 echo "Building $EXE..."
 cargo build --quiet --bin fsa_lm
fi

DUMP="$ROOT/wiki_tiny.tsv"
OUT1="$ROOT/manifest_ingest.txt"
OUT2="$ROOT/manifest_index.txt"
OUT3="$ROOT/reduce_out.txt"

if [[ "$KEEP_TMP" == "0" ]]; then
 rm -rf "$ROOT"
fi
mkdir -p "$ROOT"

{
 printf "Ada Lovelace	Ada Lovelace was an English mathematician and writer.
"
 printf "Alan Turing	Alan Turing was a pioneering computer scientist.
"
 printf "Grace Hopper	Grace Hopper helped popularize compilers.
"
 printf "Claude Shannon	Claude Shannon founded information theory.
"
} >"$DUMP"

echo
echo "Running sharded ingest..."
"$EXE" ingest-wiki-sharded --root "$ROOT" --dump "$DUMP" --shards "$SHARDS" --seg_mb 1 --row_kb 1 --chunk_rows 64 --max_docs 100 --out-file "$OUT1"
MANIFEST1="$(head -n 1 "$OUT1")"
echo "Ingest ShardManifestV1: $MANIFEST1"

echo
echo "Running sharded build-index..."
"$EXE" build-index-sharded --root "$ROOT" --shards "$SHARDS" --manifest "$MANIFEST1" --out-file "$OUT2"
MANIFEST2="$(head -n 1 "$OUT2")"
echo "Index ShardManifestV1: $MANIFEST2"

echo
echo "Running reduce-index (global merge into primary root)..."
"$EXE" reduce-index --root "$ROOT" --manifest "$MANIFEST2" --out-file "$OUT3"

REDUCE_MAN="$(sed -n '1p' "$OUT3")"
MERGED_SNAP="$(sed -n '2p' "$OUT3")"
MERGED_SIG="$(sed -n '3p' "$OUT3")"

echo "ReduceManifestV1: $REDUCE_MAN"
echo "Merged IndexSnapshotV1: $MERGED_SNAP"
echo "Merged IndexSigMapV1: $MERGED_SIG"

echo
echo "Global query snippet (primary root)..."
"$EXE" query-index --root "$ROOT" --snapshot "$MERGED_SNAP" --sig-map "$MERGED_SIG" --text "Ada Lovelace" --k 5

echo
echo "Done."
echo "Artifact store root: $ROOT"
