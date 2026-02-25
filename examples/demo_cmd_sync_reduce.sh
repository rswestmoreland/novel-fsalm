#!/usr/bin/env bash
set -euo pipefail

# Novel FSA-LM demo: artifact sync over TCP driven by ReduceManifestV1.
#
# Override knobs via env vars:
# SRC_ROOT=... (default./_tmp_sync_src)
# DST_ROOT=... (default./_tmp_sync_dst)
# SHARDS=... (default 4)
# PORT=... (default 47777)
# RW_TIMEOUT_MS=... (default 30000; 0 disables)
# KEEP_TMP=0|1 (default 0)
# EXE=... (optional; default./target/debug/fsa_lm)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

SRC_ROOT="${SRC_ROOT:-./_tmp_sync_src}"
DST_ROOT="${DST_ROOT:-./_tmp_sync_dst}"
SHARDS="${SHARDS:-4}"
PORT="${PORT:-47777}"
RW_TIMEOUT_MS="${RW_TIMEOUT_MS:-30000}"
KEEP_TMP="${KEEP_TMP:-0}"
EXE="${EXE:-$REPO_ROOT/target/debug/fsa_lm}"

if [[ ! -x "$EXE" ]]; then
 echo "Building $EXE..."
 cargo build --quiet --bin fsa_lm
fi

DUMP="$SRC_ROOT/wiki_tiny.tsv"
OUT1="$SRC_ROOT/manifest_ingest.txt"
OUT2="$SRC_ROOT/manifest_index.txt"
OUT3="$SRC_ROOT/reduce_out.txt"
SERVER_LOG="$SRC_ROOT/server.log"
SYNC_OUT="$DST_ROOT/sync_out.txt"

if [[ "$KEEP_TMP" == "0" ]]; then
 rm -rf "$SRC_ROOT" "$DST_ROOT"
fi
mkdir -p "$SRC_ROOT" "$DST_ROOT"

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
echo "Running sharded ingest (source)..."
"$EXE" ingest-wiki-sharded --root "$SRC_ROOT" --dump "$DUMP" --shards "$SHARDS" --seg_mb 1 --row_kb 1 --chunk_rows 64 --max_docs 100 --out-file "$OUT1"
MANIFEST1="$(head -n 1 "$OUT1")"
echo "Ingest ShardManifestV1: $MANIFEST1"

echo
echo "Running sharded build-index (source)..."
"$EXE" build-index-sharded --root "$SRC_ROOT" --shards "$SHARDS" --manifest "$MANIFEST1" --out-file "$OUT2"
MANIFEST2="$(head -n 1 "$OUT2")"
echo "Index ShardManifestV1: $MANIFEST2"

echo
echo "Running reduce-index (source)..."
"$EXE" reduce-index --root "$SRC_ROOT" --manifest "$MANIFEST2" --out-file "$OUT3"

REDUCE_MAN="$(sed -n '1p' "$OUT3")"
MERGED_SNAP="$(sed -n '2p' "$OUT3")"
MERGED_SIG="$(sed -n '3p' "$OUT3")"

echo "ReduceManifestV1: $REDUCE_MAN"
echo "Merged IndexSnapshotV1: $MERGED_SNAP"
echo "Merged IndexSigMapV1: $MERGED_SIG"

ADDR="127.0.0.1:$PORT"

echo
echo "Starting sync server (source) at $ADDR..."
"$EXE" serve-sync --root "$SRC_ROOT" --addr "$ADDR" --rw_timeout_ms "$RW_TIMEOUT_MS" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
trap 'kill "$SERVER_PID" >/dev/null 2>&1 || true; wait "$SERVER_PID" >/dev/null 2>&1 || true' EXIT

sleep 1

echo
echo "Syncing reduce outputs into destination root..."
"$EXE" sync-reduce --root "$DST_ROOT" --addr "$ADDR" --reduce-manifest "$REDUCE_MAN" --rw_timeout_ms "$RW_TIMEOUT_MS" --out-file "$SYNC_OUT"
echo "Sync stats:"; cat "$SYNC_OUT"

echo
echo "Stopping sync server..."
kill "$SERVER_PID" >/dev/null 2>&1 || true
wait "$SERVER_PID" >/dev/null 2>&1 || true
trap - EXIT

echo
echo "Global query snippet (destination root)..."
"$EXE" query-index --root "$DST_ROOT" --snapshot "$MERGED_SNAP" --sig-map "$MERGED_SIG" --text "Ada Lovelace" --k 5

echo
echo "Done."
echo "Source root: $SRC_ROOT"
echo "Destination root: $DST_ROOT"
