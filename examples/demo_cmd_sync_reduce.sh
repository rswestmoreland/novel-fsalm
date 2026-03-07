#!/usr/bin/env bash
set -euo pipefail

# Novel FSA-LM demo: artifact sync over TCP driven by ReduceManifestV1.
#
# This uses load-wikipedia to generate reduce artifacts deterministically.
#
# Override knobs via env vars:
#   SRC_ROOT=... (default ./_tmp_sync_src)
#   DST_ROOT=... (default ./_tmp_sync_dst)
#   SHARDS=... (default 4)
#   PORT=... (default 47777)
#   RW_TIMEOUT_MS=... (default 30000; 0 disables)
#   KEEP_TMP=0|1 (default 0)
#   EXE=... (optional; default ./target/debug/fsa_lm)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

SRC_ROOT="${SRC_ROOT:-./_tmp_sync_src}"
DST_ROOT="${DST_ROOT:-./_tmp_sync_dst}"
SHARDS="${SHARDS:-4}"
PORT="${PORT:-47777}"
RW_TIMEOUT_MS="${RW_TIMEOUT_MS:-30000}"
KEEP_TMP="${KEEP_TMP:-0}"
EXE="${EXE:-${REPO_ROOT}/target/debug/fsa_lm}"

if [[ ! -x "${EXE}" ]]; then
  echo "Building ${EXE}..."
  cargo build --quiet --bin fsa_lm
fi

DUMP="${SRC_ROOT}/wiki_tiny.tsv"
LOAD_OUT="${SRC_ROOT}/load_wikipedia_out.txt"
SERVER_LOG="${SRC_ROOT}/server.log"
SYNC_OUT="${DST_ROOT}/sync_out.txt"

if [[ "${KEEP_TMP}" == "0" ]]; then
  rm -rf "${SRC_ROOT}" "${DST_ROOT}"
fi
mkdir -p "${SRC_ROOT}" "${DST_ROOT}"

{
  printf "Ada Lovelace\tAda Lovelace was an English mathematician and writer.\n"
  printf "Alan Turing\tAlan Turing was a pioneering computer scientist.\n"
  printf "Grace Hopper\tGrace Hopper helped popularize compilers.\n"
  printf "Claude Shannon\tClaude Shannon founded information theory.\n"
} >"${DUMP}"

echo
echo "Loading Wikipedia (source; writes workspace defaults)..."
"${EXE}" load-wikipedia --root "${SRC_ROOT}" --dump "${DUMP}" --shards "${SHARDS}" --seg_mb 1 --row_kb 1 --chunk_rows 64 --max_docs 100 --out-file "${LOAD_OUT}" >/dev/null

REDUCE_MAN="$(grep '^reduce_manifest=' "${LOAD_OUT}" | head -n 1 | cut -d= -f2)"
MERGED_SNAP="$(grep '^merged_snapshot=' "${LOAD_OUT}" | head -n 1 | cut -d= -f2)"
MERGED_SIG="$(grep '^merged_sig_map=' "${LOAD_OUT}" | head -n 1 | cut -d= -f2)"

if [[ -z "${REDUCE_MAN}" ]]; then
  echo "Failed to resolve reduce_manifest from ${LOAD_OUT}" >&2
  exit 1
fi

echo "ReduceManifestV1: ${REDUCE_MAN}"
echo "Merged IndexSnapshotV1: ${MERGED_SNAP}"
echo "Merged IndexSigMapV1: ${MERGED_SIG}"

ADDR="127.0.0.1:${PORT}"

echo
echo "Starting sync server (source) at ${ADDR}..."
"${EXE}" serve-sync --root "${SRC_ROOT}" --addr "${ADDR}" --rw_timeout_ms "${RW_TIMEOUT_MS}" >"${SERVER_LOG}" 2>&1 &
SERVER_PID=$!
trap 'kill "${SERVER_PID}" >/dev/null 2>&1 || true; wait "${SERVER_PID}" >/dev/null 2>&1 || true' EXIT

sleep 1

echo
echo "Syncing reduce outputs into destination root..."
"${EXE}" sync-reduce --root "${DST_ROOT}" --addr "${ADDR}" --reduce-manifest "${REDUCE_MAN}" --rw_timeout_ms "${RW_TIMEOUT_MS}" --out-file "${SYNC_OUT}"
echo "Sync stats:"; cat "${SYNC_OUT}"

echo
echo "Stopping sync server..."
kill "${SERVER_PID}" >/dev/null 2>&1 || true
wait "${SERVER_PID}" >/dev/null 2>&1 || true
trap - EXIT

echo
echo "Global query snippet (destination root)..."
"${EXE}" query-index --root "${DST_ROOT}" --snapshot "${MERGED_SNAP}" --sig-map "${MERGED_SIG}" --text "Ada Lovelace" --k 5

echo
echo "Done."
echo "Source root: ${SRC_ROOT}"
echo "Destination root: ${DST_ROOT}"
