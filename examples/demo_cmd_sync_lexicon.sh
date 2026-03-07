#!/usr/bin/env bash
set -euo pipefail

# Novel FSA-LM demo: replicate lexicon artifacts over TCP (LexiconSnapshotV1 + LexiconSegmentV1 closure).
#
# This uses load-wiktionary to generate the LexiconSnapshot deterministically.
#
# Override knobs via env vars:
#   SRC_ROOT=... (default ./_tmp_sync_lexicon_src)
#   DST_ROOT=... (default ./_tmp_sync_lexicon_dst)
#   PORT=... (default 47778)
#   RW_TIMEOUT_MS=... (default 30000; 0 disables)
#   KEEP_TMP=0|1 (default 0)
#   EXE=... (optional; default ./target/debug/fsa_lm)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

SRC_ROOT="${SRC_ROOT:-./_tmp_sync_lexicon_src}"
DST_ROOT="${DST_ROOT:-./_tmp_sync_lexicon_dst}"
PORT="${PORT:-47778}"
RW_TIMEOUT_MS="${RW_TIMEOUT_MS:-30000}"
KEEP_TMP="${KEEP_TMP:-0}"
EXE="${EXE:-${REPO_ROOT}/target/debug/fsa_lm}"

if [[ ! -x "${EXE}" ]]; then
  echo "Building ${EXE}..."
  cargo build --quiet --bin fsa_lm
fi

WKT_XML="${SCRIPT_DIR}/wiktionary_tiny.xml"
WKT_OUT="${SRC_ROOT}/load_wiktionary_out.txt"
SERVER_LOG="${SRC_ROOT}/server.log"
SYNC_OUT="${DST_ROOT}/sync_lexicon_out.txt"

if [[ "${KEEP_TMP}" == "0" ]]; then
  rm -rf "${SRC_ROOT}" "${DST_ROOT}"
fi
mkdir -p "${SRC_ROOT}" "${DST_ROOT}"

if [[ ! -f "${WKT_XML}" ]]; then
  echo "Missing fixture: ${WKT_XML}" >&2
  exit 1
fi

echo
echo "Loading Wiktionary fixture into source root..."
"${EXE}" load-wiktionary --root "${SRC_ROOT}" --xml "${WKT_XML}" --segments 4 --max_pages 100 --out-file "${WKT_OUT}" >/dev/null

LEX_SNAP="$(grep -E '^lexicon_snapshot=' "${WKT_OUT}" | head -n 1 | cut -d= -f2)"
if [[ -z "${LEX_SNAP}" ]]; then
  echo "Failed to parse lexicon_snapshot from: ${WKT_OUT}" >&2
  cat "${WKT_OUT}" >&2
  exit 1
fi

echo "LexiconSnapshotV1: ${LEX_SNAP}"

ADDR="127.0.0.1:${PORT}"

echo
echo "Starting sync server (source) at ${ADDR}..."
"${EXE}" serve-sync --root "${SRC_ROOT}" --addr "${ADDR}" --rw_timeout_ms "${RW_TIMEOUT_MS}" >"${SERVER_LOG}" 2>&1 &
SERVER_PID=$!
trap 'kill "${SERVER_PID}" >/dev/null 2>&1 || true; wait "${SERVER_PID}" >/dev/null 2>&1 || true' EXIT

sleep 1

echo
echo "Syncing lexicon artifacts into destination root..."
"${EXE}" sync-lexicon --root "${DST_ROOT}" --addr "${ADDR}" --lexicon-snapshot "${LEX_SNAP}" --rw_timeout_ms "${RW_TIMEOUT_MS}" --out-file "${SYNC_OUT}"

echo
echo "Validating snapshot in destination root..."
"${EXE}" validate-lexicon-snapshot --root "${DST_ROOT}" --snapshot "${LEX_SNAP}"

echo
echo "Done."
echo "Source root: ${SRC_ROOT}"
echo "Destination root: ${DST_ROOT}"
