#!/usr/bin/env bash
set -euo pipefail

# Novel FSA-LM demo: load a tiny Wikipedia XML fixture, compact the index, then query before/after.
#
# This script avoids manual hash plumbing by reading snapshot ids from show-workspace.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

ROOT="${ROOT:-./demo_db_compact}"
EXE="${EXE:-${REPO_ROOT}/target/debug/fsa_lm}"

if [[ ! -x "${EXE}" ]]; then
  echo "Building ${EXE}..."
  cargo build --quiet --bin fsa_lm
fi

XML="${REPO_ROOT}/examples/wiki_tiny.xml"

rm -rf "${ROOT}"
mkdir -p "${ROOT}"

echo
echo "Loading Wikipedia (writes workspace defaults)..."
"${EXE}" load-wikipedia --root "${ROOT}" --xml "${XML}" --shards 1 --seg_mb 1 --row_kb 1 --chunk_rows 64 --max_docs 10

WS_OUT="${ROOT}/workspace_out.txt"
"${EXE}" show-workspace --root "${ROOT}" > "${WS_OUT}"
SNAP="$(grep '^merged_snapshot=' "${WS_OUT}" | head -n 1 | cut -d= -f2)"
SIG="$(grep '^merged_sig_map=' "${WS_OUT}" | head -n 1 | cut -d= -f2)"

if [[ -z "${SNAP}" || "${SNAP}" == "MISSING" ]]; then
  echo "Failed to resolve merged_snapshot from workspace" >&2
  exit 1
fi

echo "Snapshot(before): ${SNAP}"

Q="banana bread recipe"

echo
echo "Query(before):"
"${EXE}" query-index --root "${ROOT}" --snapshot "${SNAP}" --sig-map "${SIG}" --text "${Q}" --k 5

echo
echo "Plan(dry-run):"
"${EXE}" compact-index --root "${ROOT}" --snapshot "${SNAP}" --target-bytes 1 --max-out-segments 1 --dry-run --verbose

# Compact the snapshot. The command prints the new snapshot hash to stdout.
echo
echo "Compacting..."
OUT_SNAP="$("${EXE}" compact-index --root "${ROOT}" --snapshot "${SNAP}" --target-bytes 1 --max-out-segments 1 --verbose)"
echo "Snapshot(after): ${OUT_SNAP}"

echo
echo "Query(after):"
"${EXE}" query-index --root "${ROOT}" --snapshot "${OUT_SNAP}" --text "${Q}" --k 5
