#!/usr/bin/env bash
set -euo pipefail

# Novel FSA-LM demo: load a tiny Wikipedia XML fixture, then build an EvidenceBundle.
#
# This script avoids manual hash plumbing by reading snapshot ids from show-workspace.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

ROOT="${ROOT:-./demo_db_evidence}"
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
if [[ -z "${SIG}" || "${SIG}" == "MISSING" ]]; then
  echo "Failed to resolve merged_sig_map from workspace" >&2
  exit 1
fi

echo "Snapshot: ${SNAP}"
echo "SigMap:   ${SIG}"

echo
echo "Building EvidenceBundle..."
Q="banana bread recipe"
"${EXE}" build-evidence --root "${ROOT}" --snapshot "${SNAP}" --sig-map "${SIG}" --text "${Q}" --k 5 --max_items 5 --max_bytes 65536 --verbose
