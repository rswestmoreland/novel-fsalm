#!/usr/bin/env bash
set -euo pipefail

# Novel FSA-LM demo: compare the default user surface and operator surface.
#
# This script uses a tiny local TSV fixture so the same prompt can be shown in:
#   1) default user mode
#   2) operator mode
#
# Override knobs via env vars:
#   ROOT=... (default ./_tmp_compare_presentation)
#   SHARDS=... (default 4)
#   KEEP_TMP=0|1 (default 0)
#   EXE=... (optional; default ./target/debug/fsa_lm)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

ROOT="${ROOT:-./_tmp_compare_presentation}"
SHARDS="${SHARDS:-4}"
KEEP_TMP="${KEEP_TMP:-0}"
EXE="${EXE:-${REPO_ROOT}/target/debug/fsa_lm}"
PROMPT="${PROMPT:-What is night?}"

if [[ ! -x "${EXE}" ]]; then
  echo "Building ${EXE}..."
  cargo build --quiet --bin fsa_lm
fi

if [[ "${KEEP_TMP}" == "0" ]]; then
  rm -rf "${ROOT}"
fi
mkdir -p "${ROOT}"

DUMP="${ROOT}/wiki_tiny.tsv"

printf "Night	Night is the period of darkness between sunset and sunrise.
Evening	Evening is the period near the end of the day.
" > "${DUMP}"

echo
echo "Loading Wikipedia (writes workspace defaults)..."
"${EXE}" load-wikipedia --root "${ROOT}" --dump "${DUMP}" --shards "${SHARDS}" --seg_mb 1 --row_kb 1 --chunk_rows 64 --max_docs 100

echo
echo "Workspace:"
"${EXE}" show-workspace --root "${ROOT}"

echo
echo "Same prompt in default user mode:"
"${EXE}" ask --root "${ROOT}" --k 20 "${PROMPT}"

echo
echo "Same prompt in operator mode:"
"${EXE}" ask --root "${ROOT}" --k 20 --presentation operator "${PROMPT}"

echo
echo "Done."
echo "Artifact store root: ${ROOT}"
