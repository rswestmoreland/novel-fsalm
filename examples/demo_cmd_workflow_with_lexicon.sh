#!/usr/bin/env bash
set -euo pipefail

# Novel FSA-LM demo: load Wikipedia TSV + Wiktionary XML into one root and ask.
#
# This script uses the end-user workflow:
#   load-wikipedia -> load-wiktionary -> show-workspace -> ask
#
# Override knobs via env vars:
#   ROOT=... (default ./_tmp_workflow_with_lexicon)
#   SHARDS=... (default 4)
#   SEGMENTS=... (default 4)
#   KEEP_TMP=0|1 (default 0)
#   EXE=... (optional; default ./target/debug/fsa_lm)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

ROOT="${ROOT:-./_tmp_workflow_with_lexicon}"
SHARDS="${SHARDS:-4}"
SEGMENTS="${SEGMENTS:-4}"
KEEP_TMP="${KEEP_TMP:-0}"
EXE="${EXE:-${REPO_ROOT}/target/debug/fsa_lm}"

if [[ ! -x "${EXE}" ]]; then
  echo "Building ${EXE}..."
  cargo build --quiet --bin fsa_lm
fi

if [[ "${KEEP_TMP}" == "0" ]]; then
  rm -rf "${ROOT}"
fi
mkdir -p "${ROOT}"

DUMP="${ROOT}/wiki_tiny.tsv"

printf "Night\tNight is the period of darkness.\nEvening\tEvening is the time near sunset.\n" > "${DUMP}"

echo
echo "Loading Wikipedia (writes workspace defaults)..."
"${EXE}" load-wikipedia --root "${ROOT}" --dump "${DUMP}" --shards "${SHARDS}" --seg_mb 1 --row_kb 1 --chunk_rows 64 --max_docs 100

echo
echo "Loading Wiktionary (writes lexicon_snapshot into workspace defaults)..."
"${EXE}" load-wiktionary --root "${ROOT}" --xml "${SCRIPT_DIR}/wiktionary_tiny.xml" --segments "${SEGMENTS}" --max_pages 100

echo
echo "Workspace:"
"${EXE}" show-workspace --root "${ROOT}"

echo
echo "Ask without query expansion..."
"${EXE}" ask --root "${ROOT}" --k 20 "Tell me about nights."

echo
echo "Ask with query expansion enabled..."
"${EXE}" ask --root "${ROOT}" --k 20 --expand "Tell me about nights."

echo
echo "Done."
echo "Artifact store root: ${ROOT}"
