#!/usr/bin/env bash
set -euo pipefail

# Novel FSA-LM demo: load a tiny Wikipedia TSV file.
#
# This uses the end-user command load-wikipedia, which builds workspace defaults.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

ROOT="${ROOT:-./demo_db}"
EXE="${EXE:-${REPO_ROOT}/target/debug/fsa_lm}"
DUMP="${REPO_ROOT}/examples/wiki_tiny.tsv"

if [[ ! -x "${EXE}" ]]; then
  echo "Building ${EXE}..."
  cargo build --quiet --bin fsa_lm
fi

rm -rf "${ROOT}"
mkdir -p "${ROOT}"

{
  printf "Ada Lovelace\tAda Lovelace was an English mathematician and writer.\n"
  printf "Alan Turing\tAlan Turing was a pioneering computer scientist.\n"
} >"${DUMP}"

echo
echo "Loading Wikipedia TSV (writes workspace defaults)..."
"${EXE}" load-wikipedia --root "${ROOT}" --dump "${DUMP}" --shards 1 --seg_mb 1 --row_kb 1 --chunk_rows 64 --max_docs 10

echo
echo "Workspace:"
"${EXE}" show-workspace --root "${ROOT}"

echo
echo "Ask:"
"${EXE}" ask --root "${ROOT}" "Tell me about Ada Lovelace."

echo
echo "Done. Artifact store root: ${ROOT}"
