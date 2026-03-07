#!/usr/bin/env bash
set -euo pipefail

# Novel FSA-LM demo: load the tiny Wikipedia XML fixture.
#
# This uses the end-user command load-wikipedia, which builds workspace defaults.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

ROOT="${ROOT:-./demo_db_xml}"
EXE="${EXE:-${REPO_ROOT}/target/debug/fsa_lm}"
XML="${REPO_ROOT}/examples/wiki_tiny.xml"

if [[ ! -x "${EXE}" ]]; then
  echo "Building ${EXE}..."
  cargo build --quiet --bin fsa_lm
fi

rm -rf "${ROOT}"
mkdir -p "${ROOT}"

echo
echo "Loading Wikipedia XML (writes workspace defaults)..."
"${EXE}" load-wikipedia --root "${ROOT}" --xml "${XML}" --shards 1 --seg_mb 1 --row_kb 1 --chunk_rows 64 --max_docs 10

echo
echo "Workspace:"
"${EXE}" show-workspace --root "${ROOT}"
