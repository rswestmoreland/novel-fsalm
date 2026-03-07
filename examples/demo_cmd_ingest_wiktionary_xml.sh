#!/usr/bin/env bash
set -euo pipefail

# Novel FSA-LM demo: load the tiny Wiktionary XML fixture in either plain or .bz2 form.
#
# This uses the end-user command load-wiktionary, which writes lexicon_snapshot into
# workspace defaults.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

ROOT="${ROOT:-./demo_db_wiktionary}"
EXE="${EXE:-${REPO_ROOT}/target/debug/fsa_lm}"
SEGMENTS="${SEGMENTS:-4}"

if [[ ! -x "${EXE}" ]]; then
  echo "Building ${EXE}..."
  cargo build --quiet --bin fsa_lm
fi

XML="${REPO_ROOT}/examples/wiktionary_tiny.xml"
XMLBZ2="${REPO_ROOT}/examples/wiktionary_tiny.xml.bz2"

rm -rf "${ROOT}"
mkdir -p "${ROOT}"

echo
echo "Running: load-wiktionary (plain XML)..."
"${EXE}" load-wiktionary --root "${ROOT}" --xml "${XML}" --segments "${SEGMENTS}" --max_pages 10
"${EXE}" show-workspace --root "${ROOT}"

echo
echo "Running: load-wiktionary (bz2)..."
"${EXE}" load-wiktionary --root "${ROOT}" --xml-bz2 "${XMLBZ2}" --segments "${SEGMENTS}" --max_pages 10
"${EXE}" show-workspace --root "${ROOT}"
