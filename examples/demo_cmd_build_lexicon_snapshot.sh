#!/usr/bin/env bash
set -euo pipefail

# Novel FSA-LM demo: load a tiny Wiktionary fixture and validate the resulting LexiconSnapshot.
#
# For end users, load-wiktionary is the preferred way to build a lexicon snapshot.
# This script also runs validate-lexicon-snapshot to confirm the snapshot is readable.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

ROOT="${ROOT:-./demo_db_lexicon_snapshot}"
EXE="${EXE:-${REPO_ROOT}/target/debug/fsa_lm}"
SEGMENTS="${SEGMENTS:-4}"

if [[ ! -x "${EXE}" ]]; then
  echo "Building ${EXE}..."
  cargo build --quiet --bin fsa_lm
fi

XML="${REPO_ROOT}/examples/wiktionary_tiny.xml"

rm -rf "${ROOT}"
mkdir -p "${ROOT}"

echo
echo "Loading Wiktionary (writes lexicon_snapshot into workspace defaults)..."
"${EXE}" load-wiktionary --root "${ROOT}" --xml "${XML}" --segments "${SEGMENTS}" --max_pages 10

WS_OUT="${ROOT}/workspace_out.txt"
"${EXE}" show-workspace --root "${ROOT}" | tee "${WS_OUT}"

LEX="$(grep '^lexicon_snapshot=' "${WS_OUT}" | head -n 1 | cut -d= -f2)"
if [[ -z "${LEX}" || "${LEX}" == "MISSING" ]]; then
  echo "Failed to resolve lexicon_snapshot from workspace" >&2
  exit 1
fi

echo
echo "Validating LexiconSnapshotV1 ${LEX}..."
"${EXE}" validate-lexicon-snapshot --root "${ROOT}" --snapshot "${LEX}"
