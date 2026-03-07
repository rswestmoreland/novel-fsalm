#!/usr/bin/env bash
set -euo pipefail

# Novel FSA-LM demo: load-wikipedia (sharded) + global query snippet.
#
# This script uses the end-user command load-wikipedia, which performs:
#   ingest + build-index + reduce into a single root, and writes workspace defaults.
#
# Override knobs via env vars:
#   ROOT=... (default ./_tmp_reduce_index)
#   SHARDS=... (default 4)
#   KEEP_TMP=0|1 (default 0)
#   EXE=... (optional; default ./target/debug/fsa_lm)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

ROOT="${ROOT:-./_tmp_reduce_index}"
SHARDS="${SHARDS:-4}"
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
WS_OUT="${ROOT}/workspace_out.txt"

{
  printf "Ada Lovelace\tAda Lovelace was an English mathematician and writer.\n"
  printf "Alan Turing\tAlan Turing was a pioneering computer scientist.\n"
  printf "Grace Hopper\tGrace Hopper helped popularize compilers.\n"
  printf "Claude Shannon\tClaude Shannon founded information theory.\n"
} >"${DUMP}"

echo
echo "Loading Wikipedia (writes workspace defaults)..."
"${EXE}" load-wikipedia --root "${ROOT}" --dump "${DUMP}" --shards "${SHARDS}" --seg_mb 1 --row_kb 1 --chunk_rows 64 --max_docs 100

echo
echo "Workspace:"
"${EXE}" show-workspace --root "${ROOT}" | tee "${WS_OUT}"

MERGED_SNAP="$(grep '^merged_snapshot=' "${WS_OUT}" | head -n 1 | cut -d= -f2)"
MERGED_SIG="$(grep '^merged_sig_map=' "${WS_OUT}" | head -n 1 | cut -d= -f2)"

if [[ -z "${MERGED_SNAP}" || "${MERGED_SNAP}" == "MISSING" ]]; then
  echo "Failed to resolve merged_snapshot from workspace" >&2
  exit 1
fi
if [[ -z "${MERGED_SIG}" || "${MERGED_SIG}" == "MISSING" ]]; then
  echo "Failed to resolve merged_sig_map from workspace" >&2
  exit 1
fi

echo
echo "Global query snippet (uses workspace snapshot ids)..."
"${EXE}" query-index --root "${ROOT}" --snapshot "${MERGED_SNAP}" --sig-map "${MERGED_SIG}" --text "Ada Lovelace" --k 5

echo
echo "Done."
echo "Artifact store root: ${ROOT}"
