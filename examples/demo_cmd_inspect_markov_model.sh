#!/usr/bin/env bash
set -euo pipefail

# Novel FSA-LM demo: inspect a stored MarkovModelV1.
#
# Prereq:
# - Run examples/demo_cmd_build_markov_model.sh to produce:
#   $ROOT/markov_model.txt

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

ROOT="${ROOT:-${REPO_ROOT}/_tmp_markov_model}"
EXE="${EXE:-${REPO_ROOT}/target/debug/fsa_lm}"

if [[ ! -x "${EXE}" ]]; then
  echo "Building ${EXE}..."
  cargo build --quiet --bin fsa_lm
fi

if [[ ! -f "${ROOT}/markov_model.txt" ]]; then
  echo "Missing ${ROOT}/markov_model.txt. Run demo_cmd_build_markov_model.sh first." >&2
  exit 1
fi

MODEL_HASH="$(awk '{for(i=1;i<=NF;i++){if($i ~ /^model_hash=/){split($i,a,"="); print a[2]; exit}}}' "${ROOT}/markov_model.txt")"

if [[ -z "${MODEL_HASH}" ]]; then
  echo "Failed to extract model_hash from ${ROOT}/markov_model.txt" >&2
  exit 1
fi

echo "Inspecting Markov model ${MODEL_HASH}..."
"${EXE}" inspect-markov-model --root "${ROOT}" --model "${MODEL_HASH}" --top-states 5 --top-next 5 --out-file "${ROOT}/markov_model_inspect.txt"

echo
cat "${ROOT}/markov_model_inspect.txt"
