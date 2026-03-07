#!/usr/bin/env bash
set -euo pipefail

# Novel FSA-LM demo: build a MarkovModelV1 from replay logs.
#
# This script is self-contained:
# - load-wikipedia + load-wiktionary into a fresh root (workspace defaults)
# - run a short non-interactive chat with --session-file + --autosave
# - extract assistant replay ids from the ConversationPack
# - build MarkovModelV1 from those replay logs

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

ROOT="${ROOT:-${REPO_ROOT}/_tmp_markov_model}"
EXE="${EXE:-${REPO_ROOT}/target/debug/fsa_lm}"

if [[ ! -x "${EXE}" ]]; then
  echo "Building ${EXE}..."
  cargo build --quiet --bin fsa_lm
fi

rm -rf "${ROOT}"
mkdir -p "${ROOT}"

XML_WIKI="${REPO_ROOT}/examples/wiki_tiny.xml"
XML_WIKT="${REPO_ROOT}/examples/wiktionary_tiny.xml"
SESSION_FILE="${ROOT}/session.txt"
CONV_OUT="${ROOT}/conversation.txt"
REPLAY_FILE="${ROOT}/replays_markov.txt"

echo
echo "Loading Wikipedia (writes workspace defaults)..."
"${EXE}" load-wikipedia --root "${ROOT}" --xml "${XML_WIKI}" --shards 1 --seg_mb 1 --row_kb 1 --chunk_rows 64 --max_docs 25

echo
echo "Loading Wiktionary (writes lexicon_snapshot into workspace defaults)..."
"${EXE}" load-wiktionary --root "${ROOT}" --xml "${XML_WIKT}" --segments 2 --max_pages 50

echo
echo "Creating a short chat session (writes ${SESSION_FILE})..."
rm -f "${SESSION_FILE}"
{
  echo "Hello"
  echo "Tell me about Night."
  echo "Tell me about Evening."
  echo "/save"
  echo "/exit"
} | "${EXE}" chat --root "${ROOT}" --session-file "${SESSION_FILE}" --autosave --k 10 --expand

if [[ ! -f "${SESSION_FILE}" ]]; then
  echo "Missing session file after chat: ${SESSION_FILE}" >&2
  exit 1
fi

CONV_HASH="$(tr -d '\r\n' < "${SESSION_FILE}")"
if [[ -z "${CONV_HASH}" ]]; then
  echo "Empty conversation hash in session file: ${SESSION_FILE}" >&2
  exit 1
fi

echo "ConversationPack: ${CONV_HASH}"

echo
echo "Extracting replay ids from ConversationPack..."
"${EXE}" show-conversation --root "${ROOT}" "${CONV_HASH}" > "${CONV_OUT}"

grep '^msg\.[0-9][0-9]*\.replay_id=' "${CONV_OUT}" \
  | cut -d= -f2 \
  | grep -v '^NONE$' \
  | sort -u > "${REPLAY_FILE}"

if [[ ! -s "${REPLAY_FILE}" ]]; then
  echo "No replay ids found in ConversationPack; cannot build Markov model" >&2
  exit 1
fi

echo "Replay count: $(wc -l < "${REPLAY_FILE}")"

echo
echo "Building Markov model..."
"${EXE}" build-markov-model --root "${ROOT}" --replay-file "${REPLAY_FILE}" --max-replays 1024 --max-traces 50000 --order 3 --max-next 8 --max-states 8192 --out-file "${ROOT}/markov_model.txt"

echo
cat "${ROOT}/markov_model.txt"
