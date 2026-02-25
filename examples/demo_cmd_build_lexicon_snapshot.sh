#!/usr/bin/env bash
set -euo pipefail

# demo: build LexiconSnapshot from LexiconSegment hashes.
#
# This command expects LexiconSegmentV1 artifacts to already exist in the store.
# (Wiktionary ingest) will produce those segments.

ROOT="./fsa_lm_store"

# Replace these with real LexiconSegment hashes.
SEG1="0000000000000000000000000000000000000000000000000000000000000000"
SEG2="1111111111111111111111111111111111111111111111111111111111111111"

fsa_lm build-lexicon-snapshot --root "$ROOT" --segment "$SEG1" --segment "$SEG2"