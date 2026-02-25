#!/usr/bin/env bash
set -euo pipefail

# demo: scale-demo full loop, run twice, and compare the final scale report line.
# Assumes you run this from the repo root where Cargo.toml is.

ROOT1=./demo_scale_run1
ROOT2=./demo_scale_run2
OUT1=$ROOT1/out.txt
OUT2=$ROOT2/out.txt

rm -rf "$ROOT1" "$ROOT2"
mkdir -p "$ROOT1" "$ROOT2"

# Optional: tune evidence-stage caps for the demo.
# export FSA_LM_SCALE_DEMO_EVIDENCE_K=16
# export FSA_LM_SCALE_DEMO_EVIDENCE_MAX_BYTES=65536
# export FSA_LM_SCALE_DEMO_EVIDENCE_FRAME_CACHE_BYTES=8388608

echo "Running scale-demo (run1)..."
cargo run --quiet --bin fsa_lm -- scale-demo \
 --seed 1 --docs 64 --queries 32 --min_doc_tokens 16 --max_doc_tokens 32 --vocab 1024 --query_tokens 4 --tie_pair 1 \
 --ingest 1 --build_index 1 --prompts 1 --evidence 1 --answer 1 \
 --root "$ROOT1" --out-file "$OUT1"

echo "Running scale-demo (run2)..."
cargo run --quiet --bin fsa_lm -- scale-demo \
 --seed 1 --docs 64 --queries 32 --min_doc_tokens 16 --max_doc_tokens 32 --vocab 1024 --query_tokens 4 --tie_pair 1 \
 --ingest 1 --build_index 1 --prompts 1 --evidence 1 --answer 1 \
 --root "$ROOT2" --out-file "$OUT2"

LINE1=$(grep '^scale_demo_scale_report_v1' "$OUT1")
LINE2=$(grep '^scale_demo_scale_report_v1' "$OUT2")

echo
echo "Run1: $LINE1"
echo "Run2: $LINE2"
echo

if [[ "$LINE1" == "$LINE2" ]]; then
 echo "OK: scale report lines match."
else
 echo "ERROR: scale report lines differ." >&2
 exit 1
fi
