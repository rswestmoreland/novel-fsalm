#!/usr/bin/env bash
set -euo pipefail

# demo: ingest tiny wiki XML, build index snapshot, then build an EvidenceBundle.

ROOT=./demo_db_evidence
XML=./examples/wiki_tiny.xml

rm -rf "$ROOT"
mkdir -p "$ROOT"

# Ingest a tiny XML file into FrameSegment artifacts.
cargo run --bin fsa_lm -- ingest-wiki-xml --root "$ROOT" --xml "$XML" --seg_mb 1 --row_kb 1 --chunk_rows 64 --max_docs 10

# Build an IndexSnapshotV1 and capture its hash.
SNAP=$(cargo run --bin fsa_lm -- build-index --root "$ROOT")
echo "Snapshot: $SNAP"

# Build an EvidenceBundleV1 artifact and capture its hash.
Q="banana bread recipe"
EV=$(cargo run --bin fsa_lm -- build-evidence --root "$ROOT" --snapshot "$SNAP" --text "$Q" --k 5 --max_items 5 --max_bytes 65536 --verbose)
echo "EvidenceBundle: $EV"
