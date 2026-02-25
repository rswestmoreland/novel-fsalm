#!/usr/bin/env bash
set -euo pipefail

# demo: ingest tiny wiki XML, build an index snapshot, compact it, then query before/after.

ROOT=./demo_db_compact
XML=./examples/wiki_tiny.xml

rm -rf "$ROOT"
mkdir -p "$ROOT"

# Ingest a tiny XML file into FrameSegment artifacts.
cargo run --bin fsa_lm -- ingest-wiki-xml --root "$ROOT" --xml "$XML" --seg_mb 1 --row_kb 1 --chunk_rows 64 --max_docs 10

# Build an IndexSnapshotV1 and capture its hash.
SNAP=$(cargo run --bin fsa_lm -- build-index --root "$ROOT")
echo "Snapshot(before): $SNAP"

Q="banana bread recipe"

echo "Query(before):"
cargo run --bin fsa_lm -- query-index --root "$ROOT" --snapshot "$SNAP" --text "$Q" --k 5

echo "Plan(dry-run):"
cargo run --bin fsa_lm -- compact-index --root "$ROOT" --snapshot "$SNAP" --target-bytes 1 --max-out-segments 1 --dry-run --verbose

# Compact the snapshot. The command prints the new snapshot hash to stdout.
OUT=$(cargo run --bin fsa_lm -- compact-index --root "$ROOT" --snapshot "$SNAP" --target-bytes 1 --max-out-segments 1 --verbose)
echo "Snapshot(after): $OUT"

echo "Query(after):"
cargo run --bin fsa_lm -- query-index --root "$ROOT" --snapshot "$OUT" --text "$Q" --k 5
