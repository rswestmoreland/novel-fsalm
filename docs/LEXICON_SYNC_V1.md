# Lexicon replication over artifact sync (V1)

This document defines a deterministic replication flow for lexicon artifacts
(LexiconSnapshotV1 and LexiconSegmentV1) between two artifact store roots.

Lexicon artifacts are used by query expansion (answer --expand --lexicon-snapshot ...).
Index replication (sync-reduce / sync-reduce-batch) copies only the reduce outputs
listed in ReduceManifestV1. Lexicon artifacts are separate and must be replicated
independently when a destination root is used for querying and answering.

The replication flow defined here reuses the existing artifact sync protocol
(serve-sync and client GET streaming). It does not introduce new server behavior.

## Artifacts

- LexiconSegmentV1: a partitioned segment of lexicon rows.
- LexiconSnapshotV1: an immutable snapshot that references one or more
  LexiconSegmentV1 artifacts by hash.

A lexicon snapshot fully determines the set of required segments.

## CLI surface

A dedicated client command is provided for operators:

sync-lexicon --root <dst_root> --addr <ip:port> --lexicon-snapshot <hash32hex> \
  [--rw_timeout_ms <n>] [--out-file <path>]

Where:
- --root is the destination artifact store root.
- --addr points to a running serve-sync instance for the source root.
- --lexicon-snapshot is the source LexiconSnapshotV1 hash to replicate.

The command prints a single stats line and optionally writes it to --out-file.

## Deterministic algorithm

Inputs:
- destination root (dst_root)
- source sync address (addr)
- lexicon snapshot hash (snap)

Steps:

1) Ensure the snapshot exists in dst_root.
   - If snap is missing locally, fetch it via artifact sync GET and store it.

2) Decode the snapshot bytes and extract the referenced segment hashes.
   - If decode fails, stop with an error.

3) Produce a stable list of required segments.
   - Sort segment hashes lexicographically (lowercase hex) and de-duplicate.

4) For each required segment hash in stable order:
   - If present in dst_root, count it as already_present.
   - Otherwise fetch it via artifact sync GET, store it, and count bytes.

5) The replicated snapshot and segments are now available in dst_root.

The algorithm must not depend on hash map iteration order and must produce a
stable fetch order across runs.

## Output

The command emits one deterministic stats line:

sync_lexicon_stats needed_total=<n> already_present=<n> fetched=<n> bytes_fetched=<n>

Where:
- needed_total counts snapshot + segments (unique artifacts)
- already_present counts artifacts that were already in dst_root
- fetched counts artifacts fetched via GET
- bytes_fetched is the sum of stored artifact byte lengths for fetched artifacts

## Failure modes

- Snapshot missing at source: fail with a clear message.
- Snapshot decode failure: fail (do not attempt best-effort).
- Segment missing at source: fail.
- Network errors / disconnect: fail. Partial artifacts must not be left behind.

## Notes

- This flow replicates only lexicon artifacts. It does not replicate index
  snapshots, signature maps, or other artifacts.
- If a destination root is used, lexicon replication should be run in addition
  to index replication.
