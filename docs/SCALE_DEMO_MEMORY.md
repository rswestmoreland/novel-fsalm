# Scale Demo Memory Caps 

This document describes memory-related knobs for the `scale-demo` pipeline.

Goals:
- Keep scale demo runs usable on a consumer laptop (for example 16 GB RAM).
- Preserve determinism: same inputs produce the same artifact hashes.
- Keep caches bounded and optional (correctness does not depend on cache hits).

Non-goals:
- Benchmarking. Timing is intentionally not part of the canonical artifacts.

## Workload size knobs

These knobs change the amount of synthetic data generated. They have the largest impact on total runtime and total stored artifact volume.

- `--docs <n>`: number of generated documents.
- `--queries <n>`: number of generated queries.
- `--min_doc_tokens <n>`, `--max_doc_tokens <n>`: document length bounds.
- `--vocab <n>`: vocabulary size.
- `--query_tokens <n>`: query length.

Guidance:
- Start small (tens of docs, single-digit queries) when validating a new stage.
- Increase docs first to exercise ingestion/indexing.
- Increase queries to exercise retrieval/evidence/answer loops.

## Evidence-stage knobs

Evidence construction has two types of memory controls:

1) Per-bundle output budgets.
2) Bounded in-process caches used to avoid re-decoding hot FrameSegments.

### Per-bundle budgets

These affect EvidenceBundle contents and therefore change output hashes.

- `SCALE_DEMO_EVIDENCE_K` (default: 16)
 - Controls the retrieval top-k per query.
- `SCALE_DEMO_EVIDENCE_MAX_BYTES` (default: 64 KiB)
 - Controls the encoded size budget for a single EvidenceBundle.

### Bounded frame decode cache

This cache affects performance only. It should not affect outputs.

- `SCALE_DEMO_EVIDENCE_FRAME_CACHE_BYTES` (default: 8 MiB)
 - Size budget for the in-process 2Q cache that holds decoded FrameSegmentV1 values
 while attaching sketches during evidence building.

## Environment overrides for scale demo

The scale demo library accepts optional environment variables for evidence-stage caps.
These are intended for large runs and memory experiments.

- `FSA_LM_SCALE_DEMO_EVIDENCE_K`
- `FSA_LM_SCALE_DEMO_EVIDENCE_MAX_BYTES`
- `FSA_LM_SCALE_DEMO_EVIDENCE_FRAME_CACHE_BYTES`

Rules:
- If an env value is missing or parses to 0, the default constant is used.
- Values are clamped for safety:
 - `EVIDENCE_K` clamps to 4096 max.
 - `EVIDENCE_MAX_BYTES` clamps to 1 MiB max.

Example:

```
set FSA_LM_SCALE_DEMO_EVIDENCE_FRAME_CACHE_BYTES=1048576
set FSA_LM_SCALE_DEMO_EVIDENCE_MAX_BYTES=32768

./fsa_lm scale-demo --root ./artifacts_demo \
  --seed 1 --docs 256 --queries 128 \
  --min_doc_tokens 24 --max_doc_tokens 48 \
  --vocab 2048 --query_tokens 6 --tie_pair 0 \
  --ingest 1 --build_index 1 --prompts 1 --evidence 1 --answer 1
```

## Determinism notes

- Output hashes are determined by the generated workload (seed + workload cfg) and the pipeline logic.
- Cache sizing changes should not change outputs, but can change runtime.
- Changing `EVIDENCE_K` or `EVIDENCE_MAX_BYTES` changes the retrieved set and the EvidenceBundle truncation behavior, and therefore changes hashes.

Related:
- docs/CACHES_V1.md
- docs/SCALE_DEMO_RUNBOOK.md
