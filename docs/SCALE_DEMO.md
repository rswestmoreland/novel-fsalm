# Scale Demo (Track C)

The scale demo is a deterministic workload runner for exercising the Novel FSA-LM pipeline on synthetic corpora. It is designed to be:

- CPU-only and deterministic.
- Content-addressed (hash-based) at every persistence boundary.
- Restartable at each stage using only prior artifact hashes.

## Runbook

See `docs/SCALE_DEMO_RUNBOOK.md` for a step-by-step workflow and deterministic regression guidance.

## Stages and artifacts

### Generate-only

Generates a deterministic workload (docs + queries) and returns a small summary report.

- Report: `ScaleDemoReportV1`
 - `workload_hash` is stable for identical workload parameters.

### Ingest frames

Writes the generated docs as immutable `FrameSegmentV1` artifacts using the existing ingest pipeline and produces a manifest.

- Report: `ScaleDemoFramesReportV1`
 - `frame_manifest_hash` (Wiki ingest manifest artifact)
 - counts (docs_total, rows_total, segments_total)

### Build index from manifest

Builds an `IndexSnapshotV1` over the ingested frame segments and emits signature gating artifacts.

- Report: `ScaleDemoIndexReportV1`
 - `index_snapshot_hash` (IndexSnapshotV1 artifact)
 - `index_sig_map_hash` (IndexSigMapV1 artifact)
 - counts

### Generate and store PromptPacks

Generates and stores one `PromptPack` per query.

- Report: `ScaleDemoPromptsReportV1`
 - `prompt_hashes` in ascending query_id order

This stage is wired to CLI via `scale-demo --prompts 1`.

### Build and store EvidenceBundle artifacts

Builds an `EvidenceBundleV1` per query deterministically by:

- Deriving query terms from query text.
- Running signature-gated search over the generated `IndexSnapshotV1`.
- Building a canonical `EvidenceBundleV1` (optionally attaching row sketches) and storing it as a content-addressed artifact.

- Report: `ScaleDemoEvidenceReportV1`
 - `evidence_hashes` in ascending query_id order

Implementation notes:
- The domain separator for the derived `query_id` uses a bytes literal with an escaped nul: `b"scale_demo_evidence_v1\0"`.
- Default retrieval uses `k=16` (top-k) and does not include metaphone expansion.
- Evidence build uses a bounded in-process `Cache2Q` for `FrameSegmentV1` decodes during sketch attachment.


### Build and store answers (planner + realizer)

Builds a deterministic answer output per query by:

- Loading each EvidenceBundleV1 from the artifact store.
- Running the guided Planner v1 to produce:
 - AnswerPlanV1
 - PlannerHintsV1
 - ForecastV1
- Running Realizer v1 to produce a deterministic text output.
- Applying the clarifying-question policy (when enabled by PlannerHintsV1).
- Storing the output bytes, PlannerHintsV1, ForecastV1, and MarkovTraceV1 as content-addressed artifacts.

- Report: `ScaleDemoAnswersReportV1` (printed as `scale_demo_answers_v3`)
 - `answer_hashes` in ascending query_id order
 - `planner_hints_hashes` in ascending query_id order
 - `forecast_hashes` in ascending query_id order
 - `markov_trace_hashes` in ascending query_id order

Implementation notes:
- Planner config uses `PlannerCfgV1::default_v1`.
- Realizer config uses `RealizerCfgV1::new`.
- Per-query lists are stored in ascending query_id order and must have equal length.

### Stable scale report artifact

Emits a final, stable summary report that captures:

- The workload and seed parameters.
- Counts (docs/queries).
- The ingest manifest hash.
- Optional stage hashes (index snapshot, prompt list, evidence list, planner_hints list, forecasts list, answers list).

- Artifact: `ScaleDemoScaleReportV1`
 - Stored as a content-addressed artifact.
 - Printed as the final output line when `--ingest 1` is set.

The scale report is designed to be deterministic and comparable across runs.
See `docs/SCALE_REPORT_V1.md` for the schema.

### Memory caps and regression pack

- Memory/cap guidance: `docs/SCALE_DEMO_MEMORY.md`
 - Includes optional environment overrides for evidence-stage caps.
- Regression pack: `docs/SCALE_DEMO_REGRESSION_PACK.md`
 - Defines a small fixed-seed workload and recommended locking strategies.


## CLI status

The `scale-demo` CLI supports running stages end-to-end by enabling flags:

- `--ingest 1` writes FrameSegment artifacts + ingest manifest
- `--build_index 1` builds IndexSnapshot + SegmentSig + IndexSigMap
- `--prompts 1` stores one PromptPack per query
- `--evidence 1` stores one EvidenceBundleV1 per query (requires `--build_index 1`)
- `--answer 1` stores one realized answer output per query (requires `--evidence 1`)

Example:

```
./fsa_lm scale-demo --root ./artifacts \
  --seed 1 --docs 64 --queries 64 \
  --min_doc_tokens 24 --max_doc_tokens 48 \
  --vocab 512 --query_tokens 6 --tie_pair 0 \
  --ingest 1 --build_index 1 --prompts 1 --evidence 1 --answer 1
```
