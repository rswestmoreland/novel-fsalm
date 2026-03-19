# Scale report v1

`ScaleDemoScaleReportV1` is a deterministic, content-addressed summary artifact for the `scale-demo` pipeline.

It is designed to be:

- Stable across runs with the same inputs.
- A single rollup record that points at stage artifacts by hash.
- Safe to print as the final line of `scale-demo` output (deterministic formatting).

## Where it lives

- Schema and formatting: `src/scale_report.rs`
- Artifact IO: `src/scale_report_artifact.rs`
- Builder: `src/scale_demo.rs` (`build_scale_demo_scale_report_v1`)
- CLI emission: `src/bin/fsa_lm.rs` (`scale-demo`)

## Schema versioning

The Rust type name remains `ScaleDemoScaleReportV1`.
The `version` field inside the artifact is currently `3`.

When the on-disk schema changes, the internal `version` increments.
The CLI output line prefix also changes (for example, `scale_demo_scale_report_v3`).

## Canonical fields (current)

Core identity:

- `version` (u16, must be 3)
- `workload_hash` (Hash32)
- `doc_count` (u32)
- `query_count` (u32)
- `tie_pair` (u8, 0 or 1)
- `seed` (u64)

Ingest identity:

- `frame_manifest_hash` (Hash32)
- `docs_total` (u64)
- `rows_total` (u64)
- `frame_segments_total` (u32)

Index stage (present when `has_index == 1`):

- `has_index` (u8)
- `index_snapshot_hash` (Hash32)
- `index_sig_map_hash` (Hash32)
- `index_segments_total` (u32)

Prompts stage (present when `has_prompts == 1`):

- `has_prompts` (u8)
- `prompts_max_output_tokens` (u32)
- `prompts` (HashListSummaryV1)

Evidence stage (present when `has_evidence == 1`):

- `has_evidence` (u8)
- `evidence_k` (u32)
- `evidence_max_bytes` (u32)
- `evidence` (HashListSummaryV1)

Answers stage (present when `has_answers == 1`):

- `has_answers` (u8)
- `planner_max_plan_items` (u32)
- `realizer_max_evidence_items` (u16)
- `realizer_max_terms_per_row` (u16)
- `realizer_load_frame_rows` (u8)
- `answers` (HashListSummaryV1)
- `planner_hints` (HashListSummaryV1)
- `forecasts` (HashListSummaryV1)
- `markov_traces` (HashListSummaryV1)

If a stage is absent (flag is 0), its fields are set to zero values (including the zero hash).

### HashListSummaryV1

`HashListSummaryV1` summarizes an ordered list of Hash32 values:

- `count` is the number of hashes in the list.
- `list_hash` is a deterministic digest over the ordered list.
- `first` and `last` are the first and last hashes (or zero when `count == 0`).

The `list_hash` is computed with a domain separator and an internal tag string, but the tag is not stored in the summary.
The builder selects the tag based on the list type (prompts, evidence, answers, planner_hints, forecasts).

## Deterministic CLI output

The CLI prints a final line beginning with `scale_demo_scale_report_v3` that includes:

- `report=<hash>` which is the content hash of the stored `ScaleDemoScaleReportV1` artifact.
- A stable set of `key=value` pairs summarizing the report.

The canonical artifact does not embed its own hash.
