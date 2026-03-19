# Scale demo regression pack

This document defines a recommended, fixed-seed regression workload for `scale-demo`.

The scale demo pipeline is deterministic by design. A regression pack is a small, repeatable workload that:
- exercises the full pipeline (ingest -> index -> prompts -> evidence -> answer -> scale report),
- completes quickly,
- produces a single final line that can be locked and compared across builds.

## Regression pack v1

Pack v1 is defined by these CLI arguments:

- `--seed 7`
- `--docs 64`
- `--queries 16`
- `--min_doc_tokens 12`
- `--max_doc_tokens 24`
- `--vocab 512`
- `--query_tokens 6`
- `--tie_pair 1`
- `--ingest 1 --build_index 1 --prompts 1 --evidence 1 --answer 1`

The final scale report line (`scale_demo_scale_report_v3...`) includes these fields:
- `report=<hash>`: the stored ScaleDemoScaleReportV1 artifact hash
- `prompts_list_hash`, `evidence_list_hash`, `planner_hints_list_hash`, `forecasts_list_hash`, `markov_traces_list_hash`, `answers_list_hash`: ordered list summaries

For a regression check, comparing the full final line is recommended.

## How to run

Use a fresh root directory:

```
rm -rf ./artifacts_regress_v1

./fsa_lm scale-demo --root ./artifacts_regress_v1 \
  --seed 7 --docs 64 --queries 16 \
  --min_doc_tokens 12 --max_doc_tokens 24 \
  --vocab 512 --query_tokens 6 --tie_pair 1 \
  --ingest 1 --build_index 1 --prompts 1 --evidence 1 --answer 1 \
  --out-file ./scale_demo_regress_v1.txt
```

Repeat the same command, writing to `scale_demo_regress_v1_2.txt`, and verify the files are identical.

## Locking the pack

There are two common ways to lock the regression pack:

1) Commit the captured output file.
 - The `report=<hash>` field is the primary lock.
 - A CI step can run the command and compare the output file to the committed baseline.

2) Set an expected hash in CI.
 - The repository includes an integration test that can optionally compare the expected `report` field.
 - Set the environment variable `FSA_LM_REGRESSION_SCALE_DEMO_PACK_V3_REPORT_HEX` to the expected 32-byte hex.

If the env var is unset, the test still runs the pack and checks basic invariants.

## Memory notes

If a larger pack is needed, increase `--docs` and `--queries` gradually.
For memory tuning guidance and optional env overrides, see:
- docs/SCALE_DEMO_MEMORY.md
- docs/CACHES_V1.md
