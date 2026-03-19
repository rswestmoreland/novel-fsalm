# Scale demo runbook

This runbook documents how to use the `scale-demo` command to exercise the end-to-end Novel pipeline on a deterministic synthetic workload.

Goals:
- Produce a stable, comparable run summary (the final scale report line).
- Validate that retrieval, evidence, and answer stages remain deterministic.
- Provide a repeatable regression workflow on a consumer laptop.

Non-goals:
- Benchmarking guidance (timing is optional and not included in the canonical artifacts).


## Quick start

Use a fresh artifact root for a clean run:

Examples:
- examples/demo_cmd_scale_demo_full_loop.bat
- examples/demo_cmd_scale_demo_full_loop.sh

```
rm -rf ./artifacts_demo

./fsa_lm scale-demo --root ./artifacts_demo \
  --seed 1 --docs 64 --queries 64 \
  --min_doc_tokens 24 --max_doc_tokens 48 \
  --vocab 512 --query_tokens 6 --tie_pair 0 \
  --ingest 1 --build_index 1 --prompts 1 --evidence 1 --answer 1
```

This prints multiple lines, one per stage, ending with a final line beginning with:

- `scale_demo_scale_report_v3...`

That final line includes `report=<hash>` which is the content hash of the stored `ScaleDemoScaleReportV1` artifact.


## Output lines and what to compare

The CLI prints a stable sequence of stage lines:

1. `scale_demo_v1...` (workload)
2. `scale_demo_frames_v1...` (ingest)
3. `scale_demo_index_v1...` (index, if enabled)
4. `scale_demo_prompts_v1...` (prompts, if enabled)
5. `scale_demo_evidence_v1...` (evidence, if enabled)
6. `scale_demo_answers_v3...` (answers, if enabled)
7. `scale_demo_scale_report_v3...` (final rollup, printed when `--ingest 1`)

For deterministic regression checks, compare:

- The full final line (recommended).
- Or compare the following fields in the final line:
 - `report` (hash of the canonical scale report artifact)
 - `workload_hash`, `manifest`, and when present `snapshot` and `sig_map`
 - `prompts_list_hash`, `evidence_list_hash`, `planner_hints_list_hash`, `forecasts_list_hash`, `answers_list_hash` (when present)

The stage reports for prompts/evidence print counts and first/last hashes.
The answers stage report also prints planner_hints and forecasts counts and first/last hashes.
The scale report line adds `*_list_hash` fields that summarize each entire ordered list.


## Capturing output to a file

Use `--out-file` to write the full output to a file instead of stdout:

```
./fsa_lm scale-demo --root ./artifacts_demo \
  --seed 1 --docs 64 --queries 64 \
  --min_doc_tokens 24 --max_doc_tokens 48 \
  --vocab 512 --query_tokens 6 --tie_pair 0 \
  --ingest 1 --build_index 1 --prompts 1 --evidence 1 --answer 1 \
  --out-file ./run1.txt
```

Repeat with the same arguments and write to `run2.txt`, then compare:

- `diff -u run1.txt run2.txt` on Linux
- or use a file diff tool on Windows

The output should be identical for the same binary and inputs.


## Re-running and idempotence

The artifact store is content-addressed.
Re-running the same `scale-demo` command writes the same bytes and yields the same hashes.
This makes the command safe to re-run as a regression check.

If you want a clean store, delete the `--root` directory.


## Cache sizing notes (scale demo)

Scale demo uses bounded caches to keep memory usage predictable.

Evidence stage:
- `SCALE_DEMO_EVIDENCE_FRAME_CACHE_BYTES` (in `src/scale_demo.rs`) controls the in-process 2Q cache size used when decoding `FrameSegmentV1` during sketch attachment.
- `SCALE_DEMO_EVIDENCE_MAX_BYTES` controls the per-bundle byte budget.

If you need to tune caching behavior for large runs, start with `docs/CACHES_V1.md` and then consult `docs/SCALE_DEMO_MEMORY.md`.

Scale demo also accepts optional environment overrides for evidence-stage caps (see `docs/SCALE_DEMO_MEMORY.md`).
For a recommended hash-locked fixed-seed workload, see `docs/SCALE_DEMO_REGRESSION_PACK.md`.


## Troubleshooting

If two runs differ:
- Confirm the same binary and git state.
- Confirm identical CLI arguments (seed, docs, queries, vocab, query_tokens, tie_pair).
- Confirm you are comparing the same stage set (prompts/evidence/answer flags).

If `--evidence 1` fails:
- It requires `--build_index 1`.

If `--answer 1` fails:
- It requires `--evidence 1`.

