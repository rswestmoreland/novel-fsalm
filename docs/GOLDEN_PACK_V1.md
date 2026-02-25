# Golden Pack v1

Goal
----

Golden packs are small, deterministic end-to-end workloads used for regression.
They are intended to be:

- Stable across platforms (Windows/Linux)
- Content-addressed (one hash to lock in CI)
- Fast enough to run regularly

Command
-------

The v1 golden pack is executed via:

- `fsa_lm golden-pack --root <dir> [--expect <hash32hex>] [--out-file <path>]`

It prints a single line:

- `golden_pack_report_v1 report=<hash32hex> scale_report=<hash32hex> workload=<hash32hex> docs=<n> queries=<n> tie_pair=<0|1>`

Locking in CI
-------------

To lock the report hash:

- Pass `--expect <hash32hex>`, or
- Set the environment variable `FSA_LM_GOLDEN_PACK_V1_REPORT_HEX=<hash32hex>`

If the computed report hash differs, the command exits non-zero.

Determinism notes
-----------------

The golden pack runner forces these scale-demo evidence override environment variables
to defaults (by setting them to 0 for the duration of the run):

- `FSA_LM_SCALE_DEMO_EVIDENCE_K`
- `FSA_LM_SCALE_DEMO_EVIDENCE_MAX_BYTES`
- `FSA_LM_SCALE_DEMO_EVIDENCE_FRAME_CACHE_BYTES`

This is to ensure the golden pack stays stable even if the user has overrides set.

The v1 runner also realizes answers using a fixed `RealizerDirectivesV1` profile
(tone=neutral, style=debug, numbered+bullets enabled). This ensures the directives
path is covered by golden pack regression.

Workload parameters
-------------------

The current v1 "tiny" workload is:

- seed: 7
- docs: 32
- queries: 16
- min_doc_tokens: 24
- max_doc_tokens: 48
- vocab: 512
- query_tokens: 6
- tie_pair: 1

Artifacts
---------

The command stores:

- A `ScaleDemoScaleReportV1` artifact (scale report)
- A `GoldenPackReportV1` artifact (golden pack report)

The printed `report=<hash>` is the content hash of the stored golden pack report.

See also
--------

- docs/GOLDEN_PACK_TURN_PAIRS_V1.md
