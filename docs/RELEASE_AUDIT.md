Release readiness audit
======================

This file is a practical checklist for cutting a public release.

Local checks
------------

Run these from the repo root.

1) Tests (all targets)

- Windows:
  - tools\\check_warnings.bat

- Linux/WSL:
  - tools/check_warnings.sh

These commands run cargo test --all-targets with warnings treated as errors.

2) Formatting

- cargo fmt --all --check

3) Optional: fast smoke

If you want a quick CLI sanity pass after a large refactor, run:

- fsa_lm -h
- fsa_lm selftest

Operator workflow smoke
-----------------------

For the full end-to-end workflow and common failure modes, see:

- docs/OPERATOR_WORKFLOW.md

Key regression locks
--------------------

The repo includes integration tests that lock operator behavior via hashes and
stable stats lines. Two important ones:

- tests/operator_workflow_golden_pack_v1.rs
- tests/sync_resilience_regressions_v1.rs

CI checks
---------

GitHub Actions runs on Windows and Linux:

- cargo test --all-targets with warnings as errors
- cargo fmt --check
