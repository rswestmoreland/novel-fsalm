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

4) Runtime reachability audit

Before cutting a release that changes default chat behavior, review:

- docs/RUNTIME_REACHABILITY.md

This captures the release-facing runtime paths that should stay live:
workspace defaults, advisory defaults, user vs operator presentation, and
saved-session resume behavior. The resume audit should cover Markov, exemplar,
and graph restoration across both `ask` and `chat`.

5) Presentation smoke

Run one of these example scripts to verify that the same prompt stays grounded
while the visible surface changes between default user mode and operator mode.
The operator run should use `--presentation operator` and the default run should
omit it:

- examples/demo_cmd_compare_presentation.bat
- examples/demo_cmd_compare_presentation.sh

Current release-candidate gate
------------------------------

Use this gate before cutting a release that changes the default chat surface,
workspace defaults, advisory defaults, or saved-session behavior.

Required results:

- Default `ask`, `chat`, and `answer` output is conversational and user-facing.
- `--presentation operator` still shows the inspect-friendly diagnostics without
  changing grounding, evidence refs, or retrieval precedence.
- Workspace scalar defaults (`default_k`, `default_expand`, `default_meta`)
  apply automatically when matching flags are omitted.
- Workspace advisory defaults (`markov_model`, `exemplar_memory`,
  `graph_relevance`) auto-apply when configured and fall back cleanly when the
  workspace points at a missing advisory artifact.
- Saved conversation packs restore sticky advisory ids and
  `presentation_mode` on resume, with explicit CLI flags still taking
  precedence.
- README, CLI docs, workspace docs, conversation-pack docs, release notes, and
  examples describe the same runtime surface.
- Older design docs that overlap live behavior do not contradict the current
  runtime surface.
- Public-facing docs, code comments, and examples do not use internal
  `Phase`/`Subphase`/`Task` wording outside `docs/MASTER_PLAN.md`.

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

Repository upload prep
----------------------

Before tagging the release and pushing it to the public repo, confirm:

- `Cargo.toml` version, `CHANGELOG.md`, and `docs/RELEASE_NOTES.md` all name the same release.
- `README.md` describes the same default user flow shown by the example scripts.
- `examples/README.md` lists the current release smoke scripts.
- `docs/OPERATOR_WORKFLOW.md`, `docs/QUALITY_GATE_V1.md`, and `docs/RELEASING.md` all point at the same operator and release process.
- Top-level project files needed for a public repo are present: `LICENSE`, `NOTICE`, `SECURITY.md`, `CONTRIBUTING.md`, and `CODE_OF_ETHICS.md`.

CI checks
---------

GitHub Actions runs on Windows and Linux:

- cargo test --all-targets with warnings as errors
- cargo fmt --check
