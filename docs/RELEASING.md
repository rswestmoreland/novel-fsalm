# Releasing

This document describes how to cut a release of the `fsa_lm` CLI and libraries.

The repository targets deterministic behavior and warning-free builds. A release should be cut only from a clean, reproducible commit that passes CI on Windows and Linux.

## Preconditions

- You have a clean working tree (`git status` shows no local changes).
- `cargo test --all-targets` passes locally.
- Warnings are treated as errors (`-Dwarnings`) and the build stays warning-free.
- CI is green on Windows and Linux.

## Local preflight

From the repository root:

- Run tests:
  - `cargo test --all-targets`

- Enforce warning-zero:
  - Windows: `tools\check_warnings.bat`
  - Linux/WSL: `tools/check_warnings.sh`

- Enforce formatting:
  - `cargo fmt --all --check`

- Review release-facing runtime reachability:
  - `docs/RUNTIME_REACHABILITY.md`
  - `docs/RELEASE_AUDIT.md`

- Run the user-vs-operator presentation smoke if the release changes the
  default answer surface:
  - Windows: `examples\demo_cmd_compare_presentation.bat`
  - Linux/WSL: `examples/demo_cmd_compare_presentation.sh`

## Versioning

This project uses semantic versioning.

To bump the version:

1) Update `Cargo.toml` `version = "X.Y.Z"`.

2) Update `CHANGELOG.md`:
   - Keep an "Unreleased" section for upcoming changes.
   - Add a new version section with a short list of user-visible changes.

## Repository upload prep

Before you tag and push a release, verify that the repo surface shown to users is
clean and complete.

Checklist:

- `README.md` reflects the current primary user flow and default presentation.
- `CHANGELOG.md` and `docs/RELEASE_NOTES.md` match the version being cut.
- `docs/RELEASE_AUDIT.md` and `docs/RELEASING.md` match the current release gate.
- `examples/README.md` and any release-smoke scripts still point at the current CLI behavior.
- Top-level release files are present and current: `LICENSE`, `NOTICE`, `SECURITY.md`, `CONTRIBUTING.md`, and `CODE_OF_ETHICS.md`.
- Public-facing docs and examples do not use internal `Phase` / `Subphase` / `Task` wording outside `docs/MASTER_PLAN.md`.

Suggested review set before upload:

- `README.md`
- `CHANGELOG.md`
- `docs/RELEASE_NOTES.md`
- `docs/RELEASE_AUDIT.md`
- `docs/RELEASING.md`
- `examples/README.md`

## Tagging

Example for version `X.Y.Z`:

1) Commit the version and release-document updates:
   - `git add Cargo.toml CHANGELOG.md docs/RELEASE_NOTES.md docs/RELEASE_AUDIT.md docs/RELEASING.md`
   - `git commit -m "Release X.Y.Z"`

2) Create an annotated tag:
   - `git tag -a vX.Y.Z -m "vX.Y.Z"`

3) Push commit and tag:
   - `git push`
   - `git push --tags`

## GitHub release

- Create a GitHub Release from the tag.
- Copy the corresponding section from `CHANGELOG.md` into the release notes.
- Attach any optional artifacts you want to distribute (for this project, source + tag is usually sufficient).

## crates.io (optional)

If you choose to publish to crates.io:

- Verify packaging:
  - `cargo package`

- Publish:
  - `cargo publish`

Note: publishing requires additional metadata in `Cargo.toml` (repository URL, description, keywords). Keep those fields minimal until you are ready to publish.
