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

## Versioning

This project uses semantic versioning.

To bump the version:

1) Update `Cargo.toml` `version = "X.Y.Z"`.

2) Update `CHANGELOG.md`:
   - Keep an "Unreleased" section for upcoming changes.
   - Add a new version section with a short list of user-visible changes.

## Tagging

Example for version `0.1.0`:

1) Commit the version and changelog updates:
   - `git add Cargo.toml CHANGELOG.md`
   - `git commit -m "Release 0.1.0"`

2) Create an annotated tag:
   - `git tag -a v0.1.0 -m "v0.1.0"`

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
