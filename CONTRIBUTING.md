# Contributing

Thanks for your interest in improving Novel FSA-LM.

This repository prioritizes deterministic behavior, evidence-first answers, and a small, auditable code surface.

## Code of conduct
Please read and follow `CODE_OF_ETHICS.md`.

## Development setup
- Install Rust (stable).
- From the repo root:

```bash
cargo test
```

On Windows, use PowerShell or CMD. On Linux/macOS, use your shell of choice.

## Required checks

### Warning-free builds
This project treats warnings as errors.

- Windows:

```bat
tools\check_warnings.bat
```

- Linux/macOS:

```bash
tools/check_warnings.sh
```

### Formatting

```bash
cargo fmt --all
```

CI enforces `cargo fmt --check`.

## Coding rules
- No `unsafe`.
- ASCII-only comments and docs.
- Do not depend on hash map iteration order.
- Prefer canonical, content-addressed artifacts and stable ordering.
- Avoid broad search/replace changes.

## Tests
- Add tests for every new behavior.
- Prefer small deterministic fixtures.
- Integration tests that exercise the CLI are welcome.

## Submitting changes
- Create a branch.
- Make focused changes.
- Run:

```bash
cargo test --all-targets
```

- Run the warning check script.
- Open a pull request with a clear description and rationale.
