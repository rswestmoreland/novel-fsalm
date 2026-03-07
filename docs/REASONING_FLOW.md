# Reasoning flow

This document explains how Novel answers a prompt in a way that is:

- evidence-first (retrieve small structured evidence, not paragraphs)
- deterministic (stable ordering, canonical artifacts, no RNG)
- replayable (ReplayLog records the dependency chain)

It is a pipeline description of the current implementation. It is not a theory of
"general reasoning".

## Big picture

Novel separates three concerns:

1. **Select evidence** (frames, lexicon rows, and optional proof artifacts)
2. **Shape a plan** (planner hints, forecast questions, and a bounded answer plan)
3. **Render text** (realizer directives, optional surface template hints, and a
   deterministic clarifying-question append)

## End-to-end answering flow

The main pipeline used by `fsa_lm ask`, `fsa_lm chat`, and `fsa_lm answer` is:

```mermaid
flowchart TD
  A[PromptPack] --> B[Query terms and signals]

  B --> C{Prior conversation in PromptPack?}
  C -->|yes| D[ContextAnchorsV1\ncontext-anchors-v1]
  C -->|no| E[No anchors]

  B --> F{LexiconSnapshot available?}
  F -->|yes| G[Bridge expansion (bounded)\nExpanded QFV]
  F -->|no| H[Base QFV only]

  D --> I[RetrievalPolicy apply]
  E --> I
  G --> I
  H --> I

  I --> J[retrieve-v1\nHitListV1]
  J --> K[build-evidence-v1\nEvidenceBundleV1]

  K --> L[planner\nAnswerPlanV1 + PlannerHintsV1 + ForecastV1]

  L --> M[quality gate\nDirectives + Markov hints (optional)\nClarify append (bounded)]

  M --> N[realizer\nanswer text]
  N --> O[EvidenceSetV1]
  O --> P[ReplayLog steps]

  %% Optional: Markov trace is observational
  N --> Q[markov-trace-v1\nMarkovTraceV1]
  Q --> P
```

### Notes

- **Context anchors** are low-weight terms derived from recent messages. They
  improve retrieval continuity for follow-ups that omit key nouns. Anchors only
  influence retrieval and are capped. See `docs/CONTEXT_ANCHORS_V1.md`.

- **Bridge expansion** is deterministic and budgeted. It expands the query feature
  vector using lexicon relations and other rule-driven channels. See
  `docs/BRIDGE_EXPANSION.md`.

- **Quality gate** consolidates post-planning control signals (directives, Markov
  opener hints, and clarifying question policy) without introducing new claims.
  See `docs/QUALITY_GATE_V1.md`.

## Optional branches

These branches are not the "main" pipeline, but they plug into it in replayable
ways.

### Logic puzzles (optional)

Logic puzzles are handled by a deterministic solver when the input is
sufficiently structured. If the solver runs, it emits a content-addressed proof
artifact and the answer includes a ProofRef evidence item.

```mermaid
flowchart TD
  A[PromptPack] --> B{Puzzle detected?}
  B -->|no| Z[Continue normal retrieval]

  B -->|yes| C[Puzzle sketch (optional)\npuzzle-sketch-v1]
  C --> D{Compile-ready?}
  D -->|no| E[Ask one clarifying question\nForecastV1]
  D -->|yes| F[Solve (bounded)\nproof-artifact-v1]
  F --> G[Attach ProofRef into EvidenceBundleV1]
  G --> Z
```

See `docs/LOGIC_SOLVER_V1.md` for the user-facing behavior and the supported
constraint forms.

### Safety reflex (optional)

Safety handling is rules-first and deterministic. It can:

- refuse or redirect when required
- downrank unsafe plan shapes
- force bounded clarifying questions

This is integrated as a control-signal path; it does not add evidence.

See `docs/SAFETY_REFLEX_V1.md`.

### Math and other coprocessors (optional)

Math and other verifiers are designed to be small, deterministic coprocessors
that compute or verify results. When used, they emit replayable artifacts.

## Replay and observability

Novel records work as stable steps with content-addressed artifacts. The exact
step sets are defined in `docs/REPLAY_STEP_CONVENTIONS.md`. In the answering
flow, you will commonly see:

- `context-anchors-v1` when the prompt contains prior messages
- `retrieve-v1` and `build-evidence-v1`
- `planner-hints-v1` and `forecast-v1`
- `answer-v1` and `markov-trace-v1`
- `puzzle-sketch-v1` and `proof-artifact-v1` when the logic solver path is used

This is the core mechanism that makes answers auditable and reproducible.

## Determinism rules (summary)

- Canonical encoding for artifacts and step input/output sets.
- Stable ordering for all ranked lists, with explicit tie-breakers.
- No wall-clock time, no RNG, no non-deterministic iteration.
- All bounded work uses strict caps (terms, expansions, hits, evidence, steps).

For more detail, see `docs/DETERMINISM.md` and `docs/ANSWERING_LOOP.md`.
