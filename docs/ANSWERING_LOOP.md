# Answering Loop

This document describes the end-to-end answering loop implemented by `fsa_lm answer`.

The pipeline is evidence-first and deterministic:

- All persisted outputs are content-addressed artifacts.
- Retrieval and ordering are stable.
- ReplayLog captures the full dependency set as step input/output sets.

## High-level steps

1) Load the PromptPack
- Prompt text, query text, and control flags.

2) Resolve optional context
- Optional LexiconSnapshot (when query expansion is enabled).
- Optional PragmaticsFrame (when provided).

3) Retrieve
- Run deterministic search over IndexSnapshot.
- Apply signature gating (IndexSigMap) when present.

4) Build EvidenceBundle
- Convert hits into a canonical EvidenceBundleV1.
- Store EvidenceBundleV1 as an artifact.

5) Derive RealizerDirectives (optional)
- If a PragmaticsFrame is present, derive RealizerDirectivesV1.
- Store RealizerDirectivesV1 as an artifact.

6) Plan with guidance
- Run the guided planner:
 - AnswerPlanV1
 - PlannerHintsV1
 - ForecastV1
- Store PlannerHintsV1 and ForecastV1 as artifacts.

7) Realize
- Run Realizer v1 to render deterministic text from AnswerPlanV1 and EvidenceBundleV1.
- Apply RealizerDirectivesV1 when present.

8) Clarifying-question policy
- If PlannerHintsV1 requests clarifying, append the top ForecastV1 question.
- The appended question is bounded and UTF-8 safe.

9) Store answer text
- Store the realized text bytes as an artifact.

10) Emit MarkovTrace (observational)
- Build a bounded MarkovTraceV1 token stream from the realized answer path.
- Store MarkovTraceV1 as an artifact.
- The trace does not influence rendering; it is only used for offline training.

11) Build EvidenceSet
- Build a minimal EvidenceSetV1 binding the full answer text to the rendered evidence rows.
- Store EvidenceSetV1 as an artifact.

12) Emit ReplayLog
- Append stable steps capturing inputs/outputs:
 - realizer-directives-v1 (optional)
 - planner-hints-v1
 - forecast-v1
 - answer-v1
 - markov-trace-v1

## Determinism notes

- All step input/output sets are canonicalized via sorted encoding.
- PlannerHintsV1 and ForecastV1 are derived deterministically from EvidenceBundleV1 and optional PragmaticsFrame.
- The answer step inputs include guidance hashes so ReplayLog captures the full planning dependency set.

