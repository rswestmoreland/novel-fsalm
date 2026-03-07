Logic solver v1
==============

Overview
--------
Novel includes a small, deterministic finite-domain solver for logic puzzles.

The solver is evidence-first:
- It produces a content-addressed ProofArtifactV1.
- The answer loop may attach a ProofRef item into EvidenceBundleV1.
- ReplayLog records a proof-artifact-v1 step so runs are auditable.

User experience
---------------
When Novel believes the prompt is a logic puzzle, it may ask one clarifying
question at a time to avoid guessing.

Common clarifiers (in priority order):
- Variables/entities: which items are involved?
- Domain: what values are possible?
- Puzzle shape: ordering vs matching/categories.
- Constraints: rules that relate the variables.
- Uniqueness: should there be exactly one solution?

You do not need a special format to answer clarifying questions.

For the most reliable solving path, provide constraints using the optional [puzzle] block format.

Optional structured block
-------------------------
For reproducible bug reports and exact solver inputs, you can provide a
structured puzzle block:

```
[puzzle]
vars: A,B,C
domain: 1..3
expect_unique: true
constraints:
  A != B
  A < C
  if A = 1 then B != 2
[/puzzle]
```

Supported constraint forms (v1)
-------------------------------
The v1 solver contract is intentionally small.

Constraint lines support:
- Equality / inequality:
  - `A = 1`, `A != 2`
  - `A = B`, `A != B`
- Ordering:
  - `A < B`, `A <= B`, `A > B`, `A >= B`
- AllDifferent:
  - `all_different: A,B,C`
  - `all_different(A,B,C)`
- Implication (value guard):
  - `if A = 1 then B = 2`
  - `if A = 1 then B != 2`

Limits and determinism
----------------------
The solver is bounded and deterministic.

Caps (v1):
- max vars: 32
- max domain size: 64
- max constraints: 256
- max recorded solutions: 2 (used to distinguish unique vs multiple)

Determinism rules:
- Variable ordering: stable, sorted.
- Domain ordering: stable, sorted.
- Search ordering: fixed variable/value ordering with deterministic tie breaks.
- If the work cap is hit, the proof is marked truncated.

Artifacts and replay
--------------------
Artifacts:
- ProofArtifactV1: canonical encoded proof output.
- EvidenceBundleV1 ProofRef: points at the proof hash.

Replay steps:
- proof-artifact-v1: records the proof hash for the run.

Rendering
---------
When a ProofRef is present and the answer is in a steps-oriented style, the
realizer prints a short line:

`Proof solution: (unique) A=1,B=2,C=3`

This line is intentionally compact and deterministic.
