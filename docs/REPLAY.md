Replay logs
===========

Purpose
-------
A replay log is the current canonical artifact used to record, per step, the
input and output artifact hashes for replayable workflows. This enables
deterministic replay and regression checks.

Current scope
-------------
- ReplayLog v1 with:
 - version
 - list of steps: (name, inputs[], outputs[])
- Canonical encoding and decoding
- Step conventions shared by query, answer, ask, chat, and other replayable flows

Reserved extensions
-------------------
- Typed job payload references and stage configs (DecodeCfg, snapshot_id, weights_id)
- Assertions for expected hashes for CI-style checks

PromptPack linkage
------------------
A ReplayLog step can reference PromptPack artifacts using a simple convention:

- Step name: "prompt" (or user-chosen)
- inputs: empty
- outputs: [prompt_pack_hash]

This keeps ReplayLog schema stable while enabling end-to-end chains:

- prompt step outputs PromptPack hash
- later inference-style steps can reference PromptPack plus retrieval/runtime inputs
- later completion-style steps can reference the resulting output artifact hash

Step conventions
----------------
Replay steps are interpreted by convention. See:
- docs/REPLAY_STEP_CONVENTIONS.md

The key idea is that inputs/outputs are unordered sets of hashes.
Downstream tooling interprets them by step name and the required set
members for that name.
