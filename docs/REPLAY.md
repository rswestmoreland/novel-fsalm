Replay Logs (Baseline)
======================

Purpose
-------
A replay log is a canonical artifact that records, per step, the input and output artifact
hashes. This enables deterministic replay and regression checks.

 scope
--------------
- ReplayLog v1 with:
 - version
 - list of steps: (name, inputs[], outputs[])
- Canonical encoding and decoding

Later stages
------------
- Add typed job payload references and stage configs (DecodeCfg, snapshot_id, weights_id)
- Add assertions for expected hashes for CI-style checks

: PromptPack linkage
----------------------------
A ReplayLog step can reference PromptPack artifacts using a simple convention:

- Step name: "prompt" (or user-chosen)
- inputs: empty
- outputs: [prompt_pack_hash]

This keeps ReplayLog schema stable while enabling end-to-end chains:

- prompt step outputs PromptPack hash
- future infer step inputs include PromptPack hash and snapshot hash
- future infer step outputs include completion artifact hash

: Step conventions
--------------------------
Replay steps are interpreted by convention. See:
- docs/REPLAY_STEP_CONVENTIONS.md

The key idea is that inputs/outputs are unordered sets of hashes.
Downstream tooling interprets them by step name and the required set
members for that name.
