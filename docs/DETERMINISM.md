Determinism Contract (Repo Summary)
===================================

Target
------
Bitwise deterministic outputs given:
- identical binary build
- identical model weights/tokenizer
- identical memory snapshot
- identical PromptPack bytes and DecodeCfg
- fixed thread policy

Rules
-----
- Canonical encoding (codec module) for all artifacts
- Stable sorting and explicit tie-break keys
- Deterministic RNG with explicit seed
- Avoid depending on hash map iteration order
- Integer-first scoring and tool outputs

Notes
-----
Cross-machine bitwise determinism requires strict control of compiler flags and CPU
feature targets, and avoidance of floats where possible.
