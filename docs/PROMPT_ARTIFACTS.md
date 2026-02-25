PromptPack Artifacts
===============================

Goal
----
PromptPack is stored as an artifact so it can be referenced by hash in ReplayLog
and in future job envelopes.

Contract
--------
- Before storing a PromptPack, it MUST be canonicalized with PromptLimits.
- After canonicalization, encoding MUST use the canonical fast path
 (`encode_assuming_canonical`) to avoid extra clones/allocations.
- Decoding a PromptPack should always succeed for bytes previously produced by
 encoding, and fail for corrupt bytes.

API
---
- put_prompt_pack(store, &mut pack, limits) -> Hash32
- get_prompt_pack(store, &hash) -> Option<PromptPack>

Notes
-----
- Token-based limits are not implemented yet. uses byte/count limits
 from PromptLimits as a deterministic budget.
