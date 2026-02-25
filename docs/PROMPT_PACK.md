PromptPack
=====================

Purpose
-------
PromptPack is the canonical request artifact that represents the user prompt and
runtime-critical IDs for determinism.

Encoding
--------
PromptPack uses the repo codec (little-endian primitives, length-prefixed UTF-8 strings).
Encoding is canonical:
- Constraints are sorted by (key asc, value asc) during encoding.
- Messages remain in given order.

Fields (v1)
-----------
- version: u16 (1)
- seed: u64
- max_output_tokens: u32
- ids:
 - snapshot_id: Hash32
 - weights_id: Hash32
 - tokenizer_id: Hash32
- messages: Vec<Message>
 - role: u8 (0 system, 1 user, 2 assistant)
 - content: str
- constraints: Vec<ConstraintKV>
 - key: str
 - value: str

Notes
-----
- Later stages introduce bounded truncation and more typed constraints.
- The CLI and TCP protocol will transmit PromptPack as an artifact.

Canonicalization
---------------------------
PromptPack provides `canonicalize_in_place(limits)` to enforce deterministic size bounds.
This applies byte- and count-based limits (no tokenizer required yet):
- per-message UTF-8 byte cap
- max messages and max total message bytes (keep_system preference)
- constraints sorted and truncated by count and total bytes

For lower allocations, `encode_assuming_canonical` encodes without cloning constraints,
assuming they have already been canonicalized.
