Metaphonetic preprocessor v1
======================================

Goal
----
Provide a deterministic, allocation-light mapping from a token to a compact
"sounds-like" code and a stable 64-bit id (`MetaCodeId`).

This is used for fuzzy retrieval (sound-alike matching) and for reflexive
linguistic behaviors (metaphonetics), without requiring a large neural model.

v1 behavior
-----------
Input:
- A token string (typically from the tokenizer).

Normalization:
- Only ASCII letters A-Z / a-z are considered.
- Non-ASCII bytes are ignored (no transliteration in v1).
- Letters are ASCII-lowercased for processing.

Code output:
- ASCII uppercase letters plus the digit '0' for "TH".
- Output length is capped (`max_code_len`, default 12).
- Adjacent duplicate output symbols are suppressed.

Notes:
- Vowels are emitted only at the start, as 'A' (classic metaphone convention).
- The code is not intended to be reversible or perfectly linguistically accurate.

Common rules (simplified)
-------------------------
- Initial silent letters: KN, GN, PN, AE, WR => drop the first letter.
- Initial X => S.
- C: CH => X, SCH => SK, CI/CE/CY => S, else K.
- D: DGE/DGI/DGY => J, else T.
- G: GE/GI/GY => J, else K. GH can be silent or map to F in some positions.
- H: emitted only when separating consonant and vowel.
- P: PH => F.
- S: SH => X, SIO/SIA => X.
- T: TH => 0, TIO/TIA => X, TCH => (skip T).
- V => F, Q => K, X => KS, Z => S.

MetaCodeId
----------
`MetaCodeId` is derived by hashing the code bytes with domain separation:

- Hash: BLAKE3
- Prefix: b"meta\0"
- Id: first 8 digest bytes interpreted as little-endian u64.

Future work
-----------
Potential later upgrades:
- Unicode-aware normalization with explicit configuration (not default).
- A more complete Double Metaphone-like rule set.
- Dedicated handling for abbreviations and acronyms.
- Multi-code output (primary + alternate) for ambiguous cases.
