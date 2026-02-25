Tokenizer and Term IDs
=================================

This document specifies the v1 tokenization rules and the deterministic term-id
strategy used for indexing and retrieval.

Tokenization rules
------------------
A token is a maximal run of "token characters":

- Unicode alphanumeric characters (`char.is_alphanumeric`).
- ASCII underscore `_`
- ASCII hyphen `-`
- ASCII apostrophe `'`

All other characters are delimiters.

The tokenizer yields byte spans into the original UTF-8 string. It walks the
string using `char_indices`, so spans always align to UTF-8 boundaries.

Term-id strategy
----------------
Each token is mapped to a stable 64-bit `TermId`:

- Hash: BLAKE3
- Domain prefix: `b"term\0"` (domain separation)
- Normalization: ASCII lowercase of bytes `A-Z` to `a-z`.
 Non-ASCII bytes are passed through unchanged.
- Truncation: only the first `max_token_bytes` are hashed (default: 64).

The `TermId` value is the first 8 bytes of the BLAKE3 digest interpreted as a
little-endian `u64`.

Term frequencies
----------------
v1 computes term frequencies by:

1) scanning tokens and deriving a term id per token
2) sorting the term-id vector
3) counting runs to produce `TermFreq { term, tf }`

This avoids hash maps and keeps behavior deterministic. It is O(n log n) in the
number of tokens.

Future work
-----------
Later stages may add:
- stopword filtering
- dictionary coding for token strings
- alternate normalizations (Unicode case-folding) behind explicit configuration
- faster counting strategies for very large documents
