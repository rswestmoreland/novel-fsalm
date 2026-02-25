PromptLimits
=======================

Purpose
-------
PromptLimits provides deterministic, tokenizer-independent size bounds for PromptPack.

Why bytes and counts
--------------------
Token limits require a tokenizer and token accounting. Those will arrive later.
For early prototypes, byte and count limits provide:
- bounded memory and CPU for request processing
- deterministic truncation rules independent of tokenization
- stable canonical artifacts suitable for hashing and caching

Rules
-----
- Message content is truncated to a UTF-8 prefix at a char boundary.
- If message count or total bytes exceed limits:
 - If keep_system is true, System messages are kept preferentially.
 - Remaining slots are filled with the most recent non-system messages.
 - If still over total bytes, the most recent kept message is truncated first.
 - If still over, oldest non-system messages are dropped first.
 - If still over, the last remaining message is truncated again. Final fallback may drop
 messages if the budget is extremely small.

Constraints
-----------
- Constraints are sorted by (key asc, value asc).
- Truncated by max_constraints, then by max_total_constraint_bytes (drop from end).

Notes
-----
Later stages will add token-based budgets and truncation, while keeping these rules
as a deterministic fallback and for non-tokenizer code paths.
