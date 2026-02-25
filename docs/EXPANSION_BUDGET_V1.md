ExpansionBudgetV1 (Bridge expansion config)
==========================================

Status
------
 (schema + docs only).

No retrieval wiring in this stage.

Purpose
-------
Bridge expansion needs strict, deterministic budgets across multiple channels
(LEX/META/ENT/GRAPH). This file defines the v1 contract for configuring:

- global caps
- required-vs-optional cap
- per-base cap
- per-kind caps
- per-kind weight multipliers (Q16)

The policy logic and base weight table remain defined by docs/BRIDGE_EXPANSION.md.

Schema
------
Code: src/expansion_budget.rs

ExpansionBudgetV1:
- version: u32 (must be 1)
- max_expansions_total: u16
- max_required_total: u16
- max_expansions_per_base: u8
- kinds: Vec<ExpansionKindBudgetV1> (canonical order)

ExpansionKindBudgetV1:
- kind: ExpansionKindV1 (Lex|Meta|Ent|Graph)
- max_total: u16
- max_per_base: u8
- weight_mul_q16: u32 (1.0 == 65536)
- weight_floor: u16

Canonical encoding
------------------
All encodings use the crate's canonical codec (src/codec.rs).

Canonical requirements:
- kinds must be sorted by kind (ascending enum value)
- kinds must not contain duplicates

Decoding:
- rejects duplicate kinds
- rejects trailing bytes

Deterministic merge and tie-breaking
-----------------------------------
Bridge expansion selection is defined as:

1) Candidate generation (fixed rule order)
 See docs/BRIDGE_EXPANSION.md for the candidate rule list and base weights.

2) Effective weight
 - base_weight is a u16 from the rule table
 - weight_mul_q16 is applied per kind
 - effective_weight is clamped to u16
 - candidates below weight_floor are dropped

 effective_weight = clamp_u16((base_weight * weight_mul_q16) >> 16)

3) Dedup key
 Candidates are keyed by (kind, id).
 If a key repeats, keep the candidate with higher effective_weight.
 If equal effective_weight, keep the candidate that sorts earlier by the
 stable rank key below.

4) Stable rank key
 Candidates are ranked by:
 - effective_weight descending
 - kind rank (Ent, Lex, Meta, Graph)
 - id ascending

 Kind rank is fixed for v1:
 Ent < Lex < Meta < Graph

5) Budget fill
 - Partition candidates into required and optional pools.
 - Take required candidates first up to max_required_total.
 - Then take optional candidates up to max_expansions_total.
 - Enforce max_expansions_per_base and per-kind max_total/max_per_base.

Notes
-----
- ExpansionBudgetV1 is intentionally small and codec-stable.
- Future versions may add per-rule caps or additional kinds. Versioning
 supports that evolution.
