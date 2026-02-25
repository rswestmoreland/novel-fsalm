ExpandedQfvV1 (schema)
======================

Purpose
-------
ExpandedQfvV1 is the canonical, deterministic representation of a query feature
vector after bridge expansion.

This artifact is intended to be produced by rules-first expansion logic and
consumed by retrieval and evidence selection.

 scope
----------------
Schema-only. This stage defines:
- The item record fields.
- Required vs optional pools.
- Canonical ordering rules.
- Canonical byte encoding/decoding.

No retrieval wiring is performed in this stage.

Data model
----------
ExpandedQfvV1 fields:
- version: u32 (must be 1)
- tie_control_id: Id64
- required: Vec<ExpandedQfvItemV1>
- optional: Vec<ExpandedQfvItemV1>

ExpandedQfvItemV1 fields:
- kind: ExpansionKindV1
 - 1 Lex
 - 2 Meta
 - 3 Ent
 - 4 Graph
- id: Id64
- weight: u16 (0..65535)
- origin_base_kind: ExpansionKindV1
- origin_base_id: Id64
- origin_rule_id: u16

Canonical ordering
------------------
Each pool (required, optional) is stored in strict canonical order:
- weight descending
- kind ascending
- id ascending

Duplicates are not allowed.

Dedup semantics
--------------
Higher-level expansion logic should deduplicate candidates keyed by (kind, id)
before emitting ExpandedQfvV1.

In v1, duplicates are rejected at encode/decode time.

Canonical byte codec
--------------------
Encoding is little-endian and fixed-field.

Layout:
- u32 version
- u64 tie_control_id
- u32 required_count
- u32 optional_count
- required items (in canonical order)
- optional items (in canonical order)

Item layout:
- u8 kind
- u64 id
- u16 weight
- u8 origin_base_kind
- u64 origin_base_id
- u16 origin_rule_id

All decodes reject trailing bytes.

Related docs
------------
- docs/BRIDGE_EXPANSION.md
- docs/EXPANSION_BUDGET_V1.md
- src/expanded_qfv.rs
