# Runtime Reachability

This note records which release-facing features are live across the supported runtime entry paths.
It is intended as a pre-release audit aid so the default chat surface, workspace defaults, advisory artifacts,
and saved conversation behavior can be checked without reading the code.

## Entry paths

- `answer` runs one grounded answer from a prompt pack.
- `ask` wraps prompt-pack creation plus one grounded answer and optional session-file save/resume.
- `chat` wraps repeated grounded answers with optional autosave and resume.

## Verified runtime features

### Presentation mode

- `user` is the default surface for `answer`, `ask`, and `chat`.
- `operator` preserves the inspect-oriented surface for `answer`, `ask`, and `chat`.
- Saved conversation packs can retain the selected presentation mode for later resume.

### Workspace scalar defaults

When the matching CLI flag is omitted, these defaults are live in `answer`, `ask`, and `chat`:

- `default_k`
- `default_expand`
- `default_meta`

### Workspace advisory defaults

When the matching CLI flag is omitted, these advisory artifacts are live in normal runtime flow:

- `markov_model`
- `exemplar_memory`
- `graph_relevance`

Boundaries remain unchanged:

- graph enrichment stays bounded and subordinate to lexical retrieval
- exemplar memory stays advisory only
- Markov remains bounded phrasing only

### Fallback rules

- Explicit CLI artifact ids are strict. Missing explicit artifacts fail.
- Workspace advisory defaults are best effort. Missing workspace advisory artifacts fall back cleanly.
- Saved conversation-pack advisory ids are restored on resume so the same session behavior can continue.

### Saved conversation behavior

Saved conversation packs can retain these sticky runtime choices:

- `markov_model_id`
- `exemplar_memory_id`
- `graph_relevance_id`
- `presentation_mode`

Resume precedence is:

1. explicit CLI override
2. saved conversation-pack value
3. workspace default

## Coverage summary

The automated checks cover:

- hidden diagnostics in default user mode
- visible diagnostics in operator mode
- applied workspace scalar defaults
- workspace advisory defaults in `answer`, `ask`, and `chat`
- clean workspace fallback when advisory artifacts are absent
- saved conversation persistence for sticky advisory ids and presentation mode
- restored sticky behavior on resume
- restored sticky Markov, exemplar, and graph behavior across both `ask` and `chat` resume flows
- safe fallback for older conversation packs that do not contain the newer trailer fields
