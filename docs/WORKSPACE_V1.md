Workspace defaults (v1)
======================

This document defines a small, human-editable state file that lets operators and
end users run Novel without manually wiring artifact hashes into every command.

The workspace file never changes artifact contents. It only stores pointers to
already-materialized artifacts (snapshots, sig maps, lexicon snapshots, and
optional advisory artifacts).

Goals
-----
- Improve usability: a user can load data once, then run prompts with minimal
  flags.
- Preserve determinism: the workspace only selects which fixed artifacts to use.
- Preserve auditability: commands still produce the same replayable artifacts;
  the workspace is just a default input source.

File location
------------
The workspace file lives under the artifact root directory:

- <root>/workspace_v1.txt

Commands may also write a temporary file and rename it atomically:

- <root>/workspace_v1.tmp

Format
------
The file is ASCII text with one entry per line:

- Blank lines are ignored.
- Lines beginning with '#' are comments and are ignored.
- Entries are key=value pairs.
- Leading/trailing ASCII whitespace around keys and values is ignored.
- Unknown keys are ignored.
- If a key appears multiple times, the last value wins.

Keys and values
---------------
All artifact ids are lowercase hex strings.

Required keys (for answering)
-----------------------------
- merged_snapshot=<hex>
  - Content-addressed hash of the merged IndexSnapshotV1 to use as the default
    retrieval snapshot.

- merged_sig_map=<hex>
  - Content-addressed hash of the IndexSigMapV1 matching merged_snapshot.

Optional keys (for query expansion)
-----------------------------------
- lexicon_snapshot=<hex>
  - Content-addressed hash of a LexiconSnapshotV1 used for bounded query
    expansion.
  - If absent, expansion can still be requested, but commands that require a
    lexicon snapshot must fail with a clear error.

Optional keys (defaults)
------------------------
The v1 format optionally supports small scalar defaults. These never affect
artifact encodings or hashes; they only set default command parameters.

- default_k=<u32>
  - Default top-k for retrieval when not provided on the command line.
  - Applied by answer, ask, and chat when --k is omitted.

- default_expand=<0|1>
  - Default query expansion behavior for end-user commands (for example, ask).
  - Applied by answer, ask, and chat when --expand is omitted.

- default_meta=<0|1>
  - Default metaphone expansion behavior for end-user commands (for example,
    ask).
  - Applied by answer, ask, and chat when --meta is omitted.

Optional advisory artifact defaults
----------------------------------
These keys store workspace-level advisory artifact ids. They are preserved when
workspace-aware wrapper commands update other keys. They do not change artifact
encodings or hashes.

- markov_model=<hex>
  - Content-addressed hash of a MarkovModelV1 artifact.
  - Applied by answer, ask, and chat when --markov-model is omitted.

- exemplar_memory=<hex>
  - Content-addressed hash of an ExemplarMemoryV1 artifact.
  - Applied by answer, ask, and chat when --exemplar-memory is omitted.

- graph_relevance=<hex>
  - Content-addressed hash of a GraphRelevanceV1 artifact.
  - Applied by answer, ask, and chat when --graph-relevance is omitted.

All defaults are overridden by explicit CLI flags.

Live command behavior
---------------------
This section describes the current behavior for commands that opt into the
workspace defaults. It documents live CLI UX, not a change to artifact
formats.

- show-workspace
  - Reads <root>/workspace_v1.txt and prints the resolved values, including
    any advisory artifact defaults.

- load-wikipedia (wrapper)
  - Produces a merged IndexSnapshotV1 and IndexSigMapV1.
  - Writes merged_snapshot and merged_sig_map into workspace_v1.txt.

- load-wiktionary (wrapper)
  - Produces a LexiconSnapshotV1.
  - Writes lexicon_snapshot into workspace_v1.txt.

- answer
  - If --snapshot and/or --sig-map are not provided, falls back to
    merged_snapshot and merged_sig_map from workspace_v1.txt.
  - If --expand is omitted and default_expand=1 is set, enables bounded query
    expansion.
  - If graph_relevance is configured and --graph-relevance is omitted, bounded
    graph expansion is enabled automatically.
  - When expansion is active and --lexicon-snapshot is not provided, falls back
    to lexicon_snapshot from workspace_v1.txt.
  - If markov_model or exemplar_memory is configured and the matching flags are
    omitted, the answer path auto-uses those advisory artifacts.
  - If a workspace advisory artifact is configured but the artifact is absent,
    normal answer flow falls back cleanly without enabling that advisory layer.
  - Auto-used advisory artifacts keep the same runtime boundaries: graph stays
    bounded and subordinate to lexical retrieval, exemplar stays advisory only,
    and Markov stays bounded phrasing only.

- ask / chat
  - End-user facing commands that accept plain text.
  - Internally materialize a PromptPackV1 and run the normal answer pipeline.
  - Use workspace defaults for snapshot/sig map/lexicon when not provided.
  - If default_expand=1 is set and --expand is omitted, enable bounded query
    expansion in the wrapped answer path.
  - If markov_model, exemplar_memory, or graph_relevance are configured and the
    matching flags are omitted, the wrapped answer path auto-uses those
    advisory artifacts.
  - If a workspace advisory artifact is configured but the artifact is absent,
    normal ask/chat flow falls back cleanly without warnings or failures.
  - Auto-used advisory artifacts keep the same runtime boundaries in wrapped
    flows: graph stays bounded and subordinate to lexical retrieval, exemplar
    stays advisory only, and Markov stays bounded phrasing only.

Validation rules
----------------
- If merged_snapshot is present, merged_sig_map must also be present.
- If a required key is missing, commands must fail with a clear message that
  names the missing key and suggests the appropriate load command.

Determinism notes
-----------------
- The workspace is not part of artifact hashing.
- Determinism is preserved because artifact ids fully determine content.
- If the workspace points at different artifacts, results can change, but those
  changes are still deterministic and replayable.
