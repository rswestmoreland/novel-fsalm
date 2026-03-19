Artifacts (Content-Addressed Storage)
====================================

Overview
--------
An artifact is a canonical byte blob such as a PromptPack, ReplayLog,
EvidenceBundleV1, ConversationPackV1, or other schema-defined record.
Artifacts are addressed by their content hash (Hash32).

Novel provides a filesystem-based artifact store:
- put(bytes) returns hash = blake3(bytes)
- get(hash) returns the stored bytes

File layout
-----------
Filesystem layout for an artifact hash H:
 root/aa/bb/<hex(H)>.bin
where:
 aa = first byte of H in hex
 bb = second byte of H in hex

Atomic write behavior
---------------------
Best-effort atomic writes:
- write bytes to a deterministic temp file in the same directory
- rename temp to final path
If the final path exists, the store treats it as already written.

Determinism notes
-----------------
Determinism is defined over artifact bytes and hashes.
Filesystem timestamps and metadata are not part of the artifact identity.


Wikipedia ingest artifacts
------------------------------------
- WikiIngestManifestV1
 - Canonical manifest listing the FrameSegment hashes produced by a Wikipedia TSV ingest run.
 - The manifest artifact hash is the stable 'commit id' for the ingest.
 - See docs/INGEST_WIKI.md.

Known artifact formats
----------------------
- FrameSegment v1: src/frame_segment.rs
- IndexSegment v1: docs/INDEX_SEGMENT_V1.md (src/index_segment.rs)
- LexiconSegment v1: docs/LEXICON_SEGMENT_V1.md (src/lexicon_segment.rs)
- LexiconSnapshot v1: docs/LEXICON_SNAPSHOT_V1.md (src/lexicon_snapshot.rs)
- PromptPack v1: docs/PROMPT_PACK.md
- ReplayLog v1: docs/REPLAY.md
- IndexSnapshot v1: docs/INDEX_SNAPSHOT_V1.md (src/index_snapshot.rs)
- IndexPack v1: docs/INDEX_PACK_V1.md (src/index_pack.rs)
- EvidenceBundle v1: docs/EVIDENCE_BUNDLE_V1.md (src/evidence_bundle.rs)
- EvidenceSet v1: docs/EVIDENCE_SET_V1.md (src/evidence_set.rs)
- HitList v1: docs/HIT_LIST_V1.md (src/hit_list.rs)
- CompactionReport v1: docs/COMPACTION_V1.md (src/compaction_report.rs)
- SegmentSig v1: docs/SEGMENT_SIG_V1.md (src/segment_sig.rs)
- IndexSigMap v1: docs/INDEX_SIG_MAP_V1.md (src/index_sig_map.rs)
- RealizerDirectives v1: docs/REALIZER_DIRECTIVES_V1.md (src/realizer_directives.rs)
- PlannerHints v1: docs/PLANNER_HINTS_V1.md (src/planner_hints.rs)
- Forecast v1: docs/FORECAST_V1.md (src/forecast.rs)
- MarkovHints v1: docs/MARKOV_HINTS_V1.md (src/markov_hints.rs)
- MarkovModel v1: docs/MARKOV_MODEL_V1.md (src/markov_model.rs)
- MarkovTrace v1: docs/MARKOV_TRACE_V1.md (src/markov_trace.rs)
- ExemplarMemory v1: docs/EXEMPLAR_MEMORY_V1.md (src/exemplar_memory.rs)
  - Offline builder helpers live in src/exemplar_build.rs and currently mine bounded rows from PromptPack, ConversationPack, and MarkovTrace.
  - CLI builder command: `build-exemplar-memory`
- GraphRelevance v1: docs/GRAPH_RELEVANCE_V1.md (src/graph_relevance.rs)
  - CLI builder command: `build-graph-relevance`
  - Offline builder helpers live in src/graph_build.rs and currently mine conservative 1-hop rows from FrameSegmentV1.
- GoldenPackReport v1: docs/GOLDEN_PACK_V1.md (src/golden_pack_report.rs)
- ScaleDemoAnswersReport v1 (schema version 2): src/scale_demo.rs
- ScaleDemoScaleReport v1 (schema version 2): docs/SCALE_REPORT_V1.md (src/scale_report.rs)
