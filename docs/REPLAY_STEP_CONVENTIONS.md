Replay Step Conventions
=================================

Purpose
-------
ReplayLog records a sequence of steps. Each step contains:
- name: a stable step name string
- inputs: set of artifact hashes
- outputs: set of artifact hashes

The ReplayLog schema does not label individual hashes.
This doc defines the required sets per step name so downstream tooling
can interpret steps consistently.

General rules
-------------
- inputs and outputs are unordered sets. Canonical encoding sorts.
- Optional artifacts (sig maps, lexicon snapshots, pragmatics frames) are
 included when enabled.
- Step names include a "-vN" suffix when the set definition changes.

Status
------
Implemented in code now:
- prompt
- retrieve-v1
- build-evidence-v1
- realizer-directives-v1
- planner-hints-v1
- forecast-v1
- answer-v1
- markov-trace-v1

Defined here for later stages (not yet emitted by all CLIs):
- ingest-wiki-v1
- ingest-wiki-xml-v1
- build-index-v1
- build-lexicon-snapshot-v1


Step definitions
----------------

prompt
 Purpose:
 Record a PromptPack artifact.
 Inputs:
 (none)
 Outputs:
 - PromptPack hash

retrieve-v1
 Purpose:
 Query an IndexSnapshot and produce deterministic hit lists.
 Inputs:
 - IndexSnapshot hash
 - IndexSigMap hash (optional)
 - query-id bytes hash (required; stored as a raw blob)
 Outputs:
 - HitList hash (HitListV1)

 query-id blob layout (raw bytes):
 prefix: "retrieve-v1\0"

build-evidence-v1
 Purpose:
 Build an EvidenceBundleV1 from a HitList.
 Inputs:
 - HitList hash (HitListV1)
 - IndexSnapshot hash
 - IndexSigMap hash (optional)
 Outputs:
 - EvidenceBundle hash

realizer-directives-v1
 Purpose:
 Record RealizerDirectives derived from a PragmaticsFrame.
 Inputs:
 - PragmaticsFrame hash(es)
 Outputs:
 - RealizerDirectives hash

planner-hints-v1
 Purpose:
 Record PlannerHints derived from EvidenceBundle (+ optional PragmaticsFrame).
 Inputs:
 - PragmaticsFrame hash(es) (optional)
 - EvidenceBundle hash
 Outputs:
 - PlannerHints hash

forecast-v1
 Purpose:
 Record Forecast derived from PlannerHints (+ optional PragmaticsFrame).
 Inputs:
 - PragmaticsFrame hash(es) (optional)
 - PlannerHints hash
 Outputs:
 - Forecast hash

answer-v1
 Purpose:
 Full answering loop (retrieve + evidence + plan + realize).
 Inputs:
 - PromptPack hash
 - IndexSnapshot hash
 - IndexSigMap hash (optional)
 - LexiconSnapshot hash (optional; when query expansion enabled)
 - PragmaticsFrame hash(es) (optional; when provided)
 - EvidenceBundle hash
 - RealizerDirectives hash (optional)
 - PlannerHints hash
 - Forecast hash
 Outputs:
 - Answer text hash (stored as raw bytes)
 - EvidenceSet hash

markov-trace-v1
 Purpose:
 Record the MarkovTrace token stream used by the realizer for this answer.
 Inputs:
 - All answer-v1 inputs
 - Answer text hash (raw bytes)
 Outputs:
 - MarkovTrace hash

markov-hints-v1
 Purpose:
 Derive MarkovHintsV1 from a MarkovModelV1 for bounded surface-template selection.
 Inputs (recommended):
 - Answer query inputs (prompt/snapshot, optional sig-map, optional lexicon-snapshot)
 - PragmaticsFrame hash(es) (optional)
 - RealizerDirectives hash
 - MarkovModel hash
 Outputs:
 - MarkovHints hash


Planned steps
-------------

ingest-wiki-v1 (planned)
 Purpose:
 Ingest Wikipedia TSV dump into FrameSegments.
 Inputs (recommended):
 - source bytes hash (TSV dump) (if stored)
 - ingest config hash (if stored)
 Outputs:
 - FrameSegment hash(es)
 - ingest manifest hash (if/when added)

ingest-wiki-xml-v1 (planned)
 Purpose:
 Ingest Wikipedia XML dump into FrameSegments.
 Inputs (recommended):
 - source bytes hash (XML) (if stored)
 - ingest config hash (if stored)
 Outputs:
 - FrameSegment hash(es)
 - ingest manifest hash (if/when added)

build-index-v1 (planned)
 Purpose:
 Build IndexSnapshot from FrameSegments.
 Inputs:
 - FrameSegment hash(es)
 Outputs:
 - IndexSegment hash(es)
 - IndexSnapshot hash
 - SegmentSig hash(es) (if produced)
 - IndexSigMap hash (if produced)

build-lexicon-snapshot-v1 (planned)
 Purpose:
 Build LexiconSnapshot from LexiconSegments.
 Inputs:
 - LexiconSegment hash(es)
 Outputs:
 - LexiconSnapshot hash
