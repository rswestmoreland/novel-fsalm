// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Replay step naming and conventions.
//!
//! The ReplayLog format (src/replay.rs) records a list of steps.
//! Each step records a name plus sets of input/output artifact hashes.
//!
//! Conventions:
//! - Step names are stable strings and may include a "-vN" suffix.
//! - inputs/outputs are treated as unordered sets. Canonical encoding sorts.
//! - Optional artifacts (sig maps, lexicon snapshots, pragmatics frames) are
//! included when enabled so replay stays fully determined by hashes.

use crate::hash::Hash32;
use crate::replay::ReplayStep;

/// Step name for the answer loop (planner + realizer).
pub const STEP_ANSWER_V1: &str = "answer-v1";

/// Replay step name: realizer-directives-v1.
///
/// This step records RealizerDirectives artifacts derived from PragmaticsFrame.
pub const STEP_REALIZER_DIRECTIVES_V1: &str = "realizer-directives-v1";

/// Derive PlannerHintsV1 from evidence + pragmatics (if present).
pub const STEP_PLANNER_HINTS_V1: &str = "planner-hints-v1";

/// Derive ForecastV1 from evidence + PlannerHintsV1 + pragmatics (if present).
pub const STEP_FORECAST_V1: &str = "forecast-v1";

/// Derive MarkovTraceV1 from the realized answer and guidance inputs.
///
/// This step records the per-turn surface-choice token stream used by the realizer.
pub const STEP_MARKOV_TRACE_V1: &str = "markov-trace-v1";

/// Derive MarkovHintsV1 from a MarkovModelV1 and bounded context inputs.
///
/// MarkovHintsV1 is advisory-only guidance used for surface-template selection.
pub const STEP_MARKOV_HINTS_V1: &str = "markov-hints-v1";

/// Step name for ingest-wiki (TSV dump -> FrameSegments).
pub const STEP_INGEST_WIKI_V1: &str = "ingest-wiki-v1";

/// Step name for ingest-wiki-xml (Wikipedia XML -> FrameSegments).
pub const STEP_INGEST_WIKI_XML_V1: &str = "ingest-wiki-xml-v1";

/// Step name for building an IndexSnapshot from FrameSegments.
pub const STEP_BUILD_INDEX_V1: &str = "build-index-v1";

/// Step name for building a LexiconSnapshot from LexiconSegments.
pub const STEP_BUILD_LEXICON_SNAPSHOT_V1: &str = "build-lexicon-snapshot-v1";

/// Step name for retrieval against an IndexSnapshot (query-index).
pub const STEP_RETRIEVE_V1: &str = "retrieve-v1";

/// Step name for building an EvidenceBundle from retrieval hits.
pub const STEP_BUILD_EVIDENCE_V1: &str = "build-evidence-v1";

/// Construct a ReplayStep from input/output hash slices.
///
/// Note: ReplayLog encoding canonicalizes by sorting inputs/outputs.
pub fn step_from_slices(name: &str, inputs: &[Hash32], outputs: &[Hash32]) -> ReplayStep {
    ReplayStep {
        name: name.to_string(),
        inputs: inputs.to_vec(),
        outputs: outputs.to_vec(),
    }
}
