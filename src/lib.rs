// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

#![forbid(unsafe_code)]
#![deny(missing_docs)]
//! FSA-LM prototype runtime crate.
//!
//! This crate is organized around deterministic, canonical artifacts:
//! - codec: canonical byte encoding primitives
//! - hash: content hashing and stable ID derivation
//! - determinism: stable sorting and tie-break helpers
//!
//! Higher layers (PromptPack, jobs, IR frames, tools, verifier, decoder traits)
//! will be added in subsequent stages.

pub mod codec;
pub mod determinism;
pub mod hash;

/// Deterministic in-process caches.
pub mod cache;

pub mod artifact;
pub mod replay;

pub mod prompt_pack;

pub mod net;

/// Artifact sync protocol and helpers.
pub mod artifact_sync_v1;

/// Manifest-driven artifact replication over TCP.
pub mod artifact_sync;

/// PromptPack artifact helpers (store/load PromptPack by hash).
pub mod prompt_artifact;

/// ReplayLog artifact helpers (store/load ReplayLog by hash).
pub mod replay_artifact;

/// Replay step naming and conventions.
pub mod replay_steps;

/// Knowledge frame schema and id types.
pub mod frame;

/// Knowledge frame segments.
pub mod frame_segment;

/// FrameSegment persistence helpers.
pub mod frame_store;

/// Per-segment postings index for FrameSegment.
pub mod index_segment;

/// IndexSegment persistence helpers.
pub mod index_store;

/// IndexSnapshot manifest linking FrameSegment hashes to IndexSegment hashes.
pub mod index_snapshot;

/// IndexSnapshot persistence helpers.
pub mod index_snapshot_store;
/// Query-time index lookup and scoring.
pub mod index_query;

/// Canonical retrieval hit list artifact.
pub mod hit_list;

/// HitList artifact helpers.
pub mod hit_list_artifact;

/// Golden pack report schema and codec.
pub mod golden_pack;

/// Golden pack report artifact helpers.
pub mod golden_pack_artifact;

/// Golden pack runner.
pub mod golden_pack_run;

/// Golden pack turn-pairs report schema and codec.
pub mod golden_pack_turn_pairs;

/// Golden pack turn-pairs report artifact helpers.
pub mod golden_pack_turn_pairs_artifact;

/// Golden pack turn-pairs runner.
pub mod golden_pack_turn_pairs_run;

/// Golden pack conversation report schema and codec.
pub mod golden_pack_conversation;

/// Golden pack conversation report artifact helpers.
pub mod golden_pack_conversation_artifact;

/// Golden pack conversation runner.
pub mod golden_pack_conversation_run;

/// Sharded ingest manifest schema.
pub mod shard_manifest;

/// ShardManifest artifact helpers.
pub mod shard_manifest_artifact;

/// Reduce manifest schema.
pub mod reduce_manifest;

/// ReduceManifest artifact helpers.
pub mod reduce_manifest_artifact;

/// Sharded index reduce/merge.
pub mod reduce_index;

/// Sharding helpers.
pub mod sharding_v1;

/// Retrieval policy configuration and counters.
pub mod retrieval_policy;

/// Lexicon-driven query expansion configuration.
pub mod query_expansion;

/// Bridge expansion budget contract.
pub mod expansion_budget;

/// Expanded query feature vector schema.
pub mod expanded_qfv;

/// Bridge expansion builder.
pub mod expansion_builder;

/// Bridge expansion integration layer.
pub mod bridge_expansion;

/// Answer planning intermediate representation.
pub mod answer_plan;

/// Planner v1 (EvidenceBundleV1 -> AnswerPlanV1).
pub mod planner_v1;

/// Planner hints schema.
pub mod planner_hints;

/// Forecast schema.
pub mod forecast;
pub mod planner_hints_artifact;
pub mod forecast_artifact;

/// Markov/PPM hints schema.
pub mod markov_hints;

/// Markov/PPM model schema.
pub mod markov_model;

/// Markov/PPM model artifact helpers.
pub mod markov_model_artifact;

/// Markov/PPM hints artifact helpers.
pub mod markov_hints_artifact;

/// Markov choice trace schema.
pub mod markov_trace;

/// Markov choice trace artifact helpers.
pub mod markov_trace_artifact;

/// Offline Markov training + hint derivation.
pub mod markov_train;

/// Runtime Markov hint derivation helpers.
pub mod markov_runtime;

/// Quality gate consolidation helpers.
pub mod quality_gate_v1;

/// Realizer v1 (AnswerPlanV1 + evidence -> text).
pub mod realizer_v1;

/// Realizer directives schema.
pub mod realizer_directives;

/// Realizer directives artifact helpers.
pub mod realizer_directives_artifact;

/// Compaction report schema and codec.
pub mod compaction_report;

/// CompactionReport artifact helpers.
pub mod compaction_report_artifact;

/// IndexPackV1 schema and codec.
pub mod index_pack;

/// Index compaction implementation.
pub mod index_compaction;

/// Segment signatures used for query-time gating.
pub mod segment_sig;
/// SegmentSig persistence helpers.
pub mod segment_sig_store;

/// Debug bundle exporter.
pub mod debug_bundle;

/// IndexSigMap sidecar manifest.
pub mod index_sig_map;

/// IndexSigMap persistence helpers.
pub mod index_sig_map_store;
/// Query-time retrieval gating helpers.
pub mod retrieval_gating;

/// Evidence bundle schema and codec.
pub mod evidence_bundle;

/// Evidence set schema for claim-to-evidence trace.
pub mod evidence_set;

/// EvidenceBundle artifact helpers.
pub mod evidence_artifact;
/// EvidenceSet artifact helpers.
pub mod evidence_set_artifact;

/// EvidenceSet verifiers.
pub mod evidence_set_verify;


/// Evidence builder.
pub mod evidence_builder;

/// Tokenization and term id strategy.
pub mod tokenizer;

/// Metaphonetic preprocessor.
pub mod metaphone;

/// Pragmatics control-signal schema.
pub mod pragmatics_frame;
pub mod pragmatics_extract;
/// PragmaticsFrame persistence helpers.
pub mod pragmatics_frame_store;

/// Retrieval control-signal attachment point.
pub mod retrieval_control;

/// Lexicon schema for Wiktionary ingestion.
pub mod lexicon;

/// Lexicon segment codec.
pub mod lexicon_segment;
pub mod lexicon_segmenting;

/// LexiconSegment persistence helpers.
pub mod lexicon_segment_store;

/// Lexicon snapshot manifest codec.
pub mod lexicon_snapshot;

/// LexiconSnapshot persistence helpers.
pub mod lexicon_snapshot_store;

/// Lexicon snapshot read helpers for query expansion.
pub mod lexicon_expand_lookup;

/// Deterministic workload generator for scale demos.
pub mod workload_gen;

/// Scale demo orchestration helpers.
pub mod scale_demo;

/// Scale demo scale report schema and codec.
pub mod scale_report;

/// ScaleDemoScaleReport artifact helpers.
pub mod scale_report_artifact;

/// LexiconSnapshot validation helpers.
pub mod lexicon_snapshot_validate;

/// LexiconSnapshot builder from LexiconSegment hashes.
pub mod lexicon_snapshot_builder;

/// Wikipedia TSV ingestion.
pub mod wiki_ingest;
mod wiki_xml;
