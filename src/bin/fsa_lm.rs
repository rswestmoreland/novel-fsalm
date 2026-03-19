// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

// FSA-LM CLI.
//
// ASCII-only comments.
// No extra crates for argument parsing.
//
// This CLI provides basic artifact and PromptPack workflows, and a TCP framed
// artifact exchange server/client for experimentation.

use fsa_lm::artifact::{ArtifactStore, FsArtifactStore};
use fsa_lm::hash::{blake3_hash, hex32, Hash32};
use fsa_lm::prompt_pack::{Message, PromptIds, PromptLimits, PromptPack, Role};
use fsa_lm::prompt_artifact::{get_prompt_pack, put_prompt_pack};
use fsa_lm::replay::{ReplayLog, ReplayStep};
use fsa_lm::replay_artifact::{append_prompt_step, get_replay_log, put_replay_log};
use fsa_lm::replay_steps::{
    step_from_slices, STEP_ANSWER_V1, STEP_BUILD_EVIDENCE_V1, STEP_RETRIEVE_V1,
    STEP_REALIZER_DIRECTIVES_V1, STEP_PLANNER_HINTS_V1, STEP_FORECAST_V1,
    STEP_MARKOV_HINTS_V1, STEP_MARKOV_TRACE_V1, STEP_CONTEXT_ANCHORS_V1, STEP_PUZZLE_SKETCH_V1,
    STEP_PROOF_ARTIFACT_V1,
};
use fsa_lm::frame::{DocId, FrameRowV1, Id64, SourceId};
use fsa_lm::frame_segment::FrameSegmentV1;
use fsa_lm::frame_segment::FRAME_SEGMENT_MAGIC;
use fsa_lm::index_segment::IndexSegmentV1;
use fsa_lm::index_snapshot::{IndexSnapshotEntryV1, IndexSnapshotV1};
use fsa_lm::index_query::{query_terms_from_text, search_snapshot, search_snapshot_cached, search_snapshot_gated, search_snapshot_cached_gated, QueryTermsCfg, SearchCfg};
use fsa_lm::retrieval_control::RetrievalControlV1;
use fsa_lm::retrieval_policy::{apply_retrieval_policy_from_text_v1_with_anchors, RetrievalPolicyCfgV1};

use fsa_lm::logic_solver_v1::{extract_puzzle_block, parse_puzzle_block_v1, solve_puzzle_v1, LogicSolveCfgV1};
use fsa_lm::puzzle_compile_v1::{try_compile_puzzle_spec_from_sketch_and_constraints_v1, PuzzleCompileErrV1};
use fsa_lm::proof_artifact_store::put_proof_artifact_v1;
use fsa_lm::planner_hints::{PH_FLAG_PREFER_CLARIFY, PH_FLAG_PREFER_STEPS};
use fsa_lm::forecast::{
    ForecastIntentKindV1, ForecastIntentV1, ForecastQuestionV1, FORECAST_V1_MAX_INTENTS,
    FORECAST_V1_MAX_QUESTIONS,
};
use fsa_lm::realizer_directives::{
    RealizerDirectivesV1, REALIZER_DIRECTIVES_V1_VERSION, StyleV1, ToneV1,
};
use fsa_lm::exemplar_memory_artifact::{get_exemplar_memory_v1, put_exemplar_memory_v1};
use fsa_lm::graph_relevance_artifact::{get_graph_relevance_v1, put_graph_relevance_v1};
use fsa_lm::exemplar_runtime::{
    apply_exemplar_advisory_v1, lookup_exemplar_advisory_v1, ExemplarAdvisoryV1,
    EXAD_MATCH_CLARIFIER, EXAD_MATCH_COMPARISON, EXAD_MATCH_RESPONSE_MODE,
    EXAD_MATCH_STEPS, EXAD_MATCH_STRUCTURE, EXAD_MATCH_SUMMARY, EXAD_MATCH_TONE,
};
use fsa_lm::frame::derive_id64;

use fsa_lm::context_anchors::{build_context_anchors_v1, ContextAnchorsCfgV1};
use fsa_lm::context_anchors_artifact::put_context_anchors_v1;
use fsa_lm::planner_v1::{plan_from_evidence_bundle_v1_with_guidance, PlannerCfgV1, PlannerOutputV1};
use fsa_lm::planner_hints_artifact::put_planner_hints_v1;
use fsa_lm::forecast_artifact::put_forecast_v1;
use fsa_lm::quality_gate_v1::{
    build_markov_trace_tokens_v1, derive_directives_opt,
    derive_markov_hints_surface_choices_opt, realize_with_quality_gate_v1,
};
use fsa_lm::shard_manifest::{ShardEntryV1, ShardManifestV1, ShardOutputV1, SHARD_MANIFEST_V1_VERSION};
use fsa_lm::shard_manifest_artifact::{get_shard_manifest_v1, put_shard_manifest_v1};
use fsa_lm::sharding_v1::{ShardCfgV1, SHARD_MAPPING_DOC_ID_HASH32_V1};
use fsa_lm::reduce_index::{reduce_index_v1, ReduceIndexResultV1};
use fsa_lm::realizer_v1::{
    RealizerCfgV1,
};
use fsa_lm::markov_hints::MarkovHintsV1;
use fsa_lm::markov_hints_artifact::put_markov_hints_v1;
use fsa_lm::markov_model::MarkovTokenV1;
use fsa_lm::markov_trace::{MarkovTraceV1, MARKOV_TRACE_V1_VERSION};
use fsa_lm::markov_model_artifact::{get_markov_model_v1, put_markov_model_v1};
use fsa_lm::markov_trace_artifact::{get_markov_trace_v1, put_markov_trace_v1};
use fsa_lm::markov_train::{markov_corpus_hash_v1, MarkovTrainCfgV1, MarkovTrainerV1};
use fsa_lm::evidence_builder::{build_evidence_bundle_v1_from_hits, build_evidence_bundle_v1_from_hits_cached, EvidenceBuildCfgV1};
use fsa_lm::evidence_bundle::{EvidenceItemDataV1, EvidenceItemV1, EvidenceLimitsV1, ProofRefV1};
use fsa_lm::evidence_artifact::put_evidence_bundle_v1;
use fsa_lm::evidence_set::{EvidenceRowRefV1, EvidenceSetItemV1, EvidenceSetV1};
use fsa_lm::evidence_set_artifact::put_evidence_set_v1;
use fsa_lm::evidence_set_verify::verify_evidence_set_v1;
use fsa_lm::hit_list::{HitListV1, HitV1};
use fsa_lm::hit_list_artifact::put_hit_list_v1;
use fsa_lm::cache::{Cache2Q, CacheCfgV1, CacheStatsV1};

use fsa_lm::scale_report_artifact::put_scale_demo_scale_report_v1;

use fsa_lm::lexicon_snapshot_builder::build_lexicon_snapshot_v1_from_segments;

use fsa_lm::lexicon_snapshot_validate::validate_lexicon_snapshot_v1_disjoint_owners;

use fsa_lm::workspace::{read_workspace_v1, write_workspace_v1_atomic, WorkspaceV1, WORKSPACE_V1_FILENAME};

use fsa_lm::conversation_pack::{ConversationLimits, ConversationMessage, ConversationPackV1, ConversationPresentationModeV1 as PresentationModeV1, ConversationRole};
use fsa_lm::conversation_pack_artifact::{get_conversation_pack, put_conversation_pack};
use fsa_lm::exemplar_build::{
    ExemplarBuildConfigV1, ExemplarBuildInputV1, ExemplarSourceArtifactV1,
    finalize_exemplar_memory_v1, mine_exemplar_rows_from_sources_v1,
    prepare_exemplar_build_plan_v1,
};
use fsa_lm::exemplar_memory::ExemplarSupportSourceKindV1;
use fsa_lm::graph_build::{
    GraphBuildConfigV1, GraphBuildInputV1, GraphBuildSourceKindV1, GraphSourceArtifactV1,
    empty_graph_relevance_v1, finalize_graph_relevance_v1, mine_graph_rows_from_sources_v1,
    prepare_graph_build_plan_v1,
};

use fsa_lm::wiktionary_build::ingest_wiktionary_xml_to_lexicon_snapshot_v1;
use fsa_lm::wiktionary_ingest::WiktionaryParseCfg;

use fsa_lm::pragmatics_extract::{extract_pragmatics_frames_for_prompt_pack_v1, PragmaticsExtractCfg};
use fsa_lm::pragmatics_frame_store::put_pragmatics_frame_v1;
use fsa_lm::pragmatics_frame_store::get_pragmatics_frame_v1;

use fsa_lm::realizer_directives_artifact::put_realizer_directives_v1;

use fsa_lm::compaction_report::CompactionCfgV1;
use fsa_lm::index_compaction::compact_index_snapshot_v1;

use fsa_lm::frame_store::{get_frame_segment_v1, put_frame_segment_v1};
use fsa_lm::tokenizer::{term_freqs_from_text, term_id_from_token, TokenIter, TokenizerCfg};
use fsa_lm::wiki_ingest::{ingest_wiki_tsv, ingest_wiki_tsv_sharded, WikiIngestCfg, ingest_wiki_xml, ingest_wiki_xml_sharded};
use fsa_lm::scale_demo::{
    run_scale_demo_build_answers_v1, run_scale_demo_build_evidence_bundles_v1,
    run_scale_demo_build_index_from_manifest_v1, run_scale_demo_generate_and_ingest_frames_v1,
    run_scale_demo_generate_and_store_prompts_v1, run_scale_demo_generate_only_v1,
    build_scale_demo_scale_report_v1,
    ScaleDemoCfgV1, SCALE_DEMO_V1_VERSION,
};
use fsa_lm::workload_gen::{WorkloadCfgV1, WORKLOAD_GEN_V1_VERSION};
use fsa_lm::net;
use fsa_lm::artifact_sync::{run_sync_server_v1, sync_reduce_v1, sync_reduce_batch_v1, sync_lexicon_v1, SyncClientCfgV1, SyncServerCfgV1};
use fsa_lm::debug_bundle::{export_debug_bundle_v1, DebugBundleCfgV1};
use bzip2::read::BzDecoder;

use std::env;
use std::fs;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn usage() -> &'static str {
    r#"fsa_lm <cmd> [args]

Commands:

  hash [--file <path>]
  put [--root <dir>] [--file <path>]
  get [--root <dir>] <hash_hex>
  show-workspace [--root <dir>]
  show-conversation [--root <dir>] <conversation_pack_hash_hex>
  ask [--root <dir>] [--seed <u64>] [--max_tokens <u32>] [--role <role>] [--session-file <path>] [--conversation <hash32hex>] [--text <text>] [answer flags...] [<text> ...]  (logic puzzles: may ask clarifying questions; optional [puzzle] block)
  chat [--root <dir>] [--seed <u64>] [--max_tokens <u32>] [--system <text>] [--resume <conversation_pack_hash_hex>] [--session-file <path>] [--autosave] [answer flags...]  (logic puzzles: may ask clarifying questions; optional [puzzle] block)
  prompt [--root <dir>] [--seed <u64>] [--max_tokens <u32>] [--role <role>] <text>
  replay-decode [--root <dir>] <hash_hex>
  replay-new [--root <dir>]
  replay-add-prompt [--root <dir>] <replay_hash_hex> <prompt_hash_hex> [--name <step_name>]
  frame-seg-demo [--root <dir>] [--text <text>] [--chunk_rows <u32>]
  frame-seg-show [--root <dir>] <segment_hash_hex>
  ingest-wiki --dump <path> [--root <dir>] [--seg_mb <u32>] [--row_kb <u32>] [--chunk_rows <u32>] [--max_docs <u64>] [--shards <n> --shard-id <k>]
  ingest-wiki-xml (--xml <path> | --xml-bz2 <path>) [--root <dir>] [--seg_mb <u32>] [--row_kb <u32>] [--chunk_rows <u32>] [--max_docs <u64>] [--shards <n> --shard-id <k>]
  ingest-wiki-sharded --dump <path> --shards <n> [--root <dir>] [--seg_mb <u32>] [--row_kb <u32>] [--chunk_rows <u32>] [--max_docs <u64>] [--out-file <path>]
  ingest-wiki-xml-sharded (--xml <path> | --xml-bz2 <path>) --shards <n> [--root <dir>] [--seg_mb <u32>] [--row_kb <u32>] [--chunk_rows <u32>] [--max_docs <u64>] [--out-file <path>]
  load-wikipedia (--dump <path> | --xml <path> | --xml-bz2 <path>) --shards <n> [--root <dir>] [--seg_mb <u32>] [--row_kb <u32>] [--chunk_rows <u32>] [--max_docs <u64>] [--out-file <path>]
  load-wiktionary (--xml <path> | --xml-bz2 <path>) --segments <n> [--root <dir>] [--max_pages <n>] [--stats] [--out-file <path>]
  build-index [--root <dir>]
  build-index-sharded --shards <n> [--root <dir>] [--manifest <hash32hex>] [--out-file <path>]
  reduce-index --root <dir> --manifest <hash32hex> [--out-file <path>]
  run-workflow --root <dir> --dump <path> --shards <n> [--seg_mb <u32>] [--row_kb <u32>] [--chunk_rows <u32>] [--max_docs <u64>] [--out-file <path>] [--sync-addr <ip:port> --sync-root <dir>] [--max_chunk_kb <n>] [--max_artifact_mb <n>] [--rw_timeout_ms <n>]
  export-debug-bundle --root <dir> --out <path> [--include-hash <hash32hex> ...]

  ingest-wiktionary-xml --root <dir> (--xml <path> | --xml-bz2 <path>) --segments <n> [--max_pages <n>] [--stats] [--out-file <path>]
  build-lexicon-snapshot --root <dir> --segment <hash32hex> [--segment <hash32hex> ...] [--out-file <path>]
  validate-lexicon-snapshot --root <dir> --snapshot <hash32hex>
  build-pragmatics --root <dir> --prompt <hash32hex> [--source-id <u64>] [--tok-max-bytes <n>] [--lexicon-snapshot <hash32hex>] [--out-file <path>]
  query-index --root <dir> --snapshot <hash32hex> [--sig-map <hash32hex>] --text <string> [--k <n>] [--meta] [--cache-stats]
  build-evidence --root <dir> --snapshot <hash32hex> [--sig-map <hash32hex>] --text <string> [--k <n>] [--meta] [--max_items <n>] [--max_bytes <n>] [--no_sketch] [--no_verify] [--score_model <id>] [--verbose] [--cache-stats]
  answer --root <dir> --prompt <hash32hex> [--snapshot <hash32hex> [--sig-map <hash32hex>]] [--pragmatics <hash32hex> ...] [--k <n>] [--meta] [--max_terms <n>] [--no_ties] [--expand [--lexicon-snapshot <hash32hex>] [--graph-relevance <hash32hex>]] [--plan_items <n>] [--verify-trace <0|1>] [--markov-model <hash32hex>] [--markov-max-choices <n>] [--exemplar-memory <hash32hex>] [--presentation <user|operator>] [--out-file <path>]
  build-markov-model --root <dir> --replay <hash32hex> [--replay <hash32hex> ...] [--replay-file <path>] [--max-replays <n>] [--max-traces <n>] [--order <n>] [--max-next <n>] [--max-states <n>] [--out-file <path>]
  build-exemplar-memory --root <dir> [--replay <hash32hex> ...] [--prompt <hash32hex> ...] [--golden-pack <hash32hex> ...] [--golden-pack-conversation <hash32hex> ...] [--conversation-pack <hash32hex> ...] [--markov-trace <hash32hex> ...] [--max-inputs-total <n>] [--max-inputs-per-source-kind <n>] [--max-rows <n>] [--max-support-refs-per-row <n>] [--out-file <path>]
  build-graph-relevance --root <dir> [--frame-segment <hash32hex> ...] [--replay <hash32hex> ...] [--prompt <hash32hex> ...] [--conversation-pack <hash32hex> ...] [--max-inputs-total <n>] [--max-inputs-per-source-kind <n>] [--max-rows <n>] [--max-edges-per-row <n>] [--max-terms-per-frame-row <n>] [--max-entities-per-frame-row <n>] [--out-file <path>]
  inspect-markov-model --root <dir> --model <hash32hex> [--top-states <n>] [--top-next <n>] [--out-file <path>]
  scale-demo [--seed <u64>] [--docs <n>] [--queries <n>] [--min_doc_tokens <n>] [--max_doc_tokens <n>] [--vocab <n>] [--query_tokens <n>] [--tie_pair <0|1>] [--ingest <0|1>] [--build_index <0|1>] [--prompts <0|1>] [--evidence <0|1>] [--answer <0|1>] [--root <dir>] [--out-file <path>]
  golden-pack [--root <dir>] [--expect <hash32hex>] [--out-file <path>]
  golden-pack-turn-pairs [--root <dir>] [--expect <hash32hex>] [--out-file <path>]
  golden-pack-conversation [--root <dir>] [--expect <hash32hex>] [--out-file <path>]
  compact-index --root <dir> --snapshot <hash32hex> [--target-bytes <n>] [--max-out-segments <n>] [--dry-run] [--verbose]
  serve [--root <dir>] [--addr <ip:port>]
  serve-sync [--root <dir>] [--addr <ip:port>] [--max_chunk_kb <n>] [--max_artifact_mb <n>] [--rw_timeout_ms <n>]
  send-put [--addr <ip:port>] [--file <path>]
  send-get [--addr <ip:port>] <hash_hex>
  sync-reduce --root <dir> --addr <ip:port> --reduce-manifest <hash32hex> [--out-file <path>] [--max_chunk_kb <n>] [--max_artifact_mb <n>] [--rw_timeout_ms <n>]
  sync-reduce-batch --root <dir> --addr <ip:port> --reduce-manifests <path> [--out-file <path>] [--max_chunk_kb <n>] [--max_artifact_mb <n>] [--rw_timeout_ms <n>]
  sync-lexicon --root <dir> --addr <ip:port> --lexicon-snapshot <hash32hex> [--out-file <path>] [--max_chunk_kb <n>] [--max_artifact_mb <n>] [--rw_timeout_ms <n>]

Roles:

  system | user | assistant
"#
}
fn default_root() -> PathBuf {
    if let Ok(v) = env::var("FSA_LM_STORE") {
        return PathBuf::from(v);
    }
    PathBuf::from("./fsa_lm_store")
}

fn collect_bin_paths(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    let rd = match std::fs::read_dir(dir) {
        Ok(r) => r,
        Err(_) => return,
    };

    let mut entries: Vec<std::path::PathBuf> = Vec::new();
    for ent in rd {
        if let Ok(ent) = ent {
            entries.push(ent.path());
        }
    }
    entries.sort_by(|a, b| a.to_string_lossy().cmp(&b.to_string_lossy()));

    for p in entries {
        let md = match std::fs::metadata(&p) {
            Ok(m) => m,
            Err(_) => continue,
        };
        if md.is_dir() {
            collect_bin_paths(&p, out);
            continue;
        }
        if !md.is_file() {
            continue;
        }
        if p.extension().map(|e| e == "bin").unwrap_or(false) {
            out.push(p);
        }
    }
}


fn read_all_from(path_opt: Option<&str>) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    match path_opt {
        Some(p) => {
            buf = fs::read(p)?;
        }
        None => {
            let mut stdin = io::stdin();
            stdin.read_to_end(&mut buf)?;
        }
    }
    Ok(buf)
}

fn write_all_to_stdout(bytes: &[u8]) -> io::Result<()> {
    let mut out = io::stdout();
    out.write_all(bytes)?;
    Ok(())
}

const CONVERSATION_SESSION_FILE_KEY: &str = "conversation_pack";

fn read_conversation_session_file(path: &Path) -> Result<Option<Hash32>, String> {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                return Ok(None);
            }
            return Err(format!("read failed: {}", e));
        }
    };

    let s = match String::from_utf8(bytes) {
        Ok(v) => v,
        Err(_) => return Err("invalid utf-8".to_string()),
    };

    let mut last: Option<Hash32> = None;
    for raw in s.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        let key = k.trim();
        if key != CONVERSATION_SESSION_FILE_KEY {
            continue;
        }
        let val = v.trim();
        if val.is_empty() {
            continue;
        }
        let h = parse_hash32_hex(val).map_err(|e| format!("bad conversation_pack hash: {}", e))?;
        last = Some(h);
    }

    match last {
        Some(h) => Ok(Some(h)),
        None => Err(format!("missing {} key (expected line: {}=<hash32hex>)", CONVERSATION_SESSION_FILE_KEY, CONVERSATION_SESSION_FILE_KEY)),
    }
}

fn write_conversation_session_file_atomic(path: &Path, h: &Hash32) -> Result<(), String> {
    let parent = path.parent().unwrap_or(Path::new("."));
    if let Err(e) = std::fs::create_dir_all(parent) {
        return Err(format!("mkdir failed: {}", e));
    }

    let fname = path
        .file_name()
        .ok_or_else(|| "bad session-file path".to_string())?;
    let tmp_name = format!("{}.tmp.{}", fname.to_string_lossy(), std::process::id());
    let tmp_path = parent.join(tmp_name);

    let content = format!("{}={}\n", CONVERSATION_SESSION_FILE_KEY, hex32(h));
    if let Err(e) = std::fs::write(&tmp_path, content.as_bytes()) {
        return Err(format!("write tmp failed: {}", e));
    }

    if path.exists() {
        let _ = std::fs::remove_file(path);
    }
    if let Err(e) = std::fs::rename(&tmp_path, path) {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(format!("rename failed: {}", e));
    }
    Ok(())
}

fn rebuild_markov_ctx_tail_from_conversation<S: ArtifactStore>(
    store: &S,
    msgs: &[ConversationMessage],
    tail_max: usize,
) -> Vec<MarkovTokenV1> {
    let mut out: Vec<MarkovTokenV1> = Vec::new();

    for m in msgs {
        if m.role != ConversationRole::Assistant {
            continue;
        }
        let rh = match m.replay_id {
            Some(h) => h,
            None => continue,
        };

        let log = match get_replay_log(store, &rh) {
            Ok(Some(l)) => l,
            _ => continue,
        };

        let mut mt_hash_opt: Option<Hash32> = None;
        for st in log.steps.iter() {
            if st.name == STEP_MARKOV_TRACE_V1 {
                if !st.outputs.is_empty() {
                    mt_hash_opt = Some(st.outputs[0]);
                    break;
                }
            }
        }
        let mt_hash = match mt_hash_opt {
            Some(h) => h,
            None => continue,
        };

        let trace = match get_markov_trace_v1(store, &mt_hash) {
            Ok(Some(t)) => t,
            _ => continue,
        };

        if !trace.tokens.is_empty() {
            out.extend_from_slice(&trace.tokens);
            if out.len() > tail_max {
                let drop = out.len() - tail_max;
                out.drain(0..drop);
            }
        }
    }

    out
}

fn parse_u64(s: &str) -> Result<u64, String> {
    s.parse::<u64>().map_err(|_| "invalid u64".to_string())
}

fn parse_u32(s: &str) -> Result<u32, String> {
    s.parse::<u32>().map_err(|_| "invalid u32".to_string())
}

fn parse_u8(s: &str) -> Result<u8, String> {
    s.parse::<u8>().map_err(|_| "invalid u8".to_string())
}


fn env_u64(name: &str) -> Option<u64> {
    match env::var(name) {
        Ok(v) => v.parse::<u64>().ok(),
        Err(_) => None,
    }
}

fn env_u32(name: &str) -> Option<u32> {
    match env::var(name) {
        Ok(v) => v.parse::<u32>().ok(),
        Err(_) => None,
    }
}

fn cache_cfg_kind(kind: &str) -> CacheCfgV1 {
    let base_bytes = env_u64("FSA_LM_CACHE_BYTES").unwrap_or(64 * 1024 * 1024);
    let key_bytes = format!("FSA_LM_CACHE_BYTES_{}", kind);
    let max_bytes = env_u64(&key_bytes).unwrap_or(base_bytes);

    let base_items = env_u32("FSA_LM_CACHE_MAX_ITEMS").unwrap_or(0);
    let key_items = format!("FSA_LM_CACHE_MAX_ITEMS_{}", kind);
    let max_items = env_u32(&key_items).unwrap_or(base_items);

    let base_ratio = env_u32("FSA_LM_CACHE_A1_RATIO").unwrap_or(50);
    let key_ratio = format!("FSA_LM_CACHE_A1_RATIO_{}", kind);
    let ratio = env_u32(&key_ratio).unwrap_or(base_ratio);
    let a1_ratio = if ratio > 100 { 50 } else { ratio as u8 };

    CacheCfgV1 {
        max_bytes_total: max_bytes,
        max_items_total: max_items,
        a1_ratio,
    }
}

fn print_cache_stats(label: &str, stats: CacheStatsV1, bytes_live: u64) {
    let hits = stats.hits_a1.saturating_add(stats.hits_am);
    eprintln!(
        "cache_stats label={} lookups={} hits={} hits_a1={} hits_am={} misses={} inserts={} evicts_a1={} evicts_am={} rejects_oversize={} bytes_live={} bytes_evicted_total={}",
        label,
        stats.lookups,
        hits,
        stats.hits_a1,
        stats.hits_am,
        stats.misses,
        stats.inserts,
        stats.evicts_a1,
        stats.evicts_am,
        stats.rejects_oversize,
        bytes_live,
        stats.bytes_evicted_total,
    );
}

fn parse_role(s: &str) -> Result<Role, String> {
    match s {
        "system" => Ok(Role::System),
        "user" => Ok(Role::User),
        "assistant" => Ok(Role::Assistant),
        _ => Err("invalid role".to_string()),
    }
}

fn parse_presentation_mode_v1(s: &str) -> Result<PresentationModeV1, String> {
    match s {
        "user" => Ok(PresentationModeV1::User),
        "operator" => Ok(PresentationModeV1::Operator),
        _ => Err("invalid presentation mode (expected user or operator)".to_string()),
    }
}

fn presentation_mode_name_v1(v: PresentationModeV1) -> &'static str {
    v.as_str()
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ConversationRuntimeStateV1 {
    markov_model_id: Option<Hash32>,
    exemplar_memory_id: Option<Hash32>,
    graph_relevance_id: Option<Hash32>,
    presentation_mode: Option<PresentationModeV1>,
}

impl ConversationRuntimeStateV1 {
    fn from_pack(pack: &ConversationPackV1) -> Self {
        Self {
            markov_model_id: pack.markov_model_id,
            exemplar_memory_id: pack.exemplar_memory_id,
            graph_relevance_id: pack.graph_relevance_id,
            presentation_mode: pack.presentation_mode,
        }
    }

    fn apply_to_pack(self, pack: &mut ConversationPackV1) {
        pack.markov_model_id = self.markov_model_id;
        pack.exemplar_memory_id = self.exemplar_memory_id;
        pack.graph_relevance_id = self.graph_relevance_id;
        pack.presentation_mode = self.presentation_mode;
    }
}

#[derive(Clone, Debug, Default)]
struct WorkspaceRuntimeDefaultsV1 {
    workspace: Option<WorkspaceV1>,
    read_error: Option<String>,
    invalid_error: Option<String>,
    default_k: Option<u32>,
    default_expand: bool,
    default_meta: bool,
    markov_model_id: Option<Hash32>,
    exemplar_memory_id: Option<Hash32>,
    graph_relevance_id: Option<Hash32>,
}

fn load_workspace_runtime_defaults_v1(root: &Path) -> WorkspaceRuntimeDefaultsV1 {
    let mut out = WorkspaceRuntimeDefaultsV1::default();
    match read_workspace_v1(root) {
        Ok(Some(ws)) => {
            if let Err(e) = ws.validate_pair_consistency() {
                out.invalid_error = Some(e.to_string());
                return out;
            }
            out.default_k = ws.default_k;
            out.default_expand = ws.default_expand.unwrap_or(false);
            out.default_meta = ws.default_meta.unwrap_or(false);
            out.markov_model_id = ws.markov_model;
            out.exemplar_memory_id = ws.exemplar_memory;
            out.graph_relevance_id = ws.graph_relevance;
            out.workspace = Some(ws);
        }
        Ok(None) => {}
        Err(e) => {
            out.read_error = Some(e.to_string());
        }
    }
    out
}

fn resolve_conversation_runtime_state_v1(
    explicit_markov_id: Option<Hash32>,
    explicit_exemplar_id: Option<Hash32>,
    explicit_graph_id: Option<Hash32>,
    explicit_presentation_mode: Option<PresentationModeV1>,
    prior_state: ConversationRuntimeStateV1,
    workspace_defaults: &WorkspaceRuntimeDefaultsV1,
) -> ConversationRuntimeStateV1 {
    ConversationRuntimeStateV1 {
        markov_model_id: explicit_markov_id
            .or(prior_state.markov_model_id)
            .or(workspace_defaults.markov_model_id),
        exemplar_memory_id: explicit_exemplar_id
            .or(prior_state.exemplar_memory_id)
            .or(workspace_defaults.exemplar_memory_id),
        graph_relevance_id: explicit_graph_id
            .or(prior_state.graph_relevance_id)
            .or(workspace_defaults.graph_relevance_id),
        presentation_mode: explicit_presentation_mode
            .or(prior_state.presentation_mode)
            .or(Some(PresentationModeV1::User)),
    }
}

fn resolve_runtime_expand_enabled_v1(
    explicit_expand: bool,
    workspace_defaults: &WorkspaceRuntimeDefaultsV1,
    runtime_state: ConversationRuntimeStateV1,
) -> bool {
    explicit_expand || workspace_defaults.default_expand || runtime_state.graph_relevance_id.is_some()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(10 + (b - b'a')),
        b'A'..=b'F' => Some(10 + (b - b'A')),
        _ => None,
    }
}

fn parse_hash32_hex(s: &str) -> Result<Hash32, String> {
    let bs = s.as_bytes();
    if bs.len() != 64 {
        return Err("hash hex must be 64 chars".to_string());
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        let hi = hex_val(bs[i * 2]).ok_or_else(|| "invalid hex".to_string())?;
        let lo = hex_val(bs[i * 2 + 1]).ok_or_else(|| "invalid hex".to_string())?;
        out[i] = (hi << 4) | lo;
    }
    Ok(out)
}

fn store_for(root: &Path) -> FsArtifactStore {
    FsArtifactStore::new(root).expect("failed to create artifact store")
}

fn exemplar_source_kind_name_v1(kind: ExemplarSupportSourceKindV1) -> &'static str {
    match kind {
        ExemplarSupportSourceKindV1::ReplayLog => "replay",
        ExemplarSupportSourceKindV1::PromptPack => "prompt",
        ExemplarSupportSourceKindV1::GoldenPack => "golden-pack",
        ExemplarSupportSourceKindV1::GoldenPackConversation => "golden-pack-conversation",
        ExemplarSupportSourceKindV1::ConversationPack => "conversation-pack",
        ExemplarSupportSourceKindV1::MarkovTrace => "markov-trace",
    }
}

fn planner_hint_kind_name_v1(kind: fsa_lm::planner_hints::PlannerHintKindV1) -> &'static str {
    match kind {
        fsa_lm::planner_hints::PlannerHintKindV1::Clarify => "Clarify",
        fsa_lm::planner_hints::PlannerHintKindV1::AssumeAndAnswer => "AssumeAndAnswer",
        fsa_lm::planner_hints::PlannerHintKindV1::Steps => "Steps",
        fsa_lm::planner_hints::PlannerHintKindV1::SummaryFirst => "SummaryFirst",
        fsa_lm::planner_hints::PlannerHintKindV1::Compare => "Compare",
    }
}

fn forecast_intent_kind_name_v1(kind: ForecastIntentKindV1) -> &'static str {
    match kind {
        ForecastIntentKindV1::Clarify => "Clarify",
        ForecastIntentKindV1::Example => "Example",
        ForecastIntentKindV1::MoreDetail => "MoreDetail",
        ForecastIntentKindV1::Compare => "Compare",
        ForecastIntentKindV1::NextSteps => "NextSteps",
        ForecastIntentKindV1::Risks => "Risks",
        ForecastIntentKindV1::Implementation => "Implementation",
        ForecastIntentKindV1::VerifyOrTroubleshoot => "VerifyOrTroubleshoot",
    }
}

fn planner_hint_flags_name_v1(flags: u32) -> String {
    let mut xs: Vec<&'static str> = Vec::new();
    if (flags & fsa_lm::planner_hints::PH_FLAG_PREFER_CLARIFY) != 0 {
        xs.push("clarify");
    }
    if (flags & fsa_lm::planner_hints::PH_FLAG_PREFER_DIRECT) != 0 {
        xs.push("direct");
    }
    if (flags & fsa_lm::planner_hints::PH_FLAG_PREFER_STEPS) != 0 {
        xs.push("steps");
    }
    if (flags & fsa_lm::planner_hints::PH_FLAG_PREFER_CAVEATS) != 0 {
        xs.push("caveats");
    }
    if xs.is_empty() {
        "none".to_string()
    } else {
        xs.join(",")
    }
}

fn graph_reason_flags_name_v1(flags: u8) -> &'static str {
    let multi = (flags & fsa_lm::graph_relevance::GREDGE_FLAG_MULTI_HOP) != 0;
    let sym = (flags & fsa_lm::graph_relevance::GREDGE_FLAG_SYMMETRIC) != 0;
    match (multi, sym) {
        (false, false) => "direct",
        (false, true) => "sym",
        (true, false) => "multi",
        (true, true) => "multi+sym",
    }
}

fn operator_inspect_insert_ix_v1(lines: &[String]) -> usize {
    if let Some(ix) = lines.iter().position(|line| line.starts_with("directives ")) {
        return ix + 1;
    }
    if let Some(ix) = lines.iter().position(|line| line.starts_with("query_id=")) {
        return ix + 1;
    }
    lines.len()
}

fn render_operator_answer_surface_v1(text: &str, inspect_lines: &[String]) -> String {
    if inspect_lines.is_empty() {
        return text.to_string();
    }

    let ends_with_newline = text.ends_with('\n');
    let mut lines: Vec<String> = text.lines().map(|line| line.to_string()).collect();
    let insert_ix = operator_inspect_insert_ix_v1(&lines);
    for (offset, line) in inspect_lines.iter().enumerate() {
        lines.insert(insert_ix + offset, line.clone());
    }

    let mut out = lines.join("\n");
    if ends_with_newline || !inspect_lines.is_empty() {
        out.push('\n');
    }
    out
}

fn is_hidden_user_surface_line_v1(line: &str) -> bool {
    line == "Answer v1"
        || line.starts_with("query_id=")
        || line.starts_with("directives ")
        || line.starts_with("routing_trace ")
        || line.starts_with("graph_trace ")
        || line.starts_with("exemplar_match ")
}

fn strip_hidden_user_surface_lines_v1(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for part in text.split_inclusive('\n') {
        let line = part.strip_suffix('\n').unwrap_or(part);
        if !is_hidden_user_surface_line_v1(line) {
            out.push_str(part);
        }
    }
    out
}

fn parse_simple_plan_refs_v1(line: &str) -> Option<String> {
    let trimmed = line.trim();
    if !(trimmed.starts_with("- item=")
        || trimmed
            .chars()
            .next()
            .map(|c| c.is_ascii_digit())
            .unwrap_or(false))
    {
        return None;
    }
    if trimmed.contains(" kind=step") || trimmed.contains(" kind=caveat") {
        return None;
    }
    let refs_pos = trimmed.find(" refs=")?;
    let refs = &trimmed[refs_pos + 6..];
    if refs.is_empty() {
        return None;
    }
    Some(refs.to_string())
}

fn parse_plan_item_kind_and_refs_v1(line: &str) -> Option<(&str, String)> {
    let trimmed = line.trim();
    if !(trimmed.starts_with("- item=")
        || trimmed
            .chars()
            .next()
            .map(|c| c.is_ascii_digit())
            .unwrap_or(false))
    {
        return None;
    }
    let kind_pos = trimmed.find(" kind=")?;
    let kind_rest = &trimmed[kind_pos + 6..];
    let kind_end = kind_rest.find(' ').unwrap_or(kind_rest.len());
    let kind = &kind_rest[..kind_end];
    let refs_pos = trimmed.find(" refs=")?;
    let refs = trimmed[refs_pos + 6..].trim();
    if refs.is_empty() {
        return None;
    }
    Some((kind, refs.to_string()))
}

fn label_for_light_user_plan_kind_v1(kind: &str) -> Option<&'static str> {
    match kind {
        "summary" => Some("Summary"),
        "bullet" => Some("Key points"),
        "caveat" => Some("Keep in mind"),
        _ => None,
    }
}

fn label_for_procedural_user_plan_kind_v1(kind: &str) -> Option<&'static str> {
    match kind {
        "summary" => Some("Summary"),
        "step" => Some("Steps"),
        "bullet" => Some("Details"),
        "caveat" => Some("Keep in mind"),
        _ => None,
    }
}

fn push_light_user_plan_refs_v1(
    sections: &mut Vec<(&'static str, Vec<String>)>,
    label: &'static str,
    refs: &str,
) {
    let mut added_any = false;
    if let Some((last_label, last_refs)) = sections.last_mut() {
        if *last_label == label {
            for part in refs.split(',') {
                let trimmed = part.trim();
                if !trimmed.is_empty() {
                    last_refs.push(trimmed.to_string());
                    added_any = true;
                }
            }
        }
    }
    if added_any {
        return;
    }
    let mut values: Vec<String> = Vec::new();
    for part in refs.split(',') {
        let trimmed = part.trim();
        if !trimmed.is_empty() {
            values.push(trimmed.to_string());
        }
    }
    if !values.is_empty() {
        sections.push((label, values));
    }
}

fn soften_simple_user_surface_v1(text: &str) -> Option<String> {
    let plan_marker = "Plan\n";
    let evidence_marker = "\nEvidence\n";
    let plan_ix = text.find(plan_marker)?;
    let evidence_rel_ix = text[plan_ix + plan_marker.len()..].find(evidence_marker)?;
    let evidence_ix = plan_ix + plan_marker.len() + evidence_rel_ix;
    let prefix = &text[..plan_ix];
    let plan_block = &text[plan_ix + plan_marker.len()..evidence_ix];
    let evidence_block = &text[evidence_ix + evidence_marker.len()..];

    let mut refs: Vec<String> = Vec::new();
    for line in plan_block.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let line_refs = parse_simple_plan_refs_v1(trimmed)?;
        refs.push(line_refs);
    }
    if refs.is_empty() {
        return None;
    }

    let mut out = String::with_capacity(text.len() + 64);
    let prefix_trimmed = prefix.trim_end_matches('\n');
    if !prefix_trimmed.trim().is_empty() {
        out.push_str(prefix_trimmed);
        out.push_str("\n\n");
    }
    out.push_str("Based on: ");
    out.push_str(&refs.join(", "));
    out.push_str("\n\nSources\n");
    out.push_str(evidence_block);
    Some(out)
}

fn soften_structured_user_surface_v1(text: &str) -> Option<String> {
    let plan_marker = "Plan\n";
    let evidence_marker = "\nEvidence\n";
    let plan_ix = text.find(plan_marker)?;
    let evidence_rel_ix = text[plan_ix + plan_marker.len()..].find(evidence_marker)?;
    let evidence_ix = plan_ix + plan_marker.len() + evidence_rel_ix;
    let prefix = &text[..plan_ix];
    let plan_block = &text[plan_ix + plan_marker.len()..evidence_ix];
    let evidence_block = &text[evidence_ix + evidence_marker.len()..];

    let mut sections: Vec<(&'static str, Vec<String>)> = Vec::new();
    let mut saw_step_like = false;
    let mut saw_non_simple = false;

    for line in plan_block.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed == "Steps" || trimmed == "Suggested next steps" {
            saw_step_like = true;
            break;
        }
        if let Some((kind, refs)) = parse_plan_item_kind_and_refs_v1(trimmed) {
            match kind {
                "step" => {
                    saw_step_like = true;
                    break;
                }
                "summary" | "bullet" | "caveat" => {
                    let label = label_for_light_user_plan_kind_v1(kind)?;
                    if kind != "summary" {
                        saw_non_simple = true;
                    }
                    push_light_user_plan_refs_v1(&mut sections, label, &refs);
                }
                _ => return None,
            }
        }
    }

    if saw_step_like || !saw_non_simple || sections.is_empty() {
        return None;
    }

    let mut out = String::with_capacity(text.len() + 64);
    let prefix_trimmed = prefix.trim_end_matches('\n');
    if !prefix_trimmed.trim().is_empty() {
        out.push_str(prefix_trimmed);
        out.push_str("\n\n");
    }
    for (label, refs) in sections {
        if refs.is_empty() {
            continue;
        }
        out.push_str(label);
        out.push_str(": ");
        out.push_str(&refs.join(", "));
        out.push('\n');
    }
    out.push_str("\nSources\n");
    out.push_str(evidence_block);
    Some(out)
}

fn soften_clarifier_user_surface_v1(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut pending_blank = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed == "To make sure I answer the right thing:" || trimmed == "So I can answer the right thing:" {
            continue;
        }
        if let Some(rest) = line.strip_prefix("Clarifying question: ") {
            if pending_blank && !out.ends_with("\n\n") {
                out.push('\n');
            }
            out.push_str("Quick question: ");
            out.push_str(rest.trim());
            out.push('\n');
            pending_blank = false;
            continue;
        }
        if trimmed.is_empty() {
            if !out.is_empty() {
                pending_blank = true;
            }
            continue;
        }
        if pending_blank && !out.ends_with("\n\n") {
            out.push('\n');
            out.push('\n');
        }
        out.push_str(line);
        out.push('\n');
        pending_blank = false;
    }
    if !text.ends_with('\n') && out.ends_with('\n') {
        out.pop();
    }
    out
}

fn soften_procedural_user_surface_v1(text: &str) -> Option<String> {
    let plan_marker = "Plan\n";
    let evidence_marker = "\nEvidence\n";
    let plan_ix = text.find(plan_marker)?;
    let evidence_rel_ix = text[plan_ix + plan_marker.len()..].find(evidence_marker)?;
    let evidence_ix = plan_ix + plan_marker.len() + evidence_rel_ix;
    let prefix = &text[..plan_ix];
    let plan_block = &text[plan_ix + plan_marker.len()..evidence_ix];
    let evidence_block = &text[evidence_ix + evidence_marker.len()..];

    let mut sections: Vec<(&'static str, Vec<String>)> = Vec::new();
    let mut saw_step_like = false;

    for line in plan_block.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match trimmed {
            "Summary" | "Main answer" | "Steps" | "Suggested next steps" | "Details"
            | "Supporting points" | "Caveats" | "Things to keep in mind" => continue,
            _ => {}
        }
        if let Some((kind, refs)) = parse_plan_item_kind_and_refs_v1(trimmed) {
            let label = label_for_procedural_user_plan_kind_v1(kind)?;
            if kind == "step" {
                saw_step_like = true;
            }
            push_light_user_plan_refs_v1(&mut sections, label, &refs);
            continue;
        }
        return None;
    }

    if !saw_step_like || sections.is_empty() {
        return None;
    }

    let mut out = String::with_capacity(text.len() + 64);
    let prefix_trimmed = prefix.trim_end_matches('\n');
    if !prefix_trimmed.trim().is_empty() {
        out.push_str(prefix_trimmed);
        out.push_str("\n\n");
    }
    for (label, refs) in sections {
        if refs.is_empty() {
            continue;
        }
        out.push_str(label);
        out.push_str(": ");
        out.push_str(&refs.join(", "));
        out.push('\n');
    }
    out.push_str("\nSources\n");
    out.push_str(evidence_block);
    Some(out)
}

fn soften_remaining_user_banners_v1(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut pending_blank = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed == "Plan" {
            continue;
        }
        if trimmed.is_empty() {
            if !out.is_empty() {
                pending_blank = true;
            }
            continue;
        }
        if pending_blank && !out.ends_with("\n\n") {
            out.push('\n');
            out.push('\n');
        }
        if trimmed == "Evidence" {
            out.push_str("Sources");
        } else {
            out.push_str(line);
        }
        out.push('\n');
        pending_blank = false;
    }
    if !text.ends_with('\n') && out.ends_with('\n') {
        out.pop();
    }
    out
}

fn render_user_answer_surface_v1(text: &str, _inspect_lines: &[String]) -> String {
    let stripped = strip_hidden_user_surface_lines_v1(text);
    let clarified = soften_clarifier_user_surface_v1(&stripped);
    if let Some(softened) = soften_simple_user_surface_v1(&clarified) {
        return softened;
    }
    if let Some(softened) = soften_structured_user_surface_v1(&clarified) {
        return softened;
    }
    if let Some(softened) = soften_procedural_user_surface_v1(&clarified) {
        return softened;
    }
    soften_remaining_user_banners_v1(&clarified)
}

fn render_answer_surface_v1(
    mode: PresentationModeV1,
    text: &str,
    inspect_lines: &[String],
) -> String {
    match mode {
        PresentationModeV1::User => render_user_answer_surface_v1(text, inspect_lines),
        PresentationModeV1::Operator => render_operator_answer_surface_v1(text, inspect_lines),
    }
}

fn wants_inspect_lines_v1(mode: PresentationModeV1) -> bool {
    matches!(mode, PresentationModeV1::Operator)
}

fn build_routing_trace_line_v1(
    planner_hints: &fsa_lm::planner_hints::PlannerHintsV1,
    forecast: &fsa_lm::forecast::ForecastV1,
    directives_opt: Option<&RealizerDirectivesV1>,
) -> String {
    let top_hint = planner_hints
        .hints
        .first()
        .map(|h| planner_hint_kind_name_v1(h.kind))
        .unwrap_or("none");
    let top_intent = forecast
        .intents
        .first()
        .map(|it| forecast_intent_kind_name_v1(it.kind))
        .unwrap_or("none");
    let top_followup = planner_hints
        .followups
        .first()
        .map(|f| f.followup_id.0.to_string())
        .unwrap_or_else(|| "none".to_string());
    let top_question = forecast
        .questions
        .first()
        .map(|q| q.question_id.0.to_string())
        .unwrap_or_else(|| "none".to_string());
    let (tone, style) = match directives_opt {
        Some(d) => (format!("{:?}", d.tone), format!("{:?}", d.style)),
        None => ("none".to_string(), "none".to_string()),
    };
    format!(
        "routing_trace top_hint={} top_intent={} flags={} top_followup={} top_question={} tone={} style={}",
        top_hint,
        top_intent,
        planner_hint_flags_name_v1(planner_hints.flags),
        top_followup,
        top_question,
        tone,
        style,
    )
}

fn build_graph_trace_line_v1(
    query_text: &str,
    qcfg: &QueryTermsCfg,
    graph: &fsa_lm::graph_relevance::GraphRelevanceV1,
    max_reasons: usize,
) -> Option<String> {
    if max_reasons == 0 {
        return None;
    }

    let tok_cfg = TokenizerCfg {
        max_token_bytes: qcfg.tok_cfg.max_token_bytes,
    };
    let mut seed_pairs: Vec<(u64, String)> = Vec::new();
    for sp in TokenIter::new(query_text) {
        let tok = &query_text[sp.start..sp.end];
        let tl = tok.to_ascii_lowercase();
        if tl.is_empty() {
            continue;
        }
        let tid = term_id_from_token(&tl, tok_cfg);
        seed_pairs.push(((tid.0).0, tl));
    }
    seed_pairs.sort_by(|a, b| match a.0.cmp(&b.0) {
        core::cmp::Ordering::Equal => a.1.cmp(&b.1),
        o => o,
    });
    seed_pairs.dedup_by(|a, b| a.0 == b.0);

    let mut matched_seeds: usize = 0;
    let mut candidate_count: usize = 0;
    let mut reasons: Vec<String> = Vec::new();
    for (seed_id_u64, seed_text) in seed_pairs {
        let key = (fsa_lm::graph_relevance::GraphNodeKindV1::Term as u8, seed_id_u64);
        let row_ix = match graph.rows.binary_search_by(|row| ((row.seed_kind as u8), row.seed_id.0).cmp(&key)) {
            Ok(ix) => ix,
            Err(_) => continue,
        };
        let row = &graph.rows[row_ix];
        matched_seeds += 1;
        for edge in &row.edges {
            if edge.target_kind != fsa_lm::graph_relevance::GraphNodeKindV1::Term {
                continue;
            }
            if edge.target_id.0 == seed_id_u64 {
                continue;
            }
            candidate_count += 1;
            if reasons.len() < max_reasons {
                reasons.push(format!(
                    "{}:{}->{}:w{}:h{}:{}",
                    seed_text,
                    seed_id_u64,
                    edge.target_id.0,
                    edge.weight_q16,
                    edge.hop_count,
                    graph_reason_flags_name_v1(edge.flags),
                ));
            }
        }
    }

    if candidate_count == 0 {
        return None;
    }

    Some(format!(
        "graph_trace seeds={} candidates={} reasons={}",
        matched_seeds,
        candidate_count,
        reasons.join("|"),
    ))
}

fn exemplar_match_reasons_v1(advisory: &ExemplarAdvisoryV1) -> String {
    let mut xs: Vec<&'static str> = Vec::new();
    if (advisory.match_flags & EXAD_MATCH_RESPONSE_MODE) != 0 {
        xs.push("mode");
    }
    if (advisory.match_flags & EXAD_MATCH_STRUCTURE) != 0 {
        xs.push("structure");
    }
    if (advisory.match_flags & EXAD_MATCH_TONE) != 0 {
        xs.push("tone");
    }
    if (advisory.match_flags & EXAD_MATCH_SUMMARY) != 0 {
        xs.push("summary");
    }
    if (advisory.match_flags & EXAD_MATCH_STEPS) != 0 {
        xs.push("steps");
    }
    if (advisory.match_flags & EXAD_MATCH_COMPARISON) != 0 {
        xs.push("comparison");
    }
    if (advisory.match_flags & EXAD_MATCH_CLARIFIER) != 0 {
        xs.push("clarifier");
    }
    xs.join(",")
}

fn derive_exemplar_build_id_v1(
    cfg: &ExemplarBuildConfigV1,
    inputs: &[ExemplarBuildInputV1],
) -> Hash32 {
    let mut canon = inputs.to_vec();
    canon.sort_unstable();
    canon.dedup();

    let mut buf: Vec<u8> = Vec::with_capacity(32 + canon.len() * 33);
    buf.extend_from_slice(b"exemplar_build_v1");
    buf.extend_from_slice(&cfg.max_inputs_total.to_le_bytes());
    buf.extend_from_slice(&cfg.max_inputs_per_source_kind.to_le_bytes());
    buf.extend_from_slice(&cfg.max_rows.to_le_bytes());
    buf.push(cfg.max_support_refs_per_row);
    for item in &canon {
        buf.push(item.source_kind as u8);
        buf.extend_from_slice(&item.source_hash);
    }
    blake3_hash(&buf)
}

fn derive_graph_build_id_v1(cfg: &GraphBuildConfigV1, inputs: &[GraphBuildInputV1]) -> Hash32 {
    let mut canon = inputs.to_vec();
    canon.sort_unstable();
    canon.dedup();

    let mut buf: Vec<u8> = Vec::with_capacity(48 + canon.len() * 33);
    buf.extend_from_slice(b"graph_build_v1");
    buf.extend_from_slice(&cfg.max_inputs_total.to_le_bytes());
    buf.extend_from_slice(&cfg.max_inputs_per_source_kind.to_le_bytes());
    buf.extend_from_slice(&cfg.max_rows.to_le_bytes());
    buf.push(cfg.max_edges_per_row);
    buf.push(cfg.max_terms_per_frame_row);
    buf.push(cfg.max_entities_per_frame_row);
    for item in &canon {
        buf.push(item.source_kind as u8);
        buf.extend_from_slice(&item.source_hash);
    }
    blake3_hash(&buf)
}

enum LoadedGraphSourceV1 {
    FrameSegment {
        source_hash: Hash32,
        artifact: FrameSegmentV1,
    },
    ReplayLog {
        source_hash: Hash32,
        artifact: ReplayLog,
    },
    PromptPack {
        source_hash: Hash32,
        artifact: PromptPack,
    },
    ConversationPack {
        source_hash: Hash32,
        artifact: ConversationPackV1,
    },
}

impl LoadedGraphSourceV1 {
    fn as_borrowed(&self) -> GraphSourceArtifactV1<'_> {
        match self {
            LoadedGraphSourceV1::FrameSegment { source_hash, artifact } => {
                GraphSourceArtifactV1::FrameSegment {
                    source_hash: *source_hash,
                    artifact,
                }
            }
            LoadedGraphSourceV1::ReplayLog { source_hash, artifact } => {
                GraphSourceArtifactV1::ReplayLog {
                    source_hash: *source_hash,
                    artifact,
                }
            }
            LoadedGraphSourceV1::PromptPack { source_hash, artifact } => {
                GraphSourceArtifactV1::PromptPack {
                    source_hash: *source_hash,
                    artifact,
                }
            }
            LoadedGraphSourceV1::ConversationPack { source_hash, artifact } => {
                GraphSourceArtifactV1::ConversationPack {
                    source_hash: *source_hash,
                    artifact,
                }
            }
        }
    }
}

enum LoadedExemplarSourceV1 {
    ReplayLog {
        source_hash: Hash32,
        artifact: ReplayLog,
    },
    PromptPack {
        source_hash: Hash32,
        artifact: PromptPack,
    },
    GoldenPack {
        source_hash: Hash32,
        artifact: fsa_lm::golden_pack::GoldenPackReportV1,
    },
    GoldenPackConversation {
        source_hash: Hash32,
        artifact: fsa_lm::golden_pack_conversation::GoldenPackConversationReportV1,
    },
    ConversationPack {
        source_hash: Hash32,
        artifact: ConversationPackV1,
    },
    MarkovTrace {
        source_hash: Hash32,
        artifact: MarkovTraceV1,
    },
}

impl LoadedExemplarSourceV1 {
    fn as_borrowed(&self) -> ExemplarSourceArtifactV1<'_> {
        match self {
            LoadedExemplarSourceV1::ReplayLog {
                source_hash,
                artifact,
            } => ExemplarSourceArtifactV1::ReplayLog {
                source_hash: *source_hash,
                artifact,
            },
            LoadedExemplarSourceV1::PromptPack {
                source_hash,
                artifact,
            } => ExemplarSourceArtifactV1::PromptPack {
                source_hash: *source_hash,
                artifact,
            },
            LoadedExemplarSourceV1::GoldenPack {
                source_hash,
                artifact,
            } => ExemplarSourceArtifactV1::GoldenPack {
                source_hash: *source_hash,
                artifact,
            },
            LoadedExemplarSourceV1::GoldenPackConversation {
                source_hash,
                artifact,
            } => ExemplarSourceArtifactV1::GoldenPackConversation {
                source_hash: *source_hash,
                artifact,
            },
            LoadedExemplarSourceV1::ConversationPack {
                source_hash,
                artifact,
            } => ExemplarSourceArtifactV1::ConversationPack {
                source_hash: *source_hash,
                artifact,
            },
            LoadedExemplarSourceV1::MarkovTrace {
                source_hash,
                artifact,
            } => ExemplarSourceArtifactV1::MarkovTrace {
                source_hash: *source_hash,
                artifact,
            },
        }
    }
}

fn cmd_hash(args: &[String]) -> i32 {
    let mut file: Option<&str> = None;
    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--file" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --file value");
                    return 2;
                }
                file = Some(args[i].as_str());
            }
            _ => {
                eprintln!("unknown arg: {}", args[i]);
                return 2;
            }
        }
        i += 1;
    }

    let bytes = match read_all_from(file) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("read failed: {}", e);
            return 1;
        }
    };

    let h = blake3_hash(&bytes);
    println!("{}", hex32(&h));
    0
}

fn cmd_put(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut file: Option<&str> = None;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--file" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --file value");
                    return 2;
                }
                file = Some(args[i].as_str());
            }
            _ => {
                eprintln!("unknown arg: {}", args[i]);
                return 2;
            }
        }
        i += 1;
    }

    let bytes = match read_all_from(file) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("read failed: {}", e);
            return 1;
        }
    };

    let store = store_for(&root);
    match store.put(&bytes) {
        Ok(h) => {
            println!("{}", hex32(&h));
            0
        }
        Err(e) => {
            eprintln!("put failed: {}", e);
            1
        }
    }
}

fn cmd_get(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut hash_hex: Option<&str> = None;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            x => {
                if hash_hex.is_none() {
                    hash_hex = Some(x);
                } else {
                    eprintln!("unexpected arg: {}", x);
                    return 2;
                }
            }
        }
        i += 1;
    }

    let hh = match hash_hex {
        Some(x) => x,
        None => {
            eprintln!("missing hash_hex");
            return 2;
        }
    };

    let h = match parse_hash32_hex(hh) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("{}", e);
            return 2;
        }
    };

    let store = store_for(&root);
    match store.get(&h) {
        Ok(Some(bytes)) => {
            if let Err(e) = write_all_to_stdout(&bytes) {
                eprintln!("write failed: {}", e);
                return 1;
            }
            0
        }
        Ok(None) => {
            eprintln!("not found");
            3
        }
        Err(e) => {
            eprintln!("get failed: {}", e);
            1
        }
    }
}

fn cmd_show_workspace(args: &[String]) -> i32 {
    let mut root = default_root();

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            _ => {
                eprintln!("unknown arg: {}", args[i]);
                return 2;
            }
        }
        i += 1;
    }

    let (present, ws) = match read_workspace_v1(&root) {
        Ok(Some(ws)) => (true, ws),
        Ok(None) => (false, fsa_lm::workspace::WorkspaceV1::default()),
        Err(e) => {
            eprintln!("read {} failed: {}", WORKSPACE_V1_FILENAME, e);
            return 1;
        }
    };

    let pair_ok = ws.validate_pair_consistency().is_ok();
    let ready = ws.has_required_answer_keys();
    let err = if pair_ok {
        String::new()
    } else {
        ws.validate_pair_consistency().unwrap_err()
    };

    println!("workspace_file={}", WORKSPACE_V1_FILENAME);
    println!("workspace_present={}", if present { 1 } else { 0 });

    match ws.merged_snapshot {
        Some(h) => println!("merged_snapshot={}", hex32(&h)),
        None => println!("merged_snapshot=MISSING"),
    }
    match ws.merged_sig_map {
        Some(h) => println!("merged_sig_map={}", hex32(&h)),
        None => println!("merged_sig_map=MISSING"),
    }
    match ws.lexicon_snapshot {
        Some(h) => println!("lexicon_snapshot={}", hex32(&h)),
        None => println!("lexicon_snapshot=MISSING"),
    }

    match ws.default_k {
        Some(v) => println!("default_k={}", v),
        None => println!("default_k=MISSING"),
    }
    match ws.default_expand {
        Some(v) => println!("default_expand={}", if v { 1 } else { 0 }),
        None => println!("default_expand=MISSING"),
    }
    match ws.default_meta {
        Some(v) => println!("default_meta={}", if v { 1 } else { 0 }),
        None => println!("default_meta=MISSING"),
    }
    match ws.markov_model {
        Some(h) => println!("markov_model={}", hex32(&h)),
        None => println!("markov_model=MISSING"),
    }
    match ws.exemplar_memory {
        Some(h) => println!("exemplar_memory={}", hex32(&h)),
        None => println!("exemplar_memory=MISSING"),
    }
    match ws.graph_relevance {
        Some(h) => println!("graph_relevance={}", hex32(&h)),
        None => println!("graph_relevance=MISSING"),
    }

    println!("workspace_pair_ok={}", if pair_ok { 1 } else { 0 });
    println!("workspace_ready={}", if ready { 1 } else { 0 });
    println!("workspace_error={}", err);

    0
}

fn conversation_role_name(r: ConversationRole) -> &'static str {
    match r {
        ConversationRole::System => "system",
        ConversationRole::User => "user",
        ConversationRole::Assistant => "assistant",
    }
}

fn escape_one_line(s: &str) -> String {
    // Keep stdout parse-friendly for operator tools.
    // This does not aim to be reversible for arbitrary bytes; it is intended
    // for human inspection.
    s.replace("\\", "\\\\").replace("\r", "\\r").replace("\n", "\\n")
}

fn cmd_show_conversation(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut hash_hex: Option<String> = None;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "-h" | "--help" => {
                println!("{}", usage());
                return 0;
            }
            other => {
                if other.starts_with("--") {
                    eprintln!("unknown arg: {}", other);
                    return 2;
                }
                if hash_hex.is_some() {
                    eprintln!("unexpected extra arg: {}", other);
                    return 2;
                }
                hash_hex = Some(other.to_string());
            }
        }
        i += 1;
    }

    let hh = match hash_hex {
        Some(h) => h,
        None => {
            eprintln!("missing conversation pack hash");
            return 2;
        }
    };

    let h = match parse_hash32_hex(&hh) {
        Ok(x) => x,
        Err(e) => {
            eprintln!("bad conversation pack hash: {}", e);
            return 2;
        }
    };

    let store = store_for(&root);
    let pack = match get_conversation_pack(&store, &h) {
        Ok(Some(p)) => p,
        Ok(None) => {
            eprintln!("missing conversation pack {}", hex32(&h));
            return 3;
        }
        Err(e) => {
            eprintln!("load failed: {}", e);
            return 1;
        }
    };

    println!("conversation_pack={}", hex32(&h));
    println!("version={}", pack.version);
    println!("seed={}", pack.seed);
    println!("max_output_tokens={}", pack.max_output_tokens);
    println!("snapshot_id={}", hex32(&pack.snapshot_id));
    println!("sig_map_id={}", hex32(&pack.sig_map_id));
    match pack.lexicon_snapshot_id {
        Some(h) => println!("lexicon_snapshot_id={}", hex32(&h)),
        None => println!("lexicon_snapshot_id=NONE"),
    }
    match pack.markov_model_id {
        Some(h) => println!("markov_model_id={}", hex32(&h)),
        None => println!("markov_model_id=NONE"),
    }
    match pack.exemplar_memory_id {
        Some(h) => println!("exemplar_memory_id={}", hex32(&h)),
        None => println!("exemplar_memory_id=NONE"),
    }
    match pack.graph_relevance_id {
        Some(h) => println!("graph_relevance_id={}", hex32(&h)),
        None => println!("graph_relevance_id=NONE"),
    }
    match pack.presentation_mode {
        Some(v) => println!("presentation_mode={}", presentation_mode_name_v1(v)),
        None => println!("presentation_mode=NONE"),
    }

    println!("limits.max_messages={}", pack.limits.max_messages);
    println!("limits.max_total_message_bytes={}", pack.limits.max_total_message_bytes);
    println!("limits.max_message_bytes={}", pack.limits.max_message_bytes);
    println!("limits.keep_system={}", if pack.limits.keep_system { 1 } else { 0 });

    println!("messages={}", pack.messages.len());
    for (idx, m) in pack.messages.iter().enumerate() {
        println!("msg.{}.role={}", idx, conversation_role_name(m.role));
        match m.replay_id {
            Some(h) => println!("msg.{}.replay_id={}", idx, hex32(&h)),
            None => println!("msg.{}.replay_id=NONE", idx),
        }
        println!("msg.{}.content={}", idx, escape_one_line(&m.content));
    }

    0
}

fn forward_args_has_flag(args: &[String], flag: &str) -> bool {
    args.iter().any(|s| s == flag)
}

fn forward_args_hash32_value(args: &[String], flag: &str) -> Result<Option<Hash32>, String> {
    let mut i = 0usize;
    while i < args.len() {
        if args[i] == flag {
            if i + 1 >= args.len() {
                return Err(format!("missing value for {}", flag));
            }
            return parse_hash32_hex(&args[i + 1])
                .map(Some)
                .map_err(|e| format!("bad {} hash: {}", flag, e));
        }
        i += 1;
    }
    Ok(None)
}

fn forward_args_presentation_mode_v1(args: &[String]) -> Result<Option<PresentationModeV1>, String> {
    let mut i = 0usize;
    while i < args.len() {
        if args[i] == "--presentation" {
            if i + 1 >= args.len() {
                return Err("missing value for --presentation".to_string());
            }
            return parse_presentation_mode_v1(&args[i + 1]).map(Some);
        }
        i += 1;
    }
    Ok(None)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ForwardRuntimeSelectionV1 {
    expand_explicit: bool,
    k_explicit: bool,
    meta_explicit: bool,
    explicit_markov_id: Option<Hash32>,
    explicit_exemplar_id: Option<Hash32>,
    explicit_graph_id: Option<Hash32>,
    explicit_presentation_mode: Option<PresentationModeV1>,
}

#[derive(Clone, Debug)]
struct CommandRuntimeSetupV1 {
    workspace_runtime: WorkspaceRuntimeDefaultsV1,
    forward_runtime: ForwardRuntimeSelectionV1,
    pre_sticky_expand: bool,
    effective_expand: bool,
    effective_meta: bool,
    effective_k: Option<u32>,
    sticky_runtime_state: ConversationRuntimeStateV1,
}

fn resolve_forward_runtime_selection_v1(
    forward: &[String],
    expand_explicit: bool,
) -> Result<ForwardRuntimeSelectionV1, String> {
    Ok(ForwardRuntimeSelectionV1 {
        expand_explicit,
        k_explicit: forward_args_has_flag(forward, "--k"),
        meta_explicit: forward_args_has_flag(forward, "--meta"),
        explicit_markov_id: forward_args_hash32_value(forward, "--markov-model")?,
        explicit_exemplar_id: forward_args_hash32_value(forward, "--exemplar-memory")?,
        explicit_graph_id: forward_args_hash32_value(forward, "--graph-relevance")?,
        explicit_presentation_mode: forward_args_presentation_mode_v1(forward)?,
    })
}

fn prepare_command_runtime_setup_v1(
    root: &Path,
    forward: &[String],
    expand_explicit: bool,
    prior_runtime_state: ConversationRuntimeStateV1,
) -> Result<CommandRuntimeSetupV1, String> {
    let forward_runtime = resolve_forward_runtime_selection_v1(forward, expand_explicit)?;
    let workspace_runtime = load_workspace_runtime_defaults_v1(root);
    let pre_sticky_expand = forward_runtime.expand_explicit
        || workspace_runtime.default_expand
        || workspace_runtime.graph_relevance_id.is_some();
    let sticky_runtime_state = resolve_conversation_runtime_state_v1(
        forward_runtime.explicit_markov_id,
        forward_runtime.explicit_exemplar_id,
        forward_runtime.explicit_graph_id,
        forward_runtime.explicit_presentation_mode,
        prior_runtime_state,
        &workspace_runtime,
    );
    Ok(CommandRuntimeSetupV1 {
        effective_expand: resolve_runtime_expand_enabled_v1(
            forward_runtime.expand_explicit,
            &workspace_runtime,
            sticky_runtime_state,
        ),
        effective_meta: forward_runtime.meta_explicit || workspace_runtime.default_meta,
        effective_k: if forward_runtime.k_explicit {
            None
        } else {
            workspace_runtime.default_k
        },
        workspace_runtime,
        forward_runtime,
        pre_sticky_expand,
        sticky_runtime_state,
    })
}

fn append_answer_runtime_args_v1(
    aa: &mut Vec<String>,
    forward: &[String],
    runtime_setup: &CommandRuntimeSetupV1,
    resolved_snapshot_id: Hash32,
    resolved_sig_map_id: Hash32,
    resolved_lexicon_id: Option<Hash32>,
) {
    if !forward_args_has_flag(forward, "--snapshot") {
        aa.push("--snapshot".to_string());
        aa.push(hex32(&resolved_snapshot_id));
    }
    if !forward_args_has_flag(forward, "--sig-map") {
        aa.push("--sig-map".to_string());
        aa.push(hex32(&resolved_sig_map_id));
    }
    if runtime_setup.workspace_runtime.default_expand && !runtime_setup.forward_runtime.expand_explicit {
        aa.push("--expand".to_string());
    }
    if runtime_setup.effective_expand && !forward_args_has_flag(forward, "--lexicon-snapshot") {
        if let Some(h) = resolved_lexicon_id {
            aa.push("--lexicon-snapshot".to_string());
            aa.push(hex32(&h));
        }
    }
    if let Some(v) = runtime_setup.effective_k {
        aa.push("--k".to_string());
        aa.push(v.to_string());
    }
    if runtime_setup.effective_meta && !runtime_setup.forward_runtime.meta_explicit {
        aa.push("--meta".to_string());
    }
    if runtime_setup.forward_runtime.explicit_markov_id.is_none() {
        if let Some(h) = runtime_setup.sticky_runtime_state.markov_model_id {
            aa.push("--sticky-markov-model".to_string());
            aa.push(hex32(&h));
        }
    }
    if runtime_setup.forward_runtime.explicit_exemplar_id.is_none() {
        if let Some(h) = runtime_setup.sticky_runtime_state.exemplar_memory_id {
            aa.push("--sticky-exemplar-memory".to_string());
            aa.push(hex32(&h));
        }
    }
    if runtime_setup.forward_runtime.explicit_graph_id.is_none() {
        if let Some(h) = runtime_setup.sticky_runtime_state.graph_relevance_id {
            aa.push("--sticky-graph-relevance".to_string());
            aa.push(hex32(&h));
        }
    }
    if runtime_setup.forward_runtime.explicit_presentation_mode.is_none() {
        if let Some(v) = runtime_setup.sticky_runtime_state.presentation_mode {
            aa.push("--presentation".to_string());
            aa.push(presentation_mode_name_v1(v).to_string());
        }
    }
}

fn cmd_ask(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut seed: u64 = 1;
    let mut max_tokens: u32 = 256;
    let mut role: Role = Role::User;

    let mut seed_set: bool = false;
    let mut max_tokens_set: bool = false;

    let mut session_file: Option<PathBuf> = None;
    let mut conversation_hex_opt: Option<String> = None;

    let mut forward: Vec<String> = Vec::new();
    let mut text_parts: Vec<String> = Vec::new();

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--seed" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --seed value");
                    return 2;
                }
                seed = match parse_u64(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
                seed_set = true;
            }
            "--max_tokens" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --max_tokens value");
                    return 2;
                }
                max_tokens = match parse_u32(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
                max_tokens_set = true;
            }
            "--role" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --role value");
                    return 2;
                }
                role = match parse_role(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
            }
            "--session-file" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --session-file value");
                    return 2;
                }
                session_file = Some(PathBuf::from(&args[i]));
            }
            "--conversation" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --conversation value");
                    return 2;
                }
                conversation_hex_opt = Some(args[i].to_string());
            }
            "--text" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --text value");
                    return 2;
                }
                text_parts.push(args[i].to_string());
            }
            "--prompt" => {
                eprintln!("ask does not accept --prompt (use answer if you already have a prompt hash)");
                return 2;
            }
            "-h" | "--help" => {
                println!("{}", usage());
                return 0;
            }
            other => {
                if other.starts_with("--") {
                    forward.push(other.to_string());

                    let takes_val = matches!(
                        other,
                        "--snapshot"
                            | "--sig-map"
                            | "--lexicon-snapshot"
                            | "--graph-relevance"
                            | "--pragmatics"
                            | "--k"
                            | "--max_terms"
                            | "--plan_items"
                            | "--out-file"
                            | "--verify-trace"
                            | "--markov-model"
                            | "--markov-max-choices"
                            | "--exemplar-memory"
                            | "--presentation"
                    );

                    if takes_val {
                        i += 1;
                        if i >= args.len() {
                            eprintln!("missing value for {}", other);
                            return 2;
                        }
                        forward.push(args[i].to_string());
                    }
                } else {
                    // Remaining args are treated as prompt text.
                    for t in &args[i..] {
                        text_parts.push(t.to_string());
                    }
                    break;
                }
            }
        }
        i += 1;
    }

    if text_parts.is_empty() {
        eprintln!("missing prompt text");
        return 2;
    }

    let text = text_parts.join(" ");

    // IDs are zeros for now; later stages bind these to real snapshots/weights/tokenizers.
let ids = PromptIds {
    snapshot_id: [0u8; 32],
    weights_id: [0u8; 32],
    tokenizer_id: [0u8; 32],
};

if session_file.is_some() && conversation_hex_opt.is_some() {
    eprintln!("ask: cannot use both --session-file and --conversation");
    return 2;
}

let store = store_for(&root);

let expand_explicit = forward_args_has_flag(&forward, "--expand");
let override_snapshot = forward_args_has_flag(&forward, "--snapshot");
let override_sig_map = forward_args_has_flag(&forward, "--sig-map");
let override_lexicon = forward_args_has_flag(&forward, "--lexicon-snapshot");

let mut resume_hash_opt: Option<Hash32> = None;
if let Some(ref sf) = session_file {
    match read_conversation_session_file(sf) {
        Ok(Some(h)) => resume_hash_opt = Some(h),
        Ok(None) => {
            // New session.
        }
        Err(e) => {
            eprintln!("ask: invalid session file: {}", e);
            return 2;
        }
    }
} else if let Some(ref ch) = conversation_hex_opt {
    let h = match parse_hash32_hex(ch) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("ask: bad --conversation hash: {}", e);
            return 2;
        }
    };
    resume_hash_opt = Some(h);
}

let mut conv_msgs: Vec<ConversationMessage> = Vec::new();
let mut prior_replay_id_opt: Option<Hash32> = None;
let mut resolved_snapshot_id: Option<Hash32> = None;
let mut resolved_sig_map_id: Option<Hash32> = None;
let mut resolved_lexicon_id: Option<Hash32> = None;
let mut prior_runtime_state = ConversationRuntimeStateV1::default();

if let Some(h) = resume_hash_opt {
    if override_snapshot || override_sig_map || override_lexicon {
        eprintln!("ask: cannot override snapshot/sig-map/lexicon-snapshot when resuming a conversation");
        return 2;
    }

    let pack = match get_conversation_pack(&store, &h) {
        Ok(Some(p)) => p,
        Ok(None) => {
            eprintln!("ask: missing conversation pack {}", hex32(&h));
            return 3;
        }
        Err(e) => {
            eprintln!("ask: load failed: {}", e);
            return 1;
        }
    };

    if !seed_set {
        seed = pack.seed;
    }
    if !max_tokens_set {
        max_tokens = pack.max_output_tokens;
    }

    prior_runtime_state = ConversationRuntimeStateV1::from_pack(&pack);
    conv_msgs = pack.messages;

    // Capture the most recent assistant replay id for cross-turn continuation.
    for m in conv_msgs.iter().rev() {
        if m.role == ConversationRole::Assistant {
            if let Some(rid) = m.replay_id {
                prior_replay_id_opt = Some(rid);
                break;
            }
        }
    }

    resolved_snapshot_id = Some(pack.snapshot_id);
    resolved_sig_map_id = Some(pack.sig_map_id);
    resolved_lexicon_id = pack.lexicon_snapshot_id;
}

let runtime_setup = match prepare_command_runtime_setup_v1(
    &root,
    &forward,
    expand_explicit,
    prior_runtime_state,
) {
    Ok(v) => v,
    Err(e) => {
        eprintln!("ask: {}", e);
        return 2;
    }
};
let workspace_runtime = &runtime_setup.workspace_runtime;

// If we do not have determinism-critical ids yet (new session), bind them to either
// explicit flags or workspace defaults so the session is resumeable.
if resolved_snapshot_id.is_none() || resolved_sig_map_id.is_none() {
    let mut snapshot_hex_opt: Option<String> = None;
    let mut sig_map_hex_opt: Option<String> = None;
    let mut lexicon_hex_opt: Option<String> = None;

    let mut j = 0usize;
    while j < forward.len() {
        match forward[j].as_str() {
            "--snapshot" => {
                if j + 1 < forward.len() {
                    snapshot_hex_opt = Some(forward[j + 1].clone());
                }
            }
            "--sig-map" => {
                if j + 1 < forward.len() {
                    sig_map_hex_opt = Some(forward[j + 1].clone());
                }
            }
            "--lexicon-snapshot" => {
                if j + 1 < forward.len() {
                    lexicon_hex_opt = Some(forward[j + 1].clone());
                }
            }
            _ => {}
        }
        j += 1;
    }

    let mut snap: Option<Hash32> = None;
    let mut sig: Option<Hash32> = None;
    let mut lex: Option<Hash32> = None;

    if let Some(sh) = snapshot_hex_opt.as_ref() {
        snap = match parse_hash32_hex(sh) {
            Ok(h) => Some(h),
            Err(e) => {
                eprintln!("ask: bad --snapshot hash: {}", e);
                return 2;
            }
        };
    }
    if let Some(sm) = sig_map_hex_opt.as_ref() {
        sig = match parse_hash32_hex(sm) {
            Ok(h) => Some(h),
            Err(e) => {
                eprintln!("ask: bad --sig-map hash: {}", e);
                return 2;
            }
        };
    }
    if runtime_setup.pre_sticky_expand {
        if let Some(lh) = lexicon_hex_opt.as_ref() {
            lex = match parse_hash32_hex(lh) {
                Ok(h) => Some(h),
                Err(e) => {
                    eprintln!("ask: bad --lexicon-snapshot hash: {}", e);
                    return 2;
                }
            };
        }
    }

    if snap.is_none() || sig.is_none() {
        let ws = if let Some(ws) = workspace_runtime.workspace.as_ref() {
            ws.clone()
        } else if let Some(e) = workspace_runtime.read_error.as_ref() {
            eprintln!("ask: read {} failed: {}", WORKSPACE_V1_FILENAME, e);
            return 1;
        } else if let Some(e) = workspace_runtime.invalid_error.as_ref() {
            eprintln!("ask: invalid {}: {}", WORKSPACE_V1_FILENAME, e);
            return 2;
        } else {
            eprintln!("ask: need snapshot+sig-map. Provide --snapshot/--sig-map or run load-wikipedia to create {}", WORKSPACE_V1_FILENAME);
            return 2;
        };
        if !ws.has_required_answer_keys() {
            eprintln!("ask: {} missing merged_snapshot/merged_sig_map (run load-wikipedia or edit the file)", WORKSPACE_V1_FILENAME);
            return 2;
        }
        if snap.is_none() {
            snap = ws.merged_snapshot;
        }
        if sig.is_none() {
            sig = ws.merged_sig_map;
        }
        if runtime_setup.pre_sticky_expand && lex.is_none() {
            lex = ws.lexicon_snapshot;
        }
    }

    resolved_snapshot_id = snap;
    resolved_sig_map_id = sig;
    resolved_lexicon_id = lex;
} else if runtime_setup.pre_sticky_expand && resolved_lexicon_id.is_none() {
    if let Some(ws) = workspace_runtime.workspace.as_ref() {
        resolved_lexicon_id = ws.lexicon_snapshot;
    }
}

let resolved_snapshot_id = match resolved_snapshot_id {
    Some(h) => h,
    None => {
        eprintln!("ask: missing snapshot id");
        return 2;
    }
};
let resolved_sig_map_id = match resolved_sig_map_id {
    Some(h) => h,
    None => {
        eprintln!("ask: missing sig-map id");
        return 2;
    }
};
let sticky_runtime_state = runtime_setup.sticky_runtime_state;

// Append this turn's prompt message to the conversation history.
let conv_role = match role {
    Role::System => ConversationRole::System,
    Role::User => ConversationRole::User,
    Role::Assistant => ConversationRole::Assistant,
};
conv_msgs.push(ConversationMessage {
    role: conv_role,
    content: text,
    replay_id: None,
});

// Canonicalize conversation history deterministically.
{
    let mut cp = ConversationPackV1::new(
        seed,
        max_tokens,
        resolved_snapshot_id,
        resolved_sig_map_id,
        resolved_lexicon_id,
        ConversationLimits::default_v1(),
    );
    cp.messages = conv_msgs;
    cp.canonicalize_in_place();
    conv_msgs = cp.messages;
}

// Build a PromptPack from the conversation messages.
let mut pack = PromptPack::new(seed, max_tokens, ids);
let mut pm: Vec<Message> = Vec::with_capacity(conv_msgs.len());
for m in conv_msgs.iter() {
    let role = match m.role {
        ConversationRole::System => Role::System,
        ConversationRole::User => Role::User,
        ConversationRole::Assistant => Role::Assistant,
    };
    pm.push(Message { role, content: m.content.clone() });
}
pack.messages = pm;

// Apply default canonical limits to make a bounded artifact.
let limits = PromptLimits::default_v1();

let prompt_hash = match put_prompt_pack(&store, &mut pack, limits) {
    Ok(h) => h,
    Err(e) => {
        eprintln!("put failed: {}", e);
        return 1;
    }
};

let mut aa: Vec<String> = Vec::new();
aa.push("--root".to_string());
aa.push(root.to_string_lossy().to_string());
aa.push("--prompt".to_string());
aa.push(hex32(&prompt_hash));
aa.extend(forward.clone());
append_answer_runtime_args_v1(
    &mut aa,
    &forward,
    &runtime_setup,
    resolved_snapshot_id,
    resolved_sig_map_id,
    resolved_lexicon_id,
);
if let Some(rh) = prior_replay_id_opt {
    aa.push("--prior-replay".to_string());
    aa.push(hex32(&rh));
}

let mut markov_ctx_tail: Vec<MarkovTokenV1> = Vec::new();
if sticky_runtime_state.markov_model_id.is_some() && !conv_msgs.is_empty() {
    markov_ctx_tail = rebuild_markov_ctx_tail_from_conversation(&store, &conv_msgs, 64);
}

let (ans_text, out_file, _mt_tokens, replay_hash) = match answer_run_text_inner(&aa, &markov_ctx_tail) {
    Ok(x) => x,
    Err(code) => return code,
};

if let Some(path) = out_file {
    if let Err(e) = fs::write(&path, ans_text.as_bytes()) {
        eprintln!("write failed: {}", e);
        return 1;
    }
}

if let Err(e) = write_all_to_stdout(ans_text.as_bytes()) {
    eprintln!("stdout error: {}", e);
    return 1;
}

// Persist the updated conversation when requested (session-file or explicit conversation mode).
if session_file.is_some() || conversation_hex_opt.is_some() {
    conv_msgs.push(ConversationMessage {
        role: ConversationRole::Assistant,
        content: ans_text,
        replay_id: Some(replay_hash),
    });

    let mut cp = ConversationPackV1::new(
        seed,
        max_tokens,
        resolved_snapshot_id,
        resolved_sig_map_id,
        resolved_lexicon_id,
        ConversationLimits::default_v1(),
    );
    sticky_runtime_state.apply_to_pack(&mut cp);
    cp.messages = conv_msgs;
    cp.canonicalize_in_place();

    let h = match put_conversation_pack(&store, &mut cp) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("save failed: {}", e);
            return 1;
        }
    };

    if let Some(ref sf) = session_file {
        if let Err(e) = write_conversation_session_file_atomic(sf, &h) {
            eprintln!("save failed: {}", e);
            return 1;
        }
    }

    eprintln!("conversation_pack={}", hex32(&h));
}

0
}

fn cmd_chat(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut seed: u64 = 1;
    let mut seed_set: bool = false;
    let mut max_tokens: u32 = 256;
    let mut max_tokens_set: bool = false;
    let mut system_msg: Option<String> = None;
    let mut resume_hash: Option<Hash32> = None;
    let mut session_file: Option<PathBuf> = None;
    let mut autosave: bool = false;

    let mut forward: Vec<String> = Vec::new();

    let mut snapshot_hex_opt: Option<String> = None;
    let mut sig_map_hex_opt: Option<String> = None;
    let mut lexicon_hex_opt: Option<String> = None;
    let mut enable_expand: bool = false;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--seed" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --seed value");
                    return 2;
                }
                seed = match parse_u64(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
                seed_set = true;
            }
            "--max_tokens" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --max_tokens value");
                    return 2;
                }
                max_tokens = match parse_u32(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
                max_tokens_set = true;
            }
            "--system" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --system value");
                    return 2;
                }
                system_msg = Some(args[i].to_string());
            }
            "--session-file" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --session-file value");
                    return 2;
                }
                session_file = Some(PathBuf::from(&args[i]));
            }
            "--autosave" => {
                autosave = true;
            }
            "--resume" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --resume value");
                    return 2;
                }
                let h = match parse_hash32_hex(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("bad --resume hash: {}", e);
                        return 2;
                    }
                };
                resume_hash = Some(h);
            }
            "--prompt" => {
                eprintln!("chat does not accept --prompt");
                return 2;
            }
            "--text" => {
                eprintln!("chat does not accept --text (enter prompts interactively)");
                return 2;
            }
            "-h" | "--help" => {
                println!("{}", usage());
                return 0;
            }
            other => {
                if other.starts_with("--") {
                    if other == "--out-file" {
                        eprintln!("chat does not accept --out-file");
                        return 2;
                    }

                    if other == "--expand" {
                        enable_expand = true;
                        forward.push(other.to_string());
                        i += 1;
                        continue;
                    }

                    forward.push(other.to_string());

                    let takes_val = matches!(
                        other,
                        "--snapshot"
                            | "--sig-map"
                            | "--lexicon-snapshot"
                            | "--graph-relevance"
                            | "--pragmatics"
                            | "--k"
                            | "--max_terms"
                            | "--plan_items"
                            | "--verify-trace"
                            | "--markov-model"
                            | "--markov-max-choices"
                            | "--exemplar-memory"
                            | "--presentation"
                    );

                    if takes_val {
                        i += 1;
                        if i >= args.len() {
                            eprintln!("missing value for {}", other);
                            return 2;
                        }

                        if other == "--snapshot" {
                            snapshot_hex_opt = Some(args[i].to_string());
                        } else if other == "--sig-map" {
                            sig_map_hex_opt = Some(args[i].to_string());
                        } else if other == "--lexicon-snapshot" {
                            lexicon_hex_opt = Some(args[i].to_string());
                        }

                        forward.push(args[i].to_string());
                    }
                } else {
                    eprintln!("unexpected arg: {}", other);
                    return 2;
                }
            }
        }
        i += 1;
    }

    if resume_hash.is_some() && session_file.is_some() {
        eprintln!("chat: cannot use --session-file with --resume");
        return 2;
    }

    if autosave && session_file.is_none() {
        eprintln!("chat: --autosave requires --session-file");
        return 2;
    }

    let expand_explicit = enable_expand;

    // If a session file is provided and contains a conversation pack pointer, treat it as resume.
    if resume_hash.is_none() {
        if let Some(ref sf) = session_file {
            match read_conversation_session_file(sf) {
                Ok(Some(h)) => {
                    resume_hash = Some(h);
                }
                Ok(None) => {}
                Err(e) => {
                    eprintln!("chat: read session-file failed: {}", e);
                    return 2;
                }
            }
        }
    }

    let store = store_for(&root);

    let ids = PromptIds {
        snapshot_id: [0u8; 32],
        weights_id: [0u8; 32],
        tokenizer_id: [0u8; 32],
    };

    let limits = PromptLimits::default_v1();

    let mut conv_msgs: Vec<ConversationMessage> = Vec::new();
    let mut prior_runtime_state = ConversationRuntimeStateV1::default();
    let mut resume_pack_lexicon_id: Option<Hash32> = None;
    if let Some(rh) = resume_hash {
        if system_msg.is_some() {
            eprintln!("chat: cannot use --system with --resume");
            return 2;
        }
        let pack = match get_conversation_pack(&store, &rh) {
            Ok(Some(p)) => p,
            Ok(None) => {
                eprintln!("chat: missing conversation pack {}", hex32(&rh));
                return 3;
            }
            Err(e) => {
                eprintln!("chat: load failed: {}", e);
                return 1;
            }
        };

        if !seed_set {
            seed = pack.seed;
        }
        if !max_tokens_set {
            max_tokens = pack.max_output_tokens;
        }

        // If the caller did not provide snapshot/sig-map, prefer the pack ids.
        if snapshot_hex_opt.is_none() {
            snapshot_hex_opt = Some(hex32(&pack.snapshot_id));
            forward.push("--snapshot".to_string());
            forward.push(hex32(&pack.snapshot_id));
        }
        if sig_map_hex_opt.is_none() {
            sig_map_hex_opt = Some(hex32(&pack.sig_map_id));
            forward.push("--sig-map".to_string());
            forward.push(hex32(&pack.sig_map_id));
        }
        resume_pack_lexicon_id = pack.lexicon_snapshot_id;

        prior_runtime_state = ConversationRuntimeStateV1::from_pack(&pack);
        conv_msgs = pack.messages;
    } else if let Some(sys) = system_msg {
        conv_msgs.push(ConversationMessage {
            role: ConversationRole::System,
            content: sys,
            replay_id: None,
        });
    }

    let runtime_setup = match prepare_command_runtime_setup_v1(
        &root,
        &forward,
        expand_explicit,
        prior_runtime_state,
    ) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("chat: {}", e);
            return 2;
        }
    };
    let workspace_runtime = &runtime_setup.workspace_runtime;

    if runtime_setup.pre_sticky_expand && lexicon_hex_opt.is_none() {
        if let Some(lh) = resume_pack_lexicon_id {
            lexicon_hex_opt = Some(hex32(&lh));
            forward.push("--lexicon-snapshot".to_string());
            forward.push(hex32(&lh));
        }
    }

    // Resolve determinism-critical ids once for save/resume.
    let (resolved_snapshot_id, resolved_sig_map_id, resolved_lexicon_id) = {
        let mut snap: Option<Hash32> = None;
        let mut sig: Option<Hash32> = None;
        let mut lex: Option<Hash32> = None;

        if let Some(ref sh) = snapshot_hex_opt {
            snap = match parse_hash32_hex(sh) {
                Ok(h) => Some(h),
                Err(e) => {
                    eprintln!("chat: bad --snapshot hash: {}", e);
                    return 2;
                }
            };
        }
        if let Some(ref sm) = sig_map_hex_opt {
            sig = match parse_hash32_hex(sm) {
                Ok(h) => Some(h),
                Err(e) => {
                    eprintln!("chat: bad --sig-map hash: {}", e);
                    return 2;
                }
            };
        }
        if runtime_setup.pre_sticky_expand {
            if let Some(ref lh) = lexicon_hex_opt {
                lex = match parse_hash32_hex(lh) {
                    Ok(h) => Some(h),
                    Err(e) => {
                        eprintln!("chat: bad --lexicon-snapshot hash: {}", e);
                        return 2;
                    }
                };
            }
        }

        if snap.is_none() || sig.is_none() {
            let ws = if let Some(ws) = workspace_runtime.workspace.as_ref() {
                ws.clone()
            } else if let Some(e) = workspace_runtime.read_error.as_ref() {
                eprintln!("chat: read {} failed: {}", WORKSPACE_V1_FILENAME, e);
                return 1;
            } else if let Some(e) = workspace_runtime.invalid_error.as_ref() {
                eprintln!("chat: invalid {}: {}", WORKSPACE_V1_FILENAME, e);
                return 2;
            } else {
                eprintln!("chat: need snapshot+sig-map. Provide --snapshot/--sig-map or run load-wikipedia to create {}", WORKSPACE_V1_FILENAME);
                return 2;
            };
            if !ws.has_required_answer_keys() {
                eprintln!("chat: {} missing merged_snapshot/merged_sig_map (run load-wikipedia or edit the file)", WORKSPACE_V1_FILENAME);
                return 2;
            }
            if snap.is_none() {
                snap = ws.merged_snapshot;
            }
            if sig.is_none() {
                sig = ws.merged_sig_map;
            }
            if runtime_setup.pre_sticky_expand && lex.is_none() {
                lex = ws.lexicon_snapshot;
            }
        }

        (snap.unwrap(), sig.unwrap(), lex)
    };

    let sticky_runtime_state = runtime_setup.sticky_runtime_state;

    let mut markov_ctx_tail: Vec<MarkovTokenV1> = Vec::new();
    if sticky_runtime_state.markov_model_id.is_some() && !conv_msgs.is_empty() {
        markov_ctx_tail = rebuild_markov_ctx_tail_from_conversation(&store, &conv_msgs, 64);
    }

    let stdin = io::stdin();
    let mut input = stdin.lock();

    use std::io::IsTerminal;
    let interactive = stdin.is_terminal() && io::stderr().is_terminal();

    loop {
        if interactive {
            if let Err(_) = io::stderr().write_all(b"> ") {
                return 1;
            }
            let _ = io::stderr().flush();
        }

        let mut line = String::new();
        match input.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {}
            Err(e) => {
                eprintln!("chat read error: {}", e);
                return 1;
            }
        }

        let t = line.trim();
        if t.is_empty() {
            continue;
        }

        if t == "/exit" || t == "/quit" {
            break;
        }

        if t == "/help" {
            eprintln!("Commands:");
            eprintln!("  /help   show this help");
            eprintln!("  /reset  clear conversation history");
            eprintln!("  /save   store a ConversationPack artifact");
            eprintln!("  /exit   exit chat");
            continue;
        }

        if t == "/reset" {
            let keep_sys: Vec<ConversationMessage> = conv_msgs
                .iter()
                .filter(|m| m.role == ConversationRole::System)
                .cloned()
                .collect();
            conv_msgs = keep_sys;
            markov_ctx_tail.clear();
            eprintln!("history reset");
            continue;
        }

        if t == "/save" {
            let mut pack = ConversationPackV1::new(
                seed,
                max_tokens,
                resolved_snapshot_id,
                resolved_sig_map_id,
                resolved_lexicon_id,
                ConversationLimits::default_v1(),
            );
            sticky_runtime_state.apply_to_pack(&mut pack);
            pack.messages = conv_msgs.clone();
            let h = match put_conversation_pack(&store, &mut pack) {
                Ok(h) => h,
                Err(e) => {
                    eprintln!("save failed: {}", e);
                    return 1;
                }
            };
            if let Some(ref sf) = session_file {
                if let Err(e) = write_conversation_session_file_atomic(sf, &h) {
                    eprintln!("save failed: {}", e);
                    return 1;
                }
            }
            eprintln!("conversation_pack={}", hex32(&h));
            continue;
        }

        conv_msgs.push(ConversationMessage {
            role: ConversationRole::User,
            content: t.to_string(),
            replay_id: None,
        });

        // Canonicalize conversation history so PromptPack canonicalization is stable and
        // replay ids remain aligned with messages.
        {
            let mut cp = ConversationPackV1::new(
                seed,
                max_tokens,
                resolved_snapshot_id,
                resolved_sig_map_id,
                resolved_lexicon_id,
                ConversationLimits::default_v1(),
            );
            cp.messages = conv_msgs;
            cp.canonicalize_in_place();
            conv_msgs = cp.messages;
        }

        let mut pack = PromptPack::new(seed, max_tokens, ids);
        let mut pm: Vec<Message> = Vec::with_capacity(conv_msgs.len());
        for m in conv_msgs.iter() {
            let role = match m.role {
                ConversationRole::System => Role::System,
                ConversationRole::User => Role::User,
                ConversationRole::Assistant => Role::Assistant,
            };
            pm.push(Message { role, content: m.content.clone() });
        }
        pack.messages = pm;

        let prompt_hash = match put_prompt_pack(&store, &mut pack, limits) {
            Ok(h) => h,
            Err(e) => {
                eprintln!("put failed: {}", e);
                return 1;
            }
        };

        // Capture the most recent assistant replay id for cross-turn continuation.
        let mut prior_replay_id_opt: Option<Hash32> = None;
        for m in conv_msgs.iter().rev() {
            if m.role == ConversationRole::Assistant {
                if let Some(rid) = m.replay_id {
                    prior_replay_id_opt = Some(rid);
                    break;
                }
            }
        }

        let mut aa: Vec<String> = Vec::new();
        aa.push("--root".to_string());
        aa.push(root.to_string_lossy().to_string());
        aa.push("--prompt".to_string());
        aa.push(hex32(&prompt_hash));
        aa.extend(forward.clone());
        append_answer_runtime_args_v1(
            &mut aa,
            &forward,
            &runtime_setup,
            resolved_snapshot_id,
            resolved_sig_map_id,
            resolved_lexicon_id,
        );
        if let Some(rh) = prior_replay_id_opt {
            aa.push("--prior-replay".to_string());
            aa.push(hex32(&rh));
        }

        let (ans_text, _out_file, mt_tokens, replay_hash) = match answer_run_text_inner(&aa, &markov_ctx_tail) {
            Ok(x) => x,
            Err(code) => return code,
        };

        // Print answer to stdout.
        if let Err(e) = write_all_to_stdout(ans_text.as_bytes()) {
            eprintln!("stdout error: {}", e);
            return 1;
        }

        // Append assistant reply so the next turn sees history.
        conv_msgs.push(ConversationMessage {
            role: ConversationRole::Assistant,
            content: ans_text,
            replay_id: Some(replay_hash),
        });

        {
            let mut cp = ConversationPackV1::new(
                seed,
                max_tokens,
                resolved_snapshot_id,
                resolved_sig_map_id,
                resolved_lexicon_id,
                ConversationLimits::default_v1(),
            );
            cp.messages = conv_msgs;
            cp.canonicalize_in_place();
            conv_msgs = cp.messages;
        }

        if !mt_tokens.is_empty() {
            markov_ctx_tail.extend_from_slice(&mt_tokens);
            if markov_ctx_tail.len() > 64 {
                let drop = markov_ctx_tail.len() - 64;
                markov_ctx_tail.drain(0..drop);
            }
        }

        if autosave {
            let mut pack = ConversationPackV1::new(
                seed,
                max_tokens,
                resolved_snapshot_id,
                resolved_sig_map_id,
                resolved_lexicon_id,
                ConversationLimits::default_v1(),
            );
            sticky_runtime_state.apply_to_pack(&mut pack);
            pack.messages = conv_msgs.clone();
            let h = match put_conversation_pack(&store, &mut pack) {
                Ok(h) => h,
                Err(e) => {
                    eprintln!("autosave failed: {}", e);
                    return 1;
                }
            };
            if let Some(ref sf) = session_file {
                if let Err(e) = write_conversation_session_file_atomic(sf, &h) {
                    eprintln!("autosave failed: {}", e);
                    return 1;
                }
            }
        }
    }

    0
}



fn cmd_prompt(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut seed: u64 = 1;
    let mut max_tokens: u32 = 256;
    let mut role: Role = Role::User;

    let mut text_parts: Vec<String> = Vec::new();

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--seed" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --seed value");
                    return 2;
                }
                seed = match parse_u64(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
            }
            "--max_tokens" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --max_tokens value");
                    return 2;
                }
                max_tokens = match parse_u32(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
            }
            "--role" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --role value");
                    return 2;
                }
                role = match parse_role(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
            }
            x => {
                text_parts.push(x.to_string());
            }
        }
        i += 1;
    }

    if text_parts.is_empty() {
        eprintln!("missing prompt text");
        return 2;
    }

    let text = text_parts.join(" ");

    // IDs are zeros for now; later stages bind these to real snapshots/weights/tokenizers.
    let ids = PromptIds {
        snapshot_id: [0u8; 32],
        weights_id: [0u8; 32],
        tokenizer_id: [0u8; 32],
    };

    let mut pack = PromptPack::new(seed, max_tokens, ids);
    pack.messages.push(Message { role, content: text });

    // Apply default canonical limits to make a bounded artifact.
    let limits = PromptLimits::default_v1();

    let store = store_for(&root);
    match put_prompt_pack(&store, &mut pack, limits) {
        Ok(h) => {
            println!("{}", hex32(&h));
            0
        }
        Err(e) => {
            eprintln!("put failed: {}", e);
            1
        }
    }
}

fn cmd_replay_decode(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut hash_hex: Option<&str> = None;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            x => {
                if hash_hex.is_none() {
                    hash_hex = Some(x);
                } else {
                    eprintln!("unexpected arg: {}", x);
                    return 2;
                }
            }
        }
        i += 1;
    }

    let hh = match hash_hex {
        Some(x) => x,
        None => {
            eprintln!("missing hash_hex");
            return 2;
        }
    };

    let h = match parse_hash32_hex(hh) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("{}", e);
            return 2;
        }
    };

    let store = store_for(&root);
    let bytes = match store.get(&h) {
        Ok(Some(b)) => b,
        Ok(None) => {
            eprintln!("not found");
            return 3;
        }
        Err(e) => {
            eprintln!("get failed: {}", e);
            return 1;
        }
    };

    let log = match ReplayLog::decode(&bytes) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("decode failed: {}", e);
            return 1;
        }
    };

    println!("ReplayLog v{} steps={}", log.version, log.steps.len());
    for (i, st) in log.steps.iter().enumerate() {
        println!("step[{}] name={}", i, st.name);
        println!("  inputs={}", st.inputs.len());
        println!("  outputs={}", st.outputs.len());
    }
    0
}


fn cmd_replay_new(args: &[String]) -> i32 {
    let mut root = default_root();

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            x => {
                eprintln!("unexpected arg: {}", x);
                return 2;
            }
        }
        i += 1;
    }

    let store = store_for(&root);
    let log = ReplayLog::new();
    match put_replay_log(&store, &log) {
        Ok(h) => {
            println!("{}", hex32(&h));
            0
        }
        Err(e) => {
            eprintln!("put failed: {}", e);
            1
        }
    }
}


fn cmd_frame_seg_demo(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut chunk_rows: u32 = 1024;
    let mut text_parts: Vec<String> = Vec::new();

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--chunk_rows" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --chunk_rows value");
                    return 2;
                }
                match args[i].parse::<u32>() {
                    Ok(v) => chunk_rows = v,
                    Err(_) => {
                        eprintln!("invalid --chunk_rows value");
                        return 2;
                    }
                }
            }
            _ => {
                text_parts.push(args[i].clone());
            }
        }
        i += 1;
    }

    if text_parts.is_empty() {
        eprintln!("missing text");
        return 2;
    }
    let text = text_parts.join(" ");

    let store = store_for(&root);

    // Create a single demo row.
    let doc_id = DocId(fsa_lm::frame::derive_id64(b"doc\0", text.as_bytes()));
    let source_id = SourceId(fsa_lm::frame::derive_id64(b"src\0", b"demo"));
    let mut row = FrameRowV1::new(doc_id, source_id);

    let tok_cfg = TokenizerCfg::default();
    row.terms = term_freqs_from_text(&text, tok_cfg);
    row.recompute_doc_len();

    let rows = [row];
    let seg = match FrameSegmentV1::from_rows(&rows, chunk_rows) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("segment build error: {}", e);
            return 1;
        }
    };

    let hash = match put_frame_segment_v1(&store, &seg) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("store error: {}", e);
            return 1;
        }
    };

    println!("{}", hex32(&hash));
    0
}

fn cmd_frame_seg_show(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut hash_hex: Option<&str> = None;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            _ => {
                if hash_hex.is_none() {
                    hash_hex = Some(&args[i]);
                } else {
                    eprintln!("unexpected extra arg: {}", args[i]);
                    return 2;
                }
            }
        }
        i += 1;
    }

    let hh = match hash_hex {
        Some(v) => v,
        None => {
            eprintln!("missing segment hash");
            return 2;
        }
    };

    let hash = match parse_hash32_hex(hh) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("{}", e);
            return 2;
        }
    };

    let store = store_for(&root);

    let seg_opt = match get_frame_segment_v1(&store, &hash) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("load error: {}", e);
            return 1;
        }
    };

    let seg = match seg_opt {
        Some(s) => s,
        None => {
            eprintln!("not found");
            return 1;
        }
    };

    // Print a small summary (stable, no nondeterministic ordering).
    let mut rows_total: u64 = 0;
    for c in &seg.chunks {
        rows_total += c.rows as u64;
    }
    println!("segment_hash={}", hex32(&hash));
    println!("chunk_rows={}", seg.chunk_rows);
    println!("chunks={}", seg.chunks.len());
    println!("rows_total={}", rows_total);
    0
}

fn cmd_ingest_wiki(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut dump_path: Option<&str> = None;

    // Sizing knobs.
    let mut seg_mb: u32 = 4;
    let mut row_kb: u32 = 8;
    let mut chunk_rows: u32 = 1024;
    let mut max_docs: Option<u64> = None;

    // Sharding knobs.
    let mut shards: Option<u16> = None;
    let mut shard_id: Option<u16> = None;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--dump" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --dump value");
                    return 2;
                }
                dump_path = Some(&args[i]);
            }
            "--seg_mb" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --seg_mb value");
                    return 2;
                }
                seg_mb = match parse_u32(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
            }
            "--row_kb" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --row_kb value");
                    return 2;
                }
                row_kb = match parse_u32(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
            }
            "--chunk_rows" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --chunk_rows value");
                    return 2;
                }
                chunk_rows = match parse_u32(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
            }
            "--max_docs" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --max_docs value");
                    return 2;
                }
                max_docs = Some(match parse_u64(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                });
            }
            "--shards" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --shards value");
                    return 2;
                }
                let v = match parse_u32(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
                if v == 0 || v > (u16::MAX as u32) {
                    eprintln!("invalid --shards value");
                    return 2;
                }
                shards = Some(v as u16);
            }
            "--shard-id" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --shard-id value");
                    return 2;
                }
                let v = match parse_u32(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
                if v > (u16::MAX as u32) {
                    eprintln!("invalid --shard-id value");
                    return 2;
                }
                shard_id = Some(v as u16);
            }
            x => {
                eprintln!("unexpected arg: {}", x);
                return 2;
            }
        }
        i += 1;
    }

    let dump = match dump_path {
        Some(p) => p,
        None => {
            eprintln!("missing --dump <path>");
            return 2;
        }
    };

    let shard_cfg = match (shards, shard_id) {
        (None, None) => None,
        (Some(sc), Some(sid)) => {
            if sid >= sc {
                eprintln!("shard-id out of range");
                return 2;
            }
            Some(ShardCfgV1 { shard_count: sc, shard_id: sid })
        }
        _ => {
            eprintln!("provide both --shards and --shard-id");
            return 2;
        }
    };

    let base_root = root.clone();
    let root = match shard_cfg {
        Some(sc) => base_root.join(format!("shards/{:04x}", sc.shard_id)),
        None => base_root,
    };

    let store = store_for(&root);

    let f = match fs::File::open(dump) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("open failed: {}", e);
            return 1;
        }
    };
    let rr = BufReader::new(f);

    // Derive segment sizing deterministically from seg_mb and row_kb.
    // seg_rows is a target row cap, not a hard byte cap.
    let seg_bytes = (seg_mb as u64) * 1024 * 1024;
    let row_bytes = (row_kb as u64) * 1024;
    let mut seg_rows = if row_bytes == 0 { 1 } else { seg_bytes / row_bytes };
    if seg_rows < 1 {
        seg_rows = 1;
    }
    if seg_rows > (u32::MAX as u64) {
        seg_rows = u32::MAX as u64;
    }

    let mut cfg = WikiIngestCfg::default_v1();
    cfg.chunk_rows = chunk_rows;
    cfg.seg_rows = seg_rows as u32;
    cfg.row_max_bytes = row_bytes as usize;
    cfg.max_docs = max_docs;

    let mh = match match shard_cfg {
        Some(sc) => ingest_wiki_tsv_sharded(&store, rr, cfg, sc),
        None => ingest_wiki_tsv(&store, rr, cfg),
    } {
        Ok(h) => h,
        Err(e) => {
            eprintln!("ingest failed: {}", e);
            return 1;
        }
    };

    println!("{}", hex32(&mh));
    0
}


fn cmd_ingest_wiki_xml(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut xml_path: Option<&str> = None;
    let mut xml_bz2_path: Option<&str> = None;

    // Sizing knobs.
    let mut seg_mb: u32 = 4;
    let mut row_kb: u32 = 8;
    let mut chunk_rows: u32 = 1024;
    let mut max_docs: Option<u64> = None;

    // Sharding knobs.
    let mut shards: Option<u16> = None;
    let mut shard_id: Option<u16> = None;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--xml" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --xml value");
                    return 2;
                }
                xml_path = Some(&args[i]);
            }
            
            "--xml-bz2" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --xml-bz2 value");
                    return 2;
                }
                xml_bz2_path = Some(&args[i]);
            }


"--seg_mb" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --seg_mb value");
                    return 2;
                }
                seg_mb = match parse_u32(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
            }
            "--row_kb" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --row_kb value");
                    return 2;
                }
                row_kb = match parse_u32(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
            }
            "--chunk_rows" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --chunk_rows value");
                    return 2;
                }
                chunk_rows = match parse_u32(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
            }
            "--max_docs" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --max_docs value");
                    return 2;
                }
                max_docs = match parse_u64(&args[i]) {
                    Ok(v) => Some(v),
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
            }
            "--shards" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --shards value");
                    return 2;
                }
                let v = match parse_u32(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
                if v == 0 || v > (u16::MAX as u32) {
                    eprintln!("invalid --shards value");
                    return 2;
                }
                shards = Some(v as u16);
            }
            "--shard-id" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --shard-id value");
                    return 2;
                }
                let v = match parse_u32(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
                if v > (u16::MAX as u32) {
                    eprintln!("invalid --shard-id value");
                    return 2;
                }
                shard_id = Some(v as u16);
            }
            _ => {
                eprintln!("unknown arg: {}", args[i]);
                return 2;
            }
        }
        i += 1;
    }

    let (xml_path, is_bz2) = match (xml_path, xml_bz2_path) {
    (Some(p), None) => (p, false),
    (None, Some(p)) => (p, true),
    (None, None) => {
        eprintln!("missing --xml or --xml-bz2 value");
        return 2;
    }
    (Some(_), Some(_)) => {
        eprintln!("provide only one of --xml or --xml-bz2");
        return 2;
    }
};

    let shard_cfg = match (shards, shard_id) {
        (None, None) => None,
        (Some(sc), Some(sid)) => {
            if sid >= sc {
                eprintln!("shard-id out of range");
                return 2;
            }
            Some(ShardCfgV1 { shard_count: sc, shard_id: sid })
        }
        _ => {
            eprintln!("provide both --shards and --shard-id");
            return 2;
        }
    };

    let base_root = root.clone();
    let root = match shard_cfg {
        Some(sc) => base_root.join(format!("shards/{:04x}", sc.shard_id)),
        None => base_root,
    };

    let store = store_for(&root);


    let file = match std::fs::File::open(xml_path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("open failed: {}", e);
            return 1;
        }
    };
    // Translate sizing knobs to row/segment parameters.
    let seg_bytes: u64 = (seg_mb as u64).saturating_mul(1024u64).saturating_mul(1024u64);
    let row_bytes: u64 = (row_kb as u64).saturating_mul(1024u64);
    let mut seg_rows = if row_bytes == 0 { 1 } else { seg_bytes / row_bytes };
    if seg_rows == 0 {
        seg_rows = 1;
    }
    if seg_rows > (u32::MAX as u64) {
        seg_rows = u32::MAX as u64;
    }

    let mut cfg = WikiIngestCfg::default_v1();
    cfg.seg_rows = seg_rows as u32;
    cfg.chunk_rows = chunk_rows;
    cfg.row_max_bytes = row_bytes as usize;
    cfg.max_docs = max_docs;

    let mh = if is_bz2 {
    let dec = BzDecoder::new(file);
    let rr = std::io::BufReader::new(dec);
    match match shard_cfg {
        Some(sc) => ingest_wiki_xml_sharded(&store, rr, cfg, sc),
        None => ingest_wiki_xml(&store, rr, cfg),
    } {
        Ok(h) => h,
        Err(e) => {
            eprintln!("ingest failed: {}", e);
            return 1;
        }
    }
} else {
    let rr = std::io::BufReader::new(file);
    match match shard_cfg {
        Some(sc) => ingest_wiki_xml_sharded(&store, rr, cfg, sc),
        None => ingest_wiki_xml(&store, rr, cfg),
    } {
        Ok(h) => h,
        Err(e) => {
            eprintln!("ingest failed: {}", e);
            return 1;
        }
    }
};

    println!("{}", hex32(&mh));
    0
}


fn cmd_ingest_wiki_sharded(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut dump_path: Option<&str> = None;
    let mut out_file: Option<PathBuf> = None;

    // Sizing knobs.
    let mut seg_mb: u32 = 4;
    let mut row_kb: u32 = 8;
    let mut chunk_rows: u32 = 1024;
    let mut max_docs: Option<u64> = None;

    // Sharding knobs.
    let mut shards: Option<u16> = None;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--dump" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --dump value");
                    return 2;
                }
                dump_path = Some(&args[i]);
            }
            "--shards" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --shards value");
                    return 2;
                }
                let v = match parse_u32(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
                if v == 0 || v > (u16::MAX as u32) {
                    eprintln!("invalid --shards value");
                    return 2;
                }
                shards = Some(v as u16);
            }
            "--seg_mb" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --seg_mb value");
                    return 2;
                }
                seg_mb = match parse_u32(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
            }
            "--row_kb" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --row_kb value");
                    return 2;
                }
                row_kb = match parse_u32(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
            }
            "--chunk_rows" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --chunk_rows value");
                    return 2;
                }
                chunk_rows = match parse_u32(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
            }
            "--max_docs" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --max_docs value");
                    return 2;
                }
                max_docs = Some(match parse_u64(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                });
            }
            "--out-file" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --out-file value");
                    return 2;
                }
                out_file = Some(PathBuf::from(&args[i]));
            }
            x => {
                eprintln!("unexpected arg: {}", x);
                return 2;
            }
        }
        i += 1;
    }

    let dump = match dump_path {
        Some(p) => p,
        None => {
            eprintln!("missing --dump <path>");
            return 2;
        }
    };

    let shard_count = match shards {
        Some(n) => n,
        None => {
            eprintln!("missing --shards <n>");
            return 2;
        }
    };

    // Derive segment sizing deterministically from seg_mb and row_kb.
    let seg_bytes = (seg_mb as u64) * 1024 * 1024;
    let row_bytes = (row_kb as u64) * 1024;
    let mut seg_rows = if row_bytes == 0 { 1 } else { seg_bytes / row_bytes };
    if seg_rows < 1 {
        seg_rows = 1;
    }
    if seg_rows > (u32::MAX as u64) {
        seg_rows = u32::MAX as u64;
    }

    let mut entries: Vec<ShardEntryV1> = Vec::with_capacity(shard_count as usize);

    for sid in 0..shard_count {
        let shard = ShardCfgV1 { shard_count, shard_id: sid };
        if shard.validate().is_err() {
            eprintln!("bad shard cfg");
            return 2;
        }

        let shard_root = root.join(format!("shards/{:04x}", sid));
        let store = store_for(&shard_root);

        let f = match fs::File::open(dump) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("open failed: {}", e);
                return 1;
            }
        };
        let rr = BufReader::new(f);

        let mut cfg = WikiIngestCfg::default_v1();
        cfg.chunk_rows = chunk_rows;
        cfg.seg_rows = seg_rows as u32;
        cfg.row_max_bytes = row_bytes as usize;
        cfg.max_docs = max_docs;

        let mh = match ingest_wiki_tsv_sharded(&store, rr, cfg, shard) {
            Ok(h) => h,
            Err(e) => {
                eprintln!("ingest failed: {}", e);
                return 1;
            }
        };

        entries.push(ShardEntryV1 {
            shard_id: sid,
            shard_root_rel: format!("shards/{:04x}", sid),
            outputs: vec![ShardOutputV1 {
                tag: "wiki_ingest_manifest_v1".to_string(),
                hash: mh,
            }],
        });
    }

    let man = ShardManifestV1 {
        version: SHARD_MANIFEST_V1_VERSION,
        shard_count,
        mapping_id: SHARD_MAPPING_DOC_ID_HASH32_V1.to_string(),
        shards: entries,
    };

    let base_store = store_for(&root);
    let man_hash = match put_shard_manifest_v1(&base_store, &man) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("shard manifest store failed: {}", e);
            return 1;
        }
    };

    if let Some(p) = out_file {
        let s = format!("{}
", hex32(&man_hash));
        if let Err(e) = std::fs::write(&p, s.as_bytes()) {
            eprintln!("write error: {}", e);
            return 1;
        }
    }

    println!("{}", hex32(&man_hash));
    0
}

fn cmd_ingest_wiki_xml_sharded(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut xml_path: Option<&str> = None;
    let mut xml_bz2_path: Option<&str> = None;
    let mut out_file: Option<PathBuf> = None;

    // Sizing knobs.
    let mut seg_mb: u32 = 4;
    let mut row_kb: u32 = 8;
    let mut chunk_rows: u32 = 1024;
    let mut max_docs: Option<u64> = None;

    // Sharding knobs.
    let mut shards: Option<u16> = None;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--xml" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --xml value");
                    return 2;
                }
                xml_path = Some(&args[i]);
            }
            "--xml-bz2" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --xml-bz2 value");
                    return 2;
                }
                xml_bz2_path = Some(&args[i]);
            }
            "--shards" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --shards value");
                    return 2;
                }
                let v = match parse_u32(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
                if v == 0 || v > (u16::MAX as u32) {
                    eprintln!("invalid --shards value");
                    return 2;
                }
                shards = Some(v as u16);
            }
            "--seg_mb" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --seg_mb value");
                    return 2;
                }
                seg_mb = match parse_u32(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
            }
            "--row_kb" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --row_kb value");
                    return 2;
                }
                row_kb = match parse_u32(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
            }
            "--chunk_rows" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --chunk_rows value");
                    return 2;
                }
                chunk_rows = match parse_u32(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
            }
            "--max_docs" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --max_docs value");
                    return 2;
                }
                max_docs = Some(match parse_u64(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                });
            }
            "--out-file" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --out-file value");
                    return 2;
                }
                out_file = Some(PathBuf::from(&args[i]));
            }
            x => {
                eprintln!("unexpected arg: {}", x);
                return 2;
            }
        }
        i += 1;
    }

    let (xml_path, is_bz2) = match (xml_path, xml_bz2_path) {
        (Some(p), None) => (p, false),
        (None, Some(p)) => (p, true),
        (None, None) => {
            eprintln!("missing --xml or --xml-bz2 value");
            return 2;
        }
        (Some(_), Some(_)) => {
            eprintln!("provide only one of --xml or --xml-bz2");
            return 2;
        }
    };

    let shard_count = match shards {
        Some(n) => n,
        None => {
            eprintln!("missing --shards <n>");
            return 2;
        }
    };

    let seg_bytes: u64 = (seg_mb as u64).saturating_mul(1024u64).saturating_mul(1024u64);
    let row_bytes: u64 = (row_kb as u64).saturating_mul(1024u64);
    let mut seg_rows = if row_bytes == 0 { 1 } else { seg_bytes / row_bytes };
    if seg_rows == 0 {
        seg_rows = 1;
    }
    if seg_rows > (u32::MAX as u64) {
        seg_rows = u32::MAX as u64;
    }

    let mut entries: Vec<ShardEntryV1> = Vec::with_capacity(shard_count as usize);

    for sid in 0..shard_count {
        let shard = ShardCfgV1 { shard_count, shard_id: sid };
        if shard.validate().is_err() {
            eprintln!("bad shard cfg");
            return 2;
        }

        let shard_root = root.join(format!("shards/{:04x}", sid));
        let store = store_for(&shard_root);

        let file = match std::fs::File::open(xml_path) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("open failed: {}", e);
                return 1;
            }
        };

        let mut cfg = WikiIngestCfg::default_v1();
        cfg.seg_rows = seg_rows as u32;
        cfg.chunk_rows = chunk_rows;
        cfg.row_max_bytes = row_bytes as usize;
        cfg.max_docs = max_docs;

        let mh = if is_bz2 {
            let dec = BzDecoder::new(file);
            let rr = std::io::BufReader::new(dec);
            match ingest_wiki_xml_sharded(&store, rr, cfg, shard) {
                Ok(h) => h,
                Err(e) => {
                    eprintln!("ingest failed: {}", e);
                    return 1;
                }
            }
        } else {
            let rr = std::io::BufReader::new(file);
            match ingest_wiki_xml_sharded(&store, rr, cfg, shard) {
                Ok(h) => h,
                Err(e) => {
                    eprintln!("ingest failed: {}", e);
                    return 1;
                }
            }
        };

        entries.push(ShardEntryV1 {
            shard_id: sid,
            shard_root_rel: format!("shards/{:04x}", sid),
            outputs: vec![ShardOutputV1 {
                tag: "wiki_ingest_manifest_v1".to_string(),
                hash: mh,
            }],
        });
    }

    let man = ShardManifestV1 {
        version: SHARD_MANIFEST_V1_VERSION,
        shard_count,
        mapping_id: SHARD_MAPPING_DOC_ID_HASH32_V1.to_string(),
        shards: entries,
    };

    let base_store = store_for(&root);
    let man_hash = match put_shard_manifest_v1(&base_store, &man) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("shard manifest store failed: {}", e);
            return 1;
        }
    };

    if let Some(p) = out_file {
        let s = format!("{}
", hex32(&man_hash));
        if let Err(e) = std::fs::write(&p, s.as_bytes()) {
            eprintln!("write error: {}", e);
            return 1;
        }
    }

    println!("{}", hex32(&man_hash));
    0
}

fn cmd_load_wikipedia(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut dump_path: Option<PathBuf> = None;
    let mut xml_path: Option<PathBuf> = None;
    let mut xml_bz2_path: Option<PathBuf> = None;
    let mut out_file: Option<PathBuf> = None;

    // Sizing knobs.
    let mut seg_mb: u32 = 4;
    let mut row_kb: u32 = 8;
    let mut chunk_rows: u32 = 1024;
    let mut max_docs: Option<u64> = None;

    // Sharding knobs.
    let mut shards: Option<u16> = None;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--dump" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --dump value");
                    return 2;
                }
                dump_path = Some(PathBuf::from(&args[i]));
            }
            "--xml" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --xml value");
                    return 2;
                }
                xml_path = Some(PathBuf::from(&args[i]));
            }
            "--xml-bz2" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --xml-bz2 value");
                    return 2;
                }
                xml_bz2_path = Some(PathBuf::from(&args[i]));
            }
            "--shards" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --shards value");
                    return 2;
                }
                let v = match parse_u32(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
                if v == 0 || v > (u16::MAX as u32) {
                    eprintln!("invalid --shards value");
                    return 2;
                }
                shards = Some(v as u16);
            }
            "--seg_mb" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --seg_mb value");
                    return 2;
                }
                seg_mb = match parse_u32(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
            }
            "--row_kb" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --row_kb value");
                    return 2;
                }
                row_kb = match parse_u32(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
            }
            "--chunk_rows" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --chunk_rows value");
                    return 2;
                }
                chunk_rows = match parse_u32(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
            }
            "--max_docs" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --max_docs value");
                    return 2;
                }
                max_docs = Some(match parse_u64(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                });
            }
            "--out-file" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --out-file value");
                    return 2;
                }
                out_file = Some(PathBuf::from(&args[i]));
            }
            x => {
                eprintln!("unexpected arg: {}", x);
                return 2;
            }
        }
        i += 1;
    }

    let (source_path, source_kind) = match (dump_path, xml_path, xml_bz2_path) {
        (Some(p), None, None) => (p, "dump"),
        (None, Some(p), None) => (p, "xml"),
        (None, None, Some(p)) => (p, "xml-bz2"),
        (None, None, None) => {
            eprintln!("missing --dump, --xml, or --xml-bz2 input");
            return 2;
        }
        _ => {
            eprintln!("provide only one of --dump, --xml, or --xml-bz2");
            return 2;
        }
    };

    let shard_count = match shards {
        Some(n) => n,
        None => {
            eprintln!("missing --shards <n>");
            return 2;
        }
    };

    // Derive segment sizing deterministically from seg_mb and row_kb.
    let seg_bytes: u64 = (seg_mb as u64).saturating_mul(1024u64).saturating_mul(1024u64);
    let row_bytes: u64 = (row_kb as u64).saturating_mul(1024u64);
    let mut seg_rows = if row_bytes == 0 { 1 } else { seg_bytes / row_bytes };
    if seg_rows < 1 {
        seg_rows = 1;
    }
    if seg_rows > (u32::MAX as u64) {
        seg_rows = u32::MAX as u64;
    }

    // Sharded ingest.
    let mut entries: Vec<ShardEntryV1> = Vec::with_capacity(shard_count as usize);
    for sid in 0..shard_count {
        let shard = ShardCfgV1 { shard_count, shard_id: sid };
        if shard.validate().is_err() {
            eprintln!("bad shard cfg");
            return 2;
        }

        let shard_root = root.join(format!("shards/{:04x}", sid));
        let store = store_for(&shard_root);

        let mut cfg = WikiIngestCfg::default_v1();
        cfg.chunk_rows = chunk_rows;
        cfg.seg_rows = seg_rows as u32;
        cfg.row_max_bytes = row_bytes as usize;
        cfg.max_docs = max_docs;

        let mh = match source_kind {
            "dump" => {
                let f = match fs::File::open(&source_path) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("open failed: {}", e);
                        return 1;
                    }
                };
                let rr = BufReader::new(f);
                match ingest_wiki_tsv_sharded(&store, rr, cfg, shard) {
                    Ok(h) => h,
                    Err(e) => {
                        eprintln!("ingest failed: {}", e);
                        return 1;
                    }
                }
            }
            "xml" => {
                let f = match fs::File::open(&source_path) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("open failed: {}", e);
                        return 1;
                    }
                };
                let rr = BufReader::new(f);
                match ingest_wiki_xml_sharded(&store, rr, cfg, shard) {
                    Ok(h) => h,
                    Err(e) => {
                        eprintln!("ingest failed: {}", e);
                        return 1;
                    }
                }
            }
            "xml-bz2" => {
                let f = match fs::File::open(&source_path) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("open failed: {}", e);
                        return 1;
                    }
                };
                let dec = BzDecoder::new(f);
                let rr = BufReader::new(dec);
                match ingest_wiki_xml_sharded(&store, rr, cfg, shard) {
                    Ok(h) => h,
                    Err(e) => {
                        eprintln!("ingest failed: {}", e);
                        return 1;
                    }
                }
            }
            _ => {
                eprintln!("internal error: unknown input kind");
                return 1;
            }
        };

        entries.push(ShardEntryV1 {
            shard_id: sid,
            shard_root_rel: format!("shards/{:04x}", sid),
            outputs: vec![ShardOutputV1 {
                tag: "wiki_ingest_manifest_v1".to_string(),
                hash: mh,
            }],
        });
    }

    let base_store = store_for(&root);

    let ingest_man = ShardManifestV1 {
        version: SHARD_MANIFEST_V1_VERSION,
        shard_count,
        mapping_id: SHARD_MAPPING_DOC_ID_HASH32_V1.to_string(),
        shards: entries,
    };

    let ingest_man_hash = match put_shard_manifest_v1(&base_store, &ingest_man) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("shard manifest store failed: {}", e);
            return 1;
        }
    };

    // Per-shard build-index and updated manifest.
    let mut idx_entries: Vec<ShardEntryV1> = Vec::with_capacity(shard_count as usize);
    for se in ingest_man.shards.iter() {
        let sid = se.shard_id;
        let shard_root = root.join(&se.shard_root_rel);
        let shard_store = store_for(&shard_root);

        let mut outputs: Vec<ShardOutputV1> = Vec::new();
        for o in se.outputs.iter() {
            if o.tag == "index_snapshot_v1" || o.tag == "index_sig_map_v1" {
                continue;
            }
            outputs.push(o.clone());
        }

        match try_build_index_v1_in_store(&shard_store) {
            Ok(Some((snap_id, sig_map_id))) => {
                outputs.push(ShardOutputV1 {
                    tag: "index_sig_map_v1".to_string(),
                    hash: sig_map_id,
                });
                outputs.push(ShardOutputV1 {
                    tag: "index_snapshot_v1".to_string(),
                    hash: snap_id,
                });
            }
            Ok(None) => {}
            Err(e) => {
                eprintln!("build-index failed for shard {}: {}", sid, e);
                return 1;
            }
        }

        outputs.sort_by(|a, b| a.tag.cmp(&b.tag));
        let mut prev: Option<&str> = None;
        for o in outputs.iter() {
            if let Some(p) = prev {
                if o.tag.as_str() == p {
                    eprintln!("duplicate output tag in shard {}: {}", sid, o.tag);
                    return 1;
                }
            }
            prev = Some(o.tag.as_str());
        }

        idx_entries.push(ShardEntryV1 {
            shard_id: sid,
            shard_root_rel: se.shard_root_rel.clone(),
            outputs,
        });
    }

    let index_man = ShardManifestV1 {
        version: SHARD_MANIFEST_V1_VERSION,
        shard_count,
        mapping_id: SHARD_MAPPING_DOC_ID_HASH32_V1.to_string(),
        shards: idx_entries,
    };

    let index_man_hash = match put_shard_manifest_v1(&base_store, &index_man) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("shard manifest store failed: {}", e);
            return 1;
        }
    };

    // Deterministic reduce merge.
    let red: ReduceIndexResultV1 = match reduce_index_v1(&root, &index_man_hash) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("reduce-index failed: {}", e);
            return 1;
        }
    };

    // Update workspace defaults (preserve existing optional values).
    let mut ws: WorkspaceV1 = match read_workspace_v1(&root) {
        Ok(Some(w)) => w,
        Ok(None) => WorkspaceV1::default(),
        Err(e) => {
            eprintln!("workspace read error: {}", e);
            eprintln!("workspace file: {}/{}", root.to_string_lossy(), WORKSPACE_V1_FILENAME);
            return 1;
        }
    };
    ws.merged_snapshot = Some(red.merged_snapshot);
    ws.merged_sig_map = Some(red.merged_sig_map);
    if let Err(e) = ws.validate_pair_consistency() {
        eprintln!("workspace error: {}", e);
        return 1;
    }
    if let Err(e) = write_workspace_v1_atomic(&root, &ws) {
        eprintln!("workspace write error: {}", e);
        eprintln!("workspace file: {}/{}", root.to_string_lossy(), WORKSPACE_V1_FILENAME);
        return 1;
    }

    let mut out = String::new();
    out.push_str(&format!("shard_manifest_ingest={}\n", hex32(&ingest_man_hash)));
    out.push_str(&format!("shard_manifest_index={}\n", hex32(&index_man_hash)));
    out.push_str(&format!("reduce_manifest={}\n", hex32(&red.reduce_manifest)));
    out.push_str(&format!("merged_snapshot={}\n", hex32(&red.merged_snapshot)));
    out.push_str(&format!("merged_sig_map={}\n", hex32(&red.merged_sig_map)));
    out.push_str(&format!("workspace_written=1\n"));

    if let Some(p) = out_file {
        if let Err(e) = std::fs::write(&p, out.as_bytes()) {
            eprintln!("write error: {}", e);
            return 1;
        }
    }

    print!("{}", out);
    0
}


fn cmd_load_wiktionary(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut xml_path: Option<PathBuf> = None;
    let mut xml_bz2_path: Option<PathBuf> = None;
    let mut segments: Option<u32> = None;
    let mut max_pages: Option<u64> = None;
    let mut out_file: Option<PathBuf> = None;
    let mut stats = false;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--xml" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --xml value");
                    return 2;
                }
                xml_path = Some(PathBuf::from(&args[i]));
            }
            "--xml-bz2" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --xml-bz2 value");
                    return 2;
                }
                xml_bz2_path = Some(PathBuf::from(&args[i]));
            }
            "--segments" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --segments value");
                    return 2;
                }
                segments = Some(match parse_u32(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                });
            }
            "--max_pages" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --max_pages value");
                    return 2;
                }
                max_pages = Some(match parse_u64(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                });
            }
            "--out-file" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --out-file value");
                    return 2;
                }
                out_file = Some(PathBuf::from(&args[i]));
            }
            "--stats" => {
                stats = true;
            }
            x => {
                eprintln!("unknown arg: {}", x);
                return 2;
            }
        }
        i += 1;
    }

    if xml_path.is_some() && xml_bz2_path.is_some() {
        eprintln!("choose exactly one of --xml or --xml-bz2");
        return 2;
    }
    let input_path = match (xml_path, xml_bz2_path) {
        (Some(p), None) => (p, false),
        (None, Some(p)) => (p, true),
        _ => {
            eprintln!("missing --xml <path> or --xml-bz2 <path>");
            return 2;
        }
    };

    let segs = match segments {
        Some(v) if v > 0 => v as usize,
        _ => {
            eprintln!("missing --segments <n>");
            return 2;
        }
    };

    let store = match FsArtifactStore::new(&root) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("open store failed: {}", e);
            return 1;
        }
    };

    let f = match fs::File::open(&input_path.0) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("open failed: {}", e);
            return 1;
        }
    };

    let parse_cfg = WiktionaryParseCfg::default_v1();

    let rep = if input_path.1 {
        let rr = BufReader::new(BzDecoder::new(BufReader::new(f)));
        ingest_wiktionary_xml_to_lexicon_snapshot_v1(&store, rr, segs, parse_cfg, max_pages)
    } else {
        let rr = BufReader::new(f);
        ingest_wiktionary_xml_to_lexicon_snapshot_v1(&store, rr, segs, parse_cfg, max_pages)
    };

    let rep = match rep {
        Ok(r) => r,
        Err(e) => {
            eprintln!("load-wiktionary failed: {}", e);
            return 1;
        }
    };

    // Update workspace defaults (preserve existing values).
    let mut ws: WorkspaceV1 = match read_workspace_v1(&root) {
        Ok(Some(w)) => w,
        Ok(None) => WorkspaceV1::default(),
        Err(e) => {
            eprintln!("workspace read error: {}", e);
            eprintln!("workspace file: {}/{}", root.to_string_lossy(), WORKSPACE_V1_FILENAME);
            return 1;
        }
    };
    ws.lexicon_snapshot = Some(rep.snapshot_hash);
    if let Err(e) = ws.validate_pair_consistency() {
        eprintln!("workspace error: {}", e);
        return 1;
    }
    if let Err(e) = write_workspace_v1_atomic(&root, &ws) {
        eprintln!("workspace write error: {}", e);
        eprintln!("workspace file: {}/{}", root.to_string_lossy(), WORKSPACE_V1_FILENAME);
        return 1;
    }

    let mut out = String::new();
    out.push_str(&format!("lexicon_snapshot={}\n", hex32(&rep.snapshot_hash)));
    out.push_str(&format!("segments_written={}\n", rep.segment_hashes.len() as u64));
    if stats {
        out.push_str(&format!("pages_seen={}\n", rep.pages_seen));
        out.push_str(&format!("pages_english={}\n", rep.pages_kept));
        out.push_str(&format!("lemmas={}\n", rep.lemmas_total));
        out.push_str(&format!("senses={}\n", rep.senses_total));
        out.push_str(&format!("rel_edges={}\n", rep.rels_total));
        out.push_str(&format!("prons={}\n", rep.prons_total));
    }
    out.push_str("workspace_written=1\n");

    if let Some(p) = out_file {
        if let Err(e) = fs::write(&p, out.as_bytes()) {
            eprintln!("write out-file failed: {}", e);
            return 1;
        }
    }

    print!("{}", out);
    0
}


fn cmd_build_index(args: &[String]) -> i32 {
    let mut root = default_root();

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            _ => {
                eprintln!("unknown arg: {}", args[i]);
                return 2;
            }
        }
        i += 1;
    }

    let store = match FsArtifactStore::new(&root) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("store error: {}", e);
            return 1;
        }
    };

    let mut paths: Vec<PathBuf> = Vec::new();
    collect_bin_paths(store.root(), &mut paths);

    let mut entries: Vec<IndexSnapshotEntryV1> = Vec::new();
    let mut source_id: Option<fsa_lm::frame::SourceId> = None;

    let mut sig_pairs: Vec<(Hash32, Hash32)> = Vec::new();
    let bloom_bytes: usize = 4096;
    let bloom_k: u8 = 6;

    for p in paths {
        let bytes = match std::fs::read(&p) {
            Ok(b) => b,
            Err(_) => continue,
        };
        if bytes.len() < FRAME_SEGMENT_MAGIC.len() {
            continue;
        }
                if &bytes[..FRAME_SEGMENT_MAGIC.len()] != &FRAME_SEGMENT_MAGIC[..] {
            continue;
        }

        let seg_hash = blake3_hash(&bytes);
        let seg = match FrameSegmentV1::decode(&bytes) {
            Ok(s) => s,
            Err(_) => continue,
        };

        let idx = match IndexSegmentV1::build_from_segment(seg_hash, &seg) {
            Ok(x) => x,
            Err(e) => {
                eprintln!("index build error for {}: {}", p.display(), e);
                return 1;
            }
        };

        if let Some(src) = source_id {
            if idx.source_id != src {
                eprintln!(
                    "mixed source_id detected; expected {}, saw {}",
                    src.0 .0,
                    idx.source_id.0 .0
                );
                eprintln!("build-index currently requires a single source_id per snapshot");
                return 1;
            }
        } else {
            source_id = Some(idx.source_id);
        }

        let idx_bytes = match idx.encode() {
            Ok(b) => b,
            Err(e) => {
                eprintln!("index encode error: {}", e);
                return 1;
            }
        };


        let idx_hash = match store.put(&idx_bytes) {
            Ok(h) => h,
            Err(e) => {
                eprintln!("store error: {}", e);
                return 1;
            }
        };

        // Build and store a SegmentSigV1 for this index artifact (IndexSegmentV1).
        // This signature is used later for deterministic gating/skip decisions.
        let mut sig_terms: Vec<fsa_lm::frame::TermId> = Vec::with_capacity(idx.terms.len());
        for t in &idx.terms {
            sig_terms.push(t.term);
        }
        let sig = match fsa_lm::segment_sig::SegmentSigV1::build(idx_hash, &sig_terms, bloom_bytes, bloom_k) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("segment sig build error: {}", e);
                return 1;
            }
        };
        let sig_id = match fsa_lm::segment_sig_store::put_segment_sig_v1(&store, &sig) {
            Ok(h) => h,
            Err(e) => {
                eprintln!("segment sig store error: {}", e);
                return 1;
            }
        };
        sig_pairs.push((idx_hash, sig_id));

        let term_count = if idx.terms.len() > (u32::MAX as usize) {
            eprintln!("term_count overflow");
            return 1;
        } else {
            idx.terms.len() as u32
        };

        let postings_bytes = if idx.postings.len() > (u32::MAX as usize) {
            eprintln!("postings_bytes overflow");
            return 1;
        } else {
            idx.postings.len() as u32
        };

        entries.push(IndexSnapshotEntryV1 {
            frame_seg: seg_hash,
            index_seg: idx_hash,
            row_count: idx.row_count,
            term_count,
            postings_bytes,
        });
    }

    let src = match source_id {
        Some(s) => s,
        None => {
            eprintln!("no FrameSegment artifacts found under {}", root.display());
            return 1;
        }
    };

    let mut snap = IndexSnapshotV1::new(src);
    snap.entries = entries;

    let snap_bytes = match snap.encode() {
        Ok(b) => b,
        Err(e) => {
            eprintln!("snapshot encode error: {}", e);
            return 1;
        }
    };
    let snap_hash = match store.put(&snap_bytes) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("store error: {}", e);
            return 1;
        }
    };


    // Store IndexSigMapV1 sidecar for this snapshot.
    let mut sig_map = fsa_lm::index_sig_map::IndexSigMapV1::new(src);
    for (idx_id, sig_id) in sig_pairs {
        sig_map.push(idx_id, sig_id);
    }
    let sig_map_id = match fsa_lm::index_sig_map_store::put_index_sig_map_v1(&store, &sig_map) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("index sig map store error: {}", e);
            return 1;
        }
    };

    // Snapshot hash to stdout for scripting; sidecar hash to stderr.
    println!("{}", hex32(&snap_hash));
    eprintln!("index_sig_map={}", hex32(&sig_map_id));
    0
}

fn cmd_compact_index(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut snapshot_hex: Option<String> = None;
    let mut target_bytes: u64 = 64 * 1024 * 1024;
    let mut max_out_segments: u32 = 8;
    let mut dry_run: bool = false;
    let mut verbose: bool = false;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--snapshot" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --snapshot value");
                    return 2;
                }
                snapshot_hex = Some(args[i].clone());
            }
            "--target-bytes" | "--target_bytes" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --target-bytes value");
                    return 2;
                }
                match parse_u64(&args[i]) {
                    Ok(v) => target_bytes = v,
                    Err(_) => {
                        eprintln!("bad --target-bytes value");
                        return 2;
                    }
                }
            }
            "--max-out-segments" | "--max_out_segments" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --max-out-segments value");
                    return 2;
                }
                match parse_u32(&args[i]) {
                    Ok(v) if v >= 1 => max_out_segments = v,
                    _ => {
                        eprintln!("bad --max-out-segments value");
                        return 2;
                    }
                }
            }
            "--dry-run" => {
                dry_run = true;
            }
            "--verbose" => {
                verbose = true;
            }
            _ => {
                eprintln!("unknown arg: {}", args[i]);
                return 2;
            }
        }
        i += 1;
    }

    let snap_hex = match snapshot_hex {
        Some(x) => x,
        None => {
            eprintln!("missing --snapshot");
            return 2;
        }
    };
    let snap_id = match parse_hash32_hex(&snap_hex) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("bad snapshot hash: {}", e);
            return 2;
        }
    };

    let store = match FsArtifactStore::new(&root) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("store error: {}", e);
            return 1;
        }
    };

    let cfg = CompactionCfgV1 {
        target_bytes_per_out_segment: target_bytes,
        max_out_segments,
        used_even_pack_fallback: false,
        dry_run,
    };

    let res = match compact_index_snapshot_v1(&store, &snap_id, cfg) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("compaction error: {}", e);
            return 1;
        }
    };

    if dry_run {
        let r = res.report;
        println!("dry_run=1");
        println!("input_snapshot={}", hex32(&r.input_snapshot_id));
        println!("target_bytes_per_out_segment={}", r.cfg.target_bytes_per_out_segment);
        println!("max_out_segments={}", r.cfg.max_out_segments);
        println!("used_even_pack_fallback={}", if r.cfg.used_even_pack_fallback { 1 } else { 0 });
        println!("bytes_input_total={}", r.bytes_input_total);
        println!("groups={}", r.groups.len());
        if verbose {
            for (gi, g) in r.groups.iter().enumerate() {
                println!(
                    "group={} start_ix={} len={} est_bytes_in={}",
                    gi,
                    g.start_ix,
                    g.len,
                    g.est_bytes_in
                );
            }
        }
        return 0;
    }

    let out_snap = match res.report.output_snapshot_id {
        Some(h) => h,
        None => {
            eprintln!("missing output snapshot id");
            return 1;
        }
    };

    // Print the new snapshot hash to stdout for scripting.
    println!("{}", hex32(&out_snap));

    if let Some(m) = res.output_sig_map_id {
        eprintln!("index_sig_map={}", hex32(&m));
    }

    // Summary to stderr.
    if let Some(rep) = res.report_id {
        eprintln!("report={}", hex32(&rep));
    }
    eprintln!(
        "compaction bytes_in={} bytes_out={} packs={} used_even_pack_fallback={}",
        res.report.bytes_input_total,
        res.report.bytes_output_total,
        res.report.groups.len(),
        if res.report.cfg.used_even_pack_fallback { 1 } else { 0 }
    );

    if verbose {
        for (gi, g) in res.report.groups.iter().enumerate() {
            let out_id = match g.out_segment_id {
                Some(h) => hex32(&h),
                None => "<none>".to_string(),
            };
            eprintln!(
                "group={} start_ix={} len={} est_bytes_in={} out_id={} out_bytes={}",
                gi,
                g.start_ix,
                g.len,
                g.est_bytes_in,
                out_id,
                g.out_bytes
            );
        }
    }

    0
}



fn try_build_index_v1_in_store(store: &FsArtifactStore) -> Result<Option<(Hash32, Hash32)>, String> {
    let mut paths: Vec<PathBuf> = Vec::new();
    collect_bin_paths(store.root(), &mut paths);

    let mut entries: Vec<IndexSnapshotEntryV1> = Vec::new();
    let mut source_id: Option<fsa_lm::frame::SourceId> = None;

    let mut sig_pairs: Vec<(Hash32, Hash32)> = Vec::new();
    let bloom_bytes: usize = 4096;
    let bloom_k: u8 = 6;

    for p in paths {
        let bytes = match std::fs::read(&p) {
            Ok(b) => b,
            Err(_) => continue,
        };
        if bytes.len() < FRAME_SEGMENT_MAGIC.len() {
            continue;
        }
        if &bytes[..FRAME_SEGMENT_MAGIC.len()] != &FRAME_SEGMENT_MAGIC[..] {
            continue;
        }

        let seg_hash = blake3_hash(&bytes);
        let seg = match FrameSegmentV1::decode(&bytes) {
            Ok(s) => s,
            Err(_) => continue,
        };

        let idx = match IndexSegmentV1::build_from_segment(seg_hash, &seg) {
            Ok(x) => x,
            Err(e) => {
                return Err(format!("index build error for {}: {}", p.display(), e));
            }
        };

        if let Some(src) = source_id {
            if idx.source_id != src {
                return Err(format!(
                    "mixed source_id detected; expected {}, saw {}",
                    src.0 .0,
                    idx.source_id.0 .0
                ));
            }
        } else {
            source_id = Some(idx.source_id);
        }

        let idx_bytes = match idx.encode() {
            Ok(b) => b,
            Err(e) => {
                return Err(format!("index encode error: {}", e));
            }
        };

        let idx_hash = match store.put(&idx_bytes) {
            Ok(h) => h,
            Err(e) => {
                return Err(format!("store error: {}", e));
            }
        };

        // Build and store SegmentSigV1 for this IndexSegmentV1.
        let mut sig_terms: Vec<fsa_lm::frame::TermId> = Vec::with_capacity(idx.terms.len());
        for t in &idx.terms {
            sig_terms.push(t.term);
        }
        let sig = match fsa_lm::segment_sig::SegmentSigV1::build(idx_hash, &sig_terms, bloom_bytes, bloom_k) {
            Ok(s) => s,
            Err(e) => {
                return Err(format!("segment sig build error: {}", e));
            }
        };
        let sig_id = match fsa_lm::segment_sig_store::put_segment_sig_v1(store, &sig) {
            Ok(h) => h,
            Err(e) => {
                return Err(format!("segment sig store error: {}", e));
            }
        };
        sig_pairs.push((idx_hash, sig_id));

        let term_count = if idx.terms.len() > (u32::MAX as usize) {
            return Err("term_count overflow".to_string());
        } else {
            idx.terms.len() as u32
        };

        let postings_bytes = if idx.postings.len() > (u32::MAX as usize) {
            return Err("postings_bytes overflow".to_string());
        } else {
            idx.postings.len() as u32
        };

        entries.push(IndexSnapshotEntryV1 {
            frame_seg: seg_hash,
            index_seg: idx_hash,
            row_count: idx.row_count,
            term_count,
            postings_bytes,
        });
    }

    let src = match source_id {
        Some(s) => s,
        None => {
            return Ok(None);
        }
    };

    let mut snap = IndexSnapshotV1::new(src);
    snap.entries = entries;

    let snap_bytes = match snap.encode() {
        Ok(b) => b,
        Err(e) => {
            return Err(format!("snapshot encode error: {}", e));
        }
    };
    let snap_hash = match store.put(&snap_bytes) {
        Ok(h) => h,
        Err(e) => {
            return Err(format!("store error: {}", e));
        }
    };

    // Store IndexSigMapV1 sidecar for this snapshot.
    let mut sig_map = fsa_lm::index_sig_map::IndexSigMapV1::new(src);
    for (idx_id, sig_id) in sig_pairs {
        sig_map.push(idx_id, sig_id);
    }
    let sig_map_id = match fsa_lm::index_sig_map_store::put_index_sig_map_v1(store, &sig_map) {
        Ok(h) => h,
        Err(e) => {
            return Err(format!("index sig map store error: {}", e));
        }
    };

    Ok(Some((snap_hash, sig_map_id)))
}

fn cmd_build_index_sharded(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut shards: Option<u16> = None;
    let mut base_manifest_hex: Option<String> = None;
    let mut out_file: Option<PathBuf> = None;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--shards" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --shards value");
                    return 2;
                }
                let v = match parse_u32(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
                if v == 0 || v > (u16::MAX as u32) {
                    eprintln!("invalid --shards value");
                    return 2;
                }
                shards = Some(v as u16);
            }
            "--manifest" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --manifest value");
                    return 2;
                }
                base_manifest_hex = Some(args[i].clone());
            }
            "--out-file" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --out-file value");
                    return 2;
                }
                out_file = Some(PathBuf::from(&args[i]));
            }
            x => {
                eprintln!("unexpected arg: {}", x);
                return 2;
            }
        }
        i += 1;
    }

    let shard_count = match shards {
        Some(n) => n,
        None => {
            eprintln!("missing --shards <n>");
            return 2;
        }
    };

    let base_store = store_for(&root);

    // Optional: load a prior shard manifest to preserve non-index outputs.
    let mut prior_outputs: Vec<Vec<ShardOutputV1>> = vec![Vec::new(); shard_count as usize];
    if let Some(hx) = base_manifest_hex {
        let h = match parse_hash32_hex(&hx) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("bad manifest hash: {}", e);
                return 2;
            }
        };
        let man = match get_shard_manifest_v1(&base_store, &h) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("manifest load error: {}", e);
                return 1;
            }
        };
        let man = match man {
            Some(m) => m,
            None => {
                eprintln!("manifest not found");
                return 3;
            }
        };
        if man.shard_count != shard_count {
            eprintln!("manifest shard_count mismatch");
            return 2;
        }
        if man.mapping_id != SHARD_MAPPING_DOC_ID_HASH32_V1.to_string() {
            eprintln!("manifest mapping_id mismatch");
            return 2;
        }
        for se in man.shards.iter() {
            let sid = se.shard_id as usize;
            if sid < prior_outputs.len() {
                prior_outputs[sid] = se.outputs.clone();
            }
        }
    }

    let mut entries: Vec<ShardEntryV1> = Vec::with_capacity(shard_count as usize);

    for sid in 0..shard_count {
        let shard_root_rel = format!("shards/{:04x}", sid);
        let shard_root = root.join(&shard_root_rel);
        let shard_store = store_for(&shard_root);

        let mut outputs: Vec<ShardOutputV1> = Vec::new();
        // Preserve prior outputs except for the index tags we are writing in this stage.
        for o in prior_outputs[sid as usize].iter() {
            if o.tag == "index_snapshot_v1" || o.tag == "index_sig_map_v1" {
                continue;
            }
            outputs.push(o.clone());
        }

        match try_build_index_v1_in_store(&shard_store) {
            Ok(Some((snap_id, sig_map_id))) => {
                outputs.push(ShardOutputV1 {
                    tag: "index_sig_map_v1".to_string(),
                    hash: sig_map_id,
                });
                outputs.push(ShardOutputV1 {
                    tag: "index_snapshot_v1".to_string(),
                    hash: snap_id,
                });
            }
            Ok(None) => {
                // No FrameSegment artifacts in this shard; record no index outputs.
            }
            Err(e) => {
                eprintln!("build-index failed for shard {}: {}", sid, e);
                return 1;
            }
        }

        outputs.sort_by(|a, b| a.tag.cmp(&b.tag));
        // Check duplicates after sort.
        let mut prev: Option<&str> = None;
        for o in outputs.iter() {
            if let Some(p) = prev {
                if o.tag.as_str() == p {
                    eprintln!("duplicate output tag in shard {}: {}", sid, o.tag);
                    return 1;
                }
            }
            prev = Some(o.tag.as_str());
        }

        entries.push(ShardEntryV1 {
            shard_id: sid,
            shard_root_rel,
            outputs,
        });
    }

    let man = ShardManifestV1 {
        version: SHARD_MANIFEST_V1_VERSION,
        shard_count,
        mapping_id: SHARD_MAPPING_DOC_ID_HASH32_V1.to_string(),
        shards: entries,
    };

    let man_hash = match put_shard_manifest_v1(&base_store, &man) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("shard manifest store failed: {}", e);
            return 1;
        }
    };

    if let Some(p) = out_file {
        let s = format!("{}\n", hex32(&man_hash));
        if let Err(e) = std::fs::write(&p, s.as_bytes()) {
            eprintln!("write error: {}", e);
            return 1;
        }
    }

    println!("{}", hex32(&man_hash));
    0
}


fn cmd_reduce_index(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut manifest_hex: Option<&str> = None;
    let mut out_file: Option<PathBuf> = None;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--manifest" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --manifest value");
                    return 2;
                }
                manifest_hex = Some(&args[i]);
            }
            "--out-file" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --out-file value");
                    return 2;
                }
                out_file = Some(PathBuf::from(&args[i]));
            }
            x => {
                eprintln!("unexpected arg: {}", x);
                return 2;
            }
        }
        i += 1;
    }

    let man_hex = match manifest_hex {
        Some(v) => v,
        None => {
            eprintln!("missing --manifest <hash32hex>");
            return 2;
        }
    };

    let man_hash = match parse_hash32_hex(man_hex) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("{}", e);
            return 2;
        }
    };

    let res: ReduceIndexResultV1 = match reduce_index_v1(&root, &man_hash) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("reduce-index failed: {}", e);
            return 1;
        }
    };

    let out_lines = format!("{}\n{}\n{}\n",
        hex32(&res.reduce_manifest),
        hex32(&res.merged_snapshot),
        hex32(&res.merged_sig_map),
    );

    if let Some(p) = out_file {
        if let Err(e) = std::fs::write(&p, out_lines.as_bytes()) {
            eprintln!("write error: {}", e);
            return 1;
        }
    }

    print!("{}", out_lines);
    0
}

fn cmd_run_workflow(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut dump_path: Option<PathBuf> = None;
    let mut shards: Option<u16> = None;
    let mut out_file: Option<PathBuf> = None;

    // Ingest sizing knobs.
    let mut seg_mb: u32 = 4;
    let mut row_kb: u32 = 8;
    let mut chunk_rows: u32 = 1024;
    let mut max_docs: Option<u64> = None;

    // Optional sync-reduce step (client only; assumes a server is already running).
    let mut sync_addr: Option<String> = None;
    let mut sync_root: Option<PathBuf> = None;
    let mut max_chunk_kb: Option<u32> = None;
    let mut max_artifact_mb: Option<u32> = None;
    let mut rw_timeout_ms: Option<u32> = None;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--dump" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --dump value");
                    return 2;
                }
                dump_path = Some(PathBuf::from(&args[i]));
            }
            "--shards" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --shards value");
                    return 2;
                }
                let v = match parse_u32(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
                if v == 0 || v > (u16::MAX as u32) {
                    eprintln!("invalid --shards value");
                    return 2;
                }
                shards = Some(v as u16);
            }
            "--seg_mb" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --seg_mb value");
                    return 2;
                }
                seg_mb = match parse_u32(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
            }
            "--row_kb" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --row_kb value");
                    return 2;
                }
                row_kb = match parse_u32(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
            }
            "--chunk_rows" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --chunk_rows value");
                    return 2;
                }
                chunk_rows = match parse_u32(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                };
            }
            "--max_docs" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --max_docs value");
                    return 2;
                }
                max_docs = Some(match parse_u64(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                });
            }
            "--out-file" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --out-file value");
                    return 2;
                }
                out_file = Some(PathBuf::from(&args[i]));
            }
            "--sync-addr" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --sync-addr value");
                    return 2;
                }
                sync_addr = Some(args[i].clone());
            }
            "--sync-root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --sync-root value");
                    return 2;
                }
                sync_root = Some(PathBuf::from(&args[i]));
            }
            "--max_chunk_kb" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --max_chunk_kb value");
                    return 2;
                }
                max_chunk_kb = Some(match parse_u32(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                });
            }
            "--max_artifact_mb" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --max_artifact_mb value");
                    return 2;
                }
                max_artifact_mb = Some(match parse_u32(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                });
            }
            "--rw_timeout_ms" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --rw_timeout_ms value");
                    return 2;
                }
                rw_timeout_ms = Some(match parse_u32(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                });
            }
            x => {
                eprintln!("unexpected arg: {}", x);
                return 2;
            }
        }
        i += 1;
    }

    let dump = match dump_path {
        Some(p) => p,
        None => {
            eprintln!("missing --dump <path>");
            return 2;
        }
    };

    let shard_count = match shards {
        Some(n) => n,
        None => {
            eprintln!("missing --shards <n>");
            return 2;
        }
    };

    // Derive segment sizing deterministically from seg_mb and row_kb.
    let seg_bytes = (seg_mb as u64) * 1024 * 1024;
    let row_bytes = (row_kb as u64) * 1024;
    let mut seg_rows = if row_bytes == 0 { 1 } else { seg_bytes / row_bytes };
    if seg_rows < 1 {
        seg_rows = 1;
    }
    if seg_rows > (u32::MAX as u64) {
        seg_rows = u32::MAX as u64;
    }

    // Sharded ingest.
    let mut entries: Vec<ShardEntryV1> = Vec::with_capacity(shard_count as usize);
    for sid in 0..shard_count {
        let shard = ShardCfgV1 { shard_count, shard_id: sid };
        if shard.validate().is_err() {
            eprintln!("bad shard cfg");
            return 2;
        }

        let shard_root = root.join(format!("shards/{:04x}", sid));
        let store = store_for(&shard_root);

        let f = match fs::File::open(&dump) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("open failed: {}", e);
                return 1;
            }
        };
        let rr = BufReader::new(f);

        let mut cfg = WikiIngestCfg::default_v1();
        cfg.chunk_rows = chunk_rows;
        cfg.seg_rows = seg_rows as u32;
        cfg.row_max_bytes = row_bytes as usize;
        cfg.max_docs = max_docs;

        let mh = match ingest_wiki_tsv_sharded(&store, rr, cfg, shard) {
            Ok(h) => h,
            Err(e) => {
                eprintln!("ingest failed: {}", e);
                return 1;
            }
        };

        entries.push(ShardEntryV1 {
            shard_id: sid,
            shard_root_rel: format!("shards/{:04x}", sid),
            outputs: vec![ShardOutputV1 {
                tag: "wiki_ingest_manifest_v1".to_string(),
                hash: mh,
            }],
        });
    }

    let base_store = store_for(&root);

    let ingest_man = ShardManifestV1 {
        version: SHARD_MANIFEST_V1_VERSION,
        shard_count,
        mapping_id: SHARD_MAPPING_DOC_ID_HASH32_V1.to_string(),
        shards: entries,
    };

    let ingest_man_hash = match put_shard_manifest_v1(&base_store, &ingest_man) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("shard manifest store failed: {}", e);
            return 1;
        }
    };

    // Per-shard build-index and updated manifest.
    let mut idx_entries: Vec<ShardEntryV1> = Vec::with_capacity(shard_count as usize);
    for se in ingest_man.shards.iter() {
        let sid = se.shard_id;
        let shard_root = root.join(&se.shard_root_rel);
        let shard_store = store_for(&shard_root);

        let mut outputs: Vec<ShardOutputV1> = Vec::new();
        for o in se.outputs.iter() {
            if o.tag == "index_snapshot_v1" || o.tag == "index_sig_map_v1" {
                continue;
            }
            outputs.push(o.clone());
        }

        match try_build_index_v1_in_store(&shard_store) {
            Ok(Some((snap_id, sig_map_id))) => {
                outputs.push(ShardOutputV1 {
                    tag: "index_sig_map_v1".to_string(),
                    hash: sig_map_id,
                });
                outputs.push(ShardOutputV1 {
                    tag: "index_snapshot_v1".to_string(),
                    hash: snap_id,
                });
            }
            Ok(None) => {}
            Err(e) => {
                eprintln!("build-index failed for shard {}: {}", sid, e);
                return 1;
            }
        }

        outputs.sort_by(|a, b| a.tag.cmp(&b.tag));
        let mut prev: Option<&str> = None;
        for o in outputs.iter() {
            if let Some(p) = prev {
                if o.tag.as_str() == p {
                    eprintln!("duplicate output tag in shard {}: {}", sid, o.tag);
                    return 1;
                }
            }
            prev = Some(o.tag.as_str());
        }

        idx_entries.push(ShardEntryV1 {
            shard_id: sid,
            shard_root_rel: se.shard_root_rel.clone(),
            outputs,
        });
    }

    let index_man = ShardManifestV1 {
        version: SHARD_MANIFEST_V1_VERSION,
        shard_count,
        mapping_id: SHARD_MAPPING_DOC_ID_HASH32_V1.to_string(),
        shards: idx_entries,
    };

    let index_man_hash = match put_shard_manifest_v1(&base_store, &index_man) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("shard manifest store failed: {}", e);
            return 1;
        }
    };

    // Deterministic reduce merge.
    let red: ReduceIndexResultV1 = match reduce_index_v1(&root, &index_man_hash) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("reduce-index failed: {}", e);
            return 1;
        }
    };

    let mut out = String::new();
    out.push_str(&format!("shard_manifest_ingest={}
", hex32(&ingest_man_hash)));
    out.push_str(&format!("shard_manifest_index={}
", hex32(&index_man_hash)));
    out.push_str(&format!("reduce_manifest={}
", hex32(&red.reduce_manifest)));
    out.push_str(&format!("merged_snapshot={}
", hex32(&red.merged_snapshot)));
    out.push_str(&format!("merged_sig_map={}
", hex32(&red.merged_sig_map)));

    // Optional: 6c client sync step (assumes server is already running).
    if sync_addr.is_some() || sync_root.is_some() {
        let addr = match sync_addr {
            Some(a) => a,
            None => {
                eprintln!("missing --sync-addr value");
                return 2;
            }
        };
        let dst_root = match sync_root {
            Some(r) => r,
            None => {
                eprintln!("missing --sync-root value");
                return 2;
            }
        };

        let dst_store = store_for(&dst_root);
        let mut cfg = SyncClientCfgV1::default();
        if let Some(kb) = max_chunk_kb {
            match bytes_from_kb(kb) {
                Ok(b) => cfg.max_chunk_bytes = b,
                Err(e) => {
                    eprintln!("{}", e);
                    return 2;
                }
            }
        }
        if let Some(mb) = max_artifact_mb {
            match bytes_from_mb(mb) {
                Ok(b) => cfg.max_artifact_bytes = b,
                Err(e) => {
                    eprintln!("{}", e);
                    return 2;
                }
            }
        }
        if let Some(ms) = rw_timeout_ms {
            cfg.rw_timeout_ms = ms;
        }

        let stats = match sync_reduce_v1(&dst_store, &addr, &red.reduce_manifest, &cfg) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("sync-reduce failed: {}", e);
                return 1;
            }
        };
        out.push_str(&format!(
            "sync_stats needed_total={} already_present={} fetched={} bytes_fetched={}
",
            stats.needed_total,
            stats.already_present,
            stats.fetched,
            stats.bytes_fetched
        ));
    }

    print!("{}", out);
    if let Some(p) = out_file {
        if let Err(e) = fs::write(&p, out.as_bytes()) {
            eprintln!("write out-file failed: {}", e);
            return 1;
        }
    }
    0
}


fn cmd_ingest_wiktionary_xml(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut xml_path: Option<PathBuf> = None;
    let mut xml_bz2_path: Option<PathBuf> = None;
    let mut segments: Option<u32> = None;
    let mut max_pages: Option<u64> = None;
    let mut out_file: Option<PathBuf> = None;
    let mut stats = false;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--xml" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --xml value");
                    return 2;
                }
                xml_path = Some(PathBuf::from(&args[i]));
            }
            "--xml-bz2" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --xml-bz2 value");
                    return 2;
                }
                xml_bz2_path = Some(PathBuf::from(&args[i]));
            }
            "--segments" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --segments value");
                    return 2;
                }
                segments = Some(match parse_u32(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                });
            }
            "--max_pages" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --max_pages value");
                    return 2;
                }
                max_pages = Some(match parse_u64(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                });
            }
            "--out-file" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --out-file value");
                    return 2;
                }
                out_file = Some(PathBuf::from(&args[i]));
            }
            "--stats" => {
                stats = true;
            }
            x => {
                eprintln!("unknown arg: {}", x);
                return 2;
            }
        }
        i += 1;
    }

    if xml_path.is_some() && xml_bz2_path.is_some() {
        eprintln!("choose exactly one of --xml or --xml-bz2");
        return 2;
    }
    let input_path = match (xml_path, xml_bz2_path) {
        (Some(p), None) => (p, false),
        (None, Some(p)) => (p, true),
        _ => {
            eprintln!("missing --xml <path> or --xml-bz2 <path>");
            return 2;
        }
    };

    let segs = match segments {
        Some(v) if v > 0 => v as usize,
        _ => {
            eprintln!("missing --segments <n>");
            return 2;
        }
    };

    let store = match FsArtifactStore::new(&root) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("open store failed: {}", e);
            return 1;
        }
    };

    let f = match fs::File::open(&input_path.0) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("open failed: {}", e);
            return 1;
        }
    };

    let parse_cfg = WiktionaryParseCfg::default_v1();

    let rep = if input_path.1 {
        let rr = BufReader::new(BzDecoder::new(BufReader::new(f)));
        ingest_wiktionary_xml_to_lexicon_snapshot_v1(&store, rr, segs, parse_cfg, max_pages)
    } else {
        let rr = BufReader::new(f);
        ingest_wiktionary_xml_to_lexicon_snapshot_v1(&store, rr, segs, parse_cfg, max_pages)
    };

    let rep = match rep {
        Ok(r) => r,
        Err(e) => {
            eprintln!("ingest-wiktionary-xml failed: {}", e);
            return 1;
        }
    };

    let mut out = String::new();
    for h in rep.segment_hashes.iter() {
        out.push_str(&format!("segment={}\n", hex32(h)));
    }
    out.push_str(&format!("lexicon_snapshot={}\n", hex32(&rep.snapshot_hash)));
if stats {
    out.push_str(&format!("pages_seen={}\n", rep.pages_seen));
    out.push_str(&format!("pages_english={}\n", rep.pages_kept));
    out.push_str(&format!("lemmas={}\n", rep.lemmas_total));
    out.push_str(&format!("senses={}\n", rep.senses_total));
    out.push_str(&format!("rel_edges={}\n", rep.rels_total));
    out.push_str(&format!("prons={}\n", rep.prons_total));
    out.push_str(&format!(
        "segments_written={}\n",
        rep.segment_hashes.len() as u64
    ));
}


    print!("{}", out);
    if let Some(p) = out_file {
        if let Err(e) = fs::write(&p, out.as_bytes()) {
            eprintln!("write out-file failed: {}", e);
            return 1;
        }
    }
    0
}


fn cmd_build_lexicon_snapshot(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut segs: Vec<Hash32> = Vec::new();
    let mut out_file: Option<PathBuf> = None;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--segment" | "--seg" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --segment value");
                    return 2;
                }
                let h = match parse_hash32_hex(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("invalid segment hash: {}", e);
                        return 2;
                    }
                };
                segs.push(h);
            }
            "--out-file" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --out-file value");
                    return 2;
                }
                out_file = Some(PathBuf::from(&args[i]));
            }
            _ => {
                eprintln!("unknown arg: {}", args[i]);
                return 2;
            }
        }
        i += 1;
    }

    if segs.is_empty() {
        eprintln!("build-lexicon-snapshot requires at least one --segment");
        return 2;
    }

    let store = match FsArtifactStore::new(&root) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("store error: {}", e);
            return 1;
        }
    };

    let (snap_hash, snap) = match build_lexicon_snapshot_v1_from_segments(&store, &segs) {
        Ok(x) => x,
        Err(e) => {
            eprintln!("build error: {}", e);
            return 1;
        }
    };

    if let Some(p) = out_file {
        let bytes = match snap.encode() {
            Ok(b) => b,
            Err(e) => {
                eprintln!("encode error: {}", e);
                return 1;
            }
        };
        if let Err(e) = std::fs::write(&p, &bytes) {
            eprintln!("write error: {}", e);
            return 1;
        }
    }

    println!("{}", hex32(&snap_hash));
    0
}


fn cmd_validate_lexicon_snapshot(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut snap_hex: Option<String> = None;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--snapshot" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --snapshot value");
                    return 2;
                }
                snap_hex = Some(args[i].clone());
            }
            _ => {
                eprintln!("unknown arg: {}", args[i]);
                return 2;
            }
        }
        i += 1;
    }

    let snap_hex = match snap_hex {
        Some(v) => v,
        None => {
            eprintln!("validate-lexicon-snapshot requires --snapshot");
            return 2;
        }
    };

    let snap_hash = match parse_hash32_hex(&snap_hex) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("invalid snapshot hash: {}", e);
            return 2;
        }
    };

    let store = match FsArtifactStore::new(&root) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("store error: {}", e);
            return 1;
        }
    };

    match validate_lexicon_snapshot_v1_disjoint_owners(&store, &snap_hash) {
        Ok(()) => {
            println!("OK");
            0
        }
        Err(e) => {
            eprintln!("validate error: {}", e);
            1
        }
    }
}


fn cmd_build_pragmatics(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut prompt_hex: Option<String> = None;
    let mut source_id: u64 = 1;
    let mut tok_max_bytes: usize = TokenizerCfg::default().max_token_bytes;
    let mut lexicon_hex: Option<String> = None;
    let mut out_file: Option<PathBuf> = None;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--prompt" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --prompt value");
                    return 2;
                }
                prompt_hex = Some(args[i].clone());
            }
            "--source-id" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --source-id value");
                    return 2;
                }
                source_id = match parse_u64(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("invalid --source-id: {}", e);
                        return 2;
                    }
                };
            }
            "--tok-max-bytes" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --tok-max-bytes value");
                    return 2;
                }
                tok_max_bytes = match args[i].parse::<usize>() {
                    Ok(v) => v,
                    Err(_) => {
                        eprintln!("invalid --tok-max-bytes");
                        return 2;
                    }
                };
            }
            "--lexicon-snapshot" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --lexicon-snapshot value");
                    return 2;
                }
                lexicon_hex = Some(args[i].clone());
            }
            "--out-file" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --out-file value");
                    return 2;
                }
                out_file = Some(PathBuf::from(&args[i]));
            }
            _ => {
                eprintln!("unknown arg: {}", args[i]);
                return 2;
            }
        }
        i += 1;
    }


    if tok_max_bytes == 0 {
        eprintln!("tok-max-bytes must be > 0");
        return 2;
    }

    let prompt_hex = match prompt_hex {
        Some(v) => v,
        None => {
            eprintln!("build-pragmatics requires --prompt");
            return 2;
        }
    };

    let prompt_hash = match parse_hash32_hex(&prompt_hex) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("invalid prompt hash: {}", e);
            return 2;
        }
    };

    let store = match FsArtifactStore::new(&root) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("store error: {}", e);
            return 1;
        }
    };

    let pack = match get_prompt_pack(&store, &prompt_hash) {
        Ok(Some(p)) => p,
        Ok(None) => {
            eprintln!("not found");
            return 3;
        }
        Err(e) => {
            eprintln!("prompt load error: {}", e);
            return 1;
        }
    };

    let mut cfg = PragmaticsExtractCfg::default();
    cfg.tokenizer_cfg = TokenizerCfg {
        max_token_bytes: tok_max_bytes,
    };

    // Optional: lexicon-assisted cue neighborhoods for intent inference.
    let mut lex_view_opt: Option<fsa_lm::lexicon_expand_lookup::LexiconExpandLookupV1> = None;
    let mut lex_cues_opt: Option<fsa_lm::lexicon_neighborhoods::LexiconCueNeighborhoodsV1> = None;

    if let Some(lx) = lexicon_hex.as_ref() {
        let lh = match parse_hash32_hex(lx) {
            Ok(h) => h,
            Err(e) => {
                eprintln!("invalid --lexicon-snapshot hash: {}", e);
                return 2;
            }
        };

        let view_opt = match fsa_lm::lexicon_expand_lookup::load_lexicon_expand_lookup_v1(&store, &lh) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("lexicon load error: {}", e);
                return 1;
            }
        };
        let view = match view_opt {
            Some(v) => v,
            None => {
                eprintln!("missing lexicon snapshot: {}", hex32(&lh));
                return 3;
            }
        };
        let cues = fsa_lm::lexicon_neighborhoods::build_lexicon_cue_neighborhoods_v1(
            &view,
            &fsa_lm::lexicon_neighborhoods::LexiconNeighborhoodCfgV1::new(),
        );
        lex_view_opt = Some(view);
        lex_cues_opt = Some(cues);
    }

    if let (Some(v), Some(c)) = (lex_view_opt.as_ref(), lex_cues_opt.as_ref()) {
        cfg.lexicon_view = Some(v);
        cfg.lexicon_cues = Some(c);
    }

    let frames = match extract_pragmatics_frames_for_prompt_pack_v1(Id64(source_id), &pack, &cfg) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("extract error: {}", e);
            return 1;
        }
    };

    let mut hashes: Vec<Hash32> = Vec::with_capacity(frames.len());
    for f in &frames {
        let h = match put_pragmatics_frame_v1(&store, f) {
            Ok(x) => x,
            Err(e) => {
                eprintln!("store error: {}", e);
                return 1;
            }
        };
        hashes.push(h);
    }

    if let Some(p) = out_file {
        let mut out = String::new();
        for h in &hashes {
            out.push_str(&hex32(h));
            out.push('\n');
        }
        if let Err(e) = std::fs::write(&p, out.as_bytes()) {
            eprintln!("write error: {}", e);
            return 1;
        }
    }

    for h in &hashes {
        println!("{}", hex32(h));
    }

    0
}


#[cfg(test)]
mod validate_lexicon_snapshot_cli_tests {
    use super::*;

    use fsa_lm::frame::Id64;
    use fsa_lm::lexicon::{LemmaId, LemmaKeyId, LemmaRowV1, SenseId, SenseRowV1, TextId, LEXICON_SCHEMA_V1};
    use fsa_lm::lexicon_segment::LexiconSegmentV1;
    use fsa_lm::lexicon_segment_store::put_lexicon_segment_v1;
    use fsa_lm::lexicon_snapshot::{LexiconSnapshotEntryV1, LexiconSnapshotV1};
    use fsa_lm::lexicon_snapshot_store::put_lexicon_snapshot_v1;

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        use std::fs;
        let mut p = std::env::temp_dir();
        p.push("fsa_lm_tests");
        p.push(name);
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn lemma_row(id: u64) -> LemmaRowV1 {
        LemmaRowV1 {
            version: LEXICON_SCHEMA_V1,
            lemma_id: LemmaId(Id64(id)),
            lemma_key_id: LemmaKeyId(Id64(id + 1000)),
            lemma_text_id: TextId(Id64(id + 2000)),
            pos_mask: 1,
            flags: 0,
        }
    }

    #[test]
    fn cmd_validate_lexicon_snapshot_ok() {
        let dir = tmp_dir("cmd_validate_lexicon_snapshot_ok");
        let store = FsArtifactStore::new(&dir).unwrap();

        let seg1 = LexiconSegmentV1::build_from_rows(&[lemma_row(1)], &[], &[], &[]).unwrap();
        let seg2 = LexiconSegmentV1::build_from_rows(&[lemma_row(2)], &[], &[], &[]).unwrap();

        let h1 = put_lexicon_segment_v1(&store, &seg1).unwrap();
        let h2 = put_lexicon_segment_v1(&store, &seg2).unwrap();

        let mut snap = LexiconSnapshotV1::new();
        snap.entries.push(LexiconSnapshotEntryV1 {
            lex_seg: h1,
            lemma_count: 1,
            sense_count: 0,
            rel_count: 0,
            pron_count: 0,
        });
        snap.entries.push(LexiconSnapshotEntryV1 {
            lex_seg: h2,
            lemma_count: 1,
            sense_count: 0,
            rel_count: 0,
            pron_count: 0,
        });

        let snap_hash = put_lexicon_snapshot_v1(&store, &snap).unwrap();

        let args = vec![
            "--root".to_string(),
            dir.to_string_lossy().to_string(),
            "--snapshot".to_string(),
            hex32(&snap_hash),
        ];

        let rc = cmd_validate_lexicon_snapshot(&args);
        assert_eq!(rc, 0);
    }

    #[test]
    fn cmd_validate_lexicon_snapshot_conflict_fails() {
        let dir = tmp_dir("cmd_validate_lexicon_snapshot_conflict_fails");
        let store = FsArtifactStore::new(&dir).unwrap();

        // Two segments with the same lemma_id should fail the disjoint-owner rule.
        // Ensure the segment hashes differ so the snapshot can be encoded.
        let l = lemma_row(10);
        let s1 = SenseRowV1 {
            version: LEXICON_SCHEMA_V1,
            sense_id: SenseId(Id64(1010)),
            lemma_id: l.lemma_id,
            sense_rank: 0,
            gloss_text_id: TextId(Id64(3010)),
            labels_mask: 0,
        };

        let seg1 = LexiconSegmentV1::build_from_rows(&[l.clone()], &[], &[], &[]).unwrap();
        let seg2 = LexiconSegmentV1::build_from_rows(&[l], &[s1], &[], &[]).unwrap();

        let h1 = put_lexicon_segment_v1(&store, &seg1).unwrap();
        let h2 = put_lexicon_segment_v1(&store, &seg2).unwrap();

        let mut snap = LexiconSnapshotV1::new();
        snap.entries.push(LexiconSnapshotEntryV1 {
            lex_seg: h1,
            lemma_count: 1,
            sense_count: 0,
            rel_count: 0,
            pron_count: 0,
        });
        snap.entries.push(LexiconSnapshotEntryV1 {
            lex_seg: h2,
            lemma_count: 1,
            sense_count: 1,
            rel_count: 0,
            pron_count: 0,
        });

        let snap_hash = put_lexicon_snapshot_v1(&store, &snap).unwrap();

        let args = vec![
            "--root".to_string(),
            dir.to_string_lossy().to_string(),
            "--snapshot".to_string(),
            hex32(&snap_hash),
        ];

        let rc = cmd_validate_lexicon_snapshot(&args);
        assert_ne!(rc, 0);
    }
}

#[cfg(test)]
mod build_pragmatics_cli_tests {
    use super::*;

    use fsa_lm::pragmatics_frame_store::get_pragmatics_frame_v1;

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        use std::fs;
        let mut p = std::env::temp_dir();
        p.push("fsa_lm_tests");
        p.push(name);
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn cmd_build_pragmatics_ok_writes_hashes_and_frames() {
        let dir = tmp_dir("cmd_build_pragmatics_ok_writes_hashes_and_frames");
        let store = FsArtifactStore::new(&dir).unwrap();

        let ids = PromptIds {
            snapshot_id: [0u8; 32],
            weights_id: [0u8; 32],
            tokenizer_id: [0u8; 32],
        };

        let mut pack = PromptPack::new(0, 32, ids);
        pack.messages.push(Message {
            role: Role::System,
            content: "You are deterministic.".to_string(),
        });
        pack.messages.push(Message {
            role: Role::User,
            content: "What is 2+2?".to_string(),
        });
        pack.messages.push(Message {
            role: Role::User,
            content: "Please avoid unsafe code.".to_string(),
        });

        let limits = PromptLimits::default_v1();
        let prompt_hash = put_prompt_pack(&store, &mut pack, limits).unwrap();

        let out1 = dir.join("out1.txt");
        let out2 = dir.join("out2.txt");

        let args1 = vec![
            "--root".to_string(),
            dir.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--source-id".to_string(),
            "7".to_string(),
            "--tok-max-bytes".to_string(),
            "64".to_string(),
            "--out-file".to_string(),
            out1.to_string_lossy().to_string(),
        ];

        let rc = cmd_build_pragmatics(&args1);
        assert_eq!(rc, 0);

        let s = std::fs::read_to_string(&out1).unwrap();
        let lines: Vec<String> = s
            .lines()
            .map(|x| x.trim())
            .filter(|x| !x.is_empty())
            .map(|x| x.to_string())
            .collect();
        assert_eq!(lines.len(), 3);

        for (ix, hx) in lines.iter().enumerate() {
            let h = parse_hash32_hex(hx).unwrap();
            let f = get_pragmatics_frame_v1(&store, &h).unwrap().unwrap();
            assert_eq!(f.source_id, Id64(7));
            assert_eq!(f.msg_ix, ix as u32);
            assert!(f.validate().is_ok());
        }

        let args2 = vec![
            "--root".to_string(),
            dir.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--source-id".to_string(),
            "7".to_string(),
            "--tok-max-bytes".to_string(),
            "64".to_string(),
            "--out-file".to_string(),
            out2.to_string_lossy().to_string(),
        ];

        let rc2 = cmd_build_pragmatics(&args2);
        assert_eq!(rc2, 0);

        let s2 = std::fs::read_to_string(&out2).unwrap();
        assert_eq!(s, s2);
    }

    #[test]
    fn cmd_build_pragmatics_missing_prompt_returns_3() {
        let dir = tmp_dir("cmd_build_pragmatics_missing_prompt_returns_3");

        let out = dir.join("out.txt");
        let fake = [7u8; 32];

        let args = vec![
            "--root".to_string(),
            dir.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&fake),
            "--out-file".to_string(),
            out.to_string_lossy().to_string(),
        ];

        let rc = cmd_build_pragmatics(&args);
        assert_eq!(rc, 3);
    }
}

#[cfg(test)]
mod scale_demo_cli_tests {
    use super::*;

    fn tmp_dir(name: &str) -> std::path::PathBuf {
        use std::fs;
        let mut p = std::env::temp_dir();
        p.push("fsa_lm_tests");
        p.push(name);
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn cmd_scale_demo_ingest_and_build_index_writes_four_lines() {
        let dir = tmp_dir("cmd_scale_demo_ingest_and_build_index_writes_four_lines");
        let out = dir.join("out.txt");

        let args = vec![
            "--root".to_string(),
            dir.to_string_lossy().to_string(),
            "--docs".to_string(),
            "32".to_string(),
            "--queries".to_string(),
            "8".to_string(),
            "--min_doc_tokens".to_string(),
            "3".to_string(),
            "--max_doc_tokens".to_string(),
            "6".to_string(),
            "--vocab".to_string(),
            "32".to_string(),
            "--query_tokens".to_string(),
            "3".to_string(),
            "--tie_pair".to_string(),
            "1".to_string(),
            "--ingest".to_string(),
            "1".to_string(),
            "--build_index".to_string(),
            "1".to_string(),
            "--out-file".to_string(),
            out.to_string_lossy().to_string(),
        ];

        let rc = cmd_scale_demo(&args);
        assert_eq!(rc, 0);

        let s = std::fs::read_to_string(&out).unwrap();
        let lines: Vec<&str> = s.lines().filter(|l| !l.trim().is_empty()).collect();
        assert_eq!(lines.len(), 4);
        assert!(lines[0].starts_with("scale_demo_v1 "));
        assert!(lines[1].starts_with("scale_demo_frames_v1 "));
        assert!(lines[2].starts_with("scale_demo_index_v1 "));
        assert!(lines[3].starts_with("scale_demo_scale_report_v3 "));
    }
}



#[cfg(test)]
mod sharded_ingest_cli_tests {
    use super::*;

    use fsa_lm::frame::{derive_id64, DocId};
    use fsa_lm::index_snapshot_store::get_index_snapshot_v1;
    use fsa_lm::index_sig_map_store::get_index_sig_map_v1;
    use fsa_lm::index_query::{query_terms_from_text, search_snapshot_gated, QueryTermsCfg, SearchCfg};
    use fsa_lm::prompt_artifact::put_prompt_pack;
    use fsa_lm::prompt_pack::{Message, PromptIds, PromptLimits, PromptPack, Role};
    use fsa_lm::reduce_manifest_artifact::get_reduce_manifest_v1;
    use fsa_lm::shard_manifest_artifact::get_shard_manifest_v1;
    use fsa_lm::sharding_v1::shard_id_for_doc_id_hash32_v1;

    fn tmp_dir(name: &str) -> PathBuf {
        let base = std::env::temp_dir();
        let p = base.join(format!("fsa_lm_cli_shard_{}_{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn title_for_shard(target: u16, shard_count: u16) -> String {
        for i in 0..5000u32 {
            let t = format!("title_{}_{}", target, i);
            let doc_id = DocId(derive_id64(b"doc\0", t.as_bytes()));
            let sid = shard_id_for_doc_id_hash32_v1(doc_id, shard_count);
            if sid == target {
                return t;
            }
        }
        panic!("failed to find title for shard");
    }

    fn titles_for_shard(target: u16, shard_count: u16, n: usize) -> Vec<String> {
        let mut out: Vec<String> = Vec::with_capacity(n);
        for i in 0..200_000u32 {
            let t = format!("title_{}_{}", target, i);
            let doc_id = DocId(derive_id64(b"doc\0", t.as_bytes()));
            let sid = shard_id_for_doc_id_hash32_v1(doc_id, shard_count);
            if sid == target {
                out.push(t);
                if out.len() >= n {
                    return out;
                }
            }
        }
        panic!("failed to find enough titles for shard");
    }

    #[test]
    fn cmd_ingest_wiki_sharded_writes_shard_manifest_v1() {
        let root = tmp_dir("ingest_wiki_sharded");
        let dump = root.join("dump.tsv");
        let out = root.join("out.txt");

        let shard_count: u16 = 2;
        let t0 = title_for_shard(0, shard_count);
        let t1 = title_for_shard(1, shard_count);

        let mut data = String::new();
        data.push_str(&format!("{}	{}
", t0, "hello world"));
        data.push_str(&format!("{}	{}
", t1, "hello world"));
        std::fs::write(&dump, data.as_bytes()).unwrap();

        let args = vec![
            "--root".to_string(),
            root.to_string_lossy().to_string(),
            "--dump".to_string(),
            dump.to_string_lossy().to_string(),
            "--shards".to_string(),
            "2".to_string(),
            "--out-file".to_string(),
            out.to_string_lossy().to_string(),
        ];

        let rc = cmd_ingest_wiki_sharded(&args);
        assert_eq!(rc, 0);

        let out_s = std::fs::read_to_string(&out).unwrap();
        let hex = out_s.trim();
        let man_hash = parse_hash32_hex(hex).unwrap();

        let base_store = FsArtifactStore::new(&root).unwrap();
        let man = get_shard_manifest_v1(&base_store, &man_hash).unwrap().unwrap();
        assert_eq!(man.shard_count, shard_count);
        assert_eq!(man.mapping_id, SHARD_MAPPING_DOC_ID_HASH32_V1.to_string());
        assert_eq!(man.shards.len(), 2);
        assert_eq!(man.shards[0].shard_id, 0);
        assert_eq!(man.shards[1].shard_id, 1);

        for se in man.shards.iter() {
            assert_eq!(se.outputs.len(), 1);
            assert_eq!(se.outputs[0].tag, "wiki_ingest_manifest_v1".to_string());
            let shard_root = root.join(&se.shard_root_rel);
            let ss = FsArtifactStore::new(&shard_root).unwrap();
            let got = ss.get(&se.outputs[0].hash).unwrap();
            assert!(got.is_some());
        }
    }

    #[test]
    fn cmd_reduce_index_merges_shards_and_enables_global_query() {
        let root = tmp_dir("reduce_index");
        let dump = root.join("dump.tsv");
        let out1 = root.join("out_ingest.txt");
        let out2 = root.join("out_index.txt");
        let out3 = root.join("out_reduce.txt");

        let shard_count: u16 = 2;
        let t0 = title_for_shard(0, shard_count);
        let t1 = title_for_shard(1, shard_count);

        let mut data = String::new();
        data.push_str(&format!("{}\t{}\n", t0, "hello world"));
        data.push_str(&format!("{}\t{}\n", t1, "hello world"));
        std::fs::write(&dump, data.as_bytes()).unwrap();

        // Ingest shards and capture manifest.
        let args_ingest = vec![
            "--root".to_string(),
            root.to_string_lossy().to_string(),
            "--dump".to_string(),
            dump.to_string_lossy().to_string(),
            "--shards".to_string(),
            "2".to_string(),
            "--out-file".to_string(),
            out1.to_string_lossy().to_string(),
        ];
        let rc = cmd_ingest_wiki_sharded(&args_ingest);
        assert_eq!(rc, 0);

        let man0_hex = std::fs::read_to_string(&out1).unwrap().trim().to_string();

        // Build index across shards and update manifest.
        let args_index = vec![
            "--root".to_string(),
            root.to_string_lossy().to_string(),
            "--shards".to_string(),
            "2".to_string(),
            "--manifest".to_string(),
            man0_hex,
            "--out-file".to_string(),
            out2.to_string_lossy().to_string(),
        ];
        let rc = cmd_build_index_sharded(&args_index);
        assert_eq!(rc, 0);

        let man1_hex = std::fs::read_to_string(&out2).unwrap().trim().to_string();

        // Reduce shards into a merged view in the primary root.
        let args_reduce = vec![
            "--root".to_string(),
            root.to_string_lossy().to_string(),
            "--manifest".to_string(),
            man1_hex,
            "--out-file".to_string(),
            out3.to_string_lossy().to_string(),
        ];
        let rc = cmd_reduce_index(&args_reduce);
        assert_eq!(rc, 0);

        let out_s = std::fs::read_to_string(&out3).unwrap();
        let mut it = out_s.lines();
        let reduce_hex = it.next().unwrap();
        let merged_snapshot_hex = it.next().unwrap();
        let merged_sig_hex = it.next().unwrap();

        let reduce_hash = parse_hash32_hex(reduce_hex).unwrap();
        let merged_snapshot = parse_hash32_hex(merged_snapshot_hex).unwrap();
        let merged_sig_map = parse_hash32_hex(merged_sig_hex).unwrap();

        // Verify ReduceManifestV1 references match the printed merged ids.
        let base_store = FsArtifactStore::new(&root).unwrap();
        let rm = get_reduce_manifest_v1(&base_store, &reduce_hash).unwrap().unwrap();
        let mut got_snap: Option<Hash32> = None;
        let mut got_sig: Option<Hash32> = None;
        for o in rm.outputs.iter() {
            if o.tag == "index_snapshot_v1" {
                got_snap = Some(o.hash);
            }
            if o.tag == "index_sig_map_v1" {
                got_sig = Some(o.hash);
            }
        }
        assert_eq!(got_snap.unwrap(), merged_snapshot);
        assert_eq!(got_sig.unwrap(), merged_sig_map);

        // Global query (primary root) should work using merged ids.
        let args_query = vec![
            "--root".to_string(),
            root.to_string_lossy().to_string(),
            "--snapshot".to_string(),
            hex32(&merged_snapshot),
            "--sig-map".to_string(),
            hex32(&merged_sig_map),
            "--text".to_string(),
            "hello world".to_string(),
            "--k".to_string(),
            "4".to_string(),
        ];
        let rc = cmd_query_index(&args_query);
        assert_eq!(rc, 0);
    }

    // Regression lock for: a larger deterministic E2E that exercises
    // sharded ingest, sharded index build, deterministic reduce, and a gated
    // search against the merged snapshot.
    #[test]
    fn cmd_scale_regression_lock_e2e() {
        let root = tmp_dir("scale_regression_lock");
        let dump = root.join("dump.tsv");
        let out1 = root.join("out_ingest.txt");
        let out2 = root.join("out_index.txt");
        let out3 = root.join("out_reduce.txt");

        // Use knobs that force multiple segments per shard via seg_rows.
        // seg_mb=1, row_kb=64 => seg_rows=16 (1MB / 64KB).
        // docs_per_shard=64 => typically 4 segments/shard.
        let shard_count: u16 = 8;
        let docs_per_shard: usize = 64;

        let mut data = String::new();
        for sid in 0..shard_count {
            let titles = titles_for_shard(sid, shard_count, docs_per_shard);
            for (j, t) in titles.iter().enumerate() {
                // Keep tokens simple and deterministic.
                // All rows include "alpha" so we can assert non-empty search hits.
                let body = format!(
                    "alpha beta gamma shard{} doc{} x{}",
                    sid,
                    j,
                    (j % 13)
                );
                data.push_str(&format!("{}\t{}\n", t, body));
            }
        }
        std::fs::write(&dump, data.as_bytes()).unwrap();

        // Ingest shards.
        let args_ingest = vec![
            "--root".to_string(),
            root.to_string_lossy().to_string(),
            "--dump".to_string(),
            dump.to_string_lossy().to_string(),
            "--shards".to_string(),
            shard_count.to_string(),
            "--seg_mb".to_string(),
            "1".to_string(),
            "--row_kb".to_string(),
            "64".to_string(),
            "--chunk_rows".to_string(),
            "64".to_string(),
            "--max_docs".to_string(),
            "2048".to_string(),
            "--out-file".to_string(),
            out1.to_string_lossy().to_string(),
        ];
        let rc = cmd_ingest_wiki_sharded(&args_ingest);
        assert_eq!(rc, 0);
        let man0_hex = std::fs::read_to_string(&out1).unwrap().trim().to_string();

        // Build index across shards.
        let args_index = vec![
            "--root".to_string(),
            root.to_string_lossy().to_string(),
            "--shards".to_string(),
            shard_count.to_string(),
            "--manifest".to_string(),
            man0_hex,
            "--out-file".to_string(),
            out2.to_string_lossy().to_string(),
        ];
        let rc = cmd_build_index_sharded(&args_index);
        assert_eq!(rc, 0);
        let man1_hex = std::fs::read_to_string(&out2).unwrap().trim().to_string();

        // Reduce shards into a merged view in the primary root.
        let args_reduce = vec![
            "--root".to_string(),
            root.to_string_lossy().to_string(),
            "--manifest".to_string(),
            man1_hex,
            "--out-file".to_string(),
            out3.to_string_lossy().to_string(),
        ];
        let rc = cmd_reduce_index(&args_reduce);
        assert_eq!(rc, 0);

        let out_s = std::fs::read_to_string(&out3).unwrap();
        let mut it = out_s.lines();
        let _reduce_hex = it.next().unwrap();
        let merged_snapshot_hex = it.next().unwrap();
        let merged_sig_hex = it.next().unwrap();
        let merged_snapshot = parse_hash32_hex(merged_snapshot_hex).unwrap();
        let merged_sig_map = parse_hash32_hex(merged_sig_hex).unwrap();

        // Load merged artifacts and sanity-check counts.
        let base_store = FsArtifactStore::new(&root).unwrap();
        let snap = get_index_snapshot_v1(&base_store, &merged_snapshot).unwrap().unwrap();
        let sig_map = get_index_sig_map_v1(&base_store, &merged_sig_map).unwrap().unwrap();

        // With seg_rows=16 and docs_per_shard=64, we should usually have >= 24 entries.
        // Keep the bound conservative to avoid brittleness.
        assert!(snap.entries.len() >= (shard_count as usize));
        assert_eq!(sig_map.entries.len(), snap.entries.len());

        // Run a gated search and assert we get hits.
        let qcfg = QueryTermsCfg::new();
        let qterms = query_terms_from_text("alpha beta", &qcfg);
        assert!(!qterms.is_empty());
        let scfg = SearchCfg { k: 20, entry_cap: 0, dense_row_threshold: 200_000 };
        let (hits, _gate_stats) = search_snapshot_gated(
            &base_store,
            &merged_snapshot,
            &merged_sig_map,
            &qterms,
            &scfg,
        )
        .unwrap();
        assert!(!hits.is_empty());
    }


    fn find_output(outputs: &[ShardOutputV1], tag: &str) -> Option<Hash32> {
        for o in outputs.iter() {
            if o.tag == tag {
                return Some(o.hash);
            }
        }
        None
    }

    fn ingest_and_build_index(root: &PathBuf, dump: &PathBuf, shard_count: u16) -> String {
        let out1 = root.join("out_ingest.txt");
        let out2 = root.join("out_index.txt");

        let args_ingest = vec![
            "--root".to_string(),
            root.to_string_lossy().to_string(),
            "--dump".to_string(),
            dump.to_string_lossy().to_string(),
            "--shards".to_string(),
            shard_count.to_string(),
            "--out-file".to_string(),
            out1.to_string_lossy().to_string(),
        ];
        let rc = cmd_ingest_wiki_sharded(&args_ingest);
        assert_eq!(rc, 0);

        let man0_hex = std::fs::read_to_string(&out1).unwrap().trim().to_string();

        let args_index = vec![
            "--root".to_string(),
            root.to_string_lossy().to_string(),
            "--shards".to_string(),
            shard_count.to_string(),
            "--manifest".to_string(),
            man0_hex,
            "--out-file".to_string(),
            out2.to_string_lossy().to_string(),
        ];
        let rc = cmd_build_index_sharded(&args_index);
        assert_eq!(rc, 0);

        std::fs::read_to_string(&out2).unwrap().trim().to_string()
    }

    fn reduce_once(root: &PathBuf, manifest_hex: &str, out_name: &str) -> (Hash32, Hash32, Hash32) {
        let out = root.join(out_name);

        let args_reduce = vec![
            "--root".to_string(),
            root.to_string_lossy().to_string(),
            "--manifest".to_string(),
            manifest_hex.to_string(),
            "--out-file".to_string(),
            out.to_string_lossy().to_string(),
        ];
        let rc = cmd_reduce_index(&args_reduce);
        assert_eq!(rc, 0);

        let out_s = std::fs::read_to_string(&out).unwrap();
        let mut it = out_s.lines();
        let reduce_hex = it.next().unwrap();
        let merged_snapshot_hex = it.next().unwrap();
        let merged_sig_hex = it.next().unwrap();

        (
            parse_hash32_hex(reduce_hex).unwrap(),
            parse_hash32_hex(merged_snapshot_hex).unwrap(),
            parse_hash32_hex(merged_sig_hex).unwrap(),
        )
    }

    #[test]
    fn cmd_reduce_index_is_deterministic_across_repeated_runs() {
        let root = tmp_dir("reduce_deterministic");
        let dump = root.join("dump.tsv");

        let shard_count: u16 = 2;
        let t0 = title_for_shard(0, shard_count);
        let t1 = title_for_shard(1, shard_count);

        let mut data = String::new();
        data.push_str(&format!("{}\t{}\n", t0, "hello world"));
        data.push_str(&format!("{}\t{}\n", t1, "hello world"));
        std::fs::write(&dump, data.as_bytes()).unwrap();

        let man1_hex = ingest_and_build_index(&root, &dump, shard_count);

        let (r1, s1, g1) = reduce_once(&root, &man1_hex, "out_reduce_a.txt");
        let (r2, s2, g2) = reduce_once(&root, &man1_hex, "out_reduce_b.txt");

        assert_eq!(r1, r2);
        assert_eq!(s1, s2);
        assert_eq!(g1, g2);
    }

    #[test]
    fn cmd_reduce_index_allows_empty_shard() {
        let root = tmp_dir("reduce_empty_shard");
        let dump = root.join("dump.tsv");

        let shard_count: u16 = 3;
        let t0 = title_for_shard(0, shard_count);
        let t1 = title_for_shard(1, shard_count);
        // shard 2 intentionally left empty

        let mut data = String::new();
        data.push_str(&format!("{}\t{}\n", t0, "hello world"));
        data.push_str(&format!("{}\t{}\n", t1, "hello world"));
        std::fs::write(&dump, data.as_bytes()).unwrap();

        let man1_hex = ingest_and_build_index(&root, &dump, shard_count);
        let (_r, merged_snapshot, merged_sig_map) = reduce_once(&root, &man1_hex, "out_reduce.txt");

        let args_query = vec![
            "--root".to_string(),
            root.to_string_lossy().to_string(),
            "--snapshot".to_string(),
            hex32(&merged_snapshot),
            "--sig-map".to_string(),
            hex32(&merged_sig_map),
            "--text".to_string(),
            "hello world".to_string(),
            "--k".to_string(),
            "4".to_string(),
        ];
        let rc = cmd_query_index(&args_query);
        assert_eq!(rc, 0);
    }

    #[test]
    fn cmd_reduce_index_allows_many_empty_shards() {
        let root = tmp_dir("reduce_many_empty_shards");
        let dump = root.join("dump.tsv");

        let shard_count: u16 = 8;
        let t0 = title_for_shard(0, shard_count);
        let t7 = title_for_shard(7, shard_count);
        // shards 1..6 intentionally left empty

        let mut data = String::new();
        data.push_str(&format!("{}\t{}\n", t0, "hello world"));
        data.push_str(&format!("{}\t{}\n", t7, "hello world"));
        std::fs::write(&dump, data.as_bytes()).unwrap();

        let man1_hex = ingest_and_build_index(&root, &dump, shard_count);
        let (_r, merged_snapshot, merged_sig_map) = reduce_once(&root, &man1_hex, "out_reduce.txt");

        let args_query = vec![
            "--root".to_string(),
            root.to_string_lossy().to_string(),
            "--snapshot".to_string(),
            hex32(&merged_snapshot),
            "--sig-map".to_string(),
            hex32(&merged_sig_map),
            "--text".to_string(),
            "hello world".to_string(),
            "--k".to_string(),
            "4".to_string(),
        ];
        let rc = cmd_query_index(&args_query);
        assert_eq!(rc, 0);
    }


    #[test]
    fn cmd_reduce_index_ok_when_primary_root_already_has_some_artifacts() {
        let root = tmp_dir("reduce_preexisting");
        let dump = root.join("dump.tsv");

        let shard_count: u16 = 2;
        let t0 = title_for_shard(0, shard_count);
        let t1 = title_for_shard(1, shard_count);

        let mut data = String::new();
        data.push_str(&format!("{}\t{}\n", t0, "hello world"));
        data.push_str(&format!("{}\t{}\n", t1, "hello world"));
        std::fs::write(&dump, data.as_bytes()).unwrap();

        let man1_hex = ingest_and_build_index(&root, &dump, shard_count);
        let man1_hash = parse_hash32_hex(&man1_hex).unwrap();

        // Pre-copy one frame segment into the primary root before reduce.
        let base_store = FsArtifactStore::new(&root).unwrap();
        let man = get_shard_manifest_v1(&base_store, &man1_hash).unwrap().unwrap();
        let se0 = &man.shards[0];
        let shard0_root = root.join(&se0.shard_root_rel);
        let shard0_store = FsArtifactStore::new(&shard0_root).unwrap();

        let snap0 = find_output(&se0.outputs, "index_snapshot_v1").unwrap();
        let snap = get_index_snapshot_v1(&shard0_store, &snap0).unwrap().unwrap();
        let frame_seg = snap.entries[0].frame_seg;

        let bytes = shard0_store.get(&frame_seg).unwrap().unwrap();
        let hh = base_store.put(&bytes).unwrap();
        assert_eq!(hh, frame_seg);

        let (_r, merged_snapshot, merged_sig_map) = reduce_once(&root, &man1_hex, "out_reduce.txt");

        let args_query = vec![
            "--root".to_string(),
            root.to_string_lossy().to_string(),
            "--snapshot".to_string(),
            hex32(&merged_snapshot),
            "--sig-map".to_string(),
            hex32(&merged_sig_map),
            "--text".to_string(),
            "hello world".to_string(),
            "--k".to_string(),
            "4".to_string(),
        ];
        let rc = cmd_query_index(&args_query);
        assert_eq!(rc, 0);
    }

    #[test]
    fn cmd_reduce_index_enables_build_evidence_and_answer_on_primary_root() {
        let root = tmp_dir("reduce_evidence_answer");
        let dump = root.join("dump.tsv");

        let shard_count: u16 = 2;
        let t0 = title_for_shard(0, shard_count);
        let t1 = title_for_shard(1, shard_count);

        let mut data = String::new();
        data.push_str(&format!("{}\t{}\n", t0, "hello world"));
        data.push_str(&format!("{}\t{}\n", t1, "hello world"));
        std::fs::write(&dump, data.as_bytes()).unwrap();

        let man1_hex = ingest_and_build_index(&root, &dump, shard_count);
        let (_r, merged_snapshot, merged_sig_map) = reduce_once(&root, &man1_hex, "out_reduce.txt");

        // Build evidence on merged ids.
        let args_ev = vec![
            "--root".to_string(),
            root.to_string_lossy().to_string(),
            "--snapshot".to_string(),
            hex32(&merged_snapshot),
            "--sig-map".to_string(),
            hex32(&merged_sig_map),
            "--text".to_string(),
            "hello world".to_string(),
            "--k".to_string(),
            "4".to_string(),
        ];
        let rc = cmd_build_evidence(&args_ev);
        assert_eq!(rc, 0);

        // Create a minimal PromptPack and run answer on merged ids.
        let store = FsArtifactStore::new(&root).unwrap();
        let zero: Hash32 = [0u8; 32];
        let ids = PromptIds { snapshot_id: zero, weights_id: zero, tokenizer_id: zero };
        let mut pack = PromptPack::new(1, 256, ids);
        pack.messages.push(Message { role: Role::User, content: "hello world".to_string() });
        let prompt_hash = put_prompt_pack(&store, &mut pack, PromptLimits::default_v1()).unwrap();

        let out_ans = root.join("answer.txt");
        let args_ans = vec![
            "--root".to_string(),
            root.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--snapshot".to_string(),
            hex32(&merged_snapshot),
            "--sig-map".to_string(),
            hex32(&merged_sig_map),
            "--k".to_string(),
            "4".to_string(),
            "--out-file".to_string(),
            out_ans.to_string_lossy().to_string(),
        ];
        let rc = cmd_answer(&args_ans);
        assert_eq!(rc, 0);

        let ans_s = std::fs::read_to_string(&out_ans).unwrap();
        assert!(!ans_s.trim().is_empty());
    }


    #[test]
    fn cmd_run_operator_workflow_pipeline_end_to_end_small() {
        let root = tmp_dir("run_small_operator_workflow");
        let dump = root.join("dump.tsv");
        let out = root.join("out.txt");

        let shard_count: u16 = 2;
        let t0 = title_for_shard(0, shard_count);
        let t1 = title_for_shard(1, shard_count);

        let mut data = String::new();
        data.push_str(&format!("{}\t{}\n", t0, "alpha beta"));
        data.push_str(&format!("{}\t{}\n", t1, "alpha beta"));
        std::fs::write(&dump, data.as_bytes()).unwrap();

        let args = vec![
            "--root".to_string(),
            root.to_string_lossy().to_string(),
            "--dump".to_string(),
            dump.to_string_lossy().to_string(),
            "--shards".to_string(),
            "2".to_string(),
            "--out-file".to_string(),
            out.to_string_lossy().to_string(),
        ];

        let rc = cmd_run_workflow(&args);
        assert_eq!(rc, 0);

        let out_s = std::fs::read_to_string(&out).unwrap();
        let mut ingest_h: Option<Hash32> = None;
        let mut index_h: Option<Hash32> = None;
        let mut reduce_h: Option<Hash32> = None;
        let mut snap_h: Option<Hash32> = None;
        let mut sig_h: Option<Hash32> = None;

        for line in out_s.lines() {
            let mut it = line.splitn(2, '=');
            let k = it.next().unwrap_or("");
            let v = it.next().unwrap_or("");
            if v.is_empty() {
                continue;
            }
            let h = parse_hash32_hex(v).unwrap();
            match k {
                "shard_manifest_ingest" => ingest_h = Some(h),
                "shard_manifest_index" => index_h = Some(h),
                "reduce_manifest" => reduce_h = Some(h),
                "merged_snapshot" => snap_h = Some(h),
                "merged_sig_map" => sig_h = Some(h),
                _ => {}
            }
        }

        assert!(ingest_h.is_some());
        assert!(index_h.is_some());
        assert!(reduce_h.is_some());
        assert!(snap_h.is_some());
        assert!(sig_h.is_some());

        let store = FsArtifactStore::new(&root).unwrap();
        assert!(store.get(&ingest_h.unwrap()).unwrap().is_some());
        assert!(store.get(&index_h.unwrap()).unwrap().is_some());
        assert!(store.get(&reduce_h.unwrap()).unwrap().is_some());
        assert!(store.get(&snap_h.unwrap()).unwrap().is_some());
        assert!(store.get(&sig_h.unwrap()).unwrap().is_some());
    }

}


#[cfg(test)]
mod sharded_index_cli_tests {
    use super::*;

    use fsa_lm::frame::{derive_id64, DocId};
    use fsa_lm::shard_manifest_artifact::get_shard_manifest_v1;
    use fsa_lm::sharding_v1::shard_id_for_doc_id_hash32_v1;

    fn tmp_dir(name: &str) -> PathBuf {
        let base = std::env::temp_dir();
        let p = base.join(format!("fsa_lm_cli_shard_index_{}_{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn title_for_shard(target: u16, shard_count: u16) -> String {
        for i in 0..5000u32 {
            let t = format!("title_{}_{}", target, i);
            let doc_id = DocId(derive_id64(b"doc\0", t.as_bytes()));
            let sid = shard_id_for_doc_id_hash32_v1(doc_id, shard_count);
            if sid == target {
                return t;
            }
        }
        panic!("failed to find title for shard");
    }

    fn find_output(outputs: &[ShardOutputV1], tag: &str) -> Option<Hash32> {
        for o in outputs.iter() {
            if o.tag == tag {
                return Some(o.hash);
            }
        }
        None
    }

    #[test]
    fn cmd_build_index_sharded_updates_manifest_with_index_outputs() {
        let root = tmp_dir("build_index_sharded");
        let dump = root.join("dump.tsv");
        let out1 = root.join("out_ingest.txt");
        let out2 = root.join("out_index.txt");

        let shard_count: u16 = 2;
        let t0 = title_for_shard(0, shard_count);
        let t1 = title_for_shard(1, shard_count);

        let mut data = String::new();
        data.push_str(&format!("{}\t{}\n", t0, "hello world"));
        data.push_str(&format!("{}\t{}\n", t1, "hello world"));
        std::fs::write(&dump, data.as_bytes()).unwrap();

        // Ingest shards and capture manifest.
        let args_ingest = vec![
            "--root".to_string(),
            root.to_string_lossy().to_string(),
            "--dump".to_string(),
            dump.to_string_lossy().to_string(),
            "--shards".to_string(),
            "2".to_string(),
            "--out-file".to_string(),
            out1.to_string_lossy().to_string(),
        ];
        let rc = cmd_ingest_wiki_sharded(&args_ingest);
        assert_eq!(rc, 0);

        let out_s = std::fs::read_to_string(&out1).unwrap();
        let man0_hex = out_s.trim().to_string();
        let _man0_hash = parse_hash32_hex(&man0_hex).unwrap();

        // Build index across shards and update manifest.
        let args_index = vec![
            "--root".to_string(),
            root.to_string_lossy().to_string(),
            "--shards".to_string(),
            "2".to_string(),
            "--manifest".to_string(),
            man0_hex,
            "--out-file".to_string(),
            out2.to_string_lossy().to_string(),
        ];
        let rc = cmd_build_index_sharded(&args_index);
        assert_eq!(rc, 0);

        let out2_s = std::fs::read_to_string(&out2).unwrap();
        let man1_hex = out2_s.trim();
        let man1_hash = parse_hash32_hex(man1_hex).unwrap();

        let base_store = FsArtifactStore::new(&root).unwrap();
        let man1 = get_shard_manifest_v1(&base_store, &man1_hash).unwrap().unwrap();
        assert_eq!(man1.shard_count, shard_count);
        assert_eq!(man1.shards.len(), 2);

        // Verify each shard has wiki ingest + index outputs and that artifacts exist.
        for se in man1.shards.iter() {
            let shard_root = root.join(&se.shard_root_rel);
            let ss = FsArtifactStore::new(&shard_root).unwrap();

            let wiki_h = find_output(&se.outputs, "wiki_ingest_manifest_v1").unwrap();
            assert!(ss.get(&wiki_h).unwrap().is_some());

            let snap_h = find_output(&se.outputs, "index_snapshot_v1").unwrap();
            assert!(ss.get(&snap_h).unwrap().is_some());

            let sig_h = find_output(&se.outputs, "index_sig_map_v1").unwrap();
            assert!(ss.get(&sig_h).unwrap().is_some());
        }

        // Basic retrieval compatibility test on shard 0.
        let se0 = &man1.shards[0];
        let shard0_root = root.join(&se0.shard_root_rel);
        let snap0 = find_output(&se0.outputs, "index_snapshot_v1").unwrap();
        let sig0 = find_output(&se0.outputs, "index_sig_map_v1").unwrap();

        let args_query = vec![
            "--root".to_string(),
            shard0_root.to_string_lossy().to_string(),
            "--snapshot".to_string(),
            hex32(&snap0),
            "--sig-map".to_string(),
            hex32(&sig0),
            "--text".to_string(),
            "hello world".to_string(),
            "--k".to_string(),
            "2".to_string(),
        ];
        let rc = cmd_query_index(&args_query);
        assert_eq!(rc, 0);
    }
}


#[cfg(test)]
mod markov_model_build_cli_tests {
    use super::*;

    use fsa_lm::markov_hints::MarkovChoiceKindV1;
    use fsa_lm::markov_model_artifact::get_markov_model_v1;
    use fsa_lm::markov_trace::{MarkovTraceV1, MARKOV_TRACE_V1_VERSION};
    use fsa_lm::replay::{ReplayLog, ReplayStep};

    fn tmp_dir(name: &str) -> PathBuf {
        let base = std::env::temp_dir();
        let p = base.join(format!("fsa_lm_cli_markov_{}_{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn cmd_build_markov_model_from_replay_ok() {
        let root = tmp_dir("build_model");
        let store = FsArtifactStore::new(&root).unwrap();

        let trace = MarkovTraceV1 {
            version: MARKOV_TRACE_V1_VERSION,
            query_id: [7u8; 32],
            tokens: vec![
                MarkovTokenV1::new(MarkovChoiceKindV1::Opener, Id64(1)),
                MarkovTokenV1::new(MarkovChoiceKindV1::Transition, Id64(2)),
                MarkovTokenV1::new(MarkovChoiceKindV1::Closer, Id64(3)),
            ],
        };

        let trace_hash = put_markov_trace_v1(&store, &trace).unwrap();

        let mut log = ReplayLog::new();
        log.steps.push(ReplayStep {
            name: STEP_MARKOV_TRACE_V1.to_string(),
            inputs: Vec::new(),
            outputs: vec![trace_hash],
        });
        let replay_hash = put_replay_log(&store, &log).unwrap();

        let out = root.join("out.txt");

        let args = vec![
            "--root".to_string(),
            root.to_string_lossy().to_string(),
            "--replay".to_string(),
            hex32(&replay_hash),
            "--order".to_string(),
            "2".to_string(),
            "--max-next".to_string(),
            "4".to_string(),
            "--max-states".to_string(),
            "128".to_string(),
            "--out-file".to_string(),
            out.to_string_lossy().to_string(),
        ];

        let rc = cmd_build_markov_model(&args);
        assert_eq!(rc, 0);

        let s = std::fs::read_to_string(&out).unwrap();
        assert!(s.starts_with("markov_model_v1 "));

        let mut model_hash: Option<Hash32> = None;
        for part in s.split_whitespace() {
            if let Some(v) = part.strip_prefix("model_hash=") {
                model_hash = Some(parse_hash32_hex(v).unwrap());
            }
        }
        let mh = model_hash.expect("missing model_hash");

        let loaded = get_markov_model_v1(&store, &mh).unwrap();
        assert!(loaded.is_some());
    }

    #[test]
    fn cmd_inspect_markov_model_ok() {
        let root = tmp_dir("inspect_model");
        let store = FsArtifactStore::new(&root).unwrap();

        let trace = MarkovTraceV1 {
            version: MARKOV_TRACE_V1_VERSION,
            query_id: [7u8; 32],
            tokens: vec![
                MarkovTokenV1::new(MarkovChoiceKindV1::Opener, Id64(1)),
                MarkovTokenV1::new(MarkovChoiceKindV1::Transition, Id64(2)),
                MarkovTokenV1::new(MarkovChoiceKindV1::Closer, Id64(3)),
            ],
        };

        let trace_hash = put_markov_trace_v1(&store, &trace).unwrap();

        let mut log = ReplayLog::new();
        log.steps.push(ReplayStep {
            name: STEP_MARKOV_TRACE_V1.to_string(),
            inputs: Vec::new(),
            outputs: vec![trace_hash],
        });
        let replay_hash = put_replay_log(&store, &log).unwrap();

        let build_out = root.join("build.txt");
        let build_args = vec![
            "--root".to_string(),
            root.to_string_lossy().to_string(),
            "--replay".to_string(),
            hex32(&replay_hash),
            "--order".to_string(),
            "2".to_string(),
            "--max-next".to_string(),
            "4".to_string(),
            "--max-states".to_string(),
            "128".to_string(),
            "--out-file".to_string(),
            build_out.to_string_lossy().to_string(),
        ];
        let rc = cmd_build_markov_model(&build_args);
        assert_eq!(rc, 0);

        let s = std::fs::read_to_string(&build_out).unwrap();
        let mut model_hash: Option<Hash32> = None;
        for part in s.split_whitespace() {
            if let Some(v) = part.strip_prefix("model_hash=") {
                model_hash = Some(parse_hash32_hex(v).unwrap());
            }
        }
        let mh = model_hash.expect("missing model_hash");

        let inspect_out = root.join("inspect.txt");
        let inspect_args = vec![
            "--root".to_string(),
            root.to_string_lossy().to_string(),
            "--model".to_string(),
            hex32(&mh),
            "--top-states".to_string(),
            "1".to_string(),
            "--top-next".to_string(),
            "1".to_string(),
            "--out-file".to_string(),
            inspect_out.to_string_lossy().to_string(),
        ];
        let rc = cmd_inspect_markov_model(&inspect_args);
        assert_eq!(rc, 0);

        let o = std::fs::read_to_string(&inspect_out).unwrap();
        assert!(o.starts_with("markov_model_inspect_v1 "));
        assert!(o.contains(&format!("model_hash={} ", hex32(&mh))));
        assert!(o.contains("states="));
        assert!(o.contains("total_transitions="));
        assert!(o.lines().count() >= 1);
    }



    #[test]
    fn cmd_build_markov_model_truncation_is_deterministic() {
        use fsa_lm::scale_report::hash_hash32_list_v1;

        let root = tmp_dir("build_model_trunc");
        let store = FsArtifactStore::new(&root).unwrap();

        let trace_a = MarkovTraceV1 {
            version: MARKOV_TRACE_V1_VERSION,
            query_id: [1u8; 32],
            tokens: vec![
                MarkovTokenV1::new(MarkovChoiceKindV1::Opener, Id64(10)),
                MarkovTokenV1::new(MarkovChoiceKindV1::Transition, Id64(11)),
            ],
        };
        let trace_b = MarkovTraceV1 {
            version: MARKOV_TRACE_V1_VERSION,
            query_id: [2u8; 32],
            tokens: vec![
                MarkovTokenV1::new(MarkovChoiceKindV1::Opener, Id64(20)),
                MarkovTokenV1::new(MarkovChoiceKindV1::Transition, Id64(21)),
            ],
        };

        let trace_hash_a = put_markov_trace_v1(&store, &trace_a).unwrap();
        let trace_hash_b = put_markov_trace_v1(&store, &trace_b).unwrap();

        let mut log_a = ReplayLog::new();
        log_a.steps.push(ReplayStep {
            name: STEP_MARKOV_TRACE_V1.to_string(),
            inputs: Vec::new(),
            outputs: vec![trace_hash_a],
        });
        let replay_hash_a = put_replay_log(&store, &log_a).unwrap();

        let mut log_b = ReplayLog::new();
        log_b.steps.push(ReplayStep {
            name: STEP_MARKOV_TRACE_V1.to_string(),
            inputs: Vec::new(),
            outputs: vec![trace_hash_b],
        });
        let replay_hash_b = put_replay_log(&store, &log_b).unwrap();

        let mut replays = vec![replay_hash_a, replay_hash_b];
        replays.sort();

        let expected_replays = vec![replays[0]];
        let expected_traces = if replays[0] == replay_hash_a {
            vec![trace_hash_a]
        } else {
            vec![trace_hash_b]
        };

        let expected_replay_list_hash = hash_hash32_list_v1("markov_replays_v1", &expected_replays);
        let expected_trace_list_hash = hash_hash32_list_v1("markov_traces_v1", &expected_traces);

        let out = root.join("out_trunc.txt");

        // Pass the replays in reverse order to ensure sorting + truncation is deterministic.
        let args = vec![
            "--root".to_string(),
            root.to_string_lossy().to_string(),
            "--replay".to_string(),
            hex32(&replays[1]),
            "--replay".to_string(),
            hex32(&replays[0]),
            "--max-replays".to_string(),
            "1".to_string(),
            "--max-traces".to_string(),
            "1".to_string(),
            "--order".to_string(),
            "2".to_string(),
            "--max-next".to_string(),
            "4".to_string(),
            "--max-states".to_string(),
            "128".to_string(),
            "--out-file".to_string(),
            out.to_string_lossy().to_string(),
        ];

        let rc = cmd_build_markov_model(&args);
        assert_eq!(rc, 0);

        let s = std::fs::read_to_string(&out).unwrap();
        assert!(s.starts_with("markov_model_v1 "));

        let mut replay_list_hash: Option<Hash32> = None;
        let mut trace_list_hash: Option<Hash32> = None;
        let mut replays_count: Option<u32> = None;
        let mut traces_count: Option<u32> = None;

        for part in s.split_whitespace() {
            if let Some(v) = part.strip_prefix("replay_list_hash=") {
                replay_list_hash = Some(parse_hash32_hex(v).unwrap());
            }
            if let Some(v) = part.strip_prefix("trace_list_hash=") {
                trace_list_hash = Some(parse_hash32_hex(v).unwrap());
            }
            if let Some(v) = part.strip_prefix("replays=") {
                replays_count = Some(v.parse::<u32>().unwrap());
            }
            if let Some(v) = part.strip_prefix("traces=") {
                traces_count = Some(v.parse::<u32>().unwrap());
            }
        }

        assert_eq!(replays_count, Some(1));
        assert_eq!(traces_count, Some(1));
        assert_eq!(replay_list_hash, Some(expected_replay_list_hash));
        assert_eq!(trace_list_hash, Some(expected_trace_list_hash));
    }
}

#[cfg(test)]
mod exemplar_build_cli_tests {
    use super::*;
    use fsa_lm::exemplar_memory::{
        EXMEM_FLAG_HAS_MARKOV_TRACE, EXMEM_FLAG_HAS_PROMPT_PACK, EXMEM_FLAG_HAS_REPLAY_LOG,
    };
    use fsa_lm::exemplar_memory_artifact::get_exemplar_memory_v1;
    use fsa_lm::markov_hints::MarkovChoiceKindV1;

    fn tmp_dir(name: &str) -> PathBuf {
        let base = std::env::temp_dir();
        let p = base.join(format!("fsa_lm_cli_exemplar_{}_{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn parse_exemplar_hash(text: &str) -> Hash32 {
        for part in text.split_whitespace() {
            if let Some(v) = part.strip_prefix("exemplar_hash=") {
                return parse_hash32_hex(v).unwrap();
            }
        }
        panic!("missing exemplar_hash");
    }

    #[test]
    fn cmd_build_exemplar_memory_from_prompt_and_trace_ok() {
        let root = tmp_dir("build_prompt_trace");
        let store = FsArtifactStore::new(&root).unwrap();

        let mut pack = PromptPack {
            version: fsa_lm::prompt_pack::PROMPT_PACK_VERSION,
            seed: 7,
            max_output_tokens: 64,
            ids: PromptIds {
                snapshot_id: [0u8; 32],
                weights_id: [0u8; 32],
                tokenizer_id: [0u8; 32],
            },
            messages: vec![Message {
                role: Role::User,
                content: "Summarize the outage briefly.".to_string(),
            }],
            constraints: Vec::new(),
        };
        let prompt_hash = put_prompt_pack(&store, &mut pack, PromptLimits::default_v1()).unwrap();

        let trace = MarkovTraceV1 {
            version: MARKOV_TRACE_V1_VERSION,
            query_id: [9u8; 32],
            tokens: vec![
                MarkovTokenV1::new(MarkovChoiceKindV1::Opener, Id64(1)),
                MarkovTokenV1::new(MarkovChoiceKindV1::Other, derive_id64(b"markov_choice_v1", b"other:clarifier_intro:0")),
            ],
        };
        let trace_hash = put_markov_trace_v1(&store, &trace).unwrap();

        let out = root.join("exemplar.txt");
        let args = vec![
            "--root".to_string(),
            root.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--markov-trace".to_string(),
            hex32(&trace_hash),
            "--out-file".to_string(),
            out.to_string_lossy().to_string(),
        ];

        let rc = cmd_build_exemplar_memory(&args);
        assert_eq!(rc, 0);

        let text = std::fs::read_to_string(&out).unwrap();
        assert!(text.starts_with("exemplar_memory_v1 "));
        let exemplar_hash = parse_exemplar_hash(&text);
        let memory = get_exemplar_memory_v1(&store, &exemplar_hash).unwrap().unwrap();
        assert!(!memory.rows.is_empty());
        assert_eq!(memory.flags & EXMEM_FLAG_HAS_PROMPT_PACK, EXMEM_FLAG_HAS_PROMPT_PACK);
        assert_eq!(memory.flags & EXMEM_FLAG_HAS_MARKOV_TRACE, EXMEM_FLAG_HAS_MARKOV_TRACE);
    }

    #[test]
    fn cmd_build_exemplar_memory_replay_only_stores_empty_artifact() {
        let root = tmp_dir("build_replay_only");
        let store = FsArtifactStore::new(&root).unwrap();

        let replay_hash = put_replay_log(&store, &ReplayLog::new()).unwrap();
        let out = root.join("exemplar_replay.txt");
        let args = vec![
            "--root".to_string(),
            root.to_string_lossy().to_string(),
            "--replay".to_string(),
            hex32(&replay_hash),
            "--out-file".to_string(),
            out.to_string_lossy().to_string(),
        ];

        let rc = cmd_build_exemplar_memory(&args);
        assert_eq!(rc, 0);

        let text = std::fs::read_to_string(&out).unwrap();
        let exemplar_hash = parse_exemplar_hash(&text);
        let memory = get_exemplar_memory_v1(&store, &exemplar_hash).unwrap().unwrap();
        assert!(memory.rows.is_empty());
        assert_eq!(memory.flags & EXMEM_FLAG_HAS_REPLAY_LOG, EXMEM_FLAG_HAS_REPLAY_LOG);
    }
}

#[cfg(test)]
mod graph_build_cli_tests {
    use super::*;
    use fsa_lm::graph_relevance_artifact::get_graph_relevance_v1;
    use fsa_lm::frame::{DocId, EntityId, FrameRowV1, SourceId};
    use fsa_lm::graph_relevance::{
        GR_FLAG_HAS_ENTITY_ROWS, GR_FLAG_HAS_TERM_ROWS, GR_FLAG_HAS_VERB_ROWS,
    };

    fn tmp_dir(name: &str) -> PathBuf {
        let base = std::env::temp_dir();
        let p = base.join(format!("fsa_lm_cli_graph_{}_{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn parse_graph_hash(text: &str) -> Hash32 {
        for part in text.split_whitespace() {
            if let Some(v) = part.strip_prefix("graph_hash=") {
                return parse_hash32_hex(v).unwrap();
            }
        }
        panic!("missing graph_hash");
    }

    #[test]
    fn cmd_build_graph_relevance_from_frame_segment_ok() {
        let root = tmp_dir("build_frame_segment");
        let store = FsArtifactStore::new(&root).unwrap();
        let mut row = FrameRowV1::new(DocId(Id64(1)), SourceId(Id64(1)));
        row.terms = term_freqs_from_text("banana split recipe", TokenizerCfg::default());
        row.who = Some(EntityId(derive_id64(b"entity", b"dessert")));
        row.verb = Some(fsa_lm::frame::VerbId(derive_id64(b"verb", b"serve")));
        let seg = FrameSegmentV1::from_rows(&[row], 1024).unwrap();
        let seg_hash = put_frame_segment_v1(&store, &seg).unwrap();

        let out = root.join("graph.txt");
        let args = vec![
            "--root".to_string(),
            root.to_string_lossy().to_string(),
            "--frame-segment".to_string(),
            hex32(&seg_hash),
            "--out-file".to_string(),
            out.to_string_lossy().to_string(),
        ];

        let rc = cmd_build_graph_relevance(&args);
        assert_eq!(rc, 0);

        let text = std::fs::read_to_string(&out).unwrap();
        assert!(text.starts_with("graph_relevance_v1 "));
        let graph_hash = parse_graph_hash(&text);
        let graph = get_graph_relevance_v1(&store, &graph_hash).unwrap().unwrap();
        assert!(!graph.rows.is_empty());
        assert_eq!(graph.flags & GR_FLAG_HAS_TERM_ROWS, GR_FLAG_HAS_TERM_ROWS);
        assert_eq!(graph.flags & GR_FLAG_HAS_ENTITY_ROWS, GR_FLAG_HAS_ENTITY_ROWS);
        assert_eq!(graph.flags & GR_FLAG_HAS_VERB_ROWS, GR_FLAG_HAS_VERB_ROWS);
    }

    #[test]
    fn cmd_build_graph_relevance_replay_only_stores_empty_artifact() {
        let root = tmp_dir("build_replay_only");
        let store = FsArtifactStore::new(&root).unwrap();
        let replay_hash = put_replay_log(&store, &ReplayLog::new()).unwrap();
        let out = root.join("graph_replay.txt");
        let args = vec![
            "--root".to_string(),
            root.to_string_lossy().to_string(),
            "--replay".to_string(),
            hex32(&replay_hash),
            "--out-file".to_string(),
            out.to_string_lossy().to_string(),
        ];

        let rc = cmd_build_graph_relevance(&args);
        assert_eq!(rc, 0);

        let text = std::fs::read_to_string(&out).unwrap();
        let graph_hash = parse_graph_hash(&text);
        let graph = get_graph_relevance_v1(&store, &graph_hash).unwrap().unwrap();
        assert!(graph.rows.is_empty());
        assert_eq!(graph.flags, 0);
    }
}

#[cfg(test)]
mod answer_cli_tests {
    use super::*;
    use fsa_lm::frame::{DocId, FrameRowV1, Id64, SourceId};
    use fsa_lm::frame_segment::FrameSegmentV1;
    use fsa_lm::frame_store::put_frame_segment_v1;
    use fsa_lm::index_segment::IndexSegmentV1;
    use fsa_lm::index_store::put_index_segment_v1;
    use fsa_lm::index_snapshot::{IndexSnapshotEntryV1, IndexSnapshotV1};
    use fsa_lm::index_snapshot_store::put_index_snapshot_v1;
    use fsa_lm::markov_hints::MarkovChoiceKindV1;
    use fsa_lm::prompt_artifact::put_prompt_pack;
    use fsa_lm::prompt_pack::{PromptIds, PromptLimits, PromptPack, Role};
    use fsa_lm::tokenizer::{term_freqs_from_text, term_id_from_token, TokenizerCfg};

    fn tmp_dir(name: &str) -> PathBuf {
        let base = std::env::temp_dir();
        let p = base.join(format!("fsa_lm_cli_answer_{}_{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn walk_files_rec(dir: &std::path::Path, out: &mut Vec<PathBuf>) {
        let rd = match std::fs::read_dir(dir) {
            Ok(x) => x,
            Err(_) => return,
        };
        for ent in rd {
            let ent = match ent {
                Ok(x) => x,
                Err(_) => continue,
            };
            let p = ent.path();
            if p.is_dir() {
                walk_files_rec(&p, out);
            } else {
                out.push(p);
            }
        }
    }


    fn find_markov_trace_hash_for_answer(store_root: &std::path::Path, answer_hash: Hash32) -> Hash32 {
        // Locate the replay log that produced this answer (STEP_ANSWER_V1 outputs contain answer_hash),
        // then extract the STEP_MARKOV_TRACE_V1 output hash whose inputs include answer_hash.
        let mut files: Vec<PathBuf> = Vec::new();
        walk_files_rec(store_root, &mut files);
        files.sort();

        for p in files {
            let data = match std::fs::read(&p) {
                Ok(x) => x,
                Err(_) => continue,
            };
            let log = match fsa_lm::replay::ReplayLog::decode(&data) {
                Ok(x) => x,
                Err(_) => continue,
            };

            let mut has_answer = false;
            for st in log.steps.iter() {
                if st.name == STEP_ANSWER_V1 {
                    for h in st.outputs.iter() {
                        if *h == answer_hash {
                            has_answer = true;
                            break;
                        }
                    }
                }
                if has_answer {
                    break;
                }
            }
            if !has_answer {
                continue;
            }

            for st in log.steps.iter() {
                if st.name == STEP_MARKOV_TRACE_V1 {
                    let mut has_in = false;
                    for h in st.inputs.iter() {
                        if *h == answer_hash {
                            has_in = true;
                            break;
                        }
                    }
                    if !has_in {
                        continue;
                    }
                    if st.outputs.is_empty() {
                        continue;
                    }
                    return st.outputs[0];
                }
            }
        }

        panic!("markov trace hash not found");
    }


    fn parse_query_id_from_answer_text(s: &str) -> Hash32 {
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("query_id=") {
                let hex = match rest.split_whitespace().next() {
                    Some(x) => x,
                    None => rest,
                };
                return parse_hash32_hex(hex).unwrap();
            }
        }
        panic!("query_id not found");
    }

    fn parse_tone_from_answer_text(s: &str) -> fsa_lm::realizer_directives::ToneV1 {
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("directives tone=") {
                let tok = match rest.split_whitespace().next() {
                    Some(x) => x,
                    None => rest,
                };
                return match tok {
                    "Supportive" => fsa_lm::realizer_directives::ToneV1::Supportive,
                    "Neutral" => fsa_lm::realizer_directives::ToneV1::Neutral,
                    "Direct" => fsa_lm::realizer_directives::ToneV1::Direct,
                    "Cautious" => fsa_lm::realizer_directives::ToneV1::Cautious,
                    _ => panic!("unexpected tone token"),
                };
            }
        }
        panic!("directives tone not found");
    }

    fn parse_style_from_answer_text(s: &str) -> fsa_lm::realizer_directives::StyleV1 {
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("directives tone=") {
                // Format: "directives tone=<Tone> style=<Style> flags=..."
                if let Some(ix) = rest.find(" style=") {
                    let after = &rest[ix + 7..];
                    let tok = match after.split_whitespace().next() {
                        Some(x) => x,
                        None => after,
                    };
                    return match tok {
                        "Default" => fsa_lm::realizer_directives::StyleV1::Default,
                        "Checklist" => fsa_lm::realizer_directives::StyleV1::Checklist,
                        "StepByStep" => fsa_lm::realizer_directives::StyleV1::StepByStep,
                        "Debug" => fsa_lm::realizer_directives::StyleV1::Debug,
                        _ => panic!("unexpected style token"),
                    };
                }
            }
        }
        panic!("directives style not found");
    }

    fn evidence_lines_from_answer_text(s: &str) -> Vec<String> {
        s.lines()
            .filter(|line| line.starts_with("[E"))
            .map(|line| line.to_string())
            .collect()
    }

    fn plan_ref_lines_from_answer_text(s: &str) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for line in s.lines() {
            if line.starts_with("- item=") && line.contains(" refs=") {
                let refs_ix = line.find(" refs=").expect("plan refs");
                let refs = &line[refs_ix + 6..];
                for part in refs.split(',') {
                    let trimmed = part.trim();
                    if !trimmed.is_empty() {
                        out.push(format!(" refs={}", trimmed));
                    }
                }
                continue;
            }
            let user_prefixes = [
                "Based on: ",
                "Refs: ",
                "Summary: ",
                "Summary refs: ",
                "Key points: ",
                "Key points refs: ",
                "Steps: ",
                "Steps refs: ",
                "Next steps: ",
                "Next steps refs: ",
                "Details: ",
                "Details refs: ",
                "Keep in mind: ",
                "Keep in mind refs: ",
            ];
            for prefix in user_prefixes.iter() {
                if let Some(refs) = line.strip_prefix(prefix) {
                    for part in refs.split(',') {
                        let trimmed = part.trim();
                        if !trimmed.is_empty() {
                            out.push(format!(" refs={}", trimmed));
                        }
                    }
                    break;
                }
            }
        }
        out
    }

    fn inspect_line_from_answer_text(s: &str, prefix: &str) -> String {
        for line in s.lines() {
            if line.starts_with(prefix) {
                return line.to_string();
            }
        }
        panic!("inspect line not found");
    }

    #[test]
    fn parse_presentation_mode_v1_smoke() {
        assert_eq!(parse_presentation_mode_v1("user").unwrap(), PresentationModeV1::User);
        assert_eq!(
            parse_presentation_mode_v1("operator").unwrap(),
            PresentationModeV1::Operator
        );
        assert!(parse_presentation_mode_v1("debug").is_err());
    }

    #[test]
    fn wants_inspect_lines_v1_is_operator_only() {
        assert!(!wants_inspect_lines_v1(PresentationModeV1::User));
        assert!(wants_inspect_lines_v1(PresentationModeV1::Operator));
    }

    #[test]
    fn render_user_answer_surface_v1_hides_raw_diagnostics() {
        let base = "Answer v1
query_id=abcd snapshot_id=ef01
directives tone=Neutral style=Default
Plan
Evidence
";
        let inspect = vec![
            "routing_trace top_hint=SummaryFirst".to_string(),
            "graph_trace seeds=1 candidates=1 reasons=banana:fruit".to_string(),
            "exemplar_match exemplar_id=7 response_mode=Direct structure=Direct tone=Supportive".to_string(),
        ];
        let user = render_answer_surface_v1(PresentationModeV1::User, base, &inspect);
        assert!(!user.contains("Answer v1"));
        assert!(!user.contains("query_id="));
        assert!(!user.contains("snapshot_id="));
        assert!(!user.contains("directives tone="));
        assert!(!user.contains("routing_trace top_hint="));
        assert!(!user.contains("graph_trace seeds="));
        assert!(!user.contains("exemplar_match exemplar_id="));
        assert!(!user.contains("Plan"));
        assert!(user.contains("Sources"));
    }

    #[test]
    fn render_user_answer_surface_v1_removes_remaining_plan_and_evidence_banners() {
        let base = "Answer v1
query_id=abcd snapshot_id=ef01
Plan
Summary
- item=0 strength=0 refs=E0

Evidence
[E0] score=7 frame=deadbeef row=0
";
        let user = render_answer_surface_v1(PresentationModeV1::User, base, &[]);
        assert!(!user.contains("\nPlan\n"));
        assert!(!user.contains("\nEvidence\n"));
        assert!(user.contains("Summary"));
        assert!(user.contains("Sources"));
        assert!(user.contains("[E0] score=7 frame=deadbeef row=0"));
    }

    #[test]
    fn render_user_answer_surface_v1_shortens_clarifier_prefix() {
        let base = "To make sure I answer the right thing:
Clarifying question: What are the possible values?
";
        let user = render_answer_surface_v1(PresentationModeV1::User, base, &[]);
        assert!(user.contains("Quick question: What are the possible values?"));
        assert!(!user.contains("Clarifying question:"));
        assert!(!user.contains("To make sure I answer the right thing:"));
    }

    #[test]
    fn render_user_answer_surface_v1_shortens_alternate_clarifier_prefix() {
        let base = "So I can answer the right thing:
Clarifying question: Which options are you deciding between?
";
        let user = render_answer_surface_v1(PresentationModeV1::User, base, &[]);
        assert!(user.contains("Quick question: Which options are you deciding between?"));
        assert!(!user.contains("Clarifying question:"));
        assert!(!user.contains("So I can answer the right thing:"));
    }

    #[test]
    fn render_user_answer_surface_v1_softens_simple_plan_framing() {
        let base = "Answer v1\nquery_id=abcd snapshot_id=ef01\nPlan\n- item=0 kind=bullet refs=E0\n\nEvidence\n[E0] score=7 frame=deadbeef row=0\n";
        let user = render_answer_surface_v1(PresentationModeV1::User, base, &[]);
        assert!(user.contains("Based on: E0"));
        assert!(user.contains("Sources"));
        assert!(user.contains("[E0] score=7 frame=deadbeef row=0"));
        assert!(!user.contains("Plan"));
        assert!(!user.contains("Evidence"));
        assert!(!user.contains("kind=bullet"));
    }

    #[test]
    fn render_user_answer_surface_v1_lightens_multi_section_plan_framing() {
        let base = "Answer v1
query_id=abcd snapshot_id=ef01
directives tone=Neutral style=Concise
Plan
Summary
- item=0 kind=summary refs=E0
Details
- item=1 kind=bullet refs=E1, E2
Caveats
- item=2 kind=caveat refs=E3

Evidence
[E0] score=7 frame=deadbeef row=0
[E1] score=6 frame=feedface row=1
[E2] score=5 frame=facefeed row=2
[E3] score=4 frame=cafebabe row=3
";
        let user = render_answer_surface_v1(PresentationModeV1::User, base, &[]);
        assert!(user.contains("Summary: E0"));
        assert!(user.contains("Key points: E1, E2"));
        assert!(user.contains("Keep in mind: E3"));
        assert!(user.contains("Sources"));
        assert!(!user.contains("Plan"));
        assert!(!user.contains("Evidence"));
        assert!(!user.contains("kind=summary"));
        assert!(!user.contains("kind=bullet"));
        assert!(!user.contains("kind=caveat"));
    }

    #[test]
    fn render_user_answer_surface_v1_preserves_step_by_step_structure() {
        let base = "Answer v1\nquery_id=abcd snapshot_id=ef01\nPlan\nSummary\n- item=0 kind=summary refs=E0\nSteps\n- item=1 kind=step refs=E1\nDetails\n- item=2 kind=bullet refs=E2\n\nEvidence\n[E0] score=7 frame=deadbeef row=0\n[E1] score=6 frame=feedface row=1\n[E2] score=5 frame=cafebabe row=2\n";
        let user = render_answer_surface_v1(PresentationModeV1::User, base, &[]);
        assert!(user.contains("Summary: E0"));
        assert!(user.contains("Steps: E1"));
        assert!(user.contains("Details: E2"));
        assert!(user.contains("Sources"));
        assert!(!user.contains("Plan"));
        assert!(!user.contains("Evidence"));
        assert!(!user.contains("kind=step"));
    }

    #[test]
    fn render_user_answer_surface_v1_preserves_suggested_next_steps_structure() {
        let base = "Answer v1\nquery_id=abcd snapshot_id=ef01\nPlan\nMain answer\n- item=0 kind=summary refs=E0\nSuggested next steps\n- item=1 kind=step refs=E1\nThings to keep in mind\n- item=2 kind=caveat refs=E2\n\nEvidence\n[E0] score=7 frame=deadbeef row=0\n[E1] score=6 frame=feedface row=1\n[E2] score=5 frame=cafebabe row=2\n";
        let user = render_answer_surface_v1(PresentationModeV1::User, base, &[]);
        assert!(user.contains("Summary: E0"));
        assert!(user.contains("Steps: E1"));
        assert!(user.contains("Keep in mind: E2"));
        assert!(user.contains("Sources"));
    }

    #[test]
    fn render_operator_answer_surface_v1_preserves_raw_diagnostics() {
        let base = "Answer v1
query_id=abcd snapshot_id=ef01
directives tone=Neutral style=Default
Plan
";
        let inspect = vec!["routing_trace top_hint=SummaryFirst".to_string()];
        let operator = render_answer_surface_v1(PresentationModeV1::Operator, base, &inspect);
        assert!(operator.contains("Answer v1"));
        assert!(operator.contains("query_id=abcd snapshot_id=ef01"));
        assert!(operator.contains("directives tone=Neutral style=Default"));
        assert!(operator.contains("routing_trace top_hint=SummaryFirst"));
        assert!(operator.contains("Plan"));
    }

    #[test]
    fn resolve_conversation_runtime_state_v1_prefers_explicit_then_prior_then_workspace() {
        let workspace = WorkspaceRuntimeDefaultsV1 {
            default_expand: true,
            default_meta: false,
            default_k: Some(5),
            markov_model_id: Some([1u8; 32]),
            exemplar_memory_id: Some([2u8; 32]),
            graph_relevance_id: Some([3u8; 32]),
            ..WorkspaceRuntimeDefaultsV1::default()
        };
        let prior = ConversationRuntimeStateV1 {
            markov_model_id: Some([4u8; 32]),
            exemplar_memory_id: Some([5u8; 32]),
            graph_relevance_id: Some([6u8; 32]),
            presentation_mode: Some(PresentationModeV1::Operator),
        };

        let resolved = resolve_conversation_runtime_state_v1(
            Some([7u8; 32]),
            None,
            None,
            None,
            prior,
            &workspace,
        );
        assert_eq!(resolved.markov_model_id, Some([7u8; 32]));
        assert_eq!(resolved.exemplar_memory_id, Some([5u8; 32]));
        assert_eq!(resolved.graph_relevance_id, Some([6u8; 32]));
        assert_eq!(resolved.presentation_mode, Some(PresentationModeV1::Operator));
        assert!(resolve_runtime_expand_enabled_v1(false, &workspace, resolved));
    }

    #[test]
    fn prepare_command_runtime_setup_v1_prefers_prior_then_workspace_defaults() {
        let root = PathBuf::from("/tmp/novel-unused");
        let forward = vec!["--meta".to_string()];
        let prior = ConversationRuntimeStateV1 {
            markov_model_id: Some([9u8; 32]),
            exemplar_memory_id: Some([8u8; 32]),
            graph_relevance_id: Some([7u8; 32]),
            presentation_mode: Some(PresentationModeV1::Operator),
        };
        let setup = prepare_command_runtime_setup_v1(&root, &forward, false, prior).expect("runtime setup");
        assert_eq!(setup.forward_runtime.meta_explicit, true);
        assert_eq!(setup.sticky_runtime_state.markov_model_id, Some([9u8; 32]));
        assert_eq!(setup.sticky_runtime_state.exemplar_memory_id, Some([8u8; 32]));
        assert_eq!(setup.sticky_runtime_state.graph_relevance_id, Some([7u8; 32]));
        assert_eq!(setup.sticky_runtime_state.presentation_mode, Some(PresentationModeV1::Operator));
        assert_eq!(setup.effective_meta, true);
    }

    #[test]
    fn append_answer_runtime_args_v1_adds_defaults_only_when_omitted() {
        let mut aa = vec!["--prompt".to_string(), "deadbeef".to_string()];
        let forward = vec!["--markov-model".to_string(), hex32(&[1u8; 32])];
        let runtime_setup = CommandRuntimeSetupV1 {
            workspace_runtime: WorkspaceRuntimeDefaultsV1 {
                default_expand: true,
                default_meta: true,
                default_k: Some(5),
                ..WorkspaceRuntimeDefaultsV1::default()
            },
            forward_runtime: ForwardRuntimeSelectionV1 {
                explicit_markov_id: Some([1u8; 32]),
                ..ForwardRuntimeSelectionV1::default()
            },
            pre_sticky_expand: true,
            effective_expand: true,
            effective_meta: true,
            effective_k: Some(5),
            sticky_runtime_state: ConversationRuntimeStateV1 {
                markov_model_id: Some([2u8; 32]),
                exemplar_memory_id: Some([3u8; 32]),
                graph_relevance_id: Some([4u8; 32]),
                presentation_mode: Some(PresentationModeV1::User),
            },
        };
        append_answer_runtime_args_v1(
            &mut aa,
            &forward,
            &runtime_setup,
            [5u8; 32],
            [6u8; 32],
            Some([7u8; 32]),
        );
        let joined = aa.join(" ");
        assert!(joined.contains("--snapshot"));
        assert!(joined.contains("--sig-map"));
        assert!(joined.contains("--expand"));
        assert!(joined.contains("--lexicon-snapshot"));
        assert!(joined.contains("--k 5"));
        assert!(joined.contains("--meta"));
        assert!(!joined.contains("--sticky-markov-model"));
        assert!(joined.contains("--sticky-exemplar-memory"));
        assert!(joined.contains("--sticky-graph-relevance"));
        assert!(joined.contains("--presentation user"));
    }

    #[test]
    fn render_operator_answer_surface_v1_inserts_after_query_line_without_directives() {
        let base = "Answer v1
query_id=abcd snapshot_id=ef01
Plan
";
        let inspect = vec!["routing_trace top_hint=SummaryFirst".to_string()];
        let operator = render_answer_surface_v1(PresentationModeV1::Operator, base, &inspect);
        let lines: Vec<&str> = operator.lines().collect();
        let query_ix = lines
            .iter()
            .position(|line| *line == "query_id=abcd snapshot_id=ef01")
            .expect("query line");
        assert_eq!(
            lines.get(query_ix + 1).copied(),
            Some("routing_trace top_hint=SummaryFirst")
        );
    }

    #[test]
    fn render_operator_answer_surface_v1_groups_trace_lines_after_directives() {
        let base = "Answer v1
query_id=abcd snapshot_id=ef01
directives tone=Supportive style=Default
Plan
";
        let inspect = vec![
            "routing_trace top_hint=SummaryFirst".to_string(),
            "graph_trace seeds=1 candidates=1 reasons=banana:fruit".to_string(),
            "exemplar_match exemplar_id=7 response_mode=Direct structure=Direct tone=Supportive".to_string(),
        ];
        let operator = render_answer_surface_v1(PresentationModeV1::Operator, base, &inspect);
        let lines: Vec<&str> = operator.lines().collect();
        let directives_ix = lines
            .iter()
            .position(|line| *line == "directives tone=Supportive style=Default")
            .expect("directives line");
        assert_eq!(
            lines.get(directives_ix + 1).copied(),
            Some("routing_trace top_hint=SummaryFirst")
        );
        assert_eq!(
            lines.get(directives_ix + 2).copied(),
            Some("graph_trace seeds=1 candidates=1 reasons=banana:fruit")
        );
        assert_eq!(
            lines.get(directives_ix + 3).copied(),
            Some("exemplar_match exemplar_id=7 response_mode=Direct structure=Direct tone=Supportive")
        );
        assert!(operator.contains("Plan"));
    }

    #[test]
    fn cmd_answer_rejects_bad_presentation_mode() {
        let rc = cmd_answer(&[
            "--presentation".to_string(),
            "debug".to_string(),
        ]);
        assert_eq!(rc, 1);
    }

    #[test]
    fn cmd_answer_smoke() {
        let root = tmp_dir("smoke");
        let store_root = root.join("store");
        let store = fsa_lm::artifact::FsArtifactStore::new(&store_root).unwrap();

        // Build a minimal FrameSegment + IndexSegment + IndexSnapshot.
        let terms = term_freqs_from_text("banana", TokenizerCfg::default());
        let mut row = FrameRowV1::new(DocId(Id64(1)), SourceId(Id64(1)));
        row.terms = terms;
        row.recompute_doc_len();
        let frame_seg = FrameSegmentV1::from_rows(&[row], 1024).unwrap();
        let frame_hash = put_frame_segment_v1(&store, &frame_seg).unwrap();

        let idx_seg = IndexSegmentV1::build_from_segment(frame_hash, &frame_seg).unwrap();
        let idx_hash = put_index_segment_v1(&store, &idx_seg).unwrap();

        let mut snap = IndexSnapshotV1::new(SourceId(Id64(1)));
        snap.entries.push(IndexSnapshotEntryV1 {
            frame_seg: frame_hash,
            index_seg: idx_hash,
            row_count: idx_seg.row_count,
            term_count: idx_seg.terms.len() as u32,
            postings_bytes: idx_seg.postings.len() as u32,
        });
        let snap_hash = put_index_snapshot_v1(&store, &snap).unwrap();

        // Prompt pack with a user message.
        let ids = PromptIds {
            snapshot_id: [0u8; 32],
            weights_id: [0u8; 32],
            tokenizer_id: [0u8; 32],
        };
        let mut pack = PromptPack::new(123, 256, ids);
        pack.messages.push(fsa_lm::prompt_pack::Message {
            role: Role::User,
            content: "banana".to_string(),
        });
        let prompt_hash = put_prompt_pack(&store, &mut pack, PromptLimits::default_v1()).unwrap();

        let out_path = root.join("answer.txt");
        let rc = cmd_answer(&[
            "--root".to_string(),
            store_root.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--snapshot".to_string(),
            hex32(&snap_hash),
            "--out-file".to_string(),
            out_path.to_string_lossy().to_string(),
        ]);
        assert_eq!(rc, 0);

        let s = std::fs::read_to_string(&out_path).unwrap();
        assert!(!s.contains("Answer v1"));
        assert!(!s.contains("query_id="));
        assert!(s.contains("Based on: E0"));
        assert!(s.contains("Sources"));
        assert!(!s.contains("Plan"));
        assert!(!s.contains("Evidence"));
        assert!(s.contains("[E0]"));
    }


    #[test]
    fn cmd_answer_presentation_mode_preserves_grounding_and_refs() {
        let root = tmp_dir("presentation_grounding");
        let store_root = root.join("store");
        let store = fsa_lm::artifact::FsArtifactStore::new(&store_root).unwrap();

        let mut row0 = FrameRowV1::new(DocId(Id64(1)), SourceId(Id64(1)));
        row0.terms = term_freqs_from_text("banana split", TokenizerCfg::default());
        row0.recompute_doc_len();
        let mut row1 = FrameRowV1::new(DocId(Id64(2)), SourceId(Id64(1)));
        row1.terms = term_freqs_from_text("banana bread", TokenizerCfg::default());
        row1.recompute_doc_len();
        let frame_seg = FrameSegmentV1::from_rows(&[row0, row1], 1024).unwrap();
        let frame_hash = put_frame_segment_v1(&store, &frame_seg).unwrap();

        let idx_seg = IndexSegmentV1::build_from_segment(frame_hash, &frame_seg).unwrap();
        let idx_hash = put_index_segment_v1(&store, &idx_seg).unwrap();

        let mut snap = IndexSnapshotV1::new(SourceId(Id64(1)));
        snap.entries.push(IndexSnapshotEntryV1 {
            frame_seg: frame_hash,
            index_seg: idx_hash,
            row_count: idx_seg.row_count,
            term_count: idx_seg.terms.len() as u32,
            postings_bytes: idx_seg.postings.len() as u32,
        });
        let snap_hash = put_index_snapshot_v1(&store, &snap).unwrap();

        let ids = PromptIds {
            snapshot_id: [0u8; 32],
            weights_id: [0u8; 32],
            tokenizer_id: [0u8; 32],
        };
        let mut pack = PromptPack::new(123, 256, ids);
        pack.messages.push(fsa_lm::prompt_pack::Message {
            role: Role::User,
            content: "Summarize banana split briefly".to_string(),
        });
        let prompt_hash = put_prompt_pack(&store, &mut pack, PromptLimits::default_v1()).unwrap();

        let user_out_path = root.join("answer_user.txt");
        let rc = cmd_answer(&[
            "--root".to_string(),
            store_root.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--snapshot".to_string(),
            hex32(&snap_hash),
            "--out-file".to_string(),
            user_out_path.to_string_lossy().to_string(),
        ]);
        assert_eq!(rc, 0);

        let operator_out_path = root.join("answer_operator.txt");
        let rc = cmd_answer(&[
            "--root".to_string(),
            store_root.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--snapshot".to_string(),
            hex32(&snap_hash),
            "--presentation".to_string(),
            "operator".to_string(),
            "--out-file".to_string(),
            operator_out_path.to_string_lossy().to_string(),
        ]);
        assert_eq!(rc, 0);

        let user_text = std::fs::read_to_string(&user_out_path).unwrap();
        let operator_text = std::fs::read_to_string(&operator_out_path).unwrap();
        assert!(!user_text.contains("Answer v1"));
        assert!(operator_text.contains("Answer v1"));
        assert!(!user_text.contains("query_id="));
        assert!(operator_text.contains("query_id="));
        assert_eq!(
            user_text,
            render_user_answer_surface_v1(&operator_text, &[])
        );
        assert_eq!(
            evidence_lines_from_answer_text(&user_text),
            evidence_lines_from_answer_text(&operator_text)
        );
        assert_eq!(
            plan_ref_lines_from_answer_text(&user_text),
            plan_ref_lines_from_answer_text(&operator_text)
        );
    }

    #[test]
    fn cmd_answer_with_graph_relevance_adds_second_hit() {
        let root = tmp_dir("graph_expand");
        let store_root = root.join("store");
        let store = fsa_lm::artifact::FsArtifactStore::new(&store_root).unwrap();

        let mut row0 = FrameRowV1::new(DocId(Id64(1)), SourceId(Id64(1)));
        row0.terms = term_freqs_from_text("banana", TokenizerCfg::default());
        row0.recompute_doc_len();
        let mut row1 = FrameRowV1::new(DocId(Id64(2)), SourceId(Id64(1)));
        row1.terms = term_freqs_from_text("carrot", TokenizerCfg::default());
        row1.recompute_doc_len();
        let frame_seg = FrameSegmentV1::from_rows(&[row0, row1], 1024).unwrap();
        let frame_hash = put_frame_segment_v1(&store, &frame_seg).unwrap();
        let idx_seg = IndexSegmentV1::build_from_segment(frame_hash, &frame_seg).unwrap();
        let idx_hash = put_index_segment_v1(&store, &idx_seg).unwrap();
        let mut snap = IndexSnapshotV1::new(SourceId(Id64(1)));
        snap.entries.push(IndexSnapshotEntryV1 {
            frame_seg: frame_hash,
            index_seg: idx_hash,
            row_count: idx_seg.row_count,
            term_count: idx_seg.terms.len() as u32,
            postings_bytes: idx_seg.postings.len() as u32,
        });
        let snap_hash = put_index_snapshot_v1(&store, &snap).unwrap();

        let banana = term_id_from_token("banana", TokenizerCfg::default());
        let carrot = term_id_from_token("carrot", TokenizerCfg::default());
        let graph = fsa_lm::graph_relevance::GraphRelevanceV1 {
            version: fsa_lm::graph_relevance::GRAPH_RELEVANCE_V1_VERSION,
            build_id: fsa_lm::hash::blake3_hash(b"answer-graph"),
            flags: fsa_lm::graph_relevance::GR_FLAG_HAS_TERM_ROWS,
            rows: vec![fsa_lm::graph_relevance::GraphRelevanceRowV1 {
                seed_kind: fsa_lm::graph_relevance::GraphNodeKindV1::Term,
                seed_id: banana.0,
                edges: vec![fsa_lm::graph_relevance::GraphRelevanceEdgeV1::new(
                    fsa_lm::graph_relevance::GraphNodeKindV1::Term,
                    carrot.0,
                    20_000,
                    1,
                    fsa_lm::graph_relevance::GREDGE_FLAG_SYMMETRIC,
                )],
            }],
        };
        let graph_hash = fsa_lm::graph_relevance_artifact::put_graph_relevance_v1(&store, &graph).unwrap();

        let ids = PromptIds { snapshot_id: [0u8; 32], weights_id: [0u8; 32], tokenizer_id: [0u8; 32] };
        let mut pack = PromptPack::new(123, 256, ids);
        pack.messages.push(fsa_lm::prompt_pack::Message { role: Role::User, content: "banana".to_string() });
        let prompt_hash = put_prompt_pack(&store, &mut pack, PromptLimits::default_v1()).unwrap();

        let out_path = root.join("answer_graph.txt");
        let rc = cmd_answer(&[
            "--root".to_string(),
            store_root.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--snapshot".to_string(),
            hex32(&snap_hash),
            "--expand".to_string(),
            "--presentation".to_string(),
            "operator".to_string(),
            "--graph-relevance".to_string(),
            hex32(&graph_hash),
            "--out-file".to_string(),
            out_path.to_string_lossy().to_string(),
        ]);
        assert_eq!(rc, 0);
        let s = std::fs::read_to_string(&out_path).unwrap();
        assert!(s.contains("[E0]"));
        assert!(s.contains("[E1]"));
        let graph_line = inspect_line_from_answer_text(&s, "graph_trace ");
        assert!(graph_line.contains("seeds=1"));
        assert!(graph_line.contains("candidates=1"));
        assert!(graph_line.contains("banana:"));
    }

    #[test]
    fn cmd_answer_graph_expansion_keeps_lexical_evidence_first() {
        let root = tmp_dir("graph_precedence");
        let store_root = root.join("store");
        let store = fsa_lm::artifact::FsArtifactStore::new(&store_root).unwrap();

        let mut row0 = FrameRowV1::new(DocId(Id64(1)), SourceId(Id64(1)));
        row0.terms = term_freqs_from_text("banana", TokenizerCfg::default());
        row0.recompute_doc_len();
        let mut row1 = FrameRowV1::new(DocId(Id64(2)), SourceId(Id64(1)));
        row1.terms = term_freqs_from_text("carrot", TokenizerCfg::default());
        row1.recompute_doc_len();
        let frame_seg = FrameSegmentV1::from_rows(&[row0, row1], 1024).unwrap();
        let frame_hash = put_frame_segment_v1(&store, &frame_seg).unwrap();
        let idx_seg = IndexSegmentV1::build_from_segment(frame_hash, &frame_seg).unwrap();
        let idx_hash = put_index_segment_v1(&store, &idx_seg).unwrap();
        let mut snap = IndexSnapshotV1::new(SourceId(Id64(1)));
        snap.entries.push(IndexSnapshotEntryV1 {
            frame_seg: frame_hash,
            index_seg: idx_hash,
            row_count: idx_seg.row_count,
            term_count: idx_seg.terms.len() as u32,
            postings_bytes: idx_seg.postings.len() as u32,
        });
        let snap_hash = put_index_snapshot_v1(&store, &snap).unwrap();

        let banana = term_id_from_token("banana", TokenizerCfg::default());
        let carrot = term_id_from_token("carrot", TokenizerCfg::default());
        let graph = fsa_lm::graph_relevance::GraphRelevanceV1 {
            version: fsa_lm::graph_relevance::GRAPH_RELEVANCE_V1_VERSION,
            build_id: fsa_lm::hash::blake3_hash(b"answer-graph-precedence"),
            flags: fsa_lm::graph_relevance::GR_FLAG_HAS_TERM_ROWS,
            rows: vec![fsa_lm::graph_relevance::GraphRelevanceRowV1 {
                seed_kind: fsa_lm::graph_relevance::GraphNodeKindV1::Term,
                seed_id: banana.0,
                edges: vec![fsa_lm::graph_relevance::GraphRelevanceEdgeV1::new(
                    fsa_lm::graph_relevance::GraphNodeKindV1::Term,
                    carrot.0,
                    20_000,
                    1,
                    fsa_lm::graph_relevance::GREDGE_FLAG_SYMMETRIC,
                )],
            }],
        };
        let graph_hash = fsa_lm::graph_relevance_artifact::put_graph_relevance_v1(&store, &graph).unwrap();

        let ids = PromptIds { snapshot_id: [0u8; 32], weights_id: [0u8; 32], tokenizer_id: [0u8; 32] };
        let mut pack = PromptPack::new(123, 256, ids);
        pack.messages.push(fsa_lm::prompt_pack::Message { role: Role::User, content: "banana".to_string() });
        let prompt_hash = put_prompt_pack(&store, &mut pack, PromptLimits::default_v1()).unwrap();

        let out_path = root.join("answer_graph_precedence.txt");
        let rc = cmd_answer(&[
            "--root".to_string(),
            store_root.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--snapshot".to_string(),
            hex32(&snap_hash),
            "--expand".to_string(),
            "--presentation".to_string(),
            "operator".to_string(),
            "--graph-relevance".to_string(),
            hex32(&graph_hash),
            "--out-file".to_string(),
            out_path.to_string_lossy().to_string(),
        ]);
        assert_eq!(rc, 0);
        let s = std::fs::read_to_string(&out_path).unwrap();
        let evidence = evidence_lines_from_answer_text(&s);
        assert_eq!(evidence.len(), 2);
        assert!(evidence[0].contains("doc_id=1"));
        assert!(evidence[1].contains("doc_id=2"));
    }

    #[test]
    fn cmd_answer_exemplar_advisory_does_not_change_evidence_lines() {
        let root = tmp_dir("exemplar_evidence_lock");
        let store_root = root.join("store");
        let store = fsa_lm::artifact::FsArtifactStore::new(&store_root).unwrap();

        let terms = term_freqs_from_text("banana", TokenizerCfg::default());
        let mut row = FrameRowV1::new(DocId(Id64(1)), SourceId(Id64(1)));
        row.terms = terms;
        row.recompute_doc_len();
        let frame_seg = FrameSegmentV1::from_rows(&[row], 1024).unwrap();
        let frame_hash = put_frame_segment_v1(&store, &frame_seg).unwrap();
        let idx_seg = IndexSegmentV1::build_from_segment(frame_hash, &frame_seg).unwrap();
        let idx_hash = put_index_segment_v1(&store, &idx_seg).unwrap();
        let mut snap = IndexSnapshotV1::new(SourceId(Id64(1)));
        snap.entries.push(IndexSnapshotEntryV1 {
            frame_seg: frame_hash,
            index_seg: idx_hash,
            row_count: idx_seg.row_count,
            term_count: idx_seg.terms.len() as u32,
            postings_bytes: idx_seg.postings.len() as u32,
        });
        let snap_hash = put_index_snapshot_v1(&store, &snap).unwrap();

        let ids = PromptIds { snapshot_id: [0u8; 32], weights_id: [0u8; 32], tokenizer_id: [0u8; 32] };
        let mut pack = PromptPack::new(123, 256, ids);
        pack.messages.push(fsa_lm::prompt_pack::Message { role: Role::User, content: "banana".to_string() });
        let prompt_hash = put_prompt_pack(&store, &mut pack, PromptLimits::default_v1()).unwrap();

        let exemplar_memory = fsa_lm::exemplar_memory::ExemplarMemoryV1 {
            version: fsa_lm::exemplar_memory::EXEMPLAR_MEMORY_V1_VERSION,
            build_id: [0u8; 32],
            flags: 0,
            rows: vec![fsa_lm::exemplar_memory::ExemplarRowV1 {
                exemplar_id: Id64(7),
                response_mode: fsa_lm::exemplar_memory::ExemplarResponseModeV1::Direct,
                structure_kind: fsa_lm::exemplar_memory::ExemplarStructureKindV1::Direct,
                tone_kind: fsa_lm::exemplar_memory::ExemplarToneKindV1::Supportive,
                flags: 0,
                support_count: 1,
                support_refs: Vec::new(),
            }],
        };
        let exemplar_hash = fsa_lm::exemplar_memory_artifact::put_exemplar_memory_v1(&store, &exemplar_memory).unwrap();

        let base_out = root.join("answer_base.txt");
        let shaped_out = root.join("answer_exemplar.txt");
        let rc0 = cmd_answer(&[
            "--root".to_string(),
            store_root.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--snapshot".to_string(),
            hex32(&snap_hash),
            "--out-file".to_string(),
            base_out.to_string_lossy().to_string(),
        ]);
        assert_eq!(rc0, 0);
        let rc1 = cmd_answer(&[
            "--root".to_string(),
            store_root.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--snapshot".to_string(),
            hex32(&snap_hash),
            "--exemplar-memory".to_string(),
            hex32(&exemplar_hash),
            "--presentation".to_string(),
            "operator".to_string(),
            "--out-file".to_string(),
            shaped_out.to_string_lossy().to_string(),
        ]);
        assert_eq!(rc1, 0);

        let base_text = std::fs::read_to_string(&base_out).unwrap();
        let shaped_text = std::fs::read_to_string(&shaped_out).unwrap();
        assert_eq!(
            evidence_lines_from_answer_text(&base_text),
            evidence_lines_from_answer_text(&shaped_text)
        );
    }

    #[test]
    fn cmd_answer_markov_preface_does_not_change_evidence_lines() {
        let root = tmp_dir("markov_evidence_lock");
        let store_root = root.join("store");
        let store = fsa_lm::artifact::FsArtifactStore::new(&store_root).unwrap();

        let terms = term_freqs_from_text("banana", TokenizerCfg::default());
        let mut row = FrameRowV1::new(DocId(Id64(1)), SourceId(Id64(1)));
        row.terms = terms;
        row.recompute_doc_len();
        let frame_seg = FrameSegmentV1::from_rows(&[row], 1024).unwrap();
        let frame_hash = put_frame_segment_v1(&store, &frame_seg).unwrap();
        let idx_seg = IndexSegmentV1::build_from_segment(frame_hash, &frame_seg).unwrap();
        let idx_hash = put_index_segment_v1(&store, &idx_seg).unwrap();
        let mut snap = IndexSnapshotV1::new(SourceId(Id64(1)));
        snap.entries.push(IndexSnapshotEntryV1 {
            frame_seg: frame_hash,
            index_seg: idx_hash,
            row_count: idx_seg.row_count,
            term_count: idx_seg.terms.len() as u32,
            postings_bytes: idx_seg.postings.len() as u32,
        });
        let snap_hash = put_index_snapshot_v1(&store, &snap).unwrap();

        let ids = PromptIds { snapshot_id: [0u8; 32], weights_id: [0u8; 32], tokenizer_id: [0u8; 32] };
        let mut pack = PromptPack::new(123, 256, ids);
        pack.messages.push(fsa_lm::prompt_pack::Message { role: Role::User, content: "banana".to_string() });
        let prompt_hash = put_prompt_pack(&store, &mut pack, PromptLimits::default_v1()).unwrap();

        let pf = fsa_lm::pragmatics_frame::PragmaticsFrameV1 {
            version: fsa_lm::pragmatics_frame::PRAGMATICS_FRAME_V1_VERSION,
            source_id: Id64(1),
            msg_ix: 0,
            byte_len: 12,
            ascii_only: 1,
            temperature: 0,
            valence: 0,
            arousal: 0,
            politeness: 800,
            formality: 200,
            directness: 300,
            empathy_need: 800,
            mode: fsa_lm::pragmatics_frame::RhetoricModeV1::Ask,
            flags: 0,
            exclamations: 0,
            questions: 1,
            ellipses: 0,
            caps_words: 0,
            repeat_punct_runs: 0,
            quotes: 0,
            emphasis_score: 0,
            hedge_count: 0,
            intensifier_count: 0,
            profanity_count: 0,
            apology_count: 0,
            gratitude_count: 0,
            insult_count: 0,
        };
        let prag_hash = fsa_lm::pragmatics_frame_store::put_pragmatics_frame_v1(&store, &pf).unwrap();

        use fsa_lm::markov_model::{MarkovModelV1, MarkovNextV1, MarkovStateV1, MARKOV_MODEL_V1_VERSION};
        let cid0 = fsa_lm::frame::derive_id64(b"markov_choice_v1", b"preface:supportive:0");
        let cid1 = fsa_lm::frame::derive_id64(b"markov_choice_v1", b"preface:supportive:1");
        let model = MarkovModelV1 {
            version: MARKOV_MODEL_V1_VERSION,
            order_n_max: 3,
            max_next_per_state: 8,
            total_transitions: 30,
            corpus_hash: [0u8; 32],
            states: vec![MarkovStateV1 {
                context: Vec::new(),
                escape_count: 0,
                next: vec![
                    MarkovNextV1 { token: MarkovTokenV1::new(MarkovChoiceKindV1::Opener, cid1), count: 20 },
                    MarkovNextV1 { token: MarkovTokenV1::new(MarkovChoiceKindV1::Opener, cid0), count: 10 },
                ],
            }],
        };
        assert!(model.validate().is_ok());
        let model_hash = fsa_lm::markov_model_artifact::put_markov_model_v1(&store, &model).unwrap();

        let base_out = root.join("answer_markov_base.txt");
        let shaped_out = root.join("answer_markov_shaped.txt");
        let rc0 = cmd_answer(&[
            "--root".to_string(),
            store_root.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--snapshot".to_string(),
            hex32(&snap_hash),
            "--pragmatics".to_string(),
            hex32(&prag_hash),
            "--presentation".to_string(),
            "operator".to_string(),
            "--out-file".to_string(),
            base_out.to_string_lossy().to_string(),
        ]);
        assert_eq!(rc0, 0);
        let rc1 = cmd_answer(&[
            "--root".to_string(),
            store_root.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--snapshot".to_string(),
            hex32(&snap_hash),
            "--pragmatics".to_string(),
            hex32(&prag_hash),
            "--markov-model".to_string(),
            hex32(&model_hash),
            "--out-file".to_string(),
            shaped_out.to_string_lossy().to_string(),
        ]);
        assert_eq!(rc1, 0);

        let base_text = std::fs::read_to_string(&base_out).unwrap();
        let shaped_text = std::fs::read_to_string(&shaped_out).unwrap();
        assert_eq!(
            evidence_lines_from_answer_text(&base_text),
            evidence_lines_from_answer_text(&shaped_text)
        );
    }

    #[test]
    fn cmd_answer_graph_only_expansion_without_supported_hit_does_not_add_evidence() {
        let root = tmp_dir("graph_unsupported_lock");
        let store_root = root.join("store");
        let store = fsa_lm::artifact::FsArtifactStore::new(&store_root).unwrap();

        let mut row0 = FrameRowV1::new(DocId(Id64(1)), SourceId(Id64(1)));
        row0.terms = term_freqs_from_text("banana", TokenizerCfg::default());
        row0.recompute_doc_len();
        let frame_seg = FrameSegmentV1::from_rows(&[row0], 1024).unwrap();
        let frame_hash = put_frame_segment_v1(&store, &frame_seg).unwrap();
        let idx_seg = IndexSegmentV1::build_from_segment(frame_hash, &frame_seg).unwrap();
        let idx_hash = put_index_segment_v1(&store, &idx_seg).unwrap();
        let mut snap = IndexSnapshotV1::new(SourceId(Id64(1)));
        snap.entries.push(IndexSnapshotEntryV1 {
            frame_seg: frame_hash,
            index_seg: idx_hash,
            row_count: idx_seg.row_count,
            term_count: idx_seg.terms.len() as u32,
            postings_bytes: idx_seg.postings.len() as u32,
        });
        let snap_hash = put_index_snapshot_v1(&store, &snap).unwrap();

        let banana = term_id_from_token("banana", TokenizerCfg::default());
        let carrot = term_id_from_token("carrot", TokenizerCfg::default());
        let graph = fsa_lm::graph_relevance::GraphRelevanceV1 {
            version: fsa_lm::graph_relevance::GRAPH_RELEVANCE_V1_VERSION,
            build_id: fsa_lm::hash::blake3_hash(b"answer-graph-unsupported-lock"),
            flags: fsa_lm::graph_relevance::GR_FLAG_HAS_TERM_ROWS,
            rows: vec![fsa_lm::graph_relevance::GraphRelevanceRowV1 {
                seed_kind: fsa_lm::graph_relevance::GraphNodeKindV1::Term,
                seed_id: banana.0,
                edges: vec![fsa_lm::graph_relevance::GraphRelevanceEdgeV1::new(
                    fsa_lm::graph_relevance::GraphNodeKindV1::Term,
                    carrot.0,
                    20_000,
                    1,
                    fsa_lm::graph_relevance::GREDGE_FLAG_SYMMETRIC,
                )],
            }],
        };
        let graph_hash = fsa_lm::graph_relevance_artifact::put_graph_relevance_v1(&store, &graph).unwrap();

        let ids = PromptIds { snapshot_id: [0u8; 32], weights_id: [0u8; 32], tokenizer_id: [0u8; 32] };
        let mut pack = PromptPack::new(123, 256, ids);
        pack.messages.push(fsa_lm::prompt_pack::Message { role: Role::User, content: "banana".to_string() });
        let prompt_hash = put_prompt_pack(&store, &mut pack, PromptLimits::default_v1()).unwrap();

        let out_path = root.join("answer_graph_unsupported_lock.txt");
        let rc = cmd_answer(&[
            "--root".to_string(),
            store_root.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--snapshot".to_string(),
            hex32(&snap_hash),
            "--expand".to_string(),
            "--graph-relevance".to_string(),
            hex32(&graph_hash),
            "--out-file".to_string(),
            out_path.to_string_lossy().to_string(),
        ]);
        assert_eq!(rc, 0);

        let s = std::fs::read_to_string(&out_path).unwrap();
        let evidence = evidence_lines_from_answer_text(&s);
        assert_eq!(evidence.len(), 1);
        assert!(evidence[0].contains("doc_id=1"));
        assert!(!s.contains("doc_id=2"));
    }

    #[test]
    fn cmd_answer_exemplar_advisory_does_not_change_plan_refs() {
        let root = tmp_dir("exemplar_plan_ref_lock");
        let store_root = root.join("store");
        let store = fsa_lm::artifact::FsArtifactStore::new(&store_root).unwrap();

        let terms = term_freqs_from_text("banana", TokenizerCfg::default());
        let mut row = FrameRowV1::new(DocId(Id64(1)), SourceId(Id64(1)));
        row.terms = terms;
        row.recompute_doc_len();
        let frame_seg = FrameSegmentV1::from_rows(&[row], 1024).unwrap();
        let frame_hash = put_frame_segment_v1(&store, &frame_seg).unwrap();
        let idx_seg = IndexSegmentV1::build_from_segment(frame_hash, &frame_seg).unwrap();
        let idx_hash = put_index_segment_v1(&store, &idx_seg).unwrap();
        let mut snap = IndexSnapshotV1::new(SourceId(Id64(1)));
        snap.entries.push(IndexSnapshotEntryV1 {
            frame_seg: frame_hash,
            index_seg: idx_hash,
            row_count: idx_seg.row_count,
            term_count: idx_seg.terms.len() as u32,
            postings_bytes: idx_seg.postings.len() as u32,
        });
        let snap_hash = put_index_snapshot_v1(&store, &snap).unwrap();

        let ids = PromptIds { snapshot_id: [0u8; 32], weights_id: [0u8; 32], tokenizer_id: [0u8; 32] };
        let mut pack = PromptPack::new(123, 256, ids);
        pack.messages.push(fsa_lm::prompt_pack::Message { role: Role::User, content: "banana".to_string() });
        let prompt_hash = put_prompt_pack(&store, &mut pack, PromptLimits::default_v1()).unwrap();

        let exemplar_memory = fsa_lm::exemplar_memory::ExemplarMemoryV1 {
            version: fsa_lm::exemplar_memory::EXEMPLAR_MEMORY_V1_VERSION,
            build_id: [0u8; 32],
            flags: 0,
            rows: vec![fsa_lm::exemplar_memory::ExemplarRowV1 {
                exemplar_id: Id64(9),
                response_mode: fsa_lm::exemplar_memory::ExemplarResponseModeV1::Recommend,
                structure_kind: fsa_lm::exemplar_memory::ExemplarStructureKindV1::Steps,
                tone_kind: fsa_lm::exemplar_memory::ExemplarToneKindV1::Supportive,
                flags: fsa_lm::exemplar_memory::EXROW_FLAG_HAS_STEPS,
                support_count: 1,
                support_refs: Vec::new(),
            }],
        };
        let exemplar_hash = fsa_lm::exemplar_memory_artifact::put_exemplar_memory_v1(&store, &exemplar_memory).unwrap();

        let base_out = root.join("answer_plan_base.txt");
        let shaped_out = root.join("answer_plan_exemplar.txt");
        let rc0 = cmd_answer(&[
            "--root".to_string(),
            store_root.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--snapshot".to_string(),
            hex32(&snap_hash),
            "--out-file".to_string(),
            base_out.to_string_lossy().to_string(),
        ]);
        assert_eq!(rc0, 0);
        let rc1 = cmd_answer(&[
            "--root".to_string(),
            store_root.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--snapshot".to_string(),
            hex32(&snap_hash),
            "--exemplar-memory".to_string(),
            hex32(&exemplar_hash),
            "--out-file".to_string(),
            shaped_out.to_string_lossy().to_string(),
        ]);
        assert_eq!(rc1, 0);

        let base_text = std::fs::read_to_string(&base_out).unwrap();
        let shaped_text = std::fs::read_to_string(&shaped_out).unwrap();
        assert_eq!(
            plan_ref_lines_from_answer_text(&base_text),
            plan_ref_lines_from_answer_text(&shaped_text)
        );
    }

    #[test]
    fn cmd_answer_markov_preface_does_not_change_plan_refs() {
        let root = tmp_dir("markov_plan_ref_lock");
        let store_root = root.join("store");
        let store = fsa_lm::artifact::FsArtifactStore::new(&store_root).unwrap();

        let terms = term_freqs_from_text("banana", TokenizerCfg::default());
        let mut row = FrameRowV1::new(DocId(Id64(1)), SourceId(Id64(1)));
        row.terms = terms;
        row.recompute_doc_len();
        let frame_seg = FrameSegmentV1::from_rows(&[row], 1024).unwrap();
        let frame_hash = put_frame_segment_v1(&store, &frame_seg).unwrap();
        let idx_seg = IndexSegmentV1::build_from_segment(frame_hash, &frame_seg).unwrap();
        let idx_hash = put_index_segment_v1(&store, &idx_seg).unwrap();
        let mut snap = IndexSnapshotV1::new(SourceId(Id64(1)));
        snap.entries.push(IndexSnapshotEntryV1 {
            frame_seg: frame_hash,
            index_seg: idx_hash,
            row_count: idx_seg.row_count,
            term_count: idx_seg.terms.len() as u32,
            postings_bytes: idx_seg.postings.len() as u32,
        });
        let snap_hash = put_index_snapshot_v1(&store, &snap).unwrap();

        let ids = PromptIds { snapshot_id: [0u8; 32], weights_id: [0u8; 32], tokenizer_id: [0u8; 32] };
        let mut pack = PromptPack::new(123, 256, ids);
        pack.messages.push(fsa_lm::prompt_pack::Message { role: Role::User, content: "banana".to_string() });
        let prompt_hash = put_prompt_pack(&store, &mut pack, PromptLimits::default_v1()).unwrap();

        let pf = fsa_lm::pragmatics_frame::PragmaticsFrameV1 {
            version: fsa_lm::pragmatics_frame::PRAGMATICS_FRAME_V1_VERSION,
            source_id: Id64(1),
            msg_ix: 0,
            byte_len: 12,
            ascii_only: 1,
            temperature: 0,
            valence: 0,
            arousal: 0,
            politeness: 800,
            formality: 200,
            directness: 300,
            empathy_need: 800,
            mode: fsa_lm::pragmatics_frame::RhetoricModeV1::Ask,
            flags: 0,
            exclamations: 0,
            questions: 1,
            ellipses: 0,
            caps_words: 0,
            repeat_punct_runs: 0,
            quotes: 0,
            emphasis_score: 0,
            hedge_count: 0,
            intensifier_count: 0,
            profanity_count: 0,
            apology_count: 0,
            gratitude_count: 0,
            insult_count: 0,
        };
        let prag_hash = fsa_lm::pragmatics_frame_store::put_pragmatics_frame_v1(&store, &pf).unwrap();

        use fsa_lm::markov_model::{MarkovModelV1, MarkovNextV1, MarkovStateV1, MARKOV_MODEL_V1_VERSION};
        let cid0 = fsa_lm::frame::derive_id64(b"markov_choice_v1", b"preface:supportive:0");
        let cid1 = fsa_lm::frame::derive_id64(b"markov_choice_v1", b"preface:supportive:1");
        let model = MarkovModelV1 {
            version: MARKOV_MODEL_V1_VERSION,
            order_n_max: 3,
            max_next_per_state: 8,
            total_transitions: 30,
            corpus_hash: [0u8; 32],
            states: vec![MarkovStateV1 {
                context: Vec::new(),
                escape_count: 0,
                next: vec![
                    MarkovNextV1 { token: MarkovTokenV1::new(MarkovChoiceKindV1::Opener, cid1), count: 20 },
                    MarkovNextV1 { token: MarkovTokenV1::new(MarkovChoiceKindV1::Opener, cid0), count: 10 },
                ],
            }],
        };
        assert!(model.validate().is_ok());
        let model_hash = fsa_lm::markov_model_artifact::put_markov_model_v1(&store, &model).unwrap();

        let base_out = root.join("answer_markov_plan_base.txt");
        let shaped_out = root.join("answer_markov_plan_shaped.txt");
        let rc0 = cmd_answer(&[
            "--root".to_string(),
            store_root.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--snapshot".to_string(),
            hex32(&snap_hash),
            "--pragmatics".to_string(),
            hex32(&prag_hash),
            "--out-file".to_string(),
            base_out.to_string_lossy().to_string(),
        ]);
        assert_eq!(rc0, 0);
        let rc1 = cmd_answer(&[
            "--root".to_string(),
            store_root.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--snapshot".to_string(),
            hex32(&snap_hash),
            "--pragmatics".to_string(),
            hex32(&prag_hash),
            "--markov-model".to_string(),
            hex32(&model_hash),
            "--out-file".to_string(),
            shaped_out.to_string_lossy().to_string(),
        ]);
        assert_eq!(rc1, 0);

        let base_text = std::fs::read_to_string(&base_out).unwrap();
        let shaped_text = std::fs::read_to_string(&shaped_out).unwrap();
        assert_eq!(
            plan_ref_lines_from_answer_text(&base_text),
            plan_ref_lines_from_answer_text(&shaped_text)
        );
    }

    #[test]
    fn cmd_answer_with_pragmatics_hides_directives_in_user_mode() {
        let root = tmp_dir("pragmatics");
        let store_root = root.join("store");
        let store = fsa_lm::artifact::FsArtifactStore::new(&store_root).unwrap();

        // Build a minimal FrameSegment + IndexSegment + IndexSnapshot.
        let terms = term_freqs_from_text("banana", TokenizerCfg::default());
        let mut row = FrameRowV1::new(DocId(Id64(1)), SourceId(Id64(1)));
        row.terms = terms;
        row.recompute_doc_len();
        let frame_seg = FrameSegmentV1::from_rows(&[row], 1024).unwrap();
        let frame_hash = put_frame_segment_v1(&store, &frame_seg).unwrap();

        let idx_seg = IndexSegmentV1::build_from_segment(frame_hash, &frame_seg).unwrap();
        let idx_hash = put_index_segment_v1(&store, &idx_seg).unwrap();

        let mut snap = IndexSnapshotV1::new(SourceId(Id64(1)));
        snap.entries.push(IndexSnapshotEntryV1 {
            frame_seg: frame_hash,
            index_seg: idx_hash,
            row_count: idx_seg.row_count,
            term_count: idx_seg.terms.len() as u32,
            postings_bytes: idx_seg.postings.len() as u32,
        });
        let snap_hash = put_index_snapshot_v1(&store, &snap).unwrap();

        // Prompt pack with a user message.
        let ids = PromptIds {
            snapshot_id: [0u8; 32],
            weights_id: [0u8; 32],
            tokenizer_id: [0u8; 32],
        };
        let mut pack = PromptPack::new(123, 256, ids);
        pack.messages.push(fsa_lm::prompt_pack::Message {
            role: Role::User,
            content: "banana".to_string(),
        });
        let prompt_hash = put_prompt_pack(&store, &mut pack, PromptLimits::default_v1()).unwrap();

        // Minimal pragmatics frame.
        let pf = fsa_lm::pragmatics_frame::PragmaticsFrameV1 {
            version: fsa_lm::pragmatics_frame::PRAGMATICS_FRAME_V1_VERSION,
            source_id: Id64(1),
            msg_ix: 0,
            byte_len: 12,
            ascii_only: 1,
            temperature: 0,
            valence: 0,
            arousal: 0,
            politeness: 800,
            formality: 200,
            directness: 700,
            empathy_need: 600,
            mode: fsa_lm::pragmatics_frame::RhetoricModeV1::Ask,
            flags: fsa_lm::pragmatics_frame::INTENT_FLAG_HAS_CODE,
            exclamations: 0,
            questions: 1,
            ellipses: 0,
            caps_words: 0,
            repeat_punct_runs: 0,
            quotes: 0,
            emphasis_score: 0,
            hedge_count: 0,
            intensifier_count: 0,
            profanity_count: 0,
            apology_count: 0,
            gratitude_count: 0,
            insult_count: 0,
        };
        let prag_hash = fsa_lm::pragmatics_frame_store::put_pragmatics_frame_v1(&store, &pf).unwrap();

        let out_path = root.join("answer.txt");
        let rc = cmd_answer(&[
            "--root".to_string(),
            store_root.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--snapshot".to_string(),
            hex32(&snap_hash),
            "--pragmatics".to_string(),
            hex32(&prag_hash),
            "--out-file".to_string(),
            out_path.to_string_lossy().to_string(),
        ]);
        assert_eq!(rc, 0);

        let s = std::fs::read_to_string(&out_path).unwrap();
        assert!(!s.contains("directives tone="));
        assert!(!s.contains("routing_trace top_hint="));
        assert!(!s.contains("query_id="));
        assert!(s.contains("[E0]"));
        assert!(
            s.contains("Based on:")
                || s.contains("Summary:")
                || s.contains("Steps:")
                || s.contains("Plan"),
            "output={}",
            s
        );
        assert!(s.contains("Sources") || s.contains("Evidence"), "output={}", s);
    }

    

    #[test]
    fn cmd_answer_with_empty_exemplar_memory_falls_back_cleanly() {
        let root = tmp_dir("exemplar_empty");
        let store_root = root.join("store");
        let store = fsa_lm::artifact::FsArtifactStore::new(&store_root).unwrap();

        let terms = term_freqs_from_text("banana", TokenizerCfg::default());
        let mut row = FrameRowV1::new(DocId(Id64(1)), SourceId(Id64(1)));
        row.terms = terms;
        row.recompute_doc_len();
        let frame_seg = FrameSegmentV1::from_rows(&[row], 1024).unwrap();
        let frame_hash = put_frame_segment_v1(&store, &frame_seg).unwrap();

        let idx_seg = IndexSegmentV1::build_from_segment(frame_hash, &frame_seg).unwrap();
        let idx_hash = put_index_segment_v1(&store, &idx_seg).unwrap();

        let mut snap = IndexSnapshotV1::new(SourceId(Id64(1)));
        snap.entries.push(IndexSnapshotEntryV1 {
            frame_seg: frame_hash,
            index_seg: idx_hash,
            row_count: idx_seg.row_count,
            term_count: idx_seg.terms.len() as u32,
            postings_bytes: idx_seg.postings.len() as u32,
        });
        let snap_hash = put_index_snapshot_v1(&store, &snap).unwrap();

        let ids = PromptIds {
            snapshot_id: [0u8; 32],
            weights_id: [0u8; 32],
            tokenizer_id: [0u8; 32],
        };
        let mut pack = PromptPack::new(123, 256, ids);
        pack.messages.push(fsa_lm::prompt_pack::Message {
            role: Role::User,
            content: "banana".to_string(),
        });
        let prompt_hash = put_prompt_pack(&store, &mut pack, PromptLimits::default_v1()).unwrap();

        let exemplar_memory = fsa_lm::exemplar_memory::ExemplarMemoryV1 {
            version: fsa_lm::exemplar_memory::EXEMPLAR_MEMORY_V1_VERSION,
            build_id: [0u8; 32],
            flags: 0,
            rows: Vec::new(),
        };
        let exemplar_hash = put_exemplar_memory_v1(&store, &exemplar_memory).unwrap();

        let out_path = root.join("answer.txt");
        let rc = cmd_answer(&[
            "--root".to_string(),
            store_root.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--snapshot".to_string(),
            hex32(&snap_hash),
            "--exemplar-memory".to_string(),
            hex32(&exemplar_hash),
            "--presentation".to_string(),
            "operator".to_string(),
            "--out-file".to_string(),
            out_path.to_string_lossy().to_string(),
        ]);
        assert_eq!(rc, 0);

        let s = std::fs::read_to_string(&out_path).unwrap();
        assert!(!s.contains("directives tone="));
        assert!(!s.contains("I can help with that. Based on the evidence, here is the clearest answer:"));
    }

    #[test]
    fn cmd_answer_with_exemplar_memory_adds_supportive_preface() {
        let root = tmp_dir("exemplar_supportive");
        let store_root = root.join("store");
        let store = fsa_lm::artifact::FsArtifactStore::new(&store_root).unwrap();

        let terms = term_freqs_from_text("banana", TokenizerCfg::default());
        let mut row = FrameRowV1::new(DocId(Id64(1)), SourceId(Id64(1)));
        row.terms = terms;
        row.recompute_doc_len();
        let frame_seg = FrameSegmentV1::from_rows(&[row], 1024).unwrap();
        let frame_hash = put_frame_segment_v1(&store, &frame_seg).unwrap();

        let idx_seg = IndexSegmentV1::build_from_segment(frame_hash, &frame_seg).unwrap();
        let idx_hash = put_index_segment_v1(&store, &idx_seg).unwrap();

        let mut snap = IndexSnapshotV1::new(SourceId(Id64(1)));
        snap.entries.push(IndexSnapshotEntryV1 {
            frame_seg: frame_hash,
            index_seg: idx_hash,
            row_count: idx_seg.row_count,
            term_count: idx_seg.terms.len() as u32,
            postings_bytes: idx_seg.postings.len() as u32,
        });
        let snap_hash = put_index_snapshot_v1(&store, &snap).unwrap();

        let ids = PromptIds {
            snapshot_id: [0u8; 32],
            weights_id: [0u8; 32],
            tokenizer_id: [0u8; 32],
        };
        let mut pack = PromptPack::new(123, 256, ids);
        pack.messages.push(fsa_lm::prompt_pack::Message {
            role: Role::User,
            content: "banana".to_string(),
        });
        let prompt_hash = put_prompt_pack(&store, &mut pack, PromptLimits::default_v1()).unwrap();

        let exemplar_memory = fsa_lm::exemplar_memory::ExemplarMemoryV1 {
            version: fsa_lm::exemplar_memory::EXEMPLAR_MEMORY_V1_VERSION,
            build_id: [0u8; 32],
            flags: 0,
            rows: vec![fsa_lm::exemplar_memory::ExemplarRowV1 {
                exemplar_id: Id64(7),
                response_mode: fsa_lm::exemplar_memory::ExemplarResponseModeV1::Direct,
                structure_kind: fsa_lm::exemplar_memory::ExemplarStructureKindV1::Direct,
                tone_kind: fsa_lm::exemplar_memory::ExemplarToneKindV1::Supportive,
                flags: 0,
                support_count: 1,
                support_refs: Vec::new(),
            }],
        };
        let exemplar_hash = put_exemplar_memory_v1(&store, &exemplar_memory).unwrap();

        let out_path = root.join("answer.txt");
        let rc = cmd_answer(&[
            "--root".to_string(),
            store_root.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--snapshot".to_string(),
            hex32(&snap_hash),
            "--exemplar-memory".to_string(),
            hex32(&exemplar_hash),
            "--presentation".to_string(),
            "operator".to_string(),
            "--out-file".to_string(),
            out_path.to_string_lossy().to_string(),
        ]);
        assert_eq!(rc, 0);

        let s = std::fs::read_to_string(&out_path).unwrap();
        assert!(s.contains("directives tone=Supportive style=Default"));
        assert!(s.contains("routing_trace top_hint="));
        assert!(s.contains("exemplar_match exemplar_id=7 response_mode=Direct structure=Direct tone=Supportive"));
        assert!(s.contains("reasons=mode,structure"));
        assert!(s.contains("I can help with that. Based on the evidence, here is the clearest answer:"));
    }

    #[test]
    fn cmd_answer_with_exemplar_summary_first_emits_match_line() {
        let root = tmp_dir("exemplar_summary_match");
        let store_root = root.join("store");
        let store = fsa_lm::artifact::FsArtifactStore::new(&store_root).unwrap();

        let terms = term_freqs_from_text("banana split", TokenizerCfg::default());
        let mut row = FrameRowV1::new(DocId(Id64(1)), SourceId(Id64(1)));
        row.terms = terms;
        row.recompute_doc_len();
        let frame_seg = FrameSegmentV1::from_rows(&[row], 1024).unwrap();
        let frame_hash = put_frame_segment_v1(&store, &frame_seg).unwrap();

        let idx_seg = IndexSegmentV1::build_from_segment(frame_hash, &frame_seg).unwrap();
        let idx_hash = put_index_segment_v1(&store, &idx_seg).unwrap();

        let mut snap = IndexSnapshotV1::new(SourceId(Id64(1)));
        snap.entries.push(IndexSnapshotEntryV1 {
            frame_seg: frame_hash,
            index_seg: idx_hash,
            row_count: idx_seg.row_count,
            term_count: idx_seg.terms.len() as u32,
            postings_bytes: idx_seg.postings.len() as u32,
        });
        let snap_hash = put_index_snapshot_v1(&store, &snap).unwrap();

        let ids = PromptIds {
            snapshot_id: [0u8; 32],
            weights_id: [0u8; 32],
            tokenizer_id: [0u8; 32],
        };
        let mut pack = PromptPack::new(123, 256, ids);
        pack.messages.push(fsa_lm::prompt_pack::Message {
            role: Role::User,
            content: "Summarize banana split briefly".to_string(),
        });
        let prompt_hash = put_prompt_pack(&store, &mut pack, PromptLimits::default_v1()).unwrap();

        let exemplar_memory = fsa_lm::exemplar_memory::ExemplarMemoryV1 {
            version: fsa_lm::exemplar_memory::EXEMPLAR_MEMORY_V1_VERSION,
            build_id: [0u8; 32],
            flags: 0,
            rows: vec![fsa_lm::exemplar_memory::ExemplarRowV1 {
                exemplar_id: Id64(11),
                response_mode: fsa_lm::exemplar_memory::ExemplarResponseModeV1::Summarize,
                structure_kind: fsa_lm::exemplar_memory::ExemplarStructureKindV1::SummaryFirst,
                tone_kind: fsa_lm::exemplar_memory::ExemplarToneKindV1::Neutral,
                flags: fsa_lm::exemplar_memory::EXROW_FLAG_HAS_SUMMARY,
                support_count: 3,
                support_refs: Vec::new(),
            }],
        };
        let exemplar_hash = put_exemplar_memory_v1(&store, &exemplar_memory).unwrap();

        let out_path = root.join("answer.txt");
        let rc = cmd_answer(&[
            "--root".to_string(),
            store_root.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--snapshot".to_string(),
            hex32(&snap_hash),
            "--exemplar-memory".to_string(),
            hex32(&exemplar_hash),
            "--presentation".to_string(),
            "operator".to_string(),
            "--out-file".to_string(),
            out_path.to_string_lossy().to_string(),
        ]);
        assert_eq!(rc, 0);

        let s = std::fs::read_to_string(&out_path).unwrap();
        assert!(s.contains("routing_trace top_hint="));
        assert!(s.contains("exemplar_match exemplar_id=11 response_mode=Summarize structure=SummaryFirst tone=Neutral"));
        assert!(s.contains("reasons=mode,structure,tone,summary"));
        assert!(s.contains("directives tone=Neutral style=Concise"));
    }

    #[test]
    fn cmd_answer_integrated_stack_preserves_grounding() {
        let root = tmp_dir("integrated_stack");
        let store_root = root.join("store");
        let store = fsa_lm::artifact::FsArtifactStore::new(&store_root).unwrap();

        let mut row0 = FrameRowV1::new(DocId(Id64(1)), SourceId(Id64(1)));
        row0.terms = term_freqs_from_text("banana split", TokenizerCfg::default());
        row0.recompute_doc_len();
        let mut row1 = FrameRowV1::new(DocId(Id64(2)), SourceId(Id64(1)));
        row1.terms = term_freqs_from_text("carrot dessert", TokenizerCfg::default());
        row1.recompute_doc_len();
        let frame_seg = FrameSegmentV1::from_rows(&[row0, row1], 1024).unwrap();
        let frame_hash = put_frame_segment_v1(&store, &frame_seg).unwrap();

        let idx_seg = IndexSegmentV1::build_from_segment(frame_hash, &frame_seg).unwrap();
        let idx_hash = put_index_segment_v1(&store, &idx_seg).unwrap();

        let mut snap = IndexSnapshotV1::new(SourceId(Id64(1)));
        snap.entries.push(IndexSnapshotEntryV1 {
            frame_seg: frame_hash,
            index_seg: idx_hash,
            row_count: idx_seg.row_count,
            term_count: idx_seg.terms.len() as u32,
            postings_bytes: idx_seg.postings.len() as u32,
        });
        let snap_hash = put_index_snapshot_v1(&store, &snap).unwrap();

        let ids = PromptIds {
            snapshot_id: [0u8; 32],
            weights_id: [0u8; 32],
            tokenizer_id: [0u8; 32],
        };
        let mut pack = PromptPack::new(123, 256, ids);
        pack.messages.push(fsa_lm::prompt_pack::Message {
            role: Role::User,
            content: "Summarize banana split briefly".to_string(),
        });
        let prompt_hash = put_prompt_pack(&store, &mut pack, PromptLimits::default_v1()).unwrap();

        let pf = fsa_lm::pragmatics_frame::PragmaticsFrameV1 {
            version: fsa_lm::pragmatics_frame::PRAGMATICS_FRAME_V1_VERSION,
            source_id: Id64(1),
            msg_ix: 0,
            byte_len: 30,
            ascii_only: 1,
            temperature: 0,
            valence: 0,
            arousal: 0,
            politeness: 800,
            formality: 200,
            directness: 300,
            empathy_need: 800,
            mode: fsa_lm::pragmatics_frame::RhetoricModeV1::Ask,
            flags: 0,
            exclamations: 0,
            questions: 1,
            ellipses: 0,
            caps_words: 0,
            repeat_punct_runs: 0,
            quotes: 0,
            emphasis_score: 0,
            hedge_count: 0,
            intensifier_count: 0,
            profanity_count: 0,
            apology_count: 0,
            gratitude_count: 0,
            insult_count: 0,
        };
        let prag_hash = fsa_lm::pragmatics_frame_store::put_pragmatics_frame_v1(&store, &pf).unwrap();

        let banana = term_id_from_token("banana", TokenizerCfg::default());
        let carrot = term_id_from_token("carrot", TokenizerCfg::default());
        let graph = fsa_lm::graph_relevance::GraphRelevanceV1 {
            version: fsa_lm::graph_relevance::GRAPH_RELEVANCE_V1_VERSION,
            build_id: fsa_lm::hash::blake3_hash(b"integrated-graph"),
            flags: fsa_lm::graph_relevance::GR_FLAG_HAS_TERM_ROWS,
            rows: vec![fsa_lm::graph_relevance::GraphRelevanceRowV1 {
                seed_kind: fsa_lm::graph_relevance::GraphNodeKindV1::Term,
                seed_id: banana.0,
                edges: vec![fsa_lm::graph_relevance::GraphRelevanceEdgeV1::new(
                    fsa_lm::graph_relevance::GraphNodeKindV1::Term,
                    carrot.0,
                    20_000,
                    1,
                    fsa_lm::graph_relevance::GREDGE_FLAG_SYMMETRIC,
                )],
            }],
        };
        let graph_hash = fsa_lm::graph_relevance_artifact::put_graph_relevance_v1(&store, &graph).unwrap();

        let exemplar_memory = fsa_lm::exemplar_memory::ExemplarMemoryV1 {
            version: fsa_lm::exemplar_memory::EXEMPLAR_MEMORY_V1_VERSION,
            build_id: [0u8; 32],
            flags: 0,
            rows: vec![fsa_lm::exemplar_memory::ExemplarRowV1 {
                exemplar_id: Id64(21),
                response_mode: fsa_lm::exemplar_memory::ExemplarResponseModeV1::Summarize,
                structure_kind: fsa_lm::exemplar_memory::ExemplarStructureKindV1::SummaryFirst,
                tone_kind: fsa_lm::exemplar_memory::ExemplarToneKindV1::Supportive,
                flags: fsa_lm::exemplar_memory::EXROW_FLAG_HAS_SUMMARY,
                support_count: 2,
                support_refs: Vec::new(),
            }],
        };
        let exemplar_hash = put_exemplar_memory_v1(&store, &exemplar_memory).unwrap();

        use fsa_lm::markov_model::{
            MarkovModelV1, MarkovNextV1, MarkovStateV1, MARKOV_MODEL_V1_VERSION,
        };
        let cid0 = fsa_lm::frame::derive_id64(b"markov_choice_v1", b"preface:supportive:0");
        let cid1 = fsa_lm::frame::derive_id64(b"markov_choice_v1", b"preface:supportive:1");
        let model = MarkovModelV1 {
            version: MARKOV_MODEL_V1_VERSION,
            order_n_max: 3,
            max_next_per_state: 8,
            total_transitions: 30,
            corpus_hash: [0u8; 32],
            states: vec![MarkovStateV1 {
                context: Vec::new(),
                escape_count: 0,
                next: vec![
                    MarkovNextV1 {
                        token: MarkovTokenV1::new(MarkovChoiceKindV1::Opener, cid1),
                        count: 20,
                    },
                    MarkovNextV1 {
                        token: MarkovTokenV1::new(MarkovChoiceKindV1::Opener, cid0),
                        count: 10,
                    },
                ],
            }],
        };
        assert!(model.validate().is_ok());
        let model_hash = fsa_lm::markov_model_artifact::put_markov_model_v1(&store, &model).unwrap();

        let base_out_path = root.join("answer_base.txt");
        let rc = cmd_answer(&[
            "--root".to_string(),
            store_root.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--snapshot".to_string(),
            hex32(&snap_hash),
            "--pragmatics".to_string(),
            hex32(&prag_hash),
            "--expand".to_string(),
            "--graph-relevance".to_string(),
            hex32(&graph_hash),
            "--out-file".to_string(),
            base_out_path.to_string_lossy().to_string(),
        ]);
        assert_eq!(rc, 0);
        let base_text = std::fs::read_to_string(&base_out_path).unwrap();

        let shaped_out_path = root.join("answer_shaped.txt");
        let rc = cmd_answer(&[
            "--root".to_string(),
            store_root.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--snapshot".to_string(),
            hex32(&snap_hash),
            "--pragmatics".to_string(),
            hex32(&prag_hash),
            "--expand".to_string(),
            "--presentation".to_string(),
            "operator".to_string(),
            "--graph-relevance".to_string(),
            hex32(&graph_hash),
            "--exemplar-memory".to_string(),
            hex32(&exemplar_hash),
            "--markov-model".to_string(),
            hex32(&model_hash),
            "--out-file".to_string(),
            shaped_out_path.to_string_lossy().to_string(),
        ]);
        assert_eq!(rc, 0);

        let shaped_text = std::fs::read_to_string(&shaped_out_path).unwrap();
        assert!(shaped_text.contains("routing_trace top_hint="));
        assert!(shaped_text.contains("graph_trace seeds=1 candidates=1 reasons=banana:"));
        assert!(shaped_text.contains(
            "exemplar_match exemplar_id=21 response_mode=Summarize structure=SummaryFirst tone=Supportive"
        ));
        assert!(shaped_text.contains("Happy to help. Based on the evidence, here is the clearest answer:"));
        assert!(!shaped_text.contains("I can help with that. Based on the evidence, here is the clearest answer:"));
        assert_eq!(
            evidence_lines_from_answer_text(&base_text),
            evidence_lines_from_answer_text(&shaped_text)
        );
        assert_eq!(
            plan_ref_lines_from_answer_text(&base_text),
            plan_ref_lines_from_answer_text(&shaped_text)
        );
    }

    #[test]
    fn cmd_answer_markov_trace_records_preface_choice_first() {
        let root = tmp_dir("trace_preface");
        let store_root = root.join("store");
        let store = fsa_lm::artifact::FsArtifactStore::new(&store_root).unwrap();

        // Build a minimal FrameSegment + IndexSegment + IndexSnapshot.
        let terms = term_freqs_from_text("banana", TokenizerCfg::default());
        let mut row = FrameRowV1::new(DocId(Id64(1)), SourceId(Id64(1)));
        row.terms = terms;
        row.recompute_doc_len();
        let frame_seg = FrameSegmentV1::from_rows(&[row], 1024).unwrap();
        let frame_hash = put_frame_segment_v1(&store, &frame_seg).unwrap();

        let idx_seg = IndexSegmentV1::build_from_segment(frame_hash, &frame_seg).unwrap();
        let idx_hash = put_index_segment_v1(&store, &idx_seg).unwrap();

        let mut snap = IndexSnapshotV1::new(SourceId(Id64(1)));
        snap.entries.push(IndexSnapshotEntryV1 {
            frame_seg: frame_hash,
            index_seg: idx_hash,
            row_count: idx_seg.row_count,
            term_count: idx_seg.terms.len() as u32,
            postings_bytes: idx_seg.postings.len() as u32,
        });
        let snap_hash = put_index_snapshot_v1(&store, &snap).unwrap();

        // Prompt pack with a user message.
        let ids = PromptIds {
            snapshot_id: [0u8; 32],
            weights_id: [0u8; 32],
            tokenizer_id: [0u8; 32],
        };
        let mut pack = PromptPack::new(123, 256, ids);
        pack.messages.push(fsa_lm::prompt_pack::Message {
            role: Role::User,
            content: "banana".to_string(),
        });
        let prompt_hash = put_prompt_pack(&store, &mut pack, PromptLimits::default_v1()).unwrap();

        // Pragmatics frame that yields supportive tone (so a preface line is emitted).
        let pf = fsa_lm::pragmatics_frame::PragmaticsFrameV1 {
            version: fsa_lm::pragmatics_frame::PRAGMATICS_FRAME_V1_VERSION,
            source_id: Id64(1),
            msg_ix: 0,
            byte_len: 12,
            ascii_only: 1,
            temperature: 0,
            valence: 0,
            arousal: 0,
            politeness: 800,
            formality: 200,
            directness: 300,
            empathy_need: 800,
            mode: fsa_lm::pragmatics_frame::RhetoricModeV1::Ask,
            flags: 0,
            exclamations: 0,
            questions: 1,
            ellipses: 0,
            caps_words: 0,
            repeat_punct_runs: 0,
            quotes: 0,
            emphasis_score: 0,
            hedge_count: 0,
            intensifier_count: 0,
            profanity_count: 0,
            apology_count: 0,
            gratitude_count: 0,
            insult_count: 0,
        };
        let prag_hash = fsa_lm::pragmatics_frame_store::put_pragmatics_frame_v1(&store, &pf).unwrap();

        let out_path = root.join("answer.txt");
        let rc = cmd_answer(&[
            "--root".to_string(),
            store_root.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--snapshot".to_string(),
            hex32(&snap_hash),
            "--pragmatics".to_string(),
            hex32(&prag_hash),
            "--presentation".to_string(),
            "operator".to_string(),
            "--out-file".to_string(),
            out_path.to_string_lossy().to_string(),
        ]);
        assert_eq!(rc, 0);

        let s = std::fs::read_to_string(&out_path).unwrap();
        let _qid = parse_query_id_from_answer_text(&s);
        let tone = parse_tone_from_answer_text(&s);
        let style = parse_style_from_answer_text(&s);

        let answer_hash = fsa_lm::hash::blake3_hash(s.as_bytes());
        let mt_hash = find_markov_trace_hash_for_answer(&store_root, answer_hash);
        let trace = fsa_lm::markov_trace_artifact::get_markov_trace_v1(&store, &mt_hash).unwrap().unwrap();
        assert!(!trace.tokens.is_empty());

        // If the realizer emits an opener preface line, the MarkovTrace must start
        // with the corresponding preface:<tone>:<variant> token. After that, the
        // next token may be either the first structural plan token or the bounded
        // details-heading transition token introduced by the conversation sprint.
        assert_eq!(tone, fsa_lm::realizer_directives::ToneV1::Supportive);
        assert_eq!(style, fsa_lm::realizer_directives::StyleV1::Default);

        let preface_line_v0 = "I can help with that. Based on the evidence, here is the clearest answer:";
        assert!(s.contains(preface_line_v0));

        assert!(trace.tokens.len() >= 2);

        let preface_cid = fsa_lm::frame::derive_id64(b"markov_choice_v1", b"preface:supportive:0");
        assert_eq!(
            trace.tokens[0],
            MarkovTokenV1::new(MarkovChoiceKindV1::Opener, preface_cid)
        );

        let cid_transition0 =
            fsa_lm::frame::derive_id64(b"markov_choice_v1", b"transition:details_heading:0");
        let cid_transition1 =
            fsa_lm::frame::derive_id64(b"markov_choice_v1", b"transition:details_heading:1");
        let cid_summary = fsa_lm::frame::derive_id64(b"markov_choice_v1", b"plan_item:summary");
        let cid_bullet = fsa_lm::frame::derive_id64(b"markov_choice_v1", b"plan_item:bullet");
        let cid_step = fsa_lm::frame::derive_id64(b"markov_choice_v1", b"plan_item:step");
        let cid_caveat = fsa_lm::frame::derive_id64(b"markov_choice_v1", b"plan_item:caveat");

        let next = trace.tokens[1].choice_id;
        assert!(
            next == cid_transition0
                || next == cid_transition1
                || next == cid_summary
                || next == cid_bullet
                || next == cid_step
                || next == cid_caveat,
            "unexpected markov token id after preface"
        );
    }



    #[test]
    fn cmd_answer_with_markov_model_selects_preface_variant1_and_trace() {
        let root = tmp_dir("trace_preface_markov1");
        let store_root = root.join("store");
        let store = fsa_lm::artifact::FsArtifactStore::new(&store_root).unwrap();

        // Build a minimal FrameSegment + IndexSegment + IndexSnapshot.
        let terms = term_freqs_from_text("banana", TokenizerCfg::default());
        let mut row = FrameRowV1::new(DocId(Id64(1)), SourceId(Id64(1)));
        row.terms = terms;
        row.recompute_doc_len();
        let frame_seg = FrameSegmentV1::from_rows(&[row], 1024).unwrap();
        let frame_hash = put_frame_segment_v1(&store, &frame_seg).unwrap();

        let idx_seg = IndexSegmentV1::build_from_segment(frame_hash, &frame_seg).unwrap();
        let idx_hash = put_index_segment_v1(&store, &idx_seg).unwrap();

        let mut snap = IndexSnapshotV1::new(SourceId(Id64(1)));
        snap.entries.push(IndexSnapshotEntryV1 {
            frame_seg: frame_hash,
            index_seg: idx_hash,
            row_count: idx_seg.row_count,
            term_count: idx_seg.terms.len() as u32,
            postings_bytes: idx_seg.postings.len() as u32,
        });
        let snap_hash = put_index_snapshot_v1(&store, &snap).unwrap();

        // Prompt pack with a user message.
        let ids = PromptIds {
            snapshot_id: [0u8; 32],
            weights_id: [0u8; 32],
            tokenizer_id: [0u8; 32],
        };
        let mut pack = PromptPack::new(123, 256, ids);
        pack.messages.push(fsa_lm::prompt_pack::Message {
            role: Role::User,
            content: "banana".to_string(),
        });
        let prompt_hash = put_prompt_pack(&store, &mut pack, PromptLimits::default_v1()).unwrap();

        // Pragmatics frame that yields supportive tone (so a preface line is emitted).
        let pf = fsa_lm::pragmatics_frame::PragmaticsFrameV1 {
            version: fsa_lm::pragmatics_frame::PRAGMATICS_FRAME_V1_VERSION,
            source_id: Id64(1),
            msg_ix: 0,
            byte_len: 12,
            ascii_only: 1,
            temperature: 0,
            valence: 0,
            arousal: 0,
            politeness: 800,
            formality: 200,
            directness: 300,
            empathy_need: 800,
            mode: fsa_lm::pragmatics_frame::RhetoricModeV1::Ask,
            flags: 0,
            exclamations: 0,
            questions: 1,
            ellipses: 0,
            caps_words: 0,
            repeat_punct_runs: 0,
            quotes: 0,
            emphasis_score: 0,
            hedge_count: 0,
            intensifier_count: 0,
            profanity_count: 0,
            apology_count: 0,
            gratitude_count: 0,
            insult_count: 0,
        };
        let prag_hash = fsa_lm::pragmatics_frame_store::put_pragmatics_frame_v1(&store, &pf).unwrap();

        // Store a MarkovModelV1 whose unconditional state prefers the supportive
        // alternate preface template (variant 1).
        {
            use fsa_lm::markov_model::{
                MarkovModelV1, MarkovNextV1, MarkovStateV1, MARKOV_MODEL_V1_VERSION,
            };
            let cid0 = fsa_lm::frame::derive_id64(b"markov_choice_v1", b"preface:supportive:0");
            let cid1 = fsa_lm::frame::derive_id64(b"markov_choice_v1", b"preface:supportive:1");
            let s0 = MarkovStateV1 {
                context: Vec::new(),
                escape_count: 0,
                next: vec![
                    MarkovNextV1 {
                        token: MarkovTokenV1::new(MarkovChoiceKindV1::Opener, cid1),
                        count: 20,
                    },
                    MarkovNextV1 {
                        token: MarkovTokenV1::new(MarkovChoiceKindV1::Opener, cid0),
                        count: 10,
                    },
                ],
            };
            let model = MarkovModelV1 {
                version: MARKOV_MODEL_V1_VERSION,
                order_n_max: 3,
                max_next_per_state: 8,
                total_transitions: 30,
                corpus_hash: [0u8; 32],
                states: vec![s0],
            };
            assert!(model.validate().is_ok());
            let model_hash = fsa_lm::markov_model_artifact::put_markov_model_v1(&store, &model).unwrap();

            let out_path = root.join("answer.txt");
            let rc = cmd_answer(&[
                "--root".to_string(),
                store_root.to_string_lossy().to_string(),
                "--prompt".to_string(),
                hex32(&prompt_hash),
                "--snapshot".to_string(),
                hex32(&snap_hash),
                "--pragmatics".to_string(),
                hex32(&prag_hash),
                "--markov-model".to_string(),
                hex32(&model_hash),
                "--presentation".to_string(),
                "operator".to_string(),
                "--out-file".to_string(),
                out_path.to_string_lossy().to_string(),
            ]);
            assert_eq!(rc, 0);

            let s = std::fs::read_to_string(&out_path).unwrap();
            assert!(s.contains("directives tone=Supportive"));

            let preface_v1 = "Happy to help. Based on the evidence, here is the clearest answer:";
            let preface_v0 = "I can help with that. Based on the evidence, here is the clearest answer:";
            assert!(s.contains(preface_v1));
            assert!(!s.contains(preface_v0));

            let answer_hash = fsa_lm::hash::blake3_hash(s.as_bytes());
            let mt_hash = find_markov_trace_hash_for_answer(&store_root, answer_hash);
            let trace = fsa_lm::markov_trace_artifact::get_markov_trace_v1(&store, &mt_hash)
                .unwrap()
                .unwrap();
            assert!(!trace.tokens.is_empty());

            let preface_cid = fsa_lm::frame::derive_id64(b"markov_choice_v1", b"preface:supportive:1");
            assert_eq!(
                trace.tokens[0],
                MarkovTokenV1::new(MarkovChoiceKindV1::Opener, preface_cid)
            );
        }
    }

    #[test]
    fn cmd_answer_workspace_markov_keeps_grounding_and_refs() {
        let root = tmp_dir("workspace_markov_grounding_lock");
        let store_root = root.join("store");
        let store = fsa_lm::artifact::FsArtifactStore::new(&store_root).unwrap();

        let terms = term_freqs_from_text("banana", TokenizerCfg::default());
        let mut row = FrameRowV1::new(DocId(Id64(1)), SourceId(Id64(1)));
        row.terms = terms;
        row.recompute_doc_len();
        let frame_seg = FrameSegmentV1::from_rows(&[row], 1024).unwrap();
        let frame_hash = put_frame_segment_v1(&store, &frame_seg).unwrap();
        let idx_seg = IndexSegmentV1::build_from_segment(frame_hash, &frame_seg).unwrap();
        let idx_hash = put_index_segment_v1(&store, &idx_seg).unwrap();
        let mut snap = IndexSnapshotV1::new(SourceId(Id64(1)));
        snap.entries.push(IndexSnapshotEntryV1 {
            frame_seg: frame_hash,
            index_seg: idx_hash,
            row_count: idx_seg.row_count,
            term_count: idx_seg.terms.len() as u32,
            postings_bytes: idx_seg.postings.len() as u32,
        });
        let snap_hash = put_index_snapshot_v1(&store, &snap).unwrap();

        let ids = PromptIds {
            snapshot_id: [0u8; 32],
            weights_id: [0u8; 32],
            tokenizer_id: [0u8; 32],
        };
        let mut pack = PromptPack::new(123, 256, ids);
        pack.messages.push(fsa_lm::prompt_pack::Message {
            role: Role::User,
            content: "banana".to_string(),
        });
        let prompt_hash = put_prompt_pack(&store, &mut pack, PromptLimits::default_v1()).unwrap();

        let pf = fsa_lm::pragmatics_frame::PragmaticsFrameV1 {
            version: fsa_lm::pragmatics_frame::PRAGMATICS_FRAME_V1_VERSION,
            source_id: Id64(1),
            msg_ix: 0,
            byte_len: 12,
            ascii_only: 1,
            temperature: 0,
            valence: 0,
            arousal: 0,
            politeness: 800,
            formality: 200,
            directness: 300,
            empathy_need: 800,
            mode: fsa_lm::pragmatics_frame::RhetoricModeV1::Ask,
            flags: 0,
            exclamations: 0,
            questions: 1,
            ellipses: 0,
            caps_words: 0,
            repeat_punct_runs: 0,
            quotes: 0,
            emphasis_score: 0,
            hedge_count: 0,
            intensifier_count: 0,
            profanity_count: 0,
            apology_count: 0,
            gratitude_count: 0,
            insult_count: 0,
        };
        let prag_hash = fsa_lm::pragmatics_frame_store::put_pragmatics_frame_v1(&store, &pf).unwrap();

        use fsa_lm::markov_model::{
            MarkovModelV1, MarkovNextV1, MarkovStateV1, MARKOV_MODEL_V1_VERSION,
        };
        let cid0 = fsa_lm::frame::derive_id64(b"markov_choice_v1", b"preface:supportive:0");
        let cid1 = fsa_lm::frame::derive_id64(b"markov_choice_v1", b"preface:supportive:1");
        let model = MarkovModelV1 {
            version: MARKOV_MODEL_V1_VERSION,
            order_n_max: 3,
            max_next_per_state: 8,
            total_transitions: 30,
            corpus_hash: [0u8; 32],
            states: vec![MarkovStateV1 {
                context: Vec::new(),
                escape_count: 0,
                next: vec![
                    MarkovNextV1 {
                        token: MarkovTokenV1::new(MarkovChoiceKindV1::Opener, cid1),
                        count: 20,
                    },
                    MarkovNextV1 {
                        token: MarkovTokenV1::new(MarkovChoiceKindV1::Opener, cid0),
                        count: 10,
                    },
                ],
            }],
        };
        assert!(model.validate().is_ok());
        let model_hash = fsa_lm::markov_model_artifact::put_markov_model_v1(&store, &model).unwrap();

        let base_out = root.join("answer_workspace_markov_base.txt");
        let rc0 = cmd_answer(&[
            "--root".to_string(),
            store_root.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--snapshot".to_string(),
            hex32(&snap_hash),
            "--pragmatics".to_string(),
            hex32(&prag_hash),
            "--presentation".to_string(),
            "operator".to_string(),
            "--out-file".to_string(),
            base_out.to_string_lossy().to_string(),
        ]);
        assert_eq!(rc0, 0);

        let ws_text = format!("markov_model={}\n", hex32(&model_hash));
        std::fs::write(store_root.join(WORKSPACE_V1_FILENAME), ws_text.as_bytes()).unwrap();

        let shaped_out = root.join("answer_workspace_markov_shaped.txt");
        let rc1 = cmd_answer(&[
            "--root".to_string(),
            store_root.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--snapshot".to_string(),
            hex32(&snap_hash),
            "--pragmatics".to_string(),
            hex32(&prag_hash),
            "--presentation".to_string(),
            "operator".to_string(),
            "--out-file".to_string(),
            shaped_out.to_string_lossy().to_string(),
        ]);
        assert_eq!(rc1, 0);

        let base_text = std::fs::read_to_string(&base_out).unwrap();
        let shaped_text = std::fs::read_to_string(&shaped_out).unwrap();
        assert_eq!(
            evidence_lines_from_answer_text(&base_text),
            evidence_lines_from_answer_text(&shaped_text)
        );
        assert_eq!(
            plan_ref_lines_from_answer_text(&base_text),
            plan_ref_lines_from_answer_text(&shaped_text)
        );
    }

    #[test]
    fn cmd_answer_uses_workspace_markov_model_when_flag_is_omitted() {
        let root = tmp_dir("workspace_markov_default");
        let store_root = root.join("store");
        let store = fsa_lm::artifact::FsArtifactStore::new(&store_root).unwrap();

        let terms = term_freqs_from_text("banana", TokenizerCfg::default());
        let mut row = FrameRowV1::new(DocId(Id64(1)), SourceId(Id64(1)));
        row.terms = terms;
        row.recompute_doc_len();
        let frame_seg = FrameSegmentV1::from_rows(&[row], 1024).unwrap();
        let frame_hash = put_frame_segment_v1(&store, &frame_seg).unwrap();

        let idx_seg = IndexSegmentV1::build_from_segment(frame_hash, &frame_seg).unwrap();
        let idx_hash = put_index_segment_v1(&store, &idx_seg).unwrap();

        let mut snap = IndexSnapshotV1::new(SourceId(Id64(1)));
        snap.entries.push(IndexSnapshotEntryV1 {
            frame_seg: frame_hash,
            index_seg: idx_hash,
            row_count: idx_seg.row_count,
            term_count: idx_seg.terms.len() as u32,
            postings_bytes: idx_seg.postings.len() as u32,
        });
        let snap_hash = put_index_snapshot_v1(&store, &snap).unwrap();

        let ids = PromptIds {
            snapshot_id: [0u8; 32],
            weights_id: [0u8; 32],
            tokenizer_id: [0u8; 32],
        };
        let mut pack = PromptPack::new(123, 256, ids);
        pack.messages.push(fsa_lm::prompt_pack::Message {
            role: Role::User,
            content: "banana".to_string(),
        });
        let prompt_hash = put_prompt_pack(&store, &mut pack, PromptLimits::default_v1()).unwrap();

        let pf = fsa_lm::pragmatics_frame::PragmaticsFrameV1 {
            version: fsa_lm::pragmatics_frame::PRAGMATICS_FRAME_V1_VERSION,
            source_id: Id64(1),
            msg_ix: 0,
            byte_len: 12,
            ascii_only: 1,
            temperature: 0,
            valence: 0,
            arousal: 0,
            politeness: 800,
            formality: 200,
            directness: 300,
            empathy_need: 800,
            mode: fsa_lm::pragmatics_frame::RhetoricModeV1::Ask,
            flags: 0,
            exclamations: 0,
            questions: 1,
            ellipses: 0,
            caps_words: 0,
            repeat_punct_runs: 0,
            quotes: 0,
            emphasis_score: 0,
            hedge_count: 0,
            intensifier_count: 0,
            profanity_count: 0,
            apology_count: 0,
            gratitude_count: 0,
            insult_count: 0,
        };
        let prag_hash = fsa_lm::pragmatics_frame_store::put_pragmatics_frame_v1(&store, &pf).unwrap();

        use fsa_lm::markov_model::{
            MarkovModelV1, MarkovNextV1, MarkovStateV1, MARKOV_MODEL_V1_VERSION,
        };
        let cid0 = fsa_lm::frame::derive_id64(b"markov_choice_v1", b"preface:supportive:0");
        let cid1 = fsa_lm::frame::derive_id64(b"markov_choice_v1", b"preface:supportive:1");
        let s0 = MarkovStateV1 {
            context: Vec::new(),
            escape_count: 0,
            next: vec![
                MarkovNextV1 {
                    token: MarkovTokenV1::new(MarkovChoiceKindV1::Opener, cid1),
                    count: 20,
                },
                MarkovNextV1 {
                    token: MarkovTokenV1::new(MarkovChoiceKindV1::Opener, cid0),
                    count: 10,
                },
            ],
        };
        let model = MarkovModelV1 {
            version: MARKOV_MODEL_V1_VERSION,
            order_n_max: 3,
            max_next_per_state: 8,
            total_transitions: 30,
            corpus_hash: [0u8; 32],
            states: vec![s0],
        };
        assert!(model.validate().is_ok());
        let model_hash = fsa_lm::markov_model_artifact::put_markov_model_v1(&store, &model).unwrap();

        let ws_text = format!("markov_model={}\n", hex32(&model_hash));
        std::fs::write(store_root.join(WORKSPACE_V1_FILENAME), ws_text.as_bytes()).unwrap();

        let out_path = root.join("answer_workspace_markov.txt");
        let rc = cmd_answer(&[
            "--root".to_string(),
            store_root.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--snapshot".to_string(),
            hex32(&snap_hash),
            "--pragmatics".to_string(),
            hex32(&prag_hash),
            "--presentation".to_string(),
            "operator".to_string(),
            "--out-file".to_string(),
            out_path.to_string_lossy().to_string(),
        ]);
        assert_eq!(rc, 0);

        let s = std::fs::read_to_string(&out_path).unwrap();
        let preface_v1 = "Happy to help. Based on the evidence, here is the clearest answer:";
        let preface_v0 = "I can help with that. Based on the evidence, here is the clearest answer:";
        assert!(s.contains(preface_v1));
        assert!(!s.contains(preface_v0));

        let answer_hash = fsa_lm::hash::blake3_hash(s.as_bytes());
        let mt_hash = find_markov_trace_hash_for_answer(&store_root, answer_hash);
        let trace = fsa_lm::markov_trace_artifact::get_markov_trace_v1(&store, &mt_hash)
            .unwrap()
            .unwrap();
        let preface_cid = fsa_lm::frame::derive_id64(b"markov_choice_v1", b"preface:supportive:1");
        assert_eq!(
            trace.tokens[0],
            MarkovTokenV1::new(MarkovChoiceKindV1::Opener, preface_cid)
        );
    }

    #[test]
    fn cmd_answer_expand_missing_snapshot_in_store_fails() {
        let root = tmp_dir("expand_missing");
        let store_root = root.join("store");
        let store = fsa_lm::artifact::FsArtifactStore::new(&store_root).unwrap();

        // Minimal FrameSegment + IndexSnapshot as in the smoke test.
        let terms = term_freqs_from_text("banana", TokenizerCfg::default());
        let mut row = FrameRowV1::new(DocId(Id64(1)), SourceId(Id64(1)));
        row.terms = terms;
        row.recompute_doc_len();
        let frame_seg = FrameSegmentV1::from_rows(&[row], 1024).unwrap();
        let frame_hash = put_frame_segment_v1(&store, &frame_seg).unwrap();

        let idx_seg = IndexSegmentV1::build_from_segment(frame_hash, &frame_seg).unwrap();
        let idx_hash = put_index_segment_v1(&store, &idx_seg).unwrap();

        let mut snap = IndexSnapshotV1::new(SourceId(Id64(1)));
        snap.entries.push(IndexSnapshotEntryV1 {
            frame_seg: frame_hash,
            index_seg: idx_hash,
            row_count: idx_seg.row_count,
            term_count: idx_seg.terms.len() as u32,
            postings_bytes: idx_seg.postings.len() as u32,
        });
        let snap_hash = put_index_snapshot_v1(&store, &snap).unwrap();

        // Prompt pack with a user message.
        let ids = PromptIds {
            snapshot_id: [0u8; 32],
            weights_id: [0u8; 32],
            tokenizer_id: [0u8; 32],
        };
        let mut pack = PromptPack::new(123, 256, ids);
        pack.messages.push(fsa_lm::prompt_pack::Message {
            role: Role::User,
            content: "banana".to_string(),
        });
        let prompt_hash = put_prompt_pack(&store, &mut pack, PromptLimits::default_v1()).unwrap();

        // Enable expansion but point at a lexicon snapshot hash that is not in the store.
        let missing_lex: Hash32 = [1u8; 32];

        let out_path = root.join("answer.txt");
        let rc = cmd_answer(&[
            "--root".to_string(),
            store_root.to_string_lossy().to_string(),
            "--prompt".to_string(),
            hex32(&prompt_hash),
            "--snapshot".to_string(),
            hex32(&snap_hash),
            "--expand".to_string(),
            "--lexicon-snapshot".to_string(),
            hex32(&missing_lex),
            "--out-file".to_string(),
            out_path.to_string_lossy().to_string(),
        ]);
        assert_eq!(rc, 1);
    }
}

fn cmd_query_index(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut snapshot_hex: Option<String> = None;
    let mut sig_map_hex: Option<String> = None;
    let mut text: Option<String> = None;
    let mut k: usize = 10;
    let mut include_meta: bool = false;
    let mut cache_stats: bool = false;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--snapshot" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --snapshot value");
                    return 2;
                }
                snapshot_hex = Some(args[i].clone());
            }
            "--sig-map" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --sig-map value");
                    return 2;
                }
                sig_map_hex = Some(args[i].clone());
            }
            "--text" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --text value");
                    return 2;
                }
                text = Some(args[i].clone());
            }
            "--k" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --k value");
                    return 2;
                }
                match args[i].parse::<usize>() {
                    Ok(v) if v > 0 => k = v,
                    _ => {
                        eprintln!("bad --k value");
                        return 2;
                    }
                }
            }
            "--meta" => {
                include_meta = true;
            }
            "--cache-stats" => {
                cache_stats = true;
            }
            _ => {
                eprintln!("unknown arg: {}", args[i]);
                return 2;
            }
        }
        i += 1;
    }

    let snap_hex = match snapshot_hex {
        Some(x) => x,
        None => {
            eprintln!("missing --snapshot");
            return 2;
        }
    };
    let qtext = match text {
        Some(x) => x,
        None => {
            eprintln!("missing --text");
            return 2;
        }
    };

    let snap_hash = match parse_hash32_hex(&snap_hex) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("bad snapshot hash: {}", e);
            return 2;
        }
    };

    let sig_map_hash: Option<Hash32> = match sig_map_hex {
        Some(x) => match parse_hash32_hex(&x) {
            Ok(h) => Some(h),
            Err(e) => {
                eprintln!("bad sig-map hash: {}", e);
                return 2;
            }
        },
        None => None,
    };

    let store = match FsArtifactStore::new(&root) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("store error: {}", e);
            return 1;
        }
    };

    // Build the query-id blob for retrieve-v1 so ReplayLog steps are fully hash-addressed.
    let k_u32 = if k > (u32::MAX as usize) { u32::MAX } else { k as u32 };
    let entry_cap_u32: u32 = 0;
    let dense_row_threshold: u32 = 200_000;

    let mut qid_bytes: Vec<u8> = Vec::new();
    qid_bytes.extend_from_slice(b"retrieve-v1\0");
    qid_bytes.push(if include_meta { 1 } else { 0 });
    qid_bytes.extend_from_slice(&k_u32.to_le_bytes());
    qid_bytes.extend_from_slice(&entry_cap_u32.to_le_bytes());
    qid_bytes.extend_from_slice(&dense_row_threshold.to_le_bytes());
    qid_bytes.extend_from_slice(qtext.as_bytes());
    let query_id = blake3_hash(&qid_bytes);

    // Store the query-id blob as an artifact so ReplayLog steps can reference it.
    let qid_hash = match store.put(&qid_bytes) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("store error: {}", e);
            return 1;
        }
    };
    if qid_hash != query_id {
        eprintln!("internal error: query-id hash mismatch");
        return 1;
    }

    let mut qcfg = QueryTermsCfg::new();
    qcfg.include_metaphone = include_meta;
    let scfg = SearchCfg { k, entry_cap: entry_cap_u32 as usize, dense_row_threshold };

    let qterms = query_terms_from_text(&qtext, &qcfg);

    let hits = if cache_stats {
        let mut snap_cache: Cache2Q<Hash32, Arc<IndexSnapshotV1>> = Cache2Q::new(cache_cfg_kind("SNAPSHOT"));
        let mut idx_cache: Cache2Q<Hash32, Arc<IndexSegmentV1>> = Cache2Q::new(cache_cfg_kind("INDEX"));

        let (h, gate) = match sig_map_hash {
            Some(ref smh) => match search_snapshot_cached_gated(
                &store,
                &snap_hash,
                smh,
                &qterms,
                &scfg,
                Some(&mut snap_cache),
                Some(&mut idx_cache),
            ) {
                Ok((h, g)) => (h, Some(g)),
                Err(e) => {
                    eprintln!("query error: {:?}", e);
                    return 1;
                }
            },
            None => match search_snapshot_cached(
                &store,
                &snap_hash,
                &qterms,
                &scfg,
                Some(&mut snap_cache),
                Some(&mut idx_cache),
            ) {
                Ok(h) => (h, None),
                Err(e) => {
                    eprintln!("query error: {:?}", e);
                    return 1;
                }
            },
        };

        print_cache_stats("snapshot", snap_cache.stats(), snap_cache.bytes_live());
        print_cache_stats("index", idx_cache.stats(), idx_cache.bytes_live());
        if let Some(g) = gate {
            eprintln!(
                "gate.entries_total={} entries_decoded={} entries_skipped_sig={} entries_missing_sig={} query_terms_total={} bloom_probes_total={}",
                g.entries_total,
                g.entries_decoded,
                g.entries_skipped_sig,
                g.entries_missing_sig,
                g.query_terms_total,
                g.bloom_probes_total
            );
        }
        h
    } else {
        match sig_map_hash {
            Some(ref smh) => match search_snapshot_gated(&store, &snap_hash, smh, &qterms, &scfg) {
                Ok((h, g)) => {
                    eprintln!(
                        "gate.entries_total={} entries_decoded={} entries_skipped_sig={} entries_missing_sig={} query_terms_total={} bloom_probes_total={}",
                        g.entries_total,
                        g.entries_decoded,
                        g.entries_skipped_sig,
                        g.entries_missing_sig,
                        g.query_terms_total,
                        g.bloom_probes_total
                    );
                    h
                }
                Err(e) => {
                    eprintln!("query error: {:?}", e);
                    return 1;
                }
            },
            None => match search_snapshot(&store, &snap_hash, &qterms, &scfg) {
                Ok(h) => h,
                Err(e) => {
                    eprintln!("query error: {:?}", e);
                    return 1;
                }
            },
        }
    };

    // Store HitList and emit a ReplayLog step for retrieve-v1.
    let mut hl_hits: Vec<HitV1> = Vec::with_capacity(hits.len());
    for h in hits.iter() {
        hl_hits.push(HitV1 { frame_seg: h.frame_seg, row_ix: h.row_ix, score: h.score });
    }
    let hl = HitListV1 { query_id, snapshot_id: snap_hash, tie_control_id: None, hits: hl_hits };
    let hit_list_hash = match put_hit_list_v1(&store, &hl) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("store error: {}", e);
            return 1;
        }
    };

    let mut rlog = ReplayLog::new();
    let mut inputs: Vec<Hash32> = Vec::new();
    inputs.push(snap_hash);
    if let Some(smh) = sig_map_hash {
        inputs.push(smh);
    }
    inputs.push(query_id);
    rlog.steps.push(step_from_slices(STEP_RETRIEVE_V1, &inputs, &[hit_list_hash]));
    let _replay_hash = match put_replay_log(&store, &rlog) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("store error: {}", e);
            return 1;
        }
    };

    if hits.is_empty() {
        println!("no hits");
        return 0;
    }

    for h in hits.iter() {
        // Print as: score frame_seg row_ix
        println!("{}\t{}\t{}", h.score, fsa_lm::hash::hex32(&h.frame_seg), h.row_ix);
    }

    0
}



fn cmd_build_evidence(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut snapshot_hex: Option<String> = None;
    let mut sig_map_hex: Option<String> = None;
    let mut text: Option<String> = None;
    let mut k: usize = 10;
    let mut include_meta: bool = false;
    let mut max_items: Option<u32> = None;
    let mut max_bytes: Option<u32> = None;
    let mut no_sketch: bool = false;
    let mut no_verify: bool = false;
    let mut score_model_id: u32 = 0;
    let mut verbose: bool = false;
    let mut cache_stats: bool = false;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--snapshot" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --snapshot value");
                    return 2;
                }
                snapshot_hex = Some(args[i].clone());
            }
            "--sig-map" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --sig-map value");
                    return 2;
                }
                sig_map_hex = Some(args[i].clone());
            }
            "--text" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --text value");
                    return 2;
                }
                text = Some(args[i].clone());
            }
            "--k" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --k value");
                    return 2;
                }
                match args[i].parse::<usize>() {
                    Ok(v) if v > 0 => k = v,
                    _ => {
                        eprintln!("bad --k value");
                        return 2;
                    }
                }
            }
            "--meta" => {
                include_meta = true;
            }
            "--max_items" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --max_items value");
                    return 2;
                }
                match parse_u32(&args[i]) {
                    Ok(v) => max_items = Some(v),
                    Err(_) => {
                        eprintln!("bad --max_items value");
                        return 2;
                    }
                }
            }
            "--max_bytes" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --max_bytes value");
                    return 2;
                }
                match parse_u32(&args[i]) {
                    Ok(v) => max_bytes = Some(v),
                    Err(_) => {
                        eprintln!("bad --max_bytes value");
                        return 2;
                    }
                }
            }
            "--no_sketch" => {
                no_sketch = true;
            }
            "--no_verify" => {
                no_verify = true;
            }
            "--score_model" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --score_model value");
                    return 2;
                }
                match parse_u32(&args[i]) {
                    Ok(v) => score_model_id = v,
                    Err(_) => {
                        eprintln!("bad --score_model value");
                        return 2;
                    }
                }
            }
            "--verbose" => {
                verbose = true;
            }
            "--cache-stats" => {
                cache_stats = true;
            }
            _ => {
                eprintln!("unknown arg: {}", args[i]);
                return 2;
            }
        }
        i += 1;
    }

    let snap_hex = match snapshot_hex {
        Some(x) => x,
        None => {
            eprintln!("missing --snapshot");
            return 2;
        }
    };
    let qtext = match text {
        Some(x) => x,
        None => {
            eprintln!("missing --text");
            return 2;
        }
    };

    let snap_hash = match parse_hash32_hex(&snap_hex) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("bad snapshot hash: {}", e);
            return 2;
        }
    };

    let sig_map_hash: Option<Hash32> = match sig_map_hex {
        Some(x) => match parse_hash32_hex(&x) {
            Ok(h) => Some(h),
            Err(e) => {
                eprintln!("bad sig-map hash: {}", e);
                return 2;
            }
        },
        None => None,
    };

    let store = match FsArtifactStore::new(&root) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("store error: {}", e);
            return 1;
        }
    };

    let mut qcfg = QueryTermsCfg::new();
    qcfg.include_metaphone = include_meta;

    let mut scfg = SearchCfg::new();
    scfg.k = k;

    let qterms = query_terms_from_text(&qtext, &qcfg);

    let snap_cache: Option<Cache2Q<Hash32, Arc<IndexSnapshotV1>>> = if cache_stats {
        Some(Cache2Q::new(cache_cfg_kind("SNAPSHOT")))
    } else {
        None
    };
    let idx_cache: Option<Cache2Q<Hash32, Arc<IndexSegmentV1>>> = if cache_stats {
        Some(Cache2Q::new(cache_cfg_kind("INDEX")))
    } else {
        None
    };
    let mut frame_cache: Option<Cache2Q<Hash32, Arc<FrameSegmentV1>>> = if cache_stats {
        Some(Cache2Q::new(cache_cfg_kind("FRAME")))
    } else {
        None
    };

    let hits = if cache_stats {
        let mut snap_cache: Cache2Q<Hash32, Arc<IndexSnapshotV1>> = Cache2Q::new(cache_cfg_kind("SNAPSHOT"));
        let mut idx_cache: Cache2Q<Hash32, Arc<IndexSegmentV1>> = Cache2Q::new(cache_cfg_kind("INDEX"));

        let (h, gate) = match sig_map_hash {
            Some(ref smh) => match search_snapshot_cached_gated(
                &store,
                &snap_hash,
                smh,
                &qterms,
                &scfg,
                Some(&mut snap_cache),
                Some(&mut idx_cache),
            ) {
                Ok((h, g)) => (h, Some(g)),
                Err(e) => {
                    eprintln!("query error: {:?}", e);
                    return 1;
                }
            },
            None => match search_snapshot_cached(
                &store,
                &snap_hash,
                &qterms,
                &scfg,
                Some(&mut snap_cache),
                Some(&mut idx_cache),
            ) {
                Ok(h) => (h, None),
                Err(e) => {
                    eprintln!("query error: {:?}", e);
                    return 1;
                }
            },
        };

        print_cache_stats("snapshot", snap_cache.stats(), snap_cache.bytes_live());
        print_cache_stats("index", idx_cache.stats(), idx_cache.bytes_live());
        if let Some(g) = gate {
            eprintln!(
                "gate.entries_total={} entries_decoded={} entries_skipped_sig={} entries_missing_sig={} query_terms_total={} bloom_probes_total={}",
                g.entries_total,
                g.entries_decoded,
                g.entries_skipped_sig,
                g.entries_missing_sig,
                g.query_terms_total,
                g.bloom_probes_total
            );
        }
        h
    } else {
        match sig_map_hash {
            Some(ref smh) => match search_snapshot_gated(&store, &snap_hash, smh, &qterms, &scfg) {
                Ok((h, g)) => {
                    eprintln!(
                        "gate.entries_total={} entries_decoded={} entries_skipped_sig={} entries_missing_sig={} query_terms_total={} bloom_probes_total={}",
                        g.entries_total,
                        g.entries_decoded,
                        g.entries_skipped_sig,
                        g.entries_missing_sig,
                        g.query_terms_total,
                        g.bloom_probes_total
                    );
                    h
                }
                Err(e) => {
                    eprintln!("query error: {:?}", e);
                    return 1;
                }
            },
            None => match search_snapshot(&store, &snap_hash, &qterms, &scfg) {
                Ok(h) => h,
                Err(e) => {
                    eprintln!("query error: {:?}", e);
                    return 1;
                }
            },
        }
    };

    let k_u32 = if k > (u32::MAX as usize) { u32::MAX } else { k as u32 };
    let mi = max_items.unwrap_or(k_u32);
    let mb = max_bytes.unwrap_or(64 * 1024);

    let mut qid_bytes: Vec<u8> = Vec::new();
    qid_bytes.extend_from_slice(b"build-evidence-v1\0");
    qid_bytes.push(if include_meta { 1 } else { 0 });
    qid_bytes.push(if no_sketch { 1 } else { 0 });
    qid_bytes.push(if no_verify { 1 } else { 0 });
    qid_bytes.extend_from_slice(&k_u32.to_le_bytes());
    qid_bytes.extend_from_slice(&score_model_id.to_le_bytes());
    qid_bytes.extend_from_slice(&mi.to_le_bytes());
    qid_bytes.extend_from_slice(&mb.to_le_bytes());
    qid_bytes.extend_from_slice(qtext.as_bytes());
    let query_id = blake3_hash(&qid_bytes);

    // Store the query-id blob as an artifact so ReplayLog steps can reference it.
    // This blob includes the effective build-evidence config and query text.
    let qid_hash = match store.put(&qid_bytes) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("store error: {}", e);
            return 1;
        }
    };
    if qid_hash != query_id {
        eprintln!("internal error: query-id hash mismatch");
        return 1;
    }


    let limits = EvidenceLimitsV1 { segments_touched: 0, max_items: mi, max_bytes: mb };

    let mut bcfg = EvidenceBuildCfgV1::new();
    bcfg.verify_refs = !no_verify;
    bcfg.sketch.enable = !no_sketch;

    let bundle_res = if cache_stats {
        let fc = frame_cache.as_mut().expect("frame cache");
        build_evidence_bundle_v1_from_hits_cached(
            &store,
            fc,
            query_id,
            snap_hash,
            limits,
            score_model_id,
            &hits,
            &bcfg,
        )
    } else {
        build_evidence_bundle_v1_from_hits(
            &store,
            query_id,
            snap_hash,
            limits,
            score_model_id,
            &hits,
            &bcfg,
        )
    };

    let bundle = match bundle_res {
        Ok(b) => b,
        Err(e) => {
            eprintln!("build-evidence failed: {}", e);
            return 1;
        }
    };

    if cache_stats {
        if let Some(c) = snap_cache.as_ref() {
            print_cache_stats("snapshot", c.stats(), c.bytes_live());
        }
        if let Some(c) = idx_cache.as_ref() {
            print_cache_stats("index", c.stats(), c.bytes_live());
        }
        if let Some(c) = frame_cache.as_ref() {
            print_cache_stats("frame", c.stats(), c.bytes_live());
        }
    }

    let ev_hash = match put_evidence_bundle_v1(&store, &bundle) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("store error: {}", e);
            return 1;
        }
    };

    // Emit ReplayLog for build-evidence.
    let mut rlog = ReplayLog::new();
    let mut inputs: Vec<Hash32> = Vec::new();
    inputs.push(snap_hash);
    if let Some(smh) = sig_map_hash {
        inputs.push(smh);
    }
    inputs.push(query_id);
    rlog.steps.push(step_from_slices(STEP_BUILD_EVIDENCE_V1, &inputs, &[ev_hash]));
    let replay_hash = match put_replay_log(&store, &rlog) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("store error: {}", e);
            return 1;
        }
    };
    if verbose {
        eprintln!("replay_log={}", hex32(&replay_hash));
    }


    if verbose {
        let mut sketch_count: u32 = 0;
        for it in bundle.items.iter() {
            if let fsa_lm::evidence_bundle::EvidenceItemDataV1::Frame(r) = &it.data {
                if r.sketch.is_some() {
                    sketch_count = sketch_count.saturating_add(1);
                }
            }
        }
        eprintln!(
            "hits_in={}\titems_out={}\tsketches={}\tmax_items={}\tmax_bytes={}",
            hits.len(),
            bundle.items.len(),
            sketch_count,
            mi,
            mb
        );
    }

    println!("{}", hex32(&ev_hash));
    0
}

fn answer_run_text_inner(
    args: &[String],
    markov_context_tokens: &[MarkovTokenV1],
) -> Result<(String, Option<PathBuf>, Vec<MarkovTokenV1>, Hash32), i32> {
    let mut root: PathBuf = default_root();
    let mut prompt_hash: Option<Hash32> = None;
    let mut snapshot_hash: Option<Hash32> = None;
    let mut sig_map_hash: Option<Hash32> = None;
    let mut lexicon_snapshot_hash: Option<Hash32> = None;
    let mut graph_relevance_hash: Option<Hash32> = None;
    let mut graph_relevance_from_workspace: bool = false;
    let mut graph_relevance_from_runtime_state: bool = false;
    let mut enable_expand: bool = false;
    let mut expand_enabled_by_workspace_graph: bool = false;
    let mut expand_explicit: bool = false;
    let mut pragmatics_ids: Vec<Hash32> = Vec::new();
    let mut k: usize = 10;
    let mut k_explicit: bool = false;
    let mut include_meta: bool = false;
    let mut meta_explicit: bool = false;
    let mut max_terms: Option<u32> = None;
    let mut no_ties: bool = false;
    let mut plan_items: Option<u32> = None;
    let mut out_file: Option<PathBuf> = None;
    let mut verify_trace: u8 = 0;

    let mut markov_model_hash: Option<Hash32> = None;
    let mut markov_model_from_workspace: bool = false;
    let mut markov_model_from_runtime_state: bool = false;
    let mut markov_max_choices: usize = 8;
    let mut exemplar_memory_hash: Option<Hash32> = None;
    let mut exemplar_memory_from_workspace: bool = false;
    let mut exemplar_memory_from_runtime_state: bool = false;

    let mut prior_replay_hash: Option<Hash32> = None;
    let mut presentation_mode = PresentationModeV1::User;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return Err(1);
                }
                root = PathBuf::from(&args[i]);
            }
            "--prompt" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --prompt value");
                    return Err(1);
                }
                match parse_hash32_hex(&args[i]) {
                    Ok(h) => prompt_hash = Some(h),
                    Err(e) => {
                        eprintln!("bad --prompt: {}", e);
                        return Err(1);
                    }
                }
            }
            "--snapshot" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --snapshot value");
                    return Err(1);
                }
                match parse_hash32_hex(&args[i]) {
                    Ok(h) => snapshot_hash = Some(h),
                    Err(e) => {
                        eprintln!("bad --snapshot: {}", e);
                        return Err(1);
                    }
                }
            }
            "--sig-map" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --sig-map value");
                    return Err(1);
                }
                match parse_hash32_hex(&args[i]) {
                    Ok(h) => sig_map_hash = Some(h),
                    Err(e) => {
                        eprintln!("bad --sig-map: {}", e);
                        return Err(1);
                    }
                }
            }
            "--lexicon-snapshot" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --lexicon-snapshot value");
                    return Err(1);
                }
                match parse_hash32_hex(&args[i]) {
                    Ok(h) => lexicon_snapshot_hash = Some(h),
                    Err(e) => {
                        eprintln!("bad --lexicon-snapshot: {}", e);
                        return Err(1);
                    }
                }
            }
            "--graph-relevance" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --graph-relevance value");
                    return Err(1);
                }
                match parse_hash32_hex(&args[i]) {
                    Ok(h) => graph_relevance_hash = Some(h),
                    Err(e) => {
                        eprintln!("bad --graph-relevance: {}", e);
                        return Err(1);
                    }
                }
            }
            "--sticky-graph-relevance" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --sticky-graph-relevance value");
                    return Err(1);
                }
                match parse_hash32_hex(&args[i]) {
                    Ok(h) => {
                        graph_relevance_hash = Some(h);
                        graph_relevance_from_runtime_state = true;
                    }
                    Err(e) => {
                        eprintln!("bad --sticky-graph-relevance: {}", e);
                        return Err(1);
                    }
                }
            }
            "--expand" => {
                enable_expand = true;
                expand_explicit = true;
            }
            "--pragmatics" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --pragmatics value");
                    return Err(1);
                }
                match parse_hash32_hex(&args[i]) {
                    Ok(h) => pragmatics_ids.push(h),
                    Err(e) => {
                        eprintln!("bad --pragmatics: {}", e);
                        return Err(1);
                    }
                }
            }
            "--k" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --k value");
                    return Err(1);
                }
                match parse_u32(&args[i]) {
                    Ok(v) => {
                        k = v as usize;
                        k_explicit = true;
                    }
                    Err(e) => {
                        eprintln!("bad --k: {}", e);
                        return Err(1);
                    }
                }
            }
            "--meta" => {
                include_meta = true;
                meta_explicit = true;
            }
            "--max_terms" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --max_terms value");
                    return Err(1);
                }
                match parse_u32(&args[i]) {
                    Ok(v) => max_terms = Some(v),
                    Err(e) => {
                        eprintln!("bad --max_terms: {}", e);
                        return Err(1);
                    }
                }
            }
            "--no_ties" => {
                no_ties = true;
            }
            "--plan_items" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --plan_items value");
                    return Err(1);
                }
                match parse_u32(&args[i]) {
                    Ok(v) => plan_items = Some(v),
                    Err(e) => {
                        eprintln!("bad --plan_items: {}", e);
                        return Err(1);
                    }
                }
            }
            "--out-file" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --out-file value");
                    return Err(1);
                }
                out_file = Some(PathBuf::from(&args[i]));
            }
            "--verify-trace" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --verify-trace value");
                    return Err(1);
                }
                match parse_u8(&args[i]) {
                    Ok(v) => {
                        if v > 1 {
                            eprintln!("verify-trace must be 0 or 1");
                            return Err(1);
                        }
                        verify_trace = v;
                    }
                    Err(e) => {
                        eprintln!("bad --verify-trace: {}", e);
                        return Err(1);
                    }
                }
            }
            "--markov-model" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --markov-model value");
                    return Err(1);
                }
                match parse_hash32_hex(&args[i]) {
                    Ok(h) => markov_model_hash = Some(h),
                    Err(e) => {
                        eprintln!("bad --markov-model: {}", e);
                        return Err(1);
                    }
                }
            }
            "--sticky-markov-model" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --sticky-markov-model value");
                    return Err(1);
                }
                match parse_hash32_hex(&args[i]) {
                    Ok(h) => {
                        markov_model_hash = Some(h);
                        markov_model_from_runtime_state = true;
                    }
                    Err(e) => {
                        eprintln!("bad --sticky-markov-model: {}", e);
                        return Err(1);
                    }
                }
            }
            "--markov-max-choices" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --markov-max-choices value");
                    return Err(1);
                }
                match parse_u32(&args[i]) {
                    Ok(v) => {
                        if v == 0 || v > 32 {
                            eprintln!("markov-max-choices must be 1..32");
                            return Err(1);
                        }
                        markov_max_choices = v as usize;
                    }
                    Err(e) => {
                        eprintln!("bad --markov-max-choices: {}", e);
                        return Err(1);
                    }
                }
            }
            "--exemplar-memory" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --exemplar-memory value");
                    return Err(1);
                }
                match parse_hash32_hex(&args[i]) {
                    Ok(h) => exemplar_memory_hash = Some(h),
                    Err(e) => {
                        eprintln!("bad --exemplar-memory: {}", e);
                        return Err(1);
                    }
                }
            }
            "--sticky-exemplar-memory" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --sticky-exemplar-memory value");
                    return Err(1);
                }
                match parse_hash32_hex(&args[i]) {
                    Ok(h) => {
                        exemplar_memory_hash = Some(h);
                        exemplar_memory_from_runtime_state = true;
                    }
                    Err(e) => {
                        eprintln!("bad --sticky-exemplar-memory: {}", e);
                        return Err(1);
                    }
                }
            }
            "--prior-replay" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --prior-replay value");
                    return Err(1);
                }
                match parse_hash32_hex(&args[i]) {
                    Ok(h) => prior_replay_hash = Some(h),
                    Err(e) => {
                        eprintln!("bad --prior-replay: {}", e);
                        return Err(1);
                    }
                }
            }
            "--presentation" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --presentation value");
                    return Err(1);
                }
                match parse_presentation_mode_v1(&args[i]) {
                    Ok(v) => presentation_mode = v,
                    Err(e) => {
                        eprintln!("bad --presentation: {}", e);
                        return Err(1);
                    }
                }
            }
            "-h" | "--help" => {
                println!("{}", usage());
                return Err(0);
            }
            other => {
                eprintln!("unknown arg: {}", other);
                return Err(1);
            }
        }
        i += 1;
    }

    let prompt_hash = match prompt_hash {
        Some(h) => h,
        None => {
            eprintln!("missing --prompt");
            return Err(1);
        }
    };

    let store = store_for(&root);

    let workspace_runtime = load_workspace_runtime_defaults_v1(&root);
    let ws_opt = workspace_runtime.workspace.as_ref();

    if snapshot_hash.is_none() {
        if sig_map_hash.is_some() {
            eprintln!("missing --snapshot (must provide --snapshot when using --sig-map)");
            return Err(1);
        }

        let ws = if let Some(ws) = ws_opt {
            ws
        } else if let Some(e) = workspace_runtime.invalid_error.as_ref() {
            eprintln!("workspace error: {}", e);
            eprintln!("workspace file: {}/{}", root.to_string_lossy(), WORKSPACE_V1_FILENAME);
            return Err(1);
        } else if let Some(e) = workspace_runtime.read_error.as_ref() {
            eprintln!("workspace read error: {}", e);
            eprintln!("workspace file: {}/{}", root.to_string_lossy(), WORKSPACE_V1_FILENAME);
            return Err(1);
        } else {
            eprintln!("missing --snapshot and workspace defaults not found");
            eprintln!("create workspace file: {}/{}", root.to_string_lossy(), WORKSPACE_V1_FILENAME);
            return Err(1);
        };
        if !ws.has_required_answer_keys() {
            eprintln!("workspace missing merged_snapshot/merged_sig_map");
            eprintln!("workspace file: {}/{}", root.to_string_lossy(), WORKSPACE_V1_FILENAME);
            return Err(1);
        }
        snapshot_hash = ws.merged_snapshot;
        sig_map_hash = ws.merged_sig_map;
    }

    if !expand_explicit {
        enable_expand = workspace_runtime.default_expand;
    }

    if enable_expand && lexicon_snapshot_hash.is_none() {
        if let Some(ws) = ws_opt {
            lexicon_snapshot_hash = ws.lexicon_snapshot;
        }
    }

    if !k_explicit {
        if let Some(v) = workspace_runtime.default_k {
            k = v as usize;
        }
    }

    if !meta_explicit {
        include_meta = workspace_runtime.default_meta;
    }

    if graph_relevance_hash.is_none() {
        if let Some(h) = workspace_runtime.graph_relevance_id {
            graph_relevance_hash = Some(h);
            graph_relevance_from_workspace = true;
        }
    }
    if markov_model_hash.is_none() {
        if let Some(h) = workspace_runtime.markov_model_id {
            markov_model_hash = Some(h);
            markov_model_from_workspace = true;
        }
    }
    if exemplar_memory_hash.is_none() {
        if let Some(h) = workspace_runtime.exemplar_memory_id {
            exemplar_memory_hash = Some(h);
            exemplar_memory_from_workspace = true;
        }
    }

    if !expand_explicit && !enable_expand && graph_relevance_hash.is_some() {
        enable_expand = true;
        expand_enabled_by_workspace_graph = true;
    }

    let snapshot_hash = match snapshot_hash {
        Some(h) => h,
        None => {
            eprintln!("missing --snapshot");
            return Err(1);
        }
    };

    let pack = match get_prompt_pack(&store, &prompt_hash) {
        Ok(Some(p)) => p,
        Ok(None) => {
            eprintln!("prompt not found");
            return Err(1);
        }
        Err(e) => {
            eprintln!("store error: {}", e);
            return Err(1);
        }
    };

    // Identify the query message (last user turn) and keep its index.
    let mut query_msg_ix: usize = 0;
    let mut qtext: String = String::new();
    let mut found_user: bool = false;
    if !pack.messages.is_empty() {
        for ix in (0..pack.messages.len()).rev() {
            if pack.messages[ix].role == Role::User {
                query_msg_ix = ix;
                qtext = pack.messages[ix].content.clone();
                found_user = true;
                break;
            }
        }
        if !found_user {
            // Fallback: use the last message.
            let ix = pack.messages.len() - 1;
            query_msg_ix = ix;
            qtext = pack.messages[ix].content.clone();
        }
    }

    // If the query contains a structured puzzle block, attempt a strict parse.
    // v1 only uses this parse status to decide whether to ask for clarification.
    let mut puzzle_parse_failed: bool = false;
    if let Some(block) = extract_puzzle_block(&qtext) {
        if parse_puzzle_block_v1(block).is_err() {
            puzzle_parse_failed = true;
        }
    }

    // If a structured puzzle block is present and parses, attempt to solve it
    // deterministically and store a ProofArtifactV1. This is optional evidence
    // that can be attached to the EvidenceBundle.
    let mut proof_hash_opt: Option<Hash32> = None;
    let mut puzzle_constraints_parse_failed: bool = false;
    if !puzzle_parse_failed {
        if let Some(block) = extract_puzzle_block(&qtext) {
            if let Ok(spec) = parse_puzzle_block_v1(block) {
                let cfg = LogicSolveCfgV1::default_v1();
                match solve_puzzle_v1(&spec, cfg) {
                    Ok(proof) => {
                        match put_proof_artifact_v1(&store, &proof) {
                            Ok(h) => proof_hash_opt = Some(h),
                            Err(e) => {
                                eprintln!("proof store failed: {}", e);
                                return Err(1);
                            }
                        }
                    }
                    Err(_) => {
                        // If the puzzle is not solvable under the supported constraint
                        // set or caps, fall back to the normal clarify behavior.
                    }
                }
            }
        }
    }

    // Free-text sketch solve (conversational).
    //
    // If we have enough information (vars + numeric domain + parseable constraints)
    // we compile the sketch into a PuzzleSpecV1 and run the deterministic solver.
    //
    // This is separate from the structured [puzzle] block flow.
    if proof_hash_opt.is_none() && !puzzle_parse_failed {
        // Prefer a pending sketch from the prior assistant replay, if present.
        // This is the common case for "clarifier reply" turns where the user
        // provides only constraints.
        let mut pending_sketch_opt: Option<fsa_lm::puzzle_sketch_v1::PuzzleSketchV1> = None;
        if let Some(prh) = prior_replay_hash.as_ref() {
            if let Ok(Some(rlog)) = get_replay_log(&store, prh) {
                for st in rlog.steps.iter().rev() {
                    if st.name != STEP_PUZZLE_SKETCH_V1 {
                        continue;
                    }
                    if st.outputs.len() != 1 {
                        break;
                    }
                    let prev_sketch_hash = st.outputs[0];
                    if let Ok(Some(prev_art)) = fsa_lm::puzzle_sketch_artifact_store::get_puzzle_sketch_artifact_v1(&store, &prev_sketch_hash) {
                        if (prev_art.flags & fsa_lm::puzzle_sketch_artifact::PSA_FLAG_PENDING) != 0 {
                            pending_sketch_opt = Some(fsa_lm::puzzle_sketch_v1::PuzzleSketchV1 {
                                is_logic_puzzle_likely: true,
                                var_names: prev_art.var_names.clone(),
                                domain_range: prev_art.domain_range,
                                has_constraints: prev_art.has_constraints,
                                shape: prev_art.shape,
                            });
                        }
                    }
                    break;
                }
            }
        }

        let pending_vars_for_fallback: Option<Vec<String>> = pending_sketch_opt.as_ref().map(|sk| sk.var_names.clone());

        // Parse constraints once and compile/solve using the same parsed list.
        //
        // Only attempt constraint parsing when we have a pending sketch (cross-turn
        // clarification) or when the current turn includes obvious operator signals.
        // This avoids treating unrelated text as malformed constraints.
        let q_has_ops = qtext.contains("!=")
            || qtext.contains("<=")
            || qtext.contains(">=")
            || qtext.contains('=')
            || qtext.contains('<')
            || qtext.contains('>');

        if pending_sketch_opt.is_some() || q_has_ops {
            match fsa_lm::logic_solver_v1::parse_constraints_from_text_v1(&qtext, 256) {
                Ok(mut cs) => {
                    if cs.is_empty() {
                        if let Some(vs) = pending_vars_for_fallback.as_ref() {
                            cs = fsa_lm::logic_solver_v1::extract_eq_constraints_for_vars_v1(&qtext, vs, 256);
                        }
                    }

                    if !cs.is_empty() {
                    // First try: pending sketch (vars/domain/shape from prior turns).
                    if let Some(mut sk) = pending_sketch_opt {
                        sk.has_constraints = true;
                        match try_compile_puzzle_spec_from_sketch_and_constraints_v1(&sk, cs.clone()) {
                            Ok(Some(spec)) => {
                                let cfg = LogicSolveCfgV1::default_v1();
                                match solve_puzzle_v1(&spec, cfg) {
                                    Ok(proof) => match put_proof_artifact_v1(&store, &proof) {
                                        Ok(h) => proof_hash_opt = Some(h),
                                        Err(e) => {
                                            eprintln!("proof store failed: {}", e);
                                            return Err(1);
                                        }
                                    },
                                    Err(_) => {
                                        puzzle_constraints_parse_failed = true;
                                    }
                                }
                            }
                            Ok(None) => {}
                            Err(PuzzleCompileErrV1::ConstraintParseFailed) => {
                                puzzle_constraints_parse_failed = true;
                            }
                            Err(_) => {}
                        }
                    }

                    // Second try: best-effort sketch from the current turn text.
                    if proof_hash_opt.is_none() {
                        let pscfg = fsa_lm::puzzle_sketch_v1::PuzzleSketchCfgV1::default();

                        let mut lex_for_sketch: Option<Hash32> = lexicon_snapshot_hash;
                        if lex_for_sketch.is_none() {
                            if let Some(ws) = ws_opt {
                                lex_for_sketch = ws.lexicon_snapshot;
                            }
                        }

                        let mut view_opt: Option<fsa_lm::lexicon_expand_lookup::LexiconExpandLookupV1> = None;
                        let mut cues_opt: Option<fsa_lm::lexicon_neighborhoods::LexiconCueNeighborhoodsV1> = None;

                        if let Some(lh) = lex_for_sketch.as_ref() {
                            match fsa_lm::lexicon_expand_lookup::load_lexicon_expand_lookup_v1(&store, lh) {
                                Ok(Some(view)) => {
                                    let ncfg = fsa_lm::lexicon_neighborhoods::LexiconNeighborhoodCfgV1::new();
                                    let cues = fsa_lm::lexicon_neighborhoods::build_lexicon_cue_neighborhoods_v1(&view, &ncfg);
                                    view_opt = Some(view);
                                    cues_opt = Some(cues);
                                }
                                _ => {}
                            }
                        }

                        let sk = fsa_lm::puzzle_sketch_v1::build_puzzle_sketch_v1(
                            &qtext,
                            view_opt.as_ref(),
                            cues_opt.as_ref(),
                            pscfg,
                        );

                        match try_compile_puzzle_spec_from_sketch_and_constraints_v1(&sk, cs) {
                            Ok(Some(spec)) => {
                                let cfg = LogicSolveCfgV1::default_v1();
                                match solve_puzzle_v1(&spec, cfg) {
                                    Ok(proof) => match put_proof_artifact_v1(&store, &proof) {
                                        Ok(h) => proof_hash_opt = Some(h),
                                        Err(e) => {
                                            eprintln!("proof store failed: {}", e);
                                            return Err(1);
                                        }
                                    },
                                    Err(_) => {
                                        puzzle_constraints_parse_failed = true;
                                    }
                                }
                            }
                            Ok(None) => {}
                            Err(PuzzleCompileErrV1::ConstraintParseFailed) => {
                                puzzle_constraints_parse_failed = true;
                            }
                            Err(_) => {}
                        }
                    }
                    }
                }
                Err(_) => {
                    puzzle_constraints_parse_failed = true;
                }
            }
        }
    }


    let mut qcfg = QueryTermsCfg::new();
    qcfg.include_metaphone = include_meta;
    if let Some(mt) = max_terms {
        if mt == 0 {
            eprintln!("max_terms must be >= 1");
            return Err(1);
        }
        qcfg.max_terms = mt as usize;
    }

    let mut control = RetrievalControlV1::new(prompt_hash);
    if !pragmatics_ids.is_empty() {
        control.pragmatics_frame_ids = pragmatics_ids;
    }
    if let Err(e) = control.validate() {
        eprintln!("control error: {}", e);
        return Err(1);
    }

    let mut pcfg = RetrievalPolicyCfgV1::new();
    if k > (u16::MAX as usize) {
        eprintln!("k too large");
        return Err(1);
    }
    pcfg.max_hits = k as u16;
    if pcfg.max_hits == 0 {
        eprintln!("k must be >= 1");
        return Err(1);
    }
    if let Some(mt) = max_terms {
        if mt > (u16::MAX as u32) {
            eprintln!("max_terms too large");
            return Err(1);
        }
        pcfg.max_query_terms = mt as u16;
    }
    if no_ties {
        pcfg.include_ties_at_cutoff = 0;
    }

    if graph_relevance_from_workspace || graph_relevance_from_runtime_state {
        if let Some(gh) = graph_relevance_hash {
            match get_graph_relevance_v1(&store, &gh) {
                Ok(Some(_)) => {}
                Ok(None) => {
                    graph_relevance_hash = None;
                    if expand_enabled_by_workspace_graph {
                        enable_expand = false;
                    }
                }
                Err(e) => {
                    eprintln!("graph-relevance load failed: {}", e);
                    return Err(1);
                }
            }
        }
    }

    if enable_expand {
        pcfg.enable_query_expansion = 1;
        if lexicon_snapshot_hash.is_none() && graph_relevance_hash.is_none() {
            eprintln!("missing --lexicon-snapshot or --graph-relevance (required when --expand)");
            eprintln!("or set lexicon_snapshot in {}/{}", root.to_string_lossy(), WORKSPACE_V1_FILENAME);
            return Err(1);
        }
    }

    // Derive low-weight context anchors from prior conversation messages.
    // These anchors are intended to improve follow-up retrieval continuity.
    let mut context_anchors_hash_opt: Option<Hash32> = None;
    let mut context_anchors_lex_hash_opt: Option<Hash32> = None;
    let mut anchor_terms_opt: Option<Vec<fsa_lm::index_query::QueryTerm>> = None;

    // Logic puzzle pending sketch artifact hashes (recorded in ReplayLog when present).
    let mut puzzle_sketch_hash_opt: Option<Hash32> = None;
    let mut puzzle_sketch_lex_hash_opt: Option<Hash32> = None;
    if query_msg_ix > 0 && pack.messages.len() >= 2 {
        let mut lex_for_anchors: Option<fsa_lm::lexicon_expand_lookup::LexiconExpandLookupV1> = None;
        // Best-effort: if a lexicon snapshot is not explicitly in use, try workspace defaults.
        let mut lex_hash_opt = lexicon_snapshot_hash;
        if lex_hash_opt.is_none() {
            if let Some(ws) = ws_opt {
                lex_hash_opt = ws.lexicon_snapshot;
            }
        }

        if let Some(lh) = lex_hash_opt.as_ref() {
            match fsa_lm::lexicon_expand_lookup::load_lexicon_expand_lookup_v1(&store, lh) {
                Ok(Some(v)) => lex_for_anchors = Some(v),
                Ok(None) => {
                    // Snapshot missing; proceed without lexicon filtering.
                }
                Err(e) => {
                    eprintln!("context anchors: lexicon load failed: {}", e);
                    return Err(1);
                }
            }
            if lex_for_anchors.is_some() {
                context_anchors_lex_hash_opt = Some(*lh);
            }
        }

        let cfg_ca = ContextAnchorsCfgV1::default_v1();
        if let Some(b) = build_context_anchors_v1(
            prompt_hash,
            query_msg_ix,
            &pack.messages,
            &qcfg,
            lex_for_anchors.as_ref(),
            cfg_ca,
        ) {
            match put_context_anchors_v1(&store, &b.anchors) {
                Ok(h) => {
                    context_anchors_hash_opt = Some(h);
                    anchor_terms_opt = Some(b.query_terms);
                }
                Err(e) => {
                    eprintln!("context anchors: store failed: {}", e);
                    return Err(1);
                }
            }
        }
    }

    let (hits, _stats) = match apply_retrieval_policy_from_text_v1_with_anchors(
        &store,
        &snapshot_hash,
        sig_map_hash.as_ref(),
        &qtext,
        &qcfg,
        &pcfg,
        Some(&control),
        lexicon_snapshot_hash.as_ref(),
        graph_relevance_hash.as_ref(),
        None,
        anchor_terms_opt.as_deref(),
    ) {
        Ok(x) => x,
        Err(e) => {
            eprintln!("policy error: {}", e);
            return Err(1);
        }
    };

    // Derive a query_id that reflects prompt + policy.
    let mut qid_bytes: Vec<u8> = Vec::new();
    qid_bytes.extend_from_slice(b"answer-v1\0");
    qid_bytes.extend_from_slice(&prompt_hash);
    qid_bytes.extend_from_slice(&snapshot_hash);
    if let Some(smh) = sig_map_hash.as_ref() {
        qid_bytes.push(1);
        qid_bytes.extend_from_slice(smh);
    } else {
        qid_bytes.push(0);
    }
    qid_bytes.push(if include_meta { 1 } else { 0 });
    qid_bytes.push(if no_ties { 1 } else { 0 });
    qid_bytes.extend_from_slice(&(pcfg.max_query_terms as u16).to_le_bytes());
    qid_bytes.extend_from_slice(&(pcfg.max_hits as u16).to_le_bytes());
    qid_bytes.push(pcfg.enable_query_expansion);
    if pcfg.enable_query_expansion == 1 {
        if let Some(lh) = lexicon_snapshot_hash.as_ref() {
            qid_bytes.push(1);
            qid_bytes.extend_from_slice(lh);
        } else {
            qid_bytes.push(0);
        }
        if let Some(gh) = graph_relevance_hash.as_ref() {
            qid_bytes.push(1);
            qid_bytes.extend_from_slice(gh);
        } else {
            qid_bytes.push(0);
        }
    }
    qid_bytes.extend_from_slice(qtext.as_bytes());
    let query_id = blake3_hash(&qid_bytes);

    let limits = EvidenceLimitsV1 {
        segments_touched: 0,
        max_items: hits.len() as u32,
        max_bytes: 64 * 1024,
    };
    let score_model_id: u32 = 1;
    let bcfg = EvidenceBuildCfgV1::new();
    let mut bundle = match build_evidence_bundle_v1_from_hits(&store, query_id, snapshot_hash, limits, score_model_id, &hits, &bcfg) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("build-evidence failed: {}", e);
            return Err(1);
        }
    };

    let has_proof: bool = proof_hash_opt.is_some();

// If the logic solver produced a ProofArtifact, attach it as evidence.
    if let Some(ph) = proof_hash_opt.as_ref() {
        let ph = *ph;
        // Increase max_items so canonical validation remains satisfied.
        if bundle.limits.max_items != 0 {
            if bundle.limits.max_items == u32::MAX {
                eprintln!("evidence limits max_items overflow");
                return Err(1);
            }
            bundle.limits.max_items = bundle.limits.max_items.saturating_add(1);
        }

        // Use a fixed high score so proof evidence is ranked first for puzzle answers.
        bundle.items.push(EvidenceItemV1 {
            score: 1_000_000_000,
            data: EvidenceItemDataV1::Proof(ProofRefV1 { proof_id: ph }),
        });
    }

    if let Err(e) = bundle.canonicalize_in_place() {
        eprintln!("evidence canonicalize failed: {}", e);
        return Err(1);
    }

    let ev_hash = match put_evidence_bundle_v1(&store, &bundle) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("store error: {}", e);
            return Err(1);
        }
    };

    let mut pl_cfg = PlannerCfgV1::default_v1();
    if let Some(pi) = plan_items {
        if pi == 0 {
            eprintln!("plan_items must be >= 1");
            return Err(1);
        }
        if pi > 16_384 {
            eprintln!("plan_items too large");
            return Err(1);
        }
        pl_cfg.max_plan_items = pi;
    }

    let rcfg = RealizerCfgV1::new();

    let pf_opt = if control.pragmatics_frame_ids.is_empty() {
        None
    } else {
        let pid = *control
            .pragmatics_frame_ids
            .last()
            .expect("non-empty pragmatics ids");
        let pf_opt = match get_pragmatics_frame_v1(&store, &pid) {
            Ok(x) => x,
            Err(e) => {
                eprintln!("pragmatics load failed: {}", e);
                return Err(1);
            }
        };
        let pf = match pf_opt {
            Some(x) => x,
            None => {
                eprintln!("missing pragmatics frame: {}", hex32(&pid));
                return Err(1);
            }
        };
        Some(pf)
    };

    let mut directives_opt = derive_directives_opt(pf_opt.as_ref());

    // If a puzzle block is present but could not be parsed, ensure the
    // clarifying question path can emit one question even without pragmatics.
    if puzzle_parse_failed {
        if let Some(ref mut d) = directives_opt {
            if d.max_questions == 0 {
                d.max_questions = 1;
            }
        } else {
            directives_opt = Some(RealizerDirectivesV1 {
                version: REALIZER_DIRECTIVES_V1_VERSION,
                tone: ToneV1::Neutral,
                style: StyleV1::Debug,
                format_flags: 0,
                max_softeners: 0,
                max_preface_sentences: 0,
                max_hedges: 0,
                max_questions: 1,
                rationale_codes: Vec::new(),
            });
        }
    }

    let mut markov_hints_hash_opt: Option<Hash32> = None;
    let mut markov_hints_opt: Option<MarkovHintsV1> = None;

    let PlannerOutputV1 { mut plan, hints: mut planner_hints, mut forecast } = match plan_from_evidence_bundle_v1_with_guidance(&bundle, ev_hash, &pl_cfg, pf_opt.as_ref()) {
        Ok(x) => x,
        Err(e) => {
            eprintln!("plan failed: {}", e);
            return Err(1);
        }
    };
    // If we produced a proof artifact for this run, ensure the plan includes
    // at least one Step item so the base v1 realizer can surface the solution
    // line deterministically without changing the global default style.
    if proof_hash_opt.is_some() {
        planner_hints.flags |= PH_FLAG_PREFER_STEPS;
        let mut any_step: bool = false;
        for it in plan.items.iter() {
            if it.kind == fsa_lm::answer_plan::AnswerPlanItemKindV1::Step {
                any_step = true;
                break;
            }
        }
        if !any_step {
            for it in plan.items.iter_mut() {
                if it.kind == fsa_lm::answer_plan::AnswerPlanItemKindV1::Bullet {
                    it.kind = fsa_lm::answer_plan::AnswerPlanItemKindV1::Step;
                }
            }
        }
    }



    if !has_proof {

// Logic puzzle sketch + clarify (conversational, deterministic).
    //
    // We treat the structured [puzzle] block as optional input. When present and
    // malformed, we ask a clarifying question without requiring a specific format.
    // When absent, we attempt a conservative sketch from free text and ask for the
    // single most useful missing piece.

    let mut puzzle_sketch_opt: Option<fsa_lm::puzzle_sketch_v1::PuzzleSketchV1> = None;
    let mut puzzle_sketch_used_lexicon: bool = false;

    let mut puzzle_clarify_opt: Option<fsa_lm::puzzle_sketch_v1::PuzzleClarifyV1> = None;


    // Pending puzzle continuation (cross-turn).
    //
    // If the previous assistant replay produced a pending puzzle sketch, and this
    // user message looks like a clarification reply, merge the reply into the
    // prior sketch deterministically and continue the clarify sequence.
    let mut merged_from_pending: bool = false;

    if let Some(prh) = prior_replay_hash.as_ref() {
        if !puzzle_parse_failed {
            if let Ok(Some(rlog)) = fsa_lm::replay_artifact::get_replay_log(&store, prh) {
                // Find the most recent puzzle sketch step.
                let mut prev_step_opt: Option<&fsa_lm::replay::ReplayStep> = None;
                for st in rlog.steps.iter().rev() {
                    if st.name == STEP_PUZZLE_SKETCH_V1 {
                        prev_step_opt = Some(st);
                        break;
                    }
                }

                if let Some(st) = prev_step_opt {
                    if st.outputs.len() == 1 {
                        let prev_sketch_hash = st.outputs[0];
                        if let Ok(Some(prev_art)) = fsa_lm::puzzle_sketch_artifact_store::get_puzzle_sketch_artifact_v1(&store, &prev_sketch_hash) {
                            if (prev_art.flags & fsa_lm::puzzle_sketch_artifact::PSA_FLAG_PENDING) != 0 {
                                // Treat only short replies as clarifications to avoid pulling a prior
                                // sketch into an unrelated new question.
                                if qtext.len() <= 256 {
                                    let mut reply = fsa_lm::puzzle_sketch_v1::parse_puzzle_clarify_reply_v1(&qtext, 16);

                                    let q_has_ops = qtext.contains("!=")
                                        || qtext.contains("<=")
                                        || qtext.contains(">=")
                                        || qtext.contains("=")
                                        || qtext.contains("<")
                                        || qtext.contains(">");
                                    if q_has_ops {
                                        reply.has_constraints = true;
                                    }

                                    let mut provides: bool = false;
                                    if prev_art.var_names.is_empty() && !reply.var_names.is_empty() {
                                        provides = true;
                                    }
                                    if prev_art.domain_range.is_none() && reply.domain_range.is_some() {
                                        provides = true;
                                    }
                                    if prev_art.shape == fsa_lm::puzzle_sketch_v1::PuzzleShapeHintV1::Unknown && reply.shape.is_some() {
                                        provides = true;
                                    }
                                    if !prev_art.has_constraints && reply.has_constraints {
                                        provides = true;
                                    }

                                    if provides {
                                        let prev_sk = fsa_lm::puzzle_sketch_v1::PuzzleSketchV1 {
                                            is_logic_puzzle_likely: prev_art.is_logic_puzzle_likely,
                                            var_names: prev_art.var_names.clone(),
                                            domain_range: prev_art.domain_range,
                                            has_constraints: prev_art.has_constraints,
                                            shape: prev_art.shape,
                                        };

                                        let merged = fsa_lm::puzzle_sketch_v1::merge_puzzle_sketch_with_reply_v1(&prev_sk, &reply, 16);
                                        let next_q = fsa_lm::puzzle_sketch_v1::choose_puzzle_clarify_question_v1(&merged);

                                        let used_lex = (prev_art.flags & fsa_lm::puzzle_sketch_artifact::PSA_FLAG_USED_LEXICON) != 0;
                                        puzzle_sketch_used_lexicon = used_lex;

                                        // Carry forward the lexicon snapshot hash used for the prior sketch step,
                                        // if it was recorded as an input.
                                        if st.inputs.len() >= 2 {
                                            puzzle_sketch_lex_hash_opt = Some(st.inputs[1]);
                                        }

                                        let src_hash = fsa_lm::puzzle_sketch_artifact::puzzle_sketch_merged_source_hash_v1(&prev_art.source_hash, &qtext);
                                        let psa = match fsa_lm::puzzle_sketch_artifact::PuzzleSketchArtifactV1::from_sketch(
                                            prompt_hash,
                                            query_msg_ix as u32,
                                            used_lex,
                                            true,
                                            next_q.is_some(),
                                            src_hash,
                                            &merged,
                                        ) {
                                            Ok(x) => x,
                                            Err(e) => {
                                                eprintln!("puzzle sketch merge: encode failed: {}", e);
                                                return Err(1);
                                            }
                                        };

                                        let psh = match fsa_lm::puzzle_sketch_artifact_store::put_puzzle_sketch_artifact_v1(&store, &psa) {
                                            Ok(h) => h,
                                            Err(e) => {
                                                eprintln!("puzzle sketch merge: store failed: {}", e);
                                                return Err(1);
                                            }
                                        };

                                        puzzle_sketch_hash_opt = Some(psh);
                                        puzzle_sketch_opt = Some(merged);
                                        puzzle_clarify_opt = next_q;
                                        merged_from_pending = true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if !merged_from_pending {

    // If the user supplied a structured puzzle block and it failed to parse, prefer a
    // bounded clarifying question to repair the intent.
    if puzzle_parse_failed {
        planner_hints.flags |= PH_FLAG_PREFER_CLARIFY | PH_FLAG_PREFER_STEPS;

        let qid = derive_id64(b"forecast_question_v1", b"clarify:logic_puzzle:parse_failed");
        let qtxt = "I could not parse that puzzle description. Could you restate the variables, their possible values, and the constraints? Plain text is fine.";
        puzzle_clarify_opt = Some(fsa_lm::puzzle_sketch_v1::PuzzleClarifyV1 {
            question_id: qid,
            score: 10_000,
            text: qtxt.to_string(),
            kind: fsa_lm::puzzle_sketch_v1::PuzzleClarifyKindV1::NeedConstraints,
        });

        // Build a best-effort sketch so we can persist pending puzzle state.
        let pscfg = fsa_lm::puzzle_sketch_v1::PuzzleSketchCfgV1::default();
        let mut lex_for_sketch: Option<Hash32> = lexicon_snapshot_hash;
        if lex_for_sketch.is_none() {
            if let Some(ws) = ws_opt {
                lex_for_sketch = ws.lexicon_snapshot;
            }
        }

        let mut view_opt: Option<fsa_lm::lexicon_expand_lookup::LexiconExpandLookupV1> = None;
        let mut cues_opt: Option<fsa_lm::lexicon_neighborhoods::LexiconCueNeighborhoodsV1> = None;
        if let Some(lh) = lex_for_sketch.as_ref() {
            match fsa_lm::lexicon_expand_lookup::load_lexicon_expand_lookup_v1(&store, lh) {
                Ok(Some(view)) => {
                    let ncfg = fsa_lm::lexicon_neighborhoods::LexiconNeighborhoodCfgV1::new();
                    let cues = fsa_lm::lexicon_neighborhoods::build_lexicon_cue_neighborhoods_v1(&view, &ncfg);
                    view_opt = Some(view);
                    cues_opt = Some(cues);
                    puzzle_sketch_used_lexicon = true;
                    puzzle_sketch_lex_hash_opt = Some(*lh);
                }
                Ok(None) => {}
                Err(_) => {}
            }
        }

        let sk = fsa_lm::puzzle_sketch_v1::build_puzzle_sketch_v1(
            &qtext,
            view_opt.as_ref(),
            cues_opt.as_ref(),
            pscfg,
        );
        puzzle_sketch_opt = Some(sk);
    } else {
        // Prefer pragmatics flags when present, but allow a lexicon-first free-text sketch
        // to trigger logic-puzzle clarification even without explicit pragmatics input.
        let mut is_logic_from_prag: bool = false;
        if let Some(pf) = pf_opt.as_ref() {
            let f = pf.flags;
            is_logic_from_prag = (f & fsa_lm::pragmatics_frame::INTENT_FLAG_IS_LOGIC_PUZZLE) != 0;
        }

        let pscfg = fsa_lm::puzzle_sketch_v1::PuzzleSketchCfgV1::default();
        let sk0 = fsa_lm::puzzle_sketch_v1::build_puzzle_sketch_v1(&qtext, None, None, pscfg);
        let mut is_logic = is_logic_from_prag || sk0.is_logic_puzzle_likely;

        if is_logic {
            // Best-effort lexicon view for lexicon-first sketching.
            let mut lex_for_sketch: Option<Hash32> = None;
            if lexicon_snapshot_hash.is_some() {
                lex_for_sketch = lexicon_snapshot_hash;
            }
            if lex_for_sketch.is_none() {
                if let Some(ws) = ws_opt {
                    lex_for_sketch = ws.lexicon_snapshot;
                }
            }

            let mut view_opt: Option<fsa_lm::lexicon_expand_lookup::LexiconExpandLookupV1> = None;
            let mut cues_opt: Option<fsa_lm::lexicon_neighborhoods::LexiconCueNeighborhoodsV1> = None;

            if let Some(lh) = lex_for_sketch.as_ref() {
                match fsa_lm::lexicon_expand_lookup::load_lexicon_expand_lookup_v1(&store, lh) {
                    Ok(Some(view)) => {
                        let ncfg = fsa_lm::lexicon_neighborhoods::LexiconNeighborhoodCfgV1::new();
                        let cues = fsa_lm::lexicon_neighborhoods::build_lexicon_cue_neighborhoods_v1(&view, &ncfg);
                        view_opt = Some(view);
                        cues_opt = Some(cues);
                    }
                    Ok(None) => {
                        // No lexicon; keep fallback sketch.
                    }
                    Err(_) => {
                        // Keep fallback sketch on lexicon load errors.
                    }
                }
            }

            let sk = fsa_lm::puzzle_sketch_v1::build_puzzle_sketch_v1(
                &qtext,
                view_opt.as_ref(),
                cues_opt.as_ref(),
                pscfg,
            );
            // Use the refined sketch to determine whether we still treat this as a puzzle.
            is_logic = is_logic_from_prag || sk.is_logic_puzzle_likely;
            if is_logic {
                puzzle_clarify_opt = fsa_lm::puzzle_sketch_v1::choose_puzzle_clarify_question_v1(&sk);
                puzzle_sketch_opt = Some(sk);
                if view_opt.is_some() {
                    puzzle_sketch_used_lexicon = true;
                    if let Some(lh) = lex_for_sketch.as_ref() {
                        puzzle_sketch_lex_hash_opt = Some(*lh);
                    }
                }
            }
        }
    }

    }

    // If the prompt looks like it contains constraint operators, but we could not
    // compile a parseable constraint set, ask for a constraint restatement.
    //
    // This avoids a "no clarify" outcome when the user provided constraints in a
    // format the v1 solver does not support.
    if puzzle_clarify_opt.is_none() && puzzle_constraints_parse_failed {
        let qid = derive_id64(b"forecast_question_v1", b"clarify:logic_puzzle:constraints_parse_failed");
        let qtxt = "I could not parse the constraints. Could you provide each constraint on its own line using forms like A != B, A < B, all_different: A,B,C, or if A = 1 then B != 2? Plain text is fine.";
        puzzle_clarify_opt = Some(fsa_lm::puzzle_sketch_v1::PuzzleClarifyV1 {
            question_id: qid,
            score: 10_000,
            text: qtxt.to_string(),
            kind: fsa_lm::puzzle_sketch_v1::PuzzleClarifyKindV1::NeedConstraints,
        });
    }

    if let Some(pq) = puzzle_clarify_opt.as_ref() {
        planner_hints.flags |= PH_FLAG_PREFER_CLARIFY | PH_FLAG_PREFER_STEPS;

        if puzzle_sketch_hash_opt.is_none() {
        // Persist a pending puzzle sketch for cross-turn continuation.
        if puzzle_sketch_opt.is_none() {
            let pscfg = fsa_lm::puzzle_sketch_v1::PuzzleSketchCfgV1::default();
            let sk = fsa_lm::puzzle_sketch_v1::build_puzzle_sketch_v1(&qtext, None, None, pscfg);
            puzzle_sketch_opt = Some(sk);
        }
        let src_hash = fsa_lm::puzzle_sketch_artifact::puzzle_sketch_source_hash_v1(&qtext);
        let psa = match fsa_lm::puzzle_sketch_artifact::PuzzleSketchArtifactV1::from_sketch(
            prompt_hash,
            query_msg_ix as u32,
            puzzle_sketch_used_lexicon,
            false,
            true,
            src_hash,
            puzzle_sketch_opt.as_ref().expect("puzzle_sketch"),
        ) {
            Ok(x) => x,
            Err(e) => {
                eprintln!("puzzle sketch: encode failed: {}", e);
                return Err(1);
            }
        };
        let psh = match fsa_lm::puzzle_sketch_artifact_store::put_puzzle_sketch_artifact_v1(&store, &psa) {
            Ok(h) => h,
            Err(e) => {
                eprintln!("puzzle sketch: store failed: {}", e);
                return Err(1);
            }
        };
        puzzle_sketch_hash_opt = Some(psh);
        }

        // Ensure the clarifying-question append can emit one question even when
        // no pragmatics-derived directives are present.
        if let Some(ref mut d) = directives_opt {
            if d.max_questions == 0 {
                d.max_questions = 1;
            }
        } else {
            directives_opt = Some(RealizerDirectivesV1 {
                version: REALIZER_DIRECTIVES_V1_VERSION,
                tone: ToneV1::Neutral,
                style: StyleV1::Checklist,
                format_flags: 0,
                max_softeners: 0,
                max_preface_sentences: 0,
                max_hedges: 0,
                max_questions: 1,
                rationale_codes: Vec::new(),
            });
        }

        forecast.questions.retain(|q| q.question_id != pq.question_id);
        forecast.questions.push(ForecastQuestionV1 {
            question_id: pq.question_id,
            score: pq.score as i64,
            text: pq.text.clone(),
            rationale_code: 0,
        });
        forecast.questions.sort_by(|a, b| match b.score.cmp(&a.score) {
            core::cmp::Ordering::Equal => a.question_id.0.cmp(&b.question_id.0),
            o => o,
        });
        if forecast.questions.len() > FORECAST_V1_MAX_QUESTIONS {
            forecast.questions.truncate(FORECAST_V1_MAX_QUESTIONS);
        }

        let iid = derive_id64(b"forecast_intent_v1", b"clarify:logic_puzzle");
        forecast.intents.retain(|it| !(it.kind == ForecastIntentKindV1::Clarify && it.intent_id == iid));
        forecast.intents.push(ForecastIntentV1::new(
            ForecastIntentKindV1::Clarify,
            iid,
            10_000,
            0,
        ));
        forecast.intents.sort_by(|a, b| match b.score.cmp(&a.score) {
            core::cmp::Ordering::Equal => match (a.kind as u8).cmp(&(b.kind as u8)) {
                core::cmp::Ordering::Equal => a.intent_id.0.cmp(&b.intent_id.0),
                o => o,
            },
            o => o,
        });
        if forecast.intents.len() > FORECAST_V1_MAX_INTENTS {
            forecast.intents.truncate(FORECAST_V1_MAX_INTENTS);
        }
    }

    }

    let mut exemplar_advisory_opt: Option<ExemplarAdvisoryV1> = None;
    if let Some(exh) = exemplar_memory_hash {
        let exemplar_memory_opt = match get_exemplar_memory_v1(&store, &exh) {
            Ok(Some(x)) => Some(x),
            Ok(None) => {
                if !exemplar_memory_from_workspace && !exemplar_memory_from_runtime_state {
                    eprintln!("missing exemplar memory: {}", hex32(&exh));
                    return Err(1);
                }
                exemplar_memory_hash = None;
                None
            }
            Err(e) => {
                eprintln!("exemplar-memory load failed: {}", e);
                return Err(1);
            }
        };
        if let Some(exemplar_memory) = exemplar_memory_opt.as_ref() {
            if let Some(advisory) = lookup_exemplar_advisory_v1(
                exemplar_memory,
                pf_opt.as_ref(),
                &planner_hints,
                directives_opt.as_ref(),
                Some(&qtext),
            ) {
                let _ = apply_exemplar_advisory_v1(&mut plan, &mut directives_opt, &advisory);
                exemplar_advisory_opt = Some(advisory);
            }
        }
    }

    let directives_hash_opt = match directives_opt.as_ref() {
        Some(d) => match put_realizer_directives_v1(&store, d) {
            Ok(h) => Some(h),
            Err(e) => {
                eprintln!("store directives failed: {}", e);
                return Err(1);
            }
        },
        None => None,
    };

    if let Some(mh) = markov_model_hash {
        let model_opt = match get_markov_model_v1(&store, &mh) {
            Ok(x) => x,
            Err(e) => {
                eprintln!("markov-model load failed: {}", e);
                return Err(1);
            }
        };
        let model = match model_opt {
            Some(x) => Some(x),
            None => {
                if !markov_model_from_workspace && !markov_model_from_runtime_state {
                    eprintln!("missing markov model: {}", hex32(&mh));
                    return Err(1);
                }
                markov_model_hash = None;
                None
            }
        };

        if let Some(model) = model.as_ref() {
            let hints_opt = derive_markov_hints_surface_choices_opt(
                query_id,
                !control.pragmatics_frame_ids.is_empty(),
                mh,
                model,
                directives_opt.as_ref(),
                markov_context_tokens,
                markov_max_choices,
            );

            if let Some(hints) = hints_opt {
                let hh = match put_markov_hints_v1(&store, &hints) {
                    Ok(h) => h,
                    Err(e) => {
                        eprintln!("store markov hints failed: {}", e);
                        return Err(1);
                    }
                };
                markov_hints_hash_opt = Some(hh);
                markov_hints_opt = Some(hints);
            }
        }
    }

    let planner_hints_hash = match put_planner_hints_v1(&store, &planner_hints) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("store planner hints failed: {}", e);
            return Err(1);
        }
    };

    let forecast_hash = match put_forecast_v1(&store, &forecast) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("store forecast failed: {}", e);
            return Err(1);
        }
    };

    let qr = match realize_with_quality_gate_v1(
        &store,
        &bundle,
        &plan,
        &rcfg,
        directives_opt.as_ref(),
        markov_hints_opt.as_ref(),
        &planner_hints,
        &forecast,
    ) {
        Ok(x) => x,
        Err(e) => {
            eprintln!("realize failed: {}", e);
            return Err(1);
        }
    };

    let inspect_lines: Vec<String> = if wants_inspect_lines_v1(presentation_mode) {
        let mut xs: Vec<String> = Vec::new();
        xs.push(build_routing_trace_line_v1(
            &planner_hints,
            &forecast,
            directives_opt.as_ref(),
        ));
        if enable_expand {
            if let Some(graph_hash) = graph_relevance_hash.as_ref() {
                if let Ok(Some(graph)) = get_graph_relevance_v1(&store, graph_hash) {
                    if let Some(line) = build_graph_trace_line_v1(&qtext, &qcfg, &graph, 2) {
                        xs.push(line);
                    }
                }
            }
        }
        if let Some(advisory) = exemplar_advisory_opt.as_ref() {
            xs.push(format!(
                "exemplar_match exemplar_id={} response_mode={:?} structure={:?} tone={:?} score={} support_count={} reasons={}",
                advisory.exemplar_id.0,
                advisory.response_mode,
                advisory.structure_kind,
                advisory.tone_kind,
                advisory.score,
                advisory.support_count,
                exemplar_match_reasons_v1(advisory),
            ));
        }
        xs
    } else {
        Vec::new()
    };

    let text = render_answer_surface_v1(presentation_mode, &qr.text, &inspect_lines);
    let did_append_q = qr.did_append_question;

    let answer_hash = match store.put(text.as_bytes()) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("store error: {}", e);
            return Err(1);
        }
    };

    //: Emit a MarkovTraceV1 token stream capturing the stable
    // surface-choice sequence used to render this answer.
    //
    // v1 emits structural placeholder tokens derived from AnswerPlan item kinds
    // and post-render append events. Starting in (Option B), when a
    // realizer surface-template site is wired, v1 also records the actual
    // template choice id used at that site.
    //
    // In (Option B), when a realizer surface-template site is wired,
    // MarkovTraceV1 records the actual template choice id used.
    //
    // For wired surface-template sites, use the realizer-reported
    // surface-choice events as the source of truth (no re-parsing of rendered
    // text).
    let mt_tokens: Vec<MarkovTokenV1> = build_markov_trace_tokens_v1(
        &plan,
        &qr.markov,
        did_append_q,
    );
    let mt_tokens_ret: Vec<MarkovTokenV1> = mt_tokens.clone();


    let trace = MarkovTraceV1 {
        version: MARKOV_TRACE_V1_VERSION,
        query_id,
        tokens: mt_tokens,
    };

    let markov_trace_hash = match put_markov_trace_v1(&store, &trace) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("store markov trace failed: {}", e);
            return Err(1);
        }
    };


    //: Build a minimal EvidenceSetV1 that binds the full answer text
    // to the rendered evidence rows (bounded by the realizer limit).
    let mut refs: Vec<EvidenceRowRefV1> = Vec::new();
    let max_refs = rcfg.max_evidence_items as usize;
    for it in bundle.items.iter().take(max_refs) {
        if let fsa_lm::evidence_bundle::EvidenceItemDataV1::Frame(r) = &it.data {
            refs.push(EvidenceRowRefV1 {
                segment_id: r.segment_id,
                row_ix: r.row_ix,
                score: it.score,
            });
        }
    }
    refs.sort_by(|a, b| {
        let o = a.segment_id.cmp(&b.segment_id);
        if o != std::cmp::Ordering::Equal {
            return o;
        }
        a.row_ix.cmp(&b.row_ix)
    });
    let mut uniq: Vec<EvidenceRowRefV1> = Vec::with_capacity(refs.len());
    for r in refs {
        if let Some(last) = uniq.last_mut() {
            if last.segment_id == r.segment_id && last.row_ix == r.row_ix {
                if r.score > last.score {
                    last.score = r.score;
                }
                continue;
            }
        }
        uniq.push(r);
    }

    let set = EvidenceSetV1 {
        version: 1,
        evidence_bundle_id: ev_hash,
        items: vec![EvidenceSetItemV1 {
            claim_id: 1,
            claim_text: text.clone(),
            evidence_refs: uniq,
        }],
    };

    if verify_trace == 1 {
        if let Err(e) = verify_evidence_set_v1(&store, &set) {
            eprintln!("verify-trace failed: {}", e);
            return Err(3);
        }
    }

    let set_hash = match put_evidence_set_v1(&store, &set) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("store error: {}", e);
            return Err(1);
        }
    };

    // Record the deterministic artifacts in a ReplayLog step.
    let mut log = ReplayLog::new();

    if let Some(dh) = directives_hash_opt {
        let mut dins: Vec<Hash32> = Vec::new();
        for h in control.pragmatics_frame_ids.iter() {
            dins.push(*h);
        }
        log.steps.push(ReplayStep {
            name: STEP_REALIZER_DIRECTIVES_V1.to_string(),
            inputs: dins,
            outputs: vec![dh],
        });
    }

    if let Some(ch) = context_anchors_hash_opt {
        let mut ins: Vec<Hash32> = Vec::new();
        ins.push(prompt_hash);
        if let Some(lh) = context_anchors_lex_hash_opt.as_ref() {
            ins.push(*lh);
        }
        log.steps.push(step_from_slices(STEP_CONTEXT_ANCHORS_V1, &ins, &[ch]));
    }

    if let Some(sh) = puzzle_sketch_hash_opt {
        let mut ins: Vec<Hash32> = Vec::new();
        ins.push(prompt_hash);
        if let Some(lh) = puzzle_sketch_lex_hash_opt.as_ref() {
            ins.push(*lh);
        }
        log.steps.push(step_from_slices(STEP_PUZZLE_SKETCH_V1, &ins, &[sh]));
    }

    if let Some(ph) = proof_hash_opt.as_ref() {
        let ph = *ph;
        let ins: [Hash32; 1] = [prompt_hash];
        log.steps.push(step_from_slices(STEP_PROOF_ARTIFACT_V1, &ins, &[ph]));
    }

    //: record MarkovHintsV1 derivation when enabled.
    if let (Some(model_hash), Some(hh)) = (markov_model_hash, markov_hints_hash_opt) {
        let mut mh_inputs: Vec<Hash32> = Vec::new();
        mh_inputs.push(prompt_hash);
        mh_inputs.push(snapshot_hash);
        if let Some(smh) = sig_map_hash.as_ref() {
            mh_inputs.push(*smh);
        }
        if enable_expand {
            if let Some(lh) = lexicon_snapshot_hash.as_ref() {
                mh_inputs.push(*lh);
            }
        }
        for h in control.pragmatics_frame_ids.iter() {
            mh_inputs.push(*h);
        }
        if let Some(dh) = directives_hash_opt {
            mh_inputs.push(dh);
        }
        mh_inputs.push(model_hash);
        log.steps.push(step_from_slices(STEP_MARKOV_HINTS_V1, &mh_inputs, &[hh]));
    }

    //: Record planner guidance artifacts in stable steps.
    // Inputs follow the contracts:
    // - planner-hints-v1 derives from evidence (+ pragmatics if present)
    // - forecast-v1 derives from planner-hints (+ pragmatics if present)
    let mut ph_inputs: Vec<Hash32> = Vec::new();
    for h in control.pragmatics_frame_ids.iter() {
        ph_inputs.push(*h);
    }
    ph_inputs.push(ev_hash);
    log.steps.push(step_from_slices(STEP_PLANNER_HINTS_V1, &ph_inputs, &[planner_hints_hash]));

    let mut fc_inputs: Vec<Hash32> = Vec::new();
    for h in control.pragmatics_frame_ids.iter() {
        fc_inputs.push(*h);
    }
    fc_inputs.push(planner_hints_hash);
    log.steps.push(step_from_slices(STEP_FORECAST_V1, &fc_inputs, &[forecast_hash]));

    let mut ins: Vec<Hash32> = Vec::new();
    ins.push(prompt_hash);
    ins.push(snapshot_hash);
    if let Some(smh) = sig_map_hash.as_ref() {
        ins.push(*smh);
    }
    if enable_expand {
        if let Some(lh) = lexicon_snapshot_hash.as_ref() {
            ins.push(*lh);
        }
    }
    for h in control.pragmatics_frame_ids.iter() {
        ins.push(*h);
    }
    ins.push(ev_hash);

    if let Some(ch) = context_anchors_hash_opt {
        ins.push(ch);
    }
    if let Some(exh) = exemplar_memory_hash {
        ins.push(exh);
    }

    // Include guidance hashes so the answer step input set captures the full
    // deterministic dependencies of the planning path.
    if let Some(dh) = directives_hash_opt {
        ins.push(dh);
    }

    if let Some(hh) = markov_hints_hash_opt {
        ins.push(hh);
    }
    ins.push(planner_hints_hash);
    ins.push(forecast_hash);

    let mut mt_inputs = ins.clone();
    mt_inputs.push(answer_hash);

    log.steps.push(ReplayStep {
        name: STEP_ANSWER_V1.to_string(),
        inputs: ins,
        outputs: vec![answer_hash, set_hash],
    });

    log.steps.push(step_from_slices(STEP_MARKOV_TRACE_V1, &mt_inputs, &[markov_trace_hash]));

    let replay_hash = match put_replay_log(&store, &log) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("store error: {}", e);
            return Err(1);
        }
    };

    Ok((text, out_file, mt_tokens_ret, replay_hash))
}


fn answer_run_text(args: &[String]) -> Result<(String, Option<PathBuf>), i32> {
    match answer_run_text_inner(args, &[]) {
        Ok((t, of, _mt, _rh)) => Ok((t, of)),
        Err(code) => Err(code),
    }
}


fn cmd_answer(args: &[String]) -> i32 {
    let (text, out_file) = match answer_run_text(args) {
        Ok(x) => x,
        Err(code) => return code,
    };

    if let Some(path) = out_file {
        if let Err(e) = fs::write(&path, text.as_bytes()) {
            eprintln!("write failed: {}", e);
            return 1;
        }
    }

    if let Err(e) = write_all_to_stdout(text.as_bytes()) {
        eprintln!("stdout error: {}", e);
        return 1;
    }

    0
}



fn cmd_build_markov_model(args: &[String]) -> i32 {
    let mut out_file: Option<String> = None;
    let mut root = default_root();
    let mut replay_hashes: Vec<Hash32> = Vec::new();
    let mut replay_file: Option<String> = None;

    let mut order_n_max: u8 = 3;
    let mut max_next_per_state: u8 = 8;
    let mut max_states: u32 = 8192;
    let mut max_replays: u32 = 0;
    let mut max_traces: u32 = 0;

    let mut i: usize = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--out-file" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("build-markov-model: missing value for --out-file");
                    return 2;
                }
                out_file = Some(args[i].to_string());
            }
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("build-markov-model: missing value for --root");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--replay" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("build-markov-model: missing value for --replay");
                    return 2;
                }
                let h = match parse_hash32_hex(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("build-markov-model: bad replay hash: {}", e);
                        return 2;
                    }
                };
                replay_hashes.push(h);
            }
            "--replay-file" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("build-markov-model: missing value for --replay-file");
                    return 2;
                }
                replay_file = Some(args[i].to_string());
            }
            "--order" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("build-markov-model: missing value for --order");
                    return 2;
                }
                order_n_max = match parse_u8(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("build-markov-model: {}", e);
                        return 2;
                    }
                };
            }
            "--max-next" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("build-markov-model: missing value for --max-next");
                    return 2;
                }
                max_next_per_state = match parse_u8(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("build-markov-model: {}", e);
                        return 2;
                    }
                };
            }
            "--max-states" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("build-markov-model: missing value for --max-states");
                    return 2;
                }
                max_states = match parse_u32(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("build-markov-model: {}", e);
                        return 2;
                    }
                };
            }
            "--max-replays" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("build-markov-model: missing value for --max-replays");
                    return 2;
                }
                max_replays = match parse_u32(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("build-markov-model: {}", e);
                        return 2;
                    }
                };
            }
            "--max-traces" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("build-markov-model: missing value for --max-traces");
                    return 2;
                }
                max_traces = match parse_u32(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("build-markov-model: {}", e);
                        return 2;
                    }
                };
            }
            _ => {
                eprintln!("build-markov-model: unknown arg {}", args[i]);
                return 2;
            }
        }
        i += 1;
    }

    if let Some(p) = replay_file {
        let bytes = match std::fs::read(&p) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("build-markov-model: read replay file failed: {}", e);
                return 1;
            }
        };
        let s = match std::str::from_utf8(&bytes) {
            Ok(x) => x,
            Err(_) => {
                eprintln!("build-markov-model: replay file is not UTF-8");
                return 1;
            }
        };
        for line in s.lines() {
            let t = line.trim();
            if t.is_empty() {
                continue;
            }
            let h = match parse_hash32_hex(t) {
                Ok(x) => x,
                Err(e) => {
                    eprintln!("build-markov-model: bad replay hash in file: {}", e);
                    return 2;
                }
            };
            replay_hashes.push(h);
        }
    }

    replay_hashes.sort();
    replay_hashes.dedup();
    if max_replays > 0 && (replay_hashes.len() as u32) > max_replays {
        replay_hashes.truncate(max_replays as usize);
    }

    if replay_hashes.is_empty() {
        eprintln!("build-markov-model: must provide at least one --replay or --replay-file");
        return 2;
    }

    let cfg = MarkovTrainCfgV1 { order_n_max, max_next_per_state, max_states };
    if let Err(e) = cfg.validate() {
        eprintln!("build-markov-model: invalid cfg: {}", e);
        return 2;
    }

    let store = store_for(&root);

    // Collect MarkovTrace hashes from the replay logs.
    let mut trace_hashes: Vec<Hash32> = Vec::new();
    for rh in &replay_hashes {
        let log = match get_replay_log(&store, rh) {
            Ok(Some(l)) => l,
            Ok(None) => {
                eprintln!("build-markov-model: missing replay log {}", hex32(rh));
                return 3;
            }
            Err(e) => {
                eprintln!("build-markov-model: load replay log failed: {}", e);
                return 1;
            }
        };
        for st in &log.steps {
            if st.name == STEP_MARKOV_TRACE_V1 {
                trace_hashes.extend_from_slice(&st.outputs);
            }
        }
    }

    if trace_hashes.is_empty() {
        eprintln!("build-markov-model: no markov-trace-v1 steps found in replay logs");
        return 2;
    }

    trace_hashes.sort();
    trace_hashes.dedup();

    if max_traces > 0 && (trace_hashes.len() as u32) > max_traces {
        trace_hashes.truncate(max_traces as usize);
    }

    let replay_summary = fsa_lm::scale_report::HashListSummaryV1::from_list("markov_replays_v1", &replay_hashes);
    let trace_summary = fsa_lm::scale_report::HashListSummaryV1::from_list("markov_traces_v1", &trace_hashes);

    let corpus_hash = match markov_corpus_hash_v1(&cfg, &trace_hashes) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("build-markov-model: corpus hash failed: {}", e);
            return 2;
        }
    };

    let mut trainer = match MarkovTrainerV1::new(cfg) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("build-markov-model: trainer init failed: {}", e);
            return 2;
        }
    };

    for th in &trace_hashes {
        let tr = match get_markov_trace_v1(&store, th) {
            Ok(Some(x)) => x,
            Ok(None) => {
                eprintln!("build-markov-model: missing markov trace {}", hex32(th));
                return 3;
            }
            Err(e) => {
                eprintln!("build-markov-model: load markov trace failed: {}", e);
                return 1;
            }
        };
        trainer.observe_trace(&tr);
    }

    let model = match trainer.build_model(corpus_hash) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("build-markov-model: build model failed: {}", e);
            return 1;
        }
    };

    let model_hash = match put_markov_model_v1(&store, &model) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("build-markov-model: store model failed: {}", e);
            return 1;
        }
    };

    let text = format!(
        "markov_model_v1 model_hash={} corpus_hash={} replays={} replay_list_hash={} replay_first={} replay_last={} traces={} trace_list_hash={} trace_first={} trace_last={} order_n_max={} max_next_per_state={} max_states={} max_replays={} max_traces={} states={} total_transitions={}
",
        hex32(&model_hash),
        hex32(&model.corpus_hash),
        replay_hashes.len(),
        hex32(&replay_summary.list_hash),
        hex32(&replay_summary.first),
        hex32(&replay_summary.last),
        trace_hashes.len(),
        hex32(&trace_summary.list_hash),
        hex32(&trace_summary.first),
        hex32(&trace_summary.last),
        model.order_n_max,
        model.max_next_per_state,
        cfg.max_states,
        max_replays,
        max_traces,
        model.states.len(),
        model.total_transitions,
    );

    if let Some(p) = out_file {
        if let Err(e) = std::fs::write(&p, text.as_bytes()) {
            eprintln!("build-markov-model: write failed: {}", e);
            return 1;
        }
    }

    if let Err(e) = write_all_to_stdout(text.as_bytes()) {
        eprintln!("build-markov-model: stdout error: {}", e);
        return 1;
    }

    0
}


fn cmd_build_exemplar_memory(args: &[String]) -> i32 {
    let mut out_file: Option<String> = None;
    let mut root = default_root();
    let mut cfg = ExemplarBuildConfigV1::default_v1();
    let mut inputs: Vec<ExemplarBuildInputV1> = Vec::new();

    let mut i: usize = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--out-file" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("build-exemplar-memory: missing value for --out-file");
                    return 2;
                }
                out_file = Some(args[i].to_string());
            }
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("build-exemplar-memory: missing value for --root");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--replay" | "--prompt" | "--golden-pack" | "--golden-pack-conversation"
            | "--conversation-pack" | "--markov-trace" => {
                let flag = args[i].clone();
                i += 1;
                if i >= args.len() {
                    eprintln!("build-exemplar-memory: missing value for {}", flag);
                    return 2;
                }
                let h = match parse_hash32_hex(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("build-exemplar-memory: bad hash for {}: {}", flag, e);
                        return 2;
                    }
                };
                let kind = match flag.as_str() {
                    "--replay" => ExemplarSupportSourceKindV1::ReplayLog,
                    "--prompt" => ExemplarSupportSourceKindV1::PromptPack,
                    "--golden-pack" => ExemplarSupportSourceKindV1::GoldenPack,
                    "--golden-pack-conversation" => {
                        ExemplarSupportSourceKindV1::GoldenPackConversation
                    }
                    "--conversation-pack" => ExemplarSupportSourceKindV1::ConversationPack,
                    "--markov-trace" => ExemplarSupportSourceKindV1::MarkovTrace,
                    _ => unreachable!(),
                };
                inputs.push(ExemplarBuildInputV1::new(kind, h));
            }
            "--max-inputs-total" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("build-exemplar-memory: missing value for --max-inputs-total");
                    return 2;
                }
                cfg.max_inputs_total = match parse_u32(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("build-exemplar-memory: {}", e);
                        return 2;
                    }
                };
            }
            "--max-inputs-per-source-kind" => {
                i += 1;
                if i >= args.len() {
                    eprintln!(
                        "build-exemplar-memory: missing value for --max-inputs-per-source-kind"
                    );
                    return 2;
                }
                cfg.max_inputs_per_source_kind = match parse_u32(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("build-exemplar-memory: {}", e);
                        return 2;
                    }
                };
            }
            "--max-rows" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("build-exemplar-memory: missing value for --max-rows");
                    return 2;
                }
                cfg.max_rows = match parse_u32(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("build-exemplar-memory: {}", e);
                        return 2;
                    }
                };
            }
            "--max-support-refs-per-row" => {
                i += 1;
                if i >= args.len() {
                    eprintln!(
                        "build-exemplar-memory: missing value for --max-support-refs-per-row"
                    );
                    return 2;
                }
                cfg.max_support_refs_per_row = match parse_u8(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("build-exemplar-memory: {}", e);
                        return 2;
                    }
                };
            }
            _ => {
                eprintln!("build-exemplar-memory: unknown arg {}", args[i]);
                return 2;
            }
        }
        i += 1;
    }

    if inputs.is_empty() {
        eprintln!("build-exemplar-memory: must provide at least one source input");
        return 2;
    }

    if let Err(e) = cfg.validate() {
        eprintln!("build-exemplar-memory: invalid cfg: {}", e);
        return 2;
    }

    let build_id = derive_exemplar_build_id_v1(&cfg, &inputs);
    let (plan, report) = match prepare_exemplar_build_plan_v1(build_id, inputs, &cfg) {
        Ok(x) => x,
        Err(e) => {
            eprintln!("build-exemplar-memory: build plan failed: {}", e);
            return 2;
        }
    };

    let store = store_for(&root);
    let mut loaded: Vec<LoadedExemplarSourceV1> = Vec::with_capacity(plan.inputs.len());
    for item in &plan.inputs {
        match item.source_kind {
            ExemplarSupportSourceKindV1::ReplayLog => {
                let artifact = match get_replay_log(&store, &item.source_hash) {
                    Ok(Some(x)) => x,
                    Ok(None) => {
                        eprintln!(
                            "build-exemplar-memory: missing {} {}",
                            exemplar_source_kind_name_v1(item.source_kind),
                            hex32(&item.source_hash)
                        );
                        return 3;
                    }
                    Err(e) => {
                        eprintln!(
                            "build-exemplar-memory: load {} failed: {}",
                            exemplar_source_kind_name_v1(item.source_kind),
                            e
                        );
                        return 1;
                    }
                };
                loaded.push(LoadedExemplarSourceV1::ReplayLog {
                    source_hash: item.source_hash,
                    artifact,
                });
            }
            ExemplarSupportSourceKindV1::PromptPack => {
                let artifact = match get_prompt_pack(&store, &item.source_hash) {
                    Ok(Some(x)) => x,
                    Ok(None) => {
                        eprintln!(
                            "build-exemplar-memory: missing {} {}",
                            exemplar_source_kind_name_v1(item.source_kind),
                            hex32(&item.source_hash)
                        );
                        return 3;
                    }
                    Err(e) => {
                        eprintln!(
                            "build-exemplar-memory: load {} failed: {}",
                            exemplar_source_kind_name_v1(item.source_kind),
                            e
                        );
                        return 1;
                    }
                };
                loaded.push(LoadedExemplarSourceV1::PromptPack {
                    source_hash: item.source_hash,
                    artifact,
                });
            }
            ExemplarSupportSourceKindV1::GoldenPack => {
                let artifact = match fsa_lm::golden_pack_artifact::get_golden_pack_report_v1(
                    &store,
                    &item.source_hash,
                ) {
                    Ok(Some(x)) => x,
                    Ok(None) => {
                        eprintln!(
                            "build-exemplar-memory: missing {} {}",
                            exemplar_source_kind_name_v1(item.source_kind),
                            hex32(&item.source_hash)
                        );
                        return 3;
                    }
                    Err(e) => {
                        eprintln!(
                            "build-exemplar-memory: load {} failed: {}",
                            exemplar_source_kind_name_v1(item.source_kind),
                            e
                        );
                        return 1;
                    }
                };
                loaded.push(LoadedExemplarSourceV1::GoldenPack {
                    source_hash: item.source_hash,
                    artifact,
                });
            }
            ExemplarSupportSourceKindV1::GoldenPackConversation => {
                let artifact =
                    match fsa_lm::golden_pack_conversation_artifact::get_golden_pack_conversation_report_v1(
                        &store,
                        &item.source_hash,
                    ) {
                        Ok(Some(x)) => x,
                        Ok(None) => {
                            eprintln!(
                                "build-exemplar-memory: missing {} {}",
                                exemplar_source_kind_name_v1(item.source_kind),
                                hex32(&item.source_hash)
                            );
                            return 3;
                        }
                        Err(e) => {
                            eprintln!(
                                "build-exemplar-memory: load {} failed: {}",
                                exemplar_source_kind_name_v1(item.source_kind),
                                e
                            );
                            return 1;
                        }
                    };
                loaded.push(LoadedExemplarSourceV1::GoldenPackConversation {
                    source_hash: item.source_hash,
                    artifact,
                });
            }
            ExemplarSupportSourceKindV1::ConversationPack => {
                let artifact = match get_conversation_pack(&store, &item.source_hash) {
                    Ok(Some(x)) => x,
                    Ok(None) => {
                        eprintln!(
                            "build-exemplar-memory: missing {} {}",
                            exemplar_source_kind_name_v1(item.source_kind),
                            hex32(&item.source_hash)
                        );
                        return 3;
                    }
                    Err(e) => {
                        eprintln!(
                            "build-exemplar-memory: load {} failed: {}",
                            exemplar_source_kind_name_v1(item.source_kind),
                            e
                        );
                        return 1;
                    }
                };
                loaded.push(LoadedExemplarSourceV1::ConversationPack {
                    source_hash: item.source_hash,
                    artifact,
                });
            }
            ExemplarSupportSourceKindV1::MarkovTrace => {
                let artifact = match get_markov_trace_v1(&store, &item.source_hash) {
                    Ok(Some(x)) => x,
                    Ok(None) => {
                        eprintln!(
                            "build-exemplar-memory: missing {} {}",
                            exemplar_source_kind_name_v1(item.source_kind),
                            hex32(&item.source_hash)
                        );
                        return 3;
                    }
                    Err(e) => {
                        eprintln!(
                            "build-exemplar-memory: load {} failed: {}",
                            exemplar_source_kind_name_v1(item.source_kind),
                            e
                        );
                        return 1;
                    }
                };
                loaded.push(LoadedExemplarSourceV1::MarkovTrace {
                    source_hash: item.source_hash,
                    artifact,
                });
            }
        }
    }

    let borrowed: Vec<ExemplarSourceArtifactV1<'_>> = loaded
        .iter()
        .map(LoadedExemplarSourceV1::as_borrowed)
        .collect();
    let rows = match mine_exemplar_rows_from_sources_v1(&plan, &borrowed) {
        Ok(x) => x,
        Err(e) => {
            eprintln!("build-exemplar-memory: mine rows failed: {}", e);
            return 1;
        }
    };
    let memory = match finalize_exemplar_memory_v1(&plan, rows) {
        Ok(x) => x,
        Err(e) => {
            eprintln!("build-exemplar-memory: finalize failed: {}", e);
            return 1;
        }
    };
    let exemplar_hash = match put_exemplar_memory_v1(&store, &memory) {
        Ok(x) => x,
        Err(e) => {
            eprintln!("build-exemplar-memory: store exemplar memory failed: {}", e);
            return 1;
        }
    };

    let text = format!(
        "exemplar_memory_v1 exemplar_hash={} build_id={} flags=0x{:08x} inputs_seen={} inputs_kept={} inputs_deduped={} inputs_dropped_by_cap={} rows={} loaded_sources={} max_rows={} max_support_refs_per_row={}
",
        hex32(&exemplar_hash),
        hex32(&memory.build_id),
        memory.flags,
        report.inputs_seen,
        report.inputs_kept,
        report.inputs_deduped,
        report.inputs_dropped_by_cap,
        memory.rows.len(),
        loaded.len(),
        plan.max_rows,
        plan.max_support_refs_per_row,
    );

    if let Some(p) = out_file {
        if let Err(e) = std::fs::write(&p, text.as_bytes()) {
            eprintln!("build-exemplar-memory: write failed: {}", e);
            return 1;
        }
    }
    if let Err(e) = write_all_to_stdout(text.as_bytes()) {
        eprintln!("build-exemplar-memory: stdout error: {}", e);
        return 1;
    }
    0
}

fn cmd_build_graph_relevance(args: &[String]) -> i32 {
    let mut out_file: Option<String> = None;
    let mut root = default_root();
    let mut cfg = GraphBuildConfigV1::default_v1();
    let mut inputs: Vec<GraphBuildInputV1> = Vec::new();

    let mut i: usize = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--out-file" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("build-graph-relevance: missing value for --out-file");
                    return 2;
                }
                out_file = Some(args[i].to_string());
            }
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("build-graph-relevance: missing value for --root");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--frame-segment" | "--replay" | "--prompt" | "--conversation-pack" => {
                let flag = args[i].clone();
                i += 1;
                if i >= args.len() {
                    eprintln!("build-graph-relevance: missing value for {}", flag);
                    return 2;
                }
                let h = match parse_hash32_hex(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("build-graph-relevance: bad hash for {}: {}", flag, e);
                        return 2;
                    }
                };
                let kind = match flag.as_str() {
                    "--frame-segment" => GraphBuildSourceKindV1::FrameSegment,
                    "--replay" => GraphBuildSourceKindV1::ReplayLog,
                    "--prompt" => GraphBuildSourceKindV1::PromptPack,
                    "--conversation-pack" => GraphBuildSourceKindV1::ConversationPack,
                    _ => unreachable!(),
                };
                inputs.push(GraphBuildInputV1::new(kind, h));
            }
            "--max-inputs-total" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("build-graph-relevance: missing value for --max-inputs-total");
                    return 2;
                }
                cfg.max_inputs_total = match parse_u32(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("build-graph-relevance: {}", e);
                        return 2;
                    }
                };
            }
            "--max-inputs-per-source-kind" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("build-graph-relevance: missing value for --max-inputs-per-source-kind");
                    return 2;
                }
                cfg.max_inputs_per_source_kind = match parse_u32(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("build-graph-relevance: {}", e);
                        return 2;
                    }
                };
            }
            "--max-rows" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("build-graph-relevance: missing value for --max-rows");
                    return 2;
                }
                cfg.max_rows = match parse_u32(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("build-graph-relevance: {}", e);
                        return 2;
                    }
                };
            }
            "--max-edges-per-row" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("build-graph-relevance: missing value for --max-edges-per-row");
                    return 2;
                }
                cfg.max_edges_per_row = match parse_u8(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("build-graph-relevance: {}", e);
                        return 2;
                    }
                };
            }
            "--max-terms-per-frame-row" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("build-graph-relevance: missing value for --max-terms-per-frame-row");
                    return 2;
                }
                cfg.max_terms_per_frame_row = match parse_u8(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("build-graph-relevance: {}", e);
                        return 2;
                    }
                };
            }
            "--max-entities-per-frame-row" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("build-graph-relevance: missing value for --max-entities-per-frame-row");
                    return 2;
                }
                cfg.max_entities_per_frame_row = match parse_u8(&args[i]) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("build-graph-relevance: {}", e);
                        return 2;
                    }
                };
            }
            _ => {
                eprintln!("build-graph-relevance: unknown arg {}", args[i]);
                return 2;
            }
        }
        i += 1;
    }

    if inputs.is_empty() {
        eprintln!("build-graph-relevance: must provide at least one source input");
        return 2;
    }
    if let Err(e) = cfg.validate() {
        eprintln!("build-graph-relevance: invalid cfg: {}", e);
        return 2;
    }

    let build_id = derive_graph_build_id_v1(&cfg, &inputs);
    let (plan, report) = match prepare_graph_build_plan_v1(build_id, inputs, &cfg) {
        Ok(x) => x,
        Err(e) => {
            eprintln!("build-graph-relevance: build plan failed: {}", e);
            return 2;
        }
    };

    let store = store_for(&root);
    let mut loaded: Vec<LoadedGraphSourceV1> = Vec::with_capacity(plan.inputs.len());
    for item in &plan.inputs {
        match item.source_kind {
            GraphBuildSourceKindV1::FrameSegment => {
                let artifact = match get_frame_segment_v1(&store, &item.source_hash) {
                    Ok(Some(x)) => x,
                    Ok(None) => {
                        eprintln!("build-graph-relevance: missing frame-segment {}", hex32(&item.source_hash));
                        return 3;
                    }
                    Err(e) => {
                        eprintln!("build-graph-relevance: load frame-segment failed: {}", e);
                        return 1;
                    }
                };
                loaded.push(LoadedGraphSourceV1::FrameSegment { source_hash: item.source_hash, artifact });
            }
            GraphBuildSourceKindV1::ReplayLog => {
                let artifact = match get_replay_log(&store, &item.source_hash) {
                    Ok(Some(x)) => x,
                    Ok(None) => {
                        eprintln!("build-graph-relevance: missing replay {}", hex32(&item.source_hash));
                        return 3;
                    }
                    Err(e) => {
                        eprintln!("build-graph-relevance: load replay failed: {}", e);
                        return 1;
                    }
                };
                loaded.push(LoadedGraphSourceV1::ReplayLog { source_hash: item.source_hash, artifact });
            }
            GraphBuildSourceKindV1::PromptPack => {
                let artifact = match get_prompt_pack(&store, &item.source_hash) {
                    Ok(Some(x)) => x,
                    Ok(None) => {
                        eprintln!("build-graph-relevance: missing prompt {}", hex32(&item.source_hash));
                        return 3;
                    }
                    Err(e) => {
                        eprintln!("build-graph-relevance: load prompt failed: {}", e);
                        return 1;
                    }
                };
                loaded.push(LoadedGraphSourceV1::PromptPack { source_hash: item.source_hash, artifact });
            }
            GraphBuildSourceKindV1::ConversationPack => {
                let artifact = match get_conversation_pack(&store, &item.source_hash) {
                    Ok(Some(x)) => x,
                    Ok(None) => {
                        eprintln!("build-graph-relevance: missing conversation-pack {}", hex32(&item.source_hash));
                        return 3;
                    }
                    Err(e) => {
                        eprintln!("build-graph-relevance: load conversation-pack failed: {}", e);
                        return 1;
                    }
                };
                loaded.push(LoadedGraphSourceV1::ConversationPack { source_hash: item.source_hash, artifact });
            }
        }
    }

    let borrowed: Vec<GraphSourceArtifactV1<'_>> = loaded.iter().map(LoadedGraphSourceV1::as_borrowed).collect();
    let rows = match mine_graph_rows_from_sources_v1(&plan, &borrowed) {
        Ok(x) => x,
        Err(e) => {
            eprintln!("build-graph-relevance: mine rows failed: {}", e);
            return 1;
        }
    };
    let graph = if rows.is_empty() {
        empty_graph_relevance_v1(&plan)
    } else {
        match finalize_graph_relevance_v1(&plan, rows) {
            Ok(x) => x,
            Err(e) => {
                eprintln!("build-graph-relevance: finalize failed: {}", e);
                return 1;
            }
        }
    };
    let graph_hash = match put_graph_relevance_v1(&store, &graph) {
        Ok(x) => x,
        Err(e) => {
            eprintln!("build-graph-relevance: store graph relevance failed: {}", e);
            return 1;
        }
    };

    let text = format!(
        "graph_relevance_v1 graph_hash={} build_id={} flags=0x{:08x} inputs_seen={} inputs_kept={} inputs_deduped={} inputs_dropped_by_cap={} rows={} loaded_sources={} max_rows={} max_edges_per_row={} max_terms_per_frame_row={} max_entities_per_frame_row={}
",
        hex32(&graph_hash),
        hex32(&graph.build_id),
        graph.flags,
        report.inputs_seen,
        report.inputs_kept,
        report.inputs_deduped,
        report.inputs_dropped_by_cap,
        graph.rows.len(),
        loaded.len(),
        plan.max_rows,
        plan.max_edges_per_row,
        plan.max_terms_per_frame_row,
        plan.max_entities_per_frame_row,
    );

    if let Some(p) = out_file {
        if let Err(e) = std::fs::write(&p, text.as_bytes()) {
            eprintln!("build-graph-relevance: write failed: {}", e);
            return 1;
        }
    }
    if let Err(e) = write_all_to_stdout(text.as_bytes()) {
        eprintln!("build-graph-relevance: stdout error: {}", e);
        return 1;
    }
    0
}

fn cmd_inspect_markov_model(args: &[String]) -> i32 {
    use fsa_lm::markov_model_artifact::get_markov_model_v1;

    let mut out_file: Option<String> = None;
    let mut root = default_root();
    let mut model_hash: Option<Hash32> = None;
    let mut top_states: u32 = 0;
    let mut top_next: u32 = 0;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--out-file" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("inspect-markov-model: missing value for --out-file");
                    return 2;
                }
                out_file = Some(args[i].to_string());
            }
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("inspect-markov-model: missing value for --root");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--model" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("inspect-markov-model: missing value for --model");
                    return 2;
                }
                model_hash = match parse_hash32_hex(&args[i]) {
                    Ok(h) => Some(h),
                    Err(e) => {
                        eprintln!("inspect-markov-model: bad model hash: {}", e);
                        return 2;
                    }
                };
            }
            "--top-states" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("inspect-markov-model: missing value for --top-states");
                    return 2;
                }
                top_states = match parse_u32(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("inspect-markov-model: {}", e);
                        return 2;
                    }
                };
            }
            "--top-next" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("inspect-markov-model: missing value for --top-next");
                    return 2;
                }
                top_next = match parse_u32(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("inspect-markov-model: {}", e);
                        return 2;
                    }
                };
            }
            _ => {
                eprintln!("inspect-markov-model: unknown arg: {}", args[i]);
                return 2;
            }
        }
        i += 1;
    }

    let model_hash = match model_hash {
        Some(h) => h,
        None => {
            eprintln!("inspect-markov-model: missing --model");
            return 2;
        }
    };

    if top_states > 100 {
        eprintln!("inspect-markov-model: --top-states too large (max 100)");
        return 2;
    }
    if top_next > 32 {
        eprintln!("inspect-markov-model: --top-next too large (max 32)");
        return 2;
    }

    let store = store_for(&root);

    let model = match get_markov_model_v1(&store, &model_hash) {
        Ok(Some(m)) => m,
        Ok(None) => {
            eprintln!("inspect-markov-model: not found");
            return 3;
        }
        Err(e) => {
            eprintln!("inspect-markov-model: {}", e);
            return 1;
        }
    };

    if let Err(e) = model.validate() {
        eprintln!("inspect-markov-model: invalid model: {}", e);
        return 2;
    }

    fn kind_tag(k: fsa_lm::markov_hints::MarkovChoiceKindV1) -> &'static str {
        match k {
            fsa_lm::markov_hints::MarkovChoiceKindV1::Opener => "O",
            fsa_lm::markov_hints::MarkovChoiceKindV1::Transition => "T",
            fsa_lm::markov_hints::MarkovChoiceKindV1::Closer => "C",
            fsa_lm::markov_hints::MarkovChoiceKindV1::Other => "X",
        }
    }

    fn fmt_ctx(ctx: &[MarkovTokenV1]) -> String {
        let mut s = String::new();
        s.push('[');
        for (ix, t) in ctx.iter().enumerate() {
            if ix > 0 {
                s.push(',');
            }
            s.push_str(kind_tag(t.kind));
            s.push(':');
            s.push_str(&t.choice_id.0.to_string());
        }
        s.push(']');
        s
    }

    fn fmt_next(next: &[fsa_lm::markov_model::MarkovNextV1], n: usize) -> String {
        let mut s = String::new();
        s.push('[');
        for (ix, ent) in next.iter().take(n).enumerate() {
            if ix > 0 {
                s.push(',');
            }
            s.push_str(kind_tag(ent.token.kind));
            s.push(':');
            s.push_str(&ent.token.choice_id.0.to_string());
            s.push('x');
            s.push_str(&ent.count.to_string());
        }
        s.push(']');
        s
    }

    let mut out = String::new();
    out.push_str(&format!(
        "markov_model_inspect_v1 model_hash={} corpus_hash={} order_n_max={} max_next_per_state={} states={} total_transitions={}\n",
        hex32(&model_hash),
        hex32(&model.corpus_hash),
        model.order_n_max,
        model.max_next_per_state,
        model.states.len(),
        model.total_transitions
    ));

    if top_states > 0 {
        let mut sums: Vec<(u64, usize)> = Vec::with_capacity(model.states.len());
        for (idx, st) in model.states.iter().enumerate() {
            let mut sum: u64 = 0;
            for n in &st.next {
                sum += n.count as u64;
            }
            sums.push((sum, idx));
        }

        sums.sort_by(|a, b| {
            // Rank:
            // - out desc
            // - ctx_len desc
            // - ctx asc
            match b.0.cmp(&a.0) {
                core::cmp::Ordering::Equal => {}
                o => return o,
            }
            let sa = &model.states[a.1];
            let sb = &model.states[b.1];
            match sb.context.len().cmp(&sa.context.len()) {
                core::cmp::Ordering::Equal => {}
                o => return o,
            }
            sa.context.cmp(&sb.context)
        });

        let take = core::cmp::min(top_states as usize, sums.len());
        let nn = top_next as usize;
        for rank in 0..take {
            let (out_sum, idx) = sums[rank];
            let st = &model.states[idx];
            let ctx = fmt_ctx(&st.context);
            let next_s = if nn > 0 { fmt_next(&st.next, nn) } else { "[]".to_string() };
            out.push_str(&format!(
                "markov_model_state_v1 rank={} idx={} ctx_len={} out={} ctx={} next={}\n",
                rank,
                idx,
                st.context.len(),
                out_sum,
                ctx,
                next_s
            ));
        }
    }

    if let Some(path) = out_file {
        if let Err(e) = std::fs::write(&path, out.as_bytes()) {
            eprintln!("inspect-markov-model: failed to write out-file: {}", e);
            return 1;
        }
    }

    if let Err(e) = write_all_to_stdout(out.as_bytes()) {
        eprintln!("write failed: {}", e);
        return 1;
    }

    0
}

fn cmd_scale_demo(args: &[String]) -> i32 {
    let mut out_file: Option<String> = None;
    let mut root = default_root();
    let mut ingest: u8 = 0;
    let mut build_index: u8 = 0;
    let mut prompts: u8 = 0;
    let mut evidence: u8 = 0;
    let mut answer: u8 = 0;

    let mut docs: u32 = 64;
    let mut queries: u32 = 64;
    let mut min_doc_tokens: u32 = 24;
    let mut max_doc_tokens: u32 = 48;
    let mut vocab: u32 = 512;
    let mut query_tokens: u32 = 6;
    let mut seed: u64 = 1;
    let mut tie_pair: u8 = 0;

    let mut i: usize = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--out-file" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("scale-demo: missing value for --out-file");
                    return 2;
                }
                out_file = Some(args[i].to_string());
            }
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("scale-demo: missing value for --root");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--ingest" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("scale-demo: missing value for --ingest");
                    return 2;
                }
                match parse_u8(&args[i]) {
                    Ok(v) => {
                        if v > 1 {
                            eprintln!("scale-demo: --ingest must be 0 or 1");
                            return 2;
                        }
                        ingest = v;
                    }
                    Err(e) => {
                        eprintln!("scale-demo: bad --ingest: {e}");
                        return 2;
                    }
                }
            }
            "--build_index" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("scale-demo: missing value for --build_index");
                    return 2;
                }
                match parse_u8(&args[i]) {
                    Ok(v) => {
                        if v > 1 {
                            eprintln!("scale-demo: --build_index must be 0 or 1");
                            return 2;
                        }
                        build_index = v;
                    }
                    Err(e) => {
                        eprintln!("scale-demo: bad --build_index: {e}");
                        return 2;
                    }
                }
            }
            "--prompts" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("scale-demo: missing value for --prompts");
                    return 2;
                }
                match parse_u8(&args[i]) {
                    Ok(v) => {
                        if v > 1 {
                            eprintln!("scale-demo: --prompts must be 0 or 1");
                            return 2;
                        }
                        prompts = v;
                    }
                    Err(e) => {
                        eprintln!("scale-demo: bad --prompts: {e}");
                        return 2;
                    }
                }
            }
            "--evidence" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("scale-demo: missing value for --evidence");
                    return 2;
                }
                match parse_u8(&args[i]) {
                    Ok(v) => {
                        if v > 1 {
                            eprintln!("scale-demo: --evidence must be 0 or 1");
                            return 2;
                        }
                        evidence = v;
                    }
                    Err(e) => {
                        eprintln!("scale-demo: bad --evidence: {e}");
                        return 2;
                    }
                }
            }
            "--answer" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("scale-demo: missing value for --answer");
                    return 2;
                }
                match parse_u8(&args[i]) {
                    Ok(v) => {
                        if v > 1 {
                            eprintln!("scale-demo: --answer must be 0 or 1");
                            return 2;
                        }
                        answer = v;
                    }
                    Err(e) => {
                        eprintln!("scale-demo: bad --answer: {e}");
                        return 2;
                    }
                }
            }

            "--docs" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("scale-demo: missing value for --docs");
                    return 2;
                }
                match parse_u32(&args[i]) {
                    Ok(v) => docs = v,
                    Err(e) => {
                        eprintln!("scale-demo: bad --docs: {e}");
                        return 2;
                    }
                }
            }
            "--queries" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("scale-demo: missing value for --queries");
                    return 2;
                }
                match parse_u32(&args[i]) {
                    Ok(v) => queries = v,
                    Err(e) => {
                        eprintln!("scale-demo: bad --queries: {e}");
                        return 2;
                    }
                }
            }
            "--min_doc_tokens" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("scale-demo: missing value for --min_doc_tokens");
                    return 2;
                }
                match parse_u32(&args[i]) {
                    Ok(v) => min_doc_tokens = v,
                    Err(e) => {
                        eprintln!("scale-demo: bad --min_doc_tokens: {e}");
                        return 2;
                    }
                }
            }
            "--max_doc_tokens" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("scale-demo: missing value for --max_doc_tokens");
                    return 2;
                }
                match parse_u32(&args[i]) {
                    Ok(v) => max_doc_tokens = v,
                    Err(e) => {
                        eprintln!("scale-demo: bad --max_doc_tokens: {e}");
                        return 2;
                    }
                }
            }
            "--vocab" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("scale-demo: missing value for --vocab");
                    return 2;
                }
                match parse_u32(&args[i]) {
                    Ok(v) => vocab = v,
                    Err(e) => {
                        eprintln!("scale-demo: bad --vocab: {e}");
                        return 2;
                    }
                }
            }
            "--query_tokens" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("scale-demo: missing value for --query_tokens");
                    return 2;
                }
                match parse_u32(&args[i]) {
                    Ok(v) => query_tokens = v,
                    Err(e) => {
                        eprintln!("scale-demo: bad --query_tokens: {e}");
                        return 2;
                    }
                }
            }
            "--seed" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("scale-demo: missing value for --seed");
                    return 2;
                }
                match parse_u64(&args[i]) {
                    Ok(v) => seed = v,
                    Err(e) => {
                        eprintln!("scale-demo: bad --seed: {e}");
                        return 2;
                    }
                }
            }
            "--tie_pair" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("scale-demo: missing value for --tie_pair");
                    return 2;
                }
                match parse_u8(&args[i]) {
                    Ok(v) => {
                        if v > 1 {
                            eprintln!("scale-demo: --tie_pair must be 0 or 1");
                            return 2;
                        }
                        tie_pair = v;
                    }
                    Err(e) => {
                        eprintln!("scale-demo: bad --tie_pair: {e}");
                        return 2;
                    }
                }
            }
            _ => {
                eprintln!("scale-demo: unknown arg: {}", args[i]);
                return 2;
            }
        }
        i += 1;
    }

    let min_doc_tokens_u16 = if min_doc_tokens > (u16::MAX as u32) {
        eprintln!("scale-demo: --min_doc_tokens must be <= 65535");
        return 2;
    } else {
        min_doc_tokens as u16
    };
    let max_doc_tokens_u16 = if max_doc_tokens > (u16::MAX as u32) {
        eprintln!("scale-demo: --max_doc_tokens must be <= 65535");
        return 2;
    } else {
        max_doc_tokens as u16
    };
    let query_tokens_u16 = if query_tokens > (u16::MAX as u32) {
        eprintln!("scale-demo: --query_tokens must be <= 65535");
        return 2;
    } else {
        query_tokens as u16
    };


    let wcfg = WorkloadCfgV1 {
        version: WORKLOAD_GEN_V1_VERSION,
        seed,
        doc_count: docs,
        query_count: queries,
        min_tokens_per_doc: min_doc_tokens_u16,
        max_tokens_per_doc: max_doc_tokens_u16,
        vocab_size: vocab,
        query_tokens: query_tokens_u16,
        include_tie_pair: tie_pair,
    };


    let cfg = ScaleDemoCfgV1 {
        version: SCALE_DEMO_V1_VERSION,
        workload: wcfg,
    };

    if ingest == 0 && build_index == 1 {
        eprintln!("scale-demo: --build_index requires --ingest 1");
        return 2;
    }

    if ingest == 0 && (prompts == 1 || evidence == 1 || answer == 1) {
        eprintln!("scale-demo: --prompts/--evidence/--answer require --ingest 1");
        return 2;
    }

    if evidence == 1 && build_index == 0 {
        eprintln!("scale-demo: --evidence requires --build_index 1");
        return 2;
    }

    if answer == 1 && evidence == 0 {
        eprintln!("scale-demo: --answer requires --evidence 1");
        return 2;
    }

    if ingest == 0 {
        let report = match run_scale_demo_generate_only_v1(cfg) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("scale-demo: {e}");
                return 2;
            }
        };

        let mut out = report.to_string();
        out.push('\n');

        if let Some(p) = out_file {
            if let Err(e) = std::fs::write(&p, out.as_bytes()) {
                eprintln!("scale-demo: write failed: {e}");
                return 2;
            }
        } else {
            print!("{out}");
        }
        return 0;
    }

    let store = match FsArtifactStore::new(&root) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("scale-demo: bad --root: {e}");
            return 2;
        }
    };

    let (report, frames) = match run_scale_demo_generate_and_ingest_frames_v1(&store, cfg) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("scale-demo ingest: {e}");
            return 2;
        }
    };

    let index_report_opt = if build_index == 0 {
        None
    } else {
        match run_scale_demo_build_index_from_manifest_v1(&store, &frames.frame_manifest_hash) {
            Ok(r) => Some(r),
            Err(e) => {
                eprintln!("scale-demo index: {e}");
                return 2;
            }
        }
    };

    let prompts_report_opt = if prompts == 0 {
        None
    } else {
        match run_scale_demo_generate_and_store_prompts_v1(&store, cfg) {
            Ok(r) => Some(r),
            Err(e) => {
                eprintln!("scale-demo prompts: {e}");
                return 2;
            }
        }
    };

    let evidence_report_opt = if evidence == 0 {
        None
    } else {
        let ix = index_report_opt.as_ref().expect("validated --evidence requires index");
        match run_scale_demo_build_evidence_bundles_v1(&store, cfg, &ix.index_snapshot_hash, &ix.index_sig_map_hash) {
            Ok(r) => Some(r),
            Err(e) => {
                eprintln!("scale-demo evidence: {e}");
                return 2;
            }
        }
    };

    let answers_report_opt = if answer == 0 {
        None
    } else {
        let ev = evidence_report_opt.as_ref().expect("validated --answer requires evidence");
        match run_scale_demo_build_answers_v1(&store, ev) {
            Ok(r) => Some(r),
            Err(e) => {
                eprintln!("scale-demo answer: {e}");
                return 2;
            }
        }
    };


    let scale_rep = match build_scale_demo_scale_report_v1(
        &report,
        &frames,
        index_report_opt.as_ref(),
        prompts_report_opt.as_ref(),
        evidence_report_opt.as_ref(),
        answers_report_opt.as_ref(),
    ) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("scale-demo scale-report: {e}");
            return 2;
        }
    };

    let scale_hash = match put_scale_demo_scale_report_v1(&store, &scale_rep) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("scale-demo scale-report store: {e}");
            return 2;
        }
    };

    let mut out = String::new();
    out.push_str(&report.to_string());
    out.push('\n');
    out.push_str(&frames.to_string());
    out.push('\n');

    if let Some(ix) = index_report_opt.as_ref() {
        out.push_str(&ix.to_string());
        out.push('\n');
    }

    if let Some(pr) = prompts_report_opt.as_ref() {
        out.push_str(&pr.to_string());
        out.push('\n');
    }

    if let Some(er) = evidence_report_opt.as_ref() {
        out.push_str(&er.to_string());
        out.push('\n');
    }

    if let Some(ar) = answers_report_opt.as_ref() {
        out.push_str(&ar.to_string());
        out.push('\n');
    }


    out.push_str("scale_demo_scale_report_v3 ");
    out.push_str("report=");
    out.push_str(&hex32(&scale_hash));
    out.push_str(" workload_hash=");
    out.push_str(&hex32(&scale_rep.workload_hash));
    out.push_str(" docs=");
    out.push_str(&scale_rep.doc_count.to_string());
    out.push_str(" queries=");
    out.push_str(&scale_rep.query_count.to_string());
    out.push_str(" tie_pair=");
    out.push_str(&scale_rep.tie_pair.to_string());
    out.push_str(" seed=");
    out.push_str(&scale_rep.seed.to_string());
    out.push_str(" manifest=");
    out.push_str(&hex32(&scale_rep.frame_manifest_hash));
    out.push_str(" index_present=");
    out.push_str(&scale_rep.has_index.to_string());
    out.push_str(" prompts_present=");
    out.push_str(&scale_rep.has_prompts.to_string());
    out.push_str(" evidence_present=");
    out.push_str(&scale_rep.has_evidence.to_string());
    out.push_str(" answers_present=");
    out.push_str(&scale_rep.has_answers.to_string());

    if scale_rep.has_index != 0 {
        out.push_str(" snapshot=");
        out.push_str(&hex32(&scale_rep.index_snapshot_hash));
        out.push_str(" sig_map=");
        out.push_str(&hex32(&scale_rep.index_sig_map_hash));
    }

    if scale_rep.has_prompts != 0 {
        out.push_str(" prompts_list_hash=");
        out.push_str(&hex32(&scale_rep.prompts.list_hash));
        out.push_str(" prompts_count=");
        out.push_str(&scale_rep.prompts.count.to_string());
    }

    if scale_rep.has_evidence != 0 {
        out.push_str(" evidence_list_hash=");
        out.push_str(&hex32(&scale_rep.evidence.list_hash));
        out.push_str(" evidence_count=");
        out.push_str(&scale_rep.evidence.count.to_string());
    }

    if scale_rep.has_answers != 0 {
        out.push_str(" answers_list_hash=");
        out.push_str(&hex32(&scale_rep.answers.list_hash));
        out.push_str(" answers_count=");
        out.push_str(&scale_rep.answers.count.to_string());
        out.push_str(" planner_hints_list_hash=");
        out.push_str(&hex32(&scale_rep.planner_hints.list_hash));
        out.push_str(" planner_hints_count=");
        out.push_str(&scale_rep.planner_hints.count.to_string());
        out.push_str(" forecasts_list_hash=");
        out.push_str(&hex32(&scale_rep.forecasts.list_hash));
        out.push_str(" forecasts_count=");
        out.push_str(&scale_rep.forecasts.count.to_string());

        out.push_str(" markov_traces_list_hash=");
        out.push_str(&hex32(&scale_rep.markov_traces.list_hash));
        out.push_str(" markov_traces_count=");
        out.push_str(&scale_rep.markov_traces.count.to_string());
    }

    out.push('\n');

    if let Some(p) = out_file {
        if let Err(e) = std::fs::write(&p, out.as_bytes()) {
            eprintln!("scale-demo: write failed: {e}");
            return 2;
        }
    } else {
        print!("{out}");
    }

    0
}

fn cmd_golden_pack(args: &[String]) -> i32 {
    let mut root_arg: Option<String> = None;
    let mut out_file: Option<String> = None;
    let mut expect_hex: Option<String> = None;

    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--root" => {
                root_arg = it.next().cloned();
            }
            "--out-file" => {
                out_file = it.next().cloned();
            }
            "--expect" => {
                expect_hex = it.next().cloned();
            }
            _ => {
                eprintln!("unknown arg: {}", a);
                return 2;
            }
        }
    }

    let root = if let Some(r) = root_arg {
        PathBuf::from(r)
    } else {
        default_root()
    };

    let store = match FsArtifactStore::new(&root) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("failed to open artifact store at {}: {}", root.display(), e);
            return 2;
        }
    };

    let cfg = fsa_lm::golden_pack_run::GoldenPackRunCfgV1::default_tiny_v1();
    let out = match fsa_lm::golden_pack_run::run_golden_pack_v1(&store, cfg) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("golden pack failed: {}", e);
            return 2;
        }
    };

    let line = fsa_lm::golden_pack_run::format_golden_pack_run_line(&out);

    let expect = expect_hex.or_else(|| std::env::var("FSA_LM_GOLDEN_PACK_V1_REPORT_HEX").ok());
    if let Some(hex) = expect {
        match parse_hash32_hex(&hex) {
            Ok(h) => {
                if h != out.report_hash {
                    eprintln!(
                        "golden pack mismatch: expected={} got={}",
                        hex,
                        fsa_lm::hash::hex32(&out.report_hash)
                    );
                    return 2;
                }
            }
            Err(e) => {
                eprintln!("invalid --expect hash32hex: {}", e);
                return 2;
            }
        }
    }

    if let Some(p) = out_file {
        let bytes = format!("{}\n", line);
        if let Err(e) = std::fs::write(&PathBuf::from(p), bytes.as_bytes()) {
            eprintln!("failed to write out-file: {}", e);
            return 2;
        }
    }

    println!("{}", line);
    0
}

fn cmd_golden_pack_turn_pairs(args: &[String]) -> i32 {
    let mut root_arg: Option<String> = None;
    let mut out_file: Option<String> = None;
    let mut expect_hex: Option<String> = None;

    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--root" => {
                root_arg = it.next().cloned();
            }
            "--out-file" => {
                out_file = it.next().cloned();
            }
            "--expect" => {
                expect_hex = it.next().cloned();
            }
            _ => {
                eprintln!("unknown arg: {}", a);
                return 2;
            }
        }
    }

    let root = if let Some(r) = root_arg {
        PathBuf::from(r)
    } else {
        default_root()
    };

    let store = match FsArtifactStore::new(&root) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("failed to open artifact store at {}: {}", root.display(), e);
            return 2;
        }
    };

    let cfg = fsa_lm::golden_pack_turn_pairs_run::GoldenPackTurnPairsRunCfgV1::default_tiny_v1();
    let out = match fsa_lm::golden_pack_turn_pairs_run::run_golden_pack_turn_pairs_v1(&store, cfg)
    {
        Ok(o) => o,
        Err(e) => {
            eprintln!("golden pack turn-pairs failed: {}", e);
            return 2;
        }
    };

    let line = fsa_lm::golden_pack_turn_pairs_run::format_golden_pack_turn_pairs_run_line(&out);

    let expect = expect_hex
        .or_else(|| std::env::var("FSA_LM_GOLDEN_PACK_TURN_PAIRS_V1_REPORT_HEX").ok());
    if let Some(hex) = expect {
        match parse_hash32_hex(&hex) {
            Ok(h) => {
                if h != out.report_hash {
                    eprintln!(
                        "golden pack turn-pairs mismatch: expected={} got={}",
                        hex,
                        fsa_lm::hash::hex32(&out.report_hash)
                    );
                    return 2;
                }
            }
            Err(e) => {
                eprintln!("invalid --expect hash32hex: {}", e);
                return 2;
            }
        }
    }

    if let Some(p) = out_file {
        let bytes = format!("{}\n", line);
        if let Err(e) = std::fs::write(&PathBuf::from(p), bytes.as_bytes()) {
            eprintln!("failed to write out-file: {}", e);
            return 2;
        }
    }

    println!("{}", line);
    0
}

fn cmd_golden_pack_conversation(args: &[String]) -> i32 {
    let mut root_arg: Option<String> = None;
    let mut out_file: Option<String> = None;
    let mut expect_hex: Option<String> = None;

    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--root" => {
                root_arg = it.next().cloned();
            }
            "--out-file" => {
                out_file = it.next().cloned();
            }
            "--expect" => {
                expect_hex = it.next().cloned();
            }
            _ => {
                eprintln!("unknown arg: {}", a);
                return 2;
            }
        }
    }

    let root = if let Some(r) = root_arg {
        PathBuf::from(r)
    } else {
        default_root()
    };

    let store = match FsArtifactStore::new(&root) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("failed to open artifact store at {}: {}", root.display(), e);
            return 2;
        }
    };

    let cfg = fsa_lm::golden_pack_conversation_run::GoldenPackConversationRunCfgV1::default_tiny_v1();
    let out = match fsa_lm::golden_pack_conversation_run::run_golden_pack_conversation_v1(&store, cfg)
    {
        Ok(o) => o,
        Err(e) => {
            eprintln!("golden pack conversation failed: {}", e);
            return 2;
        }
    };

    let line = fsa_lm::golden_pack_conversation_run::format_golden_pack_conversation_run_line(&out);

    let expect = expect_hex
        .or_else(|| std::env::var("FSA_LM_GOLDEN_PACK_CONVERSATION_V1_REPORT_HEX").ok());
    if let Some(hex) = expect {
        match parse_hash32_hex(&hex) {
            Ok(h) => {
                if h != out.report_hash {
                    eprintln!(
                        "golden pack conversation mismatch: expected={} got={}",
                        hex,
                        fsa_lm::hash::hex32(&out.report_hash)
                    );
                    return 2;
                }
            }
            Err(e) => {
                eprintln!("invalid --expect hash32hex: {}", e);
                return 2;
            }
        }
    }

    if let Some(p) = out_file {
        let bytes = format!("{}\n", line);
        if let Err(e) = std::fs::write(&PathBuf::from(p), bytes.as_bytes()) {
            eprintln!("failed to write out-file: {}", e);
            return 2;
        }
    }

    println!("{}", line);
    0
}

fn cmd_replay_add_prompt(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut replay_hex: Option<&str> = None;
    let mut prompt_hex: Option<&str> = None;
    let mut step_name: &str = "prompt";

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--name" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --name value");
                    return 2;
                }
                step_name = &args[i];
            }
            x => {
                if replay_hex.is_none() {
                    replay_hex = Some(x);
                } else if prompt_hex.is_none() {
                    prompt_hex = Some(x);
                } else {
                    eprintln!("unexpected arg: {}", x);
                    return 2;
                }
            }
        }
        i += 1;
    }

    let rh = match replay_hex {
        Some(x) => x,
        None => {
            eprintln!("missing replay_hash_hex");
            return 2;
        }
    };
    let ph = match prompt_hex {
        Some(x) => x,
        None => {
            eprintln!("missing prompt_hash_hex");
            return 2;
        }
    };

    let replay_h = match parse_hash32_hex(rh) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("{}", e);
            return 2;
        }
    };
    let prompt_h = match parse_hash32_hex(ph) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("{}", e);
            return 2;
        }
    };

    let store = store_for(&root);
    let mut log = match get_replay_log(&store, &replay_h) {
        Ok(Some(v)) => v,
        Ok(None) => {
            eprintln!("replay log not found");
            return 3;
        }
        Err(e) => {
            eprintln!("get failed: {}", e);
            return 1;
        }
    };

    append_prompt_step(&mut log, step_name, prompt_h);

    match put_replay_log(&store, &log) {
        Ok(h) => {
            println!("{}", hex32(&h));
            0
        }
        Err(e) => {
            eprintln!("put failed: {}", e);
            1
        }
    }
}

fn handle_client(mut stream: TcpStream, root: PathBuf) -> io::Result<()> {
    let store = store_for(&root);
    loop {
        let payload = match net::read_frame(&mut stream, net::DEFAULT_MAX_FRAME) {
            Ok(p) => p,
            Err(e) => {
                // Connection closed or invalid frame.
                return Err(e);
            }
        };

        let req = match net::decode_request(&payload) {
            Ok(r) => r,
            Err(_) => {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "bad request"));
            }
        };

        match req {
            net::Request::Put(bytes) => {
                let h = store.put(&bytes).map_err(|_| io::Error::new(io::ErrorKind::Other, "put failed"))?;
                let resp = net::encode_put_resp(&h).map_err(|_| io::Error::new(io::ErrorKind::Other, "encode failed"))?;
                net::write_frame(&mut stream, &resp)?;
            }
            net::Request::Get(hash) => {
                let got = store.get(&hash).map_err(|_| io::Error::new(io::ErrorKind::Other, "get failed"))?;
                match got {
                    Some(bytes) => {
                        let resp = net::encode_get_resp(true, &bytes).map_err(|_| io::Error::new(io::ErrorKind::Other, "encode failed"))?;
                        net::write_frame(&mut stream, &resp)?;
                    }
                    None => {
                        let resp = net::encode_get_resp(false, &[]).map_err(|_| io::Error::new(io::ErrorKind::Other, "encode failed"))?;
                        net::write_frame(&mut stream, &resp)?;
                    }
                }
            }
        }
    }
}

fn cmd_serve(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut addr = "127.0.0.1:9090".to_string();

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--addr" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --addr value");
                    return 2;
                }
                addr = args[i].clone();
            }
            _ => {
                eprintln!("unknown arg: {}", args[i]);
                return 2;
            }
        }
        i += 1;
    }

    let listener = match TcpListener::bind(&addr) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("bind failed: {}", e);
            return 1;
        }
    };

    eprintln!("listening on {}", addr);
    for conn in listener.incoming() {
        match conn {
            Ok(stream) => {
                let r = root.clone();
                // Prototype: handle clients sequentially for determinism.
                // Later stages can add a deterministic worker pool.
                if let Err(e) = handle_client(stream, r) {
                    eprintln!("client error: {}", e);
                }
            }
            Err(e) => {
                eprintln!("accept error: {}", e);
            }
        }
    }

    0
}

fn cmd_send_put(args: &[String]) -> i32 {
    let mut addr = "127.0.0.1:9090".to_string();
    let mut file: Option<&str> = None;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--addr" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --addr value");
                    return 2;
                }
                addr = args[i].clone();
            }
            "--file" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --file value");
                    return 2;
                }
                file = Some(args[i].as_str());
            }
            _ => {
                eprintln!("unknown arg: {}", args[i]);
                return 2;
            }
        }
        i += 1;
    }

    let bytes = match read_all_from(file) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("read failed: {}", e);
            return 1;
        }
    };

    let mut stream = match TcpStream::connect(&addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("connect failed: {}", e);
            return 1;
        }
    };

    let req = match net::encode_put_req(&bytes) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("encode failed: {}", e);
            return 1;
        }
    };

    if let Err(e) = net::write_frame(&mut stream, &req) {
        eprintln!("write failed: {}", e);
        return 1;
    }

    let resp = match net::read_frame(&mut stream, net::DEFAULT_MAX_FRAME) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("read failed: {}", e);
            return 1;
        }
    };

    let h = match net::decode_put_resp(&resp) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("decode failed: {}", e);
            return 1;
        }
    };

    println!("{}", hex32(&h));
    0
}

fn cmd_send_get(args: &[String]) -> i32 {
    let mut addr = "127.0.0.1:9090".to_string();
    let mut hash_hex: Option<&str> = None;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--addr" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --addr value");
                    return 2;
                }
                addr = args[i].clone();
            }
            x => {
                if hash_hex.is_none() {
                    hash_hex = Some(x);
                } else {
                    eprintln!("unexpected arg: {}", x);
                    return 2;
                }
            }
        }
        i += 1;
    }

    let hh = match hash_hex {
        Some(x) => x,
        None => {
            eprintln!("missing hash_hex");
            return 2;
        }
    };

    let h = match parse_hash32_hex(hh) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("{}", e);
            return 2;
        }
    };

    let mut stream = match TcpStream::connect(&addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("connect failed: {}", e);
            return 1;
        }
    };

    let req = match net::encode_get_req(&h) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("encode failed: {}", e);
            return 1;
        }
    };

    if let Err(e) = net::write_frame(&mut stream, &req) {
        eprintln!("write failed: {}", e);
        return 1;
    }

    let resp = match net::read_frame(&mut stream, net::DEFAULT_MAX_FRAME) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("read failed: {}", e);
            return 1;
        }
    };

    let got = match net::decode_get_resp(&resp) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("decode failed: {}", e);
            return 1;
        }
    };

    match got {
        Some(bytes) => {
            if let Err(e) = write_all_to_stdout(&bytes) {
                eprintln!("write failed: {}", e);
                return 1;
            }
            0
        }
        None => {
            eprintln!("not found");
            3
        }
    }
}

fn bytes_from_kb(v: u32) -> Result<u32, String> {
    v.checked_mul(1024).ok_or_else(|| "kb too large".to_string())
}

fn bytes_from_mb(v: u32) -> Result<u32, String> {
    v.checked_mul(1024)
        .and_then(|x| x.checked_mul(1024))
        .ok_or_else(|| "mb too large".to_string())
}

fn cmd_serve_sync(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut addr = "127.0.0.1:9091".to_string();
    let mut max_chunk_kb: Option<u32> = None;
    let mut max_artifact_mb: Option<u32> = None;
    let mut rw_timeout_ms: Option<u32> = None;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--addr" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --addr value");
                    return 2;
                }
                addr = args[i].clone();
            }
            "--max_chunk_kb" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --max_chunk_kb value");
                    return 2;
                }
                match parse_u32(&args[i]) {
                    Ok(v) => max_chunk_kb = Some(v),
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                }
            }
            "--max_artifact_mb" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --max_artifact_mb value");
                    return 2;
                }
                match parse_u32(&args[i]) {
                    Ok(v) => max_artifact_mb = Some(v),
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                }
            }
            "--rw_timeout_ms" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --rw_timeout_ms value");
                    return 2;
                }
                match parse_u32(&args[i]) {
                    Ok(v) => rw_timeout_ms = Some(v),
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                }
            }
            _ => {
                eprintln!("unknown arg: {}", args[i]);
                return 2;
            }
        }
        i += 1;
    }

    let mut cfg = SyncServerCfgV1::default();
    if let Some(kb) = max_chunk_kb {
        match bytes_from_kb(kb) {
            Ok(b) => cfg.max_chunk_bytes = b,
            Err(e) => {
                eprintln!("{}", e);
                return 2;
            }
        }
    }
    if let Some(mb) = max_artifact_mb {
        match bytes_from_mb(mb) {
            Ok(b) => cfg.max_artifact_bytes = b,
            Err(e) => {
                eprintln!("{}", e);
                return 2;
            }
        }
    }

    if let Some(ms) = rw_timeout_ms {
        cfg.rw_timeout_ms = ms;
    }

    eprintln!("listening on {}", addr);
    match run_sync_server_v1(&root, &addr, cfg) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("serve-sync failed: {}", e);
            1
        }
    }
}

fn cmd_sync_lexicon(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut addr: Option<String> = None;
    let mut snapshot_hex: Option<String> = None;
    let mut out_file: Option<String> = None;
    let mut max_chunk_kb: Option<u32> = None;
    let mut max_artifact_mb: Option<u32> = None;
    let mut rw_timeout_ms: Option<u32> = None;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--addr" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --addr value");
                    return 2;
                }
                addr = Some(args[i].clone());
            }
            "--lexicon-snapshot" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --lexicon-snapshot value");
                    return 2;
                }
                snapshot_hex = Some(args[i].clone());
            }
            "--out-file" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --out-file value");
                    return 2;
                }
                out_file = Some(args[i].clone());
            }
            "--max_chunk_kb" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --max_chunk_kb value");
                    return 2;
                }
                match parse_u32(&args[i]) {
                    Ok(v) => max_chunk_kb = Some(v),
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                }
            }
            "--max_artifact_mb" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --max_artifact_mb value");
                    return 2;
                }
                match parse_u32(&args[i]) {
                    Ok(v) => max_artifact_mb = Some(v),
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                }
            }
            "--rw_timeout_ms" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --rw_timeout_ms value");
                    return 2;
                }
                match parse_u32(&args[i]) {
                    Ok(v) => rw_timeout_ms = Some(v),
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                }
            }
            _ => {
                eprintln!("unknown arg: {}", args[i]);
                return 2;
            }
        }
        i += 1;
    }

    let addr = match addr {
        Some(a) => a,
        None => {
            eprintln!("missing --addr");
            return 2;
        }
    };
    let snapshot_hex = match snapshot_hex {
        Some(h) => h,
        None => {
            eprintln!("missing --lexicon-snapshot");
            return 2;
        }
    };
    let snapshot_h = match parse_hash32_hex(&snapshot_hex) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("{}", e);
            return 2;
        }
    };

    let store = match FsArtifactStore::new(&root) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("open store failed: {}", e);
            return 1;
        }
    };

    let mut cfg = SyncClientCfgV1::default();
    if let Some(kb) = max_chunk_kb {
        match bytes_from_kb(kb) {
            Ok(b) => cfg.max_chunk_bytes = b,
            Err(e) => {
                eprintln!("{}", e);
                return 2;
            }
        }
    }
    if let Some(mb) = max_artifact_mb {
        match bytes_from_mb(mb) {
            Ok(b) => cfg.max_artifact_bytes = b,
            Err(e) => {
                eprintln!("{}", e);
                return 2;
            }
        }
    }
    if let Some(ms) = rw_timeout_ms {
        cfg.rw_timeout_ms = ms;
    }

    let stats = match sync_lexicon_v1(&store, &addr, &snapshot_h, &cfg) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("sync-lexicon failed: {}", e);
            return 1;
        }
    };

    let line = format!(
        "sync_lexicon_stats needed_total={} already_present={} fetched={} bytes_fetched={}
",
        stats.needed_total, stats.already_present, stats.fetched, stats.bytes_fetched
    );
    print!("{}", line);
    if let Some(p) = out_file {
        if let Err(e) = fs::write(&p, line.as_bytes()) {
            eprintln!("write out-file failed: {}", e);
            return 1;
        }
    }
    0
}

fn cmd_sync_reduce(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut addr: Option<String> = None;
    let mut reduce_hex: Option<String> = None;
    let mut out_file: Option<String> = None;
    let mut max_chunk_kb: Option<u32> = None;
    let mut max_artifact_mb: Option<u32> = None;
    let mut rw_timeout_ms: Option<u32> = None;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--addr" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --addr value");
                    return 2;
                }
                addr = Some(args[i].clone());
            }
            "--reduce-manifest" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --reduce-manifest value");
                    return 2;
                }
                reduce_hex = Some(args[i].clone());
            }
            "--out-file" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --out-file value");
                    return 2;
                }
                out_file = Some(args[i].clone());
            }
            "--max_chunk_kb" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --max_chunk_kb value");
                    return 2;
                }
                match parse_u32(&args[i]) {
                    Ok(v) => max_chunk_kb = Some(v),
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                }
            }
            "--max_artifact_mb" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --max_artifact_mb value");
                    return 2;
                }
                match parse_u32(&args[i]) {
                    Ok(v) => max_artifact_mb = Some(v),
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                }
            }
            "--rw_timeout_ms" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --rw_timeout_ms value");
                    return 2;
                }
                match parse_u32(&args[i]) {
                    Ok(v) => rw_timeout_ms = Some(v),
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                }
            }
            _ => {
                eprintln!("unknown arg: {}", args[i]);
                return 2;
            }
        }
        i += 1;
    }

    let addr = match addr {
        Some(a) => a,
        None => {
            eprintln!("missing --addr");
            return 2;
        }
    };
    let reduce_hex = match reduce_hex {
        Some(h) => h,
        None => {
            eprintln!("missing --reduce-manifest");
            return 2;
        }
    };
    let reduce_h = match parse_hash32_hex(&reduce_hex) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("{}", e);
            return 2;
        }
    };

    let store = match FsArtifactStore::new(&root) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("open store failed: {}", e);
            return 1;
        }
    };

    let mut cfg = SyncClientCfgV1::default();
    if let Some(kb) = max_chunk_kb {
        match bytes_from_kb(kb) {
            Ok(b) => cfg.max_chunk_bytes = b,
            Err(e) => {
                eprintln!("{}", e);
                return 2;
            }
        }
    }
    if let Some(mb) = max_artifact_mb {
        match bytes_from_mb(mb) {
            Ok(b) => cfg.max_artifact_bytes = b,
            Err(e) => {
                eprintln!("{}", e);
                return 2;
            }
        }
    }

    if let Some(ms) = rw_timeout_ms {
        cfg.rw_timeout_ms = ms;
    }

    let stats = match sync_reduce_v1(&store, &addr, &reduce_h, &cfg) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("sync-reduce failed: {}", e);
            return 1;
        }
    };

    let line = format!(
        "needed_total={} already_present={} fetched={} bytes_fetched={}
",
        stats.needed_total, stats.already_present, stats.fetched, stats.bytes_fetched
    );
    print!("{}", line);
    if let Some(p) = out_file {
        if let Err(e) = fs::write(&p, line.as_bytes()) {
            eprintln!("write out-file failed: {}", e);
            return 1;
        }
    }
    0
}

fn cmd_sync_reduce_batch(args: &[String]) -> i32 {
    let mut root = default_root();
    let mut addr: Option<String> = None;
    let mut manifests_path: Option<String> = None;
    let mut out_file: Option<String> = None;
    let mut max_chunk_kb: Option<u32> = None;
    let mut max_artifact_mb: Option<u32> = None;
    let mut rw_timeout_ms: Option<u32> = None;

    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 2;
                }
                root = PathBuf::from(&args[i]);
            }
            "--addr" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --addr value");
                    return 2;
                }
                addr = Some(args[i].clone());
            }
            "--reduce-manifests" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --reduce-manifests value");
                    return 2;
                }
                manifests_path = Some(args[i].clone());
            }
            "--out-file" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --out-file value");
                    return 2;
                }
                out_file = Some(args[i].clone());
            }
            "--max_chunk_kb" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --max_chunk_kb value");
                    return 2;
                }
                match parse_u32(&args[i]) {
                    Ok(v) => max_chunk_kb = Some(v),
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                }
            }
            "--max_artifact_mb" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --max_artifact_mb value");
                    return 2;
                }
                match parse_u32(&args[i]) {
                    Ok(v) => max_artifact_mb = Some(v),
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                }
            }
            "--rw_timeout_ms" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --rw_timeout_ms value");
                    return 2;
                }
                match parse_u32(&args[i]) {
                    Ok(v) => rw_timeout_ms = Some(v),
                    Err(e) => {
                        eprintln!("{}", e);
                        return 2;
                    }
                }
            }
            _ => {
                eprintln!("unknown arg: {}", args[i]);
                return 2;
            }
        }
        i += 1;
    }

    let addr = match addr {
        Some(a) => a,
        None => {
            eprintln!("missing --addr");
            return 2;
        }
    };
    let manifests_path = match manifests_path {
        Some(p) => p,
        None => {
            eprintln!("missing --reduce-manifests");
            return 2;
        }
    };

    let store = match FsArtifactStore::new(&root) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("open store failed: {}", e);
            return 1;
        }
    };

    // Parse manifest hashes from file (one 64-hex hash per line; blank lines and # comments allowed).
    let content = match fs::read_to_string(&manifests_path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("read reduce-manifests file failed: {}", e);
            return 1;
        }
    };
    let mut list: Vec<Hash32> = Vec::new();
    for (ln, raw) in content.lines().enumerate() {
        let t = raw.trim();
        if t.is_empty() || t.starts_with('#') {
            continue;
        }
        match parse_hash32_hex(t) {
            Ok(h) => list.push(h),
            Err(e) => {
                eprintln!("bad reduce-manifest hash on line {}: {}", ln + 1, e);
                return 2;
            }
        }
    }
    if list.is_empty() {
        eprintln!("reduce-manifests file contained no hashes");
        return 2;
    }

    let mut cfg = SyncClientCfgV1::default();
    if let Some(kb) = max_chunk_kb {
        match bytes_from_kb(kb) {
            Ok(b) => cfg.max_chunk_bytes = b,
            Err(e) => {
                eprintln!("{}", e);
                return 2;
            }
        }
    }
    if let Some(mb) = max_artifact_mb {
        match bytes_from_mb(mb) {
            Ok(b) => cfg.max_artifact_bytes = b,
            Err(e) => {
                eprintln!("{}", e);
                return 2;
            }
        }
    }
    if let Some(ms) = rw_timeout_ms {
        cfg.rw_timeout_ms = ms;
    }

    let report = match sync_reduce_batch_v1(&store, &addr, &list, &cfg) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("sync-reduce-batch failed: {}", e);
            return 1;
        }
    };

    let mut out = String::new();
    out.push_str(&format!(
        "needed_total={} already_present={} fetched={} bytes_fetched={} manifests={}\n",
        report.stats.needed_total,
        report.stats.already_present,
        report.stats.fetched,
        report.stats.bytes_fetched,
        report.manifests.len()
    ));
    for m in report.manifests.iter() {
        out.push_str(&format!(
            "manifest={} needed_total={}\n",
            hex32(&m.reduce_manifest),
            m.needed_total
        ));
    }
    print!("{}", out);
    if let Some(p) = out_file {
        if let Err(e) = fs::write(&p, out.as_bytes()) {
            eprintln!("write out-file failed: {}", e);
            return 1;
        }
    }
    0
}

fn cmd_export_debug_bundle(args: &[String]) -> i32 {
    let mut root: Option<PathBuf> = None;
    let mut out: Option<PathBuf> = None;
    let mut include: Vec<Hash32> = Vec::new();

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing value after --root");
                    return 2;
                }
                root = Some(PathBuf::from(&args[i]));
            }
            "--out" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing value after --out");
                    return 2;
                }
                out = Some(PathBuf::from(&args[i]));
            }
            "--include-hash" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing value after --include-hash");
                    return 2;
                }
                let h = match parse_hash32_hex(&args[i]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("bad include-hash: {}", e);
                        return 2;
                    }
                };
                include.push(h);
            }
            x => {
                eprintln!("unknown arg: {}\n\n{}", x, usage());
                return 2;
            }
        }
        i += 1;
    }

    let root = match root {
        Some(r) => r,
        None => {
            eprintln!("missing --root");
            return 2;
        }
    };
    let out = match out {
        Some(p) => p,
        None => {
            eprintln!("missing --out");
            return 2;
        }
    };

    let mut cfg = DebugBundleCfgV1::new(&root, &out);
    cfg.usage_text = Some(usage().to_string());
    cfg.include_hashes = include;
    if let Err(e) = export_debug_bundle_v1(&cfg) {
        eprintln!("export-debug-bundle failed: {}", e);
        return 1;
    }

    println!("{}", out.to_string_lossy());
    0
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("{}", usage());
        std::process::exit(2);
    }
    let cmd = args[1].as_str();
    let rest = &args[2..];

    let code = match cmd {
        "hash" => cmd_hash(rest),
        "put" => cmd_put(rest),
        "get" => cmd_get(rest),
        "show-workspace" => cmd_show_workspace(rest),
        "show-conversation" => cmd_show_conversation(rest),
        "ask" => cmd_ask(rest),
        "chat" => cmd_chat(rest),
        "prompt" => cmd_prompt(rest),
        "replay-decode" => cmd_replay_decode(rest),
        "serve" => cmd_serve(rest),
        "serve-sync" => cmd_serve_sync(rest),
        "send-put" => cmd_send_put(rest),
        "send-get" => cmd_send_get(rest),
        "sync-reduce" => cmd_sync_reduce(rest),
        "sync-lexicon" => cmd_sync_lexicon(rest),
        "sync-reduce-batch" => cmd_sync_reduce_batch(rest),
                "replay-new" => cmd_replay_new(rest),
        "frame-seg-demo" => cmd_frame_seg_demo(rest),
        "frame-seg-show" => cmd_frame_seg_show(rest),
        "ingest-wiki" => cmd_ingest_wiki(rest),
        "ingest-wiki-xml" => cmd_ingest_wiki_xml(rest),
        "ingest-wiki-sharded" => cmd_ingest_wiki_sharded(rest),
        "ingest-wiki-xml-sharded" => cmd_ingest_wiki_xml_sharded(rest),
        "load-wikipedia" => cmd_load_wikipedia(rest),
        "load-wiktionary" => cmd_load_wiktionary(rest),
        "build-index" => cmd_build_index(rest),
        "build-index-sharded" => cmd_build_index_sharded(rest),
        "reduce-index" => cmd_reduce_index(rest),
        "run-workflow" => cmd_run_workflow(rest),
        "export-debug-bundle" => cmd_export_debug_bundle(rest),
        "ingest-wiktionary-xml" => cmd_ingest_wiktionary_xml(rest),
        "build-lexicon-snapshot" => cmd_build_lexicon_snapshot(rest),
        "validate-lexicon-snapshot" => cmd_validate_lexicon_snapshot(rest),
        "compact-index" => cmd_compact_index(rest),
        "query-index" => cmd_query_index(rest),
        "build-evidence" => cmd_build_evidence(rest),
        "build-pragmatics" => cmd_build_pragmatics(rest),
        "answer" => cmd_answer(rest),
        "build-markov-model" => cmd_build_markov_model(rest),
        "build-exemplar-memory" => cmd_build_exemplar_memory(rest),
        "build-graph-relevance" => cmd_build_graph_relevance(rest),
        "inspect-markov-model" => cmd_inspect_markov_model(rest),
        "scale-demo" => cmd_scale_demo(rest),
        "golden-pack" => cmd_golden_pack(rest),
        "golden-pack-turn-pairs" => cmd_golden_pack_turn_pairs(rest),
        "golden-pack-conversation" => cmd_golden_pack_conversation(rest),
        "replay-add-prompt" => cmd_replay_add_prompt(rest),
_ => {
            eprintln!("unknown cmd: {}\n\n{}", cmd, usage());
            2
        }
    };

    std::process::exit(code);
}
