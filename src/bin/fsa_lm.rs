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
    STEP_MARKOV_HINTS_V1, STEP_MARKOV_TRACE_V1,
};
use fsa_lm::frame::{DocId, FrameRowV1, Id64, SourceId};
use fsa_lm::frame_segment::FrameSegmentV1;
use fsa_lm::frame_segment::FRAME_SEGMENT_MAGIC;
use fsa_lm::index_segment::IndexSegmentV1;
use fsa_lm::index_snapshot::{IndexSnapshotEntryV1, IndexSnapshotV1};
use fsa_lm::index_query::{query_terms_from_text, search_snapshot, search_snapshot_cached, search_snapshot_gated, search_snapshot_cached_gated, QueryTermsCfg, SearchCfg};
use fsa_lm::retrieval_control::RetrievalControlV1;
use fsa_lm::retrieval_policy::{apply_retrieval_policy_from_text_v1, RetrievalPolicyCfgV1};
use fsa_lm::planner_v1::{plan_from_evidence_bundle_v1_with_guidance, PlannerCfgV1, PlannerOutputV1};
use fsa_lm::planner_hints_artifact::put_planner_hints_v1;
use fsa_lm::forecast_artifact::put_forecast_v1;
use fsa_lm::quality_gate_v1::{
    build_markov_trace_tokens_v1, derive_directives_opt,
    derive_markov_hints_opener_preface_opt, realize_with_quality_gate_v1,
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
use fsa_lm::evidence_bundle::EvidenceLimitsV1;
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

use fsa_lm::pragmatics_extract::{extract_pragmatics_frames_for_prompt_pack_v1, PragmaticsExtractCfg};
use fsa_lm::pragmatics_frame_store::put_pragmatics_frame_v1;
use fsa_lm::pragmatics_frame_store::get_pragmatics_frame_v1;

use fsa_lm::realizer_directives_artifact::put_realizer_directives_v1;

use fsa_lm::compaction_report::CompactionCfgV1;
use fsa_lm::index_compaction::compact_index_snapshot_v1;

use fsa_lm::frame_store::{get_frame_segment_v1, put_frame_segment_v1};
use fsa_lm::tokenizer::{term_freqs_from_text, TokenizerCfg};
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
use fsa_lm::artifact_sync::{run_sync_server_v1, sync_reduce_v1, sync_reduce_batch_v1, SyncClientCfgV1, SyncServerCfgV1};
use fsa_lm::debug_bundle::{export_debug_bundle_v1, DebugBundleCfgV1};
use bzip2::read::BzDecoder;

use std::env;
use std::fs;
use std::io::{self, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn usage() -> &'static str {
    r#"fsa_lm <cmd> [args]

Commands:

  hash [--file <path>]
  put [--root <dir>] [--file <path>]
  get [--root <dir>] <hash_hex>
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
  build-index [--root <dir>]
  build-index-sharded --shards <n> [--root <dir>] [--manifest <hash32hex>] [--out-file <path>]
  reduce-index --root <dir> --manifest <hash32hex> [--out-file <path>]
  run-phase6 --root <dir> --dump <path> --shards <n> [--seg_mb <u32>] [--row_kb <u32>] [--chunk_rows <u32>] [--max_docs <u64>] [--out-file <path>] [--sync-addr <ip:port> --sync-root <dir>] [--max_chunk_kb <n>] [--max_artifact_mb <n>] [--rw_timeout_ms <n>]
  export-debug-bundle --root <dir> --out <path> [--include-hash <hash32hex> ...]

  build-lexicon-snapshot --root <dir> --segment <hash32hex> [--segment <hash32hex> ...] [--out-file <path>]
  validate-lexicon-snapshot --root <dir> --snapshot <hash32hex>
  build-pragmatics --root <dir> --prompt <hash32hex> [--source-id <u64>] [--tok-max-bytes <n>] [--out-file <path>]
  query-index --root <dir> --snapshot <hash32hex> [--sig-map <hash32hex>] --text <string> [--k <n>] [--meta] [--cache-stats]
  build-evidence --root <dir> --snapshot <hash32hex> [--sig-map <hash32hex>] --text <string> [--k <n>] [--meta] [--max_items <n>] [--max_bytes <n>] [--no_sketch] [--no_verify] [--score_model <id>] [--verbose] [--cache-stats]
  answer --root <dir> --prompt <hash32hex> --snapshot <hash32hex> [--sig-map <hash32hex>] [--pragmatics <hash32hex> ...] [--k <n>] [--meta] [--max_terms <n>] [--no_ties] [--expand --lexicon-snapshot <hash32hex>] [--plan_items <n>] [--verify-trace <0|1>] [--markov-model <hash32hex>] [--markov-max-choices <n>] [--out-file <path>]
  build-markov-model --root <dir> --replay <hash32hex> [--replay <hash32hex> ...] [--replay-file <path>] [--max-replays <n>] [--max-traces <n>] [--order <n>] [--max-next <n>] [--max-states <n>] [--out-file <path>]
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

fn cmd_run_phase6(args: &[String]) -> i32 {
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

    let cfg = PragmaticsExtractCfg {
        tokenizer_cfg: TokenizerCfg {
            max_token_bytes: tok_max_bytes,
        },
    };

    let frames = match extract_pragmatics_frames_for_prompt_pack_v1(Id64(source_id), &pack, cfg) {
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

        let rc = cmd_run_phase6(&args);
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
    use fsa_lm::tokenizer::{term_freqs_from_text, TokenizerCfg};

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
        assert!(s.contains("Answer v1"));
        assert!(s.contains("Plan"));
        assert!(s.contains("Evidence"));
        assert!(s.contains("[E0]"));
    }

    #[test]
    fn cmd_answer_with_pragmatics_adds_directives_header() {
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
        assert!(s.contains("directives tone="));
        assert!(s.contains("Plan"));
        assert!(s.contains("Evidence"));
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

        // This test targets Option B: if the realizer emits an opener preface line,
        // the MarkovTrace must start with the corresponding preface:<tone>:<variant> token.
        assert_eq!(tone, fsa_lm::realizer_directives::ToneV1::Supportive);
        assert_eq!(style, fsa_lm::realizer_directives::StyleV1::Default);

        let preface_line_v0 = "I can help with that. Here is what the evidence supports:";
        assert!(s.contains(preface_line_v0));

        assert!(trace.tokens.len() >= 2);

        let preface_cid = fsa_lm::frame::derive_id64(b"markov_choice_v1", b"preface:supportive:0");
        assert_eq!(
            trace.tokens[0],
            MarkovTokenV1::new(MarkovChoiceKindV1::Opener, preface_cid)
        );

        let cid_summary = fsa_lm::frame::derive_id64(b"markov_choice_v1", b"plan_item:summary");
        let cid_bullet = fsa_lm::frame::derive_id64(b"markov_choice_v1", b"plan_item:bullet");
        let cid_step = fsa_lm::frame::derive_id64(b"markov_choice_v1", b"plan_item:step");
        let cid_caveat = fsa_lm::frame::derive_id64(b"markov_choice_v1", b"plan_item:caveat");

        let next = trace.tokens[1].choice_id;
        assert!(
            next == cid_summary || next == cid_bullet || next == cid_step || next == cid_caveat,
            "unexpected plan token id"
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
                "--out-file".to_string(),
                out_path.to_string_lossy().to_string(),
            ]);
            assert_eq!(rc, 0);

            let s = std::fs::read_to_string(&out_path).unwrap();
            assert!(s.contains("directives tone=Supportive"));

            let preface_v1 = "Happy to help. Here is what the evidence supports:";
            let preface_v0 = "I can help with that. Here is what the evidence supports:";
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

fn cmd_answer(args: &[String]) -> i32 {
    let mut root: PathBuf = default_root();
    let mut prompt_hash: Option<Hash32> = None;
    let mut snapshot_hash: Option<Hash32> = None;
    let mut sig_map_hash: Option<Hash32> = None;
    let mut lexicon_snapshot_hash: Option<Hash32> = None;
    let mut enable_expand: bool = false;
    let mut pragmatics_ids: Vec<Hash32> = Vec::new();
    let mut k: usize = 10;
    let mut include_meta: bool = false;
    let mut max_terms: Option<u32> = None;
    let mut no_ties: bool = false;
    let mut plan_items: Option<u32> = None;
    let mut out_file: Option<PathBuf> = None;
    let mut verify_trace: u8 = 0;

    let mut markov_model_hash: Option<Hash32> = None;
    let mut markov_max_choices: usize = 8;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--root" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --root value");
                    return 1;
                }
                root = PathBuf::from(&args[i]);
            }
            "--prompt" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --prompt value");
                    return 1;
                }
                match parse_hash32_hex(&args[i]) {
                    Ok(h) => prompt_hash = Some(h),
                    Err(e) => {
                        eprintln!("bad --prompt: {}", e);
                        return 1;
                    }
                }
            }
            "--snapshot" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --snapshot value");
                    return 1;
                }
                match parse_hash32_hex(&args[i]) {
                    Ok(h) => snapshot_hash = Some(h),
                    Err(e) => {
                        eprintln!("bad --snapshot: {}", e);
                        return 1;
                    }
                }
            }
            "--sig-map" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --sig-map value");
                    return 1;
                }
                match parse_hash32_hex(&args[i]) {
                    Ok(h) => sig_map_hash = Some(h),
                    Err(e) => {
                        eprintln!("bad --sig-map: {}", e);
                        return 1;
                    }
                }
            }
            "--lexicon-snapshot" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --lexicon-snapshot value");
                    return 1;
                }
                match parse_hash32_hex(&args[i]) {
                    Ok(h) => lexicon_snapshot_hash = Some(h),
                    Err(e) => {
                        eprintln!("bad --lexicon-snapshot: {}", e);
                        return 1;
                    }
                }
            }
            "--expand" => {
                enable_expand = true;
            }
            "--pragmatics" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --pragmatics value");
                    return 1;
                }
                match parse_hash32_hex(&args[i]) {
                    Ok(h) => pragmatics_ids.push(h),
                    Err(e) => {
                        eprintln!("bad --pragmatics: {}", e);
                        return 1;
                    }
                }
            }
            "--k" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --k value");
                    return 1;
                }
                match parse_u32(&args[i]) {
                    Ok(v) => k = v as usize,
                    Err(e) => {
                        eprintln!("bad --k: {}", e);
                        return 1;
                    }
                }
            }
            "--meta" => {
                include_meta = true;
            }
            "--max_terms" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --max_terms value");
                    return 1;
                }
                match parse_u32(&args[i]) {
                    Ok(v) => max_terms = Some(v),
                    Err(e) => {
                        eprintln!("bad --max_terms: {}", e);
                        return 1;
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
                    return 1;
                }
                match parse_u32(&args[i]) {
                    Ok(v) => plan_items = Some(v),
                    Err(e) => {
                        eprintln!("bad --plan_items: {}", e);
                        return 1;
                    }
                }
            }
            "--out-file" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --out-file value");
                    return 1;
                }
                out_file = Some(PathBuf::from(&args[i]));
            }
            "--verify-trace" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --verify-trace value");
                    return 1;
                }
                match parse_u8(&args[i]) {
                    Ok(v) => {
                        if v > 1 {
                            eprintln!("verify-trace must be 0 or 1");
                            return 1;
                        }
                        verify_trace = v;
                    }
                    Err(e) => {
                        eprintln!("bad --verify-trace: {}", e);
                        return 1;
                    }
                }
            }
            "--markov-model" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --markov-model value");
                    return 1;
                }
                match parse_hash32_hex(&args[i]) {
                    Ok(h) => markov_model_hash = Some(h),
                    Err(e) => {
                        eprintln!("bad --markov-model: {}", e);
                        return 1;
                    }
                }
            }
            "--markov-max-choices" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("missing --markov-max-choices value");
                    return 1;
                }
                match parse_u32(&args[i]) {
                    Ok(v) => {
                        if v == 0 || v > 32 {
                            eprintln!("markov-max-choices must be 1..32");
                            return 1;
                        }
                        markov_max_choices = v as usize;
                    }
                    Err(e) => {
                        eprintln!("bad --markov-max-choices: {}", e);
                        return 1;
                    }
                }
            }
            "-h" | "--help" => {
                eprintln!("{}", usage());
                return 0;
            }
            other => {
                eprintln!("unknown arg: {}", other);
                return 1;
            }
        }
        i += 1;
    }

    let prompt_hash = match prompt_hash {
        Some(h) => h,
        None => {
            eprintln!("missing --prompt");
            return 1;
        }
    };
    let snapshot_hash = match snapshot_hash {
        Some(h) => h,
        None => {
            eprintln!("missing --snapshot");
            return 1;
        }
    };

    let store = store_for(&root);

    let pack = match get_prompt_pack(&store, &prompt_hash) {
        Ok(Some(p)) => p,
        Ok(None) => {
            eprintln!("prompt not found");
            return 1;
        }
        Err(e) => {
            eprintln!("store error: {}", e);
            return 1;
        }
    };

    let mut query_text: Option<String> = None;
    for m in pack.messages.iter().rev() {
        if m.role == Role::User {
            query_text = Some(m.content.clone());
            break;
        }
    }
    if query_text.is_none() {
        if let Some(m) = pack.messages.last() {
            query_text = Some(m.content.clone());
        }
    }
    let qtext = query_text.unwrap_or_else(|| "".to_string());


    let mut qcfg = QueryTermsCfg::new();
    qcfg.include_metaphone = include_meta;
    if let Some(mt) = max_terms {
        if mt == 0 {
            eprintln!("max_terms must be >= 1");
            return 1;
        }
        qcfg.max_terms = mt as usize;
    }

    let mut control = RetrievalControlV1::new(prompt_hash);
    if !pragmatics_ids.is_empty() {
        control.pragmatics_frame_ids = pragmatics_ids;
    }
    if let Err(e) = control.validate() {
        eprintln!("control error: {}", e);
        return 1;
    }

    let mut pcfg = RetrievalPolicyCfgV1::new();
    if k > (u16::MAX as usize) {
        eprintln!("k too large");
        return 1;
    }
    pcfg.max_hits = k as u16;
    if pcfg.max_hits == 0 {
        eprintln!("k must be >= 1");
        return 1;
    }
    if let Some(mt) = max_terms {
        if mt > (u16::MAX as u32) {
            eprintln!("max_terms too large");
            return 1;
        }
        pcfg.max_query_terms = mt as u16;
    }
    if no_ties {
        pcfg.include_ties_at_cutoff = 0;
    }

    if enable_expand {
        pcfg.enable_query_expansion = 1;
        if lexicon_snapshot_hash.is_none() {
            eprintln!("missing --lexicon-snapshot (required when --expand)");
            return 1;
        }
    }

    let (hits, _stats) = match apply_retrieval_policy_from_text_v1(
        &store,
        &snapshot_hash,
        sig_map_hash.as_ref(),
        &qtext,
        &qcfg,
        &pcfg,
        Some(&control),
        lexicon_snapshot_hash.as_ref(),
        None,
    ) {
        Ok(x) => x,
        Err(e) => {
            eprintln!("policy error: {}", e);
            return 1;
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
            qid_bytes.extend_from_slice(lh);
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
            return 1;
        }
    };

    if let Err(e) = bundle.canonicalize_in_place() {
        eprintln!("evidence canonicalize failed: {}", e);
        return 1;
    }

    let ev_hash = match put_evidence_bundle_v1(&store, &bundle) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("store error: {}", e);
            return 1;
        }
    };

    let mut pl_cfg = PlannerCfgV1::default_v1();
    if let Some(pi) = plan_items {
        if pi == 0 {
            eprintln!("plan_items must be >= 1");
            return 1;
        }
        if pi > 16_384 {
            eprintln!("plan_items too large");
            return 1;
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
                return 1;
            }
        };
        let pf = match pf_opt {
            Some(x) => x,
            None => {
                eprintln!("missing pragmatics frame: {}", hex32(&pid));
                return 1;
            }
        };
        Some(pf)
    };

    let directives_opt = derive_directives_opt(pf_opt.as_ref());

    let directives_hash_opt = match directives_opt.as_ref() {
        Some(d) => match put_realizer_directives_v1(&store, d) {
            Ok(h) => Some(h),
            Err(e) => {
                eprintln!("store directives failed: {}", e);
                return 1;
            }
        },
        None => None,
    };

    // Optional: derive MarkovHintsV1 for surface-template selection.
    //: derive MarkovHintsV1 and use them for opener/preface selection
    // when a MarkovModelV1 is supplied.
    let mut markov_hints_hash_opt: Option<Hash32> = None;
    let mut markov_hints_opt: Option<MarkovHintsV1> = None;
    if let Some(mh) = markov_model_hash.as_ref() {
        let model = match get_markov_model_v1(&store, mh) {
            Ok(x) => x,
            Err(e) => {
                eprintln!("markov-model load failed: {}", e);
                return 1;
            }
        };
        let model = match model {
            Some(x) => x,
            None => {
                eprintln!("missing markov model: {}", hex32(mh));
                return 1;
            }
        };

        let hints_opt = derive_markov_hints_opener_preface_opt(
            query_id,
            !control.pragmatics_frame_ids.is_empty(),
            *mh,
            &model,
            directives_opt.as_ref(),
            markov_max_choices,
        );

        if let Some(hints) = hints_opt {
            let hh = match put_markov_hints_v1(&store, &hints) {
                Ok(h) => h,
                Err(e) => {
                    eprintln!("store markov hints failed: {}", e);
                    return 1;
                }
            };
            // Record the hints hash so we can append a replay step and capture
            // deterministic dependencies for the answer output.
            markov_hints_hash_opt = Some(hh);
            markov_hints_opt = Some(hints);
        }
    }

    let PlannerOutputV1 { plan, hints: planner_hints, forecast } = match plan_from_evidence_bundle_v1_with_guidance(&bundle, ev_hash, &pl_cfg, pf_opt.as_ref()) {
        Ok(x) => x,
        Err(e) => {
            eprintln!("plan failed: {}", e);
            return 1;
        }
    };

    let planner_hints_hash = match put_planner_hints_v1(&store, &planner_hints) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("store planner hints failed: {}", e);
            return 1;
        }
    };

    let forecast_hash = match put_forecast_v1(&store, &forecast) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("store forecast failed: {}", e);
            return 1;
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
            return 1;
        }
    };

    let text = qr.text;
    let did_append_q = qr.did_append_question;
    let opener_preface_choice = qr.markov.opener_preface_choice;

    let answer_hash = match store.put(text.as_bytes()) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("store error: {}", e);
            return 1;
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
    // For the opener preface line, use the realizer-reported surface-choice
    // event as the source of truth (no re-parsing of rendered text).
    let mt_tokens: Vec<MarkovTokenV1> = build_markov_trace_tokens_v1(
        &plan,
        opener_preface_choice,
        did_append_q,
    );

    let trace = MarkovTraceV1 {
        version: MARKOV_TRACE_V1_VERSION,
        query_id,
        tokens: mt_tokens,
    };

    let markov_trace_hash = match put_markov_trace_v1(&store, &trace) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("store markov trace failed: {}", e);
            return 1;
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
            return 3;
        }
    }

    let set_hash = match put_evidence_set_v1(&store, &set) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("store error: {}", e);
            return 1;
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

    if let Err(e) = put_replay_log(&store, &log) {
        eprintln!("store error: {}", e);
        return 1;
    }

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
        "prompt" => cmd_prompt(rest),
        "replay-decode" => cmd_replay_decode(rest),
        "serve" => cmd_serve(rest),
        "serve-sync" => cmd_serve_sync(rest),
        "send-put" => cmd_send_put(rest),
        "send-get" => cmd_send_get(rest),
        "sync-reduce" => cmd_sync_reduce(rest),
        "sync-reduce-batch" => cmd_sync_reduce_batch(rest),
                "replay-new" => cmd_replay_new(rest),
        "frame-seg-demo" => cmd_frame_seg_demo(rest),
        "frame-seg-show" => cmd_frame_seg_show(rest),
        "ingest-wiki" => cmd_ingest_wiki(rest),
        "ingest-wiki-xml" => cmd_ingest_wiki_xml(rest),
        "ingest-wiki-sharded" => cmd_ingest_wiki_sharded(rest),
        "ingest-wiki-xml-sharded" => cmd_ingest_wiki_xml_sharded(rest),
        "build-index" => cmd_build_index(rest),
        "build-index-sharded" => cmd_build_index_sharded(rest),
        "reduce-index" => cmd_reduce_index(rest),
        "run-phase6" => cmd_run_phase6(rest),
        "export-debug-bundle" => cmd_export_debug_bundle(rest),
        "build-lexicon-snapshot" => cmd_build_lexicon_snapshot(rest),
        "validate-lexicon-snapshot" => cmd_validate_lexicon_snapshot(rest),
        "compact-index" => cmd_compact_index(rest),
        "query-index" => cmd_query_index(rest),
        "build-evidence" => cmd_build_evidence(rest),
        "build-pragmatics" => cmd_build_pragmatics(rest),
        "answer" => cmd_answer(rest),
        "build-markov-model" => cmd_build_markov_model(rest),
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
