// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Artifact sync helpers.
//!
//! This module implements manifest-driven replication over TCP using
//! artifact_sync_v1 protocol.

use crate::artifact::ArtifactStore;
use crate::artifact::{ArtifactError, ArtifactResult, FsArtifactStore};
use crate::artifact_sync_v1 as p;
use crate::hash::{hex32, Hash32};
use crate::index_sig_map::IndexSigMapV1;
use crate::index_snapshot::IndexSnapshotV1;
use crate::net;
use crate::reduce_manifest::ReduceManifestV1;

use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};

/// Errors produced by artifact sync.
#[derive(Debug)]
pub enum SyncErrorV1 {
    /// Artifact store error.
    Store(ArtifactError),
    /// IO error.
    Io(std::io::Error),
    /// Socket operation timed out.
    Timeout,
    /// Connection dropped unexpectedly.
    Disconnected,
    /// Protocol error.
    Proto(&'static str),
    /// Remote returned an error message.
    Remote(String),
}

impl core::fmt::Display for SyncErrorV1 {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            SyncErrorV1::Store(e) => write!(f, "store: {}", e),
            SyncErrorV1::Io(e) => write!(f, "io: {}", e),
            SyncErrorV1::Timeout => write!(f, "timeout"),
            SyncErrorV1::Disconnected => write!(f, "disconnected"),
            SyncErrorV1::Proto(s) => write!(f, "proto: {}", s),
            SyncErrorV1::Remote(s) => write!(f, "remote: {}", s),
        }
    }
}

impl std::error::Error for SyncErrorV1 {}

impl From<ArtifactError> for SyncErrorV1 {
    fn from(e: ArtifactError) -> Self {
        SyncErrorV1::Store(e)
    }
}

impl From<std::io::Error> for SyncErrorV1 {
    fn from(e: std::io::Error) -> Self {
        use std::io::ErrorKind;
        match e.kind() {
            ErrorKind::TimedOut | ErrorKind::WouldBlock => SyncErrorV1::Timeout,
            ErrorKind::UnexpectedEof
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::BrokenPipe => SyncErrorV1::Disconnected,
            _ => SyncErrorV1::Io(e),
        }
    }
}

/// Sync result type.
pub type SyncResult<T> = Result<T, SyncErrorV1>;

/// Sync server configuration.
#[derive(Debug, Clone)]
pub struct SyncServerCfgV1 {
    /// Maximum chunk size sent by the server.
    pub max_chunk_bytes: u32,
    /// Maximum artifact size served by the server.
    pub max_artifact_bytes: u32,
    /// Read and write timeout in milliseconds (0 disables).
    pub rw_timeout_ms: u32,
}

impl Default for SyncServerCfgV1 {
    fn default() -> Self {
        Self {
            max_chunk_bytes: p::DEFAULT_MAX_CHUNK_BYTES,
            max_artifact_bytes: p::DEFAULT_MAX_ARTIFACT_BYTES,
            rw_timeout_ms: 30_000,
        }
    }
}

/// Sync client configuration.
#[derive(Debug, Clone)]
pub struct SyncClientCfgV1 {
    /// Maximum request frame size accepted by the client.
    pub max_req_frame_bytes: u32,
    /// Maximum chunk size accepted by the client.
    pub max_chunk_bytes: u32,
    /// Maximum artifact size accepted by the client.
    pub max_artifact_bytes: u32,
    /// Read and write timeout in milliseconds (0 disables).
    pub rw_timeout_ms: u32,
}

impl Default for SyncClientCfgV1 {
    fn default() -> Self {
        Self {
            max_req_frame_bytes: p::DEFAULT_MAX_REQ_FRAME_BYTES,
            max_chunk_bytes: p::DEFAULT_MAX_CHUNK_BYTES,
            max_artifact_bytes: p::DEFAULT_MAX_ARTIFACT_BYTES,
            rw_timeout_ms: 30_000,
        }
    }
}

/// Sync summary statistics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncStatsV1 {
    /// Total number of unique artifacts required for this sync.
    pub needed_total: u32,

    /// Number of artifacts already present locally.
    pub already_present: u32,
    /// Number of artifacts fetched from the remote.
    pub fetched: u32,
    /// Total bytes written to the local store for fetched artifacts.
    pub bytes_fetched: u64,
}

impl SyncStatsV1 {
    /// Create an empty stats record.
    pub fn new() -> Self {
        Self {
            needed_total: 0,
            already_present: 0,
            fetched: 0,
            bytes_fetched: 0,
        }
    }
}

fn io_to_artifact(msg: &'static str, e: std::io::Error) -> ArtifactError {
    let _ = msg;
    ArtifactError::from(e)
}

fn ensure_parent_dir(path: &Path) -> ArtifactResult<()> {
    if let Some(p) = path.parent() {
        fs::create_dir_all(p).map_err(|e| io_to_artifact("create_dir_all failed", e))?;
    }
    Ok(())
}

fn tmp_candidates(final_path: &Path, hash: &Hash32) -> [PathBuf; 4] {
    // Mirror the store's short temp naming strategy to reduce Windows MAX_PATH risk.
    let start = (hash[31] & 0x03) as usize;
    let mut out = [
        PathBuf::new(),
        PathBuf::new(),
        PathBuf::new(),
        PathBuf::new(),
    ];

    let parent = final_path.parent().unwrap_or_else(|| Path::new("."));
    let hex = hex32(hash);
    let prefix = &hex[0..16];
    let pid = std::process::id();
    for i in 0..4 {
        let s = (start + i) & 0x03;
        out[i] = parent.join(format!("_tmp{}_{}_{}.bin", s, pid, prefix));
    }
    out
}

fn apply_rw_timeout(stream: &TcpStream, ms: u32) -> SyncResult<()> {
    use std::time::Duration;
    if ms == 0 {
        // Disable timeouts.
        stream.set_read_timeout(None).map_err(SyncErrorV1::from)?;
        stream.set_write_timeout(None).map_err(SyncErrorV1::from)?;
        return Ok(());
    }
    let d = Duration::from_millis(ms as u64);
    stream
        .set_read_timeout(Some(d))
        .map_err(SyncErrorV1::from)?;
    stream
        .set_write_timeout(Some(d))
        .map_err(SyncErrorV1::from)?;
    Ok(())
}

fn atomic_write_verified_from_chunks(
    store: &FsArtifactStore,
    expected: &Hash32,
    total_len: u32,
    chunks: &mut dyn FnMut() -> SyncResult<Option<Vec<u8>>>,
) -> SyncResult<u64> {
    let final_path = store.path_for(expected);
    if final_path.exists() {
        // Already present, skip.
        return Ok(0);
    }
    ensure_parent_dir(&final_path)?;

    if total_len == 0 {
        return Err(SyncErrorV1::Proto("bad total len"));
    }

    let tmp_paths = tmp_candidates(&final_path, expected);
    let mut last_err: Option<std::io::Error> = None;
    for tmp in tmp_paths.iter() {
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(tmp)
        {
            Ok(mut f) => {
                let mut hasher = blake3::Hasher::new();
                let mut wrote: u64 = 0;
                loop {
                    let next = match chunks() {
                        Ok(v) => v,
                        Err(e) => {
                            let _ = fs::remove_file(tmp);
                            return Err(e);
                        }
                    };
                    match next {
                        None => break,
                        Some(b) => {
                            if (wrote as u64) + (b.len() as u64) > (total_len as u64) {
                                let _ = fs::remove_file(tmp);
                                return Err(SyncErrorV1::Proto("received too many bytes"));
                            }
                            f.write_all(&b).map_err(|e| {
                                let _ = fs::remove_file(tmp);
                                SyncErrorV1::Io(e)
                            })?;
                            hasher.update(&b);
                            wrote += b.len() as u64;
                        }
                    }
                }
                if wrote != total_len as u64 {
                    let _ = fs::remove_file(tmp);
                    return Err(SyncErrorV1::Proto("received wrong length"));
                }
                f.flush().map_err(|e| {
                    let _ = fs::remove_file(tmp);
                    SyncErrorV1::Io(e)
                })?;
                drop(f);

                let got = *hasher.finalize().as_bytes();
                if &got != expected {
                    let _ = fs::remove_file(tmp);
                    return Err(SyncErrorV1::Proto("hash mismatch"));
                }

                match fs::rename(tmp, &final_path) {
                    Ok(()) => return Ok(wrote),
                    Err(e) => {
                        if final_path.exists() {
                            let _ = fs::remove_file(tmp);
                            return Ok(0);
                        }
                        let _ = fs::remove_file(tmp);
                        return Err(SyncErrorV1::Io(e));
                    }
                }
            }
            Err(e) => {
                last_err = Some(e);
                continue;
            }
        }
    }

    if let Some(e) = last_err {
        Err(SyncErrorV1::Io(e))
    } else {
        Err(SyncErrorV1::Proto("failed to create temp file"))
    }
}

fn do_hello(stream: &mut TcpStream, cfg: &SyncClientCfgV1) -> SyncResult<p::HelloAckV1> {
    let hello = p::HelloV1 {
        version: p::ARTIFACT_SYNC_V1_VERSION,
        max_chunk_bytes: cfg.max_chunk_bytes,
        max_artifact_bytes: cfg.max_artifact_bytes,
    };
    let payload =
        p::encode_hello_v1(&hello).map_err(|_| SyncErrorV1::Proto("encode hello failed"))?;
    net::write_frame(stream, &payload).map_err(SyncErrorV1::from)?;
    let resp = net::read_frame(stream, cfg.max_req_frame_bytes).map_err(SyncErrorV1::from)?;
    // Could be HELLO_ACK or ERR.
    let first = *resp
        .get(0)
        .ok_or_else(|| SyncErrorV1::Proto("empty frame"))?;
    match p::SyncMsgKind::from_u8(first).map_err(|_| SyncErrorV1::Proto("decode kind failed"))? {
        p::SyncMsgKind::HelloAck => {
            let ack = p::decode_hello_ack_v1(&resp)
                .map_err(|_| SyncErrorV1::Proto("decode hello_ack failed"))?;
            if ack.version != p::ARTIFACT_SYNC_V1_VERSION {
                return Err(SyncErrorV1::Proto("bad protocol version"));
            }
            Ok(ack)
        }
        p::SyncMsgKind::Err => {
            let e = p::decode_err_v1(&resp).map_err(|_| SyncErrorV1::Proto("decode err failed"))?;
            Err(SyncErrorV1::Remote(e.msg))
        }
        _ => Err(SyncErrorV1::Proto("unexpected hello response")),
    }
}

fn remote_err_msg(payload: &[u8]) -> Option<String> {
    if payload.is_empty() {
        return None;
    }
    if let Ok(k) = p::SyncMsgKind::from_u8(payload[0]) {
        if k == p::SyncMsgKind::Err {
            if let Ok(e) = p::decode_err_v1(payload) {
                return Some(e.msg);
            }
        }
    }
    None
}

/// Fetch one artifact by hash into the local store.
/// Returns Ok(true) if fetched, Ok(false) if already present.
pub fn fetch_one_v1(
    store: &FsArtifactStore,
    stream: &mut TcpStream,
    expected: &Hash32,
    cfg: &SyncClientCfgV1,
) -> SyncResult<(bool, u64)> {
    // Check local.
    if store.get(expected)?.is_some() {
        return Ok((false, 0));
    }

    let req =
        p::encode_get_req_v1(expected).map_err(|_| SyncErrorV1::Proto("encode get failed"))?;
    net::write_frame(stream, &req).map_err(SyncErrorV1::from)?;

    let begin_payload =
        net::read_frame(stream, cfg.max_req_frame_bytes).map_err(SyncErrorV1::from)?;
    if let Some(msg) = remote_err_msg(&begin_payload) {
        return Err(SyncErrorV1::Remote(msg));
    }
    let begin = p::decode_get_begin_v1(&begin_payload)
        .map_err(|_| SyncErrorV1::Proto("decode get_begin failed"))?;
    if !begin.found {
        return Err(SyncErrorV1::Proto("not found"));
    }
    if begin.total_len == 0 {
        return Err(SyncErrorV1::Proto("bad total len"));
    }
    if begin.total_len > cfg.max_artifact_bytes {
        return Err(SyncErrorV1::Proto("artifact too large"));
    }

    let mut done = false;
    let mut bytes_seen: u64 = 0;

    // Provide chunks via closure so the atomic writer can drive consumption.
    let mut next_chunk = || -> SyncResult<Option<Vec<u8>>> {
        if done {
            return Ok(None);
        }
        let payload =
            net::read_frame(stream, cfg.max_chunk_bytes + 16).map_err(SyncErrorV1::from)?;
        if let Some(msg) = remote_err_msg(&payload) {
            return Err(SyncErrorV1::Remote(msg));
        }
        let first = *payload
            .get(0)
            .ok_or_else(|| SyncErrorV1::Proto("empty frame"))?;
        let k =
            p::SyncMsgKind::from_u8(first).map_err(|_| SyncErrorV1::Proto("decode kind failed"))?;
        match k {
            p::SyncMsgKind::GetChunk => {
                let c = p::decode_get_chunk_v1(&payload)
                    .map_err(|_| SyncErrorV1::Proto("decode get_chunk failed"))?;
                bytes_seen += c.bytes.len() as u64;
                Ok(Some(c.bytes))
            }
            p::SyncMsgKind::GetEnd => {
                p::decode_get_end_v1(&payload)
                    .map_err(|_| SyncErrorV1::Proto("decode get_end failed"))?;
                done = true;
                Ok(None)
            }
            _ => Err(SyncErrorV1::Proto("unexpected get stream msg")),
        }
    };

    let wrote =
        atomic_write_verified_from_chunks(store, expected, begin.total_len, &mut next_chunk)?;
    // Ensure stream ended and counts match.
    if !done {
        // Drain until GetEnd if writer exited early.
        loop {
            let payload =
                net::read_frame(stream, cfg.max_chunk_bytes + 16).map_err(SyncErrorV1::from)?;
            if let Some(msg) = remote_err_msg(&payload) {
                return Err(SyncErrorV1::Remote(msg));
            }
            let k = p::SyncMsgKind::from_u8(payload[0])
                .map_err(|_| SyncErrorV1::Proto("decode kind failed"))?;
            match k {
                p::SyncMsgKind::GetChunk => {
                    let c = p::decode_get_chunk_v1(&payload)
                        .map_err(|_| SyncErrorV1::Proto("decode get_chunk failed"))?;
                    bytes_seen += c.bytes.len() as u64;
                }
                p::SyncMsgKind::GetEnd => {
                    p::decode_get_end_v1(&payload)
                        .map_err(|_| SyncErrorV1::Proto("decode get_end failed"))?;
                    break;
                }
                _ => return Err(SyncErrorV1::Proto("unexpected get stream msg")),
            }
        }
    }
    if bytes_seen != begin.total_len as u64 {
        return Err(SyncErrorV1::Proto("stream length mismatch"));
    }
    Ok((true, wrote))
}

fn collect_needed_from_reduce(
    store: &FsArtifactStore,
    reduce_manifest_hash: &Hash32,
    reduce: &ReduceManifestV1,
) -> SyncResult<Vec<Hash32>> {
    // We require reduce outputs to contain these tags.
    let mut snap_hash: Option<Hash32> = None;
    let mut sig_hash: Option<Hash32> = None;
    for o in reduce.outputs.iter() {
        if o.tag == "index_snapshot_v1" {
            snap_hash = Some(o.hash);
        } else if o.tag == "index_sig_map_v1" {
            sig_hash = Some(o.hash);
        }
    }
    let snap_h = snap_hash.ok_or_else(|| SyncErrorV1::Proto("reduce missing index_snapshot_v1"))?;
    let sig_h = sig_hash.ok_or_else(|| SyncErrorV1::Proto("reduce missing index_sig_map_v1"))?;

    let snap_b = store
        .get(&snap_h)?
        .ok_or_else(|| SyncErrorV1::Proto("missing snapshot"))?;
    let sig_b = store
        .get(&sig_h)?
        .ok_or_else(|| SyncErrorV1::Proto("missing sig map"))?;
    let snap = IndexSnapshotV1::decode(&snap_b)
        .map_err(|_| SyncErrorV1::Proto("decode snapshot failed"))?;
    let sig =
        IndexSigMapV1::decode(&sig_b).map_err(|_| SyncErrorV1::Proto("decode sig map failed"))?;

    let mut out: Vec<Hash32> = Vec::new();
    out.push(*reduce_manifest_hash);
    out.push(snap_h);
    out.push(sig_h);

    for e in snap.entries.iter() {
        out.push(e.frame_seg);
        out.push(e.index_seg);
    }
    for s in sig.entries.iter() {
        out.push(s.sig);
    }

    // Sort by bytes for stable ordering.
    out.sort();
    out.dedup();
    Ok(out)
}

/// Sync a ReduceManifestV1 and all referenced artifacts from a remote store.
pub fn sync_reduce_v1(
    local_store: &FsArtifactStore,
    remote_addr: &str,
    reduce_manifest_hash: &Hash32,
    cfg: &SyncClientCfgV1,
) -> SyncResult<SyncStatsV1> {
    use std::collections::BTreeSet;

    let mut stats = SyncStatsV1::new();
    let mut handled: BTreeSet<Hash32> = BTreeSet::new();

    let mut stream = TcpStream::connect(remote_addr).map_err(SyncErrorV1::from)?;
    stream.set_nodelay(true).ok();

    apply_rw_timeout(&stream, cfg.rw_timeout_ms)?;

    let _ack = do_hello(&mut stream, cfg)?;

    // Helper: ensure one artifact is present locally, fetching if needed.
    let mut ensure_one = |h: &Hash32| -> SyncResult<()> {
        if handled.contains(h) {
            return Ok(());
        }
        let (fetched, wrote) = fetch_one_v1(local_store, &mut stream, h, cfg)?;
        if fetched {
            stats.fetched += 1;
            stats.bytes_fetched += wrote;
        } else {
            stats.already_present += 1;
        }
        handled.insert(*h);
        Ok(())
    };

    // Ensure reduce manifest exists, then decode it.
    ensure_one(reduce_manifest_hash)?;
    let reduce_b = local_store
        .get(reduce_manifest_hash)?
        .ok_or_else(|| SyncErrorV1::Proto("missing reduce manifest after fetch"))?;
    let reduce = ReduceManifestV1::decode(&reduce_b)
        .map_err(|_| SyncErrorV1::Proto("decode reduce manifest failed"))?;

    // Ensure referenced outputs (snapshot + sig-map) exist so we can derive the full needed list.
    let mut snap_h: Option<Hash32> = None;
    let mut sig_h: Option<Hash32> = None;
    for o in reduce.outputs.iter() {
        if o.tag == "index_snapshot_v1" {
            snap_h = Some(o.hash);
        } else if o.tag == "index_sig_map_v1" {
            sig_h = Some(o.hash);
        }
    }
    let snap_hash = snap_h.ok_or_else(|| SyncErrorV1::Proto("reduce missing index_snapshot_v1"))?;
    let sig_hash = sig_h.ok_or_else(|| SyncErrorV1::Proto("reduce missing index_sig_map_v1"))?;

    ensure_one(&snap_hash)?;
    ensure_one(&sig_hash)?;

    // Derive complete needed list deterministically.
    let needed = collect_needed_from_reduce(local_store, reduce_manifest_hash, &reduce)?;
    stats.needed_total = needed.len() as u32;

    // Ensure all needed artifacts are present (skip local hits deterministically).
    for h in needed.iter() {
        ensure_one(h)?;
    }

    Ok(stats)
}

/// Per-manifest summary for batch sync.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncBatchManifestReportV1 {
    /// ReduceManifestV1 hash.
    pub reduce_manifest: Hash32,
    /// Total number of unique artifacts required for this manifest closure.
    pub needed_total: u32,
}

/// Batch sync report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncBatchReportV1 {
    /// Global sync stats for the union closure.
    pub stats: SyncStatsV1,
    /// Per-manifest closure summaries (in input order).
    pub manifests: Vec<SyncBatchManifestReportV1>,
}

/// Sync multiple ReduceManifestV1 closures in one TCP session.
///
/// This performs deterministic batching:
///
/// 1) Ensure all reduce manifests (sorted hash order).
/// 2) Ensure all referenced snapshot + sig-map artifacts (sorted hash order).
/// 3) Decode each manifest closure and build the union set.
/// 4) Ensure the union set (sorted hash order).
///
/// Already-present artifacts are skipped deterministically.
pub fn sync_reduce_batch_v1(
    local_store: &FsArtifactStore,
    remote_addr: &str,
    reduce_manifest_hashes: &[Hash32],
    cfg: &SyncClientCfgV1,
) -> SyncResult<SyncBatchReportV1> {
    use std::collections::{BTreeMap, BTreeSet};

    if reduce_manifest_hashes.is_empty() {
        return Err(SyncErrorV1::Proto("empty reduce manifest list"));
    }

    let mut stats = SyncStatsV1::new();
    let mut handled: BTreeSet<Hash32> = BTreeSet::new();

    let mut stream = TcpStream::connect(remote_addr).map_err(SyncErrorV1::from)?;
    stream.set_nodelay(true).ok();
    apply_rw_timeout(&stream, cfg.rw_timeout_ms)?;
    let _ack = do_hello(&mut stream, cfg)?;

    // Helper: ensure one artifact is present locally, fetching if needed.
    let mut ensure_one = |h: &Hash32| -> SyncResult<()> {
        if handled.contains(h) {
            return Ok(());
        }
        let (fetched, wrote) = fetch_one_v1(local_store, &mut stream, h, cfg)?;
        if fetched {
            stats.fetched += 1;
            stats.bytes_fetched += wrote;
        } else {
            stats.already_present += 1;
        }
        handled.insert(*h);
        Ok(())
    };

    // Stage 1: ensure all reduce manifests in sorted order.
    let mut uniq_reduce: Vec<Hash32> = reduce_manifest_hashes.to_vec();
    uniq_reduce.sort();
    uniq_reduce.dedup();
    for h in uniq_reduce.iter() {
        ensure_one(h)?;
    }

    // Stage 2: load each reduce manifest (input order) and collect snapshot + sig-map hashes.
    // Store decoded ReduceManifestV1 to avoid re-decoding later.
    let mut decoded: BTreeMap<Hash32, ReduceManifestV1> = BTreeMap::new();
    let mut snap_sig: Vec<Hash32> = Vec::new();

    for h in reduce_manifest_hashes.iter() {
        let reduce_b = local_store
            .get(h)?
            .ok_or_else(|| SyncErrorV1::Proto("missing reduce manifest after fetch"))?;
        let reduce = ReduceManifestV1::decode(&reduce_b)
            .map_err(|_| SyncErrorV1::Proto("decode reduce manifest failed"))?;

        let mut snap_h: Option<Hash32> = None;
        let mut sig_h: Option<Hash32> = None;
        for o in reduce.outputs.iter() {
            if o.tag == "index_snapshot_v1" {
                snap_h = Some(o.hash);
            } else if o.tag == "index_sig_map_v1" {
                sig_h = Some(o.hash);
            }
        }
        let snap = snap_h.ok_or_else(|| SyncErrorV1::Proto("reduce missing index_snapshot_v1"))?;
        let sig = sig_h.ok_or_else(|| SyncErrorV1::Proto("reduce missing index_sig_map_v1"))?;

        snap_sig.push(snap);
        snap_sig.push(sig);
        decoded.insert(*h, reduce);
    }

    snap_sig.sort();
    snap_sig.dedup();
    for h in snap_sig.iter() {
        ensure_one(h)?;
    }

    // Stage 3: compute per-manifest closures and build union.
    let mut union: BTreeSet<Hash32> = BTreeSet::new();
    let mut per: Vec<SyncBatchManifestReportV1> = Vec::new();
    for h in reduce_manifest_hashes.iter() {
        let reduce = decoded
            .get(h)
            .ok_or_else(|| SyncErrorV1::Proto("missing decoded reduce"))?;
        let needed = collect_needed_from_reduce(local_store, h, reduce)?;
        per.push(SyncBatchManifestReportV1 {
            reduce_manifest: *h,
            needed_total: needed.len() as u32,
        });
        for x in needed.iter() {
            union.insert(*x);
        }
    }

    // Stage 4: ensure union closure in sorted order.
    stats.needed_total = union.len() as u32;
    for h in union.iter() {
        ensure_one(h)?;
    }

    Ok(SyncBatchReportV1 {
        stats,
        manifests: per,
    })
}

fn send_err(stream: &mut TcpStream, msg: &str) -> std::io::Result<()> {
    let payload = p::encode_err_v1(msg)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "encode err failed"))?;
    net::write_frame(stream, &payload)
}

fn serve_one_get(
    stream: &mut TcpStream,
    store: &FsArtifactStore,
    hash: &Hash32,
    cfg: &SyncServerCfgV1,
) -> std::io::Result<()> {
    let path = store.path_for(hash);
    if !path.exists() {
        let begin = p::encode_get_begin_v1(false, 0)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "encode begin failed"))?;
        net::write_frame(stream, &begin)?;
        return Ok(());
    }
    let meta = fs::metadata(&path)?;
    let len_u64 = meta.len();
    if len_u64 == 0 {
        return send_err(stream, "empty artifact");
    }
    if len_u64 > (cfg.max_artifact_bytes as u64) {
        return send_err(stream, "artifact too large");
    }
    let total_len = len_u64 as u32;
    let begin = p::encode_get_begin_v1(true, total_len)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "encode begin failed"))?;
    net::write_frame(stream, &begin)?;

    let mut f = fs::File::open(&path)?;
    let mut buf = vec![0u8; cfg.max_chunk_bytes as usize];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        let payload = p::encode_get_chunk_v1(&buf[..n])
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "encode chunk failed"))?;
        net::write_frame(stream, &payload)?;
    }
    let end = p::encode_get_end_v1()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "encode end failed"))?;
    net::write_frame(stream, &end)?;
    Ok(())
}

/// Handle one sync client connection.
pub fn handle_sync_client_v1(
    mut stream: TcpStream,
    store: FsArtifactStore,
    cfg: SyncServerCfgV1,
) -> std::io::Result<()> {
    // Apply connection timeouts.
    if cfg.rw_timeout_ms == 0 {
        let _ = stream.set_read_timeout(None);
        let _ = stream.set_write_timeout(None);
    } else {
        let d = std::time::Duration::from_millis(cfg.rw_timeout_ms as u64);
        let _ = stream.set_read_timeout(Some(d));
        let _ = stream.set_write_timeout(Some(d));
    }

    // Expect HELLO first.
    let hello_payload = net::read_frame(&mut stream, p::DEFAULT_MAX_REQ_FRAME_BYTES)?;
    let hello = match p::decode_hello_v1(&hello_payload) {
        Ok(h) => h,
        Err(_) => {
            let _ = send_err(&mut stream, "bad hello");
            return Ok(());
        }
    };
    if hello.version != p::ARTIFACT_SYNC_V1_VERSION {
        let _ = send_err(&mut stream, "bad version");
        return Ok(());
    }
    let ack = p::HelloAckV1 {
        version: p::ARTIFACT_SYNC_V1_VERSION,
        max_chunk_bytes: cfg.max_chunk_bytes,
        max_artifact_bytes: cfg.max_artifact_bytes,
    };
    let ack_payload = p::encode_hello_ack_v1(&ack)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "encode ack failed"))?;
    net::write_frame(&mut stream, &ack_payload)?;

    loop {
        let payload = match net::read_frame(&mut stream, p::DEFAULT_MAX_REQ_FRAME_BYTES) {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };
        if payload.is_empty() {
            let _ = send_err(&mut stream, "empty req");
            return Ok(());
        }
        let k = match p::SyncMsgKind::from_u8(payload[0]) {
            Ok(v) => v,
            Err(_) => {
                let _ = send_err(&mut stream, "bad kind");
                return Ok(());
            }
        };
        match k {
            p::SyncMsgKind::Get => {
                let req = match p::decode_get_req_v1(&payload) {
                    Ok(r) => r,
                    Err(_) => {
                        let _ = send_err(&mut stream, "bad get");
                        return Ok(());
                    }
                };
                let _ = serve_one_get(&mut stream, &store, &req.hash, &cfg);
            }
            _ => {
                let _ = send_err(&mut stream, "unsupported");
                return Ok(());
            }
        }
    }
}

/// Run a sync server (sequential accept loop for determinism).
pub fn run_sync_server_v1(root: &Path, addr: &str, cfg: SyncServerCfgV1) -> SyncResult<()> {
    let store = FsArtifactStore::new(root)?;
    let listener = TcpListener::bind(addr).map_err(SyncErrorV1::Io)?;
    for conn in listener.incoming() {
        match conn {
            Ok(stream) => {
                let st = store.clone();
                let cfg2 = cfg.clone();
                if let Err(_e) = handle_sync_client_v1(stream, st, cfg2) {
                    // Ignore per-client errors.
                }
            }
            Err(e) => {
                return Err(SyncErrorV1::Io(e));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::ArtifactStore;
    use crate::frame::{Id64, SourceId};
    use crate::hash::blake3_hash;
    use crate::index_sig_map::{IndexSigMapEntryV1, IndexSigMapV1};
    use crate::index_snapshot::{IndexSnapshotEntryV1, IndexSnapshotV1};
    use crate::reduce_manifest::{ReduceManifestV1, ReduceOutputV1, REDUCE_MANIFEST_V1_VERSION};
    use std::thread;
    use std::time::Duration;

    fn tmp_dir(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push("fsa_lm_tests");
        p.push(name);
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn any_tmp_files_under(root: &Path) -> bool {
        let mut stack: Vec<PathBuf> = Vec::new();
        stack.push(root.to_path_buf());
        while let Some(p) = stack.pop() {
            let rd = match fs::read_dir(&p) {
                Ok(r) => r,
                Err(_) => continue,
            };
            for ent in rd {
                let ent = match ent {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let path = ent.path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                    if name.starts_with("_tmp") {
                        return true;
                    }
                }
            }
        }
        false
    }

    #[test]
    fn sync_reduce_round_trip_small() {
        let src = tmp_dir("sync_reduce_round_trip_small_src");
        let dst = tmp_dir("sync_reduce_round_trip_small_dst");
        let src_store = FsArtifactStore::new(&src).unwrap();
        let dst_store = FsArtifactStore::new(&dst).unwrap();

        // Create referenced artifacts.
        let frame_bytes = b"frame".to_vec();
        let index_bytes = b"index".to_vec();
        let sig_bytes = b"sig".to_vec();
        let frame_h = src_store.put(&frame_bytes).unwrap();
        let index_h = src_store.put(&index_bytes).unwrap();
        let sig_h = src_store.put(&sig_bytes).unwrap();

        let snap = IndexSnapshotV1 {
            version: 1,
            source_id: SourceId(Id64(1)),
            entries: vec![IndexSnapshotEntryV1 {
                frame_seg: frame_h,
                index_seg: index_h,
                row_count: 1,
                term_count: 1,
                postings_bytes: 1,
            }],
        };
        let snap_b = snap.encode().unwrap();
        let snap_h = src_store.put(&snap_b).unwrap();

        let sig = IndexSigMapV1 {
            source_id: SourceId(Id64(1)),
            entries: vec![IndexSigMapEntryV1 {
                index_seg: index_h,
                sig: sig_h,
            }],
        };
        let sig_b = sig.encode().unwrap();
        let sig_hh = src_store.put(&sig_b).unwrap();

        let reduce = ReduceManifestV1 {
            version: REDUCE_MANIFEST_V1_VERSION,
            shard_manifest: blake3_hash(b"shard"),
            shard_count: 2,
            mapping_id: "doc_id_hash32_v1".to_string(),
            source_id_u64: 1,
            snapshot_entries: 1,
            copied_frame_segs: 1,
            copied_index_segs: 1,
            copied_segment_sigs: 1,
            outputs: vec![
                ReduceOutputV1 {
                    tag: "index_sig_map_v1".to_string(),
                    hash: sig_hh,
                },
                ReduceOutputV1 {
                    tag: "index_snapshot_v1".to_string(),
                    hash: snap_h,
                },
            ],
        };
        let reduce_b = reduce.encode().unwrap();
        let reduce_h = src_store.put(&reduce_b).unwrap();

        // Start server on ephemeral port.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let addr_s = format!("{}:{}", addr.ip(), addr.port());

        let cfg = SyncServerCfgV1::default();
        let st = src_store.clone();
        let th = thread::spawn(move || {
            for conn in listener.incoming() {
                if let Ok(stream) = conn {
                    let _ = handle_sync_client_v1(stream, st.clone(), cfg.clone());
                    break;
                }
            }
        });

        let cfgc = SyncClientCfgV1::default();
        let stats = sync_reduce_v1(&dst_store, &addr_s, &reduce_h, &cfgc).unwrap();
        assert_eq!(stats.needed_total, 6);
        assert!(stats.fetched >= 3);
        assert!(dst_store.get(&reduce_h).unwrap().is_some());
        assert!(dst_store.get(&snap_h).unwrap().is_some());
        assert!(dst_store.get(&sig_hh).unwrap().is_some());
        assert!(dst_store.get(&frame_h).unwrap().is_some());
        assert!(dst_store.get(&index_h).unwrap().is_some());
        assert!(dst_store.get(&sig_h).unwrap().is_some());

        let _ = th.join();
    }
    #[test]
    fn sync_reduce_skips_already_present_artifacts() {
        let src = tmp_dir("sync_reduce_skips_already_present_artifacts_src");
        let dst = tmp_dir("sync_reduce_skips_already_present_artifacts_dst");
        let src_store = FsArtifactStore::new(&src).unwrap();
        let dst_store = FsArtifactStore::new(&dst).unwrap();

        // Create referenced artifacts.
        let frame_bytes = b"frame".to_vec();
        let index_bytes = b"index".to_vec();
        let sig_bytes = b"sig".to_vec();
        let frame_h = src_store.put(&frame_bytes).unwrap();
        let index_h = src_store.put(&index_bytes).unwrap();
        let sig_h = src_store.put(&sig_bytes).unwrap();

        let snap = IndexSnapshotV1 {
            version: 1,
            source_id: SourceId(Id64(1)),
            entries: vec![IndexSnapshotEntryV1 {
                frame_seg: frame_h,
                index_seg: index_h,
                row_count: 1,
                term_count: 1,
                postings_bytes: 1,
            }],
        };
        let snap_b = snap.encode().unwrap();
        let snap_h = src_store.put(&snap_b).unwrap();

        let sig = IndexSigMapV1 {
            source_id: SourceId(Id64(1)),
            entries: vec![IndexSigMapEntryV1 {
                index_seg: index_h,
                sig: sig_h,
            }],
        };
        let sig_b = sig.encode().unwrap();
        let sig_hh = src_store.put(&sig_b).unwrap();

        let reduce = ReduceManifestV1 {
            version: REDUCE_MANIFEST_V1_VERSION,
            shard_manifest: blake3_hash(b"shard"),
            shard_count: 2,
            mapping_id: "doc_id_hash32_v1".to_string(),
            source_id_u64: 1,
            snapshot_entries: 1,
            copied_frame_segs: 1,
            copied_index_segs: 1,
            copied_segment_sigs: 1,
            outputs: vec![
                ReduceOutputV1 {
                    tag: "index_sig_map_v1".to_string(),
                    hash: sig_hh,
                },
                ReduceOutputV1 {
                    tag: "index_snapshot_v1".to_string(),
                    hash: snap_h,
                },
            ],
        };
        let reduce_b = reduce.encode().unwrap();
        let reduce_h = src_store.put(&reduce_b).unwrap();

        // Seed destination with some artifacts (reduce manifest, snapshot, and frame segment).
        let r2 = dst_store.put(&reduce_b).unwrap();
        assert_eq!(r2, reduce_h);
        let s2h = dst_store.put(&snap_b).unwrap();
        assert_eq!(s2h, snap_h);
        let f2h = dst_store.put(&frame_bytes).unwrap();
        assert_eq!(f2h, frame_h);

        // Start server on ephemeral port.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let addr_s = format!("{}:{}", addr.ip(), addr.port());

        let cfg = SyncServerCfgV1::default();
        let st = src_store.clone();
        let th = thread::spawn(move || {
            for conn in listener.incoming() {
                if let Ok(stream) = conn {
                    let _ = handle_sync_client_v1(stream, st.clone(), cfg.clone());
                    break;
                }
            }
        });

        let cfgc = SyncClientCfgV1::default();
        let stats = sync_reduce_v1(&dst_store, &addr_s, &reduce_h, &cfgc).unwrap();
        assert_eq!(stats.needed_total, 6);
        assert_eq!(stats.already_present, 3);
        assert_eq!(stats.fetched, 3);
        assert!(stats.bytes_fetched > 0);

        assert!(dst_store.get(&reduce_h).unwrap().is_some());
        assert!(dst_store.get(&snap_h).unwrap().is_some());
        assert!(dst_store.get(&sig_hh).unwrap().is_some());
        assert!(dst_store.get(&frame_h).unwrap().is_some());
        assert!(dst_store.get(&index_h).unwrap().is_some());
        assert!(dst_store.get(&sig_h).unwrap().is_some());

        let _ = th.join();
    }

    #[test]
    fn sync_reduce_batch_two_manifests_overlap() {
        let src = tmp_dir("sync_reduce_batch_two_manifests_overlap_src");
        let dst = tmp_dir("sync_reduce_batch_two_manifests_overlap_dst");
        let src_store = FsArtifactStore::new(&src).unwrap();
        let dst_store = FsArtifactStore::new(&dst).unwrap();

        // Shared referenced artifacts.
        let frame_bytes = b"frame".to_vec();
        let index_bytes = b"index".to_vec();
        let sig_bytes = b"sig".to_vec();
        let frame_h = src_store.put(&frame_bytes).unwrap();
        let index_h = src_store.put(&index_bytes).unwrap();
        let sig_h = src_store.put(&sig_bytes).unwrap();

        let snap = IndexSnapshotV1 {
            version: 1,
            source_id: SourceId(Id64(1)),
            entries: vec![IndexSnapshotEntryV1 {
                frame_seg: frame_h,
                index_seg: index_h,
                row_count: 1,
                term_count: 1,
                postings_bytes: 1,
            }],
        };
        let snap_b = snap.encode().unwrap();
        let snap_h = src_store.put(&snap_b).unwrap();

        let sig = IndexSigMapV1 {
            source_id: SourceId(Id64(1)),
            entries: vec![IndexSigMapEntryV1 {
                index_seg: index_h,
                sig: sig_h,
            }],
        };
        let sig_b = sig.encode().unwrap();
        let sig_hh = src_store.put(&sig_b).unwrap();

        // Two ReduceManifestV1 artifacts pointing at the same merged outputs.
        let reduce1 = ReduceManifestV1 {
            version: REDUCE_MANIFEST_V1_VERSION,
            shard_manifest: blake3_hash(b"shard1"),
            shard_count: 2,
            mapping_id: "doc_id_hash32_v1".to_string(),
            source_id_u64: 1,
            snapshot_entries: 1,
            copied_frame_segs: 1,
            copied_index_segs: 1,
            copied_segment_sigs: 1,
            outputs: vec![
                ReduceOutputV1 {
                    tag: "index_sig_map_v1".to_string(),
                    hash: sig_hh,
                },
                ReduceOutputV1 {
                    tag: "index_snapshot_v1".to_string(),
                    hash: snap_h,
                },
            ],
        };
        let reduce2 = ReduceManifestV1 {
            version: REDUCE_MANIFEST_V1_VERSION,
            shard_manifest: blake3_hash(b"shard2"),
            shard_count: 2,
            mapping_id: "doc_id_hash32_v1".to_string(),
            source_id_u64: 1,
            snapshot_entries: 1,
            copied_frame_segs: 1,
            copied_index_segs: 1,
            copied_segment_sigs: 1,
            outputs: vec![
                ReduceOutputV1 {
                    tag: "index_sig_map_v1".to_string(),
                    hash: sig_hh,
                },
                ReduceOutputV1 {
                    tag: "index_snapshot_v1".to_string(),
                    hash: snap_h,
                },
            ],
        };
        let reduce1_h = src_store.put(&reduce1.encode().unwrap()).unwrap();
        let reduce2_h = src_store.put(&reduce2.encode().unwrap()).unwrap();

        // Start server on ephemeral port.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let addr_s = format!("{}:{}", addr.ip(), addr.port());

        let cfg = SyncServerCfgV1::default();
        let st = src_store.clone();
        let th = thread::spawn(move || {
            for conn in listener.incoming() {
                if let Ok(stream) = conn {
                    let _ = handle_sync_client_v1(stream, st.clone(), cfg.clone());
                    break;
                }
            }
        });

        let cfgc = SyncClientCfgV1::default();
        let report =
            sync_reduce_batch_v1(&dst_store, &addr_s, &[reduce1_h, reduce2_h], &cfgc).unwrap();

        assert_eq!(report.stats.needed_total, 7);
        assert_eq!(report.stats.already_present, 0);
        assert_eq!(report.stats.fetched, 7);
        assert_eq!(report.manifests.len(), 2);
        assert_eq!(report.manifests[0].needed_total, 6);
        assert_eq!(report.manifests[1].needed_total, 6);

        assert!(dst_store.get(&reduce1_h).unwrap().is_some());
        assert!(dst_store.get(&reduce2_h).unwrap().is_some());
        assert!(dst_store.get(&snap_h).unwrap().is_some());
        assert!(dst_store.get(&sig_hh).unwrap().is_some());
        assert!(dst_store.get(&frame_h).unwrap().is_some());
        assert!(dst_store.get(&index_h).unwrap().is_some());
        assert!(dst_store.get(&sig_h).unwrap().is_some());

        let _ = th.join();
    }

    #[test]
    fn sync_client_times_out_if_server_stalls() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let addr_s = format!("{}:{}", addr.ip(), addr.port());

        let th = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                // Read HELLO and then stall (no ACK).
                let _ = net::read_frame(&mut stream, p::DEFAULT_MAX_REQ_FRAME_BYTES);
                thread::sleep(Duration::from_millis(250));
            }
        });

        let mut stream = TcpStream::connect(&addr_s).unwrap();
        stream.set_nodelay(true).ok();

        let mut cfgc = SyncClientCfgV1::default();
        cfgc.rw_timeout_ms = 50;
        apply_rw_timeout(&stream, cfgc.rw_timeout_ms).unwrap();

        let err = do_hello(&mut stream, &cfgc).unwrap_err();
        assert!(matches!(err, SyncErrorV1::Timeout));

        let _ = th.join();
    }

    #[test]
    fn sync_client_reports_disconnected_on_mid_stream_drop() {
        let dst = tmp_dir("sync_client_reports_disconnected_on_mid_stream_drop_dst");
        let dst_store = FsArtifactStore::new(&dst).unwrap();
        let expected = blake3_hash(b"expected_hash_only");

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let addr_s = format!("{}:{}", addr.ip(), addr.port());

        let th = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                // HELLO -> ACK.
                let hello_payload =
                    net::read_frame(&mut stream, p::DEFAULT_MAX_REQ_FRAME_BYTES).unwrap();
                let _hello = p::decode_hello_v1(&hello_payload).unwrap();
                let ack = p::HelloAckV1 {
                    version: p::ARTIFACT_SYNC_V1_VERSION,
                    max_chunk_bytes: p::DEFAULT_MAX_CHUNK_BYTES,
                    max_artifact_bytes: p::DEFAULT_MAX_ARTIFACT_BYTES,
                };
                let ack_payload = p::encode_hello_ack_v1(&ack).unwrap();
                net::write_frame(&mut stream, &ack_payload).unwrap();

                // Expect GET.
                let get_payload =
                    net::read_frame(&mut stream, p::DEFAULT_MAX_REQ_FRAME_BYTES).unwrap();
                let get = p::decode_get_req_v1(&get_payload).unwrap();
                assert_eq!(get.hash, expected);

                // Begin + one chunk, then drop connection.
                let begin = p::encode_get_begin_v1(true, 2048).unwrap();
                net::write_frame(&mut stream, &begin).unwrap();

                let chunk = vec![0x41u8; 64];
                let payload = p::encode_get_chunk_v1(&chunk).unwrap();
                net::write_frame(&mut stream, &payload).unwrap();
                // Drop stream without GET_END.
            }
        });

        let mut stream = TcpStream::connect(&addr_s).unwrap();
        stream.set_nodelay(true).ok();
        let cfgc = SyncClientCfgV1::default();
        apply_rw_timeout(&stream, cfgc.rw_timeout_ms).unwrap();
        let _ack = do_hello(&mut stream, &cfgc).unwrap();

        let err = fetch_one_v1(&dst_store, &mut stream, &expected, &cfgc).unwrap_err();
        assert!(matches!(
            err,
            SyncErrorV1::Disconnected | SyncErrorV1::Timeout | SyncErrorV1::Io(_)
        ));
        assert!(dst_store.get(&expected).unwrap().is_none());
        assert!(!any_tmp_files_under(&dst));

        let _ = th.join();
    }
}
