// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use fsa_lm::artifact::{ArtifactStore, FsArtifactStore};
use fsa_lm::artifact_sync_v1 as p;
use fsa_lm::frame::{Id64, SourceId};
use fsa_lm::hash::{blake3_hash, hex32, Hash32};
use fsa_lm::index_sig_map::IndexSigMapV1;
use fsa_lm::index_snapshot::{IndexSnapshotEntryV1, IndexSnapshotV1};
use fsa_lm::net;
use fsa_lm::reduce_manifest::{ReduceManifestV1, ReduceOutputV1, REDUCE_MANIFEST_V1_VERSION};

use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

fn tmp_dir(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push("fsa_lm_tests");
    p.push(format!("{}_{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn pick_free_local_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn run_cmd(bin: &str, args: &[&str]) -> (i32, Vec<u8>, Vec<u8>) {
    let out = Command::new(bin).args(args).output().unwrap();
    (
        out.status.code().unwrap_or(-1),
        out.stdout,
        out.stderr,
    )
}

fn spawn_serve_sync(bin: &str, root: &str, addr: &str) -> (Child, String) {
    let mut child = Command::new(bin)
        .args(["serve-sync", "--root", root, "--addr", addr])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let stderr = child.stderr.take().unwrap();
    let mut rdr = BufReader::new(stderr);
    let mut first_line = String::new();
    let n = rdr.read_line(&mut first_line).unwrap();
    if n == 0 {
        let _ = child.wait();
        panic!("serve-sync exited early (no stderr output)");
    }

    let stderr = rdr.into_inner();
    std::thread::spawn(move || {
        let mut r = BufReader::new(stderr);
        let mut buf = String::new();
        loop {
            buf.clear();
            match r.read_line(&mut buf) {
                Ok(0) => break,
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });

    (child, first_line)
}

fn parse_u32_kv(line: &str, key: &str) -> Option<u32> {
    for part in line.split_whitespace() {
        if let Some(rest) = part.strip_prefix(key) {
            return rest.parse::<u32>().ok();
        }
    }
    None
}

fn parse_sync_stats_line(stdout: &str) -> (u32, u32, u32, u32) {
    let line = stdout.lines().next().unwrap_or("").trim();
    let needed_total = parse_u32_kv(line, "needed_total=").unwrap();
    let already_present = parse_u32_kv(line, "already_present=").unwrap();
    let fetched = parse_u32_kv(line, "fetched=").unwrap();
    let bytes_fetched = parse_u32_kv(line, "bytes_fetched=").unwrap();
    (needed_total, already_present, fetched, bytes_fetched)
}

fn any_tmp_files_under(root: &Path) -> bool {
    fn walk(dir: &Path) -> bool {
        let rd = match std::fs::read_dir(dir) {
            Ok(r) => r,
            Err(_) => return false,
        };
        for ent in rd {
            let ent = match ent {
                Ok(e) => e,
                Err(_) => continue,
            };
            let p = ent.path();
            let md = match ent.metadata() {
                Ok(m) => m,
                Err(_) => continue,
            };
            if md.is_dir() {
                if walk(&p) {
                    return true;
                }
                continue;
            }
            if !md.is_file() {
                continue;
            }
            let name = match p.file_name().and_then(|s| s.to_str()) {
                Some(s) => s,
                None => continue,
            };
            if name.starts_with("_tmp") {
                return true;
            }
        }
        false
    }
    walk(root)
}

fn build_minimal_reduce_manifest_bundle(store: &FsArtifactStore, shard_tag: &[u8]) -> Hash32 {
    // Minimal closure to exercise sync:
    // - ReduceManifestV1
    // - IndexSnapshotV1
    // - IndexSigMapV1
    // - A referenced frame segment, index segment, and segment sig.

    let frame_bytes = b"frame".to_vec();
    let index_bytes = b"index".to_vec();
    let sig_bytes = b"sig".to_vec();

    let frame_h = store.put(&frame_bytes).unwrap();
    let index_h = store.put(&index_bytes).unwrap();
    let sig_h = store.put(&sig_bytes).unwrap();

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
    let snap_h = store.put(&snap_b).unwrap();

    let mut sig_map = IndexSigMapV1::new(SourceId(Id64(1)));
    sig_map.push(index_h, sig_h);
    let sig_map_b = sig_map.encode().unwrap();
    let sig_map_h = store.put(&sig_map_b).unwrap();

    let reduce = ReduceManifestV1 {
        version: REDUCE_MANIFEST_V1_VERSION,
        shard_manifest: blake3_hash(shard_tag),
        shard_count: 1,
        mapping_id: "doc_id_hash32_v1".to_string(),
        source_id_u64: 1,
        snapshot_entries: 1,
        copied_frame_segs: 1,
        copied_index_segs: 1,
        copied_segment_sigs: 1,
        outputs: vec![
            ReduceOutputV1 {
                tag: "index_sig_map_v1".to_string(),
                hash: sig_map_h,
            },
            ReduceOutputV1 {
                tag: "index_snapshot_v1".to_string(),
                hash: snap_h,
            },
        ],
    };
    let reduce_b = reduce.encode().unwrap();
    store.put(&reduce_b).unwrap()
}

#[test]
fn sync_reduce_cli_times_out_if_server_stalls_before_ack() {
    let bin = env!("CARGO_BIN_EXE_fsa_lm");
    let dst_root = tmp_dir("sync_reduce_cli_times_out_dst");

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let addr_s = format!("{}:{}", addr.ip(), addr.port());

    let th = std::thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            // Read HELLO and then stall (no ACK).
            let _ = net::read_frame(&mut stream, p::DEFAULT_MAX_REQ_FRAME_BYTES);
            std::thread::sleep(Duration::from_millis(250));
        }
    });

    let reduce_h = [1u8; 32];
    let reduce_hex = hex32(&reduce_h);
    let (code, _stdout, stderr) = run_cmd(
        bin,
        &[
            "sync-reduce",
            "--root",
            dst_root.to_str().unwrap(),
            "--addr",
            &addr_s,
            "--reduce-manifest",
            &reduce_hex,
            "--rw_timeout_ms",
            "50",
        ],
    );

    assert_eq!(code, 1);
    let se = String::from_utf8_lossy(&stderr);
    assert!(se.to_lowercase().contains("timeout"), "stderr={}", se);

    let _ = th.join();
}

#[test]
fn sync_reduce_cli_reports_disconnected_on_mid_stream_drop() {
    let bin = env!("CARGO_BIN_EXE_fsa_lm");
    let dst_root = tmp_dir("sync_reduce_cli_mid_stream_drop_dst");

    let expected: Hash32 = blake3_hash(b"expected_reduce_hash_only");
    let expected_hex = hex32(&expected);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let addr_s = format!("{}:{}", addr.ip(), addr.port());

    let th = std::thread::spawn(move || {
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

            // Expect GET for reduce manifest.
            let get_payload =
                net::read_frame(&mut stream, p::DEFAULT_MAX_REQ_FRAME_BYTES).unwrap();
            let get = p::decode_get_req_v1(&get_payload).unwrap();
            assert_eq!(get.hash, expected);

            // Begin + one chunk, then drop connection before GET_END.
            let begin = p::encode_get_begin_v1(true, 2048).unwrap();
            net::write_frame(&mut stream, &begin).unwrap();

            let chunk = vec![0x41u8; 64];
            let payload = p::encode_get_chunk_v1(&chunk).unwrap();
            net::write_frame(&mut stream, &payload).unwrap();
            // Drop stream.
        }
    });

    let (code, _stdout, stderr) = run_cmd(
        bin,
        &[
            "sync-reduce",
            "--root",
            dst_root.to_str().unwrap(),
            "--addr",
            &addr_s,
            "--reduce-manifest",
            &expected_hex,
            "--rw_timeout_ms",
            "200",
        ],
    );

    assert_eq!(code, 1);
    let se = String::from_utf8_lossy(&stderr).to_lowercase();
    let ok = se.contains("disconnected") || se.contains("timeout") || se.contains("io error");
    assert!(ok, "stderr={}", String::from_utf8_lossy(&stderr));

    // Ensure we did not leave temp artifacts behind.
    assert!(!any_tmp_files_under(&dst_root));

    let _ = th.join();
}

#[test]
fn sync_reduce_cli_already_present_fast_path_is_stable() {
    let bin = env!("CARGO_BIN_EXE_fsa_lm");

    let base = tmp_dir("sync_reduce_cli_already_present_fast_path");
    let src_root = base.join("src");
    let dst_root = base.join("dst");
    std::fs::create_dir_all(&src_root).unwrap();
    std::fs::create_dir_all(&dst_root).unwrap();

    let src_store = FsArtifactStore::new(&src_root).unwrap();
    let reduce_h = build_minimal_reduce_manifest_bundle(&src_store, b"shard_fast_path");
    let reduce_hex = hex32(&reduce_h);

    let port = pick_free_local_port();
    let addr_s = format!("127.0.0.1:{}", port);
    let (mut server, _line) = spawn_serve_sync(bin, src_root.to_str().unwrap(), &addr_s);

    // First sync: destination empty.
    let (code1, stdout1, stderr1) = run_cmd(
        bin,
        &[
            "sync-reduce",
            "--root",
            dst_root.to_str().unwrap(),
            "--addr",
            &addr_s,
            "--reduce-manifest",
            &reduce_hex,
        ],
    );
    assert_eq!(code1, 0, "stderr={}", String::from_utf8_lossy(&stderr1));
    let s1 = String::from_utf8_lossy(&stdout1);
    let (needed1, already1, fetched1, bytes1) = parse_sync_stats_line(&s1);
    assert!(needed1 > 0);
    assert_eq!(already1, 0);
    assert_eq!(fetched1, needed1);
    assert!(bytes1 > 0);

    // Second sync: everything should be already present.
    let (code2, stdout2, stderr2) = run_cmd(
        bin,
        &[
            "sync-reduce",
            "--root",
            dst_root.to_str().unwrap(),
            "--addr",
            &addr_s,
            "--reduce-manifest",
            &reduce_hex,
        ],
    );
    assert_eq!(code2, 0, "stderr={}", String::from_utf8_lossy(&stderr2));
    let s2 = String::from_utf8_lossy(&stdout2);
    let (needed2, already2, fetched2, bytes2) = parse_sync_stats_line(&s2);
    assert_eq!(needed2, needed1);
    assert_eq!(fetched2, 0);
    assert_eq!(already2, needed1);
    assert_eq!(bytes2, 0);

    assert!(!any_tmp_files_under(&dst_root));

    let _ = server.kill();
    let _ = server.wait();
}

#[test]
fn sync_reduce_batch_cli_overlap_union_is_deduped_and_repeatable() {
    let bin = env!("CARGO_BIN_EXE_fsa_lm");

    let base = tmp_dir("sync_reduce_batch_cli_overlap");
    let src_root = base.join("src");
    let dst_root = base.join("dst");
    std::fs::create_dir_all(&src_root).unwrap();
    std::fs::create_dir_all(&dst_root).unwrap();

    let src_store = FsArtifactStore::new(&src_root).unwrap();
    let reduce1 = build_minimal_reduce_manifest_bundle(&src_store, b"overlap1");
    let reduce2 = build_minimal_reduce_manifest_bundle(&src_store, b"overlap2");

    let list_path = base.join("reduce_list.txt");
    let content = format!("{}\n{}\n", hex32(&reduce1), hex32(&reduce2));
    std::fs::write(&list_path, content.as_bytes()).unwrap();

    let port = pick_free_local_port();
    let addr_s = format!("127.0.0.1:{}", port);
    let (mut server, _line) = spawn_serve_sync(bin, src_root.to_str().unwrap(), &addr_s);

    // First batch sync.
    let (code1, stdout1, stderr1) = run_cmd(
        bin,
        &[
            "sync-reduce-batch",
            "--root",
            dst_root.to_str().unwrap(),
            "--addr",
            &addr_s,
            "--reduce-manifests",
            list_path.to_str().unwrap(),
        ],
    );
    assert_eq!(code1, 0, "stderr={}", String::from_utf8_lossy(&stderr1));
    let out1 = String::from_utf8_lossy(&stdout1);
    let first = out1.lines().next().unwrap_or("");
    let needed1 = parse_u32_kv(first, "needed_total=").unwrap();
    let already1 = parse_u32_kv(first, "already_present=").unwrap();
    let fetched1 = parse_u32_kv(first, "fetched=").unwrap();
    let manifests1 = parse_u32_kv(first, "manifests=").unwrap();
    assert_eq!(manifests1, 2);
    assert!(needed1 > 0);
    assert_eq!(already1, 0);
    assert_eq!(fetched1, needed1);

    // Ensure per-manifest needed_total lines exist and match the global union.
    let mut per_needed: Vec<u32> = Vec::new();
    for line in out1.lines().skip(1) {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with("manifest=") {
            if let Some(v) = parse_u32_kv(line, "needed_total=") {
                per_needed.push(v);
            }
        }
    }
    assert_eq!(per_needed.len(), 2);
    assert_eq!(per_needed[0], per_needed[1]);
    // The two closures are identical except for the reduce manifest hash.
    // The global union therefore includes one extra unique artifact.
    assert_eq!(needed1, per_needed[0] + 1);

    // Second batch sync: everything should be already present.
    let (code2, stdout2, stderr2) = run_cmd(
        bin,
        &[
            "sync-reduce-batch",
            "--root",
            dst_root.to_str().unwrap(),
            "--addr",
            &addr_s,
            "--reduce-manifests",
            list_path.to_str().unwrap(),
        ],
    );
    assert_eq!(code2, 0, "stderr={}", String::from_utf8_lossy(&stderr2));
    let out2 = String::from_utf8_lossy(&stdout2);
    let first2 = out2.lines().next().unwrap_or("");
    let needed2 = parse_u32_kv(first2, "needed_total=").unwrap();
    let already2 = parse_u32_kv(first2, "already_present=").unwrap();
    let fetched2 = parse_u32_kv(first2, "fetched=").unwrap();
    assert_eq!(needed2, needed1);
    assert_eq!(fetched2, 0);
    assert_eq!(already2, needed1);

    assert!(!any_tmp_files_under(&dst_root));

    let _ = server.kill();
    let _ = server.wait();
}
