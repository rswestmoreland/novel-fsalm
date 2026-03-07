// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use fsa_lm::artifact_sync_v1 as p;
use fsa_lm::hash::{blake3_hash, hex32, Hash32};
use fsa_lm::net;

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
    (out.status.code().unwrap_or(-1), out.stdout, out.stderr)
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

fn parse_sync_lexicon_stats_line(stdout: &str) -> (u32, u32, u32, u32) {
    let line = stdout.lines().next().unwrap_or("").trim();
    let needed_total = parse_u32_kv(line, "needed_total=").unwrap();
    let already_present = parse_u32_kv(line, "already_present=").unwrap();
    let fetched = parse_u32_kv(line, "fetched=").unwrap();
    let bytes_fetched = parse_u32_kv(line, "bytes_fetched=").unwrap();
    (needed_total, already_present, fetched, bytes_fetched)
}

fn parse_kv_line(stdout: &str, key: &str) -> Option<String> {
    for line in stdout.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix(key) {
            return Some(rest.trim().to_string());
        }
    }
    None
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

#[test]
fn sync_lexicon_cli_e2e_and_fast_path_are_stable() {
    let bin = env!("CARGO_BIN_EXE_fsa_lm");

    let base = tmp_dir("sync_lexicon_cli_e2e_fast_path");
    let src_root = base.join("src");
    let dst_root = base.join("dst");
    std::fs::create_dir_all(&src_root).unwrap();
    std::fs::create_dir_all(&dst_root).unwrap();

    let fixture = Path::new("examples").join("wiktionary_tiny.xml");
    assert!(fixture.is_file(), "missing fixture {}", fixture.display());

    // Build a lexicon snapshot in the source root.
    let (ic, out, err) = run_cmd(
        bin,
        &[
            "ingest-wiktionary-xml",
            "--root",
            src_root.to_str().unwrap(),
            "--xml",
            fixture.to_str().unwrap(),
            "--segments",
            "2",
            "--max_pages",
            "100",
        ],
    );
    assert_eq!(ic, 0, "stderr={}", String::from_utf8_lossy(&err));
    let o = String::from_utf8_lossy(&out);
    let snap_hex = parse_kv_line(&o, "lexicon_snapshot=").expect("missing lexicon_snapshot line");

    let port = pick_free_local_port();
    let addr_s = format!("127.0.0.1:{}", port);
    let (mut server, _line) = spawn_serve_sync(bin, src_root.to_str().unwrap(), &addr_s);

    // First sync: destination empty.
    let (c1, out1, err1) = run_cmd(
        bin,
        &[
            "sync-lexicon",
            "--root",
            dst_root.to_str().unwrap(),
            "--addr",
            &addr_s,
            "--lexicon-snapshot",
            &snap_hex,
        ],
    );
    assert_eq!(c1, 0, "stderr={}", String::from_utf8_lossy(&err1));
    let s1 = String::from_utf8_lossy(&out1);
    let (needed1, already1, fetched1, bytes1) = parse_sync_lexicon_stats_line(&s1);
    assert!(needed1 > 0);
    assert_eq!(already1, 0);
    assert_eq!(fetched1, needed1);
    assert!(bytes1 > 0);

    // Validate snapshot in destination root.
    let (vc, _vout, verr) = run_cmd(
        bin,
        &[
            "validate-lexicon-snapshot",
            "--root",
            dst_root.to_str().unwrap(),
            "--snapshot",
            &snap_hex,
        ],
    );
    assert_eq!(vc, 0, "stderr={}", String::from_utf8_lossy(&verr));

    // Second sync: everything already present.
    let (c2, out2, err2) = run_cmd(
        bin,
        &[
            "sync-lexicon",
            "--root",
            dst_root.to_str().unwrap(),
            "--addr",
            &addr_s,
            "--lexicon-snapshot",
            &snap_hex,
        ],
    );
    assert_eq!(c2, 0, "stderr={}", String::from_utf8_lossy(&err2));
    let s2 = String::from_utf8_lossy(&out2);
    let (needed2, already2, fetched2, bytes2) = parse_sync_lexicon_stats_line(&s2);
    assert_eq!(needed2, needed1);
    assert_eq!(fetched2, 0);
    assert_eq!(already2, needed1);
    assert_eq!(bytes2, 0);

    assert!(!any_tmp_files_under(&dst_root));

    let _ = server.kill();
    let _ = server.wait();
}

#[test]
fn sync_lexicon_cli_times_out_if_server_stalls_before_ack() {
    let bin = env!("CARGO_BIN_EXE_fsa_lm");
    let dst_root = tmp_dir("sync_lexicon_cli_times_out_dst");

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

    let snap_h = [2u8; 32];
    let snap_hex = hex32(&snap_h);
    let (code, _stdout, stderr) = run_cmd(
        bin,
        &[
            "sync-lexicon",
            "--root",
            dst_root.to_str().unwrap(),
            "--addr",
            &addr_s,
            "--lexicon-snapshot",
            &snap_hex,
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
fn sync_lexicon_cli_reports_disconnected_on_mid_stream_drop() {
    let bin = env!("CARGO_BIN_EXE_fsa_lm");
    let dst_root = tmp_dir("sync_lexicon_cli_mid_stream_drop_dst");

    let expected: Hash32 = blake3_hash(b"expected_lexicon_snapshot_only");
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

            // Expect GET for snapshot hash.
            let get_payload = net::read_frame(&mut stream, p::DEFAULT_MAX_REQ_FRAME_BYTES).unwrap();
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
            "sync-lexicon",
            "--root",
            dst_root.to_str().unwrap(),
            "--addr",
            &addr_s,
            "--lexicon-snapshot",
            &expected_hex,
            "--rw_timeout_ms",
            "200",
        ],
    );

    assert_eq!(code, 1);
    let se = String::from_utf8_lossy(&stderr).to_lowercase();
    let ok = se.contains("disconnected") || se.contains("timeout") || se.contains("io error");
    assert!(ok, "stderr={}", String::from_utf8_lossy(&stderr));

    assert!(!any_tmp_files_under(&dst_root));

    let _ = th.join();
}
