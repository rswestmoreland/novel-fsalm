// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use fsa_lm::hash::{blake3_hash, hex32};

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

fn find_kv<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    for part in line.split_whitespace() {
        if let Some(rest) = part.strip_prefix(key) {
            return Some(rest);
        }
    }
    None
}

fn tmp_dir(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push("fsa_lm_tests");
    p.push(format!("{}_{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn write_fixture_tsv(path: &Path) {
    // Deterministic small TSV dump: title<TAB>text
    // Keep rows short so segment sizing is not a factor.
    let mut out = String::new();
    for i in 0..16u32 {
        out.push_str(&format!(
            "Doc{:02}\tbanana apple carrot {}\n",
            i,
            if (i % 3) == 0 { "banana" } else { "apple" }
        ));
    }
    std::fs::write(path, out.as_bytes()).unwrap();
}

fn pick_free_local_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
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
    // Read at least one line so we have useful diagnostics if the process exits.
    let n = rdr.read_line(&mut first_line).unwrap();
    if n == 0 {
        // Process exited before emitting a line.
        let _ = child.wait();
        panic!("serve-sync exited early (no stderr output)");
    }

    // Drain remaining stderr in a background thread so the server cannot block
    // on a full stderr pipe during the short-lived test run.
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

fn run_cmd(bin: &str, args: &[&str]) -> (i32, Vec<u8>, Vec<u8>) {
    let out = Command::new(bin).args(args).output().unwrap();
    (
        out.status.code().unwrap_or(-1),
        out.stdout,
        out.stderr,
    )
}

fn parse_kv_lines(stdout: &str) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut it = line.splitn(2, '=');
        let k = it.next().unwrap().trim();
        let v = it.next().unwrap_or("").trim();
        m.insert(k.to_string(), v.to_string());
    }
    m
}

fn normalize_text_bytes(bytes: &[u8]) -> Vec<u8> {
    // Normalize line endings so the output hash is stable across platforms.
    let s = String::from_utf8_lossy(bytes);
    let s = s.replace("\r\n", "\n");
    let s = s.trim_end().to_string();
    s.into_bytes()
}

fn run_operator_workflow_pack(root_name: &str) -> String {
    let base = tmp_dir(root_name);
    let src_root = base.join("src");
    let dst_root = base.join("dst");
    std::fs::create_dir_all(&src_root).unwrap();
    std::fs::create_dir_all(&dst_root).unwrap();

    let dump_path = base.join("dump.tsv");
    write_fixture_tsv(&dump_path);

    let bin = env!("CARGO_BIN_EXE_fsa_lm");

    // 1) run-phase6 on the source root.
    let (code, out, err) = run_cmd(
        bin,
        &[
            "run-phase6",
            "--root",
            src_root.to_str().unwrap(),
            "--dump",
            dump_path.to_str().unwrap(),
            "--shards",
            "2",
            "--seg_mb",
            "1",
            "--row_kb",
            "4",
            "--chunk_rows",
            "32",
            "--max_docs",
            "16",
        ],
    );
    assert_eq!(
        code,
        0,
        "run-phase6 failed: stderr={}",
        String::from_utf8_lossy(&err)
    );
    let out_s = String::from_utf8_lossy(&out).replace("\r\n", "\n");
    let kv = parse_kv_lines(&out_s);

    let reduce_manifest = kv
        .get("reduce_manifest")
        .expect("reduce_manifest")
        .to_string();
    let merged_snapshot = kv
        .get("merged_snapshot")
        .expect("merged_snapshot")
        .to_string();
    let merged_sig_map = kv
        .get("merged_sig_map")
        .expect("merged_sig_map")
        .to_string();

    // 2) serve-sync from the source root.
    let port = pick_free_local_port();
    let addr = format!("127.0.0.1:{}", port);
    let (mut server, first_line) = spawn_serve_sync(bin, src_root.to_str().unwrap(), &addr);

    // Ensure the expected startup line is present (helps diagnose bind failures).
    assert!(
        first_line.contains("listening on"),
        "serve-sync stderr: {}",
        first_line
    );

    // 3) sync-reduce into the fresh destination root.
    // Retry a few times in case the server prints before it has fully bound.
    let mut sync_stdout: Vec<u8> = Vec::new();
    let mut sync_stderr: Vec<u8> = Vec::new();
    let mut sync_code: i32 = -1;
    for _try in 0..20u32 {
        let (c, o, e) = run_cmd(
            bin,
            &[
                "sync-reduce",
                "--root",
                dst_root.to_str().unwrap(),
                "--addr",
                &addr,
                "--reduce-manifest",
                &reduce_manifest,
            ],
        );
        sync_code = c;
        sync_stdout = o;
        sync_stderr = e;
        if sync_code == 0 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    // Shut down server regardless of sync outcome.
    let _ = server.kill();
    let _ = server.wait();

    assert_eq!(
        sync_code,
        0,
        "sync-reduce failed: stderr={}",
        String::from_utf8_lossy(&sync_stderr)
    );

    let sync_line = String::from_utf8_lossy(&sync_stdout)
        .replace("\r\n", "\n")
        .trim()
        .to_string();
    assert!(
        sync_line.contains("needed_total=") && sync_line.contains("bytes_fetched="),
        "sync stdout: {}",
        sync_line
    );

    // 4) Create a deterministic prompt on the destination root.
    let (pcode, pout, perr) = run_cmd(
        bin,
        &[
            "prompt",
            "--root",
            dst_root.to_str().unwrap(),
            "--seed",
            "7",
            "--max_tokens",
            "128",
            "banana",
        ],
    );
    assert_eq!(pcode, 0, "prompt failed: {}", String::from_utf8_lossy(&perr));
    let prompt_hash = String::from_utf8_lossy(&pout).trim().to_string();
    assert_eq!(prompt_hash.len(), 64, "prompt hash: {}", prompt_hash);

    // 5) query-index on the replicated root (hash stdout instead of locking raw hits).
    let (qcode, qout, qerr) = run_cmd(
        bin,
        &[
            "query-index",
            "--root",
            dst_root.to_str().unwrap(),
            "--snapshot",
            &merged_snapshot,
            "--sig-map",
            &merged_sig_map,
            "--text",
            "banana",
            "--k",
            "8",
        ],
    );
    assert_eq!(
        qcode,
        0,
        "query-index failed: stderr={}",
        String::from_utf8_lossy(&qerr)
    );
    let qnorm = normalize_text_bytes(&qout);
    assert!(!qnorm.is_empty(), "query-index returned empty stdout");
    let query_out_hash = blake3_hash(&qnorm);
    let query_out_lines = qnorm.split(|b| *b == b'\n').filter(|l| !l.is_empty()).count();

    // 6) answer on the replicated root (hash stdout instead of locking raw text).
    let (acode, aout, aerr) = run_cmd(
        bin,
        &[
            "answer",
            "--root",
            dst_root.to_str().unwrap(),
            "--prompt",
            &prompt_hash,
            "--snapshot",
            &merged_snapshot,
            "--sig-map",
            &merged_sig_map,
            "--k",
            "8",
            "--plan_items",
            "8",
            "--max_terms",
            "16",
            "--no_ties",
            "--verify-trace",
            "1",
        ],
    );
    assert_eq!(acode, 0, "answer failed: {}", String::from_utf8_lossy(&aerr));
    let anorm = normalize_text_bytes(&aout);
    assert!(!anorm.is_empty(), "answer returned empty stdout");
    let answer_out_hash = blake3_hash(&anorm);
    let answer_out_bytes = anorm.len();

    // Build a deterministic report blob with stable fields only.
    let mut blob: Vec<u8> = Vec::new();
    blob.extend_from_slice(b"operator_workflow_pack_v1\0");
    for (k, v) in kv.iter() {
        blob.extend_from_slice(k.as_bytes());
        blob.push(b'=');
        blob.extend_from_slice(v.as_bytes());
        blob.push(0);
    }
    blob.extend_from_slice(sync_line.as_bytes());
    blob.push(0);
    blob.extend_from_slice(prompt_hash.as_bytes());
    blob.push(0);
    blob.extend_from_slice(&query_out_hash);
    blob.extend_from_slice(&(query_out_lines as u32).to_le_bytes());
    blob.extend_from_slice(&answer_out_hash);
    blob.extend_from_slice(&(answer_out_bytes as u32).to_le_bytes());

    let report_hash = blake3_hash(&blob);
    let report_hex = hex32(&report_hash);

    format!(
        "operator_workflow_pack_v1 report={} merged_snapshot={} merged_sig_map={} reduce_manifest={} sync={} prompt={} query_out_hash={} query_out_lines={} answer_out_hash={} answer_out_bytes={}",
        report_hex,
        merged_snapshot,
        merged_sig_map,
        reduce_manifest,
        sync_line,
        prompt_hash,
        hex32(&query_out_hash),
        query_out_lines,
        hex32(&answer_out_hash),
        answer_out_bytes
    )
}

#[test]
fn operator_workflow_golden_pack_v1_deterministic_and_optional_lock() {
    let a = run_operator_workflow_pack("novel_fsalm_test_operator_workflow_pack_v1_a");
    let b = run_operator_workflow_pack("novel_fsalm_test_operator_workflow_pack_v1_b");

    assert!(a.starts_with("operator_workflow_pack_v1 "), "line: {}", a);
    assert!(b.starts_with("operator_workflow_pack_v1 "), "line: {}", b);

    let ra = find_kv(&a, "report=").unwrap();
    let rb = find_kv(&b, "report=").unwrap();
    assert_eq!(ra, rb, "report mismatch\nA: {}\nB: {}", a, b);

    if let Ok(expected) = std::env::var("FSA_LM_REGRESSION_OPERATOR_WORKFLOW_PACK_V1_REPORT_HEX") {
        let expected = expected.trim();
        assert_eq!(ra, expected, "expected report mismatch\nline: {}", a);
    }
}
