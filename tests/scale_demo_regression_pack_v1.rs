// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use std::process::Command;

fn find_kv<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    for part in line.split_whitespace() {
        if let Some(rest) = part.strip_prefix(key) {
            return Some(rest);
        }
    }
    None
}

fn run_scale_demo_pack(root_name: &str) -> String {
    let mut root = std::env::temp_dir();
    root.push(root_name);
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    let root_s = root.to_str().unwrap();

    let bin = env!("CARGO_BIN_EXE_fsa_lm");
    let out = Command::new(bin)
        .args([
            "scale-demo",
            "--root",
            root_s,
            "--seed",
            "7",
            "--docs",
            "64",
            "--queries",
            "16",
            "--min_doc_tokens",
            "12",
            "--max_doc_tokens",
            "24",
            "--vocab",
            "512",
            "--query_tokens",
            "6",
            "--tie_pair",
            "1",
            "--ingest",
            "1",
            "--build_index",
            "1",
            "--prompts",
            "1",
            "--evidence",
            "1",
            "--answer",
            "1",
        ])
        .output()
        .unwrap();

    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let stdout = String::from_utf8_lossy(&out.stdout);
    let lines: Vec<&str> = stdout.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(lines.len(), 7, "stdout: {}", stdout);

    let last = lines[6];
    assert!(
        last.starts_with("scale_demo_scale_report_v3 "),
        "line6: {}",
        last
    );

    // Ensure the list summary fields are present when prompts/evidence/answer are enabled.
    assert!(last.contains(" prompts_list_hash="), "line6: {}", last);
    assert!(last.contains(" evidence_list_hash="), "line6: {}", last);
    assert!(last.contains(" answers_list_hash="), "line6: {}", last);
    assert!(
        last.contains(" planner_hints_list_hash="),
        "line6: {}",
        last
    );
    assert!(last.contains(" forecasts_list_hash="), "line6: {}", last);

    last.to_string()
}

#[test]
fn scale_demo_regression_pack_v1_deterministic_and_optional_lock() {
    let a = run_scale_demo_pack("novel_fsalm_test_scale_demo_regression_pack_v1_a");
    let b = run_scale_demo_pack("novel_fsalm_test_scale_demo_regression_pack_v1_b");

    let ra = find_kv(&a, "report=").unwrap();
    let rb = find_kv(&b, "report=").unwrap();

    assert_eq!(ra, rb, "report mismatch\nA: {}\nB: {}", a, b);

    // Avoid comparing against older expected hashes when the line prefix indicates a newer
    // schema version.
    if a.starts_with("scale_demo_scale_report_v3 ") {
        if let Ok(expected) = std::env::var("FSA_LM_REGRESSION_SCALE_DEMO_PACK_V3_REPORT_HEX") {
            let expected = expected.trim();
            assert_eq!(ra, expected, "expected report mismatch\nline: {}", a);
        }
        return;
    }

    if a.starts_with("scale_demo_scale_report_v2 ") {
        if let Ok(expected) = std::env::var("FSA_LM_REGRESSION_SCALE_DEMO_PACK_V2_REPORT_HEX") {
            let expected = expected.trim();
            assert_eq!(ra, expected, "expected report mismatch\nline: {}", a);
        }
        return;
    }

    if let Ok(expected) = std::env::var("FSA_LM_REGRESSION_SCALE_DEMO_PACK_V1_REPORT_HEX") {
        let expected = expected.trim();
        assert_eq!(ra, expected, "expected report mismatch\nline: {}", a);
    }
}
