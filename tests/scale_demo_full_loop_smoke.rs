// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use std::process::Command;

#[test]
fn scale_demo_full_loop_flags_run() {
    let mut root = std::env::temp_dir();
    root.push("novel_fsalm_test_scale_demo_full_loop_flags_run");
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
            "12",
            "--queries",
            "4",
            "--min_doc_tokens",
            "3",
            "--max_doc_tokens",
            "6",
            "--vocab",
            "32",
            "--query_tokens",
            "3",
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

    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));

    let stdout = String::from_utf8_lossy(&out.stdout);
    let lines: Vec<&str> = stdout.lines().filter(|l| !l.trim().is_empty()).collect();

    assert_eq!(lines.len(), 7, "stdout: {}", stdout);

    assert!(lines[0].starts_with("scale_demo_v1 "), "line0: {}", lines[0]);
    assert!(lines[1].starts_with("scale_demo_frames_v1 "), "line1: {}", lines[1]);
    assert!(lines[2].starts_with("scale_demo_index_v1 "), "line2: {}", lines[2]);
    assert!(lines[3].starts_with("scale_demo_prompts_v1 "), "line3: {}", lines[3]);
    assert!(lines[4].starts_with("scale_demo_evidence_v1 "), "line4: {}", lines[4]);
    assert!(lines[5].starts_with("scale_demo_answers_v3 "), "line5: {}", lines[5]);
    assert!(lines[6].starts_with("scale_demo_scale_report_v3 "), "line6: {}", lines[6]);
}
