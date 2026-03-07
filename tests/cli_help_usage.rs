// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

use std::process::Command;

fn norm(s: &[u8]) -> String {
    String::from_utf8_lossy(s).replace("\r\n", "\n")
}

#[test]
fn ask_help_mentions_text_flag_and_uses_stdout() {
    let bin = env!("CARGO_BIN_EXE_fsa_lm");

    let out = Command::new(bin).args(["ask", "--help"]).output().unwrap();
    assert_eq!(out.status.code().unwrap_or(-1), 0, "stderr={}", norm(&out.stderr));

    let stdout_s = norm(&out.stdout);
    let stderr_s = norm(&out.stderr);

    assert!(stdout_s.contains("ask "), "stdout={}", stdout_s);
    assert!(stdout_s.contains("--text <text>"), "stdout={}", stdout_s);

    assert!(stderr_s.trim().is_empty(), "expected empty stderr; stderr={}", stderr_s);
}
