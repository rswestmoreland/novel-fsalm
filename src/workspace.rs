// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Root-local workspace defaults.
//!
//! The workspace file is a small, human-editable state file stored under the
//! artifact root directory. It records which immutable artifacts (index
//! snapshot, sig map, lexicon snapshot) should be used as defaults by
//! user-facing commands.
//!
//! This module provides:
//! - a parser for workspace_v1.txt (key=value, last-wins)
//! - helpers for reading/writing the file
//!
//! The workspace is not part of artifact hashing.

use crate::hash::{parse_hash32_hex, Hash32};

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// Workspace v1 file name under the artifact root.
pub const WORKSPACE_V1_FILENAME: &str = "workspace_v1.txt";

/// Workspace defaults parsed from workspace_v1.txt.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkspaceV1 {
    /// Default merged IndexSnapshotV1 hash.
    pub merged_snapshot: Option<Hash32>,
    /// Default IndexSigMapV1 hash matching merged_snapshot.
    pub merged_sig_map: Option<Hash32>,
    /// Default LexiconSnapshotV1 hash used for bounded query expansion.
    pub lexicon_snapshot: Option<Hash32>,

    /// Default retrieval top-k when not provided by the caller.
    pub default_k: Option<u32>,
    /// Default query expansion behavior for end-user commands.
    pub default_expand: Option<bool>,
    /// Default metaphone expansion behavior for end-user commands.
    pub default_meta: Option<bool>,
}

impl WorkspaceV1 {
    /// Validate the workspace for basic pair consistency.
    ///
    /// Validation rules:
    /// - merged_snapshot and merged_sig_map must be set together.
    pub fn validate_pair_consistency(&self) -> Result<(), String> {
        let a = self.merged_snapshot.is_some();
        let b = self.merged_sig_map.is_some();
        if a ^ b {
            return Err("merged_snapshot and merged_sig_map must be set together".to_string());
        }
        Ok(())
    }

    /// True if the workspace contains both required answering keys.
    pub fn has_required_answer_keys(&self) -> bool {
        self.merged_snapshot.is_some() && self.merged_sig_map.is_some()
    }
}

/// Return the full path to the workspace v1 file under the given root.
pub fn workspace_v1_path(root: &Path) -> PathBuf {
    root.join(WORKSPACE_V1_FILENAME)
}

/// Parse workspace_v1.txt content into a WorkspaceV1.
///
/// Format:
/// - Blank lines are ignored.
/// - Lines beginning with '#' are ignored.
/// - key=value pairs.
/// - Leading/trailing ASCII whitespace around keys and values is ignored.
/// - Unknown keys are ignored.
/// - If a key appears multiple times, the last value wins.
pub fn parse_workspace_v1_text(text: &str) -> Result<WorkspaceV1, String> {
    let mut ws = WorkspaceV1::default();

    for (idx, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with('#') {
            continue;
        }

        let (k, v) = match line.split_once('=') {
            Some((a, b)) => (a.trim(), b.trim()),
            None => {
                return Err(format!(
                    "workspace_v1: invalid line {} (missing '=')",
                    idx + 1
                ));
            }
        };

        match k {
            "merged_snapshot" => {
                ws.merged_snapshot = Some(parse_hash32_hex(v).map_err(|e| {
                    format!(
                        "workspace_v1: merged_snapshot invalid on line {}: {}",
                        idx + 1,
                        e
                    )
                })?);
            }
            "merged_sig_map" => {
                ws.merged_sig_map = Some(parse_hash32_hex(v).map_err(|e| {
                    format!(
                        "workspace_v1: merged_sig_map invalid on line {}: {}",
                        idx + 1,
                        e
                    )
                })?);
            }
            "lexicon_snapshot" => {
                ws.lexicon_snapshot = Some(parse_hash32_hex(v).map_err(|e| {
                    format!(
                        "workspace_v1: lexicon_snapshot invalid on line {}: {}",
                        idx + 1,
                        e
                    )
                })?);
            }
            "default_k" => {
                ws.default_k = Some(v.parse::<u32>().map_err(|_| {
                    format!("workspace_v1: default_k invalid u32 on line {}", idx + 1)
                })?);
            }
            "default_expand" => {
                ws.default_expand = Some(parse_bool01(v).map_err(|e| {
                    format!(
                        "workspace_v1: default_expand invalid on line {}: {}",
                        idx + 1,
                        e
                    )
                })?);
            }
            "default_meta" => {
                ws.default_meta = Some(parse_bool01(v).map_err(|e| {
                    format!(
                        "workspace_v1: default_meta invalid on line {}: {}",
                        idx + 1,
                        e
                    )
                })?);
            }
            _ => {
                // Unknown keys are ignored.
            }
        }
    }

    Ok(ws)
}

fn parse_bool01(s: &str) -> Result<bool, String> {
    match s {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => Err("expected 0 or 1".to_string()),
    }
}

/// Read and parse workspace_v1.txt from the given root.
///
/// Returns Ok(None) if the file does not exist.
pub fn read_workspace_v1(root: &Path) -> io::Result<Option<WorkspaceV1>> {
    let p = workspace_v1_path(root);
    if !p.exists() {
        return Ok(None);
    }
    let bytes = fs::read(&p)?;
    let s = match std::str::from_utf8(&bytes) {
        Ok(v) => v,
        Err(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "workspace_v1.txt must be valid UTF-8",
            ));
        }
    };
    let ws =
        parse_workspace_v1_text(s).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(Some(ws))
}

/// Write workspace_v1.txt atomically under the given root.
///
/// This uses a best-effort temp file + rename strategy. On Windows, replacing an
/// existing file may require removing the destination before renaming.
pub fn write_workspace_v1_atomic(root: &Path, ws: &WorkspaceV1) -> io::Result<()> {
    let p = workspace_v1_path(root);
    if let Some(parent) = p.parent() {
        fs::create_dir_all(parent)?;
    }

    let text = serialize_workspace_v1(ws);
    atomic_write_text(&p, &text)
}

fn serialize_workspace_v1(ws: &WorkspaceV1) -> String {
    // Canonical order, stable serialization.
    let mut out = String::new();
    out.push_str("# workspace defaults (v1)\n");

    if let Some(h) = ws.merged_snapshot {
        out.push_str(&format!("merged_snapshot={}\n", crate::hash::hex32(&h)));
    }
    if let Some(h) = ws.merged_sig_map {
        out.push_str(&format!("merged_sig_map={}\n", crate::hash::hex32(&h)));
    }
    if let Some(h) = ws.lexicon_snapshot {
        out.push_str(&format!("lexicon_snapshot={}\n", crate::hash::hex32(&h)));
    }

    if let Some(k) = ws.default_k {
        out.push_str(&format!("default_k={}\n", k));
    }
    if let Some(v) = ws.default_expand {
        out.push_str(&format!("default_expand={}\n", if v { 1 } else { 0 }));
    }
    if let Some(v) = ws.default_meta {
        out.push_str(&format!("default_meta={}\n", if v { 1 } else { 0 }));
    }

    out
}

fn atomic_write_text(final_path: &Path, text: &str) -> io::Result<()> {
    use std::io::Write;

    let parent = final_path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;

    let pid = std::process::id();
    let tmp0 = parent.join(format!("{}.{}.tmp", WORKSPACE_V1_FILENAME, pid));
    let tmp1 = parent.join(format!("{}.{}.tmp1", WORKSPACE_V1_FILENAME, pid));
    let candidates = [tmp0, tmp1];

    let mut last_err: Option<io::Error> = None;
    for tmp in candidates.iter() {
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(tmp)
        {
            Ok(mut f) => {
                if let Err(e) = f.write_all(text.as_bytes()) {
                    let _ = fs::remove_file(tmp);
                    return Err(e);
                }
                if let Err(e) = f.flush() {
                    let _ = fs::remove_file(tmp);
                    return Err(e);
                }
                drop(f);

                match fs::rename(tmp, final_path) {
                    Ok(()) => return Ok(()),
                    Err(e) => {
                        // On Windows, rename over an existing file may fail.
                        if final_path.exists() {
                            let _ = fs::remove_file(final_path);
                            match fs::rename(tmp, final_path) {
                                Ok(()) => return Ok(()),
                                Err(e2) => {
                                    let _ = fs::remove_file(tmp);
                                    return Err(e2);
                                }
                            }
                        }
                        let _ = fs::remove_file(tmp);
                        return Err(e);
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
        Err(e)
    } else {
        Err(io::Error::new(
            io::ErrorKind::Other,
            "failed to create temp file",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(ch: char) -> String {
        let mut s = String::new();
        for _ in 0..64 {
            s.push(ch);
        }
        s
    }

    #[test]
    fn parse_ignores_unknown_keys_and_last_wins() {
        let t = format!(
            "# comment\nunknown=abc\nmerged_snapshot={}\nmerged_snapshot={}\nmerged_sig_map={}\n",
            h('1'),
            h('2'),
            h('3')
        );
        let ws = parse_workspace_v1_text(&t).unwrap();
        assert_eq!(
            ws.merged_snapshot.unwrap(),
            parse_hash32_hex(&h('2')).unwrap()
        );
        assert_eq!(
            ws.merged_sig_map.unwrap(),
            parse_hash32_hex(&h('3')).unwrap()
        );
    }

    #[test]
    fn parse_trims_whitespace() {
        let t = format!(
            " merged_snapshot = {} \n merged_sig_map = {} \n default_expand = 1 \n",
            h('a'),
            h('b')
        );
        let ws = parse_workspace_v1_text(&t).unwrap();
        assert_eq!(
            ws.merged_snapshot.unwrap(),
            parse_hash32_hex(&h('a')).unwrap()
        );
        assert_eq!(
            ws.merged_sig_map.unwrap(),
            parse_hash32_hex(&h('b')).unwrap()
        );
        assert_eq!(ws.default_expand, Some(true));
    }

    #[test]
    fn validate_pair_consistency_rejects_half_present() {
        let t = format!("merged_snapshot={}\n", h('1'));
        let ws = parse_workspace_v1_text(&t).unwrap();
        assert!(ws.validate_pair_consistency().is_err());
    }
}
