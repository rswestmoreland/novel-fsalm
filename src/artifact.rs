// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Artifact storage (content-addressed) and artifact helpers.
//!
//! goals:
//! - Provide a minimal content-addressed artifact store for canonical byte artifacts.
//! - Use stable hashes (BLAKE3) as addresses.
//! - Prefer deterministic file layout and stable behavior.
//!
//! Notes on determinism:
//! - Determinism is defined on artifact BYTES and their HASHES, not on filesystem metadata.
//! - File creation time and OS-specific metadata are not part of the artifact identity.
//!
//! Notes on safety and performance:
//! - Avoid unsafe code.
//! - Prefer predictable path mapping and minimal allocations.
//! - Use best-effort atomic writes using temp files and rename.

use crate::hash::{blake3_hash, hex32, Hash32};
use core::fmt;
use std::fs;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};

/// An artifact store error.
#[derive(Debug)]
pub struct ArtifactError {
    msg: &'static str,
    io: Option<io::Error>,
}

impl ArtifactError {
    fn new(msg: &'static str) -> Self {
        Self { msg, io: None }
    }

    fn with_io(msg: &'static str, io: io::Error) -> Self {
        Self { msg, io: Some(io) }
    }
}

impl fmt::Display for ArtifactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(e) = &self.io {
            write!(f, "{}: {}", self.msg, e)
        } else {
            f.write_str(self.msg)
        }
    }
}

impl From<io::Error> for ArtifactError {
    fn from(e: io::Error) -> Self {
        ArtifactError::with_io("io error", e)
    }
}

/// Result for artifact operations.
pub type ArtifactResult<T> = Result<T, ArtifactError>;

/// Content-addressed artifact store interface.
pub trait ArtifactStore {
    /// Store bytes as an artifact and return its content hash.
    ///
    /// Behavior:
    /// - The returned hash is the BLAKE3 hash of the input bytes.
    /// - If the artifact already exists, the store must not rewrite it.
    fn put(&self, bytes: &[u8]) -> ArtifactResult<Hash32>;

    /// Load bytes for an artifact hash.
    ///
    /// Returns None if not found.
    fn get(&self, hash: &Hash32) -> ArtifactResult<Option<Vec<u8>>>;

    /// Return the filesystem path for an artifact hash, if applicable.
    fn path_for(&self, hash: &Hash32) -> PathBuf;
}

/// Filesystem artifact store.
/// Layout:
/// root/aa/bb/<hex>.bin
/// where aa is the first byte in hex, bb is the second byte in hex.
#[derive(Debug, Clone)]
pub struct FsArtifactStore {
    root: PathBuf,
}

impl FsArtifactStore {
    /// Create a filesystem artifact store rooted at `root`.
    pub fn new(root: impl AsRef<Path>) -> ArtifactResult<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(|e| ArtifactError::with_io("create_dir_all failed", e))?;
        Ok(Self { root })
    }

    /// Root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn shard_dirs(&self, hash: &Hash32) -> (String, String) {
        let a = format!("{:02x}", hash[0]);
        let b = format!("{:02x}", hash[1]);
        (a, b)
    }

    fn final_path(&self, hash: &Hash32) -> PathBuf {
        let (a, b) = self.shard_dirs(hash);
        let hex = hex32(hash);
        self.root.join(a).join(b).join(format!("{}.bin", hex))
    }

    fn tmp_paths(&self, final_path: &Path, hash: &Hash32) -> [PathBuf; 4] {
        // Use short temp file names to reduce the chance of hitting Windows MAX_PATH
        // when the store root is nested under a long working directory.
        //
        // Temp files are created in the same directory as the final artifact so
        // rename stays on the same filesystem.
        let start = (hash[31] & 0x03) as usize;
        let mut out = [PathBuf::new(), PathBuf::new(), PathBuf::new(), PathBuf::new()];

        let parent = final_path.parent().unwrap_or_else(|| Path::new("."));
        let hex = hex32(hash);
        let prefix = &hex[0..16];
        let pid = std::process::id();
        for i in 0..4 {
            let s = (start + i) & 0x03;
            // Example: _tmp1_12345_0123456789abcdef.bin
            out[i] = parent.join(format!("_tmp{}_{}_{}.bin", s, pid, prefix));
        }
        out
    }

    fn write_atomic(&self, final_path: &Path, bytes: &[u8], hash: &Hash32) -> ArtifactResult<()> {
        if let Some(p) = final_path.parent() {
            fs::create_dir_all(p).map_err(|e| ArtifactError::with_io("create_dir_all failed", e))?;
        }

        if final_path.exists() {
            return Ok(());
        }

        let tmp_candidates = self.tmp_paths(final_path, hash);
        let mut last_err: Option<io::Error> = None;

        for tmp_path in tmp_candidates.iter() {
            match fs::OpenOptions::new().write(true).create_new(true).open(tmp_path) {
                Ok(mut f) => {
                    if let Err(e) = f.write_all(bytes) {
                        let _ = fs::remove_file(tmp_path);
                        return Err(ArtifactError::with_io("write_all failed", e));
                    }
                    if let Err(e) = f.flush() {
                        let _ = fs::remove_file(tmp_path);
                        return Err(ArtifactError::with_io("flush failed", e));
                    }
                    drop(f);

                    match fs::rename(tmp_path, final_path) {
                        Ok(()) => return Ok(()),
                        Err(e) => {
                            if final_path.exists() {
                                let _ = fs::remove_file(tmp_path);
                                return Ok(());
                            }
                            let _ = fs::remove_file(tmp_path);
                            return Err(ArtifactError::with_io("rename failed", e));
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
            Err(ArtifactError::with_io("failed to create temp file", e))
        } else {
            Err(ArtifactError::new("failed to create temp file"))
        }
    }
}

impl ArtifactStore for FsArtifactStore {
    fn put(&self, bytes: &[u8]) -> ArtifactResult<Hash32> {
        let hash = blake3_hash(bytes);
        let final_path = self.final_path(&hash);
        self.write_atomic(&final_path, bytes, &hash)?;
        Ok(hash)
    }

    fn get(&self, hash: &Hash32) -> ArtifactResult<Option<Vec<u8>>> {
        let p = self.final_path(hash);
        if !p.exists() {
            return Ok(None);
        }
        let b = fs::read(&p).map_err(|e| ArtifactError::with_io("read failed", e))?;
        Ok(Some(b))
    }

    fn path_for(&self, hash: &Hash32) -> PathBuf {
        self.final_path(hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::blake3_hash;

    fn tmp_dir(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push("fsa_lm_tests");
        p.push(name);
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn fs_store_put_get_round_trip() {
        let dir = tmp_dir("fs_store_put_get_round_trip");
        let store = FsArtifactStore::new(&dir).unwrap();

        let bytes = b"hello artifact";
        let h = store.put(bytes).unwrap();
        assert_eq!(h, blake3_hash(bytes));

        let got = store.get(&h).unwrap().unwrap();
        assert_eq!(got, bytes);
    }

    #[test]
    fn fs_store_put_is_idempotent() {
        let dir = tmp_dir("fs_store_put_is_idempotent");
        let store = FsArtifactStore::new(&dir).unwrap();

        let bytes = b"same bytes";
        let h1 = store.put(bytes).unwrap();
        let h2 = store.put(bytes).unwrap();
        assert_eq!(h1, h2);

        let p = store.path_for(&h1);
        assert!(p.exists());
        let got = store.get(&h1).unwrap().unwrap();
        assert_eq!(got, bytes);
    }

    #[test]
    fn fs_store_layout_contains_hash_hex() {
        let dir = tmp_dir("fs_store_layout_contains_hash_hex");
        let store = FsArtifactStore::new(&dir).unwrap();

        let bytes = b"layout check";
        let h = store.put(bytes).unwrap();
        let p = store.path_for(&h);

        let hex = hex32(&h);
        assert!(p.to_string_lossy().contains(&hex));
        assert!(p.extension().unwrap().to_string_lossy().starts_with("bin"));
    }
}
