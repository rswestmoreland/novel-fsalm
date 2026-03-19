// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Richard S. Westmoreland <dev@rswestmore.land>

//! Debug bundle exporter.
//!
//! goal: export a small, shareable bundle that helps diagnose
//! failures without copying the full store.
//!
//! Safety and privacy notes:
//! - By default, this exporter does NOT include raw artifact bytes.
//! - Users may explicitly include specific artifact hashes via `include_hashes`.
//! - Text files in the store root (for example, script outputs) are included
//! with conservative size limits.
//!
//! Determinism notes:
//! - The zip container is written with fixed timestamps (0) and stable entry
//! ordering.
//! - The content of INFO/INDEX files is deterministic for the same store view.

use crate::artifact::{ArtifactStore, FsArtifactStore};
use crate::hash::{hex32, Hash32};
use core::fmt;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const DEBUG_BUNDLE_DIR: &str = "debug_bundle";
const MAX_ROOT_TEXT_FILES: usize = 32;
const MAX_ROOT_TEXT_BYTES: usize = 1 * 1024 * 1024;
const MAX_SAMPLE_HASHES: usize = 256;
const MAX_LARGEST_FILES: usize = 32;

/// Debug bundle export error.
#[derive(Debug)]
pub struct DebugBundleError {
    msg: &'static str,
    io: Option<io::Error>,
}

impl DebugBundleError {
    fn new(msg: &'static str) -> Self {
        Self { msg, io: None }
    }

    fn with_io(msg: &'static str, io: io::Error) -> Self {
        Self { msg, io: Some(io) }
    }
}

impl fmt::Display for DebugBundleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(e) = &self.io {
            write!(f, "{}: {}", self.msg, e)
        } else {
            f.write_str(self.msg)
        }
    }
}

impl From<io::Error> for DebugBundleError {
    fn from(e: io::Error) -> Self {
        DebugBundleError::with_io("io error", e)
    }
}

/// Configuration for exporting a debug bundle.
#[derive(Debug, Clone)]
pub struct DebugBundleCfgV1 {
    /// Store root (FsArtifactStore root).
    pub root: PathBuf,
    /// Output zip path.
    pub out_path: PathBuf,
    /// Optional extra text to include as USAGE.txt.
    pub usage_text: Option<String>,
    /// Explicit artifact hashes to include as raw bytes.
    pub include_hashes: Vec<Hash32>,
}

impl DebugBundleCfgV1 {
    /// Create a new configuration.
    pub fn new(root: impl AsRef<Path>, out_path: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
            out_path: out_path.as_ref().to_path_buf(),
            usage_text: None,
            include_hashes: Vec::new(),
        }
    }
}

/// Export a debug bundle zip archive.
pub fn export_debug_bundle_v1(cfg: &DebugBundleCfgV1) -> Result<(), DebugBundleError> {
    if let Some(p) = cfg.out_path.parent() {
        if !p.as_os_str().is_empty() {
            fs::create_dir_all(p)
                .map_err(|e| DebugBundleError::with_io("create_dir_all failed", e))?;
        }
    }

    let store = FsArtifactStore::new(&cfg.root).map_err(|e| {
        DebugBundleError::with_io(
            "open store failed",
            io::Error::new(io::ErrorKind::Other, e.to_string()),
        )
    })?;

    let mut zip = ZipWriter::new(&cfg.out_path)?;

    let info = build_info_text(cfg);
    zip.add_file(&format!("{}/INFO.txt", DEBUG_BUNDLE_DIR), info.as_bytes())?;

    if let Some(t) = &cfg.usage_text {
        zip.add_file(&format!("{}/USAGE.txt", DEBUG_BUNDLE_DIR), t.as_bytes())?;
    }

    let (root_listing, root_texts) = collect_root_files(&cfg.root)?;
    zip.add_file(
        &format!("{}/ROOT_LISTING.txt", DEBUG_BUNDLE_DIR),
        root_listing.as_bytes(),
    )?;
    for (name, bytes) in root_texts.into_iter() {
        zip.add_file(&format!("{}/root_files/{}", DEBUG_BUNDLE_DIR, name), &bytes)?;
    }

    let artifact_index = build_artifact_index_text(&cfg.root)?;
    zip.add_file(
        &format!("{}/ARTIFACT_INDEX.txt", DEBUG_BUNDLE_DIR),
        artifact_index.as_bytes(),
    )?;

    if !cfg.include_hashes.is_empty() {
        let mut list = String::new();
        for h in cfg.include_hashes.iter() {
            list.push_str(&hex32(h));
            list.push('\n');
        }
        zip.add_file(
            &format!("{}/INCLUDED_HASHES.txt", DEBUG_BUNDLE_DIR),
            list.as_bytes(),
        )?;

        for h in cfg.include_hashes.iter() {
            let b = store.get(h).map_err(|e| {
                DebugBundleError::with_io(
                    "artifact get failed",
                    io::Error::new(io::ErrorKind::Other, e.to_string()),
                )
            })?;
            let b = match b {
                Some(v) => v,
                None => return Err(DebugBundleError::new("include-hash not found")),
            };
            zip.add_file(
                &format!("{}/artifacts/{}.bin", DEBUG_BUNDLE_DIR, hex32(h)),
                &b,
            )?;
        }
    }

    zip.finish()?;
    Ok(())
}

fn build_info_text(cfg: &DebugBundleCfgV1) -> String {
    let mut s = String::new();
    s.push_str("name=");
    s.push_str(env!("CARGO_PKG_NAME"));
    s.push('\n');
    s.push_str("version=");
    s.push_str(env!("CARGO_PKG_VERSION"));
    s.push('\n');
    s.push_str("target_os=");
    s.push_str(std::env::consts::OS);
    s.push('\n');
    s.push_str("target_arch=");
    s.push_str(std::env::consts::ARCH);
    s.push('\n');
    s.push_str("store_root=");
    s.push_str(&cfg.root.to_string_lossy());
    s.push('\n');
    s.push_str("out_path=");
    s.push_str(&cfg.out_path.to_string_lossy());
    s.push('\n');
    s.push_str("include_hashes=");
    s.push_str(&cfg.include_hashes.len().to_string());
    s.push('\n');
    s
}

fn collect_root_files(root: &Path) -> Result<(String, Vec<(String, Vec<u8>)>), DebugBundleError> {
    let mut entries: Vec<(String, bool, u64)> = Vec::new();
    let mut txts: Vec<(String, Vec<u8>)> = Vec::new();

    let rd = fs::read_dir(root).map_err(|e| DebugBundleError::with_io("read_dir failed", e))?;
    for ent in rd {
        let ent = ent.map_err(|e| DebugBundleError::with_io("read_dir entry failed", e))?;
        let p = ent.path();
        let name = ent.file_name().to_string_lossy().to_string();
        let md = match ent.metadata() {
            Ok(m) => m,
            Err(_) => continue,
        };
        let is_dir = md.is_dir();
        let len = if md.is_file() { md.len() } else { 0 };
        entries.push((name.clone(), is_dir, len));

        if !is_dir && name.ends_with(".txt") {
            if txts.len() >= MAX_ROOT_TEXT_FILES {
                continue;
            }
            if len as usize > MAX_ROOT_TEXT_BYTES {
                continue;
            }
            let b = fs::read(&p).map_err(|e| DebugBundleError::with_io("read file failed", e))?;
            txts.push((name, b));
        }
    }

    entries.sort_by(|a, b| a.0.cmp(&b.0));
    let mut listing = String::new();
    for (name, is_dir, len) in entries.into_iter() {
        if is_dir {
            listing.push_str("dir ");
            listing.push_str(&name);
        } else {
            listing.push_str("file ");
            listing.push_str(&name);
            listing.push(' ');
            listing.push_str(&len.to_string());
        }
        listing.push('\n');
    }
    Ok((listing, txts))
}

fn build_artifact_index_text(root: &Path) -> Result<String, DebugBundleError> {
    let mut total_files: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut smallest = SmallestLexN::new(MAX_SAMPLE_HASHES);
    let mut largest = LargestBySizeN::new(MAX_LARGEST_FILES);

    // Traverse exactly two shard levels (aa/bb/*.bin). Ignore other files.
    let rd_a = match fs::read_dir(root) {
        Ok(r) => r,
        Err(e) => return Err(DebugBundleError::with_io("read_dir failed", e)),
    };
    for a in rd_a {
        let a = match a {
            Ok(v) => v,
            Err(_) => continue,
        };
        let p_a = a.path();
        let md_a = match a.metadata() {
            Ok(m) => m,
            Err(_) => continue,
        };
        if !md_a.is_dir() {
            continue;
        }
        let rd_b = match fs::read_dir(&p_a) {
            Ok(r) => r,
            Err(_) => continue,
        };
        for b in rd_b {
            let b = match b {
                Ok(v) => v,
                Err(_) => continue,
            };
            let p_b = b.path();
            let md_b = match b.metadata() {
                Ok(m) => m,
                Err(_) => continue,
            };
            if !md_b.is_dir() {
                continue;
            }
            let rd_f = match fs::read_dir(&p_b) {
                Ok(r) => r,
                Err(_) => continue,
            };
            for f in rd_f {
                let f = match f {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let md = match f.metadata() {
                    Ok(m) => m,
                    Err(_) => continue,
                };
                if !md.is_file() {
                    continue;
                }
                let name = f.file_name().to_string_lossy().to_string();
                if !name.ends_with(".bin") {
                    continue;
                }
                let hex = match name.strip_suffix(".bin") {
                    Some(h) => h,
                    None => continue,
                };
                if hex.len() != 64 {
                    continue;
                }
                total_files += 1;
                total_bytes += md.len();
                smallest.insert(hex);
                largest.insert(hex, md.len());
            }
        }
    }

    let mut s = String::new();
    s.push_str("artifact_files=");
    s.push_str(&total_files.to_string());
    s.push('\n');
    s.push_str("artifact_bytes=");
    s.push_str(&total_bytes.to_string());
    s.push('\n');

    s.push_str("sample_hashes_lex_smallest=\n");
    for h in smallest.into_sorted_vec().into_iter() {
        s.push_str(&h);
        s.push('\n');
    }

    s.push_str("largest_artifacts=\n");
    for (h, sz) in largest.into_sorted_vec().into_iter() {
        s.push_str(&h);
        s.push(' ');
        s.push_str(&sz.to_string());
        s.push('\n');
    }
    Ok(s)
}

struct SmallestLexN {
    cap: usize,
    heap: BinaryHeap<String>,
}

impl SmallestLexN {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            heap: BinaryHeap::new(),
        }
    }

    fn insert(&mut self, hex: &str) {
        if self.cap == 0 {
            return;
        }
        if self.heap.len() < self.cap {
            self.heap.push(hex.to_string());
            return;
        }
        if let Some(top) = self.heap.peek() {
            // heap is a max-heap, keep lexicographically smallest N.
            if hex < top.as_str() {
                let _ = self.heap.pop();
                self.heap.push(hex.to_string());
            }
        }
    }

    fn into_sorted_vec(self) -> Vec<String> {
        let mut v: Vec<String> = self.heap.into_iter().collect();
        v.sort();
        v
    }
}

struct LargestBySizeN {
    cap: usize,
    heap: BinaryHeap<Reverse<(u64, String)>>,
}

impl LargestBySizeN {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            heap: BinaryHeap::new(),
        }
    }

    fn insert(&mut self, hex: &str, size: u64) {
        if self.cap == 0 {
            return;
        }
        let key = Reverse((size, hex.to_string()));
        if self.heap.len() < self.cap {
            self.heap.push(key);
            return;
        }
        if let Some(top) = self.heap.peek() {
            // top is the smallest (because Reverse), keep largest N.
            if key.0 .0 > top.0 .0 {
                let _ = self.heap.pop();
                self.heap.push(key);
            }
        }
    }

    fn into_sorted_vec(self) -> Vec<(String, u64)> {
        let mut v: Vec<(u64, String)> = self.heap.into_iter().map(|r| r.0).collect();
        // sort by size desc then hex asc.
        v.sort_by(|a, b| {
            if a.0 != b.0 {
                b.0.cmp(&a.0)
            } else {
                a.1.cmp(&b.1)
            }
        });
        v.into_iter().map(|(sz, h)| (h, sz)).collect()
    }
}

// Minimal zip writer (store method). No compression, fixed timestamps.

struct ZipWriter {
    f: fs::File,
    entries: Vec<ZipCentralEntry>,
    offset: u64,
}

struct ZipCentralEntry {
    name: String,
    crc32: u32,
    size: u32,
    local_header_offset: u32,
}

impl ZipWriter {
    fn new(path: &Path) -> Result<Self, DebugBundleError> {
        let f = fs::File::create(path)
            .map_err(|e| DebugBundleError::with_io("create zip failed", e))?;
        Ok(Self {
            f,
            entries: Vec::new(),
            offset: 0,
        })
    }

    fn add_file(&mut self, name: &str, bytes: &[u8]) -> Result<(), DebugBundleError> {
        let name = name.replace('\\', "/");
        let name_bytes = name.as_bytes();
        if name_bytes.len() > u16::MAX as usize {
            return Err(DebugBundleError::new("zip entry name too long"));
        }
        if bytes.len() > u32::MAX as usize {
            return Err(DebugBundleError::new("zip entry too large"));
        }

        let crc32 = crc32_ieee(bytes);
        let size = bytes.len() as u32;
        let local_header_offset = self.offset as u32;

        // Local file header.
        self.write_u32(0x04034b50)?;
        self.write_u16(20)?; // version needed
        self.write_u16(0)?; // flags
        self.write_u16(0)?; // compression (store)
        self.write_u16(0)?; // mod time
        self.write_u16(0)?; // mod date
        self.write_u32(crc32)?;
        self.write_u32(size)?;
        self.write_u32(size)?;
        self.write_u16(name_bytes.len() as u16)?;
        self.write_u16(0)?; // extra len
        self.write_all(name_bytes)?;
        self.write_all(bytes)?;

        self.entries.push(ZipCentralEntry {
            name,
            crc32,
            size,
            local_header_offset,
        });
        Ok(())
    }

    fn finish(mut self) -> Result<(), DebugBundleError> {
        if self.entries.len() > u16::MAX as usize {
            return Err(DebugBundleError::new("zip too many entries"));
        }
        let cd_start = self.offset;
        for i in 0..self.entries.len() {
            // Avoid borrowing `self` immutably across writes.
            let (name, crc32, size, local_header_offset) = {
                let e = &self.entries[i];
                (e.name.clone(), e.crc32, e.size, e.local_header_offset)
            };
            let name_bytes = name.as_bytes();
            self.write_u32(0x02014b50)?;
            self.write_u16(20)?; // version made by
            self.write_u16(20)?; // version needed
            self.write_u16(0)?; // flags
            self.write_u16(0)?; // compression
            self.write_u16(0)?; // mod time
            self.write_u16(0)?; // mod date
            self.write_u32(crc32)?;
            self.write_u32(size)?;
            self.write_u32(size)?;
            self.write_u16(name_bytes.len() as u16)?;
            self.write_u16(0)?; // extra
            self.write_u16(0)?; // comment
            self.write_u16(0)?; // disk start
            self.write_u16(0)?; // internal attrs
            self.write_u32(0)?; // external attrs
            self.write_u32(local_header_offset)?;
            self.write_all(name_bytes)?;
        }
        let cd_end = self.offset;
        let cd_size = cd_end - cd_start;

        // End of central directory.
        self.write_u32(0x06054b50)?;
        self.write_u16(0)?; // disk
        self.write_u16(0)?; // cd start disk
        self.write_u16(self.entries.len() as u16)?;
        self.write_u16(self.entries.len() as u16)?;
        self.write_u32(cd_size as u32)?;
        self.write_u32(cd_start as u32)?;
        self.write_u16(0)?; // comment len
        self.f
            .flush()
            .map_err(|e| DebugBundleError::with_io("flush failed", e))?;
        Ok(())
    }

    fn write_all(&mut self, b: &[u8]) -> Result<(), DebugBundleError> {
        self.f
            .write_all(b)
            .map_err(|e| DebugBundleError::with_io("write failed", e))?;
        self.offset += b.len() as u64;
        Ok(())
    }

    fn write_u16(&mut self, v: u16) -> Result<(), DebugBundleError> {
        self.write_all(&v.to_le_bytes())
    }

    fn write_u32(&mut self, v: u32) -> Result<(), DebugBundleError> {
        self.write_all(&v.to_le_bytes())
    }
}

fn crc32_ieee(b: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &x in b.iter() {
        crc ^= x as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
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
    fn export_debug_bundle_basic_zip_contains_expected_names() {
        let root = tmp_dir("debug_bundle_basic");
        let store = FsArtifactStore::new(&root).unwrap();

        let h = store.put(b"hello").unwrap();
        fs::write(root.join("reduce_out.txt"), b"abc\n").unwrap();

        let out = root.join("bundle.zip");
        let mut cfg = DebugBundleCfgV1::new(&root, &out);
        cfg.usage_text = Some("usage".to_string());
        cfg.include_hashes.push(h);
        export_debug_bundle_v1(&cfg).unwrap();

        let z = fs::read(&out).unwrap();
        let hex = hex32(&h);
        assert!(z.windows(4).any(|w| w == [0x50, 0x4b, 0x05, 0x06]));
        let n1 = b"debug_bundle/INFO.txt";
        assert!(z.windows(n1.len()).any(|w| w == n1));
        let n2 = b"debug_bundle/root_files/reduce_out.txt";
        assert!(z.windows(n2.len()).any(|w| w == n2));
        let expect = format!("debug_bundle/artifacts/{}.bin", hex);
        assert!(z.windows(expect.len()).any(|w| w == expect.as_bytes()));

        // Sanity: included artifact bytes match expected hash.
        let want = blake3_hash(b"hello");
        assert_eq!(want, h);
    }
}
