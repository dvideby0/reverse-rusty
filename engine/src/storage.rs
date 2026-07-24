//! Persistence + on-disk formats. The concrete codecs live in submodules; this
//! root holds the shared low-level binary primitives (CRC-32, atomic rename, and
//! little-endian scalar read/write) and re-exports the public API so callers keep
//! using `crate::storage::{…}` unchanged.
//!
//! - [`segment`] — the `.seg` segment file format (`write_segment` + the mmap-backed
//!   `MmapSegment` read view, ADR-012)
//! - [`dict`] — feature-dictionary (de)serialization (stored inside the manifests)
//! - [`manifest`] — the engine `Manifest` + the coordinator `ClusterManifest`
//! - [`sources`] — the per-query source-text store (`SourceStore`, ADR-020 Item 1)
//! - [`backup`] — manifest-driven atomic directory snapshot (ADR-079); restore is
//!   the existing `Engine::open` / `ClusterEngine::open`
//!
//! All multi-byte values are little-endian; integrity is a trailing CRC-32 plus
//! write-to-tmp + atomic rename (`durable_rename`).

use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

mod backup;
mod dict;
mod manifest;
mod segment;
mod sources;
mod tagdict;

pub use backup::{
    copy_cluster_dir, copy_engine_dir, verify_backup, verify_cluster_backup, BackupError,
};
pub use dict::{deserialize_dict, serialize_dict};
pub use manifest::{
    read_cluster_manifest, read_manifest, write_cluster_manifest, write_manifest, ClusterManifest,
    Manifest,
};
pub use segment::{write_segment, MmapSegment};
pub use sources::{load_query_sources, LazyBase, SourceStore, StoredSource};
pub use tagdict::{deserialize_tagdict, serialize_tagdict};

// ---- shared low-level binary primitives (used by the codec submodules) ----

/// Simple CRC-32 using the standard polynomial. Used for WAL entry integrity;
/// segment files use atomic rename (write-to-tmp + rename) for integrity.
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        crc ^= u32::from(b);
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB8_8320;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

/// Atomic rename with parent-directory fsync for crash durability.
fn durable_rename(from: &Path, to: &Path) -> io::Result<()> {
    std::fs::rename(from, to)?;
    if let Some(parent) = to.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn write_u32(w: &mut impl Write, v: u32) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

fn write_u64(w: &mut impl Write, v: u64) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

fn read_u16_at(data: &[u8], off: usize) -> io::Result<u16> {
    let b: [u8; 2] = data
        .get(off..off + 2)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated u16"))?;
    Ok(u16::from_le_bytes(b))
}

fn read_u32_at(data: &[u8], off: usize) -> io::Result<u32> {
    let b: [u8; 4] = data
        .get(off..off + 4)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated u32"))?;
    Ok(u32::from_le_bytes(b))
}

fn read_u64_at(data: &[u8], off: usize) -> io::Result<u64> {
    let b: [u8; 8] = data
        .get(off..off + 8)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated u64"))?;
    Ok(u64::from_le_bytes(b))
}
