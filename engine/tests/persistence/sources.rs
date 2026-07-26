//! Query-source (`sources.dat`) persistence: bulk-ingest durability + roll-back,
//! lazy (mmap'd) sources, the v1→v2 back-compat migration, and overlay tombstones.

use crate::harness::*;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::segment::Engine;

fn committed_source_path(dir: &std::path::Path) -> std::path::PathBuf {
    let manifest =
        reverse_rusty::storage::read_manifest(&dir.join("manifest.bin")).expect("read manifest");
    dir.join(manifest.source_file_name)
}

fn next_source_temp_path(dir: &std::path::Path) -> std::path::PathBuf {
    let manifest =
        reverse_rusty::storage::read_manifest(&dir.join("manifest.bin")).expect("read manifest");
    let current = manifest
        .source_file_name
        .strip_prefix("sources_g")
        .and_then(|rest| rest.strip_suffix(".dat"))
        .expect("immutable source filename")
        .parse::<u64>()
        .expect("source generation");
    dir.join(format!("sources_g{:020}.dat", current + 1))
        .with_extension("sources.tmp")
}

fn next_segment_temp_path(dir: &std::path::Path) -> std::path::PathBuf {
    let manifest =
        reverse_rusty::storage::read_manifest(&dir.join("manifest.bin")).expect("read manifest");
    dir.join("segments")
        .join(format!("seg_{:06}.seg", manifest.next_seg_id))
        .with_extension("seg.tmp")
}

/// Hand-write a legacy v1 `sources.dat` (unordered records) for back-compat tests.
fn write_v1_sources(path: &std::path::Path, entries: &[(u64, &str)]) {
    let mut buf = Vec::new();
    buf.extend_from_slice(b"SRCS");
    buf.extend_from_slice(&1u32.to_le_bytes()); // version 1
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (id, text) in entries {
        buf.extend_from_slice(&id.to_le_bytes());
        buf.extend_from_slice(&(text.len() as u32).to_le_bytes());
        buf.extend_from_slice(text.as_bytes());
    }
    std::fs::write(path, buf).unwrap();
}

/// Hand-write the original query-only v2 format (no metadata footer).
fn write_v2_sources(path: &std::path::Path, entries: &[(u64, &str)]) {
    let mut entries = entries.to_vec();
    entries.sort_unstable_by_key(|(id, _)| *id);
    let mut buf = Vec::new();
    buf.extend_from_slice(b"SRCS");
    buf.extend_from_slice(&2u32.to_le_bytes());
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes());
    let mut blob = Vec::new();
    for (id, text) in entries {
        buf.extend_from_slice(&id.to_le_bytes());
        buf.extend_from_slice(&(blob.len() as u64).to_le_bytes());
        buf.extend_from_slice(&(text.len() as u32).to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        blob.extend_from_slice(text.as_bytes());
    }
    buf.extend_from_slice(&blob);
    let crc = reverse_rusty::storage::crc32(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    std::fs::write(path, buf).unwrap();
}

mod commit;
mod formats;
mod recovery;
