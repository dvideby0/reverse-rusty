//! Segment round-trip, mmap matching, reopen lifecycle, tag-column-on-mmap,
//! in-memory backward-compat, and the v1/v2 logical-index reverse-index paths.

use crate::harness::*;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::Engine;
use reverse_rusty::vocab::Vocab;

fn stamp_compiler_semantics(path: &std::path::Path, version: u32) {
    let mut bytes = std::fs::read(path).expect("read segment");
    bytes[12..16].copy_from_slice(&version.to_le_bytes());
    let body = bytes.len() - 4;
    let crc = reverse_rusty::storage::crc32(&bytes[..body]);
    bytes[body..].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(path, bytes).expect("write compiler-semantics stamp");
}

fn stamp_legacy_compiler_semantics(path: &std::path::Path) {
    stamp_compiler_semantics(path, 0);
}

mod aliases;
mod formats;
mod migration;
mod priority;
mod storage;
mod wal_aliases;
