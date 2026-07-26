//! Per-query source persistence (`SourceStore`) — the `logical_id → stored document`
//! store backing `_source`/explain. Resident (all in RAM) or `Lazy` (an mmap'd,
//! binary-searchable v2 file + an in-memory overlay of post-flush mutations).
//! ADR-020 Item 1. Source data never touches the match hot path.

use std::fs::File;
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;

use super::{crc32, durable_rename, read_u32_at, read_u64_at};

// -- Query source store persistence ------------------------------------------

const SOURCES_MAGIC: [u8; 4] = *b"SRCS";
const SOURCES_VERSION_V1: u32 = 1; // legacy: unordered (logical, len, text)*
const SOURCES_VERSION: u32 = 2; // sorted query-text index + optional metadata footer + CRC
const SRC_HEADER: usize = 16; // magic(4) + version(4) + count(4) + reserved(4)
const SRC_IDX_REC: usize = 24; // logical(8) + blob_off(8) + text_len(4) + pad(4)
const META_MAGIC: [u8; 4] = *b"SMET";
const META_VERSION_V1: u32 = 1;
const META_VERSION: u32 = 2;
const META_IDX_REC_V1: usize = 24; // flags(4) + version(4) + blob_off(8) + len(4) + pad(4)
const META_IDX_REC: usize = 32; // flags(4) + version(4) + generation(8) + blob_off(8) + len(4) + pad(4)
const META_FOOTER: usize = 16; // magic(4) + metadata-version(4) + directory-off(8)
const META_HEADER_MARKER: u32 = u32::from_le_bytes(META_MAGIC);
const TAGS_KNOWN: u32 = 1;
const METADATA_KNOWN: u32 = 2;

/// Canonical source material retained for one stored query.
///
/// Query text remains separately addressable in the v2 file so search-hit
/// enrichment can fetch it without decoding tags. `tags_known = false` is used
/// only when reading a source file that predates the metadata footer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredSource {
    query: String,
    version: u32,
    source_generation: u64,
    tags: Vec<(String, String)>,
    tags_known: bool,
    metadata_known: bool,
}

impl StoredSource {
    pub fn new(query: String, version: u32, tags: Vec<(String, String)>) -> Self {
        Self {
            query,
            version,
            source_generation: 0,
            tags,
            tags_known: true,
            metadata_known: true,
        }
    }

    pub(crate) fn with_generation(
        query: String,
        version: u32,
        source_generation: u64,
        tags: Vec<(String, String)>,
        tags_known: bool,
    ) -> Self {
        Self {
            query,
            version,
            source_generation,
            tags,
            tags_known,
            metadata_known: true,
        }
    }

    fn legacy(query: String) -> Self {
        Self {
            query,
            version: 1,
            source_generation: 0,
            tags: Vec::new(),
            tags_known: false,
            metadata_known: false,
        }
    }

    pub fn query(&self) -> &str {
        &self.query
    }

    pub fn version(&self) -> u32 {
        self.version
    }

    pub(crate) fn source_generation(&self) -> u64 {
        self.source_generation
    }

    pub fn tags(&self) -> &[(String, String)] {
        &self.tags
    }

    pub fn tags_known(&self) -> bool {
        self.tags_known
    }

    pub(crate) fn metadata_known(&self) -> bool {
        self.metadata_known
    }

    pub(crate) fn recover_legacy_metadata(
        &mut self,
        version: u32,
        source_generation: u64,
        tags: Option<Vec<(String, String)>>,
    ) {
        debug_assert!(
            !self.metadata_known,
            "only footer-less source records may inherit exact-store metadata"
        );
        self.version = version;
        self.source_generation = source_generation;
        self.metadata_known = true;
        if let Some(tags) = tags {
            self.tags = tags;
            self.tags_known = true;
        }
    }

    pub(crate) fn recover_missing_tags(&mut self, tags: Option<Vec<(String, String)>>) {
        debug_assert!(
            !self.tags_known,
            "only incomplete tag metadata is recovered"
        );
        if let Some(tags) = tags {
            self.tags = tags;
            self.tags_known = true;
        }
    }
}

#[inline]
fn rw_read<T>(l: &std::sync::RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    l.read().unwrap_or_else(std::sync::PoisonError::into_inner)
}
#[inline]
fn rw_write<T>(l: &std::sync::RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    l.write().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn bad_sources() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "corrupt sources file")
}

/// Per-query source text store (`logical_id → original query text`) for
/// `_source`/explain. Source text never touches the match hot path. `Resident`
/// keeps everything in RAM (the historical default, `retain_source = true`);
/// `Lazy` keeps only an in-memory overlay of post-flush mutations over an mmap'd,
/// binary-searchable v2 file, so it fetches text on demand instead of holding the
/// whole corpus resident (the production-scale memory win — ADR-020 Item 1).
pub enum SourceStore {
    Resident(std::sync::RwLock<crate::util::FastMap<u64, StoredSource>>),
    Lazy {
        base: Option<LazyBase>,
        overlay: std::sync::RwLock<crate::util::FastMap<u64, Option<StoredSource>>>,
    },
}

/// An mmap'd v2 `sources.dat`: the original sorted query index/blob plus an
/// optional backward-readable metadata footer. Naturally
/// `Send`+`Sync` — the only shared state is the read-only `Arc<Mmap>`, accessed
/// via safe `&[u8]` slicing (no raw pointers, unlike `MmapSegment`).
pub struct LazyBase {
    mmap: Arc<memmap2::Mmap>,
    index_off: usize,
    count: usize,
    blob_off: usize,
    metadata: Option<MetadataLayout>,
}

struct SourceRecord<'a> {
    logical: u64,
    query: &'a str,
    version: u32,
    source_generation: u64,
    tags_known: bool,
    metadata_known: bool,
    encoded_tags: Option<&'a [u8]>,
}

#[derive(Clone, Copy)]
struct MetadataLayout {
    version: u32,
    record_size: usize,
    directory_off: usize,
    blob_off: usize,
}

enum TagsRef<'a> {
    Decoded(&'a [(String, String)]),
    Encoded(&'a [u8]),
}

struct SourceEntryRef<'a> {
    logical: u64,
    query: &'a str,
    version: u32,
    source_generation: u64,
    tags_known: bool,
    metadata_known: bool,
    tags: TagsRef<'a>,
}

mod format;
mod lazy;
mod store;

#[cfg(test)]
use format::encode_tags;
pub use format::load_query_sources;
use format::{
    decode_tags, load_stored_sources, open_lazy_base, peek_sources_version, write_sources_v2,
};

#[cfg(test)]
mod tests;
