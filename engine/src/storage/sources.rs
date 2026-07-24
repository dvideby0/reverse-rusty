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

impl LazyBase {
    #[inline]
    fn logical_at(&self, i: usize) -> Option<u64> {
        read_u64_at(&self.mmap, self.index_off + i * SRC_IDX_REC).ok()
    }

    fn query_record(&self, i: usize) -> Option<(u64, &str)> {
        let data: &[u8] = &self.mmap;
        let rec = self.index_off + i * SRC_IDX_REC;
        let logical = read_u64_at(data, rec).ok()?;
        let query_off = read_u64_at(data, rec + 8).ok()? as usize;
        let query_len = read_u32_at(data, rec + 16).ok()? as usize;

        let query_start = self.blob_off.checked_add(query_off)?;
        let query_end = query_start.checked_add(query_len)?;
        let query = std::str::from_utf8(data.get(query_start..query_end)?).ok()?;
        Some((logical, query))
    }

    fn record(&self, i: usize) -> Option<SourceRecord<'_>> {
        let data: &[u8] = &self.mmap;
        let (logical, query) = self.query_record(i)?;
        let (version, source_generation, tags_known, metadata_known, encoded_tags) =
            match self.metadata {
                Some(metadata) => {
                    let metadata_rec = metadata.directory_off + i * metadata.record_size;
                    let flags = read_u32_at(data, metadata_rec).ok()?;
                    let version = read_u32_at(data, metadata_rec + 4).ok()?;
                    let (source_generation, tags_off, tags_len, metadata_known) =
                        if metadata.version == META_VERSION_V1 {
                            (
                                0,
                                read_u64_at(data, metadata_rec + 8).ok()? as usize,
                                read_u32_at(data, metadata_rec + 16).ok()? as usize,
                                true,
                            )
                        } else {
                            (
                                read_u64_at(data, metadata_rec + 8).ok()?,
                                read_u64_at(data, metadata_rec + 16).ok()? as usize,
                                read_u32_at(data, metadata_rec + 24).ok()? as usize,
                                flags & METADATA_KNOWN != 0,
                            )
                        };
                    let tags_start = metadata.blob_off.checked_add(tags_off)?;
                    let tags_end = tags_start.checked_add(tags_len)?;
                    (
                        version,
                        source_generation,
                        flags & TAGS_KNOWN != 0,
                        metadata_known,
                        Some(data.get(tags_start..tags_end)?),
                    )
                }
                None => (1, 0, false, false, None),
            };
        Some(SourceRecord {
            logical,
            query,
            version,
            source_generation,
            tags_known,
            metadata_known,
            encoded_tags,
        })
    }

    fn index_of(&self, logical: u64) -> Option<usize> {
        let (mut lo, mut hi) = (0usize, self.count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let found = self.logical_at(mid)?;
            match found.cmp(&logical) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(mid),
            }
        }
        None
    }

    fn find(&self, logical: u64) -> Option<SourceRecord<'_>> {
        self.record(self.index_of(logical)?)
    }

    /// Read one source only when it fits the caller's remaining byte credit.
    /// The query index/blob is deliberately read without touching the optional
    /// metadata directory/blob; winner enrichment remains the pre-ADR-116
    /// query-only mmap path. The query length is checked before `to_owned`, so an
    /// over-budget source is rejected without allocating its text.
    fn get_bounded(&self, logical: u64, max_bytes: usize) -> Result<Option<String>, usize> {
        let Some(i) = self.index_of(logical) else {
            return Ok(None);
        };
        let Some((_, query)) = self.query_record(i) else {
            return Ok(None);
        };
        if query.len() > max_bytes {
            return Err(query.len());
        }
        Ok(Some(query.to_owned()))
    }

    fn get_document(&self, logical: u64) -> Option<StoredSource> {
        let record = self.find(logical)?;
        let tags = match record.encoded_tags {
            Some(encoded) => decode_tags(encoded).ok()?,
            None => Vec::new(),
        };
        Some(StoredSource {
            query: record.query.to_owned(),
            version: record.version,
            source_generation: record.source_generation,
            tags,
            tags_known: record.tags_known,
            metadata_known: record.metadata_known,
        })
    }
}

impl SourceStore {
    pub fn new_resident() -> Self {
        SourceStore::Resident(std::sync::RwLock::new(crate::util::fast_map()))
    }

    /// An empty store of the kind selected by `retain` (no persisted file yet).
    pub fn empty(retain: bool) -> Self {
        if retain {
            Self::new_resident()
        } else {
            SourceStore::Lazy {
                base: None,
                overlay: std::sync::RwLock::new(crate::util::fast_map()),
            }
        }
    }

    /// Open a store from `path` per `retain`. `retain = true` loads everything
    /// resident (reads v1/v2). `retain = false` mmaps a v2 file lazily,
    /// first migrating a v1 file; an absent file yields an empty lazy store.
    pub fn open(path: &Path, retain: bool) -> io::Result<Self> {
        if retain {
            return Ok(SourceStore::Resident(std::sync::RwLock::new(
                load_stored_sources(path)?,
            )));
        }
        if !path.exists() {
            return Ok(SourceStore::Lazy {
                base: None,
                overlay: std::sync::RwLock::new(crate::util::fast_map()),
            });
        }
        if peek_sources_version(path)? == SOURCES_VERSION_V1 {
            // Migrate unordered v1 to sorted v2. Its tags are marked unknown so
            // the read path can fall back to the exact-store column.
            let map = load_stored_sources(path)?;
            let mut entries: Vec<SourceEntryRef<'_>> = map
                .iter()
                .map(|(logical, source)| SourceEntryRef {
                    logical: *logical,
                    query: source.query(),
                    version: source.version(),
                    source_generation: source.source_generation(),
                    tags_known: source.tags_known(),
                    metadata_known: source.metadata_known(),
                    tags: TagsRef::Decoded(source.tags()),
                })
                .collect();
            entries.sort_unstable_by_key(|entry| entry.logical);
            write_sources_v2(&entries, path)?;
        }
        Ok(SourceStore::Lazy {
            base: Some(open_lazy_base(path)?),
            overlay: std::sync::RwLock::new(crate::util::fast_map()),
        })
    }

    pub fn get(&self, logical: u64) -> Option<String> {
        self.get_bounded(logical, usize::MAX).ok().flatten()
    }

    /// Return the canonical stored document (query + write version + tags).
    /// This is off the match path and may decode the metadata part of a lazy
    /// mmap record; query-only enrichment continues through [`Self::get_bounded`].
    pub fn get_document(&self, logical: u64) -> Option<StoredSource> {
        match self {
            SourceStore::Resident(m) => rw_read(m).get(&logical).cloned(),
            SourceStore::Lazy { base, overlay } => {
                if let Some(value) = rw_read(overlay).get(&logical) {
                    return value.clone();
                }
                base.as_ref()?.get_document(logical)
            }
        }
    }

    /// Return the source only if it fits in `max_bytes`. The size check happens
    /// while the resident/mmap source is still borrowed, before cloning it into
    /// the phase-two response. `Err(actual_len)` distinguishes an over-budget
    /// source from an absent one.
    pub(crate) fn get_bounded(
        &self,
        logical: u64,
        max_bytes: usize,
    ) -> Result<Option<String>, usize> {
        match self {
            SourceStore::Resident(m) => match rw_read(m).get(&logical) {
                Some(source) if source.query.len() > max_bytes => Err(source.query.len()),
                Some(source) => Ok(Some(source.query.clone())),
                None => Ok(None),
            },
            SourceStore::Lazy { base, overlay } => {
                // Overlay (post-flush mutations) wins over the mmap base; a `None`
                // overlay entry is a tombstone (deleted since the last flush).
                if let Some(v) = rw_read(overlay).get(&logical) {
                    return match v {
                        Some(source) if source.query.len() > max_bytes => Err(source.query.len()),
                        Some(source) => Ok(Some(source.query.clone())),
                        None => Ok(None),
                    };
                }
                match base {
                    Some(base) => base.get_bounded(logical, max_bytes),
                    None => Ok(None),
                }
            }
        }
    }

    pub fn insert(&self, logical: u64, text: String) {
        self.insert_document(logical, text, 1, &[]);
    }

    /// Insert the canonical source material accepted by a write. Tags have
    /// already been scalar-coerced and validated at the caller boundary.
    pub fn insert_document(
        &self,
        logical: u64,
        text: String,
        version: u32,
        tags: &[(String, String)],
    ) {
        self.insert_document_with_generation_and_status(logical, text, version, 0, tags, true);
    }

    pub(crate) fn insert_document_with_generation(
        &self,
        logical: u64,
        text: String,
        version: u32,
        source_generation: u64,
        tags: &[(String, String)],
    ) {
        self.insert_document_with_generation_and_status(
            logical,
            text,
            version,
            source_generation,
            tags,
            true,
        );
    }

    pub(crate) fn insert_document_with_generation_and_status(
        &self,
        logical: u64,
        text: String,
        version: u32,
        source_generation: u64,
        tags: &[(String, String)],
        tags_known: bool,
    ) {
        let source = StoredSource::with_generation(
            text,
            version,
            source_generation,
            tags.to_vec(),
            tags_known,
        );
        match self {
            SourceStore::Resident(m) => {
                let mut store = rw_write(m);
                // Recovery replays WAL frames in log order, but a manifest
                // commit may have installed a later same-id bulk segment and
                // source document after an older frame. Never let that older
                // generation roll the canonical sidecar backward. Equal
                // generations still replace so legacy generation-zero frames
                // retain their chronological last-write behavior.
                if store
                    .get(&logical)
                    .is_none_or(|current| current.source_generation() <= source_generation)
                {
                    store.insert(logical, source);
                }
            }
            SourceStore::Lazy { base, overlay } => {
                let should_replace = {
                    let current = rw_read(overlay);
                    match current.get(&logical) {
                        Some(Some(current)) => current.source_generation() <= source_generation,
                        Some(None) => true,
                        None => base
                            .as_ref()
                            .and_then(|base| base.find(logical))
                            .is_none_or(|current| current.source_generation <= source_generation),
                    }
                };
                if should_replace {
                    rw_write(overlay).insert(logical, Some(source));
                }
            }
        }
    }

    pub fn remove(&self, logical: u64) {
        match self {
            SourceStore::Resident(m) => {
                rw_write(m).remove(&logical);
            }
            SourceStore::Lazy { overlay, .. } => {
                rw_write(overlay).insert(logical, None);
            }
        }
    }

    /// Best-effort live entry count (Debug/stats only — not a hot path).
    pub fn len(&self) -> usize {
        match self {
            SourceStore::Resident(m) => rw_read(m).len(),
            SourceStore::Lazy { base, overlay } => {
                let ov = rw_read(overlay);
                let mut n = ov.values().filter(|v| v.is_some()).count();
                if let Some(b) = base {
                    for i in 0..b.count {
                        if let Some(l) = b.logical_at(i) {
                            if !ov.contains_key(&l) {
                                n += 1;
                            }
                        }
                    }
                }
                n
            }
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_lazy(&self) -> bool {
        matches!(self, SourceStore::Lazy { .. })
    }

    /// Largest persisted internal source generation. Used only to seed the
    /// engine's next generation after reopen; including shadowed/tombstoned lazy
    /// base rows is conservative (it may leave a gap, never reuse a generation).
    pub(crate) fn max_source_generation(&self) -> u64 {
        match self {
            SourceStore::Resident(m) => rw_read(m)
                .values()
                .map(StoredSource::source_generation)
                .max()
                .unwrap_or(0),
            SourceStore::Lazy { base, overlay } => {
                let base_max = base.as_ref().map_or(0, |base| {
                    (0..base.count)
                        .filter_map(|i| base.record(i))
                        .map(|record| record.source_generation)
                        .max()
                        .unwrap_or(0)
                });
                let overlay_max = rw_read(overlay)
                    .values()
                    .flatten()
                    .map(StoredSource::source_generation)
                    .max()
                    .unwrap_or(0);
                base_max.max(overlay_max)
            }
        }
    }

    /// Resident heap bytes. For `Lazy` this is just the overlay; the mmap'd base
    /// is file-backed (paged), not resident heap.
    pub fn resident_bytes(&self) -> usize {
        use std::mem::size_of;
        match self {
            SourceStore::Resident(m) => {
                let g = rw_read(m);
                let chars: usize = g
                    .values()
                    .map(|source| {
                        source.query.capacity()
                            + source.tags.capacity() * size_of::<(String, String)>()
                            + source
                                .tags
                                .iter()
                                .map(|(key, value)| key.capacity() + value.capacity())
                                .sum::<usize>()
                    })
                    .sum();
                chars + g.capacity() * size_of::<(u64, StoredSource)>()
            }
            SourceStore::Lazy { overlay, .. } => {
                let g = rw_read(overlay);
                let chars: usize = g
                    .values()
                    .flatten()
                    .map(|source| {
                        source.query.capacity()
                            + source.tags.capacity() * size_of::<(String, String)>()
                            + source
                                .tags
                                .iter()
                                .map(|(key, value)| key.capacity() + value.capacity())
                                .sum::<usize>()
                    })
                    .sum();
                chars + g.capacity() * size_of::<(u64, Option<StoredSource>)>()
            }
        }
    }

    /// Durably write the store's live entries to `path` as an extended v2 file, borrowing
    /// query text and tag data (no `String` clones). `Resident` writes the whole
    /// map; `Lazy` merges the mmap base with the overlay (overlay wins;
    /// `None` = tombstone).
    pub fn write_to(&self, path: &Path) -> io::Result<()> {
        match self {
            SourceStore::Resident(m) => {
                let g = rw_read(m);
                let mut entries: Vec<SourceEntryRef<'_>> = g
                    .iter()
                    .map(|(logical, source)| SourceEntryRef {
                        logical: *logical,
                        query: source.query(),
                        version: source.version(),
                        source_generation: source.source_generation(),
                        tags_known: source.tags_known(),
                        metadata_known: source.metadata_known(),
                        tags: TagsRef::Decoded(source.tags()),
                    })
                    .collect();
                entries.sort_unstable_by_key(|entry| entry.logical);
                write_sources_v2(&entries, path)
            }
            SourceStore::Lazy { base, overlay } => {
                let ov = rw_read(overlay);
                let mut entries: Vec<SourceEntryRef<'_>> = Vec::new();
                if let Some(b) = base {
                    for i in 0..b.count {
                        if let Some(record) = b.record(i) {
                            // overlay (incl. tombstones) shadows the mmap base
                            if !ov.contains_key(&record.logical) {
                                entries.push(SourceEntryRef {
                                    logical: record.logical,
                                    query: record.query,
                                    version: record.version,
                                    source_generation: record.source_generation,
                                    tags_known: record.tags_known,
                                    metadata_known: record.metadata_known,
                                    tags: match record.encoded_tags {
                                        Some(encoded) => TagsRef::Encoded(encoded),
                                        None => TagsRef::Decoded(&[]),
                                    },
                                });
                            }
                        }
                    }
                }
                for (logical, value) in ov.iter() {
                    if let Some(source) = value {
                        entries.push(SourceEntryRef {
                            logical: *logical,
                            query: source.query(),
                            version: source.version(),
                            source_generation: source.source_generation(),
                            tags_known: source.tags_known(),
                            metadata_known: source.metadata_known(),
                            tags: TagsRef::Decoded(source.tags()),
                        });
                    }
                }
                entries.sort_unstable_by_key(|entry| entry.logical);
                write_sources_v2(&entries, path)
            }
        }
    }

    /// Visit every live `(logical, text)` pair (arbitrary order). Mirrors
    /// [`write_to`](Self::write_to)'s live-entry resolution — for `Lazy`, the
    /// overlay shadows the mmap base and a `None` overlay entry is a tombstone —
    /// but hands each pair to `f` instead of serializing. This is the read side
    /// of the "sources are the source of truth, segments are the materialized
    /// view" model: it lets the engine rebuild the index from the live source set
    /// after a normalizer change (see [`Engine::recompile_stale_segments`]).
    pub fn for_each_live(&self, mut f: impl FnMut(u64, &str)) {
        match self {
            SourceStore::Resident(m) => {
                for (k, v) in rw_read(m).iter() {
                    f(*k, v.query());
                }
            }
            SourceStore::Lazy { base, overlay } => {
                let ov = rw_read(overlay);
                if let Some(b) = base {
                    for i in 0..b.count {
                        if let Some(record) = b.record(i) {
                            // overlay (incl. tombstones) shadows the mmap base
                            if !ov.contains_key(&record.logical) {
                                f(record.logical, record.query);
                            }
                        }
                    }
                }
                for (k, v) in ov.iter() {
                    if let Some(source) = v {
                        f(*k, source.query());
                    }
                }
            }
        }
    }

    /// Visit every live canonical source document. The lazy path decodes only
    /// the tag metadata requested by this callback; query-only callers should
    /// keep using [`Self::for_each_live`].
    pub fn for_each_live_document(
        &self,
        mut f: impl FnMut(u64, &str, u32, u64, &[(String, String)], bool, bool),
    ) {
        match self {
            SourceStore::Resident(m) => {
                for (logical, source) in rw_read(m).iter() {
                    f(
                        *logical,
                        source.query(),
                        source.version(),
                        source.source_generation(),
                        source.tags(),
                        source.tags_known(),
                        source.metadata_known(),
                    );
                }
            }
            SourceStore::Lazy { base, overlay } => {
                let ov = rw_read(overlay);
                if let Some(base) = base {
                    for i in 0..base.count {
                        if let Some(record) = base.record(i) {
                            if !ov.contains_key(&record.logical) {
                                let tags = match record.encoded_tags {
                                    Some(encoded) => decode_tags(encoded),
                                    None => Ok(Vec::new()),
                                };
                                if let Ok(tags) = tags {
                                    f(
                                        record.logical,
                                        record.query,
                                        record.version,
                                        record.source_generation,
                                        &tags,
                                        record.tags_known,
                                        record.metadata_known,
                                    );
                                }
                            }
                        }
                    }
                }
                for (logical, value) in ov.iter() {
                    if let Some(source) = value {
                        f(
                            *logical,
                            source.query(),
                            source.version(),
                            source.source_generation(),
                            source.tags(),
                            source.tags_known(),
                            source.metadata_known(),
                        );
                    }
                }
            }
        }
    }
}

/// Peek the version field of a sources file (magic-checked).
fn peek_sources_version(path: &Path) -> io::Result<u32> {
    use std::io::Read;
    let mut f = File::open(path)?;
    let mut head = [0u8; 8];
    f.read_exact(&mut head)?;
    if head[0..4] != SOURCES_MAGIC {
        return Err(bad_sources());
    }
    Ok(u32::from_le_bytes([head[4], head[5], head[6], head[7]]))
}

fn encode_tags(tags: &[(String, String)], out: &mut Vec<u8>) -> io::Result<()> {
    let count = u32::try_from(tags.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "too many source tags"))?;
    out.extend_from_slice(&count.to_le_bytes());
    for (key, value) in tags {
        let key_len = u32::try_from(key.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "source tag key too long"))?;
        let value_len = u32::try_from(value.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "source tag value too long")
        })?;
        out.extend_from_slice(&key_len.to_le_bytes());
        out.extend_from_slice(&value_len.to_le_bytes());
        out.extend_from_slice(key.as_bytes());
        out.extend_from_slice(value.as_bytes());
    }
    Ok(())
}

fn encoded_tag_count(data: &[u8]) -> io::Result<usize> {
    if data.len() < 4 {
        return Err(bad_sources());
    }
    let count = read_u32_at(data, 0)? as usize;
    if count > data.len().saturating_sub(4) / 8 {
        return Err(bad_sources());
    }
    Ok(count)
}

/// Validate and visit the encoded tag slice without allocating owned strings.
/// Lazy open uses the no-op visitor so corruption still fails loud without
/// cloning an entire tagged corpus merely to discard it.
fn visit_encoded_tags(data: &[u8], mut visit: impl FnMut(&str, &str)) -> io::Result<()> {
    let count = encoded_tag_count(data)?;
    let mut cursor = 4usize;
    for _ in 0..count {
        let key_len = read_u32_at(data, cursor)? as usize;
        cursor = cursor.checked_add(4).ok_or_else(bad_sources)?;
        let value_len = read_u32_at(data, cursor)? as usize;
        cursor = cursor.checked_add(4).ok_or_else(bad_sources)?;
        let key_end = cursor.checked_add(key_len).ok_or_else(bad_sources)?;
        let key = std::str::from_utf8(data.get(cursor..key_end).ok_or_else(bad_sources)?)
            .map_err(|_| bad_sources())?;
        cursor = key_end;
        let value_end = cursor.checked_add(value_len).ok_or_else(bad_sources)?;
        let value = std::str::from_utf8(data.get(cursor..value_end).ok_or_else(bad_sources)?)
            .map_err(|_| bad_sources())?;
        cursor = value_end;
        visit(key, value);
    }
    if cursor != data.len() {
        return Err(bad_sources());
    }
    Ok(())
}

fn validate_encoded_tags(data: &[u8]) -> io::Result<()> {
    visit_encoded_tags(data, |_, _| {})
}

fn decode_tags(data: &[u8]) -> io::Result<Vec<(String, String)>> {
    let mut tags = Vec::with_capacity(encoded_tag_count(data)?);
    visit_encoded_tags(data, |key, value| {
        tags.push((key.to_owned(), value.to_owned()));
    })?;
    Ok(tags)
}

/// Write a caller-sorted set of source documents as an extended v2 file.
///
/// The original v2 header/index/query blob stays byte-readable by pre-ADR-116
/// binaries. A metadata directory/blob and fixed footer are appended before the
/// existing CRC. Old readers ignore the tail and keep source text on rollback;
/// new readers discover it from `SMET`.
fn write_sources_v2(entries: &[SourceEntryRef<'_>], path: &Path) -> io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(SRC_HEADER + entries.len() * SRC_IDX_REC + 64);
    buf.extend_from_slice(&SOURCES_MAGIC);
    buf.extend_from_slice(&SOURCES_VERSION.to_le_bytes());
    let entry_count = u32::try_from(entries.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "too many query sources"))?;
    buf.extend_from_slice(&entry_count.to_le_bytes());
    buf.extend_from_slice(&META_HEADER_MARKER.to_le_bytes());
    let mut query_blob: Vec<u8> = Vec::new();
    let mut metadata_blob: Vec<u8> = Vec::new();
    let mut query_records: Vec<(u64, u64, u32)> = Vec::with_capacity(entries.len());
    let mut metadata_records: Vec<(u32, u32, u64, u64, u32)> = Vec::with_capacity(entries.len());
    let mut prev: Option<u64> = None;
    for entry in entries {
        debug_assert!(
            prev.is_none_or(|p| p <= entry.logical),
            "write_sources_v2 requires entries sorted by logical id"
        );
        prev = Some(entry.logical);
        let query_off = query_blob.len() as u64;
        let query_len = u32::try_from(entry.query.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "query source too long"))?;
        query_blob.extend_from_slice(entry.query.as_bytes());
        query_records.push((entry.logical, query_off, query_len));

        let metadata_off = metadata_blob.len() as u64;
        match entry.tags {
            TagsRef::Decoded(tags) => encode_tags(tags, &mut metadata_blob)?,
            TagsRef::Encoded(encoded) => metadata_blob.extend_from_slice(encoded),
        }
        let metadata_len = u32::try_from(metadata_blob.len() as u64 - metadata_off)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "source metadata too long"))?;
        metadata_records.push((
            (u32::from(entry.tags_known) * TAGS_KNOWN)
                | (u32::from(entry.metadata_known) * METADATA_KNOWN),
            entry.version,
            entry.source_generation,
            metadata_off,
            metadata_len,
        ));
    }
    for (logical, query_off, query_len) in query_records {
        buf.extend_from_slice(&logical.to_le_bytes());
        buf.extend_from_slice(&query_off.to_le_bytes());
        buf.extend_from_slice(&query_len.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
    }
    buf.extend_from_slice(&query_blob);

    let metadata_directory_off = buf.len() as u64;
    for (flags, version, source_generation, metadata_off, metadata_len) in metadata_records {
        buf.extend_from_slice(&flags.to_le_bytes());
        buf.extend_from_slice(&version.to_le_bytes());
        buf.extend_from_slice(&source_generation.to_le_bytes());
        buf.extend_from_slice(&metadata_off.to_le_bytes());
        buf.extend_from_slice(&metadata_len.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
    }
    buf.extend_from_slice(&metadata_blob);
    buf.extend_from_slice(&META_MAGIC);
    buf.extend_from_slice(&META_VERSION.to_le_bytes());
    buf.extend_from_slice(&metadata_directory_off.to_le_bytes());
    let crc = crc32(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());

    let tmp = path.with_extension("sources.tmp");
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(&buf)?;
    f.sync_all()?;
    drop(f);
    durable_rename(&tmp, path)?;
    Ok(())
}

fn metadata_layout(
    data: &[u8],
    count: usize,
    query_blob_off: usize,
) -> io::Result<Option<MetadataLayout>> {
    if read_u32_at(data, 12)? != META_HEADER_MARKER {
        return Ok(None);
    }
    let footer_off = data
        .len()
        .checked_sub(4 + META_FOOTER)
        .ok_or_else(bad_sources)?;
    if data.get(footer_off..footer_off + 4) != Some(META_MAGIC.as_slice()) {
        return Err(bad_sources());
    }
    let version = read_u32_at(data, footer_off + 4)?;
    let record_size = match version {
        META_VERSION_V1 => META_IDX_REC_V1,
        META_VERSION => META_IDX_REC,
        _ => return Err(bad_sources()),
    };
    let directory_off = read_u64_at(data, footer_off + 8)? as usize;
    let blob_off = directory_off
        .checked_add(count.checked_mul(record_size).ok_or_else(bad_sources)?)
        .ok_or_else(bad_sources)?;
    if directory_off < query_blob_off || blob_off > footer_off {
        return Err(bad_sources());
    }
    for i in 0..count {
        let record = directory_off + i * record_size;
        let (tags_off, tags_len) = if version == META_VERSION_V1 {
            (
                read_u64_at(data, record + 8)? as usize,
                read_u32_at(data, record + 16)? as usize,
            )
        } else {
            (
                read_u64_at(data, record + 16)? as usize,
                read_u32_at(data, record + 24)? as usize,
            )
        };
        let tags_start = blob_off.checked_add(tags_off).ok_or_else(bad_sources)?;
        let tags_end = tags_start.checked_add(tags_len).ok_or_else(bad_sources)?;
        if tags_end > footer_off {
            return Err(bad_sources());
        }
        validate_encoded_tags(data.get(tags_start..tags_end).ok_or_else(bad_sources)?)?;
    }
    Ok(Some(MetadataLayout {
        version,
        record_size,
        directory_off,
        blob_off,
    }))
}

/// mmap a v2 sources file as a `LazyBase` (validates magic/version/CRC/bounds).
fn open_lazy_base(path: &Path) -> io::Result<LazyBase> {
    let file = File::open(path)?;
    // SAFETY: `path` is an immutable, atomically-renamed sources file written by
    // this single-writer engine and never mutated in place (a rewrite goes to a
    // tmp file + rename, leaving this inode untouched). The mapping is read-only,
    // accessed only via safe `&[u8]` slicing, and the `Arc<Mmap>` keeps it alive
    // for as long as any `LazyBase` (or clone) references it — mirroring the
    // `MmapSegment` mmap-open invariant.
    let mmap = Arc::new(unsafe { memmap2::Mmap::map(&file)? });
    let (count, index_off, blob_off, metadata) = {
        let data: &[u8] = &mmap;
        if data.len() < SRC_HEADER + 4 || data[0..4] != SOURCES_MAGIC {
            return Err(bad_sources());
        }
        let version = read_u32_at(data, 4)?;
        if version != SOURCES_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected sources v{SOURCES_VERSION}, got v{version}"),
            ));
        }
        let count = read_u32_at(data, 8)? as usize;
        let index_off = SRC_HEADER;
        let blob_off = index_off
            .checked_add(count.checked_mul(SRC_IDX_REC).ok_or_else(bad_sources)?)
            .ok_or_else(bad_sources)?;
        if blob_off + 4 > data.len() {
            return Err(bad_sources());
        }
        // CRC over everything but the trailing 4-byte checksum.
        let want = read_u32_at(data, data.len() - 4)?;
        if crc32(&data[..data.len() - 4]) != want {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "sources CRC mismatch",
            ));
        }
        let blob_limit = data.len() - 4;
        let metadata = metadata_layout(data, count, blob_off)?;
        let query_limit = metadata.map_or(blob_limit, |layout| layout.directory_off);
        let mut previous: Option<u64> = None;
        for i in 0..count {
            let rec = index_off + i * SRC_IDX_REC;
            let logical = read_u64_at(data, rec)?;
            if previous.is_some_and(|prior| prior >= logical) {
                return Err(bad_sources());
            }
            previous = Some(logical);
            let query_off = read_u64_at(data, rec + 8)? as usize;
            let query_len = read_u32_at(data, rec + 16)? as usize;
            let query_start = blob_off.checked_add(query_off).ok_or_else(bad_sources)?;
            let query_end = query_start.checked_add(query_len).ok_or_else(bad_sources)?;
            let query_bytes = data.get(query_start..query_end).ok_or_else(bad_sources)?;
            if query_end > query_limit || std::str::from_utf8(query_bytes).is_err() {
                return Err(bad_sources());
            }
        }
        (count, index_off, blob_off, metadata)
    };
    Ok(LazyBase {
        mmap,
        index_off,
        count,
        blob_off,
        metadata,
    })
}

/// Read any supported `sources.dat` fully into canonical stored documents.
fn load_stored_sources(path: &Path) -> io::Result<crate::util::FastMap<u64, StoredSource>> {
    if !path.exists() {
        return Ok(crate::util::fast_map());
    }
    let data = std::fs::read(path)?;
    if data.len() < 12 || data[0..4] != SOURCES_MAGIC {
        return Err(bad_sources());
    }
    let version = read_u32_at(&data, 4)?;
    let count = read_u32_at(&data, 8)? as usize;
    let mut store = crate::util::FastMap::with_capacity_and_hasher(
        count,
        std::hash::BuildHasherDefault::default(),
    );
    match version {
        SOURCES_VERSION_V1 => {
            let mut cursor = 12;
            for _ in 0..count {
                if cursor + 12 > data.len() {
                    break;
                }
                let logical_id = read_u64_at(&data, cursor)?;
                cursor += 8;
                let text_len = read_u32_at(&data, cursor)? as usize;
                cursor += 4;
                if cursor + text_len > data.len() {
                    break;
                }
                let text = std::str::from_utf8(&data[cursor..cursor + text_len])
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                    .to_string();
                cursor += text_len;
                store.insert(logical_id, StoredSource::legacy(text));
            }
        }
        SOURCES_VERSION => {
            let index_off = SRC_HEADER;
            let blob_off = index_off
                .checked_add(count.checked_mul(SRC_IDX_REC).ok_or_else(bad_sources)?)
                .ok_or_else(bad_sources)?;
            if blob_off + 4 > data.len() {
                return Err(bad_sources());
            }
            let want = read_u32_at(&data, data.len() - 4)?;
            if crc32(&data[..data.len() - 4]) != want {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "sources CRC mismatch",
                ));
            }
            let blob_limit = data.len() - 4;
            let metadata = metadata_layout(&data, count, blob_off)?;
            let query_limit = metadata.map_or(blob_limit, |layout| layout.directory_off);
            let mut previous: Option<u64> = None;
            for i in 0..count {
                let rec = index_off + i * SRC_IDX_REC;
                let logical_id = read_u64_at(&data, rec)?;
                if previous.is_some_and(|prior| prior >= logical_id) {
                    return Err(bad_sources());
                }
                previous = Some(logical_id);
                let boff = read_u64_at(&data, rec + 8)? as usize;
                let len = read_u32_at(&data, rec + 16)? as usize;
                let start = blob_off.checked_add(boff).ok_or_else(bad_sources)?;
                let end = start.checked_add(len).ok_or_else(bad_sources)?;
                if end > query_limit {
                    return Err(bad_sources());
                }
                let query = std::str::from_utf8(&data[start..end])
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                    .to_string();
                let source = if let Some(metadata) = metadata {
                    let metadata_rec = metadata.directory_off + i * metadata.record_size;
                    let flags = read_u32_at(&data, metadata_rec)?;
                    let stored_version = read_u32_at(&data, metadata_rec + 4)?;
                    let (source_generation, tags_off, tags_len, metadata_known) =
                        if metadata.version == META_VERSION_V1 {
                            (
                                0,
                                read_u64_at(&data, metadata_rec + 8)? as usize,
                                read_u32_at(&data, metadata_rec + 16)? as usize,
                                true,
                            )
                        } else {
                            (
                                read_u64_at(&data, metadata_rec + 8)?,
                                read_u64_at(&data, metadata_rec + 16)? as usize,
                                read_u32_at(&data, metadata_rec + 24)? as usize,
                                flags & METADATA_KNOWN != 0,
                            )
                        };
                    let tags_start = metadata
                        .blob_off
                        .checked_add(tags_off)
                        .ok_or_else(bad_sources)?;
                    let tags_end = tags_start.checked_add(tags_len).ok_or_else(bad_sources)?;
                    let tags =
                        decode_tags(data.get(tags_start..tags_end).ok_or_else(bad_sources)?)?;
                    StoredSource {
                        query,
                        version: stored_version,
                        source_generation,
                        tags,
                        tags_known: flags & TAGS_KNOWN != 0,
                        metadata_known,
                    }
                } else {
                    StoredSource::legacy(query)
                };
                store.insert(logical_id, source);
            }
        }
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported sources version {other}"),
            ));
        }
    }
    Ok(store)
}

/// Read a source file into the historical query-text-only map used by backup
/// verification and compatibility callers. Metadata is validated and then
/// deliberately projected away.
#[allow(clippy::implicit_hasher)]
pub fn load_query_sources(path: &Path) -> io::Result<crate::util::FastMap<u64, String>> {
    Ok(load_stored_sources(path)?
        .into_iter()
        .map(|(logical, source)| (logical, source.query))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::{MetadataLayout, SourceStore, META_IDX_REC, META_VERSION, SRC_HEADER, SRC_IDX_REC};
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn bounded_resident_lookup_checks_length_before_clone() {
        let store = SourceStore::new_resident();
        store.insert(7, "0123456789".to_string());
        assert_eq!(store.get_bounded(7, 9), Err(10));
        assert_eq!(
            store.get_bounded(7, 10).expect("fits"),
            Some("0123456789".to_string())
        );
        assert_eq!(store.get_bounded(8, 0).expect("absent"), None);
    }

    #[test]
    fn bounded_lazy_lookup_checks_mmap_length_before_clone() {
        let path = std::env::temp_dir().join(format!(
            "reverse-rusty-bounded-sources-{}-{}.dat",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed)
        ));
        let resident = SourceStore::new_resident();
        resident.insert(7, "0123456789".to_string());
        resident.write_to(&path).expect("write v2 sources");

        let lazy = SourceStore::open(&path, false).expect("mmap sources");
        assert_eq!(lazy.get_bounded(7, 9), Err(10));
        assert_eq!(
            lazy.get_bounded(7, 10).expect("fits"),
            Some("0123456789".to_string())
        );

        std::fs::remove_file(path).expect("remove test sources");
    }

    #[test]
    fn bounded_lazy_lookup_does_not_touch_document_metadata() {
        let path = std::env::temp_dir().join(format!(
            "reverse-rusty-query-only-sources-{}-{}.dat",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed)
        ));
        let resident = SourceStore::new_resident();
        resident.insert_document(
            7,
            "0123456789".to_string(),
            42,
            &[("tenant".to_string(), "acme".to_string())],
        );
        resident.write_to(&path).expect("write v2 sources");

        let mut lazy = SourceStore::open(&path, false).expect("mmap sources");
        let SourceStore::Lazy {
            base: Some(base), ..
        } = &mut lazy
        else {
            panic!("expected lazy mmap base");
        };
        // Poison only the metadata layout after open. Query-only lookup must
        // still succeed because it reads the original query index/blob alone.
        base.metadata = Some(MetadataLayout {
            version: META_VERSION,
            record_size: META_IDX_REC,
            directory_off: usize::MAX,
            blob_off: usize::MAX,
        });
        assert_eq!(
            lazy.get_bounded(7, 10).expect("query-only lookup"),
            Some("0123456789".to_string())
        );

        std::fs::remove_file(path).expect("remove test sources");
    }

    #[test]
    fn resident_bytes_counts_each_tag_vector_backing_allocation() {
        let tuple_bytes = std::mem::size_of::<(String, String)>();

        let resident_plain = SourceStore::new_resident();
        resident_plain.insert_document(7, String::new(), 1, &[]);
        let resident_tagged = SourceStore::new_resident();
        resident_tagged.insert_document(7, String::new(), 1, &[(String::new(), String::new())]);
        assert!(
            resident_tagged.resident_bytes() >= resident_plain.resident_bytes() + tuple_bytes,
            "resident accounting must include the tags Vec backing allocation"
        );

        let lazy_plain = SourceStore::empty(false);
        lazy_plain.insert_document(7, String::new(), 1, &[]);
        let lazy_tagged = SourceStore::empty(false);
        lazy_tagged.insert_document(7, String::new(), 1, &[(String::new(), String::new())]);
        assert!(
            lazy_tagged.resident_bytes() >= lazy_plain.resident_bytes() + tuple_bytes,
            "lazy-overlay accounting must include the tags Vec backing allocation"
        );
    }

    #[test]
    fn metadata_v1_footer_remains_readable_as_generation_zero() {
        let path = std::env::temp_dir().join(format!(
            "reverse-rusty-metadata-v1-{}-{}.dat",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed)
        ));
        let query = "topps chrome";
        let tags = vec![("tenant".to_string(), "acme".to_string())];
        let mut encoded_tags = Vec::new();
        super::encode_tags(&tags, &mut encoded_tags).expect("encode tags");

        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"SRCS");
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&super::META_HEADER_MARKER.to_le_bytes());
        bytes.extend_from_slice(&7u64.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&(query.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(query.as_bytes());
        let directory_off = bytes.len() as u64;
        bytes.extend_from_slice(&super::TAGS_KNOWN.to_le_bytes());
        bytes.extend_from_slice(&42u32.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&(encoded_tags.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&encoded_tags);
        bytes.extend_from_slice(b"SMET");
        bytes.extend_from_slice(&super::META_VERSION_V1.to_le_bytes());
        bytes.extend_from_slice(&directory_off.to_le_bytes());
        let crc = super::crc32(&bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());
        std::fs::write(&path, bytes).expect("write metadata v1 fixture");

        for retain in [true, false] {
            let store = SourceStore::open(&path, retain).expect("open metadata v1");
            let document = store.get_document(7).expect("document");
            assert_eq!(document.query(), query);
            assert_eq!(document.version(), 42);
            assert_eq!(document.source_generation(), 0);
            assert!(document.metadata_known());
            assert!(document.tags_known());
            assert_eq!(document.tags(), tags);
        }
        std::fs::remove_file(path).expect("remove metadata v1 fixture");
    }

    #[test]
    fn metadata_footer_round_trip_preserves_version_and_canonical_tags() {
        let path = std::env::temp_dir().join(format!(
            "reverse-rusty-metadata-sources-{}-{}.dat",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed)
        ));
        let resident = SourceStore::new_resident();
        resident.insert_document_with_generation(
            7,
            "topps chrome".to_string(),
            42,
            99,
            &[
                ("tenant".to_string(), "acme".to_string()),
                ("color".to_string(), "blue".to_string()),
                ("color".to_string(), "red".to_string()),
            ],
        );
        resident.write_to(&path).expect("write extended v2 sources");

        // A pre-ADR-116 v2 reader sees the unchanged 24-byte query index,
        // ignores the appended metadata/footer, and still recovers query text.
        let bytes = std::fs::read(&path).expect("read extended v2");
        assert_eq!(u32::from_le_bytes(bytes[4..8].try_into().unwrap()), 2);
        let old_blob_off = SRC_HEADER + SRC_IDX_REC;
        let old_query_off =
            u64::from_le_bytes(bytes[SRC_HEADER + 8..SRC_HEADER + 16].try_into().unwrap()) as usize;
        let old_query_len =
            u32::from_le_bytes(bytes[SRC_HEADER + 16..SRC_HEADER + 20].try_into().unwrap())
                as usize;
        assert_eq!(
            std::str::from_utf8(
                &bytes[old_blob_off + old_query_off..old_blob_off + old_query_off + old_query_len]
            )
            .expect("old-reader query"),
            "topps chrome"
        );

        let lazy = SourceStore::open(&path, false).expect("mmap extended v2 sources");
        assert_eq!(
            lazy.get_bounded(7, 12).expect("query fits").as_deref(),
            Some("topps chrome")
        );
        let document = lazy.get_document(7).expect("stored document");
        assert_eq!(document.query(), "topps chrome");
        assert_eq!(document.version(), 42);
        assert_eq!(document.source_generation(), 99);
        assert!(document.tags_known());
        assert_eq!(
            document.tags(),
            [
                ("tenant".to_string(), "acme".to_string()),
                ("color".to_string(), "blue".to_string()),
                ("color".to_string(), "red".to_string()),
            ]
        );

        std::fs::remove_file(path).expect("remove test sources");
    }

    #[test]
    fn source_generation_prevents_replay_from_rolling_document_backward() {
        let resident = SourceStore::new_resident();
        resident.insert_document_with_generation(7, "new".to_string(), 2, 20, &[]);
        resident.insert_document_with_generation(7, "old".to_string(), 1, 10, &[]);
        let document = resident.get_document(7).expect("resident document");
        assert_eq!(document.query(), "new");
        assert_eq!(document.source_generation(), 20);

        let path = std::env::temp_dir().join(format!(
            "reverse-rusty-monotonic-sources-{}-{}.dat",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed)
        ));
        resident.write_to(&path).expect("write lazy base");
        let lazy = SourceStore::open(&path, false).expect("open lazy base");
        lazy.insert_document_with_generation(7, "old".to_string(), 1, 10, &[]);
        assert_eq!(
            lazy.get_document(7).expect("base still wins").query(),
            "new"
        );
        lazy.insert_document_with_generation(7, "newest".to_string(), 3, 21, &[]);
        let document = lazy.get_document(7).expect("overlay document");
        assert_eq!(document.query(), "newest");
        assert_eq!(document.source_generation(), 21);

        std::fs::remove_file(path).expect("remove test sources");
    }
}
