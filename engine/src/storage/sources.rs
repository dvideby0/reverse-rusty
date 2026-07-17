//! Per-query source-text persistence (`SourceStore`) — the `logical_id → query text`
//! store backing `_source`/explain. Resident (all in RAM) or `Lazy` (an mmap'd,
//! binary-searchable v2 file + an in-memory overlay of post-flush mutations).
//! ADR-020 Item 1. Source text never touches the match hot path.

use std::fs::File;
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;

use super::{crc32, durable_rename, read_u32_at, read_u64_at};

// -- Query source store persistence ------------------------------------------

const SOURCES_MAGIC: [u8; 4] = *b"SRCS";
const SOURCES_VERSION_V1: u32 = 1; // legacy: unordered (logical, len, text)*
const SOURCES_VERSION: u32 = 2; // current: sorted index + blob + CRC trailer
const SRC_HEADER: usize = 16; // magic(4) + version(4) + count(4) + reserved(4)
const SRC_IDX_REC: usize = 24; // logical(8) + blob_off(8) + text_len(4) + pad(4)

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
    Resident(std::sync::RwLock<crate::util::FastMap<u64, String>>),
    Lazy {
        base: Option<LazyBase>,
        overlay: std::sync::RwLock<crate::util::FastMap<u64, Option<String>>>,
    },
}

/// An mmap'd v2 `sources.dat`: a sorted index + a text blob. Naturally
/// `Send`+`Sync` — the only shared state is the read-only `Arc<Mmap>`, accessed
/// via safe `&[u8]` slicing (no raw pointers, unlike `MmapSegment`).
pub struct LazyBase {
    mmap: Arc<memmap2::Mmap>,
    index_off: usize,
    count: usize,
    blob_off: usize,
}

impl LazyBase {
    #[inline]
    fn logical_at(&self, i: usize) -> Option<u64> {
        read_u64_at(&self.mmap, self.index_off + i * SRC_IDX_REC).ok()
    }

    /// Read one source only when it fits the caller's remaining byte credit.
    /// The mmap length is checked before `to_owned`, so an over-budget source is
    /// rejected without allocating its text.
    fn get_bounded(&self, logical: u64, max_bytes: usize) -> Result<Option<String>, usize> {
        let data: &[u8] = &self.mmap;
        let (mut lo, mut hi) = (0usize, self.count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let Some(found) = self.logical_at(mid) else {
                return Ok(None);
            };
            match found.cmp(&logical) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    let rec = self.index_off + mid * SRC_IDX_REC;
                    let Some(boff) = read_u64_at(data, rec + 8).ok().map(|v| v as usize) else {
                        return Ok(None);
                    };
                    let Some(len) = read_u32_at(data, rec + 16).ok().map(|v| v as usize) else {
                        return Ok(None);
                    };
                    if len > max_bytes {
                        return Err(len);
                    }
                    let Some(start) = self.blob_off.checked_add(boff) else {
                        return Ok(None);
                    };
                    let Some(end) = start.checked_add(len) else {
                        return Ok(None);
                    };
                    let Some(bytes) = data.get(start..end) else {
                        return Ok(None);
                    };
                    return Ok(std::str::from_utf8(bytes).ok().map(str::to_owned));
                }
            }
        }
        Ok(None)
    }

    /// The `(logical, text)` pair at index `i`, with the text borrowed from the
    /// mmap (lifetime tied to `&self`, so callers can collect it). Returns `None`
    /// on a bounds/UTF-8 check failure (the file is CRC-checked at open, so this
    /// is belt-and-suspenders). Used to rewrite the file on flush.
    fn record(&self, i: usize) -> Option<(u64, &str)> {
        let data: &[u8] = &self.mmap;
        let rec = self.index_off + i * SRC_IDX_REC;
        let logical = read_u64_at(data, rec).ok()?;
        let boff = read_u64_at(data, rec + 8).ok()? as usize;
        let len = read_u32_at(data, rec + 16).ok()? as usize;
        let start = self.blob_off + boff;
        let bytes = data.get(start..start + len)?;
        let text = std::str::from_utf8(bytes).ok()?;
        Some((logical, text))
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
    /// resident (reads v1 or v2). `retain = false` mmaps a v2 file lazily,
    /// first migrating a v1 file to v2; an absent file yields an empty lazy store.
    pub fn open(path: &Path, retain: bool) -> io::Result<Self> {
        if retain {
            return Ok(SourceStore::Resident(std::sync::RwLock::new(
                load_query_sources(path)?,
            )));
        }
        if !path.exists() {
            return Ok(SourceStore::Lazy {
                base: None,
                overlay: std::sync::RwLock::new(crate::util::fast_map()),
            });
        }
        if peek_sources_version(path)? == SOURCES_VERSION_V1 {
            // Migrate v1 → v2 so the file can be mmap'd and binary-searched.
            let map = load_query_sources(path)?;
            let mut entries: Vec<(u64, &str)> = map.iter().map(|(k, v)| (*k, v.as_str())).collect();
            entries.sort_unstable_by_key(|&(k, _)| k);
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
                Some(source) if source.len() > max_bytes => Err(source.len()),
                Some(source) => Ok(Some(source.clone())),
                None => Ok(None),
            },
            SourceStore::Lazy { base, overlay } => {
                // Overlay (post-flush mutations) wins over the mmap base; a `None`
                // overlay entry is a tombstone (deleted since the last flush).
                if let Some(v) = rw_read(overlay).get(&logical) {
                    return match v {
                        Some(source) if source.len() > max_bytes => Err(source.len()),
                        Some(source) => Ok(Some(source.clone())),
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
        match self {
            SourceStore::Resident(m) => {
                rw_write(m).insert(logical, text);
            }
            SourceStore::Lazy { overlay, .. } => {
                rw_write(overlay).insert(logical, Some(text));
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

    /// Resident heap bytes. For `Lazy` this is just the overlay; the mmap'd base
    /// is file-backed (paged), not resident heap.
    pub fn resident_bytes(&self) -> usize {
        use std::mem::size_of;
        match self {
            SourceStore::Resident(m) => {
                let g = rw_read(m);
                let chars: usize = g.values().map(String::capacity).sum();
                chars + g.capacity() * size_of::<(u64, String)>()
            }
            SourceStore::Lazy { overlay, .. } => {
                let g = rw_read(overlay);
                let chars: usize = g.values().flatten().map(String::capacity).sum();
                chars + g.capacity() * size_of::<(u64, Option<String>)>()
            }
        }
    }

    /// Durably write the store's live entries to `path` as a v2 file, borrowing
    /// text (no `String` clones). `Resident` writes the whole map; `Lazy` merges
    /// the mmap base with the overlay (overlay wins; `None` = tombstone).
    pub fn write_to(&self, path: &Path) -> io::Result<()> {
        match self {
            SourceStore::Resident(m) => {
                let g = rw_read(m);
                let mut entries: Vec<(u64, &str)> =
                    g.iter().map(|(k, v)| (*k, v.as_str())).collect();
                entries.sort_unstable_by_key(|&(k, _)| k);
                write_sources_v2(&entries, path)
            }
            SourceStore::Lazy { base, overlay } => {
                let ov = rw_read(overlay);
                let mut entries: Vec<(u64, &str)> = Vec::new();
                if let Some(b) = base {
                    for i in 0..b.count {
                        if let Some((logical, text)) = b.record(i) {
                            // overlay (incl. tombstones) shadows the mmap base
                            if !ov.contains_key(&logical) {
                                entries.push((logical, text));
                            }
                        }
                    }
                }
                for (k, v) in ov.iter() {
                    if let Some(s) = v {
                        entries.push((*k, s.as_str()));
                    }
                }
                entries.sort_unstable_by_key(|&(k, _)| k);
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
                    f(*k, v.as_str());
                }
            }
            SourceStore::Lazy { base, overlay } => {
                let ov = rw_read(overlay);
                if let Some(b) = base {
                    for i in 0..b.count {
                        if let Some((logical, text)) = b.record(i) {
                            // overlay (incl. tombstones) shadows the mmap base
                            if !ov.contains_key(&logical) {
                                f(logical, text);
                            }
                        }
                    }
                }
                for (k, v) in ov.iter() {
                    if let Some(s) = v {
                        f(*k, s.as_str());
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

/// Write a caller-sorted set of `(logical, text)` entries as a v2 sources file:
/// a sorted index + a text blob + a CRC-32 trailer, written atomically.
fn write_sources_v2(entries: &[(u64, &str)], path: &Path) -> io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(SRC_HEADER + entries.len() * SRC_IDX_REC + 64);
    buf.extend_from_slice(&SOURCES_MAGIC);
    buf.extend_from_slice(&SOURCES_VERSION.to_le_bytes());
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // reserved
    let mut blob: Vec<u8> = Vec::new();
    let mut blob_off: u64 = 0;
    let mut prev: Option<u64> = None;
    for &(logical, text) in entries {
        debug_assert!(
            prev.is_none_or(|p| p <= logical),
            "write_sources_v2 requires entries sorted by logical id"
        );
        prev = Some(logical);
        let bytes = text.as_bytes();
        buf.extend_from_slice(&logical.to_le_bytes());
        buf.extend_from_slice(&blob_off.to_le_bytes());
        buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes()); // pad
        blob.extend_from_slice(bytes);
        blob_off += bytes.len() as u64;
    }
    buf.extend_from_slice(&blob);
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
    let (count, index_off, blob_off) = {
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
        (count, index_off, blob_off)
    };
    Ok(LazyBase {
        mmap,
        index_off,
        count,
        blob_off,
    })
}

/// Read a v1 or v2 `sources.dat` fully into a map (the `Resident` path, and the
/// v1→v2 migration source). `FastMap` pins the FNV-1a hasher on purpose (stable
/// hashing across runs — see util.rs).
#[allow(clippy::implicit_hasher)]
pub fn load_query_sources(path: &Path) -> io::Result<crate::util::FastMap<u64, String>> {
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
                store.insert(logical_id, text);
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
            let blob_limit = data.len() - 4;
            for i in 0..count {
                let rec = index_off + i * SRC_IDX_REC;
                let logical_id = read_u64_at(&data, rec)?;
                let boff = read_u64_at(&data, rec + 8)? as usize;
                let len = read_u32_at(&data, rec + 16)? as usize;
                let start = blob_off + boff;
                if start + len > blob_limit {
                    break;
                }
                let text = std::str::from_utf8(&data[start..start + len])
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                    .to_string();
                store.insert(logical_id, text);
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

#[cfg(test)]
mod tests {
    use super::SourceStore;
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
}
