use super::{
    decode_tags, load_stored_sources, open_lazy_base, peek_sources_version, rw_read, rw_write,
    write_sources_v2, Path, SourceEntryRef, SourceStore, StoredSource, TagsRef, SOURCES_VERSION_V1,
};
use std::io;

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

    /// Generation-attested source lookup for immutable engine snapshots. The
    /// store itself is shared across snapshots, so the generation comparison
    /// and text clone must happen under the same resident/overlay read guard.
    pub(crate) fn get_bounded_at_generation(
        &self,
        logical: u64,
        expected_generation: u64,
        max_bytes: usize,
    ) -> Result<Option<String>, usize> {
        let copy = |source: &StoredSource| {
            if source.source_generation() != expected_generation {
                return Ok(None);
            }
            if source.query.len() > max_bytes {
                return Err(source.query.len());
            }
            Ok(Some(source.query.clone()))
        };
        match self {
            SourceStore::Resident(map) => match rw_read(map).get(&logical) {
                Some(source) => copy(source),
                None => Ok(None),
            },
            SourceStore::Lazy { base, overlay } => {
                let overlay = rw_read(overlay);
                match overlay.get(&logical) {
                    Some(Some(source)) => copy(source),
                    Some(None) => Ok(None),
                    None => base.as_ref().map_or(Ok(None), |base| {
                        base.get_bounded_at_generation(logical, expected_generation, max_bytes)
                    }),
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
        self.insert_stored(logical, source);
    }

    /// Apply one already-canonical source document using the same monotonic
    /// source-generation rule as WAL replay and ordinary inserts.
    pub(crate) fn insert_stored(&self, logical: u64, source: StoredSource) {
        let source_generation = source.source_generation();
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
        self.write_to_with_updates(&[], path)
    }

    /// Write a complete candidate source corpus with `updates` overlaid without
    /// publishing those updates in memory. This is the prepare phase of ADR-121's
    /// bulk commit: the immutable sidecar becomes durable before the manifest
    /// atomically selects it together with the new segment.
    pub(crate) fn write_to_with_updates(
        &self,
        updates: &[(u64, StoredSource)],
        path: &Path,
    ) -> io::Result<()> {
        let mut update_winners: crate::util::FastMap<u64, &StoredSource> = crate::util::fast_map();
        for (logical, source) in updates {
            if update_winners
                .get(logical)
                .is_none_or(|current| current.source_generation() <= source.source_generation())
            {
                update_winners.insert(*logical, source);
            }
        }

        // An older replay/rebuild generation must not replace a newer canonical
        // document already in the store. Filter it before suppressing the base row.
        update_winners.retain(|logical, source| {
            self.source_generation_of(*logical)
                .is_none_or(|current| current <= source.source_generation())
        });

        match self {
            SourceStore::Resident(m) => {
                let g = rw_read(m);
                let mut entries: Vec<SourceEntryRef<'_>> = g
                    .iter()
                    .filter(|(logical, _)| !update_winners.contains_key(logical))
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
                entries.extend(
                    update_winners
                        .iter()
                        .map(|(logical, source)| SourceEntryRef {
                            logical: *logical,
                            query: source.query(),
                            version: source.version(),
                            source_generation: source.source_generation(),
                            tags_known: source.tags_known(),
                            metadata_known: source.metadata_known(),
                            tags: TagsRef::Decoded(source.tags()),
                        }),
                );
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
                            if !ov.contains_key(&record.logical)
                                && !update_winners.contains_key(&record.logical)
                            {
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
                        if update_winners.contains_key(logical) {
                            continue;
                        }
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
                entries.extend(
                    update_winners
                        .iter()
                        .map(|(logical, source)| SourceEntryRef {
                            logical: *logical,
                            query: source.query(),
                            version: source.version(),
                            source_generation: source.source_generation(),
                            tags_known: source.tags_known(),
                            metadata_known: source.metadata_known(),
                            tags: TagsRef::Decoded(source.tags()),
                        }),
                );
                entries.sort_unstable_by_key(|entry| entry.logical);
                write_sources_v2(&entries, path)
            }
        }
    }

    fn source_generation_of(&self, logical: u64) -> Option<u64> {
        match self {
            SourceStore::Resident(m) => rw_read(m)
                .get(&logical)
                .map(StoredSource::source_generation),
            SourceStore::Lazy { base, overlay } => match rw_read(overlay).get(&logical) {
                Some(Some(source)) => Some(source.source_generation()),
                Some(None) => None,
                None => base
                    .as_ref()
                    .and_then(|base| base.find(logical))
                    .map(|record| record.source_generation),
            },
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
