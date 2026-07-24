//! The mmap-backed segment read view: the [`MmapSegment`] struct + its [`open`]
//! constructor (the validated, two-phase mmap parse). Its read/match surface — the
//! zero-cost slice accessors, the hot-path matchers, and `to_memory_segment` — lives
//! in the [`ops`] submodule (a descendant, so it shares the struct's private fields).
//!
//! [`open`]: MmapSegment::open

use std::io;
use std::path::Path;
use std::sync::Arc;

use super::super::{crc32, read_u32_at, read_u64_at};
use super::read::{
    parse_frozen_index, read_u16_slice, read_u32_slice, read_u64_slice, read_u8_slice,
};
use super::{
    FrozenSlot, FORMAT_VERSION_HOT, FORMAT_VERSION_OWNERSHIP, FORMAT_VERSION_RANK,
    FORMAT_VERSION_SOURCE_GENERATION, HEADER_SIZE, MAGIC,
};

mod ops;

fn slice_ptr<T>(slice: &[T]) -> *const T {
    if slice.is_empty() {
        std::ptr::NonNull::<T>::dangling().as_ptr().cast_const()
    } else {
        slice.as_ptr()
    }
}

// ---- MmapSegment ----

/// A sealed segment backed by a memory-mapped file. Provides the same matching
/// semantics as an in-memory `Segment` but with OS-managed paging — cold data
/// stays on disk until accessed, hot data stays in the page cache.
///
/// The `alive_overlay` is the only mutable state: tombstones are applied here
/// (since the mmap is read-only). On compaction, dead entries are dropped.
/// The logical→local reverse index for an `MmapSegment` (ADR-020 Item 2). Two
/// sorted parallel columns: a binary search over `logical` yields the contiguous
/// run in `local`. `Mapped` borrows the columns straight from the mmap (v2 files —
/// ~zero resident heap, paged on demand); `Owned` holds them in RAM (reconstructed
/// from a v1 file that predates the column section, far smaller than the old
/// per-logical `Vec` map, and reclaimed once the segment is recompacted to v2).
#[derive(Clone)]
enum MmapLogicalIndex {
    Mapped {
        logical: *const u64,
        local: *const u32,
        count: usize,
    },
    Owned {
        logical: Vec<u64>,
        local: Vec<u32>,
    },
}

pub struct MmapSegment {
    mmap: Arc<memmap2::Mmap>,
    num_queries: u32,
    /// The file's header format version (1..=8). v4 ⇔ the segment holds class-D
    /// always-candidates (the ADR-068 rollback fence); v5 ⇔ it holds class-H
    /// hot-tier entries (the ADR-105 fence + the hot-index section) — surfaced so
    /// the manifest commit can propagate the fence to its own version word.
    format_version: u32,
    // ExactStore slices (offsets into the mmap, cast at load time)
    req_mask: *const u64,
    forb_mask: *const u64,
    req_off: *const u32,
    req_len: *const u16,
    req_blob: *const u32,
    req_blob_len: usize,
    forb_off: *const u32,
    forb_len: *const u16,
    forb_blob: *const u32,
    forb_blob_len: usize,
    q_group_start: *const u32,
    q_group_count: *const u16,
    group_off: *const u32,
    group_off_len: usize,
    group_len: *const u16,
    anyof_blob: *const u32,
    anyof_blob_len: usize,
    // Per-query tag column (ADR-049). `tag_count` is the number of tag_off/tag_len
    // entries (== num_queries for a v3 segment, 0 for a pre-tag v1/v2 segment).
    tag_off: *const u32,
    tag_len: *const u16,
    tag_blob: *const u32,
    tag_blob_len: usize,
    tag_count: usize,
    version_arr: *const u32,
    logical_arr: *const u64,
    // Optional v6 fixed typed-priority column. `priority_count == 0` pre-v6.
    priority_arr: *const i64,
    priority_count: usize,
    // Optional v7 ADR-109 ownership columns. `placement_count == 0` pre-v7.
    placement_generation: *const u64,
    placement_num_shards: *const u32,
    placement_mode: *const u8,
    placement_off: *const u32,
    placement_len: *const u32,
    placement_blob: *const u32,
    placement_blob_len: usize,
    placement_count: usize,
    // Optional v8 source/exact coupling column. `source_generation_count == 0`
    // pre-v8; accessors expose legacy generation zero in that case.
    source_generation: *const u64,
    source_generation_count: usize,
    // Main index
    main_slots: *const FrozenSlot,
    main_cap: usize,
    main_mask: u64,
    main_blob: *const u32,
    main_blob_len: usize,
    // Broad index
    broad_slots: *const FrozenSlot,
    broad_cap: usize,
    broad_mask: u64,
    broad_blob: *const u32,
    broad_blob_len: usize,
    // Hot-tier index (class H, ADR-105; v5). Absent pre-v5 / on hot-free files:
    // cap 0 + dangling pointers, same soundness pattern as the tag column.
    hot_slots: *const FrozenSlot,
    hot_cap: usize,
    hot_mask: u64,
    hot_blob: *const u32,
    hot_blob_len: usize,
    // Filter
    filter_data: *const u64,
    filter_num_blocks: usize,
    filter_mask: u64,
    // Meta
    class_arr: *const u8,
    // Alive overlay (in-memory, mutable for tombstones)
    pub(crate) alive_overlay: Vec<bool>,
    /// O(1) counter of alive (non-tombstoned) entries.
    alive_counter: usize,
    /// The DEAD locals, maintained incrementally alongside `alive_overlay`
    /// (seeded from the on-disk flags, one insert per tombstone) so the manifest
    /// commit can serialize it in O(deletes) instead of rescanning the segment
    /// (ADR-066). Invariant: `dead_overlay` ≡ the dead set of `alive_overlay`.
    dead_overlay: roaring::RoaringBitmap,
    // Path for cleanup/identification
    path: std::path::PathBuf,
    /// Vocab epoch at which this segment's queries were compiled.
    pub vocab_epoch: u64,
    /// Reverse index (logical_id → local_ids) as sorted parallel columns —
    /// borrowed from the mmap (v2) or reconstructed (v1). See [`MmapLogicalIndex`].
    logical_index: MmapLogicalIndex,
}

/// Cross-validate the per-query SoA columns against the blobs they index, once at
/// open so the hot path (`verify_slices` / `to_memory_segment`) can slice the blobs
/// branch-free (ADR-052 extended to *intra-section* consistency).
///
/// `checked_section_end` already proved each section's own `count` lands inside the
/// mmap, but NOT that `req_off[i] + req_len[i]` lands inside `req_blob` (etc.). A
/// writer bug, a torn write that re-passes CRC, or tampering could leave an offset
/// column pointing past its blob; the unchecked `&blob[o..o+l]` slices downstream
/// would then panic (out-of-bounds) instead of failing loud. This verifies, for
/// every query `i`, that every column entry indexes inside its blob — and that the
/// any-of group window and each group's posting land inside their arrays — turning a
/// corrupt segment into a typed `InvalidData` error.
#[allow(clippy::too_many_arguments)]
fn validate_columns(
    format_version: u32,
    num_queries: usize,
    req_off: &[u32],
    req_len: &[u16],
    req_blob_len: usize,
    forb_off: &[u32],
    forb_len: &[u16],
    forb_blob_len: usize,
    q_group_start: &[u32],
    q_group_count: &[u16],
    group_off: &[u32],
    group_len: &[u16],
    anyof_blob_len: usize,
    tag_off: &[u32],
    tag_len: &[u16],
    tag_blob_len: usize,
) -> io::Result<()> {
    let invalid = |msg: &'static str| io::Error::new(io::ErrorKind::InvalidData, msg);

    // The per-query columns are indexed by local id `0..num_queries` (the accessors
    // read exactly `num_queries` elements), so each must hold at least that many.
    if req_off.len() < num_queries
        || req_len.len() < num_queries
        || forb_off.len() < num_queries
        || forb_len.len() < num_queries
        || q_group_start.len() < num_queries
        || q_group_count.len() < num_queries
    {
        return Err(invalid("segment per-query column shorter than num_queries"));
    }
    // The tag column is indexed by local id `0..num_queries` just like the others
    // (the writer pushes one `tag_off`/`tag_len` entry per query, length 0 for an
    // untagged query — `ExactStore::push`). So for any v3+ file it MUST hold one
    // entry per query. v1/v2 predate the section and read back empty.
    //
    // We must NOT relax this to "either empty or full-length": a torn/corrupt v3+
    // tag section that re-passes CRC could surface as a zero-length column, which
    // would otherwise read every query back as untagged — silently dropping tagged
    // queries from *filtered* percolation instead of failing loud. Tags never gate
    // the lossless cover (matching.md §5.3), so this is not a positive-semantics FN,
    // but it is exactly the intra-segment corruption this validation exists to catch.
    let tags_expected = format_version >= 3 && num_queries > 0;
    if tags_expected && (tag_off.len() < num_queries || tag_len.len() < num_queries) {
        return Err(invalid("segment tag column shorter than num_queries"));
    }

    // Each `off + len` must land inside its blob; `as usize` widens u32/u16 so the
    // add cannot wrap on a 64-bit target.
    let fits =
        |off: u32, len: u16, blob_len: usize| -> bool { off as usize + len as usize <= blob_len };

    for i in 0..num_queries {
        if !fits(req_off[i], req_len[i], req_blob_len) {
            return Err(invalid("segment req column overruns req_blob"));
        }
        if !fits(forb_off[i], forb_len[i], forb_blob_len) {
            return Err(invalid("segment forb column overruns forb_blob"));
        }
        // The any-of group window for query `i` must land inside group_off/group_len.
        let gs = q_group_start[i] as usize;
        let gc = q_group_count[i] as usize;
        let gend = gs
            .checked_add(gc)
            .ok_or_else(|| invalid("segment any-of group window overflows usize"))?;
        if gend > group_off.len() || gend > group_len.len() {
            return Err(invalid("segment any-of group window overruns group arrays"));
        }
        if tags_expected && !fits(tag_off[i], tag_len[i], tag_blob_len) {
            return Err(invalid("segment tag column overruns tag_blob"));
        }
    }

    // Every group's posting must land inside anyof_blob (groups are shared across
    // queries, so validate the whole group_off/group_len array once). The two arrays
    // are parallel, so a length mismatch is itself corruption.
    if group_off.len() != group_len.len() {
        return Err(invalid(
            "segment any-of group_off/group_len length mismatch",
        ));
    }
    for (&go, &gl) in group_off.iter().zip(group_len.iter()) {
        if go as usize + gl as usize > anyof_blob_len {
            return Err(invalid("segment any-of group overruns anyof_blob"));
        }
    }

    Ok(())
}

// SAFETY: every raw pointer in MmapSegment points into the read-only `Arc<Mmap>`
// it owns. The mapping is never written through, does not move, and stays alive
// for as long as any clone (clones share the Arc). All other fields are Send,
// and `alive_overlay`/`alive_counter` are only mutated through `&mut self`, so
// moving a MmapSegment between threads cannot race.
unsafe impl Send for MmapSegment {}
// SAFETY: as argued for the `Send` impl above, all shared state behind the raw
// pointers is immutable for the segment's lifetime, so `&MmapSegment` can be
// shared across threads without data races.
unsafe impl Sync for MmapSegment {}

impl Clone for MmapSegment {
    fn clone(&self) -> Self {
        MmapSegment {
            mmap: Arc::clone(&self.mmap),
            num_queries: self.num_queries,
            format_version: self.format_version,
            req_mask: self.req_mask,
            forb_mask: self.forb_mask,
            req_off: self.req_off,
            req_len: self.req_len,
            req_blob: self.req_blob,
            req_blob_len: self.req_blob_len,
            forb_off: self.forb_off,
            forb_len: self.forb_len,
            forb_blob: self.forb_blob,
            forb_blob_len: self.forb_blob_len,
            q_group_start: self.q_group_start,
            q_group_count: self.q_group_count,
            group_off: self.group_off,
            group_off_len: self.group_off_len,
            group_len: self.group_len,
            anyof_blob: self.anyof_blob,
            anyof_blob_len: self.anyof_blob_len,
            tag_off: self.tag_off,
            tag_len: self.tag_len,
            tag_blob: self.tag_blob,
            tag_blob_len: self.tag_blob_len,
            tag_count: self.tag_count,
            version_arr: self.version_arr,
            logical_arr: self.logical_arr,
            priority_arr: self.priority_arr,
            priority_count: self.priority_count,
            placement_generation: self.placement_generation,
            placement_num_shards: self.placement_num_shards,
            placement_mode: self.placement_mode,
            placement_off: self.placement_off,
            placement_len: self.placement_len,
            placement_blob: self.placement_blob,
            placement_blob_len: self.placement_blob_len,
            placement_count: self.placement_count,
            source_generation: self.source_generation,
            source_generation_count: self.source_generation_count,
            main_slots: self.main_slots,
            main_cap: self.main_cap,
            main_mask: self.main_mask,
            main_blob: self.main_blob,
            main_blob_len: self.main_blob_len,
            broad_slots: self.broad_slots,
            broad_cap: self.broad_cap,
            broad_mask: self.broad_mask,
            broad_blob: self.broad_blob,
            broad_blob_len: self.broad_blob_len,
            hot_slots: self.hot_slots,
            hot_cap: self.hot_cap,
            hot_mask: self.hot_mask,
            hot_blob: self.hot_blob,
            hot_blob_len: self.hot_blob_len,
            filter_data: self.filter_data,
            filter_num_blocks: self.filter_num_blocks,
            filter_mask: self.filter_mask,
            class_arr: self.class_arr,
            alive_overlay: self.alive_overlay.clone(),
            alive_counter: self.alive_counter,
            dead_overlay: self.dead_overlay.clone(),
            path: self.path.clone(),
            vocab_epoch: self.vocab_epoch,
            logical_index: self.logical_index.clone(),
        }
    }
}

impl std::fmt::Debug for MmapSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MmapSegment")
            .field("num_queries", &self.num_queries)
            .field("path", &self.path)
            .field("alive_count", &self.alive_count())
            .finish()
    }
}

impl MmapSegment {
    /// Whether this segment's file carries the class-D rollback fence (format v4,
    /// ADR-068) — i.e. it holds at least one always-candidate. The manifest commit
    /// ORs this across registered segments to pick its own version word.
    pub fn carries_class_d_fence(&self) -> bool {
        let mut counts = [0u64; 5];
        self.class_counts(&mut counts);
        counts[3] != 0
    }

    /// Whether this segment's file carries the hot-tier fence (format v5,
    /// ADR-105) — i.e. it holds class-H entries a pre-ADR-105 binary would
    /// silently never probe. Propagated to the engine manifest's version word.
    pub fn carries_hot_fence(&self) -> bool {
        self.hot_cap != 0
    }

    /// Whether this file carries v8 source/exact generations. Propagated to the
    /// standalone manifest so older recovery code refuses the corpus loudly
    /// instead of skipping the unreadable segment.
    pub fn carries_source_generation_fence(&self) -> bool {
        self.format_version >= FORMAT_VERSION_SOURCE_GENERATION
    }

    /// Load a segment from a file, memory-mapping it.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = std::fs::File::open(path)?;
        // SAFETY: memory-mapping is unsafe because the mapping aliases the file's
        // bytes and the borrow checker cannot prove the file is not mutated
        // underneath us. Reverse Rusty segment files are immutable once written
        // (segments are append-only and never edited in place; compaction writes
        // a new file and atomically swaps it), so the mapped region is effectively
        // read-only for the lifetime of this `Arc<Mmap>`.
        let mmap = Arc::new(unsafe { memmap2::Mmap::map(&file)? });

        if mmap.len() < HEADER_SIZE + 4 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "file too small"));
        }
        // Verify trailing CRC32
        {
            let content = &mmap[..mmap.len() - 4];
            let stored_crc = read_u32_at(&mmap, mmap.len() - 4)?;
            if crc32(content) != stored_crc {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("segment file CRC mismatch: {}", path.display()),
                ));
            }
        }
        // We need to parse the mmap contents to extract offsets and lengths,
        // then store raw pointers into the mmap. To satisfy the borrow checker
        // (we move `mmap` into the struct but store pointers derived from it),
        // we use a two-phase approach: parse with a temporary borrow to get
        // offsets/lengths, then construct pointers from the base after move.

        // Phase 1: validate and parse offsets/lengths from a temporary borrow
        let format_version = {
            let data = &mmap[..];
            if data[0..4] != MAGIC {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "bad magic"));
            }
            let version = read_u32_at(data, 4)?;
            // v1–v8 are supported (v1 reconstructs the reverse index; v1/v2 read
            // back with an empty tag column; v4 is the class-D fence; v5 adds
            // the hot index; v6 priority, v7 ownership, and v8 source generation
            // append cumulative exact-row columns).
            if !(1..=FORMAT_VERSION_SOURCE_GENERATION).contains(&version) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsupported format version {version}"),
                ));
            }
            version
        };

        // Phase 2: extract section layout using raw pointer arithmetic.
        // All pointers are derived from `base` which points into `mmap`.
        // After we move `mmap` into the struct, the backing memory doesn't move
        // (it's OS-mapped), so the pointers remain valid for the struct's lifetime.
        let base = mmap.as_ptr();
        let mmap_len = mmap.len();
        // SAFETY: `base`/`mmap_len` come straight from the live `mmap` (still owned
        // on the stack here), so the pointer is valid and aligned for `mmap_len`
        // bytes of `u8`. This borrow is read-only and dropped before `mmap` moves
        // into the struct.
        let data_for_parse = unsafe { std::slice::from_raw_parts(base, mmap_len) };

        let num_queries = read_u32_at(data_for_parse, 8)?;
        let exact_off = read_u64_at(data_for_parse, 16)? as usize;
        let main_off = read_u64_at(data_for_parse, 24)? as usize;
        let broad_off = read_u64_at(data_for_parse, 32)? as usize;
        let filter_off = read_u64_at(data_for_parse, 40)? as usize;
        let meta_off = read_u64_at(data_for_parse, 48)? as usize;

        // ---- Parse exact section ----
        let mut cursor = exact_off;
        let (req_mask_s, next) = read_u64_slice(data_for_parse, cursor)?;
        cursor = next;
        let (forb_mask_s, next) = read_u64_slice(data_for_parse, cursor)?;
        cursor = next;
        let (req_off_s, next) = read_u32_slice(data_for_parse, cursor)?;
        cursor = next;
        let (req_len_s, next) = read_u16_slice(data_for_parse, cursor)?;
        cursor = next;
        let (req_blob_s, next) = read_u32_slice(data_for_parse, cursor)?;
        cursor = next;
        let (forb_off_s, next) = read_u32_slice(data_for_parse, cursor)?;
        cursor = next;
        let (forb_len_s, next) = read_u16_slice(data_for_parse, cursor)?;
        cursor = next;
        let (forb_blob_s, next) = read_u32_slice(data_for_parse, cursor)?;
        cursor = next;
        let (q_group_start_s, next) = read_u32_slice(data_for_parse, cursor)?;
        cursor = next;
        let (q_group_count_s, next) = read_u16_slice(data_for_parse, cursor)?;
        cursor = next;
        let (group_off_s, next) = read_u32_slice(data_for_parse, cursor)?;
        cursor = next;
        let (group_len_s, next) = read_u16_slice(data_for_parse, cursor)?;
        cursor = next;
        let (anyof_blob_s, next) = read_u32_slice(data_for_parse, cursor)?;
        cursor = next;
        let (version_s, next) = read_u32_slice(data_for_parse, cursor)?;
        cursor = next;
        let (logical_s, after_logical) = read_u64_slice(data_for_parse, cursor)?;
        let (priority_s, priority_count, after_priority) = if format_version >= FORMAT_VERSION_RANK
        {
            let (raw, next) = read_u64_slice(data_for_parse, after_logical)?;
            if raw.len() != num_queries as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "segment priority column length mismatch",
                ));
            }
            // SAFETY: i64/u64 have identical size and alignment; every bit pattern
            // is valid for both, and the immutable slice remains mmap-borrowed.
            let signed =
                unsafe { std::slice::from_raw_parts(raw.as_ptr().cast::<i64>(), raw.len()) };
            (signed, signed.len(), next)
        } else {
            (&[][..], 0usize, after_logical)
        };
        let priority_ptr = if priority_count == 0 {
            std::ptr::NonNull::<i64>::dangling().as_ptr().cast_const()
        } else {
            priority_s.as_ptr()
        };

        let (
            placement_generation_s,
            placement_num_shards_s,
            placement_mode_s,
            placement_off_s,
            placement_len_s,
            placement_blob_s,
            after_placement,
        ) = if format_version >= FORMAT_VERSION_OWNERSHIP {
            let (generation, next) = read_u64_slice(data_for_parse, after_priority)?;
            let (num_shards, next) = read_u32_slice(data_for_parse, next)?;
            let (mode, next) = read_u8_slice(data_for_parse, next)?;
            let (off, next) = read_u32_slice(data_for_parse, next)?;
            let (len, next) = read_u32_slice(data_for_parse, next)?;
            let (blob, next) = read_u32_slice(data_for_parse, next)?;
            let n = num_queries as usize;
            if generation.len() != n
                || num_shards.len() != n
                || mode.len() != n
                || off.len() != n
                || len.len() != n
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "segment ownership column length mismatch",
                ));
            }
            for i in 0..n {
                let start = off[i] as usize;
                let count = len[i] as usize;
                let positions = blob.get(start..start + count).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "segment ownership positions overrun placement blob",
                    )
                })?;
                crate::ownership::QueryPlacement::from_raw(
                    crate::ownership::PlacementGeneration(generation[i]),
                    num_shards[i],
                    mode[i],
                    positions.to_vec(),
                )
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            }
            (generation, num_shards, mode, off, len, blob, next)
        } else {
            (
                &[][..],
                &[][..],
                &[][..],
                &[][..],
                &[][..],
                &[][..],
                after_priority,
            )
        };
        let placement_count = placement_generation_s.len();
        let source_generation_s = if format_version >= FORMAT_VERSION_SOURCE_GENERATION {
            let (generation, _) = read_u64_slice(data_for_parse, after_placement)?;
            if generation.len() != num_queries as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "segment source-generation column length mismatch",
                ));
            }
            generation
        } else {
            &[][..]
        };
        let source_generation_count = source_generation_s.len();

        // ---- Parse main index ----
        let (main_slots_s, main_blob_s, main_cap) = parse_frozen_index(data_for_parse, main_off)?;

        // ---- Parse broad index ----
        let (broad_slots_s, broad_blob_s, broad_cap) =
            parse_frozen_index(data_for_parse, broad_off)?;

        // ---- Parse hot-tier index (v5, ADR-105) ----
        // Pre-v5 files (and, defensively, a v5 header with a zero offset) have no
        // section: cap 0 + dangling pointers, the tag-column soundness pattern.
        let (hot_slots_s, hot_blob_s, hot_cap) = if format_version >= FORMAT_VERSION_HOT {
            let hoff = read_u64_at(data_for_parse, 72)? as usize;
            if hoff != 0 {
                parse_frozen_index(data_for_parse, hoff)?
            } else {
                (&[][..], &[][..], 0usize)
            }
        } else {
            (&[][..], &[][..], 0usize)
        };
        let hot_slots_ptr = if hot_cap != 0 {
            hot_slots_s.as_ptr()
        } else {
            std::ptr::NonNull::<FrozenSlot>::dangling()
                .as_ptr()
                .cast_const()
        };
        let hot_blob_ptr = if hot_blob_s.is_empty() {
            std::ptr::NonNull::<u32>::dangling().as_ptr().cast_const()
        } else {
            hot_blob_s.as_ptr()
        };

        // ---- Parse filter ----
        let filter_num_blocks = read_u32_at(data_for_parse, filter_off)? as usize;
        let filter_mask_val = read_u64_at(data_for_parse, filter_off + 8)?;
        let filter_data_off = filter_off + 16;
        let filter_data_ptr = if filter_num_blocks > 0 {
            // SAFETY: `filter_data_off` is an offset within the CRC-verified mmap
            // (derived from `filter_off`, itself read from the validated header),
            // so `base.add(filter_data_off)` stays in bounds of the mapping. The
            // result is only read back through `filter_data()`, which bounds it to
            // `filter_num_blocks * 8` u64s laid down by the writer.
            unsafe { base.add(filter_data_off).cast::<u64>() }
        } else {
            std::ptr::null()
        };

        // ---- Parse meta ----
        cursor = meta_off;
        let (class_s, next) = read_u8_slice(data_for_parse, cursor)?;
        cursor = next;
        let (alive_s, _) = read_u8_slice(data_for_parse, cursor)?;

        // Validate the class bytes against the version's ceiling ONCE at open
        // (class 4 = H exists only in v5 files; anything higher came from a
        // future build). A corrupt/foreign byte would otherwise be silently
        // mis-bucketed by `class_counts`/`to_memory_segment` — fail loud instead.
        let class_ceiling: u8 = if format_version >= FORMAT_VERSION_HOT {
            4
        } else {
            3
        };
        if let Some(bad) = class_s.iter().find(|&&b| b > class_ceiling) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "cost-class byte {bad} exceeds format v{format_version}'s ceiling {class_ceiling}: {}",
                    path.display()
                ),
            ));
        }

        // Build alive overlay from on-disk data; seed the dead set from the same
        // flags so it stays ≡ the overlay's dead entries from the start (ADR-066).
        let alive_overlay: Vec<bool> = alive_s.iter().map(|&b| b != 0).collect();
        let alive_counter = alive_overlay.iter().filter(|&&a| a).count();
        let dead_overlay: roaring::RoaringBitmap = alive_overlay
            .iter()
            .enumerate()
            .filter(|(_, &a)| !a)
            .map(|(i, _)| i as u32)
            .collect();

        // Reverse index (ADR-020 Item 2): v2 borrows the sorted columns straight
        // from the mmap (zero resident heap); v1 reconstructs them in RAM from
        // `logical_arr` (one logical id per local).
        let logical_index = if format_version >= 2 {
            let loff = read_u64_at(data_for_parse, 56)? as usize;
            let (li_logical_s, after) = read_u64_slice(data_for_parse, loff)?;
            let (li_local_s, _) = read_u32_slice(data_for_parse, after)?;
            if li_logical_s.len() != li_local_s.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "logical index column length mismatch",
                ));
            }
            MmapLogicalIndex::Mapped {
                logical: li_logical_s.as_ptr(),
                local: li_local_s.as_ptr(),
                count: li_logical_s.len(),
            }
        } else {
            let mut pairs: Vec<(u64, u32)> = logical_s
                .iter()
                .take(num_queries as usize)
                .enumerate()
                .map(|(i, &lid)| (lid, i as u32))
                .collect();
            pairs.sort_unstable();
            let logical = pairs.iter().map(|&(l, _)| l).collect();
            let local = pairs.iter().map(|&(_, c)| c).collect();
            MmapLogicalIndex::Owned { logical, local }
        };

        // Tag section (ADR-049): v3 borrows the SoA tag columns straight from the mmap;
        // v1/v2 have no section, so the columns read back empty (every query untagged).
        // A non-null dangling pointer keeps the empty-slice accessors sound.
        let (tag_off_s, tag_len_s, tag_blob_ptr, tag_blob_len, tag_count) = if format_version >= 3 {
            let toff = read_u64_at(data_for_parse, 64)? as usize;
            let (tag_off_s, after) = read_u32_slice(data_for_parse, toff)?;
            let (tag_len_s, after2) = read_u16_slice(data_for_parse, after)?;
            let (tag_blob_s, _) = read_u32_slice(data_for_parse, after2)?;
            if tag_off_s.len() != tag_len_s.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "tag column length mismatch",
                ));
            }
            (
                tag_off_s,
                tag_len_s,
                tag_blob_s.as_ptr(),
                tag_blob_s.len(),
                tag_off_s.len(),
            )
        } else {
            (
                &[][..],
                &[][..],
                std::ptr::NonNull::<u32>::dangling().as_ptr().cast_const(),
                0usize,
                0usize,
            )
        };
        let tag_off_ptr = if tag_count != 0 {
            tag_off_s.as_ptr()
        } else {
            std::ptr::NonNull::<u32>::dangling().as_ptr().cast_const()
        };
        let tag_len_ptr = if tag_count != 0 {
            tag_len_s.as_ptr()
        } else {
            std::ptr::NonNull::<u16>::dangling().as_ptr().cast_const()
        };

        // Cross-validate the per-query columns against their blobs once, here at open,
        // so the hot path stays branch-free. Turns an intra-section inconsistency (a
        // CRC-valid offset/length column overrunning its blob) into a fail-loud
        // `InvalidData` error instead of an out-of-bounds slice panic downstream.
        validate_columns(
            format_version,
            num_queries as usize,
            req_off_s,
            req_len_s,
            req_blob_s.len(),
            forb_off_s,
            forb_len_s,
            forb_blob_s.len(),
            q_group_start_s,
            q_group_count_s,
            group_off_s,
            group_len_s,
            anyof_blob_s.len(),
            tag_off_s,
            tag_len_s,
            tag_blob_len,
        )?;

        Ok(MmapSegment {
            format_version,
            mmap,
            num_queries,
            req_mask: req_mask_s.as_ptr(),
            forb_mask: forb_mask_s.as_ptr(),
            req_off: req_off_s.as_ptr(),
            req_len: req_len_s.as_ptr(),
            req_blob: req_blob_s.as_ptr(),
            req_blob_len: req_blob_s.len(),
            forb_off: forb_off_s.as_ptr(),
            forb_len: forb_len_s.as_ptr(),
            forb_blob: forb_blob_s.as_ptr(),
            forb_blob_len: forb_blob_s.len(),
            q_group_start: q_group_start_s.as_ptr(),
            q_group_count: q_group_count_s.as_ptr(),
            group_off: group_off_s.as_ptr(),
            group_off_len: group_off_s.len(),
            group_len: group_len_s.as_ptr(),
            anyof_blob: anyof_blob_s.as_ptr(),
            anyof_blob_len: anyof_blob_s.len(),
            tag_off: tag_off_ptr,
            tag_len: tag_len_ptr,
            tag_blob: tag_blob_ptr,
            tag_blob_len,
            tag_count,
            version_arr: version_s.as_ptr(),
            logical_arr: logical_s.as_ptr(),
            priority_arr: priority_ptr,
            priority_count,
            placement_generation: slice_ptr(placement_generation_s),
            placement_num_shards: slice_ptr(placement_num_shards_s),
            placement_mode: slice_ptr(placement_mode_s),
            placement_off: slice_ptr(placement_off_s),
            placement_len: slice_ptr(placement_len_s),
            placement_blob: slice_ptr(placement_blob_s),
            placement_blob_len: placement_blob_s.len(),
            placement_count,
            source_generation: slice_ptr(source_generation_s),
            source_generation_count,
            main_slots: main_slots_s.as_ptr(),
            main_cap,
            main_mask: if main_cap > 0 {
                (main_cap - 1) as u64
            } else {
                0
            },
            main_blob: main_blob_s.as_ptr(),
            main_blob_len: main_blob_s.len(),
            broad_slots: broad_slots_s.as_ptr(),
            broad_cap,
            broad_mask: if broad_cap > 0 {
                (broad_cap - 1) as u64
            } else {
                0
            },
            broad_blob: broad_blob_s.as_ptr(),
            broad_blob_len: broad_blob_s.len(),
            hot_slots: hot_slots_ptr,
            hot_cap,
            hot_mask: if hot_cap > 0 { (hot_cap - 1) as u64 } else { 0 },
            hot_blob: hot_blob_ptr,
            hot_blob_len: hot_blob_s.len(),
            filter_data: filter_data_ptr,
            filter_num_blocks,
            filter_mask: filter_mask_val,
            class_arr: class_s.as_ptr(),
            alive_overlay,
            alive_counter,
            dead_overlay,
            path: path.to_path_buf(),
            vocab_epoch: 0,
            logical_index,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Well-formed per-query columns for ONE untagged query against empty blobs:
    // one entry each, every offset/len landing inside a zero-length blob. Calls
    // `validate_columns` for that one query at `version` with the given tag columns.
    fn validate_one_query(version: u32, tag_off: &[u32], tag_len: &[u16]) -> io::Result<()> {
        let off1 = [0u32];
        let len1 = [0u16];
        validate_columns(
            version,
            1,
            &off1,
            &len1,
            0,
            &off1,
            &len1,
            0,
            &off1,
            &len1,
            &[],
            &[],
            0,
            tag_off,
            tag_len,
            0,
        )
    }

    /// A v3+ segment with `num_queries > 0` MUST carry a per-query tag column (the
    /// writer pushes one entry per query, length 0 when untagged). A zero-length tag
    /// column on such a file is corruption — e.g. a torn write that re-passes CRC —
    /// and must fail loud rather than silently read every query back as untagged
    /// (which would drop tagged queries from filtered percolation). Codex review.
    #[test]
    fn v3_with_queries_requires_tag_column() {
        let err = validate_one_query(3, &[], &[])
            .expect_err("v3 + queries + empty tag column must fail loud");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    /// The same empty tag column on a pre-tag v1/v2 file is legitimate (the section
    /// did not exist), so it must still open — back-compat is preserved.
    #[test]
    fn v2_with_queries_allows_empty_tag_column() {
        validate_one_query(2, &[], &[]).expect("v2 untagged column must still validate");
    }

    #[test]
    fn v6_rejects_priority_column_count_mismatch() {
        let path = std::env::temp_dir().join(format!(
            "reverse_rusty_bad_rank_column_{}.seg",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let norm = crate::normalize::Normalizer::default_vocab().expect("normalizer");
        let mut dict = crate::dict::Dict::new();
        let ast = crate::dsl::parse("topps chrome").expect("query");
        let mut lc = String::new();
        let ex = crate::compile::extract(&ast, &norm, &mut dict, &mut lc);
        dict.finalize_mask();
        let mut segment = crate::segment::Segment::new();
        segment
            .add_compiled_ranked(
                &ex,
                &[],
                &dict,
                1,
                1,
                crate::rank::RankValues { priority: 9 },
                crate::segment::CompileKnobs {
                    accept_class_d: false,
                    hot_anchor_threshold: 0,
                    dedup_bodies: true,
                },
            )
            .expect("accepted query");
        crate::storage::write_segment(&segment, &path).expect("write v6");

        let mut bytes = std::fs::read(&path).expect("segment bytes");
        let mut cursor = read_u64_at(&bytes, 16).expect("exact offset") as usize;
        for kind in [8u8, 8, 4, 2, 4, 4, 2, 4, 4, 2, 4, 2, 4, 4, 8] {
            cursor = match kind {
                8 => read_u64_slice(&bytes, cursor).expect("u64 column").1,
                4 => read_u32_slice(&bytes, cursor).expect("u32 column").1,
                2 => read_u16_slice(&bytes, cursor).expect("u16 column").1,
                _ => unreachable!(),
            };
        }
        // `cursor` is the appended priority array's count word. Keep the file
        // CRC-valid so open reaches the structural count validation.
        bytes[cursor..cursor + 4].copy_from_slice(&0u32.to_le_bytes());
        let n = bytes.len();
        let crc = crate::storage::crc32(&bytes[..n - 4]);
        bytes[n - 4..].copy_from_slice(&crc.to_le_bytes());
        std::fs::write(&path, bytes).expect("rewrite malformed segment");

        let error = MmapSegment::open(&path).expect_err("rank count mismatch must fail loud");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("priority column length"));
        let _ = std::fs::remove_file(path);
    }
}
