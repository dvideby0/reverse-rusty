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
    FrozenSlot, FORMAT_VERSION_COMPOUND_PREDICATE, FORMAT_VERSION_HOT, FORMAT_VERSION_OWNERSHIP,
    FORMAT_VERSION_PHRASE_PREDICATE, FORMAT_VERSION_RANK, FORMAT_VERSION_SOURCE_GENERATION,
    HEADER_SIZE, MAGIC,
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
    /// The file's header format version (1..=10). v4 ⇔ the segment holds class-D
    /// always-candidates (the ADR-068 rollback fence); v5 ⇔ it holds class-H
    /// hot-tier entries (the ADR-105 fence + the hot-index section) — surfaced so
    /// the manifest commit can propagate the fence to its own version word.
    format_version: u32,
    /// AST→compiled-query lowering semantics baked into this segment. Zero and
    /// one are the pre-ADR-118 and pre-ADR-119 lowerings; current writers stamp
    /// [`CURRENT_COMPILER_SEMANTICS_VERSION`].
    compiler_semantics_version: u32,
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
    // Optional v9/v10 compound exact-predicate columns.
    predicate_off: *const u32,
    predicate_len: *const u32,
    predicate_blob: *const u32,
    predicate_blob_len: usize,
    predicate_count: usize,
    /// Live rows carrying a positioned predicate. The mmap payload is
    /// append-only, so tombstones maintain this separately from its programs.
    live_phrase_predicates: usize,
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
    predicate_off: &[u32],
    predicate_len: &[u32],
    predicate_blob: &[u32],
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
    let predicates_expected =
        format_version >= FORMAT_VERSION_COMPOUND_PREDICATE && num_queries > 0;
    if predicates_expected
        && (predicate_off.len() != num_queries || predicate_len.len() != num_queries)
    {
        return Err(invalid("segment compound-predicate column length mismatch"));
    }

    // Each `off + len` must land inside its blob; `as usize` widens u32/u16 so the
    // add cannot wrap on a 64-bit target.
    let fits =
        |off: u32, len: u16, blob_len: usize| -> bool { off as usize + len as usize <= blob_len };

    let mut saw_phrase_predicate = false;
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
        if predicates_expected {
            let start = predicate_off[i] as usize;
            let end = start
                .checked_add(predicate_len[i] as usize)
                .ok_or_else(|| invalid("segment compound predicate window overflows usize"))?;
            let program = predicate_blob
                .get(start..end)
                .ok_or_else(|| invalid("segment compound predicate overruns blob"))?;
            crate::exact::validate_predicate(program)
                .map_err(|message| io::Error::new(io::ErrorKind::InvalidData, message))?;
            if crate::exact::predicate_has_phrases(program) {
                if format_version < FORMAT_VERSION_PHRASE_PREDICATE {
                    return Err(invalid(
                        "quoted predicate program requires segment format v10",
                    ));
                }
                saw_phrase_predicate = true;
            }
        }
    }
    if format_version >= FORMAT_VERSION_PHRASE_PREDICATE
        && num_queries != 0
        && !saw_phrase_predicate
    {
        return Err(invalid(
            "segment format v10 has no quoted predicate program",
        ));
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
            compiler_semantics_version: self.compiler_semantics_version,
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
            predicate_off: self.predicate_off,
            predicate_len: self.predicate_len,
            predicate_blob: self.predicate_blob,
            predicate_blob_len: self.predicate_blob_len,
            predicate_count: self.predicate_count,
            live_phrase_predicates: self.live_phrase_predicates,
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

mod open;

#[cfg(test)]
mod tests;
