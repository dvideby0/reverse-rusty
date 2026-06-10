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
use super::{FrozenSlot, FORMAT_VERSION_CLASS_D, HEADER_SIZE, MAGIC};

mod ops;

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
    /// The file's header format version (1..=4). v4 ⇔ the segment holds class-D
    /// always-candidates (the ADR-068 rollback fence) — surfaced so the manifest
    /// commit can propagate the fence to its own version word.
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
        self.format_version >= FORMAT_VERSION_CLASS_D
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
            // v1–v4 are all supported (v1 reconstructs the reverse index; v1/v2 read
            // back with an empty tag column; v4 is layout-identical to v3 — the bump
            // is the class-D rollback fence, ADR-068).
            if !(1..=FORMAT_VERSION_CLASS_D).contains(&version) {
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
        let (logical_s, _) = read_u64_slice(data_for_parse, cursor)?;

        // ---- Parse main index ----
        let (main_slots_s, main_blob_s, main_cap) = parse_frozen_index(data_for_parse, main_off)?;

        // ---- Parse broad index ----
        let (broad_slots_s, broad_blob_s, broad_cap) =
            parse_frozen_index(data_for_parse, broad_off)?;

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
        let (tag_off_ptr, tag_len_ptr, tag_blob_ptr, tag_blob_len, tag_count) =
            if format_version >= 3 {
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
                    tag_off_s.as_ptr(),
                    tag_len_s.as_ptr(),
                    tag_blob_s.as_ptr(),
                    tag_blob_s.len(),
                    tag_off_s.len(),
                )
            } else {
                (
                    std::ptr::NonNull::<u32>::dangling().as_ptr().cast_const(),
                    std::ptr::NonNull::<u16>::dangling().as_ptr().cast_const(),
                    std::ptr::NonNull::<u32>::dangling().as_ptr().cast_const(),
                    0usize,
                    0usize,
                )
            };

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
