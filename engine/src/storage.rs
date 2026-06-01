//! Segment file format — binary serialization and mmap-backed deserialization.
//!
//! Design: docs/DECISIONS.md (ADR-012)
//! Invariant: A written segment file, when mmap'd back, produces identical match
//!   results to the in-memory Segment it was serialized from
//! Hot path: MmapSegment::match_into is on the hot path (same role as Segment::match_into)
//!
//! ## File layout
//!
//! ```text
//! [FileHeader]              80 bytes — magic, version, counts, section offsets
//! [ExactStore arrays]       variable — flat SoA arrays, each prefixed with element count
//! [Main CandidateIndex]     variable — frozen open-addressing hash table + posting blob
//! [Broad CandidateIndex]    variable — same layout as main
//! [SegmentFilter]           variable — bloom filter data
//! [Metadata]                variable — cost classes + alive flags
//! ```
//!
//! All multi-byte values are little-endian. Arrays are padded to 8-byte alignment
//! between sections so mmap'd slices can be cast directly to typed pointers.

use std::fs::File;
use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

use crate::compile::CostClass;
use crate::dict::FeatureId;
use crate::index::CandidateIndex;
use crate::segment::{MatchStats, Segment};

// ---- constants ----

const MAGIC: [u8; 4] = *b"PERC";
// v1: original layout. v2 (ADR-020 Item 2): adds a sorted logical-index column
// section (logical_index_off at header bytes 56..64); v1 files are still read
// (the reverse index is reconstructed in memory on open).
const FORMAT_VERSION: u32 = 2;
const HEADER_SIZE: usize = 80;

// Section offset positions within the header (byte offset from file start).
// Header layout:
//   0..4    magic
//   4..8    format_version (u32 LE)
//   8..12   num_queries (u32 LE)
//   12..16  reserved (u32)
//   16..24  exact_section_off (u64 LE)
//   24..32  main_index_off (u64 LE)
//   32..40  broad_index_off (u64 LE)
//   40..48  filter_off (u64 LE)
//   48..56  meta_off (u64 LE)
//   56..64  logical_index_off (u64 LE; 0 or absent in v1)
//   64..80  reserved (16 bytes, zeroed)

// ---- CRC-32 (IEEE / ISO 3309) ----

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

// ---- frozen hash table for on-disk CandidateIndex ----

/// One slot in the frozen open-addressing hash table.
/// key=0 is the empty sentinel (sig_key output is astronomically unlikely to be 0).
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct FrozenSlot {
    key: u64,
    offset: u32, // byte offset into the posting blob (in u32 units)
    len: u32,    // number of u32 posting IDs
}

/// Build a frozen hash table + posting blob from an in-memory CandidateIndex.
/// Returns (slots, posting_blob).
fn freeze_index(index: &CandidateIndex) -> (Vec<FrozenSlot>, Vec<u32>) {
    let n = index.num_signatures();
    if n == 0 {
        return (vec![FrozenSlot::default(); 1], Vec::new());
    }
    // Capacity: next power of 2 >= 2*n (load factor ~50%)
    let cap = (n * 2).next_power_of_two().max(4);
    let mask = (cap - 1) as u64;
    let mut slots = vec![FrozenSlot::default(); cap];
    let mut blob = Vec::new();

    index.for_each_posting(|key, posting| {
        let offset = blob.len() as u32;
        // Flatten posting IDs into the blob
        posting.for_each(|id| blob.push(id));
        let len = blob.len() as u32 - offset;
        // Insert into hash table with linear probing
        let mut idx = key & mask;
        loop {
            let slot = &mut slots[idx as usize];
            if slot.key == 0 {
                *slot = FrozenSlot { key, offset, len };
                break;
            }
            idx = (idx + 1) & mask;
        }
    });

    (slots, blob)
}

/// Probe a frozen hash table for a signature key, returning the posting slice.
#[inline]
fn frozen_probe<'a>(
    key: u64,
    slots: &[FrozenSlot],
    blob: &'a [u32],
    mask: u64,
) -> Option<&'a [u32]> {
    let cap = slots.len();
    if cap == 0 {
        return None;
    }
    let mut idx = (key & mask) as usize;
    for _ in 0..cap {
        let slot = slots.get(idx)?;
        if slot.key == key {
            let start = slot.offset as usize;
            let end = start + slot.len as usize;
            return blob.get(start..end);
        }
        if slot.key == 0 {
            return None;
        }
        idx = (idx + 1) & (mask as usize);
    }
    None
}

// ---- low-level I/O helpers ----

fn write_u32(w: &mut impl Write, v: u32) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

fn write_u64(w: &mut impl Write, v: u64) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
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

/// Align a byte offset up to the next 8-byte boundary.
fn align8(pos: u64) -> u64 {
    (pos + 7) & !7
}

/// Write padding bytes to align to 8 bytes.
fn pad_to_8(w: &mut (impl Write + Seek)) -> io::Result<()> {
    let pos = w.stream_position()?;
    let aligned = align8(pos);
    let pad = (aligned - pos) as usize;
    if pad > 0 {
        w.write_all(&[0u8; 8][..pad])?;
    }
    Ok(())
}

/// Write a slice of u32 values: [count: u32, data: [u32; count], pad_to_8].
fn write_u32_array(w: &mut (impl Write + Seek), data: &[u32]) -> io::Result<()> {
    write_u32(w, data.len() as u32)?;
    // SAFETY: a `&[u32]` can always be viewed as `len * 4` bytes — every bit
    // pattern is a valid `u8`, the view aliases the same memory read-only, and
    // its lifetime is bound to `data`.
    let bytes = unsafe { std::slice::from_raw_parts(data.as_ptr().cast::<u8>(), data.len() * 4) };
    w.write_all(bytes)?;
    pad_to_8(w)
}

/// Write a slice of u16 values: [count: u32, data: [u16; count], pad_to_8].
fn write_u16_array(w: &mut (impl Write + Seek), data: &[u16]) -> io::Result<()> {
    write_u32(w, data.len() as u32)?;
    // SAFETY: viewing a `&[u16]` as `len * 2` bytes is always valid (every bit
    // pattern is a valid `u8`); the read-only view's lifetime is bound to `data`.
    let bytes = unsafe { std::slice::from_raw_parts(data.as_ptr().cast::<u8>(), data.len() * 2) };
    w.write_all(bytes)?;
    pad_to_8(w)
}

/// Write a slice of u64 values: [count: u32, pad(4), data: [u64; count]].
/// Already 8-byte aligned after data (u64 elements).
fn write_u64_array(w: &mut (impl Write + Seek), data: &[u64]) -> io::Result<()> {
    write_u32(w, data.len() as u32)?;
    // pad count to 8 bytes
    w.write_all(&[0u8; 4])?;
    // SAFETY: viewing a `&[u64]` as `len * 8` bytes is always valid (every bit
    // pattern is a valid `u8`); the read-only view's lifetime is bound to `data`.
    let bytes = unsafe { std::slice::from_raw_parts(data.as_ptr().cast::<u8>(), data.len() * 8) };
    w.write_all(bytes)?;
    Ok(())
}

/// Write a slice of u8 values: [count: u32, data: [u8; count], pad_to_8].
fn write_u8_array(w: &mut (impl Write + Seek), data: &[u8]) -> io::Result<()> {
    write_u32(w, data.len() as u32)?;
    w.write_all(data)?;
    pad_to_8(w)
}

// ---- reading helpers (from mmap'd bytes) ----

/// Read a u32-element array: [count: u32, data...]. Returns (slice, next_offset).
/// The slice is cast from the raw bytes (requires alignment — guaranteed by pad_to_8).
fn read_u32_slice(data: &[u8], off: usize) -> io::Result<(&[u32], usize)> {
    let count = read_u32_at(data, off)? as usize;
    let data_off = off + 4;
    // SAFETY: `count` was read from `data`, which this crate only passes after
    // verifying the segment's trailing CRC32 (see `MmapSegment::open`), so the
    // `count` u32s are present and in-bounds. `off` is 8-aligned and the mmap
    // base is page-aligned, so `data_off` meets `u32`'s alignment. The slice
    // borrows `data`.
    let slice =
        unsafe { std::slice::from_raw_parts(data.as_ptr().add(data_off).cast::<u32>(), count) };
    let end = align8((data_off + count * 4) as u64) as usize;
    Ok((slice, end))
}

/// Read a u16-element array.
fn read_u16_slice(data: &[u8], off: usize) -> io::Result<(&[u16], usize)> {
    let count = read_u32_at(data, off)? as usize;
    let data_off = off + 4;
    // SAFETY: `count` was read from CRC-verified `data` (see `MmapSegment::open`),
    // so the `count` u16s are present and in-bounds. `off` is 8-aligned and the
    // mmap base is page-aligned, so `data_off` (= off + 4) meets `u16`'s
    // 2-byte alignment. The slice borrows `data`.
    let slice =
        unsafe { std::slice::from_raw_parts(data.as_ptr().add(data_off).cast::<u16>(), count) };
    let end = align8((data_off + count * 2) as u64) as usize;
    Ok((slice, end))
}

/// Read a u64-element array: [count: u32, pad(4), data...].
fn read_u64_slice(data: &[u8], off: usize) -> io::Result<(&[u64], usize)> {
    let count = read_u32_at(data, off)? as usize;
    // 4 count + 4 pad
    let data_off = off + 8;
    // SAFETY: `count` was read from CRC-verified `data` (see `MmapSegment::open`),
    // so the `count` u64s are present and in-bounds. `off` is 8-aligned and the
    // mmap base is page-aligned, so `data_off` (= off + 8) meets `u64`'s 8-byte
    // alignment. The slice borrows `data`.
    let slice =
        unsafe { std::slice::from_raw_parts(data.as_ptr().add(data_off).cast::<u64>(), count) };
    let end = data_off + count * 8; // already aligned
    Ok((slice, end))
}

/// Read a u8-element array.
fn read_u8_slice(data: &[u8], off: usize) -> io::Result<(&[u8], usize)> {
    let count = read_u32_at(data, off)? as usize;
    let data_off = off + 4;
    let slice = &data[data_off..data_off + count];
    let end = align8((data_off + count) as u64) as usize;
    Ok((slice, end))
}

// ---- segment write ----

/// Write a sealed Segment to a file. Uses atomic write (tmp + rename) for safety.
pub fn write_segment(seg: &Segment, path: &Path) -> io::Result<()> {
    let tmp_path = path.with_extension("seg.tmp");
    let mut f = std::fs::File::create(&tmp_path)?;

    // Reserve space for header (will fill in section offsets at the end)
    f.write_all(&[0u8; HEADER_SIZE])?;

    // ---- Exact section ----
    pad_to_8(&mut f)?;
    let exact_off = f.stream_position()?;
    write_exact_section(&mut f, seg)?;

    // ---- Main index ----
    pad_to_8(&mut f)?;
    let main_off = f.stream_position()?;
    write_frozen_index_section(&mut f, seg.main_index())?;

    // ---- Broad index ----
    pad_to_8(&mut f)?;
    let broad_off = f.stream_position()?;
    write_frozen_index_section(&mut f, seg.broad_index())?;

    // ---- Filter ----
    pad_to_8(&mut f)?;
    let filter_off = f.stream_position()?;
    write_filter_section(&mut f, seg)?;

    // ---- Meta (class + alive) ----
    pad_to_8(&mut f)?;
    let meta_off = f.stream_position()?;
    write_meta_section(&mut f, seg)?;

    // ---- Logical index columns (sorted reverse index; ADR-020 Item 2) ----
    // Two parallel sorted arrays so a reader can binary-search a logical id and
    // return its contiguous local-id run, without rebuilding a resident map.
    pad_to_8(&mut f)?;
    let logical_off = f.stream_position()?;
    let (li_logical, li_local) = seg.logical_columns();
    write_u64_array(&mut f, &li_logical)?;
    write_u32_array(&mut f, &li_local)?;

    // ---- Write header ----
    f.seek(SeekFrom::Start(0))?;
    f.write_all(&MAGIC)?;
    write_u32(&mut f, FORMAT_VERSION)?;
    write_u32(&mut f, seg.len() as u32)?;
    write_u32(&mut f, 0)?; // reserved
    write_u64(&mut f, exact_off)?;
    write_u64(&mut f, main_off)?;
    write_u64(&mut f, broad_off)?;
    write_u64(&mut f, filter_off)?;
    write_u64(&mut f, meta_off)?;
    write_u64(&mut f, logical_off)?;
    // remaining header bytes are already zero (reserved)

    // Compute CRC32 of the entire file and append it as the trailing 4 bytes
    f.sync_all()?;
    drop(f);
    let content = std::fs::read(&tmp_path)?;
    let file_crc = crc32(&content);
    let mut f = std::fs::OpenOptions::new().append(true).open(&tmp_path)?;
    write_u32(&mut f, file_crc)?;
    f.sync_all()?;
    drop(f);
    durable_rename(&tmp_path, path)?;
    Ok(())
}

/// Write the ExactStore arrays from a Segment. Accesses internal state through
/// the public accessor methods we'll add to ExactStore.
fn write_exact_section(w: &mut (impl Write + Seek), seg: &Segment) -> io::Result<()> {
    let exact = seg.exact_store();
    write_u64_array(w, exact.req_masks())?;
    write_u64_array(w, exact.forb_masks())?;
    write_u32_array(w, exact.req_offs())?;
    write_u16_array(w, exact.req_lens())?;
    write_u32_array(w, exact.req_blobs())?;
    write_u32_array(w, exact.forb_offs())?;
    write_u16_array(w, exact.forb_lens())?;
    write_u32_array(w, exact.forb_blobs())?;
    write_u32_array(w, exact.q_group_starts())?;
    write_u16_array(w, exact.q_group_counts())?;
    write_u32_array(w, exact.group_offs())?;
    write_u16_array(w, exact.group_lens())?;
    write_u32_array(w, exact.anyof_blobs())?;
    write_u32_array(w, exact.versions())?;
    write_u64_array(w, exact.logicals())?;
    Ok(())
}

fn write_frozen_index_section(
    w: &mut (impl Write + Seek),
    index: &CandidateIndex,
) -> io::Result<()> {
    let (slots, blob) = freeze_index(index);
    // Write slots as a u64-aligned array (each slot is 16 bytes = 2 u64s)
    let cap = slots.len();
    write_u32(w, cap as u32)?;
    // pad to 8
    w.write_all(&[0u8; 4])?;
    // SAFETY: `FrozenSlot` is `#[repr(C)]` and padding-free ({u64,u32,u32} = 16
    // bytes), so a `&[FrozenSlot]` of `cap` elements can be viewed as
    // `cap * size_of::<FrozenSlot>()` initialized bytes; the read-only view's
    // lifetime is bound to `slots`.
    let slot_bytes = unsafe {
        std::slice::from_raw_parts(
            slots.as_ptr().cast::<u8>(),
            cap * std::mem::size_of::<FrozenSlot>(),
        )
    };
    w.write_all(slot_bytes)?;
    pad_to_8(w)?;
    // Write posting blob
    write_u32_array(w, &blob)?;
    Ok(())
}

fn write_filter_section(w: &mut (impl Write + Seek), seg: &Segment) -> io::Result<()> {
    if let Some(filter) = seg.filter_ref() {
        write_u32(w, filter.num_blocks_raw() as u32)?;
        w.write_all(&[0u8; 4])?; // pad
        write_u64(w, filter.mask_raw())?;
        let data = filter.data_raw();
        // SAFETY: viewing a `&[u64]` as `len * 8` bytes is always valid (every
        // bit pattern is a valid `u8`); the read-only view borrows `data`.
        let bytes =
            unsafe { std::slice::from_raw_parts(data.as_ptr().cast::<u8>(), data.len() * 8) };
        w.write_all(bytes)?;
    } else {
        write_u32(w, 0)?; // no filter
        w.write_all(&[0u8; 4])?;
        write_u64(w, 0)?;
    }
    Ok(())
}

fn write_meta_section(w: &mut (impl Write + Seek), seg: &Segment) -> io::Result<()> {
    let classes: Vec<u8> = seg
        .classes()
        .iter()
        .map(|c| match c {
            CostClass::A => 0,
            CostClass::B => 1,
            CostClass::C => 2,
            CostClass::D => 3,
        })
        .collect();
    write_u8_array(w, &classes)?;
    let alive: Vec<u8> = seg.alive_flags().iter().map(|&a| u8::from(a)).collect();
    write_u8_array(w, &alive)?;
    Ok(())
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
            // v1 and v2 are both supported (v1 reconstructs the reverse index).
            if version != 1 && version != FORMAT_VERSION {
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

        // Build alive overlay from on-disk data.
        let alive_overlay: Vec<bool> = alive_s.iter().map(|&b| b != 0).collect();
        let alive_counter = alive_overlay.iter().filter(|&&a| a).count();

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

        Ok(MmapSegment {
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
            path: path.to_path_buf(),
            vocab_epoch: 0,
            logical_index,
        })
    }

    // ---- slice accessors (zero-cost, just pointer arithmetic) ----

    /// View `len` elements of `T` at `ptr` as a slice borrowed from `&self`.
    ///
    /// Every section accessor below funnels through this one helper so the
    /// pointer-to-slice reconstruction has a single audited `unsafe` site.
    ///
    /// # The invariant that makes every caller sound
    ///
    /// All `(ptr, len)` pairs are the ones captured in [`MmapSegment::open`] from
    /// the mmap that `self` owns. At that point:
    /// * the mapping was fully validated — trailing CRC32 over the file body, plus
    ///   magic bytes and format version — before any pointer was taken, so the
    ///   bytes are exactly what the writer produced and `len` matches the section;
    /// * the writer pads every section to an 8-byte boundary, and the element
    ///   types used here (`u64`/`u32`/`u16`/`FrozenSlot`) all have alignment
    ///   dividing 8, so `ptr` is properly aligned;
    /// * `self` owns the backing `Arc<Mmap>`, which is immutable and never moves,
    ///   and it outlives the returned borrow, so the slice can neither dangle nor
    ///   be mutated underneath the reader.
    ///
    /// Callers must therefore only pass pointer/length pairs originating from
    /// `open`'s validated parse (never a null pointer — see `filter_data`).
    // `&self` is load-bearing: it ties the returned slice's lifetime to the mmap
    // owner so the borrow checker forbids use-after-unmap. clippy can't see that
    // the body's safety contract depends on the borrow.
    #[allow(clippy::unused_self)]
    #[inline]
    fn mmap_slice<T>(&self, ptr: *const T, len: usize) -> &[T] {
        // SAFETY: upheld by the construction invariant documented above — `ptr`
        // references `len` correctly-aligned, initialized `T`s inside the live,
        // immutable mmap owned by `self`.
        unsafe { std::slice::from_raw_parts(ptr, len) }
    }

    #[inline]
    fn req_mask(&self) -> &[u64] {
        self.mmap_slice(self.req_mask, self.num_queries as usize)
    }
    #[inline]
    fn forb_mask(&self) -> &[u64] {
        self.mmap_slice(self.forb_mask, self.num_queries as usize)
    }
    #[inline]
    fn req_off(&self) -> &[u32] {
        self.mmap_slice(self.req_off, self.num_queries as usize)
    }
    #[inline]
    fn req_len(&self) -> &[u16] {
        self.mmap_slice(self.req_len, self.num_queries as usize)
    }
    #[inline]
    fn req_blob(&self) -> &[u32] {
        self.mmap_slice(self.req_blob, self.req_blob_len)
    }
    #[inline]
    fn forb_off(&self) -> &[u32] {
        self.mmap_slice(self.forb_off, self.num_queries as usize)
    }
    #[inline]
    fn forb_len(&self) -> &[u16] {
        self.mmap_slice(self.forb_len, self.num_queries as usize)
    }
    #[inline]
    fn forb_blob(&self) -> &[u32] {
        self.mmap_slice(self.forb_blob, self.forb_blob_len)
    }
    #[inline]
    fn q_group_start(&self) -> &[u32] {
        self.mmap_slice(self.q_group_start, self.num_queries as usize)
    }
    #[inline]
    fn q_group_count(&self) -> &[u16] {
        self.mmap_slice(self.q_group_count, self.num_queries as usize)
    }
    #[inline]
    fn group_off(&self) -> &[u32] {
        self.mmap_slice(self.group_off, self.group_off_len)
    }
    #[inline]
    fn group_len(&self) -> &[u16] {
        self.mmap_slice(self.group_len, self.group_off_len)
    }
    #[inline]
    fn anyof_blob(&self) -> &[u32] {
        self.mmap_slice(self.anyof_blob, self.anyof_blob_len)
    }

    #[inline]
    fn main_slots(&self) -> &[FrozenSlot] {
        self.mmap_slice(self.main_slots, self.main_cap)
    }
    #[inline]
    fn main_blob(&self) -> &[u32] {
        self.mmap_slice(self.main_blob, self.main_blob_len)
    }
    #[inline]
    fn broad_slots(&self) -> &[FrozenSlot] {
        self.mmap_slice(self.broad_slots, self.broad_cap)
    }
    #[inline]
    fn broad_blob(&self) -> &[u32] {
        self.mmap_slice(self.broad_blob, self.broad_blob_len)
    }

    #[inline]
    fn filter_data(&self) -> &[u64] {
        // Guard the null sentinel: a segment with no filter stores a null
        // `filter_data` pointer, which `mmap_slice`/`from_raw_parts` forbid.
        if self.filter_num_blocks == 0 {
            return &[];
        }
        self.mmap_slice(self.filter_data, self.filter_num_blocks * 8)
    }

    // ---- public interface ----

    pub fn len(&self) -> usize {
        self.num_queries as usize
    }

    pub fn is_empty(&self) -> bool {
        self.num_queries == 0
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn tombstone(&mut self, local_id: u32) {
        if let Some(slot) = self.alive_overlay.get_mut(local_id as usize) {
            if *slot {
                self.alive_counter -= 1;
            }
            *slot = false;
        }
    }

    /// The sorted `logical_id` column (borrowed from the mmap for v2, owned for v1).
    #[inline]
    fn li_logical(&self) -> &[u64] {
        match &self.logical_index {
            MmapLogicalIndex::Mapped { logical, count, .. } => self.mmap_slice(*logical, *count),
            MmapLogicalIndex::Owned { logical, .. } => logical,
        }
    }
    /// The parallel `local_id` column.
    #[inline]
    fn li_local(&self) -> &[u32] {
        match &self.logical_index {
            MmapLogicalIndex::Mapped { local, count, .. } => self.mmap_slice(*local, *count),
            MmapLogicalIndex::Owned { local, .. } => local,
        }
    }

    pub fn locals_for_logical(&self, logical_id: u64) -> &[u32] {
        // Columns are sorted by (logical_id, local_id), so a logical id's local
        // ids form a contiguous run — binary-search its bounds and slice.
        let logs = self.li_logical();
        let lo = logs.partition_point(|&l| l < logical_id);
        let hi = logs.partition_point(|&l| l <= logical_id);
        &self.li_local()[lo..hi]
    }

    /// Number of alive (non-tombstoned) entries (O(1)).
    pub fn alive_count(&self) -> usize {
        self.alive_counter
    }

    /// Tally entries by cost class into `c` (`[A, B, C, D]`), reading the persisted
    /// per-entry class bytes. Counts ALL entries (including tombstoned), matching
    /// [`Segment::class_counts`](crate::segment::Segment::class_counts) so introspection
    /// is identical whether a segment is in-memory or mmap'd (the latter is what a
    /// reopened durable cluster attaches — ADR-032).
    pub fn class_counts(&self, c: &mut [u64; 4]) {
        let n = self.len();
        for i in 0..n {
            // SAFETY: `i < n == num_queries`, the length of the `class_arr` byte array
            // parsed from the mmap (same bound `to_memory_segment` uses).
            let class_byte = unsafe { *self.class_arr.add(i) };
            c[(class_byte as usize).min(3)] += 1;
        }
    }

    pub fn holes_ratio(&self) -> f64 {
        let total = self.len();
        if total == 0 {
            return 0.0;
        }
        1.0 - (self.alive_count() as f64 / total as f64)
    }

    /// Resident heap bytes used by the logical→local reverse index. The SoA and
    /// candidate index are mmap'd (file-backed, paged), but this reverse index is
    /// rebuilt resident at `open` — a `Vec` per logical id — so it is a real
    /// resident cost the file-backed accounting misses.
    pub fn logical_index_bytes(&self) -> usize {
        match &self.logical_index {
            // v2 columns live in the mmap (file-backed/paged) — ~zero resident heap.
            MmapLogicalIndex::Mapped { .. } => 0,
            // v1 reconstruct holds flat owned columns (12 B/query, vs the old
            // per-logical Vec map) until the segment is recompacted to v2.
            MmapLogicalIndex::Owned { logical, local } => {
                logical.capacity() * std::mem::size_of::<u64>()
                    + local.capacity() * std::mem::size_of::<u32>()
            }
        }
    }

    /// Resident heap bytes used by the mutable alive overlay (tombstones). This
    /// stays in RAM even for an mmap'd segment because the mapping is read-only.
    pub fn alive_bytes(&self) -> usize {
        self.alive_overlay.capacity() * std::mem::size_of::<bool>()
    }

    #[inline]
    pub(crate) fn logical(&self, id: u32) -> u64 {
        // SAFETY: `logical_arr` is the `num_queries`-long u64 array parsed from the
        // mmap in `open`. Callers only pass local ids `< num_queries` (they come
        // from posting lists built over this segment's own entries), so the offset
        // is in bounds of the immutable mapping `self` owns.
        unsafe { *self.logical_arr.add(id as usize) }
    }

    /// Integer-only exact verification — same logic as ExactStore::verify but
    /// operating on mmap'd slices.
    #[inline]
    pub fn verify(&self, id: u32, tmask: u64, tfeats: &[FeatureId]) -> bool {
        crate::exact::verify_slices(
            id,
            tmask,
            tfeats,
            self.req_mask(),
            self.forb_mask(),
            self.req_off(),
            self.req_len(),
            self.req_blob(),
            self.forb_off(),
            self.forb_len(),
            self.forb_blob(),
            self.q_group_start(),
            self.q_group_count(),
            self.group_off(),
            self.group_len(),
            self.anyof_blob(),
        )
    }

    // ---- broad-lane batch evaluation surface (mmap twin of the in-memory
    // `Segment` accessors used by `segment::broad_batch`). Lets the columnar
    // broad evaluator drive mmap and in-memory segments through one body. ----

    /// Probe the broad frozen table for `key` (after the anchor-filter check),
    /// appending reachable local IDs to `cands` (epoch-deduped via `seen`). The
    /// reachability primitive for the batch broad lane — mirrors the broad block
    /// of `match_into` (filter gate + probe) so the columnar path skips the same
    /// probes the per-title path would.
    #[inline]
    pub(crate) fn broad_reach(
        &self,
        key: u64,
        epoch: u32,
        seen: &mut [u32],
        cands: &mut Vec<u32>,
        stats: &mut MatchStats,
    ) {
        stats.probes_attempted += 1;
        if self.filter_num_blocks > 0 && !self.may_contain(key) {
            stats.probes_skipped += 1;
            return;
        }
        if let Some(posting) =
            frozen_probe(key, self.broad_slots(), self.broad_blob(), self.broad_mask)
        {
            stats.postings_scanned += posting.len() as u32;
            stats.broad_postings_scanned += posting.len() as u32;
            for &local in posting {
                if seen[local as usize] != epoch {
                    seen[local as usize] = epoch;
                    cands.push(local);
                }
            }
        }
    }

    /// Liveness for one local ID (mmap tombstone overlay).
    #[inline]
    pub(crate) fn is_alive_at(&self, local: u32) -> bool {
        self.alive_overlay[local as usize]
    }

    /// Whether `local`'s entire semantics is its hot anchor — the pure-anchor
    /// skip-verify fast path. Mmap twin of [`crate::exact::ExactStore::is_pure_anchor`].
    #[inline]
    pub(crate) fn is_pure_anchor(&self, local: u32) -> bool {
        let i = local as usize;
        self.req_len()[i] == 0
            && self.forb_mask()[i] == 0
            && self.forb_len()[i] == 0
            && self.q_group_count()[i] == 0
            && self.req_mask()[i].is_power_of_two()
    }

    /// Columnar batch verification for one query against a title batch, writing
    /// the matching-title bitmap into `acc`. Mmap twin of
    /// [`crate::exact::ExactStore::eval_batch`]; shares
    /// [`crate::exact::eval_batch_slices`] so the in-memory and mmap broad-batch
    /// paths cannot drift.
    #[inline]
    pub(crate) fn eval_batch<'a>(
        &self,
        local: u32,
        tmask_batch: &[u64],
        lookup: impl Fn(FeatureId) -> Option<&'a [u64]>,
        acc: &mut [u64],
        grp: &mut [u64],
    ) {
        crate::exact::eval_batch_slices(
            local as usize,
            tmask_batch,
            lookup,
            acc,
            grp,
            self.req_mask(),
            self.forb_mask(),
            self.req_off(),
            self.req_len(),
            self.req_blob(),
            self.forb_off(),
            self.forb_len(),
            self.forb_blob(),
            self.q_group_start(),
            self.q_group_count(),
            self.group_off(),
            self.group_len(),
            self.anyof_blob(),
        );
    }

    /// Filter check: is this signature key possibly in this segment?
    #[inline]
    fn may_contain(&self, key: u64) -> bool {
        if self.filter_num_blocks == 0 {
            return true; // no filter = don't skip
        }
        crate::filter::bloom_check(key, self.filter_data(), self.filter_mask)
    }

    /// Probe this segment for one title — same semantics as Segment::match_into.
    #[allow(clippy::too_many_arguments)]
    pub fn match_into(
        &self,
        feats: &[FeatureId],
        tmask: u64,
        dict: &crate::dict::Dict,
        epoch: u32,
        seen: &mut [u32],
        out: &mut Vec<u64>,
        include_broad: bool,
        stats: &mut MatchStats,
    ) {
        let has_filter = self.filter_num_blocks > 0;

        // arity-1 signatures
        for &f in feats {
            let key = crate::util::sig_key(&[f]);
            stats.probes_attempted += 1;
            if has_filter && !self.may_contain(key) {
                stats.probes_skipped += 1;
                continue;
            }
            self.probe_index(key, true, epoch, tmask, feats, seen, out, stats, false);
        }
        // arity-2 signatures
        for &h in feats {
            if crate::compile::is_hot(dict, h) {
                for &o in feats {
                    if o != h {
                        let (a, b) = if h < o { (h, o) } else { (o, h) };
                        let key = crate::util::sig_key(&[a, b]);
                        stats.probes_attempted += 1;
                        if has_filter && !self.may_contain(key) {
                            stats.probes_skipped += 1;
                            continue;
                        }
                        self.probe_index(key, true, epoch, tmask, feats, seen, out, stats, false);
                    }
                }
            }
        }
        // broad lane
        if include_broad {
            for &f in feats {
                let key = crate::util::sig_key(&[f]);
                stats.probes_attempted += 1;
                if has_filter && !self.may_contain(key) {
                    stats.probes_skipped += 1;
                    continue;
                }
                self.probe_index(key, false, epoch, tmask, feats, seen, out, stats, true);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[inline]
    fn probe_index(
        &self,
        key: u64,
        is_main: bool,
        epoch: u32,
        tmask: u64,
        feats: &[FeatureId],
        seen: &mut [u32],
        out: &mut Vec<u64>,
        stats: &mut MatchStats,
        is_broad: bool,
    ) {
        let (slots, blob, mask) = if is_main {
            (self.main_slots(), self.main_blob(), self.main_mask)
        } else {
            (self.broad_slots(), self.broad_blob(), self.broad_mask)
        };

        if let Some(posting) = frozen_probe(key, slots, blob, mask) {
            stats.postings_scanned += posting.len() as u32;
            for &local in posting {
                if seen[local as usize] == epoch {
                    continue;
                }
                seen[local as usize] = epoch;
                stats.unique_candidates += 1;
                if is_broad {
                    stats.broad_candidates += 1;
                } else {
                    stats.main_candidates += 1;
                }
                if !self.alive_overlay[local as usize] {
                    continue;
                }
                if self.verify(local, tmask, feats) {
                    out.push(self.logical(local));
                }
            }
        }
    }

    /// Reconstruct an in-memory Segment from this mmap'd segment. Used by
    /// compaction to produce source data for Segment::compact_from.
    pub fn to_memory_segment(&self) -> Segment {
        use crate::exact::ExactStore;
        let n = self.num_queries as usize;

        let mut exact = ExactStore::new();
        let mut classes = Vec::with_capacity(n);
        let mut alive = Vec::with_capacity(n);

        // Copy exact store arrays
        for i in 0..n {
            let rm = self.req_mask()[i];
            let fm = self.forb_mask()[i];
            let ro = self.req_off()[i] as usize;
            let rl = self.req_len()[i] as usize;
            let fo = self.forb_off()[i] as usize;
            let fl = self.forb_len()[i] as usize;
            let gs = self.q_group_start()[i] as usize;
            let gc = self.q_group_count()[i] as usize;
            // SAFETY: the loop runs `i` over `0..n` where `n == num_queries`, and
            // `version_arr`/`logical_arr` are both `num_queries`-long arrays parsed
            // from the mmap in `open`, so both offsets are in bounds of the
            // immutable mapping `self` owns.
            let (ver, log) = unsafe { (*self.version_arr.add(i), *self.logical_arr.add(i)) };

            exact.push_raw(
                rm,
                fm,
                &self.req_blob()[ro..ro + rl],
                &self.forb_blob()[fo..fo + fl],
                (
                    gs,
                    gc,
                    self.group_off(),
                    self.group_len(),
                    self.anyof_blob(),
                ),
                ver,
                log,
            );

            // SAFETY: `i < n == num_queries`, and `class_arr` is the
            // `num_queries`-long class byte array parsed from the mmap, so the
            // offset is in bounds of the immutable mapping.
            let class_byte = unsafe { *self.class_arr.add(i) };
            classes.push(match class_byte {
                0 => CostClass::A,
                1 => CostClass::B,
                2 => CostClass::C,
                _ => CostClass::D,
            });
            alive.push(self.alive_overlay[i]);
        }

        // Rebuild candidate indexes from frozen tables
        let mut main = CandidateIndex::new();
        for slot in self.main_slots() {
            if slot.key != 0 {
                let start = slot.offset as usize;
                let end = start + slot.len as usize;
                for &id in &self.main_blob()[start..end] {
                    main.insert(slot.key, id);
                }
            }
        }

        let mut broad = CandidateIndex::new();
        for slot in self.broad_slots() {
            if slot.key != 0 {
                let start = slot.offset as usize;
                let end = start + slot.len as usize;
                for &id in &self.broad_blob()[start..end] {
                    broad.insert(slot.key, id);
                }
            }
        }

        let mut seg = Segment::from_parts(main, broad, exact, classes, alive);
        seg.vocab_epoch = self.vocab_epoch;
        seg
    }
}

fn parse_frozen_index(data: &[u8], off: usize) -> io::Result<(&[FrozenSlot], &[u32], usize)> {
    let cap = read_u32_at(data, off)? as usize;
    // 4 count + 4 pad
    let slots_off = off + 8;
    // SAFETY: `data` is the CRC-verified mmap (validated in `MmapSegment::open`
    // before any of this runs). `off` is a section offset from the validated
    // header and the writer pads sections to 8 bytes, so `slots_off = off + 8` is
    // 8-aligned — and `FrozenSlot` is `#[repr(C)]`, 16 bytes, padding-free (see
    // its definition), with alignment 8, so the reinterpret is correctly aligned.
    // The writer laid down exactly `cap` slots here, so `cap` elements are in
    // bounds of the mapping.
    let slots = unsafe {
        std::slice::from_raw_parts(data.as_ptr().add(slots_off).cast::<FrozenSlot>(), cap)
    };
    let after_slots = align8((slots_off + cap * std::mem::size_of::<FrozenSlot>()) as u64) as usize;
    let (blob, _) = read_u32_slice(data, after_slots)?;
    Ok((slots, blob, cap))
}

// ---- Dict serialization (for manifest) ----

/// Serialize the feature dictionary to a binary format.
/// Layout: [num_features: u32, then for each: name_len: u16, name: [u8], kind: u8, freq: u32, mask_bit: u8]
/// Followed by: [finalized: u8]
pub fn serialize_dict(dict: &crate::dict::Dict) -> Vec<u8> {
    let mut buf = Vec::new();
    let n = dict.len();
    buf.extend_from_slice(&(n as u32).to_le_bytes());
    for i in 0..n {
        let id = i as FeatureId;
        let name = dict.name(id);
        let name_bytes = name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(name_bytes);
        buf.push(kind_to_u8(dict.kind(id)));
        buf.extend_from_slice(&dict.freq(id).to_le_bytes());
        buf.push(dict.mask_bit(id));
    }
    buf.push(u8::from(dict.is_finalized()));
    buf
}

/// Deserialize a Dict from bytes produced by `serialize_dict`.
pub fn deserialize_dict(data: &[u8]) -> io::Result<crate::dict::Dict> {
    use crate::dict::Dict;
    let mut cursor = 0usize;
    if data.len() < 4 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "dict too short"));
    }
    let n = read_u32_at(data, cursor)? as usize;
    cursor += 4;
    let mut dict = Dict::new();
    for _ in 0..n {
        let name_len = data
            .get(cursor..cursor + 2)
            .and_then(|s| <[u8; 2]>::try_from(s).ok())
            .map(u16::from_le_bytes)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated dict name_len"))?
            as usize;
        cursor += 2;
        let name =
            std::str::from_utf8(data.get(cursor..cursor + name_len).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "truncated dict name")
            })?)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        cursor += name_len;
        let kind = u8_to_kind(data[cursor]);
        cursor += 1;
        let freq = read_u32_at(data, cursor)?;
        cursor += 4;
        let mask_bit = data[cursor];
        cursor += 1;
        dict.intern(name, kind);
        dict.set_freq_and_mask(dict.len() as FeatureId - 1, freq, mask_bit);
    }
    if cursor < data.len() && data[cursor] == 1 {
        dict.mark_finalized();
    }
    Ok(dict)
}

fn kind_to_u8(k: crate::dict::FeatureKind) -> u8 {
    use crate::dict::FeatureKind::{
        Brand, Category, Flag, Generic, Grade, Grader, GraderGrade, Player, Year,
    };
    match k {
        Year => 0,
        Brand => 1,
        Player => 2,
        Category => 3,
        Grader => 4,
        Grade => 5,
        GraderGrade => 6,
        Flag => 7,
        Generic => 8,
    }
}

fn u8_to_kind(b: u8) -> crate::dict::FeatureKind {
    use crate::dict::FeatureKind::{
        Brand, Category, Flag, Generic, Grade, Grader, GraderGrade, Player, Year,
    };
    match b {
        0 => Year,
        1 => Brand,
        2 => Player,
        3 => Category,
        4 => Grader,
        5 => Grade,
        6 => GraderGrade,
        7 => Flag,
        _ => Generic,
    }
}

// ---- Manifest file ----

const MANIFEST_MAGIC: [u8; 4] = *b"PMAN";
const MANIFEST_VERSION: u32 = 1;

/// Engine manifest — records the list of active segment files, dict state,
/// and counters. Written atomically (tmp + rename) alongside segment files.
pub struct Manifest {
    pub segment_files: Vec<String>,
    pub next_seg_id: u64,
    pub dict_data: Vec<u8>,
    pub rejected_parse: u64,
    pub rejected_class_d: u64,
}

pub fn write_manifest(manifest: &Manifest, path: &Path) -> io::Result<()> {
    let tmp = path.with_extension("manifest.tmp");
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(&MANIFEST_MAGIC)?;
    write_u32(&mut f, MANIFEST_VERSION)?;
    write_u64(&mut f, manifest.next_seg_id)?;
    write_u64(&mut f, manifest.rejected_parse)?;
    write_u64(&mut f, manifest.rejected_class_d)?;
    // segment file list
    write_u32(&mut f, manifest.segment_files.len() as u32)?;
    for name in &manifest.segment_files {
        let bytes = name.as_bytes();
        write_u32(&mut f, bytes.len() as u32)?;
        f.write_all(bytes)?;
    }
    // dict blob
    write_u32(&mut f, manifest.dict_data.len() as u32)?;
    f.write_all(&manifest.dict_data)?;
    // CRC of everything written so far
    f.sync_all()?;
    drop(f);
    // Read back for CRC (simple approach)
    let content = std::fs::read(&tmp)?;
    let crc = crc32(&content);
    let mut f = std::fs::OpenOptions::new().append(true).open(&tmp)?;
    write_u32(&mut f, crc)?;
    f.sync_all()?;
    drop(f);
    durable_rename(&tmp, path)?;
    Ok(())
}

pub fn read_manifest(path: &Path) -> io::Result<Manifest> {
    let data = std::fs::read(path)?;
    if data.len() < 12 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "manifest too small",
        ));
    }
    // Verify CRC (last 4 bytes)
    if data.len() < 4 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "no CRC"));
    }
    let content = &data[..data.len() - 4];
    let stored_crc = read_u32_at(&data, data.len() - 4)?;
    if crc32(content) != stored_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "manifest CRC mismatch",
        ));
    }

    if data[0..4] != MANIFEST_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad manifest magic",
        ));
    }
    let version = read_u32_at(&data, 4)?;
    if version != MANIFEST_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported manifest version {version} (expected {MANIFEST_VERSION})"),
        ));
    }
    let mut cursor = 8usize;
    let next_seg_id = read_u64_at(&data, cursor)?;
    cursor += 8;
    let rejected_parse = read_u64_at(&data, cursor)?;
    cursor += 8;
    let rejected_class_d = read_u64_at(&data, cursor)?;
    cursor += 8;

    let num_files = read_u32_at(&data, cursor)? as usize;
    cursor += 4;
    let mut segment_files = Vec::with_capacity(num_files);
    for _ in 0..num_files {
        let len = read_u32_at(&data, cursor)? as usize;
        cursor += 4;
        let name = std::str::from_utf8(&data[cursor..cursor + len])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
            .to_string();
        cursor += len;
        segment_files.push(name);
    }

    let dict_len = read_u32_at(&data, cursor)? as usize;
    cursor += 4;
    let dict_data = data[cursor..cursor + dict_len].to_vec();

    Ok(Manifest {
        segment_files,
        next_seg_id,
        dict_data,
        rejected_parse,
        rejected_class_d,
    })
}

// -- Cluster coordinator manifest + base snapshot (ADR-031) ------------------
//
// The coordinator's durable cluster-state document + base snapshot, the peers of the
// engine `Manifest` + `sources.dat` one level up. The manifest is the atomic commit
// point (tmp + CRC + rename); it pins the frozen dict (so reopen uses the SAME feature
// space → byte-identical placement), the ring config, and the log replay cursor /
// epoch. The base snapshot is the live query set `logical → (version, dsl)` — the
// `sources.dat` v2 shape plus a version column.

const CLUSTER_MANIFEST_MAGIC: [u8; 4] = *b"RCMN";
// v2 (ADR-032): the base is per-shard COMPILED segments (the `segment_registry`),
// not a raw-DSL snapshot file. v1 had a `snapshot_file: String`; the reader rejects
// it (pre-release branch — no on-disk v1 to migrate).
const CLUSTER_MANIFEST_VERSION: u32 = 2;

/// The coordinator's cluster-state document (the analogue of what a Raft quorum will
/// later hold). Written atomically (tmp + CRC + rename) — the SINGLE commit point that
/// makes a checkpoint all-or-nothing: it pins the frozen dict + ring + log cursor AND
/// the per-shard segment registry that constitutes the committed base (ADR-032).
pub struct ClusterManifest {
    /// The log epoch / checkpoint generation (bumped on `checkpoint`).
    pub epoch: u64,
    /// The log position the committed segment base captures through; replay starts after it.
    pub snapshot_pos: u64,
    /// `Dict::fingerprint()` of the frozen dict — verified on open (fail loud on drift).
    pub dict_fingerprint: u64,
    /// Ring config (re-derives a byte-identical `HashRing`).
    pub num_shards: u32,
    pub vnodes: u32,
    /// Default broad-lane toggle.
    pub include_broad: bool,
    /// Per-shard committed base: `segment_registry[i]` is the list of `.seg` filenames
    /// (relative to `shard_<i>/segments/`) that constitute shard `i`'s base. This is the
    /// atomic-commit replacement for the v1 raw-DSL snapshot — on open a shard
    /// attaches-and-mmaps exactly these instead of re-ingesting (ADR-032).
    pub segment_registry: Vec<Vec<String>>,
    /// Per-shard next segment-id counter (parallel to `segment_registry`), so a flush
    /// after reopen never reuses/clobbers a committed segment filename.
    pub next_seg_ids: Vec<u64>,
    /// `serialize_dict(frozen dict)` — the authoritative feature space, stored ONCE here
    /// (shards do not embed their own dict copy).
    pub dict_data: Vec<u8>,
}

pub fn write_cluster_manifest(manifest: &ClusterManifest, path: &Path) -> io::Result<()> {
    let tmp = path.with_extension("cmanifest.tmp");
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(&CLUSTER_MANIFEST_MAGIC)?;
    write_u32(&mut f, CLUSTER_MANIFEST_VERSION)?;
    write_u64(&mut f, manifest.epoch)?;
    write_u64(&mut f, manifest.snapshot_pos)?;
    write_u64(&mut f, manifest.dict_fingerprint)?;
    write_u32(&mut f, manifest.num_shards)?;
    write_u32(&mut f, manifest.vnodes)?;
    f.write_all(&[u8::from(manifest.include_broad)])?;
    // Per-shard segment registry: outer count, then each shard's filename list.
    write_u32(&mut f, manifest.segment_registry.len() as u32)?;
    for files in &manifest.segment_registry {
        write_u32(&mut f, files.len() as u32)?;
        for name in files {
            let b = name.as_bytes();
            write_u32(&mut f, b.len() as u32)?;
            f.write_all(b)?;
        }
    }
    // Per-shard next-seg-id counters (parallel to the registry).
    write_u32(&mut f, manifest.next_seg_ids.len() as u32)?;
    for &id in &manifest.next_seg_ids {
        write_u64(&mut f, id)?;
    }
    write_u32(&mut f, manifest.dict_data.len() as u32)?;
    f.write_all(&manifest.dict_data)?;
    f.sync_all()?;
    drop(f);
    // Read back for the trailing CRC (same simple approach as write_manifest).
    let content = std::fs::read(&tmp)?;
    let crc = crc32(&content);
    let mut f = std::fs::OpenOptions::new().append(true).open(&tmp)?;
    write_u32(&mut f, crc)?;
    f.sync_all()?;
    drop(f);
    durable_rename(&tmp, path)?;
    Ok(())
}

pub fn read_cluster_manifest(path: &Path) -> io::Result<ClusterManifest> {
    let data = std::fs::read(path)?;
    if data.len() < 12 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "cluster manifest too small",
        ));
    }
    let content = &data[..data.len() - 4];
    let stored_crc = read_u32_at(&data, data.len() - 4)?;
    if crc32(content) != stored_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "cluster manifest CRC mismatch",
        ));
    }
    if data[0..4] != CLUSTER_MANIFEST_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad cluster manifest magic",
        ));
    }
    let version = read_u32_at(&data, 4)?;
    if version != CLUSTER_MANIFEST_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported cluster manifest version {version}"),
        ));
    }
    let mut cursor = 8usize;
    let epoch = read_u64_at(&data, cursor)?;
    cursor += 8;
    let snapshot_pos = read_u64_at(&data, cursor)?;
    cursor += 8;
    let dict_fingerprint = read_u64_at(&data, cursor)?;
    cursor += 8;
    let num_shards = read_u32_at(&data, cursor)?;
    cursor += 4;
    let vnodes = read_u32_at(&data, cursor)?;
    cursor += 4;
    let include_broad = *data
        .get(cursor)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated cluster manifest"))?
        != 0;
    cursor += 1;
    // Per-shard segment registry (outer count, then each shard's filename list).
    let shard_count = read_u32_at(&data, cursor)? as usize;
    cursor += 4;
    let mut segment_registry: Vec<Vec<String>> = Vec::with_capacity(shard_count);
    for _ in 0..shard_count {
        let nfiles = read_u32_at(&data, cursor)? as usize;
        cursor += 4;
        let mut files = Vec::with_capacity(nfiles);
        for _ in 0..nfiles {
            let len = read_u32_at(&data, cursor)? as usize;
            cursor += 4;
            let name = std::str::from_utf8(data.get(cursor..cursor + len).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "truncated registry filename")
            })?)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
            .to_string();
            cursor += len;
            files.push(name);
        }
        segment_registry.push(files);
    }
    // Per-shard next-seg-id counters (parallel to the registry).
    let nids = read_u32_at(&data, cursor)? as usize;
    cursor += 4;
    let mut next_seg_ids = Vec::with_capacity(nids);
    for _ in 0..nids {
        next_seg_ids.push(read_u64_at(&data, cursor)?);
        cursor += 8;
    }
    let dict_len = read_u32_at(&data, cursor)? as usize;
    cursor += 4;
    let dict_data = data
        .get(cursor..cursor + dict_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated dict blob"))?
        .to_vec();

    Ok(ClusterManifest {
        epoch,
        snapshot_pos,
        dict_fingerprint,
        num_shards,
        vnodes,
        include_broad,
        segment_registry,
        next_seg_ids,
        dict_data,
    })
}

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

    fn get(&self, logical: u64) -> Option<String> {
        let data: &[u8] = &self.mmap;
        let (mut lo, mut hi) = (0usize, self.count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.logical_at(mid)?.cmp(&logical) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    let rec = self.index_off + mid * SRC_IDX_REC;
                    let boff = read_u64_at(data, rec + 8).ok()? as usize;
                    let len = read_u32_at(data, rec + 16).ok()? as usize;
                    let start = self.blob_off + boff;
                    let bytes = data.get(start..start + len)?;
                    return std::str::from_utf8(bytes).ok().map(str::to_owned);
                }
            }
        }
        None
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
        match self {
            SourceStore::Resident(m) => rw_read(m).get(&logical).cloned(),
            SourceStore::Lazy { base, overlay } => {
                // Overlay (post-flush mutations) wins over the mmap base; a `None`
                // overlay entry is a tombstone (deleted since the last flush).
                if let Some(v) = rw_read(overlay).get(&logical) {
                    return v.clone();
                }
                base.as_ref().and_then(|b| b.get(logical))
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
    use super::*;

    /// The v2 cluster manifest's nested per-shard registry + next-seg-id columns must
    /// round-trip byte-exactly (varied per-shard file counts, including an empty shard).
    /// The hand-rolled length-prefixed encoding is easy to get cursor-wrong, so pin it.
    #[test]
    fn cluster_manifest_v2_round_trips_registry() {
        let dir = std::env::temp_dir().join(format!("rr_cmanifest_rt_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("cluster_manifest.bin");

        let manifest = ClusterManifest {
            epoch: 7,
            snapshot_pos: 42,
            dict_fingerprint: 0xDEAD_BEEF_1234_5678,
            num_shards: 3,
            vnodes: 64,
            include_broad: true,
            segment_registry: vec![
                vec!["seg_000001.seg".to_string(), "seg_000004.seg".to_string()],
                vec![], // an empty shard (no committed segments)
                vec!["seg_000002.seg".to_string()],
            ],
            next_seg_ids: vec![5, 1, 3],
            dict_data: vec![1, 2, 3, 4, 5],
        };
        write_cluster_manifest(&manifest, &path).expect("write");
        let got = read_cluster_manifest(&path).expect("read");

        assert_eq!(got.epoch, manifest.epoch);
        assert_eq!(got.snapshot_pos, manifest.snapshot_pos);
        assert_eq!(got.dict_fingerprint, manifest.dict_fingerprint);
        assert_eq!(got.num_shards, manifest.num_shards);
        assert_eq!(got.vnodes, manifest.vnodes);
        assert_eq!(got.include_broad, manifest.include_broad);
        assert_eq!(got.segment_registry, manifest.segment_registry);
        assert_eq!(got.next_seg_ids, manifest.next_seg_ids);
        assert_eq!(got.dict_data, manifest.dict_data);

        // A flipped byte in the body must fail the trailing-CRC check (fail loud).
        let mut bytes = std::fs::read(&path).expect("read raw");
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;
        std::fs::write(&path, &bytes).expect("corrupt");
        assert!(
            read_cluster_manifest(&path).is_err(),
            "corrupt manifest must error"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
