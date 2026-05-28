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

use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;

use crate::compile::CostClass;
use crate::dict::FeatureId;
use crate::index::CandidateIndex;
use crate::segment::{MatchStats, Segment};

// ---- constants ----

const MAGIC: [u8; 4] = *b"PERC";
const FORMAT_VERSION: u32 = 1;
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
//   56..80  reserved (24 bytes, zeroed)

// ---- CRC-32 (IEEE / ISO 3309) ----

/// Simple CRC-32 using the standard polynomial. Used for WAL entry integrity;
/// segment files use atomic rename (write-to-tmp + rename) for integrity.
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFFFFFF;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
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
    if slots.is_empty() {
        return None;
    }
    let mut idx = key & mask;
    loop {
        let slot = unsafe { slots.get_unchecked(idx as usize) };
        if slot.key == key {
            let start = slot.offset as usize;
            let end = start + slot.len as usize;
            return Some(&blob[start..end]);
        }
        if slot.key == 0 {
            return None;
        }
        idx = (idx + 1) & mask;
    }
}

// ---- low-level I/O helpers ----

fn write_u32(w: &mut impl Write, v: u32) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

fn write_u64(w: &mut impl Write, v: u64) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

fn read_u32_at(data: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(data[off..off + 4].try_into().unwrap())
}

fn read_u64_at(data: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(data[off..off + 8].try_into().unwrap())
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
    // 4 bytes for count + data — write raw bytes
    let bytes = unsafe {
        std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 4)
    };
    w.write_all(bytes)?;
    pad_to_8(w)
}

/// Write a slice of u16 values: [count: u32, data: [u16; count], pad_to_8].
fn write_u16_array(w: &mut (impl Write + Seek), data: &[u16]) -> io::Result<()> {
    write_u32(w, data.len() as u32)?;
    let bytes = unsafe {
        std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 2)
    };
    w.write_all(bytes)?;
    pad_to_8(w)
}

/// Write a slice of u64 values: [count: u32, pad(4), data: [u64; count]].
/// Already 8-byte aligned after data (u64 elements).
fn write_u64_array(w: &mut (impl Write + Seek), data: &[u64]) -> io::Result<()> {
    write_u32(w, data.len() as u32)?;
    w.write_all(&[0u8; 4])?; // pad count to 8 bytes
    let bytes = unsafe {
        std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 8)
    };
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
fn read_u32_slice(data: &[u8], off: usize) -> (&[u32], usize) {
    let count = read_u32_at(data, off) as usize;
    let data_off = off + 4;
    let slice = unsafe {
        std::slice::from_raw_parts(
            data.as_ptr().add(data_off) as *const u32,
            count,
        )
    };
    let end = align8((data_off + count * 4) as u64) as usize;
    (slice, end)
}

/// Read a u16-element array.
fn read_u16_slice(data: &[u8], off: usize) -> (&[u16], usize) {
    let count = read_u32_at(data, off) as usize;
    let data_off = off + 4;
    let slice = unsafe {
        std::slice::from_raw_parts(
            data.as_ptr().add(data_off) as *const u16,
            count,
        )
    };
    let end = align8((data_off + count * 2) as u64) as usize;
    (slice, end)
}

/// Read a u64-element array: [count: u32, pad(4), data...].
fn read_u64_slice(data: &[u8], off: usize) -> (&[u64], usize) {
    let count = read_u32_at(data, off) as usize;
    let data_off = off + 8; // 4 count + 4 pad
    let slice = unsafe {
        std::slice::from_raw_parts(
            data.as_ptr().add(data_off) as *const u64,
            count,
        )
    };
    let end = data_off + count * 8; // already aligned
    (slice, end)
}

/// Read a u8-element array.
fn read_u8_slice(data: &[u8], off: usize) -> (&[u8], usize) {
    let count = read_u32_at(data, off) as usize;
    let data_off = off + 4;
    let slice = &data[data_off..data_off + count];
    let end = align8((data_off + count) as u64) as usize;
    (slice, end)
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
    std::fs::rename(&tmp_path, path)?;
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

fn write_frozen_index_section(w: &mut (impl Write + Seek), index: &CandidateIndex) -> io::Result<()> {
    let (slots, blob) = freeze_index(index);
    // Write slots as a u64-aligned array (each slot is 16 bytes = 2 u64s)
    let cap = slots.len();
    write_u32(w, cap as u32)?;
    w.write_all(&[0u8; 4])?; // pad to 8
    let slot_bytes = unsafe {
        std::slice::from_raw_parts(
            slots.as_ptr() as *const u8,
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
        let bytes = unsafe {
            std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 8)
        };
        w.write_all(bytes)?;
    } else {
        write_u32(w, 0)?; // no filter
        w.write_all(&[0u8; 4])?;
        write_u64(w, 0)?;
    }
    Ok(())
}

fn write_meta_section(w: &mut (impl Write + Seek), seg: &Segment) -> io::Result<()> {
    let classes: Vec<u8> = seg.classes().iter().map(|c| match c {
        CostClass::A => 0,
        CostClass::B => 1,
        CostClass::C => 2,
        CostClass::D => 3,
    }).collect();
    write_u8_array(w, &classes)?;
    let alive: Vec<u8> = seg.alive_flags().iter().map(|&a| if a { 1 } else { 0 }).collect();
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
pub struct MmapSegment {
    #[allow(dead_code)]
    mmap: memmap2::Mmap,
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
    alive_overlay: Vec<bool>,
    /// O(1) counter of alive (non-tombstoned) entries.
    alive_counter: usize,
    // Path for cleanup/identification
    path: std::path::PathBuf,
}

// SAFETY: MmapSegment is safe to send/share across threads. The raw pointers
// point into the mmap which lives as long as the struct. The alive_overlay is
// only mutated through &mut self.
unsafe impl Send for MmapSegment {}
unsafe impl Sync for MmapSegment {}

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
        let mmap = unsafe { memmap2::Mmap::map(&file)? };

        if mmap.len() < HEADER_SIZE + 4 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "file too small"));
        }
        // Verify trailing CRC32
        {
            let content = &mmap[..mmap.len() - 4];
            let stored_crc = u32::from_le_bytes(
                mmap[mmap.len() - 4..].try_into().unwrap()
            );
            if crc32(content) != stored_crc {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("segment file CRC mismatch: {:?}", path),
                ));
            }
        }
        // We need to parse the mmap contents to extract offsets and lengths,
        // then store raw pointers into the mmap. To satisfy the borrow checker
        // (we move `mmap` into the struct but store pointers derived from it),
        // we use a two-phase approach: parse with a temporary borrow to get
        // offsets/lengths, then construct pointers from the base after move.

        // Phase 1: validate and parse offsets/lengths from a temporary borrow
        {
            let data = &mmap[..];
            if &data[0..4] != &MAGIC {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "bad magic"));
            }
            let version = read_u32_at(data, 4);
            if version != FORMAT_VERSION {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsupported format version {}", version),
                ));
            }
        }

        // Phase 2: extract section layout using raw pointer arithmetic.
        // All pointers are derived from `base` which points into `mmap`.
        // After we move `mmap` into the struct, the backing memory doesn't move
        // (it's OS-mapped), so the pointers remain valid for the struct's lifetime.
        let base = mmap.as_ptr();
        let mmap_len = mmap.len();
        let data_for_parse = unsafe { std::slice::from_raw_parts(base, mmap_len) };

        let num_queries = read_u32_at(data_for_parse, 8);
        let exact_off = read_u64_at(data_for_parse, 16) as usize;
        let main_off = read_u64_at(data_for_parse, 24) as usize;
        let broad_off = read_u64_at(data_for_parse, 32) as usize;
        let filter_off = read_u64_at(data_for_parse, 40) as usize;
        let meta_off = read_u64_at(data_for_parse, 48) as usize;

        // ---- Parse exact section ----
        let mut cursor = exact_off;
        let (req_mask_s, next) = read_u64_slice(data_for_parse, cursor); cursor = next;
        let (forb_mask_s, next) = read_u64_slice(data_for_parse, cursor); cursor = next;
        let (req_off_s, next) = read_u32_slice(data_for_parse, cursor); cursor = next;
        let (req_len_s, next) = read_u16_slice(data_for_parse, cursor); cursor = next;
        let (req_blob_s, next) = read_u32_slice(data_for_parse, cursor); cursor = next;
        let (forb_off_s, next) = read_u32_slice(data_for_parse, cursor); cursor = next;
        let (forb_len_s, next) = read_u16_slice(data_for_parse, cursor); cursor = next;
        let (forb_blob_s, next) = read_u32_slice(data_for_parse, cursor); cursor = next;
        let (q_group_start_s, next) = read_u32_slice(data_for_parse, cursor); cursor = next;
        let (q_group_count_s, next) = read_u16_slice(data_for_parse, cursor); cursor = next;
        let (group_off_s, next) = read_u32_slice(data_for_parse, cursor); cursor = next;
        let (group_len_s, next) = read_u16_slice(data_for_parse, cursor); cursor = next;
        let (anyof_blob_s, next) = read_u32_slice(data_for_parse, cursor); cursor = next;
        let (version_s, next) = read_u32_slice(data_for_parse, cursor); cursor = next;
        let (logical_s, _) = read_u64_slice(data_for_parse, cursor);

        // ---- Parse main index ----
        let (main_slots_s, main_blob_s, main_cap) = parse_frozen_index(data_for_parse, main_off);

        // ---- Parse broad index ----
        let (broad_slots_s, broad_blob_s, broad_cap) = parse_frozen_index(data_for_parse, broad_off);

        // ---- Parse filter ----
        let filter_num_blocks = read_u32_at(data_for_parse, filter_off) as usize;
        let filter_mask_val = read_u64_at(data_for_parse, filter_off + 8);
        let filter_data_off = filter_off + 16;
        let filter_data_ptr = if filter_num_blocks > 0 {
            unsafe { base.add(filter_data_off) as *const u64 }
        } else {
            std::ptr::null()
        };

        // ---- Parse meta ----
        cursor = meta_off;
        let (class_s, next) = read_u8_slice(data_for_parse, cursor); cursor = next;
        let (alive_s, _) = read_u8_slice(data_for_parse, cursor);

        // Build alive overlay from on-disk data
        let alive_overlay: Vec<bool> = alive_s.iter().map(|&b| b != 0).collect();
        let alive_counter = alive_overlay.iter().filter(|&&a| a).count();

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
            main_mask: if main_cap > 0 { (main_cap - 1) as u64 } else { 0 },
            main_blob: main_blob_s.as_ptr(),
            main_blob_len: main_blob_s.len(),
            broad_slots: broad_slots_s.as_ptr(),
            broad_cap,
            broad_mask: if broad_cap > 0 { (broad_cap - 1) as u64 } else { 0 },
            broad_blob: broad_blob_s.as_ptr(),
            broad_blob_len: broad_blob_s.len(),
            filter_data: filter_data_ptr,
            filter_num_blocks,
            filter_mask: filter_mask_val,
            class_arr: class_s.as_ptr(),
            alive_overlay,
            alive_counter,
            path: path.to_path_buf(),
        })
    }

    // ---- slice accessors (zero-cost, just pointer arithmetic) ----

    #[inline]
    fn req_mask(&self) -> &[u64] {
        unsafe { std::slice::from_raw_parts(self.req_mask, self.num_queries as usize) }
    }
    #[inline]
    fn forb_mask(&self) -> &[u64] {
        unsafe { std::slice::from_raw_parts(self.forb_mask, self.num_queries as usize) }
    }
    #[inline]
    fn req_off(&self) -> &[u32] {
        unsafe { std::slice::from_raw_parts(self.req_off, self.num_queries as usize) }
    }
    #[inline]
    fn req_len(&self) -> &[u16] {
        unsafe { std::slice::from_raw_parts(self.req_len, self.num_queries as usize) }
    }
    #[inline]
    fn req_blob(&self) -> &[u32] {
        unsafe { std::slice::from_raw_parts(self.req_blob, self.req_blob_len) }
    }
    #[inline]
    fn forb_off(&self) -> &[u32] {
        unsafe { std::slice::from_raw_parts(self.forb_off, self.num_queries as usize) }
    }
    #[inline]
    fn forb_len(&self) -> &[u16] {
        unsafe { std::slice::from_raw_parts(self.forb_len, self.num_queries as usize) }
    }
    #[inline]
    fn forb_blob(&self) -> &[u32] {
        unsafe { std::slice::from_raw_parts(self.forb_blob, self.forb_blob_len) }
    }
    #[inline]
    fn q_group_start(&self) -> &[u32] {
        unsafe { std::slice::from_raw_parts(self.q_group_start, self.num_queries as usize) }
    }
    #[inline]
    fn q_group_count(&self) -> &[u16] {
        unsafe { std::slice::from_raw_parts(self.q_group_count, self.num_queries as usize) }
    }
    #[inline]
    fn group_off(&self) -> &[u32] {
        unsafe { std::slice::from_raw_parts(self.group_off, self.group_off_len) }
    }
    #[inline]
    fn group_len(&self) -> &[u16] {
        unsafe { std::slice::from_raw_parts(self.group_len, self.group_off_len) }
    }
    #[inline]
    fn anyof_blob(&self) -> &[u32] {
        unsafe { std::slice::from_raw_parts(self.anyof_blob, self.anyof_blob_len) }
    }

    #[inline]
    fn main_slots(&self) -> &[FrozenSlot] {
        unsafe { std::slice::from_raw_parts(self.main_slots, self.main_cap) }
    }
    #[inline]
    fn main_blob(&self) -> &[u32] {
        unsafe { std::slice::from_raw_parts(self.main_blob, self.main_blob_len) }
    }
    #[inline]
    fn broad_slots(&self) -> &[FrozenSlot] {
        unsafe { std::slice::from_raw_parts(self.broad_slots, self.broad_cap) }
    }
    #[inline]
    fn broad_blob(&self) -> &[u32] {
        unsafe { std::slice::from_raw_parts(self.broad_blob, self.broad_blob_len) }
    }

    #[inline]
    fn filter_data(&self) -> &[u64] {
        if self.filter_num_blocks == 0 {
            return &[];
        }
        unsafe {
            std::slice::from_raw_parts(self.filter_data, self.filter_num_blocks * 8)
        }
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

    /// Number of alive (non-tombstoned) entries (O(1)).
    pub fn alive_count(&self) -> usize {
        self.alive_counter
    }

    pub fn holes_ratio(&self) -> f64 {
        let total = self.len();
        if total == 0 {
            return 0.0;
        }
        1.0 - (self.alive_count() as f64 / total as f64)
    }

    #[inline]
    fn logical(&self, id: u32) -> u64 {
        unsafe { *self.logical_arr.add(id as usize) }
    }


    /// Integer-only exact verification — same logic as ExactStore::verify but
    /// operating on mmap'd slices.
    #[inline]
    pub fn verify(&self, id: u32, tmask: u64, tfeats: &[FeatureId]) -> bool {
        crate::exact::verify_slices(
            id, tmask, tfeats,
            self.req_mask(), self.forb_mask(),
            self.req_off(), self.req_len(), self.req_blob(),
            self.forb_off(), self.forb_len(), self.forb_blob(),
            self.q_group_start(), self.q_group_count(),
            self.group_off(), self.group_len(), self.anyof_blob(),
        )
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
            self.probe_index(
                key, true, epoch, tmask, feats, seen, out, stats, false,
            );
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
                        self.probe_index(
                            key, true, epoch, tmask, feats, seen, out, stats, false,
                        );
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
                self.probe_index(
                    key, false, epoch, tmask, feats, seen, out, stats, true,
                );
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
            let ver = unsafe { *self.version_arr.add(i) };
            let log = unsafe { *self.logical_arr.add(i) };

            exact.push_raw(
                rm, fm,
                &self.req_blob()[ro..ro + rl],
                &self.forb_blob()[fo..fo + fl],
                (gs, gc, self.group_off(), self.group_len(), self.anyof_blob()),
                ver, log,
            );

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

        Segment::from_parts(main, broad, exact, classes, alive)
    }
}

fn parse_frozen_index<'a>(data: &'a [u8], off: usize) -> (&'a [FrozenSlot], &'a [u32], usize) {
    let cap = read_u32_at(data, off) as usize;
    let slots_off = off + 8; // 4 count + 4 pad
    let slots = unsafe {
        std::slice::from_raw_parts(
            data.as_ptr().add(slots_off) as *const FrozenSlot,
            cap,
        )
    };
    let after_slots = align8((slots_off + cap * std::mem::size_of::<FrozenSlot>()) as u64) as usize;
    let (blob, _) = read_u32_slice(data, after_slots);
    (slots, blob, cap)
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
    buf.push(if dict.is_finalized() { 1 } else { 0 });
    buf
}

/// Deserialize a Dict from bytes produced by `serialize_dict`.
pub fn deserialize_dict(data: &[u8]) -> io::Result<crate::dict::Dict> {
    use crate::dict::Dict;
    let mut cursor = 0usize;
    if data.len() < 4 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "dict too short"));
    }
    let n = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
    cursor += 4;
    let mut dict = Dict::new();
    for _ in 0..n {
        let name_len = u16::from_le_bytes(data[cursor..cursor + 2].try_into().unwrap()) as usize;
        cursor += 2;
        let name = std::str::from_utf8(&data[cursor..cursor + name_len])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        cursor += name_len;
        let kind = u8_to_kind(data[cursor]);
        cursor += 1;
        let freq = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap());
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
    use crate::dict::FeatureKind::*;
    match k {
        Year => 0, Brand => 1, Player => 2, Category => 3,
        Grader => 4, Grade => 5, GraderGrade => 6, Flag => 7, Generic => 8,
    }
}

fn u8_to_kind(b: u8) -> crate::dict::FeatureKind {
    use crate::dict::FeatureKind::*;
    match b {
        0 => Year, 1 => Brand, 2 => Player, 3 => Category,
        4 => Grader, 5 => Grade, 6 => GraderGrade, 7 => Flag,
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
    std::fs::rename(&tmp, path)?;
    Ok(())
}

pub fn read_manifest(path: &Path) -> io::Result<Manifest> {
    let data = std::fs::read(path)?;
    if data.len() < 12 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "manifest too small"));
    }
    // Verify CRC (last 4 bytes)
    if data.len() < 4 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "no CRC"));
    }
    let content = &data[..data.len() - 4];
    let stored_crc = u32::from_le_bytes(data[data.len() - 4..].try_into().unwrap());
    if crc32(content) != stored_crc {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "manifest CRC mismatch"));
    }

    if &data[0..4] != &MANIFEST_MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad manifest magic"));
    }
    let version = read_u32_at(&data, 4);
    if version != MANIFEST_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported manifest version {} (expected {})", version, MANIFEST_VERSION),
        ));
    }
    let mut cursor = 8usize;
    let next_seg_id = read_u64_at(&data, cursor); cursor += 8;
    let rejected_parse = read_u64_at(&data, cursor); cursor += 8;
    let rejected_class_d = read_u64_at(&data, cursor); cursor += 8;

    let num_files = read_u32_at(&data, cursor) as usize; cursor += 4;
    let mut segment_files = Vec::with_capacity(num_files);
    for _ in 0..num_files {
        let len = read_u32_at(&data, cursor) as usize; cursor += 4;
        let name = std::str::from_utf8(&data[cursor..cursor + len])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
            .to_string();
        cursor += len;
        segment_files.push(name);
    }

    let dict_len = read_u32_at(&data, cursor) as usize; cursor += 4;
    let dict_data = data[cursor..cursor + dict_len].to_vec();

    Ok(Manifest {
        segment_files,
        next_seg_id,
        dict_data,
        rejected_parse,
        rejected_class_d,
    })
}
