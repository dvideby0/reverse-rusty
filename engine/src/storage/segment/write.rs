//! The segment write path: serialize an in-memory [`Segment`] to a `.seg` file.
//!
//! [`write_segment`] lays down the header + sections (exact SoA, frozen main/broad
//! indexes, anchor filter, meta, logical-index columns, tag column) with atomic
//! write-to-tmp + rename, then appends the trailing CRC-32. The frozen-table
//! serializer ([`freeze_index`]) is the inverse of the read side's
//! `parse_frozen_index`.

use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;

use crate::compile::CostClass;
use crate::index::CandidateIndex;
use crate::segment::Segment;

use super::super::{crc32, durable_rename, write_u32, write_u64};
use super::{
    align8, FrozenSlot, FORMAT_VERSION, FORMAT_VERSION_CLASS_D, FORMAT_VERSION_HOT, HEADER_SIZE,
    MAGIC,
};

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

    // ---- Tag section (per-query metadata; ADR-049) ----
    // Three parallel arrays exactly like the required tail: tag_off/tag_len index into
    // a sorted tag_blob of TagIds. A reader of an older (v1/v2) file finds no section and
    // reads back an empty tag column (every query untagged).
    pad_to_8(&mut f)?;
    let tag_off_pos = f.stream_position()?;
    let exact = seg.exact_store();
    write_u32_array(&mut f, exact.tag_offs())?;
    write_u16_array(&mut f, exact.tag_lens())?;
    write_u32_array(&mut f, exact.tag_blobs())?;

    // ---- Hot-tier index (class H; ADR-105) ----
    // Written ONLY when the segment holds class-H entries: the section (and the
    // v5 version word carrying it) is what makes a hot-bearing file refuse a
    // pre-ADR-105 reader loudly. Hot-free segments write no section and leave
    // the header slot zero — byte-identical v3/v4 output.
    let has_hot = seg
        .classes()
        .iter()
        .any(|c| matches!(c, crate::compile::CostClass::H));
    debug_assert_eq!(
        has_hot,
        seg.hot_index().num_signatures() > 0,
        "class column and hot index must agree on hot-tier presence"
    );
    let hot_off = if has_hot {
        pad_to_8(&mut f)?;
        let off = f.stream_position()?;
        write_frozen_index_section(&mut f, seg.hot_index())?;
        off
    } else {
        0
    };

    // ---- Write header ----
    f.seek(SeekFrom::Start(0))?;
    f.write_all(&MAGIC)?;
    // Version ladder (highest capability wins): a segment holding ≥1 class-H
    // entry writes v5 (the hot-index section + the rollback fence — a pre-ADR-105
    // reader never probes the hot index, so it must fail loudly on this file
    // rather than serve it with those queries silently unmatchable). Otherwise a
    // segment holding ≥1 class-D always-candidate writes v4 (layout-identical to
    // v3) purely as the ADR-068 rollback fence. Otherwise v3, byte-identically.
    let has_class_d = seg
        .classes()
        .iter()
        .any(|c| matches!(c, crate::compile::CostClass::D));
    write_u32(
        &mut f,
        if has_hot {
            FORMAT_VERSION_HOT
        } else if has_class_d {
            FORMAT_VERSION_CLASS_D
        } else {
            FORMAT_VERSION
        },
    )?;
    write_u32(&mut f, seg.len() as u32)?;
    write_u32(&mut f, 0)?; // reserved
    write_u64(&mut f, exact_off)?;
    write_u64(&mut f, main_off)?;
    write_u64(&mut f, broad_off)?;
    write_u64(&mut f, filter_off)?;
    write_u64(&mut f, meta_off)?;
    write_u64(&mut f, logical_off)?;
    write_u64(&mut f, tag_off_pos)?;
    write_u64(&mut f, hot_off)?;

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
            // Class byte 4 appears only in v5 files (the write path pairs any
            // class-H entry with FORMAT_VERSION_HOT), so a pre-v5 reader can
            // never encounter it — it refuses the file at the version check.
            CostClass::H => 4,
        })
        .collect();
    write_u8_array(w, &classes)?;
    let alive: Vec<u8> = seg.alive_flags().iter().map(|&a| u8::from(a)).collect();
    write_u8_array(w, &alive)?;
    Ok(())
}
