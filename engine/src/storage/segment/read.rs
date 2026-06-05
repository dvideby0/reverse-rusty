//! The segment read helpers: the typed-slice section readers over mmap'd bytes,
//! their bounds validation, and the frozen-hash-table probe/parse.
//!
//! These are the byte-level primitives the [`MmapSegment`](super::MmapSegment) read
//! view is built from — `open` parses sections with `read_*_slice`/`parse_frozen_index`,
//! and the hot-path matchers probe with `frozen_probe`. The trailing CRC proves byte
//! integrity, not structural validity, so every reader funnels through
//! [`checked_section_end`] before an unsafe cast (ADR-052).

use std::io;

use super::super::read_u32_at;
use super::{align8, FrozenSlot};

/// Probe a frozen hash table for a signature key, returning the posting slice.
#[inline]
pub(in crate::storage::segment) fn frozen_probe<'a>(
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

// ---- reading helpers (from mmap'd bytes) ----

/// Read a u32-element array: [count: u32, data...]. Returns (slice, next_offset).
/// The slice is cast from the raw bytes (requires alignment — guaranteed by pad_to_8).
pub(in crate::storage::segment) fn read_u32_slice(
    data: &[u8],
    off: usize,
) -> io::Result<(&[u32], usize)> {
    let count = read_u32_at(data, off)? as usize;
    let data_off = off + 4;
    // Bounds-validate the section against the mmap BEFORE the unsafe cast — the CRC
    // proves byte integrity, not that `count` is structurally valid (ADR-052).
    let data_end = checked_section_end(data, data_off, count, 4)?;
    // SAFETY: `checked_section_end` verified the `count` u32s lie within `data`. `off`
    // is 8-aligned (the writer `pad_to_8`s every section and the parse walk only
    // advances by aligned amounts) and the mmap base is page-aligned, so `data_off`
    // (= off + 4) meets `u32`'s 4-byte alignment. The slice borrows `data`.
    let slice =
        unsafe { std::slice::from_raw_parts(data.as_ptr().add(data_off).cast::<u32>(), count) };
    let end = align8(data_end as u64) as usize;
    Ok((slice, end))
}

/// Read a u16-element array.
pub(in crate::storage::segment) fn read_u16_slice(
    data: &[u8],
    off: usize,
) -> io::Result<(&[u16], usize)> {
    let count = read_u32_at(data, off)? as usize;
    let data_off = off + 4;
    // Bounds-validate before the unsafe cast (CRC ≠ structural validity, ADR-052).
    let data_end = checked_section_end(data, data_off, count, 2)?;
    // SAFETY: `checked_section_end` verified the `count` u16s lie within `data`. `off`
    // is 8-aligned (writer `pad_to_8`) and the mmap base is page-aligned, so `data_off`
    // (= off + 4) meets `u16`'s 2-byte alignment. The slice borrows `data`.
    let slice =
        unsafe { std::slice::from_raw_parts(data.as_ptr().add(data_off).cast::<u16>(), count) };
    let end = align8(data_end as u64) as usize;
    Ok((slice, end))
}

/// Read a u64-element array: [count: u32, pad(4), data...].
pub(in crate::storage::segment) fn read_u64_slice(
    data: &[u8],
    off: usize,
) -> io::Result<(&[u64], usize)> {
    let count = read_u32_at(data, off)? as usize;
    // 4 count + 4 pad
    let data_off = off + 8;
    // Bounds-validate before the unsafe cast (CRC ≠ structural validity, ADR-052).
    let data_end = checked_section_end(data, data_off, count, 8)?;
    // SAFETY: `checked_section_end` verified the `count` u64s lie within `data`. `off`
    // is 8-aligned (writer `pad_to_8`) and the mmap base is page-aligned, so `data_off`
    // (= off + 8) meets `u64`'s 8-byte alignment. The slice borrows `data`.
    let slice =
        unsafe { std::slice::from_raw_parts(data.as_ptr().add(data_off).cast::<u64>(), count) };
    Ok((slice, data_end)) // data_end is already 8-aligned (data_off + count*8)
}

/// Read a u8-element array.
pub(in crate::storage::segment) fn read_u8_slice(
    data: &[u8],
    off: usize,
) -> io::Result<(&[u8], usize)> {
    let count = read_u32_at(data, off)? as usize;
    let data_off = off + 4;
    let data_end = checked_section_end(data, data_off, count, 1)?;
    let slice = &data[data_off..data_end];
    let end = align8(data_end as u64) as usize;
    Ok((slice, end))
}

/// Validate that a section of `count` elements × `elem_bytes` each, starting at byte
/// `data_off`, lies fully within `data` — no integer overflow, no overrun past the
/// mapping. Returns the section's end byte offset (before alignment padding).
///
/// The trailing CRC (checked in [`MmapSegment::open`](super::MmapSegment::open)) proves
/// the bytes are intact, NOT that a length/offset read *out of* those bytes is
/// structurally valid. A CRC-consistent but malformed segment — a `count` that overruns
/// the mmap, from a writer bug, a torn write that happens to re-pass CRC, or tampering —
/// would otherwise let the `from_raw_parts` casts below construct an out-of-bounds typed
/// slice (undefined behavior, since the cast trusts `count` for the slice length and
/// downstream indexing trusts that length in turn). Validating here turns that into a
/// fail-loud `InvalidData` error, matching the corrupt-segment-fails-loud contract
/// (ADR-052; cf. ADR-032). The `read_u*_slice` readers call this before every cast.
fn checked_section_end(
    data: &[u8],
    data_off: usize,
    count: usize,
    elem_bytes: usize,
) -> io::Result<usize> {
    let invalid = |msg: &'static str| io::Error::new(io::ErrorKind::InvalidData, msg);
    let byte_len = count
        .checked_mul(elem_bytes)
        .ok_or_else(|| invalid("segment section length overflows usize"))?;
    let end = data_off
        .checked_add(byte_len)
        .ok_or_else(|| invalid("segment section offset overflows usize"))?;
    if end > data.len() {
        return Err(invalid("segment section extends past end of file"));
    }
    Ok(end)
}

pub(in crate::storage::segment) fn parse_frozen_index(
    data: &[u8],
    off: usize,
) -> io::Result<(&[FrozenSlot], &[u32], usize)> {
    let cap = read_u32_at(data, off)? as usize;
    // 4 count + 4 pad
    let slots_off = off + 8;
    // Bounds-validate the `cap` slots against the mmap BEFORE the unsafe cast — the
    // CRC proves byte integrity, not that `cap` is structurally valid (ADR-052).
    let after_slots_raw =
        checked_section_end(data, slots_off, cap, std::mem::size_of::<FrozenSlot>())?;
    // SAFETY: `checked_section_end` verified the `cap` `FrozenSlot`s lie within `data`.
    // `off` is a section offset from the validated header and the writer pads sections
    // to 8 bytes, so `slots_off = off + 8` is 8-aligned — and `FrozenSlot` is
    // `#[repr(C)]`, 16 bytes, padding-free (see its definition), alignment 8, so the
    // reinterpret is correctly aligned. The slice borrows `data`.
    let slots = unsafe {
        std::slice::from_raw_parts(data.as_ptr().add(slots_off).cast::<FrozenSlot>(), cap)
    };
    let after_slots = align8(after_slots_raw as u64) as usize;
    let (blob, _) = read_u32_slice(data, after_slots)?;
    Ok((slots, blob, cap))
}

#[cfg(test)]
mod bounds_tests {
    //! ADR-052: the typed-slice readers must reject a CRC-valid-but-structurally
    //! malformed segment (a section `count`/offset that overruns the mapping) with a
    //! fail-loud `InvalidData` error, never an out-of-bounds `from_raw_parts` (UB).
    use super::{checked_section_end, read_u32_slice};
    use std::io::ErrorKind;

    #[test]
    fn checked_section_end_validates_bounds_and_overflow() {
        let data = vec![0u8; 64];
        // In bounds: 4 u32s starting at byte 8 → ends at 24.
        assert_eq!(checked_section_end(&data, 8, 4, 4).unwrap(), 24);
        // Exactly fills the buffer.
        assert_eq!(checked_section_end(&data, 0, 16, 4).unwrap(), 64);
        // One element past the end → InvalidData (not a slice overrun).
        assert_eq!(
            checked_section_end(&data, 0, 17, 4).unwrap_err().kind(),
            ErrorKind::InvalidData
        );
        // count * elem overflows usize → InvalidData (no panic).
        assert_eq!(
            checked_section_end(&data, 0, usize::MAX, 4)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidData
        );
        // data_off + byte_len overflows usize → InvalidData.
        assert_eq!(
            checked_section_end(&data, usize::MAX, 1, 4)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidData
        );
    }

    #[test]
    fn read_u32_slice_rejects_count_past_eof() {
        // An 8-byte buffer whose section header claims 1000 u32s. Without the bounds
        // check this constructed a 1000-element slice over 4 bytes of mapping (UB);
        // now it fails loud.
        let mut data = vec![0u8; 8];
        data[0..4].copy_from_slice(&1000u32.to_le_bytes()); // count = 1000
        let err = read_u32_slice(&data, 0).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn read_u32_slice_accepts_in_bounds() {
        // count = 1, then one u32 of data (8-byte buffer, already 8-aligned end).
        let mut data = vec![0u8; 8];
        data[0..4].copy_from_slice(&1u32.to_le_bytes());
        data[4..8].copy_from_slice(&0x1234_5678u32.to_le_bytes());
        let (slice, end) = read_u32_slice(&data, 0).unwrap();
        assert_eq!(slice, &[0x1234_5678]);
        assert_eq!(end, 8);
    }
}
