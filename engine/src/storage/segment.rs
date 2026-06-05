//! The on-disk segment (`.seg`) file format: binary serialization (`write_segment`)
//! and the mmap-backed read view (`MmapSegment`). Design: ADR-012.
//!
//! Invariant: a written segment file, when mmap'd back, produces identical match
//! results to the in-memory `Segment` it was serialized from. `MmapSegment::match_into`
//! is on the hot path (same role as `Segment::match_into`).
//!
//! This root holds the format primitives shared by the write and read submodules —
//! the magic/version/header constants, the frozen-hash-table [`FrozenSlot`], and the
//! 8-byte alignment helper — plus the submodule decls and the public re-exports
//! (`crate::storage::segment::{write_segment, MmapSegment}` stays byte-identical):
//!   - [`write`] — `write_segment` + the section serializers (in-memory → `.seg`)
//!   - [`read`]  — the section-reading byte helpers + frozen-table probe/parse
//!   - [`mmap`]  — the [`MmapSegment`] read view (struct + `open`) and its read/match
//!     surface (the [`mmap::ops`] submodule)

mod mmap;
mod read;
mod write;

pub use mmap::MmapSegment;
pub use write::write_segment;

// ---- constants ----

const MAGIC: [u8; 4] = *b"PERC";
// v1: original layout. v2 (ADR-020 Item 2): adds a sorted logical-index column
// section (logical_index_off at header bytes 56..64); v1 files are still read
// (the reverse index is reconstructed in memory on open). v3 (ADR-049): adds a
// per-query tag section (tag_section_off at header bytes 64..72) holding the SoA
// tag column behind filtered percolation; v1/v2 files are still read (their queries
// read back as untagged — an empty tag column).
const FORMAT_VERSION: u32 = 3;
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
//   64..72  tag_section_off (u64 LE; 0 or absent in v1/v2)
//   72..80  reserved (8 bytes, zeroed)

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

/// Align a byte offset up to the next 8-byte boundary.
fn align8(pos: u64) -> u64 {
    (pos + 7) & !7
}
