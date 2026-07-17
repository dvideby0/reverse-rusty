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
// read back as untagged — an empty tag column). v4 (ADR-068): byte-identical layout
// to v3 — written ONLY for a segment holding ≥1 class-D always-candidate, as a
// **rollback fence**: a pre-ADR-068 binary opens v3 fine but never probes the
// universal signature, so a v4 file's class-D queries would silently stop matching;
// the version bump makes that reader fail loudly ("unsupported format version")
// instead. Class-D-free segments keep writing v3, so rollback stays clean for
// anyone who never enabled the lane. v5 (ADR-105): adds the HOT-TIER index
// section (hot_index_off in the previously-reserved header bytes 72..80; class
// byte 4 = class H in the meta section) — written ONLY for a segment holding ≥1
// class-H entry, doubling as the same kind of rollback fence: a pre-ADR-105
// binary never probes the hot index, so its entries would silently stop
// matching; the unsupported-version refusal makes that loud. Hot-free segments
// keep writing v3/v4 byte-identically. v6 (ADR-108) appends one signed i64
// priority per exact row, but is written only when at least one value is non-zero;
// old v1-v5 rows lower their cached legacy priority tag at open/compaction time.
const FORMAT_VERSION: u32 = 3;
const FORMAT_VERSION_CLASS_D: u32 = 4;
const FORMAT_VERSION_HOT: u32 = 5;
/// v6 (ADR-108): exact section appends one `i64` priority per query. Written
/// only when at least one value is non-zero, so all-zero segments retain their
/// previous v3/v4/v5 rollback behavior and bytes.
const FORMAT_VERSION_RANK: u32 = 6;
/// v7 (ADR-109): exact section appends allocation-free distributed placement
/// columns. Standalone segments without placement continue to write v1-v6.
const FORMAT_VERSION_OWNERSHIP: u32 = 7;
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
//   72..80  hot_index_off (u64 LE; v5 — 0 or absent pre-v5: those bytes were
//            reserved-zero, so a legacy header reads back as "no hot section")

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

#[cfg(test)]
mod tests {
    use super::*;

    fn read_u32(bytes: &[u8], offset: usize) -> u32 {
        u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("u32 field"))
    }

    fn skip_array(bytes: &[u8], offset: usize, width: usize) -> usize {
        let count = read_u32(bytes, offset) as usize;
        let data = if width == 8 { offset + 8 } else { offset + 4 };
        align8((data + count * width) as u64) as usize
    }

    fn reseal(bytes: &mut [u8]) {
        let body = bytes.len() - 4;
        let crc = crate::storage::crc32(&bytes[..body]);
        bytes[body..].copy_from_slice(&crc.to_le_bytes());
    }

    fn ownership_offsets(bytes: &[u8]) -> (usize, usize) {
        let mut cursor =
            u64::from_le_bytes(bytes[16..24].try_into().expect("exact offset")) as usize;
        // Required/forbidden masks through logical ids.
        for width in [8usize, 8, 4, 2, 4, 4, 2, 4, 4, 2, 4, 2, 4, 4, 8] {
            cursor = skip_array(bytes, cursor, width);
        }
        cursor = skip_array(bytes, cursor, 8); // priority (always present in v7)
        let generation_count = cursor;
        cursor = skip_array(bytes, cursor, 8);
        cursor = skip_array(bytes, cursor, 4); // placement num_shards
        let mode_count = cursor;
        (generation_count, mode_count)
    }

    fn v7_segment_bytes() -> (std::path::PathBuf, Vec<u8>) {
        let norm = crate::normalize::Normalizer::default_vocab().expect("normalizer");
        let mut dict = crate::dict::Dict::new();
        let mut lc = String::new();
        let ast = crate::dsl::parse("1994 topps").expect("query");
        let ex = crate::compile::extract(&ast, &norm, &mut dict, &mut lc);
        dict.finalize_mask();
        let placement = crate::ownership::QueryPlacement::selective(
            crate::ownership::PlacementGeneration(3),
            4,
            vec![2],
        )
        .expect("placement");
        let mut segment = crate::segment::Segment::new();
        segment
            .add_compiled_ranked_placed(
                &ex,
                &[],
                &dict,
                99,
                7,
                crate::rank::RankValues::default(),
                &placement,
                crate::segment::CompileKnobs {
                    accept_class_d: true,
                    hot_anchor_threshold: 0,
                    dedup_bodies: true,
                },
            )
            .expect("accepted query");
        let dir = std::env::temp_dir().join(format!(
            "rr_segment_v7_{}_{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join("ownership.seg");
        write_segment(&segment, &path).expect("write v7 segment");
        let bytes = std::fs::read(&path).expect("read segment");
        (path, bytes)
    }

    #[test]
    fn v7_ownership_columns_round_trip_and_malformed_columns_fail_loud() {
        let (path, original) = v7_segment_bytes();
        assert_eq!(read_u32(&original, 4), FORMAT_VERSION_OWNERSHIP);
        {
            let mmap = MmapSegment::open(&path).expect("open v7");
            let placement = mmap.placement(0);
            assert_eq!(
                placement.generation,
                crate::ownership::PlacementGeneration(3)
            );
            assert_eq!(placement.num_shards, 4);
            assert_eq!(placement.positions, &[2]);
        }

        let (_, mode_count) = ownership_offsets(&original);
        let mut bad_mode = original.clone();
        bad_mode[mode_count + 4] = 0xff;
        reseal(&mut bad_mode);
        std::fs::write(&path, bad_mode).expect("write bad mode");
        let error = MmapSegment::open(&path).expect_err("unknown mode must fail");
        assert!(error.to_string().contains("UnknownMode"), "got: {error}");

        let (generation_count, _) = ownership_offsets(&original);
        let mut bad_len = original;
        bad_len[generation_count..generation_count + 4].copy_from_slice(&0u32.to_le_bytes());
        reseal(&mut bad_len);
        std::fs::write(&path, bad_len).expect("write bad column length");
        let error = MmapSegment::open(&path).expect_err("column mismatch must fail");
        assert!(
            error
                .to_string()
                .contains("ownership column length mismatch"),
            "got: {error}"
        );

        if let Some(dir) = path.parent() {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}
