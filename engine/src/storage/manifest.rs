//! The engine manifest (`Manifest`) and the coordinator cluster manifest
//! (`ClusterManifest`) — binary, atomically-written cluster-state documents (the
//! atomic commit point for a checkpoint). ADR-014, ADR-031/032, ADR-046 (v3 vocab).

use std::io::{self, Write};
use std::path::Path;

use super::{crc32, read_u32_at, read_u64_at, write_u32, write_u64};
use publish::publish_with_crc;

mod cluster;
mod publish;
#[cfg(test)]
mod tests;

pub use cluster::{read_cluster_manifest, write_cluster_manifest, ClusterManifest};

// ---- Manifest file ----

const MANIFEST_MAGIC: [u8; 4] = *b"PMAN";
// v1: original layout. v2 (ADR-049): appends `tag_dict_data` — the serialized per-query
// tag space (`TagDict`) behind filtered percolation, so interned tag ids survive reopen.
// A v1 manifest reads back with an empty `tag_dict_data` (no tags).
// v3 (ADR-066): appends `wal_seq_watermark` + `segment_tombstones` — the per-segment
// dead-locals bitmaps (the Lucene `.liv` analogue), making base-segment tombstone state
// durable at the manifest commit point. Before v3, a base-segment delete lived ONLY in
// the in-RAM mmap alive-overlay + its WAL frame, so the flush-time WAL reset silently
// dropped it (the deleted query resurrected on reopen). A v1/v2 manifest reads back with
// watermark 0 and no bitmaps.
const MANIFEST_VERSION: u32 = 3;
// v4 (ADR-068): byte-identical layout to v3 — written ONLY while a registered segment
// holds class-D always-candidates, as the **rollback fence**. The fence must live HERE,
// not (only) in the segment file version: a pre-ADR-068 binary's recovery SKIPS an
// unreadable segment as corrupt (event + continue), which would silently drop the whole
// mixed segment — but an unsupported MANIFEST version fails `Engine::open` outright,
// the loud refusal rollback needs. A class-D-free commit keeps writing v3.
const MANIFEST_VERSION_CLASS_D: u32 = 4;
// v5 (ADR-105): written ONLY while a registered segment holds class-H hot-tier entries —
// the same manifest-level rollback fence as v4 (a pre-ADR-105 binary never probes the hot
// index, so a mixed corpus must refuse to open loudly rather than silently stop matching
// those queries). Unlike v4, v5 is NOT layout-identical: it appends `hot_anchor_theta`
// (the θ the hot entries were classified under — recorded for forensics/observability;
// the LIVE config stays authoritative for new classification, since an A↔H divergence is
// correctness-benign by the ADR-105 placement argument). Hot-free commits keep v3/v4.
const MANIFEST_VERSION_HOT: u32 = 5;
// v6 (ADR-116 hardening): written while any registered segment carries the v8
// source-generation column. Standalone recovery skips unreadable segment files,
// so the manifest itself must fence older binaries or they would silently serve
// a partial corpus. Layout is v5-compatible and always appends the recorded θ
// (zero when no hot tier is present).
const MANIFEST_VERSION_SOURCE_GENERATION: u32 = 6;
// v7 (ADR-121): appends the basename of an immutable source sidecar. The sidecar
// is fsync'd before the manifest rename, so this ONE commit point selects the
// exact segment registry and its complete canonical-source corpus together.
// Pre-v7 manifests keep selecting the legacy mutable `sources.dat`.
const MANIFEST_VERSION_SOURCE_COMMIT: u32 = 7;

/// Engine manifest — records the list of active segment files, dict state,
/// and counters. Written atomically (tmp + rename) alongside segment files.
pub struct Manifest {
    pub segment_files: Vec<String>,
    /// `true` ⇔ some registered segment holds class-D always-candidates (ADR-068).
    /// Not serialized as data — it selects the version word (v4 vs v3), the loud
    /// rollback fence. Set from the version on read.
    pub class_d_fence: bool,
    /// `true` ⇔ some registered segment holds class-H hot-tier entries (ADR-105).
    /// Selects the v5 version word (which outranks v4); set from the version on
    /// read. NOTE: a v5 manifest reads back `class_d_fence = true` conservatively —
    /// the write side always recomputes both fences from the live segments, so
    /// the read-back value is informational only.
    pub hot_fence: bool,
    /// `true` ⇔ some registered segment carries internal source generations.
    /// Selects manifest v6, the loud rollback fence for segment v8.
    pub source_generation_fence: bool,
    /// The hot-anchor threshold θ the corpus's class-H entries were classified
    /// under (ADR-105) — recorded in v5 manifests for forensics; 0 otherwise.
    /// The live `EngineConfig` stays authoritative for new classification.
    pub hot_anchor_theta: u32,
    pub next_seg_id: u64,
    pub dict_data: Vec<u8>,
    /// `serialize_tagdict(tag dict)` — the frozen tag space (ADR-049). Empty when no
    /// tagged queries have been stored; a v1 manifest reads back empty.
    pub tag_dict_data: Vec<u8>,
    pub rejected_parse: u64,
    pub rejected_class_d: u64,
    /// The WAL sequence number of the last entry whose effects this manifest commit
    /// has captured (ADR-066). On recovery, a positional `Tombstone` frame targeting a
    /// BASE segment with `seq <= wal_seq_watermark` is skipped: its effect is already
    /// in `segment_tombstones` (or its entry was dropped by a compaction merge), and
    /// the segment *positions* it addresses may have been renumbered since — replaying
    /// it could tombstone an unrelated query. Frames newer than the watermark address
    /// exactly this manifest's `segment_files` list (every segments-vec mutation
    /// commits a manifest), so they replay correctly. 0 = nothing captured (v1/v2).
    pub wal_seq_watermark: u64,
    /// Per-segment DEAD locals at commit time (ADR-066): `(segment_file_name,
    /// serialized RoaringBitmap of tombstoned local ids)`, recorded only for segments
    /// that carry tombstones. Applied on open after the segment is attached, BEFORE the
    /// WAL tail replays — so a delete against a base segment survives the flush-time
    /// WAL reset that previously dropped its only durable record.
    pub segment_tombstones: Vec<(String, Vec<u8>)>,
    /// Immutable source-sidecar basename selected by this commit (ADR-121).
    /// Pre-v7 manifests read back as the legacy `sources.dat`.
    pub source_file_name: String,
}

pub fn write_manifest(manifest: &Manifest, path: &Path) -> io::Result<()> {
    super::validate_sidecar_basename(&manifest.source_file_name)?;
    let tmp = path.with_extension("manifest.tmp");
    publish_with_crc(path, &tmp, |f| {
        f.write_all(&MANIFEST_MAGIC)?;
        write_u32(
            f,
            if manifest.source_file_name != "sources.dat" {
                MANIFEST_VERSION_SOURCE_COMMIT
            } else if manifest.source_generation_fence {
                MANIFEST_VERSION_SOURCE_GENERATION
            } else if manifest.hot_fence {
                MANIFEST_VERSION_HOT
            } else if manifest.class_d_fence {
                MANIFEST_VERSION_CLASS_D
            } else {
                MANIFEST_VERSION
            },
        )?;
        write_u64(f, manifest.next_seg_id)?;
        write_u64(f, manifest.rejected_parse)?;
        write_u64(f, manifest.rejected_class_d)?;
        // segment file list
        write_u32(f, manifest.segment_files.len() as u32)?;
        for name in &manifest.segment_files {
            let bytes = name.as_bytes();
            write_u32(f, bytes.len() as u32)?;
            f.write_all(bytes)?;
        }
        // dict blob
        write_u32(f, manifest.dict_data.len() as u32)?;
        f.write_all(&manifest.dict_data)?;
        // v2: tag-dict blob (ADR-049; empty when no tags).
        write_u32(f, manifest.tag_dict_data.len() as u32)?;
        f.write_all(&manifest.tag_dict_data)?;
        // v3 (ADR-066): WAL watermark + per-segment dead-locals bitmaps.
        write_u64(f, manifest.wal_seq_watermark)?;
        write_u32(f, manifest.segment_tombstones.len() as u32)?;
        for (name, bitmap) in &manifest.segment_tombstones {
            let nb = name.as_bytes();
            write_u32(f, nb.len() as u32)?;
            f.write_all(nb)?;
            write_u32(f, bitmap.len() as u32)?;
            f.write_all(bitmap)?;
        }
        // v5 (ADR-105): the recorded θ — appended ONLY under the hot fence, so hot-free
        // manifests stay byte-identical v3/v4.
        if manifest.hot_fence
            || manifest.source_generation_fence
            || manifest.source_file_name != "sources.dat"
        {
            write_u32(f, manifest.hot_anchor_theta)?;
        }
        // v7 (ADR-121): select the already-durable immutable source corpus in the
        // same atomic document as the segment registry.
        if manifest.source_file_name != "sources.dat" {
            let bytes = manifest.source_file_name.as_bytes();
            let len = u32::try_from(bytes.len()).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "source filename is too long")
            })?;
            write_u32(f, len)?;
            f.write_all(bytes)?;
        }
        Ok(())
    })
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
    // v1..=v7 are accepted; v2 appends `tag_dict_data` (ADR-049), v3 appends the WAL
    // watermark + per-segment dead-locals bitmaps (ADR-066), v4 is the class-D fence
    // (ADR-068), v5 appends the recorded θ under the hot fence (ADR-105), v6 is the
    // source-generation rollback fence, and v7 appends the selected immutable source
    // sidecar (ADR-121) — each absent in earlier versions.
    if !(1..=MANIFEST_VERSION_SOURCE_COMMIT).contains(&version) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unsupported manifest version {version} (expected 1..={MANIFEST_VERSION_SOURCE_COMMIT})"
            ),
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
        // Route through `data.get(..)` like the dict/tag-dict/tombstone reads below,
        // so a crafted (CRC-recomputed) `len` that overruns the buffer fails loud with
        // a typed `InvalidData` error instead of panicking on the slice index.
        let name = std::str::from_utf8(data.get(cursor..cursor + len).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "truncated segment filename")
        })?)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
        .to_string();
        cursor += len;
        segment_files.push(name);
    }

    let dict_len = read_u32_at(&data, cursor)? as usize;
    cursor += 4;
    let dict_data = data
        .get(cursor..cursor + dict_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated dict blob"))?
        .to_vec();
    cursor += dict_len;
    // v2 appends the tag-dict blob; v1 has none (read back as empty).
    let tag_dict_data = if version >= 2 {
        let tlen = read_u32_at(&data, cursor)? as usize;
        cursor += 4;
        let t = data
            .get(cursor..cursor + tlen)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated tag-dict blob"))?
            .to_vec();
        cursor += tlen;
        t
    } else {
        Vec::new()
    };
    // v3 appends the WAL watermark + per-segment dead-locals bitmaps (ADR-066); v1/v2
    // read back with watermark 0 and no bitmaps (their era had no durable record of
    // base-segment tombstones to restore).
    let (wal_seq_watermark, segment_tombstones) = if version >= 3 {
        let watermark = read_u64_at(&data, cursor)?;
        cursor += 8;
        let n = read_u32_at(&data, cursor)? as usize;
        cursor += 4;
        let mut tombs = Vec::with_capacity(n);
        for _ in 0..n {
            let nlen = read_u32_at(&data, cursor)? as usize;
            cursor += 4;
            let name = std::str::from_utf8(data.get(cursor..cursor + nlen).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "truncated tombstone filename")
            })?)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
            .to_string();
            cursor += nlen;
            let blen = read_u32_at(&data, cursor)? as usize;
            cursor += 4;
            let bitmap = data
                .get(cursor..cursor + blen)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "truncated tombstone bitmap")
                })?
                .to_vec();
            cursor += blen;
            tombs.push((name, bitmap));
        }
        (watermark, tombs)
    } else {
        (0, Vec::new())
    };
    // v5 appends the recorded θ (ADR-105); absent in earlier versions.
    let hot_anchor_theta = if version >= MANIFEST_VERSION_HOT {
        let t = read_u32_at(&data, cursor)?;
        cursor += 4;
        t
    } else {
        0
    };
    let source_file_name = if version >= MANIFEST_VERSION_SOURCE_COMMIT {
        let len = read_u32_at(&data, cursor)? as usize;
        cursor += 4;
        let end = cursor.checked_add(len).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid source filename length")
        })?;
        let name = std::str::from_utf8(content.get(cursor..end).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "truncated source filename")
        })?)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
        .to_string();
        cursor = end;
        super::validate_sidecar_basename(&name)?;
        name
    } else {
        "sources.dat".to_string()
    };
    if cursor != content.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected trailing manifest data",
        ));
    }

    Ok(Manifest {
        segment_files,
        class_d_fence: version >= MANIFEST_VERSION_CLASS_D,
        hot_fence: version == MANIFEST_VERSION_HOT
            || (version >= MANIFEST_VERSION_SOURCE_GENERATION && hot_anchor_theta != 0),
        source_generation_fence: version >= MANIFEST_VERSION_SOURCE_GENERATION,
        hot_anchor_theta,
        next_seg_id,
        dict_data,
        tag_dict_data,
        rejected_parse,
        rejected_class_d,
        wal_seq_watermark,
        segment_tombstones,
        source_file_name,
    })
}
