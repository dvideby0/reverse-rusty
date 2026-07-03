//! The engine manifest (`Manifest`) and the coordinator cluster manifest
//! (`ClusterManifest`) — binary, atomically-written cluster-state documents (the
//! atomic commit point for a checkpoint). ADR-014, ADR-031/032, ADR-046 (v3 vocab).

use std::io::{self, Write};
use std::path::Path;

use super::{crc32, durable_rename, read_u32_at, read_u64_at, write_u32, write_u64};

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
}

pub fn write_manifest(manifest: &Manifest, path: &Path) -> io::Result<()> {
    let tmp = path.with_extension("manifest.tmp");
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(&MANIFEST_MAGIC)?;
    write_u32(
        &mut f,
        if manifest.hot_fence {
            MANIFEST_VERSION_HOT
        } else if manifest.class_d_fence {
            MANIFEST_VERSION_CLASS_D
        } else {
            MANIFEST_VERSION
        },
    )?;
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
    // v2: tag-dict blob (ADR-049; empty when no tags).
    write_u32(&mut f, manifest.tag_dict_data.len() as u32)?;
    f.write_all(&manifest.tag_dict_data)?;
    // v3 (ADR-066): WAL watermark + per-segment dead-locals bitmaps.
    write_u64(&mut f, manifest.wal_seq_watermark)?;
    write_u32(&mut f, manifest.segment_tombstones.len() as u32)?;
    for (name, bitmap) in &manifest.segment_tombstones {
        let nb = name.as_bytes();
        write_u32(&mut f, nb.len() as u32)?;
        f.write_all(nb)?;
        write_u32(&mut f, bitmap.len() as u32)?;
        f.write_all(bitmap)?;
    }
    // v5 (ADR-105): the recorded θ — appended ONLY under the hot fence, so hot-free
    // manifests stay byte-identical v3/v4.
    if manifest.hot_fence {
        write_u32(&mut f, manifest.hot_anchor_theta)?;
    }
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
    // v1..=v5 are accepted; v2 appends `tag_dict_data` (ADR-049), v3 appends the WAL
    // watermark + per-segment dead-locals bitmaps (ADR-066), v4 is the class-D fence
    // (ADR-068), and v5 appends the recorded θ under the hot fence (ADR-105) — each
    // absent in earlier versions.
    if !(1..=MANIFEST_VERSION_HOT).contains(&version) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported manifest version {version} (expected 1..={MANIFEST_VERSION_HOT})"),
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
        let _ = cursor;
        t
    } else {
        0
    };

    Ok(Manifest {
        segment_files,
        class_d_fence: version >= MANIFEST_VERSION_CLASS_D,
        hot_fence: version >= MANIFEST_VERSION_HOT,
        hot_anchor_theta,
        next_seg_id,
        dict_data,
        tag_dict_data,
        rejected_parse,
        rejected_class_d,
        wal_seq_watermark,
        segment_tombstones,
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
// v3 (ADR-046): appends `vocab_data` — the serialized `Vocab` behind the current
// normalizer, so a runtime vocabulary change (an alias) survives reopen. A v2
// manifest reads back with an empty `vocab_data` (no installed vocab).
// v4 (ADR-049): appends `tag_dict_data` — the serialized frozen tag space (`TagDict`)
// behind filtered percolation, so interned tag ids survive reopen. A v2/v3 manifest
// reads back with an empty `tag_dict_data` (no tags).
const CLUSTER_MANIFEST_VERSION: u32 = 4;
// v5 (ADR-080): the replicate-to-all broad-layout marker (layout-identical to v4 — the version
// word IS the marker). The broad lane (class C + B-arity-2 + opt-in class D) now lives on EVERY
// shard, evaluated on one broad-eval shard per title (not pinned to shard 0), so EVERY ADR-080
// durable cluster writes v5. A TWO-WAY fence, both halves load-bearing for zero false negatives
// (the cluster has no per-shard manifest — segments-only durable, ADR-032 — so it must live here):
//   (1) ROLLBACK — a pre-ADR-080 binary accepts only v2..=4 and fails `ClusterEngine::open` on v5,
//       so it never places broad on shard 0 only (which the new rotating routing would mis-read)
//       and never silently drops class-D (it has no universal-signature probe).
//   (2) FORWARD — the new binary refuses to OPEN a v<5 cluster, whose broad lives on shard 0 only
//       and would be mis-routed by the rotating broad-eval shard. Such a cluster must be rebuilt.
const CLUSTER_MANIFEST_VERSION_REPLICATE_ALL: u32 = 5;

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
    /// `true` ⇔ this cluster uses the ADR-080 replicate-to-all broad layout (broad on every
    /// shard, evaluated on one broad-eval shard per title) — every ADR-080 cluster sets it. Not
    /// serialized as data — it selects the version word (v5 vs v4), the two-way fence
    /// `ClusterEngine::open` requires (a v<5 / legacy-layout cluster is refused). Set from the
    /// version on read.
    pub broad_replicate_all: bool,
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
    /// The serialized [`Vocab`](crate::vocab::Vocab) behind the current normalizer
    /// (ADR-046), or empty when the cluster was built directly from a `Normalizer`
    /// with no runtime vocabulary change. On reopen, a non-empty blob rebuilds the
    /// normalizer so a declared alias survives the restart. Written by v3; a v2
    /// manifest reads back as empty.
    pub vocab_data: Vec<u8>,
    /// `serialize_tagdict(frozen tag dict)` — the authoritative tag space behind
    /// filtered percolation (ADR-049), so reopened shards resolve `(key,value)` tags to
    /// the SAME `TagId`s. Written by v4; a v2/v3 manifest reads back as empty (no tags).
    pub tag_dict_data: Vec<u8>,
}

pub fn write_cluster_manifest(manifest: &ClusterManifest, path: &Path) -> io::Result<()> {
    let tmp = path.with_extension("cmanifest.tmp");
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(&CLUSTER_MANIFEST_MAGIC)?;
    write_u32(
        &mut f,
        if manifest.broad_replicate_all {
            CLUSTER_MANIFEST_VERSION_REPLICATE_ALL
        } else {
            CLUSTER_MANIFEST_VERSION
        },
    )?;
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
    // v3: the serialized vocab (empty when none installed).
    write_u32(&mut f, manifest.vocab_data.len() as u32)?;
    f.write_all(&manifest.vocab_data)?;
    // v4: the serialized tag dict (empty when no tags; ADR-049).
    write_u32(&mut f, manifest.tag_dict_data.len() as u32)?;
    f.write_all(&manifest.tag_dict_data)?;
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
    // v2, v3 and v4 are accepted; v3 appends `vocab_data` (ADR-046) and v4 appends
    // `tag_dict_data` (ADR-049), each absent in the earlier versions.
    if !(2..=CLUSTER_MANIFEST_VERSION_REPLICATE_ALL).contains(&version) {
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
    cursor += dict_len;
    // v3 appends the serialized vocab; v2 has none (read back as empty).
    let vocab_data = if version >= 3 {
        let vlen = read_u32_at(&data, cursor)? as usize;
        cursor += 4;
        let v = data
            .get(cursor..cursor + vlen)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated vocab blob"))?
            .to_vec();
        cursor += vlen;
        v
    } else {
        Vec::new()
    };
    // v4 appends the serialized tag dict; v2/v3 have none (read back as empty).
    let tag_dict_data = if version >= 4 {
        let tlen = read_u32_at(&data, cursor)? as usize;
        cursor += 4;
        data.get(cursor..cursor + tlen)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated tag-dict blob"))?
            .to_vec()
    } else {
        Vec::new()
    };

    Ok(ClusterManifest {
        epoch,
        snapshot_pos,
        dict_fingerprint,
        num_shards,
        vnodes,
        include_broad,
        broad_replicate_all: version >= CLUSTER_MANIFEST_VERSION_REPLICATE_ALL,
        segment_registry,
        next_seg_ids,
        dict_data,
        vocab_data,
        tag_dict_data,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The v3 engine manifest round-trips the WAL watermark + per-segment dead-locals
    /// bitmaps (ADR-066) alongside every earlier field.
    #[test]
    fn engine_manifest_v3_round_trips_watermark_and_tombstones() {
        let dir = std::env::temp_dir().join(format!("rr_manifest_v3_rt_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("manifest.bin");

        let manifest = Manifest {
            segment_files: vec!["seg_000001.seg".to_string(), "seg_000002.seg".to_string()],
            class_d_fence: false,
            hot_fence: false,
            hot_anchor_theta: 0,
            next_seg_id: 3,
            dict_data: vec![1, 2, 3],
            tag_dict_data: vec![4, 5],
            rejected_parse: 7,
            rejected_class_d: 9,
            wal_seq_watermark: 42,
            segment_tombstones: vec![("seg_000001.seg".to_string(), vec![10, 20, 30])],
        };
        write_manifest(&manifest, &path).expect("write");
        let got = read_manifest(&path).expect("read");
        assert_eq!(got.segment_files, manifest.segment_files);
        assert_eq!(got.next_seg_id, manifest.next_seg_id);
        assert_eq!(got.dict_data, manifest.dict_data);
        assert_eq!(got.tag_dict_data, manifest.tag_dict_data);
        assert_eq!(got.rejected_parse, manifest.rejected_parse);
        assert_eq!(got.rejected_class_d, manifest.rejected_class_d);
        assert_eq!(got.wal_seq_watermark, 42);
        assert_eq!(got.segment_tombstones, manifest.segment_tombstones);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A v2 manifest (written by a pre-ADR-066 binary) reads back with watermark 0 and
    /// no tombstone bitmaps. Hand-rolled bytes so the pin is at the format level, not
    /// against our own writer.
    #[test]
    fn engine_manifest_v2_reads_back_without_v3_section() {
        let dir = std::env::temp_dir().join(format!("rr_manifest_v2_bc_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("manifest.bin");

        let mut bytes: Vec<u8> = Vec::new();
        bytes.extend_from_slice(b"PMAN");
        bytes.extend_from_slice(&2u32.to_le_bytes()); // version 2
        bytes.extend_from_slice(&5u64.to_le_bytes()); // next_seg_id
        bytes.extend_from_slice(&1u64.to_le_bytes()); // rejected_parse
        bytes.extend_from_slice(&2u64.to_le_bytes()); // rejected_class_d
        bytes.extend_from_slice(&1u32.to_le_bytes()); // 1 segment file
        let name = b"seg_000001.seg";
        bytes.extend_from_slice(&(name.len() as u32).to_le_bytes());
        bytes.extend_from_slice(name);
        bytes.extend_from_slice(&3u32.to_le_bytes()); // dict blob
        bytes.extend_from_slice(&[7, 8, 9]);
        bytes.extend_from_slice(&0u32.to_le_bytes()); // empty tag-dict blob
        let crc = crc32(&bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());
        std::fs::write(&path, &bytes).expect("write v2 bytes");

        let got = read_manifest(&path).expect("read v2");
        assert_eq!(got.segment_files, vec!["seg_000001.seg".to_string()]);
        assert_eq!(got.next_seg_id, 5);
        assert_eq!(got.dict_data, vec![7, 8, 9]);
        assert!(got.tag_dict_data.is_empty());
        assert_eq!(got.wal_seq_watermark, 0, "v2 has no watermark");
        assert!(got.segment_tombstones.is_empty(), "v2 has no bitmaps");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Tier-D (defense-in-depth): a crafted segment-filename length prefix that overruns
    /// the manifest buffer must fail loud with a typed `InvalidData` error, not panic on a
    /// slice index — matching the dict/tag-dict/tombstone reads in `read_manifest`. Only
    /// reachable via tampering that also recomputes the trailing CRC, which this forges.
    #[test]
    fn manifest_segment_filename_length_overrun_fails_loud() {
        let dir = std::env::temp_dir().join(format!("rr_manifest_fn_ovr_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("manifest.bin");

        let mut bytes: Vec<u8> = Vec::new();
        bytes.extend_from_slice(b"PMAN");
        bytes.extend_from_slice(&2u32.to_le_bytes()); // version 2 (simplest layout)
        bytes.extend_from_slice(&5u64.to_le_bytes()); // next_seg_id
        bytes.extend_from_slice(&0u64.to_le_bytes()); // rejected_parse
        bytes.extend_from_slice(&0u64.to_le_bytes()); // rejected_class_d
        bytes.extend_from_slice(&1u32.to_le_bytes()); // 1 segment file
                                                      // A length prefix far larger than any remaining bytes → would index out of bounds.
        bytes.extend_from_slice(&1_000_000u32.to_le_bytes());
        bytes.extend_from_slice(b"seg_000001.seg"); // only 14 bytes actually present
                                                    // Re-seal the trailing whole-file CRC so the CRC gate passes and the structural
                                                    // guard is what fires.
        let crc = crc32(&bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());
        std::fs::write(&path, &bytes).expect("write forged bytes");

        match read_manifest(&path) {
            Err(e) => assert_eq!(e.kind(), io::ErrorKind::InvalidData, "got: {e}"),
            Ok(_) => panic!("overrunning segment-filename length must fail loud"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The v4 cluster manifest's nested per-shard registry + next-seg-id columns + the
    /// appended vocab and tag-dict blobs must round-trip byte-exactly (varied per-shard
    /// file counts, including an empty shard). The hand-rolled length-prefixed encoding is
    /// easy to get cursor-wrong, so pin it.
    #[test]
    fn cluster_manifest_v4_round_trips_registry_vocab_and_tagdict() {
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
            broad_replicate_all: false,
            segment_registry: vec![
                vec!["seg_000001.seg".to_string(), "seg_000004.seg".to_string()],
                vec![], // an empty shard (no committed segments)
                vec!["seg_000002.seg".to_string()],
            ],
            next_seg_ids: vec![5, 1, 3],
            dict_data: vec![1, 2, 3, 4, 5],
            vocab_data: vec![9, 8, 7, 6], // a non-empty (opaque) vocab blob — the v3 field
            tag_dict_data: vec![11, 22, 33], // a non-empty (opaque) tag-dict blob — the v4 field
        };
        write_cluster_manifest(&manifest, &path).expect("write");
        let got = read_cluster_manifest(&path).expect("read");

        assert_eq!(got.epoch, manifest.epoch);
        assert_eq!(got.snapshot_pos, manifest.snapshot_pos);
        assert_eq!(got.dict_fingerprint, manifest.dict_fingerprint);
        assert_eq!(got.num_shards, manifest.num_shards);
        assert_eq!(got.vnodes, manifest.vnodes);
        assert_eq!(got.include_broad, manifest.include_broad);
        assert_eq!(got.broad_replicate_all, manifest.broad_replicate_all);
        // A non-replicate-all (legacy-shaped) manifest writes the v4 version word.
        let raw = std::fs::read(&path).expect("read raw for version");
        assert_eq!(
            read_u32_at(&raw, 4).unwrap(),
            4,
            "broad_replicate_all=false ⇒ v4"
        );
        assert_eq!(got.segment_registry, manifest.segment_registry);
        assert_eq!(got.next_seg_ids, manifest.next_seg_ids);
        assert_eq!(got.dict_data, manifest.dict_data);
        assert_eq!(got.vocab_data, manifest.vocab_data);
        assert_eq!(got.tag_dict_data, manifest.tag_dict_data);

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

    /// The cluster v5 ADR-080 replicate-to-all marker: an ADR-080 commit writes the v5 version
    /// word (round-tripping `broad_replicate_all`), and a pre-ADR-080 binary — modeled by a
    /// forged FUTURE version — fails loud rather than mis-decoding (the loud refusal the two-way
    /// fence exists for; the cluster has no per-shard manifest to carry it instead).
    #[test]
    fn cluster_manifest_v5_replicate_all_marker_round_trips_and_unknown_version_fails_loud() {
        let dir = std::env::temp_dir().join(format!("rr_cmanifest_v5_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("cluster_manifest_v5.bin");

        let manifest = ClusterManifest {
            epoch: 3,
            snapshot_pos: 9,
            dict_fingerprint: 0xABCD,
            num_shards: 4,
            vnodes: 64,
            include_broad: true,
            broad_replicate_all: true, // an ADR-080 replicate-to-all cluster
            segment_registry: vec![vec![], vec![], vec![], vec![]],
            next_seg_ids: vec![1, 1, 1, 1],
            dict_data: vec![1, 2, 3],
            vocab_data: Vec::new(),
            tag_dict_data: Vec::new(),
        };
        write_cluster_manifest(&manifest, &path).expect("write");

        let raw = std::fs::read(&path).expect("read raw");
        assert_eq!(
            read_u32_at(&raw, 4).unwrap(),
            5,
            "ADR-080 replicate-to-all ⇒ cluster manifest v5"
        );
        let got = read_cluster_manifest(&path).expect("read");
        assert!(got.broad_replicate_all, "v5 reads back as replicate-to-all");
        assert_eq!(got.segment_registry, manifest.segment_registry);

        // Forge a v6 (future) version word + re-seal the trailing whole-file CRC, so the
        // version range check is what fires: an unsupported version must error.
        let mut bytes = raw.clone();
        bytes[4..8].copy_from_slice(&6u32.to_le_bytes());
        let body = bytes.len() - 4;
        let crc = crc32(&bytes[..body]);
        bytes[body..].copy_from_slice(&crc.to_le_bytes());
        std::fs::write(&path, &bytes).expect("rewrite");
        match read_cluster_manifest(&path) {
            Err(e) => assert!(
                e.to_string()
                    .contains("unsupported cluster manifest version"),
                "got: {e}"
            ),
            Ok(_) => panic!("future cluster manifest version must fail loud"),
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}
