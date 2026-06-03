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
const MANIFEST_VERSION: u32 = 2;

/// Engine manifest — records the list of active segment files, dict state,
/// and counters. Written atomically (tmp + rename) alongside segment files.
pub struct Manifest {
    pub segment_files: Vec<String>,
    pub next_seg_id: u64,
    pub dict_data: Vec<u8>,
    /// `serialize_tagdict(tag dict)` — the frozen tag space (ADR-049). Empty when no
    /// tagged queries have been stored; a v1 manifest reads back empty.
    pub tag_dict_data: Vec<u8>,
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
    // v2: tag-dict blob (ADR-049; empty when no tags).
    write_u32(&mut f, manifest.tag_dict_data.len() as u32)?;
    f.write_all(&manifest.tag_dict_data)?;
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
    // v1 and v2 are both accepted; v2 appends `tag_dict_data` (ADR-049), absent in v1.
    if version != 1 && version != MANIFEST_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported manifest version {version} (expected 1 or {MANIFEST_VERSION})"),
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
    let dict_data = data
        .get(cursor..cursor + dict_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated dict blob"))?
        .to_vec();
    cursor += dict_len;
    // v2 appends the tag-dict blob; v1 has none (read back as empty).
    let tag_dict_data = if version >= 2 {
        let tlen = read_u32_at(&data, cursor)? as usize;
        cursor += 4;
        data.get(cursor..cursor + tlen)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated tag-dict blob"))?
            .to_vec()
    } else {
        Vec::new()
    };

    Ok(Manifest {
        segment_files,
        next_seg_id,
        dict_data,
        tag_dict_data,
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
// v3 (ADR-046): appends `vocab_data` — the serialized `Vocab` behind the current
// normalizer, so a runtime vocabulary change (an alias) survives reopen. A v2
// manifest reads back with an empty `vocab_data` (no installed vocab).
// v4 (ADR-049): appends `tag_dict_data` — the serialized frozen tag space (`TagDict`)
// behind filtered percolation, so interned tag ids survive reopen. A v2/v3 manifest
// reads back with an empty `tag_dict_data` (no tags).
const CLUSTER_MANIFEST_VERSION: u32 = 4;

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
    if !(2..=CLUSTER_MANIFEST_VERSION).contains(&version) {
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
}
