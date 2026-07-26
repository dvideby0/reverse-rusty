//! Coordinator cluster-manifest schema, codec, validation, and recovery policy.

use std::io::{self, Write};
use std::path::Path;

use super::super::{
    crc32, read_u32_at, read_u64_at, validate_sidecar_basename, write_u32, write_u64,
    CURRENT_COMPILER_SEMANTICS_VERSION,
};
use super::publish::publish_with_crc;

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
/// v6 (ADR-109): appends the monotonic placement generation and fences out
/// durable clusters whose segments do not carry emission-ownership metadata.
const CLUSTER_MANIFEST_VERSION_OWNERSHIP: u32 = 6;
/// v7 (ADR-118): appends the compiler-semantics generation plus one
/// manifest-selected source-sidecar basename per shard. The marker covers
/// uncheckpointed coordinator-log tails even when the committed segment base is
/// empty; the source names make a blue/green rebuild's source corpus atomic
/// with the segment registry.
const CLUSTER_MANIFEST_VERSION_COMPILER_SEMANTICS: u32 = 7;

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
    /// Logical placement generation shared by every registered segment row.
    pub placement_generation: crate::ownership::PlacementGeneration,
    /// Per-shard committed base: `segment_registry[i]` is the list of `.seg` filenames
    /// (relative to `shard_<i>/segments/`) that constitute shard `i`'s base. This is the
    /// atomic-commit replacement for the v1 raw-DSL snapshot — on open a shard
    /// attaches-and-mmaps exactly these instead of re-ingesting (ADR-032).
    pub segment_registry: Vec<Vec<String>>,
    /// Per-shard next segment-id counter (parallel to `segment_registry`), so a flush
    /// after reopen never reuses/clobbers a committed segment filename.
    pub next_seg_ids: Vec<u64>,
    /// Compiler lowering semantics for the committed base and coordinator-log
    /// tail. A v6 manifest reads as zero and is rebuilt before serving.
    pub compiler_semantics_version: u32,
    /// Per-shard source-sidecar basenames, selected by the same atomic commit as
    /// `segment_registry`. A v6 manifest defaults each shard to `sources.dat`.
    pub source_files: Vec<String>,
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
    if manifest.segment_registry.len() != manifest.num_shards as usize
        || manifest.next_seg_ids.len() != manifest.num_shards as usize
        || manifest.source_files.len() != manifest.num_shards as usize
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cluster manifest per-shard columns do not match num_shards",
        ));
    }
    if manifest.compiler_semantics_version != CURRENT_COMPILER_SEMANTICS_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "cluster manifest compiler semantics {} does not equal current {}",
                manifest.compiler_semantics_version, CURRENT_COMPILER_SEMANTICS_VERSION
            ),
        ));
    }
    for name in &manifest.source_files {
        validate_sidecar_basename(name)?;
    }
    let tmp = path.with_extension("cmanifest.tmp");
    publish_with_crc(path, &tmp, |f| {
        f.write_all(&CLUSTER_MANIFEST_MAGIC)?;
        write_u32(f, CLUSTER_MANIFEST_VERSION_COMPILER_SEMANTICS)?;
        write_u64(f, manifest.epoch)?;
        write_u64(f, manifest.snapshot_pos)?;
        write_u64(f, manifest.dict_fingerprint)?;
        write_u32(f, manifest.num_shards)?;
        write_u32(f, manifest.vnodes)?;
        f.write_all(&[u8::from(manifest.include_broad)])?;
        // Per-shard segment registry: outer count, then each shard's filename list.
        write_u32(f, manifest.segment_registry.len() as u32)?;
        for files in &manifest.segment_registry {
            write_u32(f, files.len() as u32)?;
            for name in files {
                let b = name.as_bytes();
                write_u32(f, b.len() as u32)?;
                f.write_all(b)?;
            }
        }
        // Per-shard next-seg-id counters (parallel to the registry).
        write_u32(f, manifest.next_seg_ids.len() as u32)?;
        for &id in &manifest.next_seg_ids {
            write_u64(f, id)?;
        }
        write_u32(f, manifest.dict_data.len() as u32)?;
        f.write_all(&manifest.dict_data)?;
        // v3: the serialized vocab (empty when none installed).
        write_u32(f, manifest.vocab_data.len() as u32)?;
        f.write_all(&manifest.vocab_data)?;
        // v4: the serialized tag dict (empty when no tags; ADR-049).
        write_u32(f, manifest.tag_dict_data.len() as u32)?;
        f.write_all(&manifest.tag_dict_data)?;
        write_u64(f, manifest.placement_generation.0)?;
        write_u32(f, manifest.compiler_semantics_version)?;
        write_u32(f, manifest.source_files.len() as u32)?;
        for name in &manifest.source_files {
            let bytes = name.as_bytes();
            write_u32(f, bytes.len() as u32)?;
            f.write_all(bytes)?;
        }
        Ok(())
    })
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
    if content[0..4] != CLUSTER_MANIFEST_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad cluster manifest magic",
        ));
    }
    let version = read_u32_at(content, 4)?;
    // ADR-109 is a rebuild-only cluster migration: v1-v5 have no durable emission-owner
    // generation, while a future version must never be guessed at.
    if !(CLUSTER_MANIFEST_VERSION_OWNERSHIP..=CLUSTER_MANIFEST_VERSION_COMPILER_SEMANTICS)
        .contains(&version)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            if version < CLUSTER_MANIFEST_VERSION_OWNERSHIP {
                format!(
                    "cluster manifest v{version} predates ADR-109 ownership metadata; rebuild the durable cluster with this binary"
                )
            } else {
                format!("unsupported cluster manifest version {version}")
            },
        ));
    }
    let mut cursor = 8usize;
    let epoch = read_u64_at(content, cursor)?;
    cursor += 8;
    let snapshot_pos = read_u64_at(content, cursor)?;
    cursor += 8;
    let dict_fingerprint = read_u64_at(content, cursor)?;
    cursor += 8;
    let num_shards = read_u32_at(content, cursor)?;
    cursor += 4;
    let vnodes = read_u32_at(content, cursor)?;
    cursor += 4;
    let include_broad = *content
        .get(cursor)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated cluster manifest"))?
        != 0;
    cursor += 1;
    // Per-shard segment registry (outer count, then each shard's filename list).
    let shard_count = read_u32_at(content, cursor)? as usize;
    cursor += 4;
    let mut segment_registry: Vec<Vec<String>> = Vec::with_capacity(shard_count);
    for _ in 0..shard_count {
        let nfiles = read_u32_at(content, cursor)? as usize;
        cursor += 4;
        let mut files = Vec::with_capacity(nfiles);
        for _ in 0..nfiles {
            let len = read_u32_at(content, cursor)? as usize;
            cursor += 4;
            let name = std::str::from_utf8(content.get(cursor..cursor + len).ok_or_else(|| {
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
    let nids = read_u32_at(content, cursor)? as usize;
    cursor += 4;
    let mut next_seg_ids = Vec::with_capacity(nids);
    for _ in 0..nids {
        next_seg_ids.push(read_u64_at(content, cursor)?);
        cursor += 8;
    }
    let dict_len = read_u32_at(content, cursor)? as usize;
    cursor += 4;
    let dict_data = content
        .get(cursor..cursor + dict_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated dict blob"))?
        .to_vec();
    cursor += dict_len;
    // v3 appends the serialized vocab; v2 has none (read back as empty).
    let vocab_data = if version >= 3 {
        let vlen = read_u32_at(content, cursor)? as usize;
        cursor += 4;
        let v = content
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
        let tlen = read_u32_at(content, cursor)? as usize;
        cursor += 4;
        let tags = content
            .get(cursor..cursor + tlen)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated tag-dict blob"))?
            .to_vec();
        cursor += tlen;
        tags
    } else {
        Vec::new()
    };
    let placement_generation = crate::ownership::PlacementGeneration(read_u64_at(content, cursor)?);
    cursor += 8;
    if placement_generation == crate::ownership::PlacementGeneration::STANDALONE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "cluster manifest has standalone placement generation zero",
        ));
    }

    let (compiler_semantics_version, source_files) =
        if version >= CLUSTER_MANIFEST_VERSION_COMPILER_SEMANTICS {
            let semantics = read_u32_at(content, cursor)?;
            cursor += 4;
            if semantics > CURRENT_COMPILER_SEMANTICS_VERSION {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsupported compiler semantics version {semantics}"),
                ));
            }
            let count = read_u32_at(content, cursor)? as usize;
            cursor += 4;
            let mut names = Vec::with_capacity(count);
            for _ in 0..count {
                let len = read_u32_at(content, cursor)? as usize;
                cursor += 4;
                let raw = content.get(cursor..cursor + len).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "truncated source filename")
                })?;
                cursor += len;
                let name = std::str::from_utf8(raw)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                    .to_string();
                validate_sidecar_basename(&name)?;
                names.push(name);
            }
            (semantics, names)
        } else {
            (0, vec!["sources.dat".to_string(); num_shards as usize])
        };
    let expected_shards = num_shards as usize;
    if segment_registry.len() != expected_shards
        || next_seg_ids.len() != expected_shards
        || source_files.len() != expected_shards
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "cluster manifest per-shard columns do not match {num_shards} shards \
                 (registry={}, next_ids={}, sources={})",
                segment_registry.len(),
                next_seg_ids.len(),
                source_files.len(),
            ),
        ));
    }
    if cursor != content.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "cluster manifest has trailing bytes",
        ));
    }

    Ok(ClusterManifest {
        epoch,
        snapshot_pos,
        dict_fingerprint,
        num_shards,
        vnodes,
        include_broad,
        broad_replicate_all: version >= CLUSTER_MANIFEST_VERSION_REPLICATE_ALL,
        placement_generation,
        segment_registry,
        next_seg_ids,
        compiler_semantics_version,
        source_files,
        dict_data,
        vocab_data,
        tag_dict_data,
    })
}
