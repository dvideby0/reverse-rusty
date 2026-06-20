//! Manifest-driven, atomic directory backup for the single-node engine and the
//! cluster coordinator (ADR-079, the mechanism behind ADR-065 criterion 11).
//!
//! ## Why this is not a plain `cp -r`
//! A *live* hot-copy of a `data_dir` is unsafe: a concurrent flush/compaction
//! commits a new manifest and then deletes the superseded `.seg` files
//! (`cleanup_segment_files` at the end of `do_compact_range`). An external copier
//! that reads the manifest and then copies segments can race that deletion, so the
//! copied manifest references files the copy missed. These helpers are therefore
//! invoked BY the engine while it holds its own write-path exclusion (so no
//! compaction can run), and they copy exactly the files the just-committed manifest
//! names — orphan `.seg` files left by an earlier crashed compaction are skipped.
//!
//! ## Restore
//! There is no restore code here: restore is the existing `Engine::open` /
//! `ClusterEngine::open` pointed at the (relocated) backup directory. These helpers
//! only produce a consistent on-disk snapshot.
//!
//! ## Atomicity of the backup itself
//! Everything is staged into a sibling `<dest>.backup.tmp` directory, fsync'd, and
//! renamed into place. A crash mid-backup leaves only the staging dir (removed on
//! the next attempt), never a half-populated `dest`. Within the staging dir the
//! manifest is written LAST, mirroring the engine's own "build durable, then
//! commit" discipline, so the staged tree is internally consistent before the
//! rename.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use super::{load_query_sources, read_cluster_manifest, read_manifest, MmapSegment};

// On-disk filenames. Mirrored from the writers (single-node:
// segment/persistence.rs + segment/lifecycle/{construct,recovery}.rs; cluster:
// cluster/coordinator.rs's CLUSTER_MANIFEST_FILE/CLUSTER_LOG_FILE + shard_dir).
// Kept local (not shared constants) to avoid churning those call sites; if these
// ever diverge a round-trip test in this module and the durability oracles fail.
const ENGINE_MANIFEST: &str = "manifest.bin";
const ENGINE_WAL: &str = "wal.log";
const SOURCES: &str = "sources.dat";
const SEGMENTS_DIR: &str = "segments";
const CLUSTER_MANIFEST: &str = "cluster_manifest.bin";
const CLUSTER_LOG: &str = "cluster.log";

/// A backup could not be produced or did not verify.
#[derive(Debug)]
pub enum BackupError {
    /// The engine/cluster has no `data_dir` — there is nothing on disk to back up.
    NotDurable,
    /// Durability is degraded (a prior WAL/segment/manifest write failed): the
    /// on-disk state is known-incomplete, so a snapshot of it would be unsound.
    PersistenceDegraded,
    /// The destination already exists; refuse to silently overwrite a prior backup.
    DestExists(PathBuf),
    /// A manifest required for the backup/verify was missing.
    MissingManifest(PathBuf),
    /// A manifest-referenced segment file is absent from the backup.
    MissingSegment(String),
    /// A backed-up segment failed its structural/CRC check (`MmapSegment::open`).
    CorruptSegment { name: String, source: io::Error },
    /// An underlying filesystem error.
    Io(io::Error),
}

impl fmt::Display for BackupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BackupError::NotDurable => {
                write!(f, "engine is not durable (no data_dir): nothing to back up")
            }
            BackupError::PersistenceDegraded => write!(
                f,
                "engine persistence is degraded; refusing to back up a known-incomplete state"
            ),
            BackupError::DestExists(p) => {
                write!(f, "backup destination already exists: {}", p.display())
            }
            BackupError::MissingManifest(p) => {
                write!(f, "manifest not found: {}", p.display())
            }
            BackupError::MissingSegment(name) => {
                write!(f, "backup is missing a referenced segment: {name}")
            }
            BackupError::CorruptSegment { name, source } => {
                write!(f, "backed-up segment {name} failed validation: {source}")
            }
            BackupError::Io(e) => write!(f, "backup I/O error: {e}"),
        }
    }
}

impl std::error::Error for BackupError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BackupError::CorruptSegment { source, .. } | BackupError::Io(source) => Some(source),
            _ => None,
        }
    }
}

impl From<io::Error> for BackupError {
    fn from(e: io::Error) -> Self {
        BackupError::Io(e)
    }
}

/// Copy a file and fsync the destination's data. The parent directory is created
/// if needed; the directory entry is made durable by the caller's `fsync_dir`.
fn copy_file_durable(src: &Path, dst: &Path) -> io::Result<()> {
    if let Some(parent) = dst.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::copy(src, dst)?;
    std::fs::File::open(dst)?.sync_all()?;
    Ok(())
}

/// fsync a directory so prior renames/creates within it are durable.
fn fsync_dir(dir: &Path) -> io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

/// The sibling staging directory for `dest` (same parent ⇒ same filesystem ⇒ the
/// final rename is atomic, never `EXDEV`).
fn staging_dir(dest: &Path) -> PathBuf {
    let mut name = dest
        .file_name()
        .map(std::ffi::OsString::from)
        .unwrap_or_default();
    name.push(".backup.tmp");
    match dest.parent().filter(|p| !p.as_os_str().is_empty()) {
        Some(parent) => parent.join(name),
        None => PathBuf::from(name),
    }
}

/// Atomically commit a fully-staged directory to `dest` (rename + parent fsync).
fn commit_staging(staging: &Path, dest: &Path) -> io::Result<()> {
    std::fs::rename(staging, dest)?;
    match dest.parent().filter(|p| !p.as_os_str().is_empty()) {
        Some(parent) => fsync_dir(parent),
        None => fsync_dir(Path::new(".")),
    }
}

/// Run `stage` into a fresh staging dir, `verify` the staged tree, then atomically
/// rename it onto `dest`. Refuses a pre-existing `dest`; cleans the staging dir on any
/// failure. Verifying BEFORE the commit means a verification failure leaves NO `dest`
/// behind (a retry isn't blocked by a half-written backup).
fn staged_backup<S, V>(dest: &Path, stage: S, verify: V) -> Result<(), BackupError>
where
    S: FnOnce(&Path) -> Result<(), BackupError>,
    V: FnOnce(&Path) -> Result<(), BackupError>,
{
    if dest.exists() {
        return Err(BackupError::DestExists(dest.to_path_buf()));
    }
    let staging = staging_dir(dest);
    // Remove any leftover staging from a prior aborted attempt.
    std::fs::remove_dir_all(&staging).ok();
    std::fs::create_dir_all(&staging)?;
    match stage(&staging).and_then(|()| verify(&staging)) {
        Ok(()) => {
            commit_staging(&staging, dest)?;
            Ok(())
        }
        Err(e) => {
            std::fs::remove_dir_all(&staging).ok();
            Err(e)
        }
    }
}

/// Back up a single-node engine `data_dir` into `dest`.
///
/// Copies the manifest-referenced segments, then `sources.dat`/`wal.log` (each if
/// present), then `manifest.bin` last. Orphan `.seg` files are skipped. The caller
/// MUST hold the engine's write-path exclusion for the duration of this call so no
/// concurrent compaction deletes a referenced segment mid-copy.
pub fn copy_engine_dir(src: &Path, dest: &Path) -> Result<(), BackupError> {
    staged_backup(
        dest,
        |staging| stage_engine_dir(src, staging),
        verify_backup,
    )
}

fn stage_engine_dir(src: &Path, staging: &Path) -> Result<(), BackupError> {
    std::fs::create_dir_all(staging.join(SEGMENTS_DIR))?;
    let manifest_path = src.join(ENGINE_MANIFEST);
    let has_manifest = manifest_path.exists();
    // Manifest-referenced segments first (orphans on disk are skipped — they are
    // not in the list). A never-checkpointed engine has no manifest: its acked
    // writes live only in the WAL, copied below.
    if has_manifest {
        let manifest = read_manifest(&manifest_path)?;
        for name in &manifest.segment_files {
            copy_file_durable(
                &src.join(SEGMENTS_DIR).join(name),
                &staging.join(SEGMENTS_DIR).join(name),
            )?;
        }
    }
    // Aux files, present-iff. The WAL pairs with the manifest's wal_seq_watermark;
    // both are copied at a consistent point because the caller holds the write lock.
    for aux in [SOURCES, ENGINE_WAL] {
        let s = src.join(aux);
        if s.exists() {
            copy_file_durable(&s, &staging.join(aux))?;
        }
    }
    // Manifest LAST (commit-point ordering).
    if has_manifest {
        copy_file_durable(&manifest_path, &staging.join(ENGINE_MANIFEST))?;
    }
    fsync_dir(&staging.join(SEGMENTS_DIR))?;
    fsync_dir(staging)?;
    Ok(())
}

/// Back up a cluster coordinator `data_dir` into `dest`.
///
/// Copies each shard's manifest-referenced segments + `sources.dat`, then
/// `cluster.log`, then `cluster_manifest.bin` last. Replica directories are NOT
/// copied — `ClusterEngine::open` rebuilds them from the primaries via peer
/// recovery. The caller MUST `checkpoint()` first (so the source dir is consistent
/// and every clean shard's `sources.dat` exists) and hold the cluster write lock
/// across both the checkpoint and this copy.
pub fn copy_cluster_dir(src: &Path, dest: &Path) -> Result<(), BackupError> {
    staged_backup(
        dest,
        |staging| stage_cluster_dir(src, staging),
        verify_cluster_backup,
    )
}

fn stage_cluster_dir(src: &Path, staging: &Path) -> Result<(), BackupError> {
    let manifest_path = src.join(CLUSTER_MANIFEST);
    if !manifest_path.exists() {
        return Err(BackupError::MissingManifest(manifest_path));
    }
    let manifest = read_cluster_manifest(&manifest_path)?;
    for (i, files) in manifest.segment_registry.iter().enumerate() {
        let shard = shard_dir_name(i);
        let dst_seg = staging.join(&shard).join(SEGMENTS_DIR);
        std::fs::create_dir_all(&dst_seg)?;
        let src_seg = src.join(&shard).join(SEGMENTS_DIR);
        for name in files {
            copy_file_durable(&src_seg.join(name), &dst_seg.join(name))?;
        }
        // Per-shard sources.dat (persisted on every shard at checkpoint via the
        // ADR-074 seal seam, even when its memtable was empty).
        let src_sources = src.join(&shard).join(SOURCES);
        if src_sources.exists() {
            copy_file_durable(&src_sources, &staging.join(&shard).join(SOURCES))?;
        }
        fsync_dir(&dst_seg)?;
        fsync_dir(&staging.join(&shard))?;
    }
    // Coordinator log, then the manifest LAST (commit-point ordering).
    let log = src.join(CLUSTER_LOG);
    if log.exists() {
        copy_file_durable(&log, &staging.join(CLUSTER_LOG))?;
    }
    copy_file_durable(&manifest_path, &staging.join(CLUSTER_MANIFEST))?;
    fsync_dir(staging)?;
    Ok(())
}

/// Shard directory name (mirrors `cluster::coordinator::shard_dir`).
fn shard_dir_name(shard: usize) -> String {
    format!("shard_{shard:03}")
}

/// Validate a single-node backup: the manifest (if present) parses, every segment it
/// references opens + passes its CRC check, and the `sources.dat` store (if present)
/// loads — i.e. everything `Engine::open` will read. A manifest-absent backup (a
/// never-checkpointed engine whose state is WAL-only, or an empty engine) is
/// structurally valid; the WAL itself is validated by `Engine::backup_to` before the
/// copy (kept out of `storage` to avoid a `storage`→`wal` dependency).
pub fn verify_backup(dir: &Path) -> Result<(), BackupError> {
    let manifest_path = dir.join(ENGINE_MANIFEST);
    if manifest_path.exists() {
        let manifest = read_manifest(&manifest_path)?;
        verify_segments(&dir.join(SEGMENTS_DIR), &manifest.segment_files)?;
    }
    verify_sources(&dir.join(SOURCES))
}

/// Validate a cluster backup: the cluster manifest parses and, for every shard, each
/// referenced segment opens + passes its CRC check and the shard's `sources.dat` loads
/// — everything `ClusterEngine::open` will read per shard.
pub fn verify_cluster_backup(dir: &Path) -> Result<(), BackupError> {
    let manifest_path = dir.join(CLUSTER_MANIFEST);
    if !manifest_path.exists() {
        return Err(BackupError::MissingManifest(manifest_path));
    }
    let manifest = read_cluster_manifest(&manifest_path)?;
    for (i, files) in manifest.segment_registry.iter().enumerate() {
        let shard = dir.join(shard_dir_name(i));
        verify_segments(&shard.join(SEGMENTS_DIR), files)?;
        verify_sources(&shard.join(SOURCES))?;
    }
    Ok(())
}

fn verify_segments(seg_dir: &Path, files: &[String]) -> Result<(), BackupError> {
    for name in files {
        let seg = seg_dir.join(name);
        if !seg.exists() {
            return Err(BackupError::MissingSegment(name.clone()));
        }
        // MmapSegment::open validates magic + version + trailing CRC.
        MmapSegment::open(&seg).map_err(|e| BackupError::CorruptSegment {
            name: name.clone(),
            source: e,
        })?;
    }
    Ok(())
}

/// Validate a `sources.dat` store (no-op if absent): `open` loads it via the same
/// `load_query_sources`, so a corrupt copy must fail the backup, not the restore.
fn verify_sources(path: &Path) -> Result<(), BackupError> {
    if path.exists() {
        load_query_sources(path).map_err(|e| {
            BackupError::Io(io::Error::new(e.kind(), format!("{}: {e}", path.display())))
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{
        write_cluster_manifest, write_manifest, ClusterManifest, Manifest, SourceStore,
    };

    /// Write a valid `sources.dat` (so the round-trip fixtures pass the new
    /// sources validation; the rejection tests write garbage on purpose).
    fn write_valid_sources(path: &Path) {
        let store = SourceStore::new_resident();
        store.insert(1, "a stored query".into());
        store.write_to(path).unwrap();
    }

    fn empty_manifest(files: Vec<String>) -> Manifest {
        Manifest {
            segment_files: files,
            class_d_fence: false,
            next_seg_id: 1,
            dict_data: Vec::new(),
            tag_dict_data: Vec::new(),
            rejected_parse: 0,
            rejected_class_d: 0,
            wal_seq_watermark: 0,
            segment_tombstones: Vec::new(),
        }
    }

    fn tmp_root(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("rr-backup-unit-{tag}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn engine_backup_round_trips_files_and_verifies() {
        let root = tmp_root("engine-rt");
        let src = root.join("src");
        std::fs::create_dir_all(src.join(SEGMENTS_DIR)).unwrap();
        // An empty-corpus manifest (no segments) + a WAL + sources.dat.
        write_manifest(&empty_manifest(vec![]), &src.join(ENGINE_MANIFEST)).unwrap();
        std::fs::write(src.join(ENGINE_WAL), b"wal-bytes").unwrap();
        write_valid_sources(&src.join(SOURCES));

        let dest = root.join("dest");
        copy_engine_dir(&src, &dest).unwrap();

        // Files are present and byte-identical.
        assert_eq!(
            std::fs::read(src.join(ENGINE_WAL)).unwrap(),
            std::fs::read(dest.join(ENGINE_WAL)).unwrap()
        );
        assert_eq!(
            std::fs::read(src.join(ENGINE_MANIFEST)).unwrap(),
            std::fs::read(dest.join(ENGINE_MANIFEST)).unwrap()
        );
        verify_backup(&dest).unwrap();
        // No leftover staging dir.
        assert!(!staging_dir(&dest).exists());
    }

    #[test]
    fn engine_backup_refuses_existing_dest() {
        let root = tmp_root("engine-dest-exists");
        let src = root.join("src");
        std::fs::create_dir_all(src.join(SEGMENTS_DIR)).unwrap();
        write_manifest(&empty_manifest(vec![]), &src.join(ENGINE_MANIFEST)).unwrap();
        let dest = root.join("dest");
        std::fs::create_dir_all(&dest).unwrap();
        match copy_engine_dir(&src, &dest) {
            Err(BackupError::DestExists(_)) => {}
            other => panic!("expected DestExists, got {other:?}"),
        }
    }

    #[test]
    fn engine_backup_without_manifest_is_valid_wal_only() {
        let root = tmp_root("engine-wal-only");
        let src = root.join("src");
        std::fs::create_dir_all(src.join(SEGMENTS_DIR)).unwrap();
        std::fs::write(src.join(ENGINE_WAL), b"wal-only").unwrap();
        let dest = root.join("dest");
        copy_engine_dir(&src, &dest).unwrap();
        assert!(dest.join(ENGINE_WAL).exists());
        assert!(!dest.join(ENGINE_MANIFEST).exists());
        verify_backup(&dest).unwrap();
    }

    #[test]
    fn verify_detects_missing_and_corrupt_segment() {
        let root = tmp_root("verify-bad");
        let dir = root.join("backup");
        std::fs::create_dir_all(dir.join(SEGMENTS_DIR)).unwrap();
        // Manifest references a segment that does not exist → MissingSegment.
        write_manifest(
            &empty_manifest(vec!["seg_000001.seg".into()]),
            &dir.join(ENGINE_MANIFEST),
        )
        .unwrap();
        match verify_backup(&dir) {
            Err(BackupError::MissingSegment(n)) => assert_eq!(n, "seg_000001.seg"),
            other => panic!("expected MissingSegment, got {other:?}"),
        }
        // Now create a garbage "segment" → CorruptSegment (fails MmapSegment::open).
        std::fs::write(
            dir.join(SEGMENTS_DIR).join("seg_000001.seg"),
            b"not a segment",
        )
        .unwrap();
        match verify_backup(&dir) {
            Err(BackupError::CorruptSegment { name, .. }) => assert_eq!(name, "seg_000001.seg"),
            other => panic!("expected CorruptSegment, got {other:?}"),
        }
    }

    #[test]
    fn cluster_backup_round_trips_and_verifies() {
        let root = tmp_root("cluster-rt");
        let src = root.join("src");
        // Two shards, both with empty registries (empty corpus) + a coordinator log.
        for i in 0..2 {
            std::fs::create_dir_all(src.join(shard_dir_name(i)).join(SEGMENTS_DIR)).unwrap();
            write_valid_sources(&src.join(shard_dir_name(i)).join(SOURCES));
        }
        std::fs::write(src.join(CLUSTER_LOG), b"clog").unwrap();
        let manifest = ClusterManifest {
            epoch: 1,
            snapshot_pos: 0,
            dict_fingerprint: 0,
            num_shards: 2,
            vnodes: 64,
            include_broad: true,
            broad_replicate_all: true,
            segment_registry: vec![vec![], vec![]],
            next_seg_ids: vec![1, 1],
            dict_data: Vec::new(),
            vocab_data: Vec::new(),
            tag_dict_data: Vec::new(),
        };
        write_cluster_manifest(&manifest, &src.join(CLUSTER_MANIFEST)).unwrap();

        let dest = root.join("dest");
        copy_cluster_dir(&src, &dest).unwrap();
        assert!(dest.join(CLUSTER_LOG).exists());
        assert!(dest.join(CLUSTER_MANIFEST).exists());
        assert!(dest.join(shard_dir_name(1)).join(SOURCES).exists());
        verify_cluster_backup(&dest).unwrap();
    }

    #[test]
    fn cluster_verify_requires_manifest() {
        let root = tmp_root("cluster-no-manifest");
        let dir = root.join("backup");
        std::fs::create_dir_all(&dir).unwrap();
        match verify_cluster_backup(&dir) {
            Err(BackupError::MissingManifest(_)) => {}
            other => panic!("expected MissingManifest, got {other:?}"),
        }
    }

    #[test]
    fn verify_rejects_corrupt_sources() {
        // A corrupt sources.dat (open would fail loading it) must fail verify, not be
        // silently accepted (codex P1).
        let root = tmp_root("engine-corrupt-sources");
        let dir = root.join("backup");
        std::fs::create_dir_all(dir.join(SEGMENTS_DIR)).unwrap();
        write_manifest(&empty_manifest(vec![]), &dir.join(ENGINE_MANIFEST)).unwrap();
        std::fs::write(dir.join(SOURCES), b"not a valid sources store").unwrap();
        assert!(
            verify_backup(&dir).is_err(),
            "corrupt sources must fail verify"
        );
    }

    #[test]
    fn cluster_verify_rejects_corrupt_shard_sources() {
        // A corrupt per-shard sources.dat must fail verify (codex P1): otherwise the
        // endpoint acks a backup ClusterEngine::open later refuses.
        let root = tmp_root("cluster-corrupt-sources");
        let dir = root.join("backup");
        for i in 0..2 {
            std::fs::create_dir_all(dir.join(shard_dir_name(i)).join(SEGMENTS_DIR)).unwrap();
        }
        write_valid_sources(&dir.join(shard_dir_name(0)).join(SOURCES));
        std::fs::write(dir.join(shard_dir_name(1)).join(SOURCES), b"corrupt").unwrap();
        let manifest = ClusterManifest {
            epoch: 1,
            snapshot_pos: 0,
            dict_fingerprint: 0,
            num_shards: 2,
            vnodes: 64,
            include_broad: true,
            broad_replicate_all: true,
            segment_registry: vec![vec![], vec![]],
            next_seg_ids: vec![1, 1],
            dict_data: Vec::new(),
            vocab_data: Vec::new(),
            tag_dict_data: Vec::new(),
        };
        write_cluster_manifest(&manifest, &dir.join(CLUSTER_MANIFEST)).unwrap();
        assert!(
            verify_cluster_backup(&dir).is_err(),
            "corrupt shard sources must fail verify"
        );
    }

    #[test]
    fn copy_verifies_before_commit_so_a_bad_source_leaves_no_dest() {
        // A manifest referencing a corrupt segment fails verification, which now runs
        // on the staging tree BEFORE the rename (codex P2) — so no dest is created and
        // a retry isn't blocked by a half-written backup.
        let root = tmp_root("verify-before-commit");
        let src = root.join("src");
        std::fs::create_dir_all(src.join(SEGMENTS_DIR)).unwrap();
        std::fs::write(src.join(SEGMENTS_DIR).join("seg_000001.seg"), b"garbage").unwrap();
        write_manifest(
            &empty_manifest(vec!["seg_000001.seg".into()]),
            &src.join(ENGINE_MANIFEST),
        )
        .unwrap();
        let dest = root.join("dest");
        match copy_engine_dir(&src, &dest) {
            Err(BackupError::CorruptSegment { .. }) => {}
            other => panic!("expected CorruptSegment, got {other:?}"),
        }
        assert!(
            !dest.exists(),
            "verify failure must not leave a dest behind"
        );
        assert!(!staging_dir(&dest).exists(), "staging must be cleaned up");
    }
}
