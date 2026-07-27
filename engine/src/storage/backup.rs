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
//! Everything is staged into a unique sibling
//! `<dest>.backup.tmp.<pid>.<sequence>` directory, fsync'd, and promoted with an
//! atomic no-clobber rename where the platform supports it. A crash mid-backup
//! leaves only its uniquely-owned staging dir, never a half-populated `dest`.
//! Within the staging dir the manifest is written LAST, mirroring the engine's
//! own "build durable, then commit" discipline, so the staged tree is internally
//! consistent before the rename.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

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

static STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Whether any filesystem entry occupies `path`. Unlike [`Path::exists`], this
/// treats a dangling symlink as occupied, so a backup cannot silently replace
/// it during the final rename.
fn path_entry_exists(path: &Path) -> Result<bool, BackupError> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(BackupError::Io(error)),
    }
}

/// Reserve a unique sibling staging directory for `dest` (same parent ⇒ same
/// filesystem ⇒ the final rename is atomic, never `EXDEV`). Unique ownership
/// prevents two processes targeting the same destination from deleting or
/// writing each other's staging trees.
fn reserve_staging_dir(dest: &Path) -> Result<PathBuf, BackupError> {
    let mut name = dest
        .file_name()
        .map(std::ffi::OsString::from)
        .unwrap_or_default();
    name.push(".backup.tmp.");
    name.push(std::process::id().to_string());
    name.push(".");
    let parent = dest
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;

    for _ in 0..1_024 {
        let sequence = STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let mut candidate_name = name.clone();
        candidate_name.push(sequence.to_string());
        let candidate = parent.join(candidate_name);
        match std::fs::create_dir(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(BackupError::Io(error)),
        }
    }
    Err(BackupError::Io(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not reserve a unique backup staging directory",
    )))
}

/// Rename a directory without replacing an entry that appeared at `dest` after
/// request validation. Linux/Android and Apple platforms provide an atomic
/// no-replace flag. The portability fallback repeats the symlink-aware check
/// immediately before `std::fs::rename`; supported production platforms never
/// have that check/rename race.
fn rename_noreplace(staging: &Path, dest: &Path) -> io::Result<()> {
    #[cfg(any(
        target_os = "android",
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "visionos",
        target_os = "watchos",
        target_os = "redox",
    ))]
    {
        use rustix::fs::{renameat_with, RenameFlags, CWD};
        use rustix::io::Errno;

        match renameat_with(CWD, staging, CWD, dest, RenameFlags::NOREPLACE) {
            Ok(()) => return Ok(()),
            Err(Errno::NOSYS | Errno::INVAL | Errno::NOTSUP) => {}
            Err(error) => return Err(error.into()),
        }
    }

    match std::fs::symlink_metadata(dest) {
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("backup destination already exists: {}", dest.display()),
            ))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    std::fs::rename(staging, dest)
}

/// Atomically commit a fully-staged directory to `dest` without clobbering a
/// competing entry, then make the new parent-directory entry durable.
fn commit_staging(staging: &Path, dest: &Path) -> Result<(), BackupError> {
    if let Err(error) = rename_noreplace(staging, dest) {
        return if error.kind() == io::ErrorKind::AlreadyExists {
            Err(BackupError::DestExists(dest.to_path_buf()))
        } else {
            Err(BackupError::Io(error))
        };
    }
    match dest.parent().filter(|p| !p.as_os_str().is_empty()) {
        Some(parent) => fsync_dir(parent)?,
        None => fsync_dir(Path::new("."))?,
    }
    Ok(())
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
    if path_entry_exists(dest)? {
        return Err(BackupError::DestExists(dest.to_path_buf()));
    }
    let staging = reserve_staging_dir(dest)?;
    match stage(&staging)
        .and_then(|()| verify(&staging))
        .and_then(|()| commit_staging(&staging, dest))
    {
        Ok(()) => Ok(()),
        Err(e) => {
            std::fs::remove_dir_all(&staging).ok();
            Err(e)
        }
    }
}

/// Back up a single-node engine `data_dir` into `dest`.
///
/// Copies the manifest-referenced segments, then its selected source sidecar and
/// `wal.log`, then `manifest.bin` last. Orphan segment/source files are skipped. The caller
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
    let manifest = if has_manifest {
        let manifest = read_manifest(&manifest_path)?;
        for name in &manifest.segment_files {
            copy_file_durable(
                &src.join(SEGMENTS_DIR).join(name),
                &staging.join(SEGMENTS_DIR).join(name),
            )?;
        }
        Some(manifest)
    } else {
        None
    };
    // The source sidecar selected by v7 is part of the commit and therefore
    // mandatory. Legacy/no-manifest `sources.dat` remains optional.
    let source_name = manifest
        .as_ref()
        .map_or(SOURCES, |manifest| manifest.source_file_name.as_str());
    let source = src.join(source_name);
    if source.exists() {
        copy_file_durable(&source, &staging.join(source_name))?;
    } else if source_name != SOURCES {
        return Err(BackupError::Io(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "manifest-selected source sidecar is missing: {}",
                source.display()
            ),
        )));
    }
    // The WAL pairs with the manifest's wal_seq_watermark; both are copied at a
    // consistent point because the caller holds the write lock.
    let wal = src.join(ENGINE_WAL);
    if wal.exists() {
        copy_file_durable(&wal, &staging.join(ENGINE_WAL))?;
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
/// Copies each shard's manifest-referenced segments + selected source sidecar, then
/// `cluster.log`, then `cluster_manifest.bin` last. Replica directories are NOT
/// copied — `ClusterEngine::open` rebuilds them from the primaries via peer
/// recovery. The caller MUST `checkpoint()` first (so the source dir is consistent
/// and every clean shard's selected source sidecar exists) and hold the cluster write lock
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
        // Per-shard source sidecar selected by the same manifest as the segment
        // registry (persisted even when the memtable was empty).
        let source_name = &manifest.source_files[i];
        let src_sources = src.join(&shard).join(source_name);
        if src_sources.exists() {
            copy_file_durable(&src_sources, &staging.join(&shard).join(source_name))?;
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
/// references opens + passes its CRC check, and its selected source store loads —
/// i.e. everything `Engine::open` will read. A manifest-absent backup (a
/// never-checkpointed engine whose state is WAL-only, or an empty engine) is
/// structurally valid; the WAL itself is validated by `Engine::backup_to` before the
/// copy (kept out of `storage` to avoid a `storage`→`wal` dependency).
pub fn verify_backup(dir: &Path) -> Result<(), BackupError> {
    let manifest_path = dir.join(ENGINE_MANIFEST);
    if manifest_path.exists() {
        let manifest = read_manifest(&manifest_path)?;
        verify_segments(&dir.join(SEGMENTS_DIR), &manifest.segment_files)?;
        verify_sources(
            &dir.join(&manifest.source_file_name),
            manifest.source_file_name != SOURCES,
        )
    } else {
        verify_sources(&dir.join(SOURCES), false)
    }
}

/// Validate a cluster backup: the cluster manifest parses and, for every shard, each
/// referenced segment opens + passes its CRC check and the shard's selected source sidecar loads
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
        verify_sources(&shard.join(&manifest.source_files[i]), false)?;
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

/// Validate a selected source store (optionally a legacy `sources.dat`): `open`
/// loads it via the same `load_query_sources`, so a corrupt copy must fail the
/// backup, not the restore.
fn verify_sources(path: &Path, required: bool) -> Result<(), BackupError> {
    if path.exists() {
        load_query_sources(path).map_err(|e| {
            BackupError::Io(io::Error::new(e.kind(), format!("{}: {e}", path.display())))
        })?;
    } else if required {
        return Err(BackupError::Io(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "manifest-selected source sidecar is missing: {}",
                path.display()
            ),
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
