//! `impl Engine` — `backup_to`: a consistent on-disk snapshot of this engine's
//! `data_dir` into a fresh directory (ADR-079, ADR-065 criterion 11). Restore is
//! the existing [`Engine::open`] pointed at the (relocated) backup directory.

use std::path::Path;

use crate::segment::Engine;
use crate::storage::{self, BackupError};
use crate::wal::Wal;

impl Engine {
    /// Back up this engine's durable state into `dest` (which must not already exist).
    ///
    /// Takes `&mut self` deliberately: it must run under the engine's single-writer
    /// exclusion (the server serializes every mutation behind `Mutex<Engine>`), so
    /// no concurrent flush/compaction can delete a segment between reading the
    /// manifest and copying the files it lists — the copy is race-free by
    /// construction. Reads keep flowing off the lock-free snapshot. The copy is
    /// manifest-driven (orphan `.seg` files are skipped) and includes `sources.dat`
    /// and the WAL; on restore [`Engine::open`] replays the WAL tail, so no flush is
    /// forced here. `copy_engine_dir` verifies the staged tree (segments + sources)
    /// before the atomic commit, so a failure leaves no `dest` behind.
    ///
    /// Returns [`BackupError::NotDurable`] for an in-memory engine,
    /// [`BackupError::PersistenceDegraded`] when a prior durability write failed
    /// (the on-disk state is known-incomplete), [`BackupError::DestExists`] when
    /// `dest` already exists, and I/O / validation errors otherwise.
    pub fn backup_to(&mut self, dest: &Path) -> Result<(), BackupError> {
        let Some(src) = self.config.data_dir.clone() else {
            return Err(BackupError::NotDurable);
        };
        if !self.persistence_healthy {
            return Err(BackupError::PersistenceDegraded);
        }
        // Validate the WAL before snapshotting it: a never-flushed engine's acked
        // writes live ONLY here, and the copy is byte-faithful, so a recoverable
        // source WAL ⇒ a recoverable backup WAL. `Wal::recover` is exactly what
        // `open` runs — it tolerates a torn tail (the documented crash semantics)
        // and errs only on a missing/too-small/bad-magic file (real corruption we
        // must not silently snapshot). This lives here, not in `storage`, to avoid a
        // `storage`→`wal` dependency.
        let wal_path = src.join("wal.log");
        if wal_path.exists() {
            Wal::recover(&wal_path).map_err(|e| {
                BackupError::Io(std::io::Error::new(e.kind(), format!("wal.log: {e}")))
            })?;
        }
        storage::copy_engine_dir(&src, dest)
    }
}
