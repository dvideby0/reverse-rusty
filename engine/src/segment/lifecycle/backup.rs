//! `impl Engine` — `backup_to`: a consistent on-disk snapshot of this engine's
//! `data_dir` into a fresh directory (ADR-079, ADR-065 criterion 11). Restore is
//! the existing [`Engine::open`] pointed at the (relocated) backup directory.

use std::path::Path;

use crate::segment::Engine;
use crate::storage::{self, BackupError};

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
    /// forced here. The produced backup is verified before returning.
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
        storage::copy_engine_dir(&src, dest)?;
        storage::verify_backup(dest)
    }
}
