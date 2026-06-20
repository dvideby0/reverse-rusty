//! `impl ClusterEngine` — `backup_to`: checkpoint, then snapshot the coordinator's
//! durable state into a fresh directory (ADR-079, ADR-065 criterion 11). Restore is
//! the existing [`ClusterEngine::open`] pointed at the (relocated) backup directory.

use std::path::Path;

use crate::cluster::coordinator::ClusterEngine;
use crate::cluster::shard::ShardError;
use crate::storage::BackupError;

impl ClusterEngine {
    /// Back up the cluster's durable state into `dest` (which must not already exist).
    ///
    /// `&self` (matches [`checkpoint`](ClusterEngine::checkpoint)); the caller holds
    /// the cluster write-serialization lock across this call (see the
    /// `_cluster/backup` handler), so no concurrent mutation runs and no shard
    /// compaction deletes a segment mid-copy. The pre-conditions (in-memory cluster,
    /// pre-existing `dest`) are rejected BEFORE `checkpoint()` so a bad request never
    /// has the side effect of bumping the epoch / truncating the log. `checkpoint()`
    /// then runs so the source dir is fully consistent — `seal_for_checkpoint`
    /// persists every shard's `sources.dat` even when its memtable is empty (the
    /// ADR-074 seam), which a raw hot-copy would miss. Then `copy_cluster_dir` copies
    /// the coordinator manifest, `cluster.log`, and each shard's manifest-referenced
    /// segments and `sources.dat`, verifying the staged tree (segments + sources)
    /// before the atomic commit so a failure leaves no `dest`. Replica directories are
    /// NOT copied; `open` rebuilds them from the primaries via peer recovery.
    ///
    /// Returns [`ShardError::Config`] for a bad request (in-memory cluster or existing
    /// `dest` — a 400), and [`ShardError::Log`] for a checkpoint / copy / validation
    /// failure (a 503).
    pub fn backup_to(&self, dest: &Path) -> Result<(), ShardError> {
        let Some(src) = self.data_dir.clone() else {
            return Err(ShardError::Config(
                "cluster is in-memory (no data_dir): nothing to back up".into(),
            ));
        };
        // Reject an existing dest up front: checkpoint() has side effects (epoch bump,
        // log truncation), so a request that can't succeed must not run it.
        if dest.exists() {
            return Err(ShardError::Config(format!(
                "backup destination already exists: {}",
                dest.display()
            )));
        }
        // Make the source dir a consistent on-disk snapshot (seal + atomic manifest
        // + log truncate + orphan GC). Fails loud if any shard fell back to memory.
        self.checkpoint()?;
        // copy_cluster_dir stages + verifies + atomically commits. A precondition
        // error (NotDurable/DestExists) is a bad request (400); a copy/verify failure
        // is a durability problem (503).
        crate::storage::copy_cluster_dir(&src, dest).map_err(|e| match e {
            BackupError::NotDurable | BackupError::DestExists(_) => {
                ShardError::Config(format!("cluster backup: {e}"))
            }
            other => ShardError::Log(format!("cluster backup: {other}")),
        })
    }
}
