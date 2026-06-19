//! `impl ClusterEngine` — `backup_to`: checkpoint, then snapshot the coordinator's
//! durable state into a fresh directory (ADR-079, ADR-065 criterion 11). Restore is
//! the existing [`ClusterEngine::open`] pointed at the (relocated) backup directory.

use std::path::Path;

use crate::cluster::coordinator::ClusterEngine;
use crate::cluster::shard::ShardError;

impl ClusterEngine {
    /// Back up the cluster's durable state into `dest` (which must not already exist).
    ///
    /// `&self` (matches [`checkpoint`](ClusterEngine::checkpoint)); the caller holds
    /// the cluster write-serialization lock across this call (see the
    /// `_cluster/backup` handler), so no concurrent mutation runs and no shard
    /// compaction deletes a segment mid-copy. `checkpoint()` runs FIRST so the source
    /// dir is fully consistent — `seal_for_checkpoint` persists every shard's
    /// `sources.dat` even when its memtable is empty (the ADR-074 seam), which a raw
    /// hot-copy would miss — then the coordinator manifest + `cluster.log` + each
    /// shard's manifest-referenced segments + `sources.dat` are copied. Replica
    /// directories are NOT copied; `open` rebuilds them from the primaries via peer
    /// recovery. The backup is verified before returning.
    ///
    /// Returns [`ShardError::Config`] for an in-memory cluster (nothing on disk to
    /// back up), and [`ShardError::Log`] for a checkpoint / copy / validation failure.
    pub fn backup_to(&self, dest: &Path) -> Result<(), ShardError> {
        let Some(src) = self.data_dir.clone() else {
            return Err(ShardError::Config(
                "cluster is in-memory (no data_dir): nothing to back up".into(),
            ));
        };
        // Make the source dir a consistent on-disk snapshot (seal + atomic manifest
        // + log truncate + orphan GC). Fails loud if any shard fell back to memory.
        self.checkpoint()?;
        crate::storage::copy_cluster_dir(&src, dest)
            .map_err(|e| ShardError::Log(format!("cluster backup copy: {e}")))?;
        crate::storage::verify_cluster_backup(dest)
            .map_err(|e| ShardError::Log(format!("cluster backup verify: {e}")))?;
        Ok(())
    }
}
