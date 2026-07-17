//! Coordinator-side unique logical-id directory.
//!
//! Exact distributed top-K requires every logical match to belong to one shard
//! result. Query placement is content-derived, so two different query rows that
//! reuse one logical id cannot in general be co-routed to one owner. Cluster
//! ingestion therefore has insert-only logical-id semantics; callers replace an
//! existing id through `upsert_query`.
//!
//! The committed corpus is kept as a sorted `u64` column (eight bytes per id).
//! Live inserts/removes use small overlays, avoiding a full hash-table copy of a
//! multi-million-query base. Same-id mutations are serialized through striped
//! locks while different ids remain independent.

use std::sync::{MutexGuard, RwLockReadGuard, RwLockWriteGuard};

use crate::cluster::shard::ShardError;
use crate::util::FastSet;

use super::ClusterEngine;

pub(super) const LOGICAL_WRITE_STRIPES: usize = 256;

#[derive(Default)]
pub(super) struct LogicalIdDirectory {
    base: Vec<u64>,
    added: FastSet<u64>,
    removed: FastSet<u64>,
    /// True only when this directory was installed from an authoritative source
    /// (build/bulk-ingest/durable-open enumeration, or a provably-empty fresh
    /// assembly). A coordinator attached to an already-populated cluster it
    /// cannot enumerate (the gRPC connect shape — `RemoteShard` has no live-id
    /// enumeration RPC yet) stays unauthoritative, and insert-only admission
    /// fails closed instead of vacuously admitting duplicates.
    authoritative: bool,
}

impl LogicalIdDirectory {
    fn from_ids(mut ids: Vec<u64>) -> Result<Self, ShardError> {
        sort_and_check_unique(&mut ids)?;
        Ok(Self {
            base: ids,
            added: FastSet::default(),
            removed: FastSet::default(),
            authoritative: true,
        })
    }

    fn contains(&self, logical: u64) -> bool {
        self.added.contains(&logical)
            || (!self.removed.contains(&logical) && self.base.binary_search(&logical).is_ok())
    }

    /// Returns true when this call changed absent -> present.
    fn insert(&mut self, logical: u64) -> bool {
        if self.contains(logical) {
            return false;
        }
        if self.base.binary_search(&logical).is_ok() {
            self.removed.remove(&logical);
        } else {
            self.added.insert(logical);
        }
        true
    }

    /// Returns true when this call changed present -> absent.
    fn remove(&mut self, logical: u64) -> bool {
        if self.added.remove(&logical) {
            return true;
        }
        if self.removed.contains(&logical) || self.base.binary_search(&logical).is_err() {
            return false;
        }
        self.removed.insert(logical);
        true
    }

    /// Merge live overlays into the compact sorted base during an explicit
    /// maintenance boundary. `retain` reuses the base allocation; only newly
    /// added ids can grow it.
    fn compact(&mut self) {
        if self.added.is_empty() && self.removed.is_empty() {
            return;
        }
        let Self {
            base,
            added,
            removed,
            authoritative: _,
        } = self;
        base.retain(|logical| !removed.contains(logical));
        base.extend(added.drain());
        base.sort_unstable();
        removed.clear();
    }
}

/// Sort an initial corpus's compact id column and reject semantic duplicates
/// before any shard is mutated. This avoids constructing a second, hash-table
/// sized copy of every id during bulk load.
pub(super) fn sort_and_check_unique(ids: &mut [u64]) -> Result<(), ShardError> {
    ids.sort_unstable();
    if let Some(duplicate) = ids
        .windows(2)
        .find_map(|pair| (pair[0] == pair[1]).then_some(pair[0]))
    {
        return Err(ShardError::DuplicateLogicalId(duplicate));
    }
    Ok(())
}

fn read_directory(
    lock: &std::sync::RwLock<LogicalIdDirectory>,
) -> RwLockReadGuard<'_, LogicalIdDirectory> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_directory(
    lock: &std::sync::RwLock<LogicalIdDirectory>,
) -> RwLockWriteGuard<'_, LogicalIdDirectory> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl ClusterEngine {
    pub(super) fn logical_write_guard(&self, logical: u64) -> MutexGuard<'_, ()> {
        // Sequential ids spread evenly; the xor also mixes structured high bits.
        let mixed = logical ^ (logical >> 32);
        let stripe = mixed as usize % self.logical_write_stripes.len();
        self.logical_write_stripes[stripe]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Exclusively fence the empty-cluster bulk-load boundary against every
    /// incremental logical-id mutation. Locks are always taken in stripe order;
    /// single-id writers take only one, so there is no lock-order cycle.
    pub(super) fn logical_bulk_write_guards(&self) -> Vec<MutexGuard<'_, ()>> {
        self.logical_write_stripes
            .iter()
            .map(|stripe| {
                stripe
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
            })
            .collect()
    }

    pub(super) fn contains_logical_id(&self, logical: u64) -> bool {
        read_directory(&self.logical_ids).contains(logical)
    }

    /// Whether the directory reflects the full committed corpus (see
    /// [`LogicalIdDirectory::authoritative`]).
    pub(super) fn logical_ids_authoritative(&self) -> bool {
        read_directory(&self.logical_ids).authoritative
    }

    /// Test hook: simulate the connect-to-populated-cluster shape, where the
    /// coordinator cannot enumerate the corpus and the directory is unseeded.
    #[cfg(test)]
    pub(super) fn unseed_logical_ids_for_test(&self) {
        *write_directory(&self.logical_ids) = LogicalIdDirectory::default();
    }

    /// Reserve an id for a committed add/upsert. Returns true when it was fresh.
    pub(super) fn insert_logical_id(&self, logical: u64) -> bool {
        write_directory(&self.logical_ids).insert(logical)
    }

    pub(super) fn remove_logical_id(&self, logical: u64) -> bool {
        write_directory(&self.logical_ids).remove(logical)
    }

    pub(super) fn replace_logical_ids(&self, ids: Vec<u64>) -> Result<(), ShardError> {
        *write_directory(&self.logical_ids) = LogicalIdDirectory::from_ids(ids)?;
        Ok(())
    }

    pub(super) fn compact_logical_ids(&self) {
        write_directory(&self.logical_ids).compact();
    }
}

#[cfg(test)]
mod tests {
    use super::LogicalIdDirectory;

    #[test]
    fn sorted_base_and_live_overlays_preserve_membership() {
        let mut ids = LogicalIdDirectory::from_ids(vec![9, 1, 5]).expect("unique");
        assert!(ids.contains(1));
        assert!(!ids.contains(2));
        assert!(ids.insert(2));
        assert!(!ids.insert(2));
        assert!(ids.remove(5));
        assert!(!ids.contains(5));
        assert!(ids.insert(5));
        assert!(ids.contains(5));
    }

    #[test]
    fn duplicate_base_is_rejected() {
        assert!(LogicalIdDirectory::from_ids(vec![7, 3, 7]).is_err());
    }

    #[test]
    fn maintenance_compaction_folds_live_overlays_into_sorted_base() {
        let mut ids = LogicalIdDirectory::from_ids(vec![9, 1, 5]).expect("unique");
        assert!(ids.remove(5));
        assert!(ids.insert(2));
        ids.compact();
        assert_eq!(ids.base, vec![1, 2, 9]);
        assert!(ids.added.is_empty());
        assert!(ids.removed.is_empty());
    }
}
