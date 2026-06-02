//! openraft `RaftLogStorage`/`RaftLogReader` for the control plane — a CRC-framed durable
//! Raft log (or in-memory), reusing the clog/wal torn-tail pattern + atomic vote/committed
//! files via `control_store` (ADR-041).

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{
    Entry, LogId, LogState, OptionalSend, RaftLogReader, StorageError, StorageIOError, Vote,
};

use crate::cluster::control::ControlError;
use crate::cluster::control_store;

use super::TypeConfig;

struct LogStoreInner {
    /// Log entries keyed by index — consecutive, no holes (the Raft correctness requirement).
    log: BTreeMap<u64, Entry<TypeConfig>>,
    last_purged: Option<LogId<u64>>,
    vote: Option<Vote<u64>>,
    committed: Option<LogId<u64>>,
    /// Durable backing (ADR-041): `None` ⇒ in-memory (the in-process oracle / single-process
    /// embedding — byte-identical to ADR-038); `Some` ⇒ persisted under this manager node's raft
    /// dir, with `log_file` the CRC-framed append handle and `fsync` the durability policy.
    paths: Option<control_store::RaftPaths>,
    log_file: Option<std::fs::File>,
    fsync: bool,
}

/// A [`RaftLogStorage`] for the control plane — in-memory or durable (ADR-041). `Arc`-shared so
/// `get_log_reader` hands out a cheap clone over the SAME inner (and, when durable, the same
/// append handle).
#[derive(Clone)]
pub(in crate::cluster::control_raft) struct LogStore {
    inner: Arc<Mutex<LogStoreInner>>,
}

impl LogStore {
    /// In-memory store (no disk) — the in-process oracle / single-process path.
    pub(in crate::cluster::control_raft) fn in_memory() -> Self {
        LogStore {
            inner: Arc::new(Mutex::new(LogStoreInner {
                log: BTreeMap::new(),
                last_purged: None,
                vote: None,
                committed: None,
                paths: None,
                log_file: None,
                fsync: false,
            })),
        }
    }

    /// Durable store rooted at `dir` (ADR-041): load the persisted log (dropping a torn tail) +
    /// vote + committed + last-purged, then open the append handle. A restarting manager node
    /// resumes its Raft hard state from here.
    pub(in crate::cluster::control_raft) fn open(
        dir: &Path,
        fsync: bool,
    ) -> Result<Self, ControlError> {
        // A `Copy` fn-pointer mapper (non-capturing closure) so each `?` site reuses it.
        let cf: fn(std::io::Error) -> ControlError =
            |e| ControlError::Backend(format!("raft store open: {e}"));
        let paths = control_store::RaftPaths::new(dir.to_path_buf());
        let entries: Vec<Entry<TypeConfig>> =
            control_store::read_records(&paths.log()).map_err(cf)?;
        let mut log = BTreeMap::new();
        for e in entries {
            log.insert(e.log_id.index, e);
        }
        let vote = control_store::read_value(&paths.vote()).map_err(cf)?;
        let committed: Option<LogId<u64>> = control_store::read_value(&paths.committed())
            .map_err(cf)?
            .flatten();
        let last_purged = control_store::read_value(&paths.purged()).map_err(cf)?;
        let log_file = control_store::ensure_log(&paths.log()).map_err(cf)?;
        Ok(LogStore {
            inner: Arc::new(Mutex::new(LogStoreInner {
                log,
                last_purged,
                vote,
                committed,
                paths: Some(paths),
                log_file: Some(log_file),
                fsync,
            })),
        })
    }

    fn lock(&self) -> MutexGuard<'_, LogStoreInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Atomically rewrite the durable log to its current entries and re-open the append handle — the
/// durable form of `truncate`/`purge` (which drop a suffix/prefix). A no-op for an in-memory store.
fn rewrite_and_reopen(inner: &mut LogStoreInner) -> std::io::Result<()> {
    let Some(path) = inner.paths.as_ref().map(control_store::RaftPaths::log) else {
        return Ok(());
    };
    {
        let records: Vec<&Entry<TypeConfig>> = inner.log.values().collect();
        control_store::rewrite_records(&path, &records, inner.fsync)?;
    }
    inner.log_file = Some(control_store::ensure_log(&path)?);
    Ok(())
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        Ok(self
            .lock()
            .log
            .range(range)
            .map(|(_, e)| e.clone())
            .collect())
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let inner = self.lock();
        let last = inner.log.values().next_back().map(|e| e.log_id);
        let last_purged = inner.last_purged;
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last.or(last_purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.lock();
        inner.vote = Some(*vote);
        // The vote is hard state — durable before returning, else a crash could lose an election
        // promise and allow two leaders in one term (Raft safety). fsync per the policy.
        if let Some(paths) = &inner.paths {
            control_store::write_value(&paths.vote(), vote, inner.fsync)
                .map_err(|e| StorageIOError::write_vote(&e))?;
        }
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.lock().vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        let mut inner = self.lock();
        inner.committed = committed;
        // Persist committed so a restart re-applies (snapshot.last, committed] (openraft's
        // save_committed contract — without it the SM would resume only at the last snapshot).
        if let Some(paths) = &inner.paths {
            control_store::write_value(&paths.committed(), &committed, inner.fsync)
                .map_err(|e| StorageIOError::write_logs(&e))?;
        }
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.lock().committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        {
            let mut inner = self.lock();
            let fsync = inner.fsync;
            for entry in entries {
                // Durable-first: persist the framed record BEFORE acknowledging the flush, so a
                // crash never loses an appended entry the leader believes is durable (Raft
                // correctness). In-memory ⇒ no file ⇒ readable the instant it's in the map.
                if let Some(file) = inner.log_file.as_mut() {
                    control_store::append_record(file, &entry, fsync)
                        .map_err(|e| StorageIOError::write_logs(&e))?;
                }
                inner.log.insert(entry.log_id.index, entry);
            }
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.lock();
        let keys: Vec<u64> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
        }
        // Drop the conflicting suffix on disk too (atomic rewrite + reopen the append handle).
        rewrite_and_reopen(&mut inner).map_err(|e| StorageIOError::write_logs(&e))?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.lock();
        inner.last_purged = Some(log_id);
        let keys: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
        }
        // Persist the new lower bound (so get_log_state is consistent after restart) + compact the
        // file. A snapshot already covers the purged prefix (openraft only purges post-snapshot).
        if let Some(paths) = &inner.paths {
            control_store::write_value(&paths.purged(), &log_id, inner.fsync)
                .map_err(|e| StorageIOError::write_logs(&e))?;
        }
        rewrite_and_reopen(&mut inner).map_err(|e| StorageIOError::write_logs(&e))?;
        Ok(())
    }
}
