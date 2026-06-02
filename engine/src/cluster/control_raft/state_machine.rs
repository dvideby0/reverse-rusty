//! openraft `RaftStateMachine`/`RaftSnapshotBuilder` for the control plane — applies committed
//! `ClusterStateChange` entries through the shared `control::apply` funnel (live ≡ replay) with
//! snapshot build/install + restart rebuild (ADR-038/041).

use std::io::Cursor;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use openraft::storage::RaftStateMachine;
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, OptionalSend, RaftSnapshotBuilder, Snapshot,
    SnapshotMeta, StorageError, StorageIOError, StoredMembership,
};
use serde::{Deserialize, Serialize};

use crate::cluster::control::{ClusterState, ControlError, NodeId};
use crate::cluster::control_store;

use super::{ClusterStateResponse, TypeConfig};

/// A built snapshot: the serialized [`ClusterState`] + its meta. `serde` so the durable backend
/// (ADR-041) persists it whole (`SnapshotMeta`/`Vec<u8>` are already serde — cf. `SnapshotEnvelope`).
#[derive(Clone, Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<u64, BasicNode>,
    data: Vec<u8>,
}

struct SmInner {
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
    /// The committed cluster-state document (ADR-037).
    state: ClusterState,
    snapshot: Option<StoredSnapshot>,
    /// Monotonic counter for unique snapshot ids (no wall-clock in library code).
    snapshot_idx: u64,
    /// Durable backing (ADR-041): `None` ⇒ in-memory; `Some` ⇒ the snapshot is persisted here so a
    /// restart rebuilds the SM from it (+ the log tail openraft replays up to `committed`).
    paths: Option<control_store::RaftPaths>,
    fsync: bool,
}

/// A [`RaftStateMachine`] over the cluster-state document — in-memory or durable (ADR-041).
/// `Arc`-shared so the owning [`RaftControlPlane`] can read the committed document directly (after
/// a linearizable check), and so `get_snapshot_builder` hands out a cheap clone.
#[derive(Clone)]
pub(in crate::cluster::control_raft) struct StateMachine {
    inner: Arc<Mutex<SmInner>>,
}

impl StateMachine {
    /// In-memory genesis from a seed document — the openraft-side analogue of
    /// [`InMemoryControlPlane::single_node`](crate::cluster::control::InMemoryControlPlane::single_node),
    /// so both backends start from a byte-identical document.
    pub(in crate::cluster::control_raft) fn in_memory(genesis: ClusterState) -> Self {
        Self::with_inner(SmInner {
            last_applied: None,
            last_membership: StoredMembership::default(),
            state: genesis,
            snapshot: None,
            snapshot_idx: 0,
            paths: None,
            fsync: false,
        })
    }

    /// Durable state machine rooted at `dir` (ADR-041): if a snapshot is persisted, rebuild the SM
    /// from it (state + last_applied + membership = the snapshot point); else genesis. openraft then
    /// replays the durable log tail (snapshot.last, committed] on top, so the SM resumes at committed.
    pub(in crate::cluster::control_raft) fn open(
        dir: &Path,
        genesis: ClusterState,
        fsync: bool,
    ) -> Result<Self, ControlError> {
        let cf: fn(std::io::Error) -> ControlError =
            |e| ControlError::Backend(format!("raft store open: {e}"));
        let paths = control_store::RaftPaths::new(dir.to_path_buf());
        let stored: Option<StoredSnapshot> =
            control_store::read_value(&paths.snapshot()).map_err(cf)?;
        let inner = match stored {
            Some(s) => {
                let state: ClusterState = serde_json::from_slice(&s.data).map_err(|e| {
                    ControlError::Backend(format!("control snapshot decode on restart: {e}"))
                })?;
                let last_applied = s.meta.last_log_id;
                let last_membership = s.meta.last_membership.clone();
                SmInner {
                    last_applied,
                    last_membership,
                    state,
                    snapshot: Some(s),
                    snapshot_idx: 0,
                    paths: Some(paths),
                    fsync,
                }
            }
            None => SmInner {
                last_applied: None,
                last_membership: StoredMembership::default(),
                state: genesis,
                snapshot: None,
                snapshot_idx: 0,
                paths: Some(paths),
                fsync,
            },
        };
        Ok(Self::with_inner(inner))
    }

    fn with_inner(inner: SmInner) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    fn lock(&self) -> MutexGuard<'_, SmInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A direct, NON-linearizable read of the committed document — used for follower-convergence
    /// assertions, where a linearizable read would forward to the leader.
    pub(in crate::cluster::control_raft) fn local_state(&self) -> ClusterState {
        self.lock().state.clone()
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let mut inner = self.lock();
        let data =
            serde_json::to_vec(&inner.state).map_err(|e| StorageIOError::read_state_machine(&e))?;
        inner.snapshot_idx += 1;
        let snapshot_id = match inner.last_applied {
            Some(log_id) => format!("{}-{}", log_id.index, inner.snapshot_idx),
            None => format!("none-{}", inner.snapshot_idx),
        };
        let meta = SnapshotMeta {
            last_log_id: inner.last_applied,
            last_membership: inner.last_membership.clone(),
            snapshot_id,
        };
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };
        // Persist the snapshot (ADR-041): it is what a restart rebuilds the SM from, and it lets
        // openraft purge the covered log prefix.
        if let Some(paths) = &inner.paths {
            control_store::write_value(&paths.snapshot(), &stored, inner.fsync)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        }
        inner.snapshot = Some(stored);
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let inner = self.lock();
        Ok((inner.last_applied, inner.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<ClusterStateResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut inner = self.lock();
        let mut responses = Vec::new();
        for entry in entries {
            inner.last_applied = Some(entry.log_id);
            match entry.payload {
                // A new leader's no-op marker — does NOT advance the semantic epoch.
                EntryPayload::Blank => {}
                // The ONE apply funnel shared with InMemoryControlPlane (live ≡ replay).
                EntryPayload::Normal(change) => {
                    crate::cluster::control::apply(&mut inner.state, change);
                    inner.state.epoch += 1;
                }
                // Raft membership ⇒ the app voter set (the faithful change_membership mapping).
                EntryPayload::Membership(m) => {
                    let mut voters: Vec<NodeId> = m.voter_ids().map(NodeId).collect();
                    voters.sort_unstable();
                    voters.dedup();
                    inner.state.voters = voters;
                    inner.last_membership = StoredMembership::new(Some(entry.log_id), m);
                    inner.state.epoch += 1;
                }
            }
            let version = inner.state.epoch;
            responses.push(ClusterStateResponse { version });
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let data = snapshot.into_inner();
        let state: ClusterState = serde_json::from_slice(&data)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        let mut inner = self.lock();
        inner.state = state;
        inner.last_applied = meta.last_log_id;
        inner.last_membership = meta.last_membership.clone();
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data,
        };
        // Persist an installed snapshot too (a follower catching up via the leader must survive its
        // own restart from the same point — ADR-041).
        if let Some(paths) = &inner.paths {
            control_store::write_value(&paths.snapshot(), &stored, inner.fsync)
                .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        }
        inner.snapshot = Some(stored);
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        Ok(self.lock().snapshot.as_ref().map(|s| Snapshot {
            meta: s.meta.clone(),
            snapshot: Box::new(Cursor::new(s.data.clone())),
        }))
    }
}
