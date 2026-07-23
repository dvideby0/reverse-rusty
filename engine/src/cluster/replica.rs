//! `ReplicatedShard` — a [`Shard`] composite owning one shard position's PRIMARY plus
//! N replica copies (clustering build-path step 4: the Elasticsearch/Cassandra
//! primary+replica HA model; ADR-035).
//!
//! It implements [`Shard`] and slots into the coordinator's `Vec<Box<dyn Shard>>` via
//! `from_parts` with ZERO coordinator changes — the coordinator still sees ONE shard per
//! position; the RF copies live inside this box. It composes over any `Box<dyn Shard>`, so
//! a replica may be in-process ([`LocalShard`]) or, behind the `distributed` feature, a
//! remote gRPC shard.
//!
//! ## Why replication is set-correct
//! Matching emits LOGICAL ids (local ids are segment-internal and issued append-only), so a
//! replica fed the SAME ordered op stream as its primary holds the SAME set of live logical
//! queries — byte-identical local ids are NOT required. Replication therefore reduces to
//! "apply the same op to every copy," which is what the write methods do.
//!
//! ## The correctness guards (zero false negatives)
//! - **Reads serve the primary; failover is transport-only and in-sync-only.** A read tries
//!   the primary and, only on a [`ShardError::Remote`] (unreachable), falls over to the next
//!   replica whose `in_sync` flag is set. A [`DictMismatch`](ShardError::DictMismatch) /
//!   `Config` / `Log` error propagates immediately (failing over would mask a real bug). If
//!   every reachable copy fails the error propagates — NEVER an empty/partial set (that would
//!   be a silent false negative). A replica that missed a write (out of sync) is never served.
//! - **Aggregation presents the PRIMARY's view.** `num_queries`/`class_counts` reflect ONE
//!   copy (the primary, or an in-sync replica on failover — they are set-equal), and
//!   `delete_by_logical_id` returns the PRIMARY's count. The coordinator SUMS these across
//!   shard POSITIONS, so summing replicas here would multiply totals by the replication factor.
//! - **Writes are primary-authoritative; replica failures are tolerated.** A write applies to
//!   the primary first (its return value is the composite's); if the primary errors the op
//!   fails. The same op then fans out to the in-sync replicas; a replica that errors is dropped
//!   from the in-sync set and a [`ReplicaDesync`](DurabilityOp::ReplicaDesync) event is
//!   surfaced (redundancy is reduced, but the write succeeded on the authoritative primary —
//!   the Elasticsearch model). A `wait_for_active_shards`-style write precondition is deferred
//!   to the control plane.
//!
//! ## Durability
//! The PRIMARY is the durable copy recorded in the coordinator manifest, so
//! `seal_for_checkpoint`/`segment_filenames`/`next_seg_id` delegate to it. Replicas are rebuilt
//! by [`peer_recover`] (durable clusters) or by replaying the op stream (in-memory clusters);
//! they are never catalogued in the manifest (the Elasticsearch "replicas are allocated, not
//! catalogued; the primary + log are the durable truth" stance).

mod shard_impl;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::events::{DurabilityOp, EngineEvent};
use crate::normalize::Normalizer;
use crate::tagdict::TagDict;

use super::clog::LogPos;
use super::shard::{apply_mutation, EventSink, LocalShard, Shard, ShardError};

struct ReplicaSlot {
    shard: Box<dyn Shard>,
    /// Cleared when a replicated op to this replica failed: it may be missing a write, so
    /// reads must NOT fail over to it (a stale read would be a silent false negative). Reset
    /// only by a (future) peer re-recovery.
    in_sync: AtomicBool,
}

/// A [`Shard`] composite: one primary + N replicas for a single shard position.
pub(crate) struct ReplicatedShard {
    primary: Box<dyn Shard>,
    /// The replica set. `Mutex<Vec<Arc<_>>>` (not a bare `Vec`) so a peer-recovered replica can be
    /// promoted into the in-sync set AT RUNTIME (ADR-040's finalize) without rebuilding the
    /// composite. Reads/fan-out snapshot-clone the `Arc` handles out under the lock and then work
    /// on the clones, so a slow (remote) probe never holds the lock; mutation of the set happens
    /// only under `write_lock`, so it can't race a fan-out.
    replicas: Mutex<Vec<Arc<ReplicaSlot>>>,
    /// Serializes write/seal so a write never interleaves with another op's replica fan-out, and
    /// so the finalize step ([`Self::promote_recovered_replica`]) can briefly quiesce the position
    /// to drain the last residual + insert the new replica atomically. Reads are lock-free.
    write_lock: Mutex<()>,
    /// Where degraded-redundancy events go once the coordinator installs its observer; until
    /// then they buffer in `pending_events` and flush on [`Self::set_event_sink`].
    event_sink: Mutex<Option<EventSink>>,
    pending_events: Mutex<Vec<EngineEvent>>,
}

impl ReplicatedShard {
    /// Wrap a primary + replicas. RF = 1 + `replicas.len()`. All copies must already be
    /// set-equal (seeded with the same op stream, or peer-recovered from the primary).
    pub(crate) fn new(primary: Box<dyn Shard>, replicas: Vec<Box<dyn Shard>>) -> Self {
        let replicas = replicas
            .into_iter()
            .map(|shard| {
                Arc::new(ReplicaSlot {
                    shard,
                    in_sync: AtomicBool::new(true),
                })
            })
            .collect();
        ReplicatedShard {
            primary,
            replicas: Mutex::new(replicas),
            write_lock: Mutex::new(()),
            event_sink: Mutex::new(None),
            pending_events: Mutex::new(Vec::new()),
        }
    }

    /// Snapshot-clone the current replica handles (cheap `Arc` clones) so a read/fan-out can work
    /// on a stable list without holding the replica-set lock across a (possibly remote) probe.
    fn replica_handles(&self) -> Vec<Arc<ReplicaSlot>> {
        self.replicas
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ()> {
        self.write_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Surface a degraded-redundancy event: deliver to the sink if installed, else buffer it
    /// for delivery when the coordinator calls [`Self::set_event_sink`].
    fn emit(&self, ev: EngineEvent) {
        let sink = self
            .event_sink
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(sink) = sink {
            sink(&ev);
        } else {
            self.pending_events
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(ev);
        }
    }

    /// Run a read on the primary; on a TRANSPORT error fall over to in-sync replicas in
    /// order. Structural errors propagate. Returns `Err` (never an empty result) if every
    /// reachable copy fails — a dropped probe must fail the read, not shrink it.
    fn read<T>(&self, op: impl Fn(&dyn Shard) -> Result<T, ShardError>) -> Result<T, ShardError> {
        let mut last_err = match op(self.primary.as_ref()) {
            Ok(v) => return Ok(v),
            Err(e @ ShardError::Remote(_)) => e,
            Err(e) => return Err(e),
        };
        for slot in self.replica_handles() {
            if !slot.in_sync.load(Ordering::Acquire) {
                continue; // never serve a stale replica
            }
            match op(slot.shard.as_ref()) {
                Ok(v) => return Ok(v),
                Err(e @ ShardError::Remote(_)) => last_err = e,
                Err(e) => return Err(e),
            }
        }
        Err(last_err)
    }

    /// Streaming-read analogue of [`Self::read`]. A transport failure may fail
    /// over only before the attempted copy has emitted its first provisional
    /// chunk. Once a chunk has escaped, replaying the request against another
    /// copy would splice two attempts into one sequence; fail closed and let the
    /// job retry under its idempotency contract instead (ADR-114).
    fn read_stream<T>(
        &self,
        sink: &mut dyn crate::delivery::ChunkSink,
        mut op: impl FnMut(&dyn Shard, &mut dyn crate::delivery::ChunkSink) -> Result<T, ShardError>,
    ) -> Result<T, ShardError> {
        struct CountingSink<'a> {
            inner: &'a mut dyn crate::delivery::ChunkSink,
            emitted: bool,
        }

        impl crate::delivery::ChunkSink for CountingSink<'_> {
            fn send_chunk(
                &mut self,
                chunk: &crate::delivery::MatchChunk,
            ) -> Result<(), crate::delivery::ChunkSinkError> {
                self.inner.send_chunk(chunk)?;
                self.emitted = true;
                Ok(())
            }

            fn check_cancelled(&mut self) -> Result<(), crate::delivery::ChunkSinkError> {
                self.inner.check_cancelled()
            }
        }

        fn attempt<T>(
            shard: &dyn Shard,
            sink: &mut dyn crate::delivery::ChunkSink,
            op: &mut impl FnMut(
                &dyn Shard,
                &mut dyn crate::delivery::ChunkSink,
            ) -> Result<T, ShardError>,
        ) -> (Result<T, ShardError>, bool) {
            let mut counting = CountingSink {
                inner: sink,
                emitted: false,
            };
            let result = op(shard, &mut counting);
            (result, counting.emitted)
        }

        let (primary, emitted) = attempt(self.primary.as_ref(), sink, &mut op);
        let mut last_err = match primary {
            Ok(value) => return Ok(value),
            Err(error @ ShardError::Remote(_)) if !emitted => error,
            Err(error) => return Err(error),
        };
        for slot in self.replica_handles() {
            if !slot.in_sync.load(Ordering::Acquire) {
                continue;
            }
            let (result, emitted) = attempt(slot.shard.as_ref(), sink, &mut op);
            match result {
                Ok(value) => return Ok(value),
                Err(error @ ShardError::Remote(_)) if !emitted => last_err = error,
                Err(error) => return Err(error),
            }
        }
        Err(last_err)
    }

    /// Fan a write (already applied to the primary) to the in-sync replicas. A replica that
    /// errors is dropped from the in-sync set and flagged; the op still succeeds (the
    /// authoritative primary holds the write). Caller holds [`Self::lock`].
    fn fan_to_replicas(&self, op: impl Fn(&dyn Shard) -> Result<(), ShardError>) {
        for (i, slot) in self.replica_handles().iter().enumerate() {
            if !slot.in_sync.load(Ordering::Acquire) {
                continue;
            }
            if let Err(e) = op(slot.shard.as_ref()) {
                slot.in_sync.store(false, Ordering::Release);
                self.emit(EngineEvent::DurabilityFailure {
                    op: DurabilityOp::ReplicaDesync,
                    detail: format!(
                        "replica {i} dropped from the in-sync set after a failed replicated \
                         op; redundancy reduced until peer re-recovery"
                    ),
                    error: e.to_string(),
                });
            }
        }
    }

    /// Atomically promote a peer-recovered `replica` into the in-sync set under a brief write
    /// quiesce (ADR-040). Holding `write_lock` blocks every composite write/fan-out, so the final
    /// residual drain + the in-sync insertion happen with NO write able to slip between them (which
    /// would be a silently-missed replica write — a redundancy gap, not a correctness gap, but
    /// closed regardless). The window is just the residual the convergence loop left, then the
    /// retention lease is released so the primary may trim the consumed tail again.
    fn promote_recovered_replica(
        &self,
        replica: Box<dyn Shard>,
        from: LogPos,
        norm: &Normalizer,
        dict: &Dict,
        lease: u64,
    ) -> Result<(), ShardError> {
        let _g = self.lock();
        // Re-entrancy-safe under `write_lock`: this reads the primary's translog + writes the new
        // replica's engine, neither of which takes the composite lock.
        catch_up_replica(replica.as_ref(), self.primary.as_ref(), norm, dict, from)?;
        self.replicas
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Arc::new(ReplicaSlot {
                shard: replica,
                in_sync: AtomicBool::new(true),
            }));
        self.primary.release_retention_lease(lease)?;
        Ok(())
    }
}

/// Bring a fresh replica up to a DURABLE primary's state — the in-process analogue of
/// Elasticsearch peer recovery ("stream segments from a peer, then replay the translog tail").
/// Seals the primary (flush memtable + reseal base-segment tombstones) so its on-disk `.seg`
/// set is a consistent snapshot at position `P`, copies those files (and `sources.dat` if
/// present — display-only, tolerated absent) into a clean `replica_dir`, attaches them via
/// [`LocalShard::open_segments`] (fail-loud on a missing/corrupt segment), and **replays the
/// primary's translog tail (ops > P)** into the new replica.
///
/// Because the tail captures the writes that landed *during* the copy, the position need
/// **not** be quiesced for the copy window (ADR-039 — closing ADR-036's gap): segments hold
/// exactly ops ≤ P and the tail exactly ops > P, so there is no overlap and no double-apply.
/// Returns the new replica plus the high-water position it caught up to; a later
/// [`catch_up_replica`] from that cursor drains any further tail (the brief finalize under
/// sustained writes). DURABLE-primary only: an in-memory primary has no files to copy.
pub(crate) fn peer_recover(
    norm: &Arc<Normalizer>,
    dict: &Arc<Dict>,
    tag_dict: &Arc<TagDict>,
    mut config: EngineConfig,
    primary: &dyn Shard,
    primary_dir: &Path,
    replica_dir: &Path,
) -> Result<(LocalShard, LogPos), ShardError> {
    // 1. Seal so the primary's on-disk segments are a consistent, tombstone-baked snapshot at
    //    position `P`; the translog's remaining tail is exactly the un-sealed ops > P.
    let snapshot_pos = primary.seal_for_checkpoint()?;
    let files = primary.segment_filenames()?;
    let next_seg_id = primary.next_seg_id()?;

    // 2. Clean slate, then copy each committed segment from the peer. (A stale dir from a
    //    prior recovery is cleared so no orphan lingers; a real removal failure surfaces.)
    let replica_seg_dir = replica_dir.join("segments");
    if replica_seg_dir.exists() {
        std::fs::remove_dir_all(&replica_seg_dir).map_err(|e| {
            ShardError::Log(format!(
                "peer recovery: clearing stale segments {}: {e}",
                replica_seg_dir.display()
            ))
        })?;
    }
    std::fs::create_dir_all(&replica_seg_dir).map_err(|e| {
        ShardError::Log(format!(
            "peer recovery: creating {}: {e}",
            replica_seg_dir.display()
        ))
    })?;
    let primary_seg_dir = primary_dir.join("segments");
    for name in &files {
        std::fs::copy(primary_seg_dir.join(name), replica_seg_dir.join(name))
            .map_err(|e| ShardError::Log(format!("peer recovery: copying segment {name}: {e}")))?;
    }

    // 3. `sources.dat` is display-only (never on the match path): copy it if present, but a
    //    missing one must not fail recovery.
    let replica_sources = replica_dir.join("sources.dat");
    if replica_sources.exists() {
        std::fs::remove_file(&replica_sources).map_err(|e| {
            ShardError::Log(format!("peer recovery: clearing stale sources.dat: {e}"))
        })?;
    }
    let primary_sources = primary_dir.join("sources.dat");
    if primary_sources.exists() {
        std::fs::copy(&primary_sources, &replica_sources)
            .map_err(|e| ShardError::Log(format!("peer recovery: copying sources.dat: {e}")))?;
    }

    // 4. Attach the copied segments against the shared dict (fail-loud on any missing/corrupt).
    config.data_dir = Some(replica_dir.to_path_buf());
    let replica = LocalShard::open_segments(
        Arc::clone(norm),
        Arc::clone(dict),
        Arc::clone(tag_dict),
        config,
        &files,
        next_seg_id,
    )?;

    // 5. Replay the primary's translog tail (ops > P) — the writes that landed during the
    //    copy. This is what lifts the quiesce: the segment copy ran concurrently with writes,
    //    and those writes are recovered here rather than lost.
    let hwm = catch_up_replica(&replica, primary, norm, dict, snapshot_pos)?;
    Ok((replica, hwm))
}

/// Replay `source`'s un-sealed translog tail (ops strictly after `from`) into `target` through
/// the normal write path — so each op is re-derived from its raw DSL against the frozen dict
/// (byte-identical to the original write) and re-logged into `target`'s own translog. Returns
/// the highest position applied (`from` if the tail was empty). The catch-up half of no-quiesce
/// recovery (ADR-039): re-runnable, so calling it again with the returned cursor drains any
/// further tail — the basis for a brief finalize catch-up after the bulk copy.
pub(crate) fn catch_up_replica(
    target: &dyn Shard,
    source: &dyn Shard,
    norm: &Normalizer,
    dict: &Dict,
    from: LogPos,
) -> Result<LogPos, ShardError> {
    let tail = source.translog_tail(from)?;
    let mut hwm = from;
    for (pos, m) in &tail {
        // `None`: a translog entry was stored AT this position by the source, so
        // placement coverage holds by construction for its replica.
        apply_mutation(target, norm, dict, m, None)?;
        hwm = (*pos).max(hwm);
    }
    Ok(hwm)
}
