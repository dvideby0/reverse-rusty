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

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use crate::compile::Extracted;
use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::events::{DurabilityOp, EngineEvent};
use crate::normalize::Normalizer;
use crate::segment::{IngestReport, MatchStats};

use super::clog::{ClusterMutation, LogPos};
use super::shard::{apply_mutation, EventSink, LocalShard, Shard, ShardError};

/// One replica copy plus its in-sync flag.
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

impl Shard for ReplicatedShard {
    // ---- reads (primary, with in-sync failover) ----
    fn percolate(
        &self,
        title: &str,
        include_broad: bool,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        self.read(|s| s.percolate(title, include_broad))
    }

    fn num_queries(&self) -> Result<usize, ShardError> {
        self.read(|s| s.num_queries())
    }

    fn class_counts(&self) -> Result<[u64; 4], ShardError> {
        self.read(|s| s.class_counts())
    }

    // ---- writes (primary-authoritative, fan out to replicas) ----
    fn ingest_extracted(
        &self,
        items: &[(u64, Extracted, String, u32)],
    ) -> Result<IngestReport, ShardError> {
        let _g = self.lock();
        let report = self.primary.ingest_extracted(items)?;
        self.fan_to_replicas(|s| s.ingest_extracted(items).map(|_| ()));
        Ok(report)
    }

    fn insert_extracted(
        &self,
        ex: &Extracted,
        logical: u64,
        version: u32,
        text: &str,
    ) -> Result<Option<u32>, ShardError> {
        let _g = self.lock();
        let out = self.primary.insert_extracted(ex, logical, version, text)?;
        self.fan_to_replicas(|s| s.insert_extracted(ex, logical, version, text).map(|_| ()));
        Ok(out)
    }

    fn delete_by_logical_id(&self, logical: u64) -> Result<usize, ShardError> {
        let _g = self.lock();
        let n = self.primary.delete_by_logical_id(logical)?;
        self.fan_to_replicas(|s| s.delete_by_logical_id(logical).map(|_| ()));
        Ok(n)
    }

    fn flush(&self) -> Result<(), ShardError> {
        let _g = self.lock();
        self.primary.flush()?;
        self.fan_to_replicas(|s| s.flush());
        Ok(())
    }

    // ---- durable checkpoint: PRIMARY ONLY (replicas are not in the manifest) ----
    fn seal_for_checkpoint(&self) -> Result<LogPos, ShardError> {
        let _g = self.lock();
        self.primary.seal_for_checkpoint()
    }

    fn segment_filenames(&self) -> Result<Vec<String>, ShardError> {
        self.primary.segment_filenames()
    }

    fn next_seg_id(&self) -> Result<u64, ShardError> {
        self.primary.next_seg_id()
    }

    // The position-bearing tail is the PRIMARY's (the authoritative copy + the recovery
    // source); a recovering replica is brought up from the primary's segments + this tail.
    fn translog_tail(&self, from: LogPos) -> Result<Vec<(LogPos, ClusterMutation)>, ShardError> {
        self.primary.translog_tail(from)
    }

    // ---- translog retention (ADR-040): the PRIMARY is the recovery source, so leases live there ----
    fn acquire_retention_lease(&self) -> Result<(u64, LogPos), ShardError> {
        self.primary.acquire_retention_lease()
    }
    fn renew_retention_lease(&self, lease: u64, to: LogPos) -> Result<(), ShardError> {
        self.primary.renew_retention_lease(lease, to)
    }
    fn release_retention_lease(&self, lease: u64) -> Result<(), ShardError> {
        self.primary.release_retention_lease(lease)
    }

    /// Bring up a NEW replica and add it to the in-sync set WITHOUT quiescing writes for the
    /// segment-copy window (ADR-040's finalize, closing ADR-036's whole-copy quiesce): peer-recover
    /// a snapshot at `P` + the initial tail, loop the catch-up until the tail stops advancing (a
    /// `max_passes` safety bound), then promote under a brief write quiesce. A retention lease held
    /// on the primary for the whole flow guarantees the tail the recovery still needs is never
    /// trimmed by a concurrent seal — so correctness never depends on the loop converging, only the
    /// final quiesce window's size does (`max_passes` shrinks it toward zero). Durable primary only.
    fn add_recovered_replica(
        &self,
        norm: &Arc<Normalizer>,
        dict: &Arc<Dict>,
        config: EngineConfig,
        primary_dir: &Path,
        replica_dir: &Path,
        max_passes: usize,
    ) -> Result<(), ShardError> {
        // One lease pins the primary's tail across the whole recovery; released at promotion, or
        // on failure (below) so a botched recovery can't pin the translog forever.
        let (lease, _at) = self.primary.acquire_retention_lease()?;
        let recover = || -> Result<(Box<dyn Shard>, LogPos), ShardError> {
            // Bulk copy at P + the initial tail replay — writes to the primary continue throughout.
            let (replica, mut hwm) = peer_recover(
                norm,
                dict,
                config,
                self.primary.as_ref(),
                primary_dir,
                replica_dir,
            )?;
            self.primary.renew_retention_lease(lease, hwm)?;
            // Convergence loop: drain the un-sealed tail repeatedly until it stops advancing,
            // shrinking the residual the final quiesce must cover. Renew the lease each pass so the
            // primary can GC the now-consumed prefix on its next seal.
            for _ in 0..max_passes {
                let next = catch_up_replica(&replica, self.primary.as_ref(), norm, dict, hwm)?;
                self.primary.renew_retention_lease(lease, next)?;
                if next == hwm {
                    break; // tail fully drained at this instant — converged
                }
                hwm = next;
            }
            Ok((Box::new(replica) as Box<dyn Shard>, hwm))
        };
        match recover() {
            Ok((replica, hwm)) => self.promote_recovered_replica(replica, hwm, norm, dict, lease),
            Err(e) => {
                // Best-effort cleanup: release the lease so a botched recovery can't pin the
                // primary's translog forever. A release failure (only a remote primary could
                // produce one) is surfaced, not swallowed; the original recovery error is returned.
                if let Err(rel) = self.primary.release_retention_lease(lease) {
                    self.emit(EngineEvent::DurabilityFailure {
                        op: DurabilityOp::ReplicaDesync,
                        detail: "releasing the retention lease after a failed peer recovery also \
                                 failed; the primary may retain extra translog until its next \
                                 successful seal"
                            .into(),
                        error: rel.to_string(),
                    });
                }
                Err(e)
            }
        }
    }

    // ---- observability ----
    fn set_event_sink(&self, sink: EventSink) {
        let pending: Vec<EngineEvent> = {
            let mut p = self
                .pending_events
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            std::mem::take(&mut *p)
        };
        for ev in &pending {
            sink(ev);
        }
        *self
            .event_sink
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(sink);
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
        apply_mutation(target, norm, dict, m)?;
        hwm = (*pos).max(hwm);
    }
    Ok(hwm)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU8;

    use super::*;

    /// (shared normalizer, frozen dict, per-query `(id, Extracted, dsl)`) — what
    /// [`compile_corpus`] returns.
    type CompiledCorpus = (Arc<Normalizer>, Arc<Dict>, Vec<(u64, Extracted, String)>);

    /// Compile a list of `(id, DSL)` into a shared frozen dict + the per-query `Extracted`,
    /// mirroring `ClusterEngine::build`'s pass A (extract into the dict, then finalize the
    /// hot mask). Lets a test seed a `LocalShard` at the same low level the coordinator uses.
    fn compile_corpus(dsls: &[(u64, &str)]) -> CompiledCorpus {
        let norm = Arc::new(Normalizer::default_vocab().expect("built-in vocab"));
        let mut dict = Dict::new();
        let mut lc = String::new();
        let mut out = Vec::new();
        for (id, dsl) in dsls {
            let ast = crate::dsl::parse(dsl).expect("test dsl parses");
            let ex = crate::compile::extract(&ast, &norm, &mut dict, &mut lc);
            out.push((*id, ex, (*dsl).to_string()));
        }
        dict.finalize_mask();
        (norm, Arc::new(dict), out)
    }

    fn scratch_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("rr_replica_{}_{}", tag, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// A fault-injecting `Shard` for the failover/ack tests: reads return a configured error
    /// (or an empty result), writes optionally error.
    struct FailingShard {
        /// 0 = ok, 1 = `Remote`, 2 = `DictMismatch` — applied to every read.
        read_mode: AtomicU8,
        fail_writes: AtomicBool,
    }

    impl FailingShard {
        fn reads_remote() -> Self {
            FailingShard {
                read_mode: AtomicU8::new(1),
                fail_writes: AtomicBool::new(false),
            }
        }
        fn reads_dict_mismatch() -> Self {
            FailingShard {
                read_mode: AtomicU8::new(2),
                fail_writes: AtomicBool::new(false),
            }
        }
        fn writes_fail() -> Self {
            FailingShard {
                read_mode: AtomicU8::new(0),
                fail_writes: AtomicBool::new(false),
            }
            .with_failing_writes()
        }
        fn with_failing_writes(self) -> Self {
            self.fail_writes.store(true, Ordering::Release);
            self
        }
        fn read_err(&self) -> Option<ShardError> {
            match self.read_mode.load(Ordering::Acquire) {
                1 => Some(ShardError::Remote("injected".into())),
                2 => Some(ShardError::DictMismatch {
                    expected: 1,
                    actual: 2,
                }),
                _ => None,
            }
        }
        fn write_err(&self) -> Result<(), ShardError> {
            if self.fail_writes.load(Ordering::Acquire) {
                Err(ShardError::Remote("injected write".into()))
            } else {
                Ok(())
            }
        }
    }

    impl Shard for FailingShard {
        fn percolate(&self, _t: &str, _b: bool) -> Result<(Vec<u64>, MatchStats), ShardError> {
            match self.read_err() {
                Some(e) => Err(e),
                None => Ok((Vec::new(), MatchStats::default())),
            }
        }
        fn num_queries(&self) -> Result<usize, ShardError> {
            self.read_err().map_or(Ok(0), Err)
        }
        fn class_counts(&self) -> Result<[u64; 4], ShardError> {
            self.read_err().map_or(Ok([0; 4]), Err)
        }
        fn ingest_extracted(
            &self,
            _i: &[(u64, Extracted, String, u32)],
        ) -> Result<IngestReport, ShardError> {
            self.write_err().map(|()| IngestReport::default())
        }
        fn insert_extracted(
            &self,
            _e: &Extracted,
            _l: u64,
            _v: u32,
            _t: &str,
        ) -> Result<Option<u32>, ShardError> {
            self.write_err().map(|()| Some(0))
        }
        fn delete_by_logical_id(&self, _l: u64) -> Result<usize, ShardError> {
            self.write_err().map(|()| 0)
        }
        fn flush(&self) -> Result<(), ShardError> {
            self.write_err()
        }
        fn seal_for_checkpoint(&self) -> Result<LogPos, ShardError> {
            Ok(LogPos(0))
        }
        fn segment_filenames(&self) -> Result<Vec<String>, ShardError> {
            Ok(Vec::new())
        }
        fn next_seg_id(&self) -> Result<u64, ShardError> {
            Ok(0)
        }
        fn translog_tail(
            &self,
            _from: LogPos,
        ) -> Result<Vec<(LogPos, ClusterMutation)>, ShardError> {
            Ok(Vec::new())
        }
    }

    fn seed(shard: &dyn Shard, corpus: &[(u64, Extracted, String)]) {
        for (id, ex, dsl) in corpus {
            shard
                .insert_extracted(ex, *id, 1, dsl)
                .expect("seed insert");
        }
    }

    #[test]
    fn read_fails_over_to_in_sync_replica() {
        let (norm, dict, corpus) = compile_corpus(&[(1, "alpha bravo"), (2, "charlie delta")]);
        let replica = LocalShard::new(
            Arc::clone(&norm),
            Arc::clone(&dict),
            EngineConfig::default(),
        );
        seed(&replica, &corpus);
        let rs = ReplicatedShard::new(
            Box::new(FailingShard::reads_remote()) as Box<dyn Shard>,
            vec![Box::new(replica) as Box<dyn Shard>],
        );

        // Primary errors on read (transport); the composite fails over to the in-sync replica.
        let (ids, _) = rs
            .percolate("alpha bravo zulu", false)
            .expect("failover read");
        assert!(
            ids.contains(&1),
            "failover must return the replica's match: {ids:?}"
        );

        // Drop the replica out of sync: with no healthy copy the read must ERR, never return
        // an empty set (that would be a silent false negative).
        rs.replica_handles()[0]
            .in_sync
            .store(false, Ordering::Release);
        assert!(
            matches!(
                rs.percolate("alpha bravo zulu", false),
                Err(ShardError::Remote(_))
            ),
            "with no in-sync copy the read must surface an error, not an empty set"
        );
    }

    #[test]
    fn read_does_not_fail_over_on_dict_mismatch() {
        let (norm, dict, corpus) = compile_corpus(&[(1, "alpha bravo")]);
        let replica = LocalShard::new(
            Arc::clone(&norm),
            Arc::clone(&dict),
            EngineConfig::default(),
        );
        seed(&replica, &corpus);
        let rs = ReplicatedShard::new(
            Box::new(FailingShard::reads_dict_mismatch()) as Box<dyn Shard>,
            vec![Box::new(replica) as Box<dyn Shard>],
        );
        // A DictMismatch is structural: it must propagate, not fail over to the (matching)
        // replica — failing over would mask a divergent feature space, itself a silent-FN hazard.
        assert!(
            matches!(
                rs.percolate("alpha bravo zulu", false),
                Err(ShardError::DictMismatch { .. })
            ),
            "DictMismatch must propagate without failover"
        );
    }

    #[test]
    fn primary_write_failure_propagates() {
        let (norm, dict, corpus) = compile_corpus(&[(1, "alpha bravo")]);
        let healthy = LocalShard::new(
            Arc::clone(&norm),
            Arc::clone(&dict),
            EngineConfig::default(),
        );
        let rs = ReplicatedShard::new(
            Box::new(FailingShard::writes_fail()) as Box<dyn Shard>,
            vec![Box::new(healthy) as Box<dyn Shard>],
        );
        let (id, ex, dsl) = &corpus[0];
        assert!(
            rs.insert_extracted(ex, *id, 1, dsl).is_err(),
            "a primary write failure must fail the op"
        );
    }

    #[test]
    fn replica_write_failure_is_tolerated_and_flagged() {
        let (norm, dict, corpus) = compile_corpus(&[(1, "alpha bravo")]);
        let primary = LocalShard::new(
            Arc::clone(&norm),
            Arc::clone(&dict),
            EngineConfig::default(),
        );
        let rs = ReplicatedShard::new(
            Box::new(primary) as Box<dyn Shard>,
            vec![Box::new(FailingShard::writes_fail()) as Box<dyn Shard>],
        );
        // Capture surfaced events (avoid requiring EngineEvent: Clone — record a flag).
        let saw_desync = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&saw_desync);
        rs.set_event_sink(Arc::new(move |ev: &EngineEvent| {
            if matches!(
                ev,
                EngineEvent::DurabilityFailure {
                    op: DurabilityOp::ReplicaDesync,
                    ..
                }
            ) {
                flag.store(true, Ordering::Release);
            }
        }));

        let (id, ex, dsl) = &corpus[0];
        assert!(
            rs.insert_extracted(ex, *id, 1, dsl).is_ok(),
            "a replica write failure must not fail the op (primary is authoritative)"
        );
        assert!(
            !rs.replica_handles()[0].in_sync.load(Ordering::Acquire),
            "the failed replica must drop out of the in-sync set"
        );
        assert!(
            saw_desync.load(Ordering::Acquire),
            "a ReplicaDesync event must be surfaced"
        );
        assert!(
            rs.percolate("alpha bravo zulu", true).is_ok(),
            "the primary still serves reads after a replica desyncs"
        );
    }

    #[test]
    fn replicas_stay_set_equal_through_op_stream() {
        let (norm, dict, corpus) = compile_corpus(&[
            (1, "alpha bravo"),
            (2, "charlie delta"),
            (3, "echo foxtrot"),
        ]);
        let primary = LocalShard::new(
            Arc::clone(&norm),
            Arc::clone(&dict),
            EngineConfig::default(),
        );
        let replica = LocalShard::new(
            Arc::clone(&norm),
            Arc::clone(&dict),
            EngineConfig::default(),
        );
        let rs = ReplicatedShard::new(Box::new(primary), vec![Box::new(replica)]);

        // Drive a mixed op stream through the composite.
        for (id, ex, dsl) in &corpus {
            rs.insert_extracted(ex, *id, 1, dsl).expect("insert");
        }
        rs.delete_by_logical_id(2).expect("delete");

        // Primary and replica must hold the same live set.
        assert_eq!(
            rs.primary.num_queries().expect("primary count"),
            rs.replica_handles()[0]
                .shard
                .num_queries()
                .expect("replica count"),
            "primary and replica query counts diverged"
        );
        for title in [
            "alpha bravo zulu",
            "charlie delta zulu",
            "echo foxtrot zulu",
            "nothing here",
        ] {
            let (mut p, _) = rs.primary.percolate(title, true).expect("primary read");
            let (mut r, _) = rs.replica_handles()[0]
                .shard
                .percolate(title, true)
                .expect("replica read");
            p.sort_unstable();
            r.sort_unstable();
            assert_eq!(p, r, "primary and replica diverged on {title:?}");
        }
        // id 2 was deleted on the primary (and, by fan-out, the replica).
        let (deleted_probe, _) = rs
            .primary
            .percolate("charlie delta zulu", true)
            .expect("read");
        assert!(
            !deleted_probe.contains(&2),
            "the deleted id must be gone on the primary"
        );
    }

    #[test]
    fn aggregation_is_primary_only() {
        // num_queries / class_counts reflect ONE copy (not summed across replicas), so the
        // coordinator's cross-position sums stay correct at RF>1.
        let (norm, dict, corpus) = compile_corpus(&[(1, "alpha bravo"), (2, "charlie delta")]);
        let primary = LocalShard::new(
            Arc::clone(&norm),
            Arc::clone(&dict),
            EngineConfig::default(),
        );
        let replica = LocalShard::new(
            Arc::clone(&norm),
            Arc::clone(&dict),
            EngineConfig::default(),
        );
        let rs = ReplicatedShard::new(Box::new(primary), vec![Box::new(replica)]);
        for (id, ex, dsl) in &corpus {
            rs.insert_extracted(ex, *id, 1, dsl).expect("insert");
        }
        assert_eq!(
            rs.num_queries().expect("count"),
            2,
            "num_queries must be the primary's (2), not summed across copies (4)"
        );
        assert_eq!(
            rs.class_counts().expect("class counts").iter().sum::<u64>(),
            2,
            "class counts must total the primary's queries (2), not summed across copies (4)"
        );
        let removed = rs.delete_by_logical_id(1).expect("delete");
        assert_eq!(
            removed, 1,
            "delete count must be the primary's (1), not summed across copies"
        );
    }

    #[test]
    fn peer_recover_reproduces_primary_set_including_tombstone() {
        let (norm, dict, corpus) = compile_corpus(&[
            (1, "alpha bravo"),
            (2, "charlie delta"),
            (3, "echo foxtrot"),
        ]);
        let tmp = scratch_dir("recover");
        let primary_dir = tmp.join("primary");
        let replica_dir = tmp.join("replica");

        // Durable primary: seed, flush to a base segment, then delete id 2 (a BASE tombstone,
        // so peer recovery's reseal must bake it in — else id 2 would resurrect).
        let pc = EngineConfig {
            data_dir: Some(primary_dir.clone()),
            ..EngineConfig::default()
        };
        let primary = LocalShard::new_durable(Arc::clone(&norm), Arc::clone(&dict), pc)
            .expect("durable primary");
        seed(&primary, &corpus);
        primary.flush().expect("flush to base");
        primary.delete_by_logical_id(2).expect("delete id 2");

        let (replica, _hwm) = peer_recover(
            &norm,
            &dict,
            EngineConfig::default(),
            &primary,
            &primary_dir,
            &replica_dir,
        )
        .expect("peer recovery");

        for title in [
            "alpha bravo zulu",
            "charlie delta zulu",
            "echo foxtrot zulu",
        ] {
            let (mut p, _) = primary.percolate(title, true).expect("primary read");
            let (mut r, _) = replica.percolate(title, true).expect("replica read");
            p.sort_unstable();
            r.sort_unstable();
            assert_eq!(p, r, "recovered replica diverged on {title:?}");
        }
        let (probe, _) = replica.percolate("charlie delta zulu", true).expect("read");
        assert!(
            !probe.contains(&2),
            "the baked tombstone must not resurrect on the recovered replica"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn peer_recover_replays_tail_without_quiescing() {
        // The headline in-process property (ADR-039): a segment snapshot is taken at position
        // `P`, writes land AFTER it (id 10 added, id 1 removed — in the primary's translog,
        // > P), and the recovering replica catches them up via the TRANSLOG TAIL — no segment
        // re-copy, no quiesce. Ordered (snapshot → write → catch-up) for determinism; it
        // exercises the exact path a concurrent recovery uses for writes that arrive during the
        // copy window. The pre-catch-up staleness assertion proves the writes truly post-date
        // the snapshot (else the test would pass trivially).
        let (norm, dict, corpus) = compile_corpus(&[
            (1, "alpha bravo"),
            (2, "charlie delta"),
            (3, "echo foxtrot"),
            (10, "alpha bravo"),
        ]);
        let tmp = scratch_dir("tail");
        let primary_dir = tmp.join("primary");
        let replica_dir = tmp.join("replica");

        let pc = EngineConfig {
            data_dir: Some(primary_dir.clone()),
            ..EngineConfig::default()
        };
        let primary = LocalShard::new_durable(Arc::clone(&norm), Arc::clone(&dict), pc)
            .expect("durable primary");
        // The snapshot corpus = ids 1..3 (id 10 is held back for a post-snapshot add).
        for (id, ex, dsl) in corpus.iter().take(3) {
            primary.insert_extracted(ex, *id, 1, dsl).expect("seed");
        }

        // Snapshot: peer_recover seals the primary at P, copies segments, replays the (empty)
        // tail; `hwm` is the position the replica is caught up to in the primary's log space.
        let (replica, hwm) = peer_recover(
            &norm,
            &dict,
            EngineConfig::default(),
            &primary,
            &primary_dir,
            &replica_dir,
        )
        .expect("peer recovery");

        // Writes that land AFTER the snapshot (into the primary's translog, > hwm).
        let (_, ex10, dsl10) = &corpus[3]; // id 10, "alpha bravo"
        primary
            .insert_extracted(ex10, 10, 1, dsl10)
            .expect("post-snapshot add");
        primary
            .delete_by_logical_id(1)
            .expect("post-snapshot delete");

        // Pre-catch-up the replica is STALE (still has id 1, lacks id 10): the writes truly
        // post-date the copied snapshot.
        let (pre, _) = replica.percolate("alpha bravo zulu", true).expect("read");
        assert!(
            pre.contains(&1) && !pre.contains(&10),
            "replica must be stale before catch-up (proving writes post-date the snapshot): {pre:?}"
        );

        // Replay the tail (ops > hwm) — the no-quiesce recovery delta.
        catch_up_replica(&replica, &primary, &norm, &dict, hwm).expect("catch up");

        // The replica now equals the primary on every probe: id 10 present, id 1 gone.
        for title in [
            "alpha bravo zulu",
            "charlie delta zulu",
            "echo foxtrot zulu",
        ] {
            let (mut p, _) = primary.percolate(title, true).expect("primary");
            let (mut r, _) = replica.percolate(title, true).expect("replica");
            p.sort_unstable();
            r.sort_unstable();
            assert_eq!(
                p, r,
                "replica diverged from primary on {title:?} after catch-up"
            );
        }
        let (after, _) = replica.percolate("alpha bravo zulu", true).expect("read");
        assert!(
            after.contains(&10) && !after.contains(&1),
            "the translog tail was not applied on catch-up: {after:?}"
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn durable_shard_self_restarts_from_translog() {
        // ADR-039 §6: a durable data node crashes with un-sealed writes in its translog and
        // restarts from disk — `new_durable` finds the checkpoint sidecar, attaches the committed
        // segments AND replays the translog tail (the ops the last seal had not yet baked). The
        // reopened shard equals the pre-crash live set, with a removed id NOT resurrecting.
        let (norm, dict, corpus) = compile_corpus(&[
            (1, "alpha bravo"),
            (2, "charlie delta"),
            (3, "echo foxtrot"),
            (4, "golf hotel"),
        ]);
        let tmp = scratch_dir("selfrestart");
        let cfg = EngineConfig {
            data_dir: Some(tmp.clone()),
            ..EngineConfig::default()
        };

        {
            let shard = LocalShard::new_durable(Arc::clone(&norm), Arc::clone(&dict), cfg.clone())
                .expect("durable shard");
            // Sealed base: ids 1, 2 (flushed into a segment; the sidecar commits at position P).
            shard
                .insert_extracted(&corpus[0].1, 1, 1, &corpus[0].2)
                .expect("ins 1");
            shard
                .insert_extracted(&corpus[1].1, 2, 1, &corpus[1].2)
                .expect("ins 2");
            shard.seal_for_checkpoint().expect("seal");
            // Un-sealed translog tail (> P): add 3, add 4, remove 1 — only in the translog.
            shard
                .insert_extracted(&corpus[2].1, 3, 1, &corpus[2].2)
                .expect("ins 3");
            shard
                .insert_extracted(&corpus[3].1, 4, 1, &corpus[3].2)
                .expect("ins 4");
            shard.delete_by_logical_id(1).expect("del 1");
            // "Crash": drop without another seal — the tail lives only in the translog.
        }

        // Restart from the sidecar: attach segments (1, 2) + replay the tail (add 3, add 4,
        // remove 1) → live set {2, 3, 4}.
        let reopened = LocalShard::new_durable(Arc::clone(&norm), Arc::clone(&dict), cfg)
            .expect("self-restart");
        let probe = |title: &str| -> Vec<u64> {
            let (mut ids, _) = reopened.percolate(title, true).expect("read");
            ids.sort_unstable();
            ids
        };
        assert_eq!(
            probe("alpha bravo zulu"),
            Vec::<u64>::new(),
            "id 1 was removed in the tail; it must not resurrect on self-restart"
        );
        assert_eq!(probe("charlie delta zulu"), vec![2], "sealed id 2 survives");
        assert_eq!(
            probe("echo foxtrot zulu"),
            vec![3],
            "tail add id 3 recovered"
        );
        assert_eq!(probe("golf hotel zulu"), vec![4], "tail add id 4 recovered");
        // Physical entry count: 2 sealed (ids 1, 2) + 2 tail adds (ids 3, 4). id 1's sealed entry
        // is tombstoned (the matching probes above prove it is excluded), not yet compacted away —
        // exactly what a non-restarted shard applying the same ops reports.
        assert_eq!(
            reopened.num_queries().expect("count"),
            4,
            "physical count = 2 sealed + 2 tail (id 1 tombstoned, awaiting compaction)"
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn seal_honors_retention_lease_so_concurrent_seal_keeps_the_recovery_tail() {
        // ADR-040: a peer recovery acquires a retention lease at position `at`, then more writes
        // land and the source SEALS AGAIN (a concurrent checkpoint, or another recovery's
        // FetchSegments). Without the lease that seal would trim the translog to its new `P`,
        // erasing the tail (> at) the in-flight recovery still needs — a silent false negative.
        // With the lease the seal trims only to `at`, so the tail survives; releasing it lets the
        // source GC again. (This is the latent FN ADR-039's no-quiesce path left open.)
        let (norm, dict, corpus) = compile_corpus(&[
            (1, "alpha bravo"),
            (2, "charlie delta"),
            (3, "echo foxtrot"),
        ]);
        let dir = scratch_dir("retain");
        let cfg = EngineConfig {
            data_dir: Some(dir.clone()),
            ..EngineConfig::default()
        };
        let primary = LocalShard::new_durable(Arc::clone(&norm), Arc::clone(&dict), cfg)
            .expect("durable primary");
        // Seed id 1 and seal a base — the recovery baseline.
        primary
            .insert_extracted(&corpus[0].1, 1, 1, &corpus[0].2)
            .expect("ins 1");
        let at_seal = primary.seal_for_checkpoint().expect("seal 1");

        // The recovery pins the tail at the current high-water.
        let (lease, at) = primary.acquire_retention_lease().expect("lease");
        assert_eq!(at, at_seal, "lease pins the post-seal high-water");

        // Writes land AFTER the snapshot (into the translog, > at).
        primary
            .insert_extracted(&corpus[1].1, 2, 1, &corpus[1].2)
            .expect("ins 2");
        primary
            .insert_extracted(&corpus[2].1, 3, 1, &corpus[2].2)
            .expect("ins 3");

        // A concurrent seal: WITHOUT the lease it would trim to its new P and drop (at, P]; the
        // lease holds the floor at `at`, so the tail the recovery needs survives.
        let p1 = primary.seal_for_checkpoint().expect("seal 2");
        assert!(
            p1 > at,
            "the second seal advanced the checkpoint past the pinned point"
        );
        let tail = primary.translog_tail(at).expect("tail");
        let ids: Vec<u64> = tail
            .iter()
            .map(|(_, m)| match m {
                ClusterMutation::Add { logical, .. } | ClusterMutation::Remove { logical } => {
                    *logical
                }
            })
            .collect();
        assert_eq!(
            ids,
            vec![2, 3],
            "the lease kept the post-snapshot tail (> at)"
        );

        // Release: the next seal trims freely again (GC), so the pinned tail is now gone.
        primary.release_retention_lease(lease).expect("release");
        primary.seal_for_checkpoint().expect("seal 3");
        assert!(
            primary
                .translog_tail(at)
                .expect("tail after release")
                .is_empty(),
            "a released lease lets the source GC the consumed tail"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn add_recovered_replica_promotes_an_in_sync_set_equal_replica() {
        // ADR-040 finalize: add a replica to a live position at runtime — peer-recover + converge +
        // promote under a brief quiesce. The promoted replica is in-sync (a later write fans out to
        // it) and set-equal to the primary.
        let (norm, dict, corpus) = compile_corpus(&[
            (1, "alpha bravo"),
            (2, "charlie delta"),
            (3, "golf hotel"), // written AFTER promotion, so the frozen dict must already know it
        ]);
        let tmp = scratch_dir("addrep");
        let primary_dir = tmp.join("primary");
        let replica_dir = tmp.join("replica");
        let pc = EngineConfig {
            data_dir: Some(primary_dir.clone()),
            ..EngineConfig::default()
        };
        let primary = LocalShard::new_durable(Arc::clone(&norm), Arc::clone(&dict), pc)
            .expect("durable primary");
        primary
            .insert_extracted(&corpus[0].1, 1, 1, &corpus[0].2)
            .expect("ins 1");
        primary
            .insert_extracted(&corpus[1].1, 2, 1, &corpus[1].2)
            .expect("ins 2");

        // A composite with the durable primary and NO replicas yet; grow one at runtime.
        let rs = ReplicatedShard::new(Box::new(primary), vec![]);
        rs.add_recovered_replica(
            &norm,
            &dict,
            EngineConfig::default(),
            &primary_dir,
            &replica_dir,
            8,
        )
        .expect("add replica");

        assert_eq!(rs.replica_handles().len(), 1, "one replica promoted");
        assert!(
            rs.replica_handles()[0].in_sync.load(Ordering::Acquire),
            "the promoted replica is in the in-sync set"
        );

        // A write AFTER promotion must fan out to the new replica (proof it is truly in-sync).
        rs.insert_extracted(&corpus[2].1, 3, 1, &corpus[2].2)
            .expect("post-promotion write");

        let replica = rs.replica_handles()[0].clone();
        for title in ["alpha bravo zulu", "charlie delta zulu", "golf hotel zulu"] {
            let (mut p, _) = rs.primary.percolate(title, true).expect("primary");
            let (mut r, _) = replica.shard.percolate(title, true).expect("replica");
            p.sort_unstable();
            r.sort_unstable();
            assert_eq!(
                p, r,
                "replica diverged from primary on {title:?} after promotion"
            );
        }
        let (probe, _) = replica
            .shard
            .percolate("golf hotel zulu", true)
            .expect("read");
        assert!(
            probe.contains(&3),
            "the post-promotion write must have fanned out to the in-sync replica: {probe:?}"
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
