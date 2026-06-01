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

use super::shard::{EventSink, LocalShard, Shard, ShardError};

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
    replicas: Vec<ReplicaSlot>,
    /// Serializes write/seal so a write never interleaves with another op's replica fan-out
    /// (and so a future live peer-recovery can quiesce the position). Reads are lock-free.
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
            .map(|shard| ReplicaSlot {
                shard,
                in_sync: AtomicBool::new(true),
            })
            .collect();
        ReplicatedShard {
            primary,
            replicas,
            write_lock: Mutex::new(()),
            event_sink: Mutex::new(None),
            pending_events: Mutex::new(Vec::new()),
        }
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
        for slot in &self.replicas {
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
        for (i, slot) in self.replicas.iter().enumerate() {
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
    fn seal_for_checkpoint(&self) -> Result<(), ShardError> {
        let _g = self.lock();
        self.primary.seal_for_checkpoint()
    }

    fn segment_filenames(&self) -> Result<Vec<String>, ShardError> {
        self.primary.segment_filenames()
    }

    fn next_seg_id(&self) -> Result<u64, ShardError> {
        self.primary.next_seg_id()
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

/// Bring a fresh replica up to a DURABLE primary's state by copying its sealed segments —
/// the in-process analogue of Elasticsearch peer recovery ("stream segments from a peer").
/// Seals the primary (flush memtable + reseal base-segment tombstones) so its on-disk `.seg`
/// set reflects every applied delete, copies those files (and `sources.dat` if present —
/// display-only, tolerated absent) into a clean `replica_dir`, then attaches them via
/// [`LocalShard::open_segments`] (fail-loud on a missing/corrupt segment). The caller keeps
/// the position quiesced for this window (no concurrent writes). DURABLE-primary only: an
/// in-memory primary has no files to copy (its `segment_filenames` errors).
pub(crate) fn peer_recover(
    norm: Arc<Normalizer>,
    dict: Arc<Dict>,
    mut config: EngineConfig,
    primary: &dyn Shard,
    primary_dir: &Path,
    replica_dir: &Path,
) -> Result<LocalShard, ShardError> {
    // 1. Seal so the primary's on-disk segments are a consistent, tombstone-baked snapshot.
    primary.seal_for_checkpoint()?;
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
    LocalShard::open_segments(norm, dict, config, &files, next_seg_id)
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
        fn seal_for_checkpoint(&self) -> Result<(), ShardError> {
            Ok(())
        }
        fn segment_filenames(&self) -> Result<Vec<String>, ShardError> {
            Ok(Vec::new())
        }
        fn next_seg_id(&self) -> Result<u64, ShardError> {
            Ok(0)
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
        rs.replicas[0].in_sync.store(false, Ordering::Release);
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
            !rs.replicas[0].in_sync.load(Ordering::Acquire),
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
            rs.replicas[0].shard.num_queries().expect("replica count"),
            "primary and replica query counts diverged"
        );
        for title in [
            "alpha bravo zulu",
            "charlie delta zulu",
            "echo foxtrot zulu",
            "nothing here",
        ] {
            let (mut p, _) = rs.primary.percolate(title, true).expect("primary read");
            let (mut r, _) = rs.replicas[0]
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

        let replica = peer_recover(
            Arc::clone(&norm),
            Arc::clone(&dict),
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
}
