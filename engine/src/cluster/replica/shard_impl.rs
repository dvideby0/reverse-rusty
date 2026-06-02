//! `impl Shard for ReplicatedShard` — the composite's Shard-trait surface: reads (primary +
//! in-sync-replica failover), primary-authoritative writes with replica fan-out, durability
//! delegation to the primary, retention-lease plumbing, and runtime replica growth.

use std::path::Path;
use std::sync::{Arc, PoisonError};

use crate::cluster::clog::{ClusterMutation, LogPos};
use crate::cluster::shard::{EventSink, Shard, ShardError};
use crate::compile::Extracted;
use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::events::{DurabilityOp, EngineEvent};
use crate::normalize::Normalizer;
use crate::segment::{IngestReport, MatchStats};

use super::{catch_up_replica, peer_recover, ReplicatedShard};

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

    fn live_sources(&self) -> Result<Vec<(u64, String)>, ShardError> {
        // Replicas are set-equal copies of the primary, so any in-sync copy yields
        // the same source set — read with the same in-sync failover as `num_queries`.
        self.read(|s| s.live_sources())
    }

    fn is_local(&self) -> bool {
        true
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
