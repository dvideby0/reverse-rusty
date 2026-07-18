//! `impl Shard for ReplicatedShard` — the composite's Shard-trait surface: reads (primary +
//! in-sync-replica failover), primary-authoritative writes with replica fan-out, durability
//! delegation to the primary, retention-lease plumbing, and runtime replica growth.

use std::path::Path;
use std::sync::{Arc, PoisonError};

use crate::cluster::clog::{ClusterMutation, LogPos};
use crate::cluster::shard::{EventSink, FetchedMatch, Shard, ShardError, ShardRankedMatch};
use crate::compile::Extracted;
use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::events::{DurabilityOp, EngineEvent};
use crate::exact::TagPredicate;
use crate::normalize::Normalizer;
use crate::segment::{IngestReport, MatchStats, PlacedQuery};
use crate::tagdict::TagDict;

use super::{catch_up_replica, peer_recover, ReplicatedShard};

impl Shard for ReplicatedShard {
    // ---- reads (primary, with in-sync failover) ----
    fn percolate_filtered(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        self.read(|s| s.percolate_filtered(title, include_broad, pred))
    }

    fn percolate_filtered_owned(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        context: &crate::ownership::OwnershipContext,
        current_position: u32,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        self.read(|shard| {
            shard.percolate_filtered_owned(title, include_broad, pred, context, current_position)
        })
    }

    fn percolate_filtered_ranked(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        spec: &crate::rank::CompiledRankSpec,
    ) -> Result<(Vec<(u64, i64)>, MatchStats), ShardError> {
        // Set-equal copies carry identical tags (identical op streams), so any in-sync
        // copy yields the same scores — the same failover as `percolate_filtered`.
        self.read(|s| s.percolate_filtered_ranked(title, include_broad, pred, spec))
    }

    fn percolate_filtered_ranked_owned(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        spec: &crate::rank::CompiledRankSpec,
        context: &crate::ownership::OwnershipContext,
        current_position: u32,
    ) -> Result<(Vec<(u64, i64)>, MatchStats), ShardError> {
        self.read(|shard| {
            shard.percolate_filtered_ranked_owned(
                title,
                include_broad,
                pred,
                spec,
                context,
                current_position,
            )
        })
    }

    fn percolate_top_k_owned(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        program: &crate::rank::CompiledRankProgram,
        options: crate::result::TopKOptions,
        context: &crate::ownership::OwnershipContext,
        current_position: u32,
        deadline: Option<std::time::Instant>,
    ) -> Result<ShardRankedMatch, ShardError> {
        self.read(|shard| {
            shard.percolate_top_k_owned(
                title,
                include_broad,
                pred,
                program,
                options,
                context,
                current_position,
                deadline,
            )
        })
    }

    // ---- ADR-113 PIT: PRIMARY-ONLY, deliberately no read failover ----
    // A PIT is a per-engine pin: the primary's pinned snapshot does not exist
    // on a replica, so failing a pit read over would silently serve a
    // different generation into a cursor stream. A failover mid-cursor
    // surfaces as PitNotFound → the coordinator's 409 stale-cursor.
    fn open_pit(&self, pit: u64) -> Result<(), ShardError> {
        self.primary.open_pit(pit)
    }

    fn close_pit(&self, pit: u64) -> Result<(), ShardError> {
        self.primary.close_pit(pit)
    }

    fn percolate_top_k_owned_pit(
        &self,
        pit: u64,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        program: &crate::rank::CompiledRankProgram,
        options: crate::result::TopKOptions,
        context: &crate::ownership::OwnershipContext,
        current_position: u32,
        deadline: Option<std::time::Instant>,
    ) -> Result<ShardRankedMatch, ShardError> {
        self.primary.percolate_top_k_owned_pit(
            pit,
            title,
            include_broad,
            pred,
            program,
            options,
            context,
            current_position,
            deadline,
        )
    }

    fn percolate_top_k_batch_owned(
        &self,
        titles: &[crate::cluster::shard::BatchTitleRequest<'_>],
        include_broad: bool,
        pred: &TagPredicate,
        program: &crate::rank::CompiledRankProgram,
        options: crate::result::TopKOptions,
        current_position: u32,
        deadline: Option<std::time::Instant>,
    ) -> Result<crate::cluster::shard::ShardBatchRankedMatch, ShardError> {
        self.read(|shard| {
            shard.percolate_top_k_batch_owned(
                titles,
                include_broad,
                pred,
                program,
                options,
                current_position,
                deadline,
            )
        })
    }

    fn fetch_matches(
        &self,
        logical_ids: &[u64],
        max_source_bytes: usize,
        deadline: Option<std::time::Instant>,
    ) -> Result<Vec<FetchedMatch>, ShardError> {
        self.read(|shard| shard.fetch_matches(logical_ids, max_source_bytes, deadline))
    }

    fn num_queries(&self) -> Result<usize, ShardError> {
        self.read(|s| s.num_queries())
    }

    fn class_counts(&self) -> Result<[u64; 5], ShardError> {
        self.read(|s| s.class_counts())
    }

    fn validate_ownership(
        &self,
        position: u32,
        generation: crate::ownership::PlacementGeneration,
        num_shards: u32,
    ) -> Result<(), ShardError> {
        self.primary
            .validate_ownership(position, generation, num_shards)?;
        let replicas = self.replicas.lock().unwrap_or_else(PoisonError::into_inner);
        for replica in replicas.iter() {
            replica
                .shard
                .validate_ownership(position, generation, num_shards)?;
        }
        Ok(())
    }

    fn live_sources(&self) -> Result<Vec<(u64, String)>, ShardError> {
        // Replicas are set-equal copies of the primary, so any in-sync copy yields
        // the same source set — read with the same in-sync failover as `num_queries`.
        self.read(|s| s.live_sources())
    }

    fn live_logical_ids(&self) -> Result<Vec<u64>, ShardError> {
        // Replicas are set-equal; avoid copying source text while rebuilding the
        // coordinator's compact admission directory on durable open.
        self.read(|s| s.live_logical_ids())
    }

    fn live_sources_tagged(
        &self,
    ) -> Result<Vec<crate::cluster::shard::LiveTaggedQuery>, ShardError> {
        // Same set-equal-copies argument as `live_sources`: identical op streams
        // carry identical tags AND versions, so any in-sync copy yields the same set.
        self.read(|s| s.live_sources_tagged())
    }

    fn is_local(&self) -> bool {
        true
    }

    #[cfg(feature = "distributed")]
    fn live_endpoints(&self) -> Vec<String> {
        // The GC keep-set (ADR-096): EVERY member's endpoints — the primary's plus each replica's,
        // in-sync or not (conservative: an out-of-sync replica still holds data a re-recovery may
        // read; keeping it costs disk, dropping it live would be destructive). Snapshot the slot
        // `Arc`s under the lock, then query lock-free (the composite's usual discipline).
        let mut eps = self.primary.live_endpoints();
        let slots: Vec<_> = {
            let replicas = self
                .replicas
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            replicas.clone()
        };
        for slot in slots {
            eps.extend(slot.shard.live_endpoints());
        }
        eps.sort_unstable();
        eps.dedup();
        eps
    }

    fn source_of(&self, logical: u64) -> Result<Option<String>, ShardError> {
        // Set-equal copies ⇒ any in-sync copy answers; same failover as the other reads.
        self.read(|s| s.source_of(logical))
    }

    // ---- writes (primary-authoritative, fan out to replicas) ----
    fn ingest_extracted(&self, items: &[PlacedQuery]) -> Result<IngestReport, ShardError> {
        let _g = self.lock();
        let report = self.primary.ingest_extracted(items)?;
        self.fan_to_replicas(|s| s.ingest_extracted(items).map(|_| ()));
        Ok(report)
    }

    fn insert_extracted_with_tags(
        &self,
        ex: &Extracted,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
    ) -> Result<Option<u32>, ShardError> {
        let _g = self.lock();
        // Every copy shares the one frozen tag dict (ADR-055), so each re-resolves these raw tags
        // to the same `TagId`s — replicas stay set-equal with the primary by construction.
        let out = self
            .primary
            .insert_extracted_with_tags(ex, logical, version, text, tags)?;
        self.fan_to_replicas(|s| {
            s.insert_extracted_with_tags(ex, logical, version, text, tags)
                .map(|_| ())
        });
        Ok(out)
    }

    fn insert_extracted_with_placement(
        &self,
        ex: &Extracted,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
        placement: &crate::ownership::QueryPlacement,
    ) -> Result<Option<u32>, ShardError> {
        let _g = self.lock();
        let out = self
            .primary
            .insert_extracted_with_placement(ex, logical, version, text, tags, placement)?;
        self.fan_to_replicas(|shard| {
            shard
                .insert_extracted_with_placement(ex, logical, version, text, tags, placement)
                .map(|_| ())
        });
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
    #[allow(clippy::too_many_arguments)]
    fn add_recovered_replica(
        &self,
        norm: &Arc<Normalizer>,
        dict: &Arc<Dict>,
        tag_dict: &Arc<TagDict>,
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
                tag_dict,
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
