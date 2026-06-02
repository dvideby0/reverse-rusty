//! `impl ClusterEngine` — gRPC remote-cluster construction + cross-node operations
//! (`distributed` feature): remote / replicated assembly, peer recovery, live handoff.

use std::sync::Arc;

use crate::cluster::clog::LogPos;
use crate::cluster::handoff::{wrap_handoff, HandoffShard};
use crate::cluster::ring::HashRing;
use crate::cluster::shard::{Shard, ShardError};
use crate::dict::Dict;
use crate::events::{DurabilityOp, EngineEvent};
use crate::normalize::Normalizer;

use super::{ClusterConfig, ClusterDurable, ClusterEngine, ShardGroup};

impl ClusterEngine {
    /// Install per-position handoff handles built by a gRPC builder (ADR-043). Consumes + returns
    /// `self` so a builder can chain it after [`Self::from_parts`]; `handoffs` must be index-aligned
    /// with `shards` (one [`HandoffShard`] per position, sharing the boxed copy already in `shards`,
    /// both produced by [`wrap_handoff`]). The in-process/default path never calls this, so its
    /// `handoffs` stays empty and the cluster is byte-identical to pre-6a.
    fn with_handoffs(mut self, handoffs: Vec<Arc<HandoffShard>>) -> Self {
        self.handoffs = handoffs;
        self
    }

    /// The fence generation each shard position's backing is currently serving under (ADR-043) —
    /// introspection for the handoff state, index-aligned with positions. Empty on the
    /// in-process/default path (no position is handoff-wrapped). Stage 6b's `execute_handoff`
    /// advances a position's generation when it re-points it to a new owner; this is how a
    /// test/operator observes the live map.
    pub fn handoff_generations(&self) -> Vec<u64> {
        self.handoffs.iter().map(|h| h.generation()).collect()
    }

    /// Assemble a cluster whose K shards are REMOTE (gRPC) — one per `endpoints[i]`,
    /// connected on the given tokio `handle`. Placement + routing run here on the
    /// coordinator, while each server re-compiles DSL read-only against its copy of the
    /// frozen dict, so the ids line up only when the dicts match. To guarantee that, the
    /// coordinator **ships** its dict to each server at connect (ADR-034): an empty/pending
    /// server adopts it, a server already holding it no-ops, and a server holding *data*
    /// under a divergent dict refuses — surfaced as [`ShardError::DictMismatch`], so a
    /// divergent feature space fails loud instead of dropping matches silently (the ADR-029
    /// handshake, now backed by shipping). A data node therefore need not rebuild a
    /// byte-identical dict from the corpus out-of-band; only `norm` must still match the
    /// servers' (`default_vocab()` today — normalizer shipping is a later step, ADR-034).
    /// `endpoints.len()` must equal `config.num_shards`; endpoint `i` serves shard `i`.
    /// Load the corpus afterwards with [`Self::ingest`].
    pub fn connect_remote(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        config: &ClusterConfig,
        endpoints: &[String],
        handle: &tokio::runtime::Handle,
    ) -> Result<Self, ShardError> {
        if endpoints.len() != config.num_shards {
            return Err(ShardError::Config(format!(
                "connect_remote needs exactly one endpoint per shard: got {} endpoints \
                 for {} shards",
                endpoints.len(),
                config.num_shards
            )));
        }
        if config.replication_factor > 1 {
            return Err(ShardError::Config(
                "connect_remote does not support replication_factor > 1; remote per-shard \
                 replication is clustering step 4b (ADR-036)"
                    .into(),
            ));
        }
        let ring = HashRing::new(config.num_shards, config.vnodes)?;
        // Cross-process shared-dict invariant: placement/routing ids line up only when every
        // server's frozen dict equals this coordinator's. SHIP it (ADR-034): serialize once,
        // then adopt per endpoint. An empty server adopts; a server already holding this dict
        // no-ops; a server holding data under a divergent dict refuses → DictMismatch (loud,
        // never a silent drop). Servers therefore needn't rebuild the dict from the corpus.
        let expected = dict.fingerprint();
        let dict_bytes = crate::storage::serialize_dict(&dict);
        let mut shards: Vec<Box<dyn Shard>> = Vec::with_capacity(endpoints.len());
        // Wrap each remote position in a `HandoffShard` so it can be re-pointed at a new owner at
        // runtime (ADR-043); the typed handles are installed via `with_handoffs` below.
        let mut handoffs: Vec<Arc<HandoffShard>> = Vec::with_capacity(endpoints.len());
        for ep in endpoints {
            let remote = crate::cluster::remote::RemoteShard::connect_and_adopt(
                ep.clone(),
                handle.clone(),
                dict_bytes.clone(),
                expected,
            )?;
            let (boxed, h) = wrap_handoff(Box::new(remote), 0);
            shards.push(boxed);
            handoffs.push(h);
        }
        // A remote cluster is non-durable at the coordinator in this increment (the
        // coordinator-level durable log is the in-process story; cross-node durability
        // is a later step). Use the in-memory log so behavior is unchanged.
        let durable =
            ClusterDurable::in_memory(config.num_shards as u32, config.vnodes, dict.fingerprint());
        Ok(Self::from_parts(
            norm,
            dict,
            ring,
            shards,
            config.include_broad,
            config.replication_factor,
            config.per_shard.clone(),
            durable,
        )?
        .with_handoffs(handoffs))
    }

    /// Assemble a cluster whose K shard POSITIONS are each a [`ReplicatedShard`](crate::cluster::replica::ReplicatedShard)
    /// over RF gRPC [`RemoteShard`]s (a primary + replicas), one [`ShardGroup`] per position. Ships +
    /// adopts the frozen dict on EVERY endpoint (ADR-034), then wraps position `i`'s RemoteShards
    /// into one composite boxed as the `i`-th shard — so the coordinator's placement / routing /
    /// merge is identical to a non-replicated remote cluster, while reads fail over to a replica and
    /// writes fan out (ADR-035). `groups.len()` must equal `config.num_shards`; a group with no
    /// replicas degenerates to a bare `RemoteShard` (identical to [`Self::connect_remote`]). Load the
    /// corpus afterwards with [`Self::ingest`].
    pub fn connect_replicated(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        config: &ClusterConfig,
        groups: &[ShardGroup],
        handle: &tokio::runtime::Handle,
    ) -> Result<Self, ShardError> {
        if groups.len() != config.num_shards {
            return Err(ShardError::Config(format!(
                "connect_replicated needs one ShardGroup per shard: got {} for {} shards",
                groups.len(),
                config.num_shards
            )));
        }
        let ring = HashRing::new(config.num_shards, config.vnodes)?;
        let expected = dict.fingerprint();
        let dict_bytes = crate::storage::serialize_dict(&dict);
        let mut shards: Vec<Box<dyn Shard>> = Vec::with_capacity(groups.len());
        // Each position (a bare remote or a ReplicatedShard group) is wrapped in a `HandoffShard`
        // so the whole group can be re-pointed at a new owner at runtime (ADR-043).
        let mut handoffs: Vec<Arc<HandoffShard>> = Vec::with_capacity(groups.len());
        for g in groups {
            let primary = crate::cluster::remote::RemoteShard::connect_and_adopt(
                g.primary.clone(),
                handle.clone(),
                dict_bytes.clone(),
                expected,
            )?;
            let mut replicas: Vec<Box<dyn Shard>> = Vec::with_capacity(g.replicas.len());
            for ep in &g.replicas {
                let r = crate::cluster::remote::RemoteShard::connect_and_adopt(
                    ep.clone(),
                    handle.clone(),
                    dict_bytes.clone(),
                    expected,
                )?;
                replicas.push(Box::new(r) as Box<dyn Shard>);
            }
            let shard: Box<dyn Shard> = if replicas.is_empty() {
                Box::new(primary)
            } else {
                Box::new(crate::cluster::replica::ReplicatedShard::new(
                    Box::new(primary) as Box<dyn Shard>,
                    replicas,
                ))
            };
            let (boxed, h) = wrap_handoff(shard, 0);
            shards.push(boxed);
            handoffs.push(h);
        }
        let durable =
            ClusterDurable::in_memory(config.num_shards as u32, config.vnodes, dict.fingerprint());
        Ok(Self::from_parts(
            norm,
            dict,
            ring,
            shards,
            config.include_broad,
            config.replication_factor,
            config.per_shard.clone(),
            durable,
        )?
        .with_handoffs(handoffs))
    }

    /// Cross-node peer recovery (ADR-036 + ADR-039 + ADR-040): bring a fresh, durable, **pending**
    /// node up as a copy of a shard by streaming a peer's segments AND replaying its translog tail —
    /// so writes to the source need **not** be quiesced for the copy window (ADR-036's gap, closed
    /// by the per-shard translog). The flow: pin the source's un-sealed tail with a **retention
    /// lease** (ADR-040) so the segment-copy seal — and any concurrent seal — cannot trim it away;
    /// ship the frozen dict to `target_endpoint` (adopt); drive its `RecoverFrom`, which pulls
    /// `source_endpoint`'s sealed segments at position `P`, attaches them, and reports `P`; then
    /// replay the source's translog tail (ops > `P`) into the target via the shared apply funnel,
    /// **looping** to drain residual writes until it stops advancing (the finalize — the window a
    /// final external quiesce would cover shrinks toward zero). Releases the lease on completion.
    /// Returns `(num_queries, high_water)`. Correctness never depends on the loop converging — the
    /// lease keeps the tail safe — only the residual size does.
    pub fn peer_recover_replica(
        &self,
        source_endpoint: &str,
        target_endpoint: &str,
        handle: &tokio::runtime::Handle,
    ) -> Result<(u64, u64), ShardError> {
        // Bound on the convergence loop (a safety cap, not a correctness requirement).
        const FINALIZE_PASSES: usize = 8;
        let expected = self.dict.fingerprint();
        // Pin the source's tail BEFORE the segment-copy seal trims it (ADR-040). Held across the
        // whole recovery; released below whether it converges or errors.
        let source = crate::cluster::remote::RemoteShard::connect(
            source_endpoint.to_string(),
            handle.clone(),
            expected,
        )?;
        let (lease, _pinned) = source.acquire_retention_lease()?;

        let recover = || -> Result<(u64, u64), ShardError> {
            let dict_bytes = crate::storage::serialize_dict(&self.dict);
            // Ship the dict so the fresh node attaches segments against the right feature space.
            let target = crate::cluster::remote::RemoteShard::connect_and_adopt(
                target_endpoint.to_string(),
                handle.clone(),
                dict_bytes,
                expected,
            )?;
            // Bulk copy: segments at snapshot position P (the source keeps serving + writing).
            let (_segments, _nq, p) = target.recover_from(source_endpoint, expected)?;
            // Tail replay + convergence: drain the source tail (> P) through the SAME apply funnel
            // as a live write (re-derived from DSL against the frozen dict), looping until it stops
            // advancing. Renew the lease each pass so the source may GC the consumed prefix.
            let mut hwm = LogPos(p);
            for _ in 0..FINALIZE_PASSES {
                let next = crate::cluster::replica::catch_up_replica(
                    &target, &source, &self.norm, &self.dict, hwm,
                )?;
                source.renew_retention_lease(lease, next)?;
                if next == hwm {
                    break; // tail drained at this instant — converged
                }
                hwm = next;
            }
            let num_queries = target.num_queries()? as u64;
            Ok((num_queries, hwm.0))
        };
        let out = recover();
        // Always release the lease (a held one would pin the source's translog forever). A release
        // failure on an otherwise-successful recovery is surfaced as an event, not conflated with
        // the recovery outcome (the replica is good; the source may just retain extra translog).
        if let Err(e) = source.release_retention_lease(lease) {
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::ReplicaDesync,
                detail: "releasing the peer-recovery retention lease on the source failed; the \
                         source may retain extra translog until its next successful seal"
                    .into(),
                error: e.to_string(),
            });
        }
        out
    }

    /// Re-run the translog catch-up (ADR-039): replay `source`'s tail (ops strictly after
    /// `after`) into the already-recovered `target`, returning the new high-water source position.
    /// The brief finalize after [`Self::peer_recover_replica`]'s bulk copy — under sustained
    /// writes, recovery converges by repeating this until the high-water stops advancing (the
    /// window where a final quiesce would shrink to the residual delta).
    pub fn catch_up_recovered_replica(
        &self,
        source_endpoint: &str,
        target_endpoint: &str,
        after: u64,
        handle: &tokio::runtime::Handle,
    ) -> Result<u64, ShardError> {
        let expected = self.dict.fingerprint();
        let source = crate::cluster::remote::RemoteShard::connect(
            source_endpoint.to_string(),
            handle.clone(),
            expected,
        )?;
        let target = crate::cluster::remote::RemoteShard::connect(
            target_endpoint.to_string(),
            handle.clone(),
            expected,
        )?;
        let hwm = crate::cluster::replica::catch_up_replica(
            &target,
            &source,
            &self.norm,
            &self.dict,
            LogPos(after),
        )?;
        Ok(hwm.0)
    }

    /// Live data-moving handoff (ADR-044, clustering step 6b): move shard `position` from its
    /// current owner (`source_endpoint`) to a new owner (`target_endpoint`) WITHOUT dropping a match
    /// and WITHOUT pausing reads. The byte mover is peer recovery (ADR-036/039); this adds the
    /// **serve-then-drop routing flip** (the 6a [`HandoffShard`]) + a **write fence** on the old
    /// owner. Under one retention lease held on the source for the whole move, the flow is:
    /// peer-recover the target from the source (bulk segments at `P` + drain the translog tail — NO
    /// quiesce: the source keeps serving reads + accepting writes throughout); **fence** the source
    /// (its data-mutating writes now return an error, so a brief write-quiesce for `position` begins,
    /// while reads + the recovery RPCs stay served); **drain to convergence** (the fenced source's
    /// tail is finite + frozen, so looping the catch-up until the high-water stops advancing captures
    /// every op it ever accepted — closing the TOCTOU a single final catch-up would leave); then
    /// **flip** — atomically re-point `position`'s backing at the target (the old source backing is
    /// dropped from routing: in-flight reads complete against it, new reads + writes go to the target,
    /// ending the quiesce). Returns the new fence/handoff generation. Requires a handoff-capable
    /// cluster (built via [`Self::connect_remote`]/[`Self::connect_replicated`]); errors fail-closed —
    /// a write briefly rejected in the fence→flip window is the caller's to retry (it never silently
    /// vanishes), and a source that fails to converge aborts the flip (leaving the source fenced)
    /// rather than dropping a write. "Drop the old owner" = drop it from ROUTING, not teardown (its
    /// server keeps running; tearing it down is a separate ops step).
    pub fn execute_handoff(
        &self,
        position: usize,
        source_endpoint: &str,
        target_endpoint: &str,
        handle: &tokio::runtime::Handle,
    ) -> Result<u64, ShardError> {
        // Safety cap on the pre-fence drain loop (best-effort, while writes still flow); correctness
        // rests on the post-fence drain converging, not on this.
        const DRAIN_PASSES: usize = 8;
        // Generous cap on the post-fence drain. The fenced source has a finite, frozen tail, so this
        // converges in O(in-flight writes) passes; the cap only bounds a misbehaving source.
        const FINAL_DRAIN_CAP: usize = 1024;
        let handoff = self
            .handoffs
            .get(position)
            .ok_or_else(|| {
                ShardError::Config(format!(
                    "execute_handoff: shard position {position} is not handoff-capable (the cluster \
                     was not built via connect_remote/connect_replicated)"
                ))
            })?
            .clone();
        let new_gen = handoff.generation() + 1;
        let expected = self.dict.fingerprint();

        // Connect to the source and pin its un-sealed tail for the WHOLE move, so the segment-copy
        // seal — or any concurrent seal — cannot trim away the tail we still need (ADR-040).
        let source = crate::cluster::remote::RemoteShard::connect(
            source_endpoint.to_string(),
            handle.clone(),
            expected,
        )?;
        let (lease, _pinned) = source.acquire_retention_lease()?;

        let do_move = || -> Result<u64, ShardError> {
            // Ship the dict + drive the target to pull the source's segments at snapshot `P` (the
            // source keeps serving + writing — no quiesce).
            let dict_bytes = crate::storage::serialize_dict(&self.dict);
            let target = crate::cluster::remote::RemoteShard::connect_and_adopt(
                target_endpoint.to_string(),
                handle.clone(),
                dict_bytes,
                expected,
            )?;
            let (_segments, _nq, p) = target.recover_from(source_endpoint, expected)?;
            // Drain the tail (writes that landed during the copy), renewing the lease each pass.
            let mut hwm = LogPos(p);
            for _ in 0..DRAIN_PASSES {
                let next = crate::cluster::replica::catch_up_replica(
                    &target, &source, &self.norm, &self.dict, hwm,
                )?;
                source.renew_retention_lease(lease, next)?;
                if next == hwm {
                    break;
                }
                hwm = next;
            }
            // FENCE the source: it stops accepting writes (the write-quiesce for `position` begins).
            // Reads + FetchTranslog stay served, so the catch-up below still works.
            source.fence(new_gen)?;
            // Final drain to CONVERGENCE. A write that passed the source's fence check just before
            // the fence took effect can still append AFTER a single catch-up reads the tail (a
            // TOCTOU), so one pass is not enough. But the fenced source accepts no new writes, so its
            // tail is now finite and frozen: loop the catch-up until the high-water stops advancing.
            // Convergence (NOT a fixed pass count) is what guarantees the target holds every op the
            // source ever accepted — the flip below therefore cannot drop a write. The fence
            // guarantees this terminates; the cap only guards a misbehaving (still-accepting) source,
            // in which case we abort fail-closed rather than flip onto a not-yet-converged target.
            let mut converged = false;
            for _ in 0..FINAL_DRAIN_CAP {
                let next = crate::cluster::replica::catch_up_replica(
                    &target, &source, &self.norm, &self.dict, hwm,
                )?;
                source.renew_retention_lease(lease, next)?;
                if next == hwm {
                    converged = true;
                    break;
                }
                hwm = next;
            }
            if !converged {
                return Err(ShardError::Remote(format!(
                    "execute_handoff: fenced source {source_endpoint} did not converge (tail still \
                     advancing past {}) within {FINAL_DRAIN_CAP} passes; aborting the flip to avoid \
                     dropping a write (the source remains fenced)",
                    hwm.0
                )));
            }
            // FLIP: re-point `position` at the target (reuse the recovery `target` as the new
            // backing). The old source backing is dropped from routing — serve-then-drop — and
            // writes to `position` now reach the target, ending the quiesce.
            handoff.swap_backing(Box::new(target), new_gen);
            Ok(new_gen)
        };
        let out = do_move();
        // Always release the lease (the source keeps serving reads regardless; it may now trim its
        // tail freely). A release failure on an otherwise-successful handoff is surfaced as an event,
        // not conflated with the outcome (the new owner is good; the source may just retain translog).
        if let Err(e) = source.release_retention_lease(lease) {
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::ReplicaDesync,
                detail:
                    "releasing the handoff retention lease on the source failed; the old owner \
                         may retain extra translog until its next successful seal"
                        .into(),
                error: e.to_string(),
            });
        }
        out
    }
}
