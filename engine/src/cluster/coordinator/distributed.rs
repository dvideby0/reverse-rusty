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
use crate::tagdict::TagDict;

use crate::cluster::security::ClientSecurity;
use crate::cluster::transport_metrics::TransportMetrics;

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

    /// Install the live-handoff drain caps from `ClusterConfig` (ADR-044/048). The in-process
    /// default leaves the `from_parts` defaults (8 / 1024); the gRPC builders chain this after
    /// `from_parts` so a handoff on a remote cluster honors the configured caps (and a test can
    /// force the abort path with `handoff_final_drain_cap = 0`).
    fn with_handoff_caps(mut self, drain_passes: usize, final_drain_cap: usize) -> Self {
        self.handoff_drain_passes = drain_passes;
        self.handoff_final_drain_cap = final_drain_cap;
        self
    }

    /// Retain the tokio runtime handle the cluster was connected on (ADR-048), so the autoscaler's
    /// `tick` can drive `execute_handoff` (which needs a handle for its sync→async `block_on`
    /// bridge). Only the gRPC builders call this; the in-process path leaves it `None`.
    fn with_handle(mut self, handle: tokio::runtime::Handle) -> Self {
        self.handle = Some(handle);
        self
    }

    /// Retain the mesh client security (ADR-071) the cluster was connected with, so every
    /// LATER internal connection (peer recovery, live handoff) rides the same TLS + token.
    /// Only the secure gRPC builders set it; the default stays empty (plaintext).
    fn with_client_security(mut self, security: ClientSecurity) -> Self {
        self.client_security = security;
        self
    }

    /// Install the SHARED transport-metrics collector (ADR-085) the gRPC builders also handed
    /// to each serving `RemoteShard`, so remote per-RPC stats aggregate on the engine (read via
    /// [`Self::transport_metrics`]). Replaces the empty one `from_parts` created. Only the gRPC
    /// builders call this; the in-process path keeps its all-zero collector.
    fn with_transport_metrics(mut self, metrics: Arc<TransportMetrics>) -> Self {
        self.transport_metrics = metrics;
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
        tag_dict: Arc<TagDict>,
        config: &ClusterConfig,
        endpoints: &[String],
        handle: &tokio::runtime::Handle,
    ) -> Result<Self, ShardError> {
        Self::connect_remote_with_security(
            norm,
            dict,
            tag_dict,
            config,
            endpoints,
            handle,
            ClientSecurity::default(),
        )
    }

    /// [`connect_remote`](Self::connect_remote) over a secured mesh (ADR-071): TLS per the
    /// client config + the cluster token on every RPC, including the connect-time
    /// `AdoptDict` handshake and every LATER internal connection (peer recovery, handoff —
    /// the config is retained). A default (empty) config is byte-identical.
    pub fn connect_remote_with_security(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: &ClusterConfig,
        endpoints: &[String],
        handle: &tokio::runtime::Handle,
        security: ClientSecurity,
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
        // Ship the frozen tag space alongside the dict (ADR-055), so each server resolves ingested
        // tags against the same space the coordinator's filter `TagId`s came from.
        let expected_tag = tag_dict.fingerprint();
        let tag_dict_bytes = crate::storage::serialize_tagdict(&tag_dict);
        let mut shards: Vec<Box<dyn Shard>> = Vec::with_capacity(endpoints.len());
        // Wrap each remote position in a `HandoffShard` so it can be re-pointed at a new owner at
        // runtime (ADR-043); the typed handles are installed via `with_handoffs` below.
        let mut handoffs: Vec<Arc<HandoffShard>> = Vec::with_capacity(endpoints.len());
        // ONE shared transport-metrics collector (ADR-085): every serving RemoteShard records
        // into it and the engine reads it via `transport_metrics()` (installed below).
        let metrics = Arc::new(TransportMetrics::new());
        // CO-LOCATION (ADR-093 Stage 2): several positions may share one endpoint (fewer pods than
        // shards, expressed by repeating an endpoint in the list). `endpoints[i]` is still position
        // `i`'s endpoint (the len check holds), but the FIRST position on each distinct endpoint
        // ships+adopts the node dict; every LATER position on that node reuses it via a lightweight
        // `AddShard` (no dict re-ship / re-deserialize). Routing stays position-indexed, so
        // co-location is transparent to it.
        // INITIAL is EXACT here, not a placeholder: a remote cluster cannot bump the
        // placement generation (`set_vocab`/`resize` refuse handoff-wrapped and
        // non-local shards, and every builder below wraps positions in HandoffShard),
        // so the generation a data node persisted at adopt time is always INITIAL. If
        // a future increment lifts that refusal it must thread the real generation
        // through these builders — the failure until then is a loud connect-time
        // `adopt_dict` refusal, never a silent mismatch.
        let mut adopted: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for (position, ep) in endpoints.iter().enumerate() {
            let shard_id = position as u32;
            let remote = if adopted.insert(ep.as_str()) {
                crate::cluster::remote::RemoteShard::connect_and_adopt_with_security(
                    ep,
                    handle.clone(),
                    dict_bytes.clone(),
                    expected,
                    tag_dict_bytes.clone(),
                    expected_tag,
                    shard_id,
                    crate::ownership::PlacementGeneration::INITIAL,
                    config.num_shards as u32,
                    &security,
                )?
            } else {
                crate::cluster::remote::RemoteShard::connect_and_add_shard_with_security(
                    ep,
                    handle.clone(),
                    expected,
                    expected_tag,
                    shard_id,
                    crate::ownership::PlacementGeneration::INITIAL,
                    config.num_shards as u32,
                    &security,
                )?
            }
            .with_metrics(Arc::clone(&metrics));
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
            tag_dict,
            ring,
            shards,
            config.include_broad,
            config.replication_factor,
            config.per_shard.clone(),
            durable,
        )?
        .with_handoffs(handoffs)
        .with_handoff_caps(config.handoff_drain_passes, config.handoff_final_drain_cap)
        .with_handle(handle.clone())
        .with_client_security(security)
        .with_transport_metrics(metrics))
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
        tag_dict: Arc<TagDict>,
        config: &ClusterConfig,
        groups: &[ShardGroup],
        handle: &tokio::runtime::Handle,
    ) -> Result<Self, ShardError> {
        Self::connect_replicated_with_security(
            norm,
            dict,
            tag_dict,
            config,
            groups,
            handle,
            ClientSecurity::default(),
        )
    }

    /// [`connect_replicated`](Self::connect_replicated) over a secured mesh (ADR-071) —
    /// the replicated analogue of
    /// [`connect_remote_with_security`](Self::connect_remote_with_security); the config is
    /// retained for later internal connections. A default (empty) config is byte-identical.
    pub fn connect_replicated_with_security(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        tag_dict: Arc<TagDict>,
        config: &ClusterConfig,
        groups: &[ShardGroup],
        handle: &tokio::runtime::Handle,
        security: ClientSecurity,
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
        // Ship the frozen tag space alongside the dict on every endpoint (ADR-055).
        let expected_tag = tag_dict.fingerprint();
        let tag_dict_bytes = crate::storage::serialize_tagdict(&tag_dict);
        let mut shards: Vec<Box<dyn Shard>> = Vec::with_capacity(groups.len());
        // Each position (a bare remote or a ReplicatedShard group) is wrapped in a `HandoffShard`
        // so the whole group can be re-pointed at a new owner at runtime (ADR-043).
        let mut handoffs: Vec<Arc<HandoffShard>> = Vec::with_capacity(groups.len());
        // ONE shared transport-metrics collector (ADR-085); see `connect_remote_with_security`.
        let metrics = Arc::new(TransportMetrics::new());
        // CO-LOCATION (ADR-093 Stage 3): a primary and/or replicas of different positions may share
        // one endpoint (fewer pods than shards × RF). The FIRST connection to each distinct endpoint
        // ships+adopts the node dict; every LATER slot on that node reuses it via a lightweight
        // `AddShard` (no dict re-ship / re-deserialize). This set spans BOTH primaries and replicas
        // across all groups, so a node hosting e.g. pos-0's primary and pos-1's replica adopts once
        // and gains its second slot via `AddShard` (which keys on the node dict, not the shard-id).
        let mut adopted: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for (position, g) in groups.iter().enumerate() {
            // A replica hosts the SAME global position (shard-id) as its primary (ADR-093).
            let shard_id = position as u32;
            let primary = if adopted.insert(g.primary.as_str()) {
                crate::cluster::remote::RemoteShard::connect_and_adopt_with_security(
                    &g.primary,
                    handle.clone(),
                    dict_bytes.clone(),
                    expected,
                    tag_dict_bytes.clone(),
                    expected_tag,
                    shard_id,
                    crate::ownership::PlacementGeneration::INITIAL,
                    config.num_shards as u32,
                    &security,
                )?
            } else {
                crate::cluster::remote::RemoteShard::connect_and_add_shard_with_security(
                    &g.primary,
                    handle.clone(),
                    expected,
                    expected_tag,
                    shard_id,
                    crate::ownership::PlacementGeneration::INITIAL,
                    config.num_shards as u32,
                    &security,
                )?
            }
            .with_metrics(Arc::clone(&metrics));
            let mut replicas: Vec<Box<dyn Shard>> = Vec::with_capacity(g.replicas.len());
            for ep in &g.replicas {
                let r = if adopted.insert(ep.as_str()) {
                    crate::cluster::remote::RemoteShard::connect_and_adopt_with_security(
                        ep,
                        handle.clone(),
                        dict_bytes.clone(),
                        expected,
                        tag_dict_bytes.clone(),
                        expected_tag,
                        shard_id,
                        crate::ownership::PlacementGeneration::INITIAL,
                        config.num_shards as u32,
                        &security,
                    )?
                } else {
                    crate::cluster::remote::RemoteShard::connect_and_add_shard_with_security(
                        ep,
                        handle.clone(),
                        expected,
                        expected_tag,
                        shard_id,
                        crate::ownership::PlacementGeneration::INITIAL,
                        config.num_shards as u32,
                        &security,
                    )?
                }
                .with_metrics(Arc::clone(&metrics));
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
            tag_dict,
            ring,
            shards,
            config.include_broad,
            config.replication_factor,
            config.per_shard.clone(),
            durable,
        )?
        .with_handoffs(handoffs)
        .with_handoff_caps(config.handoff_drain_passes, config.handoff_final_drain_cap)
        .with_handle(handle.clone())
        .with_client_security(security)
        .with_transport_metrics(metrics))
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
        shard_id: u32,
        source_endpoint: &str,
        target_endpoint: &str,
        handle: &tokio::runtime::Handle,
    ) -> Result<(u64, u64), ShardError> {
        // Bound on the convergence loop (a safety cap, not a correctness requirement).
        const FINALIZE_PASSES: usize = 8;
        let expected = self.dict.fingerprint();
        let expected_tag = self.tag_dict.fingerprint();
        // Pin the source's tail BEFORE the segment-copy seal trims it (ADR-040). Held across the
        // whole recovery; released below whether it converges or errors.
        // Recover the source's slot `shard_id` into the target's slot `shard_id` (ADR-093): a
        // relocation/replication keeps the SAME global position (e.g. position 1's primary hosts slot 1).
        let source = crate::cluster::remote::RemoteShard::connect_with_security(
            source_endpoint,
            handle.clone(),
            expected,
            expected_tag,
            shard_id,
            &self.client_security,
        )?
        .with_metrics(Arc::clone(&self.transport_metrics));
        let (lease, _pinned) = source.acquire_retention_lease()?;

        let recover = || -> Result<(u64, u64), ShardError> {
            let dict_bytes = crate::storage::serialize_dict(&self.dict);
            // Ship the dict + frozen tag space so the fresh node attaches segments against the right
            // feature + tag space (ADR-055).
            let target = crate::cluster::remote::RemoteShard::connect_and_adopt_with_security(
                target_endpoint,
                handle.clone(),
                dict_bytes,
                expected,
                crate::storage::serialize_tagdict(&self.tag_dict),
                self.tag_dict.fingerprint(),
                shard_id,
                self.placement_generation(),
                self.num_shards() as u32,
                &self.client_security,
            )?
            .with_metrics(Arc::clone(&self.transport_metrics));
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
        shard_id: u32,
        source_endpoint: &str,
        target_endpoint: &str,
        after: u64,
        handle: &tokio::runtime::Handle,
    ) -> Result<u64, ShardError> {
        let expected = self.dict.fingerprint();
        let expected_tag = self.tag_dict.fingerprint();
        // Catch up the target's slot `shard_id` from the source's same slot (ADR-093).
        let source = crate::cluster::remote::RemoteShard::connect_with_security(
            source_endpoint,
            handle.clone(),
            expected,
            expected_tag,
            shard_id,
            &self.client_security,
        )?
        .with_metrics(Arc::clone(&self.transport_metrics));
        let target = crate::cluster::remote::RemoteShard::connect_with_security(
            target_endpoint,
            handle.clone(),
            expected,
            expected_tag,
            shard_id,
            &self.client_security,
        )?
        .with_metrics(Arc::clone(&self.transport_metrics));
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
    ///
    /// Reserves `{source, target}` in the busy-endpoint move ledger for the whole move (ADR-095), so
    /// a raw handoff — the REST `POST /_cluster/handoff` path — serializes against every concurrent
    /// data-moving reassign touching either node. (Before the ledger, a raw handoff took NO guard at
    /// all and could race a `reassign_and_move` of the same position — a latent hole ADR-095
    /// closes.) [`reassign_and_move`](Self::reassign_and_move) calls the unguarded `_inner` variant
    /// instead: its own ticket already covers both endpoints, and re-reserving here would
    /// self-deadlock.
    pub fn execute_handoff(
        &self,
        position: usize,
        source_endpoint: &str,
        target_endpoint: &str,
        handle: &tokio::runtime::Handle,
    ) -> Result<u64, ShardError> {
        let _ticket = self
            .move_ledger
            .reserve(&[source_endpoint, target_endpoint]);
        self.execute_handoff_inner(position, source_endpoint, target_endpoint, handle)
    }

    /// [`execute_handoff`](Self::execute_handoff) minus the ledger reservation — for callers already
    /// holding a [`MoveTicket`](super::reassign::MoveLedger) covering `{source, target}` (the
    /// data-moving reassign path). Never call this without such a ticket: two unguarded handoffs
    /// sharing a node would interleave their fence windows.
    pub(in crate::cluster::coordinator) fn execute_handoff_inner(
        &self,
        position: usize,
        source_endpoint: &str,
        target_endpoint: &str,
        handle: &tokio::runtime::Handle,
    ) -> Result<u64, ShardError> {
        // Drain caps (ADR-044/048), tunable via `ClusterConfig` and retained on the engine.
        // `drain_passes` bounds the pre-fence drain (best-effort, while writes still flow);
        // correctness rests on the post-fence drain CONVERGING, not on this. `final_drain_cap`
        // bounds the post-fence drain — the fenced source has a finite, frozen tail, so it
        // converges in O(in-flight writes) passes and the cap only bounds a misbehaving source
        // (past it the flip aborts and the source auto-unfences, ADR-048). A test sets the cap to
        // 0 to force the abort deterministically.
        let drain_passes = self.handoff_drain_passes;
        let final_drain_cap = self.handoff_final_drain_cap;
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
        let expected_tag = self.tag_dict.fingerprint();

        // Connect to the source and pin its un-sealed tail for the WHOLE move, so the segment-copy
        // seal — or any concurrent seal — cannot trim away the tail we still need (ADR-040).
        let source = crate::cluster::remote::RemoteShard::connect_with_security(
            source_endpoint,
            handle.clone(),
            expected,
            expected_tag,
            // Fence/recover/lease the RIGHT slot: this handoff moves shard `position` (ADR-093).
            position as u32,
            &self.client_security,
        )?
        .with_metrics(Arc::clone(&self.transport_metrics));
        let (lease, _pinned) = source.acquire_retention_lease()?;

        let do_move = || -> Result<u64, ShardError> {
            // Ship the dict + frozen tag space + drive the target to pull the source's segments at
            // snapshot `P` (the source keeps serving + writing — no quiesce).
            let dict_bytes = crate::storage::serialize_dict(&self.dict);
            let target = crate::cluster::remote::RemoteShard::connect_and_adopt_with_security(
                target_endpoint,
                handle.clone(),
                dict_bytes,
                expected,
                crate::storage::serialize_tagdict(&self.tag_dict),
                self.tag_dict.fingerprint(),
                position as u32,
                self.placement_generation(),
                self.num_shards() as u32,
                &self.client_security,
            )?
            .with_metrics(Arc::clone(&self.transport_metrics));
            let (_segments, _nq, p) = target.recover_from(source_endpoint, expected)?;
            // Drain the tail (writes that landed during the copy), renewing the lease each pass.
            let mut hwm = LogPos(p);
            for _ in 0..drain_passes {
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
            // The fence RPC carries a write-deadline (ADR-085): a lost/slow response can return
            // Err AFTER the server applied the fence. Attempt the CAS-safe unfence(new_gen) on
            // failure (it lifts a fence the server DID apply at new_gen, no-op otherwise) so a
            // failed handoff never strands the source rejecting writes.
            if let Err(e) = source.fence(new_gen) {
                if let Err(ue) = source.unfence(new_gen) {
                    self.emit(EngineEvent::DurabilityFailure {
                        op: DurabilityOp::ReplicaDesync,
                        detail: "fence failed during handoff and the CAS-safe unfence cleanup \
                                 also failed; if the server had applied the fence the source \
                                 remains fenced and needs manual recovery"
                            .into(),
                        error: ue.to_string(),
                    });
                }
                return Err(e);
            }
            // From here the source is write-quiesced. Any failure BEFORE the flip must LIFT the
            // fence (ADR-048) so the source resumes serving — otherwise an aborted handoff leaves it
            // permanently quiesced (a write-rejecting node needing a manual restart). The
            // drain-to-convergence and its cap live in this scope; the flip (the success path) is
            // outside it and deliberately keeps the old owner fenced/dropped (serve-then-drop).
            //
            // Final drain to CONVERGENCE. A write that passed the source's fence check just before
            // the fence took effect can still append AFTER a single catch-up reads the tail (a
            // TOCTOU), so one pass is not enough. But the fenced source accepts no new writes, so its
            // tail is now finite and frozen: loop the catch-up until the high-water stops advancing.
            // Convergence (NOT a fixed pass count) is what guarantees the target holds every op the
            // source ever accepted — the flip below therefore cannot drop a write. The fence
            // guarantees this terminates; the cap only guards a misbehaving (still-accepting) source,
            // in which case we abort fail-closed rather than flip onto a not-yet-converged target.
            let drained = (|| -> Result<(), ShardError> {
                let mut converged = false;
                for _ in 0..final_drain_cap {
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
                        "execute_handoff: fenced source {source_endpoint} did not converge (tail \
                         still advancing past {}) within {final_drain_cap} passes; aborting the \
                         flip to avoid dropping a write",
                        hwm.0
                    )));
                }
                Ok(())
            })();
            if let Err(e) = drained {
                // AUTO-UNFENCE (ADR-048): lift the fence we set so the source resumes accepting
                // writes instead of staying permanently quiesced. CAS-guarded server-side (only
                // this generation's fence is cleared), so it is safe even under a concurrent
                // handoff. If the unfence RPC ITSELF fails, the source is still fenced and needs
                // manual recovery — surface that as an event, but return the ORIGINAL abort error
                // (don't mask why the handoff failed).
                if let Err(ue) = source.unfence(new_gen) {
                    self.emit(EngineEvent::DurabilityFailure {
                        op: DurabilityOp::ReplicaDesync,
                        detail: "auto-unfence after an aborted handoff failed; the source remains \
                                 fenced at the handoff generation and needs manual recovery"
                            .into(),
                        error: ue.to_string(),
                    });
                }
                return Err(e);
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
