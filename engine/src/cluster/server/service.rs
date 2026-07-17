//! `impl ShardService for ShardServer` — the gRPC service surface: percolate / counts /
//! dict-fingerprint reads, the adopt-dict + write RPCs (ingest / insert / delete / flush /
//! fence), and the peer-recovery RPCs (FetchSegments / RecoverFrom / FetchTranslog /
//! RetentionLease) + their server-streaming helpers.
//!
//! This file holds the trait impl itself; the heavier per-RPC bodies live in focused
//! submodules so each concern is self-contained (the trait impl stays a thin delegator,
//! since Rust requires every method in ONE `impl` block):
//!   - [`dict_adopt`] — the `AdoptDict` body (dict + tag-space shipping, ADR-034/055)
//!   - [`recovery`]   — the peer-recovery RPCs (FetchSegments / RecoverFrom / FetchTranslog) + their server-streaming helpers (ADR-035/036/039)
//!   - [`leases`]     — translog retention leases + the live-handoff write fence (RetentionLease / Fence / Unfence, ADR-040/044/048)
//!   - [`gc`]         — the orphan-slot GC RPCs (ListShards / DropShard, ADR-096)

use std::pin::Pin;
use std::time::Instant;

use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::cluster::node_metrics::ShardRpc;
use crate::cluster::proto;
use crate::cluster::proto::shard_service_server::ShardService;
use crate::cluster::shard::Shard;
use crate::segment::PlacedQuery;

use super::{compile_item, ShardServer};

mod add_shard;
mod dict_adopt;
mod gc;
mod leases;
mod recovery;

#[tonic::async_trait]
impl ShardService for ShardServer {
    async fn percolate(
        &self,
        request: Request<proto::PercolateRequest>,
    ) -> Result<Response<proto::PercolateReply>, Status> {
        // Service-latency timing (ADR-100): one Instant pair at the handler boundary, success
        // paths only (error paths `?`-return before the observe) — the engine hot path is never
        // touched.
        let started = Instant::now();
        let req = request.into_inner();
        let ownership = proto::ownership_from_proto(req.ownership.clone())
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        self.validate_placement_config(ownership.generation(), ownership.num_shards())?;
        // Rebuild the tag filter from the already-resolved `TagId` groups (ADR-055); empty ⇒
        // unfiltered. The ids are authoritative-from-coordinator, so the server never re-resolves
        // strings — immune to any server-side tag-space skew on reads.
        let pred = proto::tag_predicate_from_proto(req.filter);
        let (slot, st) = self.loaded_slot(req.shard_id)?;
        // A `rank` spec (ADR-075) rides the same already-compiled-ids pattern: score this
        // shard's matched ids and echo `ranked = true` so the client can tell a scored reply
        // from an old server that silently ignored the field. Absent ⇒ the pre-rank wire.
        if let Some(rank) = req.rank {
            let spec = proto::rank_spec_from_proto(rank);
            let (scored, stats) = st
                .shard
                .percolate_filtered_ranked_owned(
                    &req.title,
                    req.include_broad,
                    &pred,
                    &spec,
                    &ownership,
                    req.shard_id,
                )
                .map_err(|e| Status::internal(e.to_string()))?;
            let (ids, scores) = scored.into_iter().unzip();
            slot.latency
                .observe(ShardRpc::PercolateRanked, started.elapsed());
            slot.broad.record(&stats);
            return Ok(Response::new(proto::PercolateReply {
                ids,
                stats: Some(proto::stats_from_engine(stats)),
                scores,
                ranked: true,
                ownership_applied: true,
            }));
        }
        let (ids, stats) = st
            .shard
            .percolate_filtered_owned(
                &req.title,
                req.include_broad,
                &pred,
                &ownership,
                req.shard_id,
            )
            .map_err(|e| Status::internal(e.to_string()))?;
        slot.latency.observe(ShardRpc::Percolate, started.elapsed());
        // Broad-lane cost accumulation (ADR-101): unconditional — include_broad=false stats carry
        // all-zero broad fields, and a fetch_add(0) is branch-free noise.
        slot.broad.record(&stats);
        Ok(Response::new(proto::PercolateReply {
            ids,
            stats: Some(proto::stats_from_engine(stats)),
            scores: Vec::new(),
            ranked: false,
            ownership_applied: true,
        }))
    }

    async fn num_queries(
        &self,
        request: Request<proto::ShardRef>,
    ) -> Result<Response<proto::CountReply>, Status> {
        let count = self
            .loaded_slot(request.into_inner().shard_id)?
            .1
            .shard
            .num_queries()
            .map_err(|e| Status::internal(e.to_string()))? as u64;
        Ok(Response::new(proto::CountReply { count }))
    }

    async fn class_counts(
        &self,
        request: Request<proto::ShardRef>,
    ) -> Result<Response<proto::ClassCountsReply>, Status> {
        let counts = self
            .loaded_slot(request.into_inner().shard_id)?
            .1
            .shard
            .class_counts()
            .map_err(|e| Status::internal(e.to_string()))?;
        // Wire contract (ADR-105): `counts` stays exactly [A, B, C, D] — a pre-ADR-105
        // coordinator hard-errors on any other length — and class H rides the additive
        // `hot` field (default-0 to older readers).
        Ok(Response::new(proto::ClassCountsReply {
            counts: counts[..4].to_vec(),
            hot: counts[4],
        }))
    }

    async fn dict_fingerprint(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::DictFingerprintReply>, Status> {
        // Node-level (ADR-093): the dict/tag-dict fingerprints are a node-wide content invariant, read
        // from the node-scope adopted space (not any slot). A truly pending node — no adopt yet — is
        // not-ready, matching the pre-ADR-093 `loaded()?` failing on a pending server.
        let space = self
            .node_dict
            .load_full()
            .ok_or_else(|| Status::failed_precondition("shard has not adopted a dict yet"))?;
        Ok(Response::new(proto::DictFingerprintReply {
            fingerprint: space.dict.fingerprint(),
            // ADR-077: the probe carries the tag-space identity too, so a bare
            // `connect` (no adopt) verifies BOTH dicts. A stale server omits this
            // (proto3 zero), which can never equal a real fingerprint — loud, not
            // silently unverified.
            tag_dict_fingerprint: space.tag_dict.fingerprint(),
            // ADR-080: this binary serves the replicate-to-all broad layout. A pre-ADR-080
            // server omits the field (proto3 false), so a replicate-all coordinator refuses it.
            broad_replicate_all: true,
            placement_generation: space.placement_generation.0,
            num_shards: space.num_shards,
        }))
    }

    /// Adopt a frozen dict shipped by the coordinator (ADR-034). The wire carries the
    /// serialized dict (`crate::storage::serialize_dict`) + the coordinator's fingerprint
    /// of it. Contract:
    /// - bad bytes / fingerprint disagreeing with the deserialized dict → `invalid_argument`
    ///   (a corrupt or version-skewed ship, never silently trusted);
    /// - **empty** shard (pending, or adopted-but-no-data) → adopt: build a fresh
    ///   `LocalShard` over the shipped dict;
    /// - already adopted the **same** dict → idempotent no-op;
    /// - **non-empty** shard whose dict **differs** → `failed_precondition`: refuse, because
    ///   re-basing already-loaded data onto a different feature space would silently corrupt
    ///   matches. The coordinator surfaces this as `ShardError::DictMismatch`.
    ///
    /// Single-coordinator / adopt-before-ingest is the intended use; concurrent adopts are
    /// not synchronized beyond the atomic state swap (last writer wins, both with the same
    /// dict in practice).
    async fn adopt_dict(
        &self,
        request: Request<proto::AdoptDictRequest>,
    ) -> Result<Response<proto::AdoptDictReply>, Status> {
        dict_adopt::adopt_dict(self, request)
    }

    /// Create a co-located slot on a node that has already adopted the dict (ADR-093 Stage 2) —
    /// reuses the node-scope frozen space by `Arc`, no dict re-ship.
    async fn add_shard(
        &self,
        request: Request<proto::AddShardRequest>,
    ) -> Result<Response<proto::AddShardReply>, Status> {
        add_shard::add_shard(self, request)
    }

    async fn ingest_extracted(
        &self,
        request: Request<proto::IngestRequest>,
    ) -> Result<Response<proto::IngestReply>, Status> {
        let started = Instant::now();
        let req = request.into_inner();
        let (slot, st) = self.loaded_slot(req.shard_id)?;
        slot.check_not_fenced()?;
        let items = req.items;
        let mut lc = String::new();
        let mut rejected_parse = 0u64;
        let mut extracted: Vec<PlacedQuery> = Vec::with_capacity(items.len());
        for it in items {
            let placement = proto::placement_from_proto(it.placement.clone())
                .map_err(|error| Status::failed_precondition(error.to_string()))?;
            self.validate_placement_config(placement.generation(), placement.num_shards())?;
            placement
                .validate_for_shard(req.shard_id, placement.generation(), placement.num_shards())
                .map_err(|error| Status::failed_precondition(error.to_string()))?;
            match compile_item(&self.norm, &st.dict, &it.dsl, &mut lc) {
                // Carry the raw tags forward; the shard's engine resolves them read-only against the
                // adopted frozen tag space (ADR-055).
                Some(ex) => extracted.push(PlacedQuery {
                    logical: it.logical_id,
                    ex,
                    dsl: it.dsl,
                    // Store the wire version verbatim — the coordinator's REST layer already
                    // defaulted an absent version to 1 before placing, so an explicit value
                    // (incl. 0) is caller-supplied and must round-trip identically to the
                    // in-process / single-node path. Clamping here was a deployment-dependent
                    // divergence: the coordinator logged N while the shard stored N.max(1).
                    version: it.version,
                    tags: proto::tags_from_proto(it.tags),
                    // The wire is dict-agnostic (raw tags only) — pre-resolved ids never arrive.
                    tag_ids: Vec::new(),
                    rank: crate::rank::RankValues::default(),
                    placement,
                }),
                None => rejected_parse += 1,
            }
        }
        let report = st.shard.ingest_local(&extracted);
        slot.latency.observe(ShardRpc::Ingest, started.elapsed());
        Ok(Response::new(proto::IngestReply {
            ingested: report.ingested as u64,
            rejected_parse: rejected_parse + report.rejected_parse as u64,
            rejected_class_d: report.rejected_class_d as u64,
        }))
    }

    async fn insert_extracted(
        &self,
        request: Request<proto::InsertRequest>,
    ) -> Result<Response<proto::InsertReply>, Status> {
        let req = request.into_inner();
        let (slot, st) = self.loaded_slot(req.shard_id)?;
        slot.check_not_fenced()?;
        let item = req
            .item
            .ok_or_else(|| Status::invalid_argument("InsertRequest.item is required"))?;
        let placement = proto::placement_from_proto(item.placement.clone())
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        self.validate_placement_config(placement.generation(), placement.num_shards())?;
        placement
            .validate_for_shard(req.shard_id, placement.generation(), placement.num_shards())
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        let mut lc = String::new();
        let Some(ex) = compile_item(&self.norm, &st.dict, &item.dsl, &mut lc) else {
            // The coordinator already parsed before placing, so this should not happen;
            // report "not inserted" rather than fabricate a memtable id.
            return Ok(Response::new(proto::InsertReply {
                present: false,
                local_id: 0,
            }));
        };
        let tags = proto::tags_from_proto(item.tags);
        // Store the wire version verbatim (see `ingest_extracted`): the coordinator already
        // defaulted an absent version to 1, so an explicit value (incl. 0) is caller-supplied
        // and must match the in-process / single-node store rather than be clamped to 1.
        let out = st
            .shard
            .insert_extracted_with_placement(
                &ex,
                item.logical_id,
                item.version,
                &item.dsl,
                &tags,
                &placement,
            )
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(proto::InsertReply {
            present: out.is_some(),
            local_id: out.unwrap_or(0),
        }))
    }

    async fn delete(
        &self,
        request: Request<proto::DeleteRequest>,
    ) -> Result<Response<proto::DeleteReply>, Status> {
        let req = request.into_inner();
        self.validate_placement_config(
            crate::ownership::PlacementGeneration(req.placement_generation),
            req.num_shards,
        )?;
        let (slot, st) = self.loaded_slot(req.shard_id)?;
        slot.check_not_fenced()?;
        let removed = st
            .shard
            .delete_by_logical_id(req.logical_id)
            .map_err(|e| Status::internal(e.to_string()))? as u64;
        Ok(Response::new(proto::DeleteReply { removed }))
    }

    async fn flush(
        &self,
        request: Request<proto::FlushRequest>,
    ) -> Result<Response<proto::FlushReply>, Status> {
        let req = request.into_inner();
        self.validate_placement_config(
            crate::ownership::PlacementGeneration(req.placement_generation),
            req.num_shards,
        )?;
        self.loaded_slot(req.shard_id)?
            .1
            .shard
            .flush()
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(proto::FlushReply {}))
    }

    // ---- peer recovery (ADR-035/036, clustering build-path step 4b) ----
    type FetchSegmentsStream =
        Pin<Box<dyn Stream<Item = Result<proto::FetchSegmentsChunk, Status>> + Send>>;

    /// Stream this (durable) shard's sealed segments to a recovering peer: seal a consistent
    /// snapshot, send the manifest frame (the complete file set + seg-id cursor), then a chunked
    /// run per `.seg` (and `sources.dat` if present). Refuses if the server is not durable or
    /// the requester's dict fingerprint diverges (never ships segments compiled against a
    /// different feature space).
    async fn fetch_segments(
        &self,
        request: Request<proto::FetchSegmentsRequest>,
    ) -> Result<Response<Self::FetchSegmentsStream>, Status> {
        recovery::fetch_segments(self, request)
    }

    /// Accept peer recovery — the recovering node pulls from a peer (the Elasticsearch model):
    /// connect to `source_endpoint`, drain its `FetchSegments`, write the files under our own
    /// data_dir (tmp+rename), attach them, and swap in the recovered shard. Refuses if not
    /// durable or the dict fingerprint diverges. Returns the snapshot's translog position `P`
    /// (ADR-039): the orchestrator then replays the source's translog tail (> P) into this node
    /// via `FetchTranslog`, so the source need NOT quiesce writes during the segment copy
    /// (closing ADR-036's gap). Segment attach here is at the snapshot only; the tail catch-up
    /// is the coordinator's `peer_recover_replica`.
    async fn recover_from(
        &self,
        request: Request<proto::RecoverFromRequest>,
    ) -> Result<Response<proto::RecoverFromReply>, Status> {
        recovery::recover_from(self, request).await
    }

    // ---- per-shard translog tail (ADR-039) ----
    type FetchTranslogStream =
        Pin<Box<dyn Stream<Item = Result<proto::TranslogEntry, Status>> + Send>>;

    /// Stream this shard's un-sealed translog tail — every logged mutation with position
    /// strictly after `after_seqno`, oldest-first. Read-only: it does NOT seal (unlike
    /// `FetchSegments`), so the source keeps serving + accepting writes while a recovering peer
    /// catches up (ADR-039 — the no-quiesce property). The tail is the small un-sealed delta, so
    /// it is read once and streamed from memory. Refuses a dict-fingerprint mismatch.
    async fn fetch_translog(
        &self,
        request: Request<proto::FetchTranslogRequest>,
    ) -> Result<Response<Self::FetchTranslogStream>, Status> {
        recovery::fetch_translog(self, request)
    }

    // ---- translog retention leases (ADR-040) ----
    /// Acquire / renew / release a translog retention lease on this shard (`op` 0 / 1 / 2), so a
    /// recovering peer can pin the un-sealed tail it still needs against a concurrent seal
    /// (ADR-040, closing a latent false negative in ADR-039's no-quiesce path). Refuses a
    /// dict-fingerprint mismatch. `acquire` returns the new lease id + the pinned position;
    /// `renew`/`release` return zeros.
    async fn retention_lease(
        &self,
        request: Request<proto::RetentionLeaseRequest>,
    ) -> Result<Response<proto::RetentionLeaseReply>, Status> {
        leases::retention_lease(self, request)
    }

    // ---- live handoff: write fence (ADR-044, clustering step 6b) ----
    /// Demote this node at `generation`: data-mutating writes (`insert`/`delete`/`ingest`) are
    /// rejected thereafter, while reads + the recovery RPCs stay served (serve-then-drop). Monotonic
    /// — a stale, lower-generation Fence never un-fences. Refuses a dict-fingerprint mismatch
    /// (consistent with the other guarded RPCs). The handoff orchestrator
    /// (`ClusterEngine::execute_handoff`) fences the old owner here, drains its tail to the new
    /// owner, then flips routing — so the fence holds a brief write-quiesce across the flip.
    async fn fence(
        &self,
        request: Request<proto::FenceRequest>,
    ) -> Result<Response<proto::FenceReply>, Status> {
        leases::fence(self, request)
    }

    // ---- live handoff: un-fence on abort (ADR-048) ----
    /// Lift a fence held at EXACTLY `generation` (a compare-and-swap from `generation` back to 0):
    /// data-mutating writes resume. The CAS preserves the `fence` monotonic-safety story — only the
    /// handoff that fenced at this generation can lift it, so a stale/duplicate Unfence, or a node
    /// since re-fenced at a higher generation by a newer handoff, is a safe no-op (the fence is
    /// untouched). Refuses a dict-fingerprint mismatch (consistent with the other guarded RPCs).
    /// Returns the fence generation after the call (0 ⇒ un-fenced). `execute_handoff` calls this
    /// when a handoff aborts after fencing, so the source is not left permanently write-quiesced.
    async fn unfence(
        &self,
        request: Request<proto::UnfenceRequest>,
    ) -> Result<Response<proto::UnfenceReply>, Status> {
        leases::unfence(self, request)
    }

    // ---- orphan-slot GC (ADR-096) ----
    /// The node's slot inventory (fence generation, live count, unexpired leases) + its
    /// dict/tag-dict fingerprints — what the coordinator's GC sweep classifies on. Read-only.
    async fn list_shards(
        &self,
        request: Request<proto::Empty>,
    ) -> Result<Response<proto::ListShardsReply>, Status> {
        gc::list_shards(self, request)
    }

    /// Drop one slot (remove from the map + reclaim `shard_<id>/`), guarded: fingerprints, the
    /// fence-armed CAS, and the retention-lease check — see the [`gc`] module docs. An absent
    /// slot replies `dropped = false` (idempotent).
    async fn drop_shard(
        &self,
        request: Request<proto::DropShardRequest>,
    ) -> Result<Response<proto::DropShardReply>, Status> {
        gc::drop_shard(self, request)
    }

    // ---- content fingerprint (ADR-097) ----
    /// The slot's order-independent live-set fingerprint — the group move's skip-a-complete-
    /// retained-member comparison. Read-only, fence-transparent.
    async fn content_fingerprint(
        &self,
        request: Request<proto::ContentFingerprintRequest>,
    ) -> Result<Response<proto::ContentFingerprintReply>, Status> {
        recovery::content_fingerprint(self, request)
    }
}
