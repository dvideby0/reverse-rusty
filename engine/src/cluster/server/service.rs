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

use std::pin::Pin;

use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::cluster::proto;
use crate::cluster::proto::shard_service_server::ShardService;
use crate::cluster::shard::Shard;
use crate::segment::PlacedQuery;

use super::{compile_item, ShardServer};

mod dict_adopt;
mod leases;
mod recovery;

#[tonic::async_trait]
impl ShardService for ShardServer {
    async fn percolate(
        &self,
        request: Request<proto::PercolateRequest>,
    ) -> Result<Response<proto::PercolateReply>, Status> {
        let req = request.into_inner();
        // Rebuild the tag filter from the already-resolved `TagId` groups (ADR-055); empty ⇒
        // unfiltered. The ids are authoritative-from-coordinator, so the server never re-resolves
        // strings — immune to any server-side tag-space skew on reads.
        let pred = proto::tag_predicate_from_proto(req.filter);
        let st = self.loaded()?;
        // A `rank` spec (ADR-075) rides the same already-compiled-ids pattern: score this
        // shard's matched ids and echo `ranked = true` so the client can tell a scored reply
        // from an old server that silently ignored the field. Absent ⇒ the pre-rank wire.
        if let Some(rank) = req.rank {
            let spec = proto::rank_spec_from_proto(rank);
            let (scored, stats) = st
                .shard
                .percolate_filtered_ranked(&req.title, req.include_broad, &pred, &spec)
                .map_err(|e| Status::internal(e.to_string()))?;
            let (ids, scores) = scored.into_iter().unzip();
            return Ok(Response::new(proto::PercolateReply {
                ids,
                stats: Some(proto::stats_from_engine(stats)),
                scores,
                ranked: true,
            }));
        }
        let (ids, stats) = st
            .shard
            .percolate_filtered(&req.title, req.include_broad, &pred)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(proto::PercolateReply {
            ids,
            stats: Some(proto::stats_from_engine(stats)),
            scores: Vec::new(),
            ranked: false,
        }))
    }

    async fn num_queries(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::CountReply>, Status> {
        let count = self
            .loaded()?
            .shard
            .num_queries()
            .map_err(|e| Status::internal(e.to_string()))? as u64;
        Ok(Response::new(proto::CountReply { count }))
    }

    async fn class_counts(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::ClassCountsReply>, Status> {
        let counts = self
            .loaded()?
            .shard
            .class_counts()
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(proto::ClassCountsReply {
            counts: counts.to_vec(),
        }))
    }

    async fn dict_fingerprint(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::DictFingerprintReply>, Status> {
        let st = self.loaded()?;
        Ok(Response::new(proto::DictFingerprintReply {
            fingerprint: st.dict.fingerprint(),
            // ADR-077: the probe carries the tag-space identity too, so a bare
            // `connect` (no adopt) verifies BOTH dicts. A stale server omits this
            // (proto3 zero), which can never equal a real fingerprint — loud, not
            // silently unverified.
            tag_dict_fingerprint: st.tag_dict.fingerprint(),
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

    async fn ingest_extracted(
        &self,
        request: Request<proto::IngestRequest>,
    ) -> Result<Response<proto::IngestReply>, Status> {
        self.check_not_fenced()?;
        let st = self.loaded()?;
        let items = request.into_inner().items;
        let mut lc = String::new();
        let mut rejected_parse = 0u64;
        let mut extracted: Vec<PlacedQuery> = Vec::with_capacity(items.len());
        for it in items {
            match compile_item(&self.norm, &st.dict, &it.dsl, &mut lc) {
                // Carry the raw tags forward; the shard's engine resolves them read-only against the
                // adopted frozen tag space (ADR-055).
                Some(ex) => extracted.push(PlacedQuery {
                    logical: it.logical_id,
                    ex,
                    dsl: it.dsl,
                    version: it.version.max(1),
                    tags: proto::tags_from_proto(it.tags),
                    // The wire is dict-agnostic (raw tags only) — pre-resolved ids never arrive.
                    tag_ids: Vec::new(),
                }),
                None => rejected_parse += 1,
            }
        }
        let report = st.shard.ingest_local(&extracted);
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
        self.check_not_fenced()?;
        let st = self.loaded()?;
        let item = request
            .into_inner()
            .item
            .ok_or_else(|| Status::invalid_argument("InsertRequest.item is required"))?;
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
        let out = st
            .shard
            .insert_extracted_with_tags(&ex, item.logical_id, item.version.max(1), &item.dsl, &tags)
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
        self.check_not_fenced()?;
        let removed = self
            .loaded()?
            .shard
            .delete_by_logical_id(request.into_inner().logical_id)
            .map_err(|e| Status::internal(e.to_string()))? as u64;
        Ok(Response::new(proto::DeleteReply { removed }))
    }

    async fn flush(
        &self,
        _request: Request<proto::FlushRequest>,
    ) -> Result<Response<proto::FlushReply>, Status> {
        self.loaded()?
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
}
