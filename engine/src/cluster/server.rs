//! `ShardServer` — serves the gRPC `ShardService` over ONE in-process `LocalShard`.
//!
//! Construct it over the SAME frozen `Arc<Dict>` / `Arc<Normalizer>` the coordinator
//! uses for placement. The write path carries raw DSL (not pre-extracted feature
//! ids), so the server re-compiles read-only against ITS copy of that dict — a
//! dict-agnostic wire that fails loud on mismatch rather than corrupting matches.
//! Placement + routing stay the coordinator's job; the server is a dumb executor of
//! `percolate` / `ingest` / `insert` / `delete` / `flush`.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::compile::{extract_readonly, Extracted};
use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::normalize::Normalizer;

use super::clog::LogPos;
use super::proto;
use super::proto::shard_service_server::{ShardService, ShardServiceServer};
use super::shard::{LocalShard, Shard, ShardError};

/// The adopted feature space + the shard compiled over it. Held behind an
/// [`ArcSwapOption`] in [`ShardServer`] so a server can start *pending* (no dict) and
/// adopt one shipped by the coordinator at connect (ADR-034).
struct ServerState {
    dict: Arc<Dict>,
    shard: LocalShard,
}

/// A gRPC server wrapping ONE in-process shard.
///
/// The (dict, shard) pair is **swappable**: a server may start *pending* (dict-less) via
/// [`ShardServer::pending`] and adopt the coordinator's frozen dict through the `AdoptDict`
/// RPC, so a data node need not rebuild a byte-identical dict from the corpus out-of-band
/// (ADR-034). `norm` + `config` are fixed for the server's life (the normalizer must still
/// match the coordinator's — `default_vocab()` today; see ADR-034 scope note).
pub struct ShardServer {
    norm: Arc<Normalizer>,
    config: EngineConfig,
    /// `Some` ⇒ a **durable** node: its shard persists segments under this dir (ADR-035), so
    /// the node can serve `FetchSegments` (stream its segments to a recovering peer) and accept
    /// `RecoverFrom` (pull a peer's segments + attach). `None` ⇒ in-memory (today's default).
    /// When set, `AdoptDict` builds a durable (segments-only) shard rather than an in-memory one.
    data_dir: Option<PathBuf>,
    /// `None` until a dict is adopted; reads against a pending server return
    /// `failed_precondition`.
    state: ArcSwapOption<ServerState>,
    /// The fence generation (ADR-044, step 6b): `0` ⇒ not fenced; `> 0` ⇒ this node has been
    /// demoted as the owner of its shard at that generation, so data-mutating writes
    /// (`insert`/`delete`/`ingest`) return `failed_precondition`. Reads + the recovery RPCs stay
    /// served (serve-then-drop). Set monotonically by the `Fence` RPC (a stale lower-gen Fence
    /// never un-fences). A live handoff fences the old owner, drains its tail to the new owner, then
    /// flips routing — the fence holds a brief write-quiesce across that flip.
    fenced_at_generation: AtomicU64,
}

impl ShardServer {
    /// Build a server over a fresh `LocalShard` sharing the given frozen `norm`/`dict` —
    /// the pre-built path (the dict is already arranged to match the coordinator's).
    pub fn new(norm: Arc<Normalizer>, dict: Arc<Dict>, config: EngineConfig) -> Self {
        let shard = LocalShard::new(Arc::clone(&norm), Arc::clone(&dict), config.clone());
        let state = ArcSwapOption::from(Some(Arc::new(ServerState { dict, shard })));
        ShardServer {
            norm,
            config,
            data_dir: None,
            state,
            fenced_at_generation: AtomicU64::new(0),
        }
    }

    /// Build a **pending** server: no dict yet, awaiting an `AdoptDict` from the coordinator
    /// (ADR-034). Reads return `failed_precondition` until a dict is adopted. This is how a
    /// data node starts in a real multi-node deploy — empty, then handed the frozen dict —
    /// instead of rebuilding a byte-identical dict from the whole corpus out-of-band.
    pub fn pending(norm: Arc<Normalizer>, config: EngineConfig) -> Self {
        ShardServer {
            norm,
            config,
            data_dir: None,
            state: ArcSwapOption::from(None),
            fenced_at_generation: AtomicU64::new(0),
        }
    }

    /// A **durable, pending** server (ADR-035/036): empty (awaiting `AdoptDict`) but rooted at
    /// `data_dir`, so once it adopts a dict its shard persists segments there. This is the real
    /// recovering/replica node — after adoption it can serve `FetchSegments` and accept
    /// `RecoverFrom`. The durable analogue of [`Self::pending`].
    pub fn pending_durable(norm: Arc<Normalizer>, config: EngineConfig, data_dir: PathBuf) -> Self {
        ShardServer {
            norm,
            config,
            data_dir: Some(data_dir),
            state: ArcSwapOption::from(None),
            fenced_at_generation: AtomicU64::new(0),
        }
    }

    /// A **durable, pre-built** server: build a segments-only durable shard over `dict` rooted
    /// at `data_dir`. The durable analogue of [`Self::new`]. Errors if the durable engine cannot
    /// be created (e.g. the dir is unwritable).
    pub fn new_durable(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        config: EngineConfig,
        data_dir: PathBuf,
    ) -> Result<Self, ShardError> {
        let mut sc = config.clone();
        sc.data_dir = Some(data_dir.clone());
        let shard = LocalShard::new_durable(Arc::clone(&norm), Arc::clone(&dict), sc)?;
        let state = ArcSwapOption::from(Some(Arc::new(ServerState { dict, shard })));
        Ok(ShardServer {
            norm,
            config,
            data_dir: Some(data_dir),
            state,
            fenced_at_generation: AtomicU64::new(0),
        })
    }

    /// The adopted state, or `failed_precondition` if the server is still pending.
    fn loaded(&self) -> Result<Arc<ServerState>, Status> {
        self.state
            .load_full()
            .ok_or_else(|| Status::failed_precondition("shard has not adopted a dict yet"))
    }

    /// Reject a data-mutating write if this node has been fenced (demoted by a live handoff,
    /// ADR-044). Called by `insert`/`delete`/`ingest` only — reads + the recovery RPCs deliberately
    /// do NOT call it, so the demoted owner keeps serving them until the coordinator stops routing
    /// to it (serve-then-drop), and an in-flight read never hits the fence.
    fn check_not_fenced(&self) -> Result<(), Status> {
        let gen = self.fenced_at_generation.load(Ordering::Acquire);
        if gen > 0 {
            return Err(Status::failed_precondition(format!(
                "shard is fenced at generation {gen} (demoted by a handoff); writes are rejected"
            )));
        }
        Ok(())
    }

    /// Compile + bulk-load raw `(id, DSL)` queries into this shard before serving —
    /// the server-side preload for standing up a populated node. Read-only against the
    /// adopted frozen dict; parse failures are skipped (like `build`/`ingest`). No-op on a
    /// pending (not-yet-adopted) server.
    pub fn ingest_dsl(&self, items: &[(u64, String)]) {
        let Some(st) = self.state.load_full() else {
            return;
        };
        let mut lc = String::new();
        let extracted: Vec<(u64, Extracted, String, u32)> = items
            .iter()
            .filter_map(|(logical, dsl)| {
                let ast = crate::dsl::parse(dsl).ok()?;
                let ex = extract_readonly(&ast, &self.norm, &st.dict, &mut lc);
                Some((*logical, ex, dsl.clone(), 1))
            })
            .collect();
        st.shard.ingest_local(&extracted);
    }

    /// Serve `ShardService` on `addr` until the returned future completes.
    pub async fn serve(self, addr: SocketAddr) -> Result<(), tonic::transport::Error> {
        tonic::transport::Server::builder()
            .add_service(ShardServiceServer::new(self))
            .serve(addr)
            .await
    }

    /// Serve with a graceful-shutdown `signal` future — used by tests to stop cleanly.
    pub async fn serve_with_shutdown<F>(
        self,
        addr: SocketAddr,
        signal: F,
    ) -> Result<(), tonic::transport::Error>
    where
        F: std::future::Future<Output = ()>,
    {
        tonic::transport::Server::builder()
            .add_service(ShardServiceServer::new(self))
            .serve_with_shutdown(addr, signal)
            .await
    }

    /// Serve `ShardService` on an already-bound `incoming` listener (no rebind). Lets a
    /// caller bind the socket first and learn its port — an ephemeral `:0` for tests, or
    /// socket activation in production — without the bind→drop→rebind gap that re-binding
    /// by address would open.
    pub async fn serve_with_incoming(
        self,
        incoming: tonic::transport::server::TcpIncoming,
    ) -> Result<(), tonic::transport::Error> {
        tonic::transport::Server::builder()
            .add_service(ShardServiceServer::new(self))
            .serve_with_incoming(incoming)
            .await
    }
}

/// Compile one raw query read-only against the shared frozen dict (parse failure →
/// `None`, counted by the caller as a rejected-parse).
fn compile_item(norm: &Normalizer, dict: &Dict, dsl: &str, lc: &mut String) -> Option<Extracted> {
    let ast = crate::dsl::parse(dsl).ok()?;
    Some(extract_readonly(&ast, norm, dict, lc))
}

#[tonic::async_trait]
impl ShardService for ShardServer {
    async fn percolate(
        &self,
        request: Request<proto::PercolateRequest>,
    ) -> Result<Response<proto::PercolateReply>, Status> {
        let req = request.into_inner();
        let (ids, stats) = self
            .loaded()?
            .shard
            .percolate(&req.title, req.include_broad)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(proto::PercolateReply {
            ids,
            stats: Some(proto::stats_from_engine(stats)),
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
        Ok(Response::new(proto::DictFingerprintReply {
            fingerprint: self.loaded()?.dict.fingerprint(),
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
        let req = request.into_inner();
        let dict = crate::storage::deserialize_dict(&req.dict)
            .map_err(|e| Status::invalid_argument(format!("deserializing shipped dict: {e}")))?;
        let fp = dict.fingerprint();
        if fp != req.fingerprint {
            return Err(Status::invalid_argument(format!(
                "shipped dict integrity check failed: bytes fingerprint to {fp:#018x} but the \
                 request claims {:#018x}",
                req.fingerprint
            )));
        }

        let adopt = match self.state.load_full().as_deref() {
            // Already serving this exact dict → nothing to do.
            Some(st) if st.dict.fingerprint() == fp => false,
            // A different dict is already in place; only safe to replace if no data depends on it.
            Some(st) => {
                let n = st
                    .shard
                    .num_queries()
                    .map_err(|e| Status::internal(e.to_string()))?;
                if n > 0 {
                    return Err(Status::failed_precondition(format!(
                        "shard holds {n} queries under dict {:#018x}; refusing to adopt a \
                         divergent dict {fp:#018x} (re-basing loaded data is unsafe)",
                        st.dict.fingerprint()
                    )));
                }
                true // adopted but empty → safe to re-adopt
            }
            // Pending → adopt.
            None => true,
        };

        if adopt {
            let dict = Arc::new(dict);
            // A durable node (data_dir set) builds a segments-only durable shard so its writes
            // persist `.seg` files — required to later serve `FetchSegments` or be a recovering
            // replica (ADR-035/036). An in-memory node keeps today's behavior.
            let shard = match &self.data_dir {
                Some(dir) => {
                    let mut sc = self.config.clone();
                    sc.data_dir = Some(dir.clone());
                    LocalShard::new_durable(Arc::clone(&self.norm), Arc::clone(&dict), sc)
                        .map_err(|e| Status::internal(format!("durable adopt: {e}")))?
                }
                None => LocalShard::new(
                    Arc::clone(&self.norm),
                    Arc::clone(&dict),
                    self.config.clone(),
                ),
            };
            self.state
                .store(Some(Arc::new(ServerState { dict, shard })));
        }

        Ok(Response::new(proto::AdoptDictReply { fingerprint: fp }))
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
        let mut extracted: Vec<(u64, Extracted, String, u32)> = Vec::with_capacity(items.len());
        for it in items {
            match compile_item(&self.norm, &st.dict, &it.dsl, &mut lc) {
                Some(ex) => extracted.push((it.logical_id, ex, it.dsl, it.version.max(1))),
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
        let out = st
            .shard
            .insert_extracted(&ex, item.logical_id, item.version.max(1), &item.dsl)
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
        let st = self.loaded()?;
        let Some(dir) = self.data_dir.clone() else {
            return Err(Status::failed_precondition(
                "shard is not durable; cannot stream segments for peer recovery",
            ));
        };
        let fp = st.dict.fingerprint();
        if request.into_inner().dict_fingerprint != fp {
            return Err(Status::failed_precondition(
                "FetchSegments dict-fingerprint mismatch (divergent feature space)",
            ));
        }
        // Seal so the on-disk `.seg` set reflects live state (memtable flushed, base tombstones
        // baked) — else a deleted query could resurrect on the recovered replica. The returned
        // position `P` is what the sealed segments capture through; the recovering node replays
        // the translog tail (> P) via FetchTranslog to catch writes that land during the copy
        // (ADR-039), so the source need NOT quiesce.
        let up_to_seqno = st
            .shard
            .seal_for_checkpoint()
            .map_err(|e| Status::internal(format!("seal before FetchSegments: {e}")))?
            .0;
        let files = st
            .shard
            .segment_filenames()
            .map_err(|e| Status::internal(format!("collecting segment filenames: {e}")))?;
        let next_seg_id = st
            .shard
            .next_seg_id()
            .map_err(|e| Status::internal(format!("next_seg_id: {e}")))?;

        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move {
            let seg_dir = dir.join("segments");
            let sources = dir.join("sources.dat");
            let has_sources = sources.exists();
            let manifest = proto::FetchSegmentsChunk {
                frame: Some(proto::fetch_segments_chunk::Frame::Manifest(
                    proto::FetchManifest {
                        segment_files: files.clone(),
                        next_seg_id,
                        dict_fingerprint: fp,
                        has_sources,
                        up_to_seqno,
                    },
                )),
            };
            if tx.send(Ok(manifest)).await.is_err() {
                return;
            }
            for name in &files {
                if !stream_file(&tx, name, &seg_dir.join(name)).await {
                    return;
                }
            }
            if has_sources {
                stream_file(&tx, "sources.dat", &sources).await;
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
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
        let st = self.loaded()?;
        let Some(dir) = self.data_dir.clone() else {
            return Err(Status::failed_precondition(
                "shard is not durable; cannot accept peer recovery",
            ));
        };
        let req = request.into_inner();
        let dict_fp = st.dict.fingerprint();
        if req.dict_fingerprint != dict_fp {
            return Err(Status::failed_precondition(
                "RecoverFrom dict-fingerprint mismatch (divergent feature space)",
            ));
        }
        let mut client =
            proto::shard_service_client::ShardServiceClient::connect(req.source_endpoint.clone())
                .await
                .map_err(|e| {
                    Status::unavailable(format!(
                        "connecting to recovery source {}: {e}",
                        req.source_endpoint
                    ))
                })?;
        let mut stream = client
            .fetch_segments(proto::FetchSegmentsRequest {
                dict_fingerprint: dict_fp,
            })
            .await?
            .into_inner();

        let seg_dir = dir.join("segments");
        std::fs::create_dir_all(&seg_dir)
            .map_err(|e| Status::internal(format!("creating {}: {e}", seg_dir.display())))?;
        let (files, next_seg_id, up_to_seqno) =
            drain_recovery_stream(&mut stream, &dir, &seg_dir).await?;

        // Attach the received segments against our adopted dict (fail-loud on missing/corrupt).
        let mut sc = self.config.clone();
        sc.data_dir = Some(dir.clone());
        let shard = LocalShard::open_segments(
            Arc::clone(&self.norm),
            Arc::clone(&st.dict),
            sc,
            &files,
            next_seg_id,
        )
        .map_err(|e| Status::internal(format!("attaching recovered segments: {e}")))?;
        let num_queries = shard
            .num_queries()
            .map_err(|e| Status::internal(e.to_string()))? as u64;
        let segments_attached = files.len() as u64;
        self.state.store(Some(Arc::new(ServerState {
            dict: Arc::clone(&st.dict),
            shard,
        })));
        Ok(Response::new(proto::RecoverFromReply {
            segments_attached,
            num_queries,
            up_to_seqno,
        }))
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
        let st = self.loaded()?;
        let req = request.into_inner();
        let fp = st.dict.fingerprint();
        if req.dict_fingerprint != fp {
            return Err(Status::failed_precondition(
                "FetchTranslog dict-fingerprint mismatch (divergent feature space)",
            ));
        }
        let tail = st
            .shard
            .translog_tail(LogPos(req.after_seqno))
            .map_err(|e| Status::internal(format!("reading translog tail: {e}")))?;
        let entries: Vec<Result<proto::TranslogEntry, Status>> = tail
            .into_iter()
            .map(|(pos, m)| Ok(proto::translog_entry_from_mutation(pos, &m)))
            .collect();
        Ok(Response::new(Box::pin(tokio_stream::iter(entries))))
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
        let st = self.loaded()?;
        let req = request.into_inner();
        if req.dict_fingerprint != st.dict.fingerprint() {
            return Err(Status::failed_precondition(
                "RetentionLease dict-fingerprint mismatch (divergent feature space)",
            ));
        }
        match req.op {
            0 => {
                let (lease_id, pos) = st
                    .shard
                    .acquire_retention_lease()
                    .map_err(|e| Status::internal(format!("acquire retention lease: {e}")))?;
                Ok(Response::new(proto::RetentionLeaseReply {
                    lease_id,
                    pos: pos.0,
                }))
            }
            1 => {
                st.shard
                    .renew_retention_lease(req.lease_id, LogPos(req.pos))
                    .map_err(|e| Status::internal(format!("renew retention lease: {e}")))?;
                Ok(Response::new(proto::RetentionLeaseReply::default()))
            }
            2 => {
                st.shard
                    .release_retention_lease(req.lease_id)
                    .map_err(|e| Status::internal(format!("release retention lease: {e}")))?;
                Ok(Response::new(proto::RetentionLeaseReply::default()))
            }
            other => Err(Status::invalid_argument(format!(
                "RetentionLease: unknown op {other} (expected 0=acquire, 1=renew, 2=release)"
            ))),
        }
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
        let st = self.loaded()?;
        let req = request.into_inner();
        if req.dict_fingerprint != st.dict.fingerprint() {
            return Err(Status::failed_precondition(
                "Fence dict-fingerprint mismatch (divergent feature space)",
            ));
        }
        // Monotonic max: a later, lower-generation Fence (a stale/duplicate message) never lowers
        // the fence. `fetch_max` returns the previous value; the stored value becomes the max.
        let prev = self
            .fenced_at_generation
            .fetch_max(req.generation, Ordering::AcqRel);
        Ok(Response::new(proto::FenceReply {
            fenced_at_generation: prev.max(req.generation),
        }))
    }
}

/// Stream one file as a contiguous run of ≤256 KiB `FileChunk`s ending with `last = true`.
/// Reads the file into memory once (bounded per-file — fine for a recovery path; a chunked
/// file read is a future refinement). Returns `false` to abort the stream (read error — the
/// error is forwarded to the receiver first — or the receiver hung up).
async fn stream_file(
    tx: &tokio::sync::mpsc::Sender<Result<proto::FetchSegmentsChunk, Status>>,
    name: &str,
    path: &std::path::Path,
) -> bool {
    const CHUNK: usize = 256 * 1024;
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) => {
            tx.send(Err(Status::internal(format!(
                "reading {name} for FetchSegments: {e}"
            ))))
            .await
            .ok();
            return false;
        }
    };
    let mut off = 0usize;
    loop {
        let end = (off + CHUNK).min(bytes.len());
        let last = end == bytes.len();
        let chunk = proto::FetchSegmentsChunk {
            frame: Some(proto::fetch_segments_chunk::Frame::File(proto::FileChunk {
                name: name.to_string(),
                data: bytes[off..end].to_vec(),
                last,
            })),
        };
        if tx.send(Ok(chunk)).await.is_err() {
            return false;
        }
        if last {
            return true;
        }
        off = end;
    }
}

/// Drain a `FetchSegments` stream into `dir`: the manifest frame first, then per-file runs
/// written via tmp+rename (so a crash mid-recovery never leaves a half-written `.seg` that a
/// later attach would CRC-reject). Validates that every manifested segment fully arrived — a
/// truncated stream errors rather than attaching a subset (a silent shard-sized false
/// negative). Returns the attach file list + seg-id cursor from the manifest.
async fn drain_recovery_stream(
    stream: &mut tonic::Streaming<proto::FetchSegmentsChunk>,
    dir: &std::path::Path,
    seg_dir: &std::path::Path,
) -> Result<(Vec<String>, u64, u64), Status> {
    use std::io::Write as _;
    let final_path = |name: &str| -> PathBuf {
        if name == "sources.dat" {
            dir.join("sources.dat")
        } else {
            seg_dir.join(name)
        }
    };
    let mut manifest: Option<proto::FetchManifest> = None;
    let mut received: std::collections::HashSet<String> = std::collections::HashSet::new();
    // The currently-open tmp file: (name, handle, tmp path). Files arrive as contiguous runs.
    let mut cur: Option<(String, std::fs::File, PathBuf)> = None;

    while let Some(chunk) = stream.message().await? {
        match chunk.frame {
            Some(proto::fetch_segments_chunk::Frame::Manifest(m)) => manifest = Some(m),
            Some(proto::fetch_segments_chunk::Frame::File(fc)) => {
                if cur.as_ref().is_none_or(|(n, _, _)| *n != fc.name) {
                    let fin = final_path(&fc.name);
                    let tmp = PathBuf::from(format!("{}.tmp", fin.display()));
                    let f = std::fs::File::create(&tmp)
                        .map_err(|e| Status::internal(format!("create {}: {e}", tmp.display())))?;
                    cur = Some((fc.name.clone(), f, tmp));
                }
                if let Some((_, f, _)) = cur.as_mut() {
                    f.write_all(&fc.data)
                        .map_err(|e| Status::internal(format!("writing {}: {e}", fc.name)))?;
                }
                if fc.last {
                    if let Some((name, f, tmp)) = cur.take() {
                        f.sync_all()
                            .map_err(|e| Status::internal(format!("sync {name}: {e}")))?;
                        drop(f);
                        std::fs::rename(&tmp, final_path(&name))
                            .map_err(|e| Status::internal(format!("rename {name}: {e}")))?;
                        received.insert(name);
                    }
                }
            }
            None => {}
        }
    }
    let manifest =
        manifest.ok_or_else(|| Status::internal("recovery stream had no manifest frame"))?;
    for name in &manifest.segment_files {
        if !received.contains(name) {
            return Err(Status::internal(format!(
                "recovery stream truncated: segment {name} did not fully arrive"
            )));
        }
    }
    Ok((
        manifest.segment_files,
        manifest.next_seg_id,
        manifest.up_to_seqno,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tonic::{Code, Request};

    use super::proto::shard_service_server::ShardService;
    use super::{proto, ShardServer};
    use crate::compile::extract;
    use crate::config::EngineConfig;
    use crate::dict::Dict;
    use crate::normalize::Normalizer;
    use crate::storage::serialize_dict;

    fn norm() -> Arc<Normalizer> {
        Arc::new(Normalizer::default_vocab().expect("built-in vocab"))
    }

    /// A frozen dict interned over `snips` in order (mirrors the gRPC oracle helper).
    fn frozen_dict(snips: &[&str], norm: &Normalizer) -> Dict {
        let mut d = Dict::new();
        let mut lc = String::new();
        for q in snips {
            if let Ok(ast) = crate::dsl::parse(q) {
                let _ = extract(&ast, norm, &mut d, &mut lc);
            }
        }
        d.finalize_mask();
        d
    }

    fn adopt_req(dict: &Dict) -> Request<proto::AdoptDictRequest> {
        Request::new(proto::AdoptDictRequest {
            dict: serialize_dict(dict),
            fingerprint: dict.fingerprint(),
        })
    }

    fn current_fp(srv: &ShardServer) -> u64 {
        srv.state.load_full().expect("adopted").dict.fingerprint()
    }

    /// Exercises every arm of the `AdoptDict` contract through the real async handler:
    /// pending-read-fails, empty→adopt, same-fp→no-op, bad-fp→invalid, empty-different→re-adopt,
    /// and non-empty-divergent→refuse (the load-bearing silent-FN guard).
    #[test]
    fn adopt_dict_state_machine() {
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        let n = norm();
        let d1 = frozen_dict(&["1994 upper deck", "psa 10"], &n);
        let d2 = frozen_dict(&["1994 upper deck", "psa 10", "1995 fleer ultra"], &n);
        assert_ne!(
            d1.fingerprint(),
            d2.fingerprint(),
            "test setup: the two dicts must differ"
        );

        let srv = ShardServer::pending(Arc::clone(&n), EngineConfig::default());
        // Pending: reads fail loud rather than fabricating an empty result.
        assert!(srv.state.load_full().is_none());
        let err = rt
            .block_on(srv.num_queries(Request::new(proto::Empty {})))
            .expect_err("pending read must fail");
        assert_eq!(err.code(), Code::FailedPrecondition);

        // Empty → adopt d1.
        let fp = rt
            .block_on(srv.adopt_dict(adopt_req(&d1)))
            .expect("adopt onto empty")
            .into_inner()
            .fingerprint;
        assert_eq!(fp, d1.fingerprint());
        assert_eq!(current_fp(&srv), d1.fingerprint());

        // Same dict again → idempotent no-op.
        rt.block_on(srv.adopt_dict(adopt_req(&d1)))
            .expect("re-adopt same dict is a no-op");
        assert_eq!(current_fp(&srv), d1.fingerprint());

        // Integrity: d2 bytes but d1's claimed fingerprint → invalid_argument.
        let bad = Request::new(proto::AdoptDictRequest {
            dict: serialize_dict(&d2),
            fingerprint: d1.fingerprint(),
        });
        assert_eq!(
            rt.block_on(srv.adopt_dict(bad))
                .expect_err("fingerprint mismatch must be rejected")
                .code(),
            Code::InvalidArgument
        );

        // Empty shard, different valid dict → re-adopt allowed (no data at risk).
        rt.block_on(srv.adopt_dict(adopt_req(&d2)))
            .expect("re-adopt onto still-empty shard");
        assert_eq!(current_fp(&srv), d2.fingerprint());

        // Load data, then a DIVERGENT dict → refused (the silent-FN guard).
        srv.ingest_dsl(&[(1u64, "1994 upper deck".to_string())]);
        let n_loaded = rt
            .block_on(srv.num_queries(Request::new(proto::Empty {})))
            .expect("count after load")
            .into_inner()
            .count;
        assert!(n_loaded >= 1, "expected loaded data, got {n_loaded}");
        assert_eq!(
            rt.block_on(srv.adopt_dict(adopt_req(&d1)))
                .expect_err("divergent dict on a non-empty shard must be refused")
                .code(),
            Code::FailedPrecondition
        );
        // The SAME dict on a non-empty shard is still a no-op (not refused).
        rt.block_on(srv.adopt_dict(adopt_req(&d2)))
            .expect("same dict on a populated shard is a no-op");
        assert_eq!(current_fp(&srv), d2.fingerprint());
    }

    /// The live-handoff write fence (ADR-044): once `Fence` lands, data-mutating writes
    /// (`insert`/`delete`/`ingest`) are rejected with `FailedPrecondition`, while reads stay served
    /// (serve-then-drop); the fence is monotonic and dict-fingerprint-guarded.
    #[test]
    fn fence_rejects_writes_but_serves_reads() {
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        let n = norm();
        let d = frozen_dict(&["1994 upper deck", "psa 10"], &n);
        let fp = d.fingerprint();
        let srv = ShardServer::new(Arc::clone(&n), Arc::new(d), EngineConfig::default());
        srv.ingest_dsl(&[(1u64, "1994 upper deck".to_string())]);

        let insert = |id: u64, dsl: &str| {
            Request::new(proto::InsertRequest {
                item: Some(proto::AddItem {
                    logical_id: id,
                    dsl: dsl.to_string(),
                    version: 1,
                }),
            })
        };

        // Before the fence: a write succeeds.
        rt.block_on(srv.insert_extracted(insert(2, "psa 10")))
            .expect("insert before fence");

        // Fence at generation 5.
        let fenced = rt
            .block_on(srv.fence(Request::new(proto::FenceRequest {
                generation: 5,
                dict_fingerprint: fp,
            })))
            .expect("fence")
            .into_inner()
            .fenced_at_generation;
        assert_eq!(fenced, 5);

        // After the fence: every data-mutating write is rejected.
        assert_eq!(
            rt.block_on(srv.insert_extracted(insert(3, "psa 10")))
                .expect_err("insert after fence")
                .code(),
            Code::FailedPrecondition
        );
        assert_eq!(
            rt.block_on(srv.delete(Request::new(proto::DeleteRequest { logical_id: 1 })))
                .expect_err("delete after fence")
                .code(),
            Code::FailedPrecondition
        );
        assert_eq!(
            rt.block_on(srv.ingest_extracted(Request::new(proto::IngestRequest { items: vec![] })))
                .expect_err("ingest after fence")
                .code(),
            Code::FailedPrecondition
        );

        // ...but reads still serve (serve-then-drop): num_queries + percolate keep working.
        let cnt = rt
            .block_on(srv.num_queries(Request::new(proto::Empty {})))
            .expect("read after fence")
            .into_inner()
            .count;
        assert!(cnt >= 1, "reads stay served while fenced: {cnt}");
        rt.block_on(srv.percolate(Request::new(proto::PercolateRequest {
            title: "1994 upper deck".to_string(),
            include_broad: false,
        })))
        .expect("percolate after fence");

        // Monotonic: a stale, lower-generation fence never lowers the fence.
        let after_stale = rt
            .block_on(srv.fence(Request::new(proto::FenceRequest {
                generation: 3,
                dict_fingerprint: fp,
            })))
            .expect("stale fence")
            .into_inner()
            .fenced_at_generation;
        assert_eq!(after_stale, 5, "a lower-gen fence must not lower the fence");
        assert_eq!(
            rt.block_on(srv.insert_extracted(insert(4, "psa 10")))
                .expect_err("still fenced after a stale fence")
                .code(),
            Code::FailedPrecondition
        );

        // A dict-fingerprint mismatch is refused (never fences across a divergent feature space).
        assert_eq!(
            rt.block_on(srv.fence(Request::new(proto::FenceRequest {
                generation: 9,
                dict_fingerprint: fp ^ 0xDEAD_BEEF,
            })))
            .expect_err("fence fp mismatch")
            .code(),
            Code::FailedPrecondition
        );
    }
}
