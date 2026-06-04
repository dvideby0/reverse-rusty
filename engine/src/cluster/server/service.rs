//! `impl ShardService for ShardServer` — the gRPC service surface: percolate / counts /
//! dict-fingerprint reads, the adopt-dict + write RPCs (ingest / insert / delete / flush /
//! fence), and the peer-recovery RPCs (FetchSegments / RecoverFrom / FetchTranslog /
//! RetentionLease) + their server-streaming helpers.

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::cluster::clog::LogPos;
use crate::cluster::proto;
use crate::cluster::proto::shard_service_server::ShardService;
use crate::cluster::shard::{LocalShard, Shard};
use crate::segment::PlacedQuery;

use super::{compile_item, ServerState, ShardServer};

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
        let (ids, stats) = self
            .loaded()?
            .shard
            .percolate_filtered(&req.title, req.include_broad, &pred)
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
        // The frozen tag space ships ATOMICALLY with the dict (ADR-055). An empty blob ⇒ an empty
        // (untagged) tag space — back-compatible with a coordinator that ships no tags.
        let tag_dict = crate::storage::deserialize_tagdict(&req.tag_dict).map_err(|e| {
            Status::invalid_argument(format!("deserializing shipped tag dict: {e}"))
        })?;
        let tag_fp = tag_dict.fingerprint();
        if tag_fp != req.tag_dict_fingerprint {
            return Err(Status::invalid_argument(format!(
                "shipped tag-dict integrity check failed: bytes fingerprint to {tag_fp:#018x} but \
                 the request claims {:#018x}",
                req.tag_dict_fingerprint
            )));
        }

        let adopt = match self.state.load_full().as_deref() {
            // Already serving this exact dict AND tag space → nothing to do.
            Some(st) if st.dict.fingerprint() == fp && st.tag_dict.fingerprint() == tag_fp => false,
            // A different dict / tag space is already in place; only safe to replace if no data
            // depends on it (re-basing loaded data onto a divergent feature/tag space is unsafe).
            Some(st) => {
                let n = st
                    .shard
                    .num_queries()
                    .map_err(|e| Status::internal(e.to_string()))?;
                if n > 0 {
                    return Err(Status::failed_precondition(format!(
                        "shard holds {n} queries under dict {:#018x}; refusing to adopt a \
                         divergent dict {fp:#018x} / tag space (re-basing loaded data is unsafe)",
                        st.dict.fingerprint()
                    )));
                }
                true // adopted but empty → safe to re-adopt (e.g. a pre-built `new` server gaining tags)
            }
            // Pending → adopt.
            None => true,
        };

        if adopt {
            let dict = Arc::new(dict);
            let tag_dict = Arc::new(tag_dict);
            // A durable node (data_dir set) builds a segments-only durable shard so its writes
            // persist `.seg` files — required to later serve `FetchSegments` or be a recovering
            // replica (ADR-035/036). An in-memory node keeps today's behavior.
            let shard = match &self.data_dir {
                Some(dir) => {
                    let mut sc = self.config.clone();
                    sc.data_dir = Some(dir.clone());
                    LocalShard::new_durable(
                        Arc::clone(&self.norm),
                        Arc::clone(&dict),
                        Arc::clone(&tag_dict),
                        sc,
                    )
                    .map_err(|e| Status::internal(format!("durable adopt: {e}")))?
                }
                None => LocalShard::new(
                    Arc::clone(&self.norm),
                    Arc::clone(&dict),
                    Arc::clone(&tag_dict),
                    self.config.clone(),
                ),
            };
            self.state.store(Some(Arc::new(ServerState {
                dict,
                tag_dict,
                shard,
            })));
        }

        Ok(Response::new(proto::AdoptDictReply {
            fingerprint: fp,
            tag_dict_fingerprint: tag_fp,
        }))
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
            // Preserve the node's adopted frozen tag space (ADR-055); the recovered segments already
            // carry resolved `TagId`s, and the tail catch-up re-resolves its raw tags against it.
            Arc::clone(&st.tag_dict),
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
            tag_dict: Arc::clone(&st.tag_dict),
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
        let st = self.loaded()?;
        let req = request.into_inner();
        if req.dict_fingerprint != st.dict.fingerprint() {
            return Err(Status::failed_precondition(
                "Unfence dict-fingerprint mismatch (divergent feature space)",
            ));
        }
        // CAS from the exact generation this handoff fenced at. If the node is at 0 (not fenced)
        // or at a higher generation (a newer handoff re-fenced it), the swap fails and the fence
        // is left as-is — we report its current value.
        let now_gen = match self.fenced_at_generation.compare_exchange(
            req.generation,
            0,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => 0,
            Err(actual) => actual,
        };
        Ok(Response::new(proto::UnfenceReply {
            fenced_at_generation: now_gen,
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
