//! The peer-recovery RPC bodies (ADR-035/036/039) — `FetchSegments` / `RecoverFrom` /
//! `FetchTranslog` — plus their server-streaming helpers. Split out of the
//! [`ShardService`](super) trait impl, which delegates here; the associated stream-type
//! declarations stay on the trait impl (Rust requires it), so these return the concrete
//! `Pin<Box<dyn Stream…>>` they alias to.

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::cluster::clog::LogPos;
use crate::cluster::coordinator::shard_dir;
use crate::cluster::proto;
use crate::cluster::shard::{LocalShard, Shard};

use super::super::{ServerState, ShardServer};

/// The concrete stream types behind `ShardService::{FetchSegmentsStream, FetchTranslogStream}`
/// (the trait impl aliases the same types); spelled once here so the extracted body fns are not
/// flagged `clippy::type_complexity`.
type FetchSegmentsStream =
    Pin<Box<dyn Stream<Item = Result<proto::FetchSegmentsChunk, Status>> + Send>>;
type FetchTranslogStream = Pin<Box<dyn Stream<Item = Result<proto::TranslogEntry, Status>> + Send>>;

/// Body of [`ShardService::fetch_segments`](crate::cluster::proto::shard_service_server::ShardService::fetch_segments).
pub(super) fn fetch_segments(
    server: &ShardServer,
    request: Request<proto::FetchSegmentsRequest>,
) -> Result<Response<FetchSegmentsStream>, Status> {
    let req = request.into_inner();
    let (_slot, st) = server.loaded_slot(req.shard_id)?;
    let Some(root) = server.data_dir.clone() else {
        return Err(Status::failed_precondition(
            "shard is not durable; cannot stream segments for peer recovery",
        ));
    };
    // This slot's segments live under its per-shard subdir (ADR-093).
    let dir = shard_dir(&root, req.shard_id as usize);
    let fp = st.dict.fingerprint();
    if req.dict_fingerprint != fp {
        return Err(Status::failed_precondition(
            "FetchSegments dict-fingerprint mismatch (divergent feature space)",
        ));
    }
    // ADR-077: the streamed segments' tag columns hold resolved `TagId`s — shipping
    // them into a divergent tag space would silently mis-filter, so refuse like the dict.
    if req.tag_dict_fingerprint != st.tag_dict.fingerprint() {
        return Err(Status::failed_precondition(
            "FetchSegments tag-dict-fingerprint mismatch (divergent tag space)",
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

/// Body of [`ShardService::recover_from`](crate::cluster::proto::shard_service_server::ShardService::recover_from).
pub(super) async fn recover_from(
    server: &ShardServer,
    request: Request<proto::RecoverFromRequest>,
) -> Result<Response<proto::RecoverFromReply>, Status> {
    let req = request.into_inner();
    // `loaded_slot` returns owned `Arc`s (the map guard is already dropped), so holding `slot` across
    // the peer dial + stream `.await`s below never holds the std `RwLock`.
    let (slot, st) = server.loaded_slot(req.shard_id)?;
    let Some(root) = server.data_dir.clone() else {
        return Err(Status::failed_precondition(
            "shard is not durable; cannot accept peer recovery",
        ));
    };
    // Recover INTO this slot's per-shard subdir (ADR-093) — never clobber the node's other shards.
    let dir = shard_dir(&root, req.shard_id as usize);
    let dict_fp = st.dict.fingerprint();
    if req.dict_fingerprint != dict_fp {
        return Err(Status::failed_precondition(
            "RecoverFrom dict-fingerprint mismatch (divergent feature space)",
        ));
    }
    let tag_fp = st.tag_dict.fingerprint();
    if req.tag_dict_fingerprint != tag_fp {
        return Err(Status::failed_precondition(
            "RecoverFrom tag-dict-fingerprint mismatch (divergent tag space)",
        ));
    }
    // Dial the peer source through the MESH path (ADR-071): this node's client
    // security (TLS + token) applies to the outbound pull exactly as it does to a
    // coordinator connection — a bare connect here would silently bypass the mesh
    // (and a secured source would reject the unauthenticated FetchSegments anyway).
    let mut client =
        crate::cluster::remote::connect_mesh(&req.source_endpoint, &server.client_security)
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
            tag_dict_fingerprint: tag_fp,
            // A relocation/replication keeps the SAME global position, so pull the source's slot of
            // the same shard-id we're recovering into (ADR-093).
            shard_id: req.shard_id,
        })
        .await?
        .into_inner();

    let seg_dir = dir.join("segments");
    std::fs::create_dir_all(&seg_dir)
        .map_err(|e| Status::internal(format!("creating {}: {e}", seg_dir.display())))?;
    let (files, next_seg_id, up_to_seqno) =
        drain_recovery_stream(&mut stream, &dir, &seg_dir).await?;

    // Attach the received segments against our adopted dict (fail-loud on missing/corrupt).
    let mut sc = server.config.clone();
    sc.data_dir = Some(dir.clone());
    let shard = LocalShard::open_segments(
        Arc::clone(&server.norm),
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
    // Store into THIS slot's state cell (preserving its fence generation) — never a node-wide swap,
    // so a recovery never clobbers a co-located shard (ADR-093, the codex-P1 fix's read side).
    slot.state.store(Some(Arc::new(ServerState {
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

/// Body of [`ShardService::fetch_translog`](crate::cluster::proto::shard_service_server::ShardService::fetch_translog).
pub(super) fn fetch_translog(
    server: &ShardServer,
    request: Request<proto::FetchTranslogRequest>,
) -> Result<Response<FetchTranslogStream>, Status> {
    let req = request.into_inner();
    let (_slot, st) = server.loaded_slot(req.shard_id)?;
    let fp = st.dict.fingerprint();
    if req.dict_fingerprint != fp {
        return Err(Status::failed_precondition(
            "FetchTranslog dict-fingerprint mismatch (divergent feature space)",
        ));
    }
    // ADR-077: the tail's raw tags re-resolve against the receiver's tag space —
    // consistent only if both sides hold the SAME frozen tag dict.
    if req.tag_dict_fingerprint != st.tag_dict.fingerprint() {
        return Err(Status::failed_precondition(
            "FetchTranslog tag-dict-fingerprint mismatch (divergent tag space)",
        ));
    }
    let tail = st
        .shard
        .translog_tail(LogPos(req.after_seqno))
        .map_err(|e| Status::internal(format!("reading translog tail: {e}")))?;
    let entries: Vec<Result<proto::TranslogEntry, Status>> = tail
        .into_iter()
        .map(|(pos, m)| {
            // An unrepresentable frame (a whole Upsert — see the mapper) fails the
            // stream LOUD: a recovery built on a half-shipped frame would diverge
            // silently from its source.
            proto::translog_entry_from_mutation(pos, &m).ok_or_else(|| {
                Status::internal(
                    "translog holds a frame FetchTranslog cannot represent (whole Upsert); \
                     per-shard translogs must carry decomposed ops (ADR-070)",
                )
            })
        })
        .collect();
    Ok(Response::new(Box::pin(tokio_stream::iter(entries))))
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

/// Validate that every file the manifest advertised fully arrived (a truncated stream must
/// error, not attach a subset and report success). Covers BOTH the segment files AND
/// `sources.dat` — the latter streams LAST, so a stream cut between the final segment and a
/// complete `sources.dat` would otherwise attach a full segment set yet silently lose the
/// query SOURCES (percolate stays intact, but `_doc` reads + a corpus rebuild lose those
/// queries). A source-less node (`has_sources == false`) is unaffected — the byte-identical
/// pre-sources path. Extracted so the receiver-side check is unit-testable without a live
/// gRPC stream.
fn validate_received(
    manifest: &proto::FetchManifest,
    received: &std::collections::HashSet<String>,
) -> Result<(), Status> {
    for name in &manifest.segment_files {
        if !received.contains(name) {
            return Err(Status::internal(format!(
                "recovery stream truncated: segment {name} did not fully arrive"
            )));
        }
    }
    if manifest.has_sources && !received.contains("sources.dat") {
        return Err(Status::internal(
            "recovery stream truncated: sources.dat did not fully arrive",
        ));
    }
    Ok(())
}

/// Drain a `FetchSegments` stream into `dir`: the manifest frame first, then per-file runs
/// written via tmp+rename (so a crash mid-recovery never leaves a half-written `.seg` that a
/// later attach would CRC-reject). Validates that every manifested file (segments AND
/// `sources.dat`) fully arrived — a truncated stream errors rather than attaching a subset (a
/// silent shard-sized false negative). Returns the attach file list + seg-id cursor from the
/// manifest.
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
    validate_received(&manifest, &received)?;
    Ok((
        manifest.segment_files,
        manifest.next_seg_id,
        manifest.up_to_seqno,
    ))
}

/// Body of [`ShardService::content_fingerprint`](crate::cluster::proto::shard_service_server::ShardService::content_fingerprint)
/// (ADR-097): the slot's order-independent live-set fingerprint — what the group move compares
/// to skip a provably-complete retained member's re-copy. Fingerprint-guarded like every
/// recovery RPC; reads through a fence (the caller's whole point is that the group is
/// write-quiesced when it asks).
pub(super) fn content_fingerprint(
    server: &ShardServer,
    request: Request<proto::ContentFingerprintRequest>,
) -> Result<Response<proto::ContentFingerprintReply>, Status> {
    let req = request.into_inner();
    let (_slot, st) = server.loaded_slot(req.shard_id)?;
    if req.dict_fingerprint != st.dict.fingerprint() {
        return Err(Status::failed_precondition(
            "ContentFingerprint dict-fingerprint mismatch (divergent feature space)",
        ));
    }
    if req.tag_dict_fingerprint != st.tag_dict.fingerprint() {
        return Err(Status::failed_precondition(
            "ContentFingerprint tag-dict-fingerprint mismatch (divergent tag space)",
        ));
    }
    // A refused fingerprint (a source-less/partial store) surfaces as failed_precondition: the
    // caller's skip check treats any error as "not provable" and falls back to the re-copy.
    let (fp_lo, fp_hi, live_count) = st
        .shard
        .content_fingerprint128()
        .map_err(|e| Status::failed_precondition(e.to_string()))?;
    Ok(Response::new(proto::ContentFingerprintReply {
        fp_lo,
        fp_hi,
        live_count,
    }))
}

#[cfg(test)]
mod tests {
    use super::validate_received;
    use crate::cluster::proto;
    use std::collections::HashSet;

    fn manifest(segs: &[&str], has_sources: bool) -> proto::FetchManifest {
        proto::FetchManifest {
            segment_files: segs.iter().map(|s| (*s).to_string()).collect(),
            next_seg_id: 1,
            dict_fingerprint: 0,
            has_sources,
            up_to_seqno: 0,
        }
    }

    fn received(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| (*s).to_string()).collect()
    }

    /// B3: a stream truncated after the final segment but before/while `sources.dat` —
    /// every segment present, `has_sources` advertised, but `sources.dat` missing — must
    /// error rather than attach a complete segment set and report success (which would
    /// silently lose the query SOURCES).
    #[test]
    fn missing_sources_when_advertised_is_an_error() {
        let m = manifest(&["seg-0.seg", "seg-1.seg"], true);
        let got = received(&["seg-0.seg", "seg-1.seg"]); // sources.dat NOT received
        let err = validate_received(&m, &got).expect_err("must reject a missing sources.dat");
        assert!(
            err.message().contains("sources.dat"),
            "error must name sources.dat: {}",
            err.message()
        );
    }

    /// The complete stream — every segment AND `sources.dat` received — succeeds.
    #[test]
    fn complete_stream_with_sources_succeeds() {
        let m = manifest(&["seg-0.seg"], true);
        let got = received(&["seg-0.seg", "sources.dat"]);
        assert!(validate_received(&m, &got).is_ok());
    }

    /// A source-less node (`has_sources == false`) succeeds without `sources.dat` — the
    /// byte-identical pre-sources path is not regressed by the new check.
    #[test]
    fn source_less_node_succeeds_without_sources() {
        let m = manifest(&["seg-0.seg", "seg-1.seg"], false);
        let got = received(&["seg-0.seg", "seg-1.seg"]);
        assert!(validate_received(&m, &got).is_ok());
    }

    /// A missing SEGMENT still errors (the pre-existing check is preserved).
    #[test]
    fn missing_segment_is_an_error() {
        let m = manifest(&["seg-0.seg", "seg-1.seg"], false);
        let got = received(&["seg-0.seg"]); // seg-1 missing
        let err = validate_received(&m, &got).expect_err("must reject a missing segment");
        assert!(
            err.message().contains("seg-1.seg"),
            "error must name the missing segment: {}",
            err.message()
        );
    }
}
