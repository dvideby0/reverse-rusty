//! ADR-110 bounded top-K and winner-only source-fetch RPC bodies.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::cluster::node_metrics::ShardRpc;
use crate::cluster::proto;
use crate::cluster::shard::{Shard, ShardError};
use crate::result::{QueryScope, TopKOptions, MAX_TOP_K};

use super::super::ShardServer;

pub(super) fn percolate_top_k(
    server: &ShardServer,
    request: Request<proto::PercolateTopKRequest>,
) -> Result<Response<proto::PercolateTopKReply>, Status> {
    let started = Instant::now();
    let req = request.into_inner();
    let ownership = proto::ownership_from_proto(req.ownership)
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    server.validate_placement_config(ownership.generation(), ownership.num_shards())?;
    let deadline = deadline_from_remaining(req.remaining_micros)?;
    let rank = req
        .rank
        .ok_or_else(|| Status::invalid_argument("bounded top-k requires a rank program"))?;
    let program = proto::rank_program_from_proto(rank);
    let pred = proto::tag_predicate_from_proto(req.filter);
    let (slot, state) = server.loaded_slot(req.shard_id)?;
    let options = TopKOptions {
        size: req.size as usize,
        track_total_hits_up_to: req.track_total_hits_up_to,
        query_scope: if req.include_broad {
            QueryScope::WithBroad
        } else {
            QueryScope::Standard
        },
    };
    let ranked = match state.shard.percolate_top_k_owned(
        &req.title,
        req.include_broad,
        &pred,
        &program,
        options,
        &ownership,
        req.shard_id,
        Some(deadline),
    ) {
        Ok(ranked) => ranked,
        Err(error) => {
            if matches!(error, ShardError::DeadlineExceeded) {
                slot.ranked.record_cancellation();
            }
            return Err(read_status(&error));
        }
    };
    let reply = proto::PercolateTopKReply {
        hits: ranked
            .hits
            .into_iter()
            .map(|hit| proto::RankedHit {
                logical_id: hit.logical_id,
                score: hit.score,
            })
            .collect(),
        total_hits: Some(proto::total_hits_to_proto(ranked.total_hits)),
        stats: Some(proto::stats_from_engine(ranked.stats)),
        rank_stats: Some(proto::rank_stats_to_proto(ranked.rank_stats)),
        bounded: true,
        ownership_applied: true,
        requested_size: req.size,
        placement_generation: ownership.generation().get(),
        num_shards: ownership.num_shards(),
    };
    let encoded = reverse_rusty_shard_proto::encoded_len(&reply);
    if let Err(status) = server.check_result_bytes(encoded) {
        slot.ranked.record_cap_rejection();
        return Err(status);
    }
    slot.ranked.record_top_k(
        reply.hits.len(),
        encoded,
        ranked.total_hits.relation == crate::result::TotalHitsRelation::Eq,
    );
    slot.latency
        .observe(ShardRpc::PercolateTopK, started.elapsed());
    slot.broad.record(&ranked.stats);
    Ok(Response::new(reply))
}

pub(super) fn fetch_matches(
    server: &ShardServer,
    request: Request<proto::FetchMatchesRequest>,
) -> Result<Response<super::ShardServiceStream>, Status> {
    let started = Instant::now();
    let req = request.into_inner();
    server.validate_placement_config(
        crate::ownership::PlacementGeneration(req.placement_generation),
        req.num_shards,
    )?;
    if req.logical_ids.len() > MAX_TOP_K {
        return Err(Status::invalid_argument(format!(
            "fetch_matches accepts at most {MAX_TOP_K} winner ids"
        )));
    }
    let mut seen = HashSet::with_capacity(req.logical_ids.len());
    if req.logical_ids.iter().any(|&id| !seen.insert(id)) {
        return Err(Status::invalid_argument(
            "fetch_matches winner ids must be unique",
        ));
    }
    let deadline = deadline_from_remaining(req.remaining_micros)?;
    let (slot, state) = server.loaded_slot(req.shard_id)?;
    let max_source_bytes = usize::try_from(req.max_source_bytes)
        .map_err(|_| Status::invalid_argument("max_source_bytes is out of range"))?;
    let max_result_bytes = server.max_grpc_result_bytes;
    let placement_generation = req.placement_generation;
    let num_shards = req.num_shards;
    let logical_ids = req.logical_ids;
    // Retain one lock-free current-view snapshot for the whole stream. A write
    // published during delivery must not mix source generations within a group.
    let snapshot = state.shard.metrics_snapshot();

    // A small bounded channel makes tonic/HTTP2 flow control effective: at most
    // eight source messages are queued while a slow coordinator drains the stream.
    // Sources are looked up one at a time with the request's remaining byte credit,
    // so neither a Vec<FetchedMatch> nor a Vec<FetchMatch> materializes the response.
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tokio::task::spawn_blocking(move || {
        let mut remaining = max_source_bytes;
        let mut source_bytes = 0usize;
        for logical_id in logical_ids {
            // The shared credit step (deadline → bounded lookup → decrement) is
            // the same kernel LocalShard::fetch_matches drains in-process.
            let source = match crate::cluster::shard::fetch_source_step(
                &snapshot,
                logical_id,
                &mut remaining,
                max_source_bytes,
                Some(deadline),
            ) {
                Ok(source) => source,
                Err(error) => {
                    if matches!(error, ShardError::DeadlineExceeded) {
                        slot.ranked.record_cancellation();
                    }
                    drop(tx.blocking_send(Err(read_status(&error))));
                    return;
                }
            };
            let bytes = source.len();
            source_bytes = source_bytes.saturating_add(bytes);
            let message = proto::FetchMatch {
                logical_id,
                source,
                placement_generation,
                num_shards,
            };
            let encoded = reverse_rusty_shard_proto::encoded_len(&message);
            if encoded > max_result_bytes {
                slot.ranked.record_cap_rejection();
                drop(tx.blocking_send(Err(Status::resource_exhausted(format!(
                    "encoded result is {encoded} bytes; configured maximum is {max_result_bytes}"
                )))));
                return;
            }
            if tx.blocking_send(Ok(message)).is_err() {
                return;
            }
        }
        slot.ranked.record_fetch(source_bytes);
        slot.latency
            .observe(ShardRpc::FetchMatches, started.elapsed());
    });
    Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
}

fn deadline_from_remaining(micros: u64) -> Result<Instant, Status> {
    if micros == 0 {
        return Err(Status::deadline_exceeded("request deadline exhausted"));
    }
    Instant::now()
        .checked_add(Duration::from_micros(micros))
        .ok_or_else(|| Status::invalid_argument("remaining deadline is out of range"))
}

// The message strings below are a frozen cross-version contract: a pre-ADR-111
// client reconstructs typed errors from them (`ranked_rpc_err`'s substring
// fallback). The ADR-111 metadata codes ride alongside, never instead.
fn read_status(error: &ShardError) -> Status {
    use crate::cluster::ranked_wire::{attach, RankedWireCode};
    match error {
        ShardError::DeadlineExceeded => Status::deadline_exceeded(error.to_string()),
        ShardError::Admission(_) => Status::invalid_argument(error.to_string()),
        ShardError::OwnershipMismatch(_) => attach(
            Status::failed_precondition(error.to_string()),
            RankedWireCode::OwnershipMismatch,
            None,
        ),
        ShardError::Protocol(_) => Status::failed_precondition(error.to_string()),
        ShardError::SourceUnavailable(logical) => attach(
            Status::not_found(error.to_string()),
            RankedWireCode::SourceUnavailable,
            Some(*logical),
        ),
        ShardError::EnrichmentLimit { limit } => attach(
            Status::resource_exhausted(
                "ranked enrichment byte credit exhausted before source materialization",
            ),
            RankedWireCode::EnrichmentLimit,
            Some(u64::try_from(*limit).unwrap_or(u64::MAX)),
        ),
        _ => Status::internal(error.to_string()),
    }
}
