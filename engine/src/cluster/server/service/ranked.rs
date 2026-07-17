//! ADR-110 bounded top-K and winner-only source-fetch RPC bodies.

use std::collections::HashSet;
use std::time::{Duration, Instant};

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
    let rows = match state.shard.fetch_matches(&req.logical_ids, Some(deadline)) {
        Ok(rows) => rows,
        Err(error) => {
            if matches!(error, ShardError::DeadlineExceeded) {
                slot.ranked.record_cancellation();
            }
            return Err(read_status(&error));
        }
    };
    if rows.len() != req.logical_ids.len() {
        return Err(Status::internal(
            "source fetch did not return exactly one row per winner",
        ));
    }
    let mut messages = Vec::with_capacity(rows.len());
    let mut source_bytes = 0usize;
    for (expected, row) in req.logical_ids.into_iter().zip(rows) {
        if row.logical_id != expected {
            return Err(Status::internal(
                "source fetch returned rows out of request order",
            ));
        }
        source_bytes = source_bytes.saturating_add(row.source.len());
        let message = proto::FetchMatch {
            logical_id: row.logical_id,
            source: row.source,
            placement_generation: req.placement_generation,
            num_shards: req.num_shards,
        };
        if let Err(status) =
            server.check_result_bytes(reverse_rusty_shard_proto::encoded_len(&message))
        {
            slot.ranked.record_cap_rejection();
            return Err(status);
        }
        messages.push(Ok(message));
    }
    slot.ranked.record_fetch(source_bytes);
    slot.latency
        .observe(ShardRpc::FetchMatches, started.elapsed());
    let stream: super::ShardServiceStream = Box::pin(tokio_stream::iter(messages));
    Ok(Response::new(stream))
}

fn deadline_from_remaining(micros: u64) -> Result<Instant, Status> {
    if micros == 0 {
        return Err(Status::deadline_exceeded("request deadline exhausted"));
    }
    Instant::now()
        .checked_add(Duration::from_micros(micros))
        .ok_or_else(|| Status::invalid_argument("remaining deadline is out of range"))
}

fn read_status(error: &ShardError) -> Status {
    match error {
        ShardError::DeadlineExceeded => Status::deadline_exceeded(error.to_string()),
        ShardError::Admission(_) => Status::invalid_argument(error.to_string()),
        ShardError::OwnershipMismatch(_) | ShardError::Protocol(_) => {
            Status::failed_precondition(error.to_string())
        }
        ShardError::SourceUnavailable(_) => Status::not_found(error.to_string()),
        _ => Status::internal(error.to_string()),
    }
}
