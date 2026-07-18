//! ADR-112 bounded ranked title-batch RPC body.
//!
//! One unary request (per-title ownership contexts, one shared program/K/
//! threshold/deadline) → the columnar batch kernel via
//! `Shard::percolate_top_k_batch_owned` → a streamed reply of one bounded
//! title frame per request title IN ORDER plus exactly one trailing summary
//! frame (the client's completeness sentinel). Every frame obeys the
//! per-message result cap; a cap hit fails the whole call rather than
//! truncating (no-partial, ADR-110 precedent).

use std::time::Instant;

use tonic::{Request, Response, Status};

use crate::cluster::node_metrics::ShardRpc;
use crate::cluster::proto;
use crate::cluster::shard::{BatchTitleRequest, Shard, ShardError};
use crate::result::{
    QueryScope, TopKOptions, TotalHitsRelation, MAX_RANKED_BATCH_HEAP_ROWS,
    MAX_RANKED_BATCH_TITLES, MAX_TOP_K,
};

use super::super::ShardServer;
use super::ranked::{deadline_from_remaining, read_status};

pub(super) fn percolate_top_k_batch(
    server: &ShardServer,
    request: Request<proto::PercolateTopKBatchRequest>,
) -> Result<Response<super::BatchTopKStream>, Status> {
    let started = Instant::now();
    let req = request.into_inner();
    // Trust-but-verify admission (the coordinator pre-checks the same bounds).
    if req.titles.is_empty() {
        return Err(Status::invalid_argument(
            "batch top-k requires at least one title",
        ));
    }
    if req.titles.len() > MAX_RANKED_BATCH_TITLES {
        return Err(Status::invalid_argument(format!(
            "batch top-k accepts at most {MAX_RANKED_BATCH_TITLES} titles"
        )));
    }
    if req.size as usize > MAX_TOP_K {
        return Err(Status::invalid_argument(format!(
            "batch top-k size exceeds maximum {MAX_TOP_K}"
        )));
    }
    let requested_rows = u64::from(req.size).saturating_mul(req.titles.len() as u64);
    if requested_rows > MAX_RANKED_BATCH_HEAP_ROWS {
        return Err(Status::invalid_argument(format!(
            "batch top-k heap budget of {requested_rows} rows exceeds maximum \
             {MAX_RANKED_BATCH_HEAP_ROWS}"
        )));
    }
    let rank = req
        .rank
        .ok_or_else(|| Status::invalid_argument("bounded batch top-k requires a rank program"))?;
    let program = proto::rank_program_from_proto(rank);
    let pred = proto::tag_predicate_from_proto(req.filter);
    // Decode + node-validate EVERY context fail-closed: a mixed-generation
    // batch must refuse loudly here, never fall through to silent
    // no-owner-suppressed emissions.
    let mut entries = Vec::with_capacity(req.titles.len());
    for entry in req.titles {
        let context = proto::ownership_from_proto(entry.ownership)
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        server.validate_placement_config(context.generation(), context.num_shards())?;
        entries.push((entry.title, context));
    }
    let deadline = deadline_from_remaining(req.remaining_micros)?;
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
    let requests: Vec<BatchTitleRequest<'_>> = entries
        .iter()
        .map(|(title, context)| BatchTitleRequest { title, context })
        .collect();
    let ranked = match state.shard.percolate_top_k_batch_owned(
        &requests,
        req.include_broad,
        &pred,
        &program,
        options,
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
    let generation = entries[0].1.generation().get();
    let num_shards = entries[0].1.num_shards();
    let stats = ranked.stats;
    let titles_served = u32::try_from(ranked.titles.len()).unwrap_or(u32::MAX);

    // The whole bounded result is already in memory (≤ K × titles rows by the
    // admission bound); frames are cap-checked up front so a cap violation
    // fails the call instead of truncating a stream mid-flight.
    let mut frames: Vec<Result<proto::PercolateTopKBatchFrame, Status>> =
        Vec::with_capacity(ranked.titles.len() + 1);
    for (index, title) in ranked.titles.into_iter().enumerate() {
        let exact = title.total_hits.relation == TotalHitsRelation::Eq;
        let frame = proto::PercolateTopKBatchFrame {
            frame: Some(proto::percolate_top_k_batch_frame::Frame::Title(
                proto::PercolateTopKTitleResult {
                    title_index: u32::try_from(index).unwrap_or(u32::MAX),
                    hits: title
                        .hits
                        .into_iter()
                        .map(|hit| proto::RankedHit {
                            logical_id: hit.logical_id,
                            score: hit.score,
                        })
                        .collect(),
                    total_hits: Some(proto::total_hits_to_proto(title.total_hits)),
                    rank_stats: Some(proto::rank_stats_to_proto(title.rank_stats)),
                    bounded: true,
                    ownership_applied: true,
                    requested_size: req.size,
                    placement_generation: generation,
                    num_shards,
                },
            )),
        };
        let encoded = reverse_rusty_shard_proto::encoded_len(&frame);
        if let Err(status) = server.check_result_bytes(encoded) {
            slot.ranked.record_cap_rejection();
            return Err(status);
        }
        let hits = match &frame.frame {
            Some(proto::percolate_top_k_batch_frame::Frame::Title(result)) => result.hits.len(),
            _ => 0,
        };
        // Per-title accounting through the existing fixed-cardinality
        // counters: a batch of N titles records N rows/relations, exactly as N
        // single top-K calls would.
        slot.ranked.record_top_k(hits, encoded, exact);
        frames.push(Ok(frame));
    }
    let summary = proto::PercolateTopKBatchFrame {
        frame: Some(proto::percolate_top_k_batch_frame::Frame::Summary(
            proto::PercolateTopKBatchSummary {
                titles_served,
                stats: Some(proto::stats_from_engine(stats)),
                placement_generation: generation,
                num_shards,
            },
        )),
    };
    let encoded = reverse_rusty_shard_proto::encoded_len(&summary);
    if let Err(status) = server.check_result_bytes(encoded) {
        slot.ranked.record_cap_rejection();
        return Err(status);
    }
    frames.push(Ok(summary));
    slot.latency
        .observe(ShardRpc::PercolateTopKBatch, started.elapsed());
    slot.broad.record(&stats);
    Ok(Response::new(Box::pin(tokio_stream::iter(frames))))
}
