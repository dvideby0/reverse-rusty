//! ADR-114 bounded exhaustive shard stream.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::cluster::node_metrics::ShardRpc;
use crate::cluster::proto;
use crate::cluster::shard::{Shard, ShardError};
use crate::delivery::{ChunkSink, ChunkSinkError, MatchChunk, MAX_MATCH_CHUNK_SIZE};

use super::super::{ShardServer, ShardSlot};
use super::ranked::{deadline_from_remaining, read_status};

struct GrpcChunkSink {
    tx: tokio::sync::mpsc::Sender<Result<proto::PercolateAllFrame, Status>>,
    max_result_bytes: usize,
    slot: Arc<ShardSlot>,
    deadline: Instant,
    terminal_status: Option<Status>,
}

type ExhaustiveFrame = Result<proto::PercolateAllFrame, Status>;

struct QueuedAdmission {
    tx: tokio::sync::mpsc::Sender<ExhaustiveFrame>,
}

type SharedAdmission = Arc<Mutex<Option<QueuedAdmission>>>;

fn take_admission(admission: &SharedAdmission) -> Option<QueuedAdmission> {
    admission
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take()
}

async fn watch_queued_admission(
    admission: SharedAdmission,
    closed_tx: tokio::sync::mpsc::Sender<ExhaustiveFrame>,
    worker_started: tokio::sync::oneshot::Receiver<()>,
    deadline: Instant,
) {
    tokio::select! {
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            if let Some(expired) = take_admission(&admission) {
                drop(expired.tx.try_send(Err(Status::deadline_exceeded(
                    "request deadline exhausted before an exhaustive worker started",
                ))));
            }
        }
        () = closed_tx.closed() => {
            drop(take_admission(&admission));
        }
        _ = worker_started => {
            // The worker now owns the sole response sender. Dropping this
            // watcher's clone is load-bearing: otherwise a successful stream
            // would remain open until its deadline after sending the summary.
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BoundedSendError {
    Deadline,
    Closed,
}

fn send_before<T>(
    tx: &tokio::sync::mpsc::Sender<T>,
    mut pending: T,
    deadline: Instant,
) -> Result<(), BoundedSendError> {
    loop {
        if Instant::now() >= deadline {
            return Err(BoundedSendError::Deadline);
        }
        match tx.try_send(pending) {
            Ok(()) => return Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Full(returned)) => {
                pending = returned;
                let remaining = deadline
                    .checked_duration_since(Instant::now())
                    .unwrap_or_default();
                if remaining.is_zero() {
                    return Err(BoundedSendError::Deadline);
                }
                std::thread::sleep(remaining.min(Duration::from_millis(1)));
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                return Err(BoundedSendError::Closed);
            }
        }
    }
}

impl GrpcChunkSink {
    fn check_active(&mut self) -> Result<(), ChunkSinkError> {
        if Instant::now() >= self.deadline {
            self.terminal_status
                .get_or_insert_with(|| Status::deadline_exceeded("request deadline exhausted"));
            return Err(ChunkSinkError::new(
                "exhaustive gRPC request deadline exceeded",
            ));
        }
        if self.tx.is_closed() {
            return Err(ChunkSinkError::new("exhaustive gRPC receiver disconnected"));
        }
        Ok(())
    }

    fn send_frame(&mut self, frame: proto::PercolateAllFrame) -> Result<(), ChunkSinkError> {
        let encoded = reverse_rusty_shard_proto::encoded_len(&frame);
        if encoded > self.max_result_bytes {
            self.slot.ranked.record_cap_rejection();
            let status = Status::resource_exhausted(format!(
                "encoded result is {encoded} bytes; configured maximum is {}",
                self.max_result_bytes
            ));
            self.terminal_status = Some(status);
            return Err(ChunkSinkError::new(
                "exhaustive frame exceeds the configured gRPC result cap",
            ));
        }
        // A stalled HTTP/2 consumer must not pin this blocking worker beyond
        // the request deadline.
        match send_before(&self.tx, Ok(frame), self.deadline) {
            Ok(()) => Ok(()),
            Err(BoundedSendError::Deadline) => {
                self.terminal_status
                    .get_or_insert_with(|| Status::deadline_exceeded("request deadline exhausted"));
                Err(ChunkSinkError::new(
                    "exhaustive gRPC request deadline exceeded",
                ))
            }
            Err(BoundedSendError::Closed) => {
                Err(ChunkSinkError::new("exhaustive gRPC receiver disconnected"))
            }
        }
    }
}

impl ChunkSink for GrpcChunkSink {
    fn send_chunk(&mut self, chunk: &MatchChunk) -> Result<(), ChunkSinkError> {
        self.send_frame(proto::PercolateAllFrame {
            frame: Some(proto::percolate_all_frame::Frame::Chunk(
                proto::ExhaustiveChunk {
                    sequence: chunk.sequence,
                    matches: chunk
                        .matches
                        .iter()
                        .map(|member| proto::ExhaustiveHit {
                            logical_id: member.logical_id,
                            score: member.score.unwrap_or_default(),
                            has_score: member.score.is_some(),
                        })
                        .collect(),
                },
            )),
        })
    }

    fn check_cancelled(&mut self) -> Result<(), ChunkSinkError> {
        self.check_active()
    }
}

pub(super) fn percolate_all(
    server: &ShardServer,
    request: Request<proto::PercolateAllRequest>,
) -> Result<Response<super::ExhaustiveStream>, Status> {
    let started = Instant::now();
    let req = request.into_inner();
    let ownership = proto::ownership_from_proto(req.ownership)
        .map_err(|error| Status::failed_precondition(error.to_string()))?;
    server.validate_placement_config(ownership.generation(), ownership.num_shards())?;
    validate_broad_owner(req.include_broad, req.shard_id, &ownership)?;
    let chunk_size = usize::try_from(req.chunk_size)
        .map_err(|_| Status::invalid_argument("chunk_size is out of range"))?;
    if chunk_size == 0 || chunk_size > MAX_MATCH_CHUNK_SIZE {
        return Err(Status::invalid_argument(format!(
            "chunk_size must be within 1..={MAX_MATCH_CHUNK_SIZE}"
        )));
    }
    let requested_duration = Duration::from_micros(req.remaining_micros);
    if requested_duration > server.max_exhaustive_stream_duration {
        return Err(Status::invalid_argument(format!(
            "remaining exhaustive deadline exceeds this shard's {} second maximum",
            server.max_exhaustive_stream_duration.as_secs_f64()
        )));
    }
    let deadline = deadline_from_remaining(req.remaining_micros)?;
    let pred = proto::tag_predicate_from_proto(req.filter);
    let program = req.rank.map(proto::rank_program_from_proto);
    let (slot, state) = server.loaded_slot(req.shard_id)?;
    let max_result_bytes = server.max_grpc_result_bytes;
    let placement_generation = ownership.generation().get();
    let num_shards = ownership.num_shards();
    let shard_id = req.shard_id;
    let title = req.title;
    let include_broad = req.include_broad;
    // This admission lives on the shard node, not only at the HTTP
    // coordinator. Direct callers and multiple coordinators therefore cannot
    // create an unbounded number of long-lived `spawn_blocking` matchers.
    let permit = Arc::clone(&server.exhaustive_permits)
        .try_acquire_owned()
        .map_err(|_| {
            Status::resource_exhausted("all exhaustive shard-stream workers are in use")
        })?;

    // One small bounded queue is the only fan-out buffer. Deadline-bounded
    // try_send polling propagates tonic/HTTP2 demand to the synchronous matcher
    // without letting a stalled client pin the blocking worker indefinitely.
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    // Keep the only worker sender in revocable shared state until the blocking
    // closure starts. The permit itself stays captured by that closure until
    // Tokio schedules it: even if the request expires first, the dormant
    // closure continues to occupy one of this node's bounded queue slots.
    // Releasing and reusing the permit here would let callers enqueue an
    // unbounded number of expired closures in Tokio's global blocking pool.
    let closed_tx = tx.clone();
    let admission = Arc::new(Mutex::new(Some(QueuedAdmission { tx })));
    let worker_admission = Arc::clone(&admission);
    let expiry_admission = Arc::clone(&admission);
    let (worker_started_tx, worker_started_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(watch_queued_admission(
        expiry_admission,
        closed_tx,
        worker_started_rx,
        deadline,
    ));
    tokio::task::spawn_blocking(move || {
        // Load-bearing queue bound: do not drop this permit until the queued
        // closure has actually begun executing.
        let _permit = permit;
        let _watcher_finished_before_start = worker_started_tx.send(()).is_err();
        let Some(QueuedAdmission { tx }) = take_admission(&worker_admission) else {
            return;
        };
        if Instant::now() >= deadline {
            drop(tx.try_send(Err(Status::deadline_exceeded(
                "request deadline exhausted before an exhaustive worker started",
            ))));
            return;
        }
        let mut sink = GrpcChunkSink {
            tx,
            max_result_bytes,
            slot: Arc::clone(&slot),
            deadline,
            terminal_status: None,
        };
        let result = state.shard.percolate_all_owned(
            &title,
            include_broad,
            &pred,
            program.as_ref(),
            chunk_size,
            &ownership,
            shard_id,
            Some(deadline),
            &mut sink,
        );
        match result {
            Ok(result) => {
                let frame = proto::PercolateAllFrame {
                    frame: Some(proto::percolate_all_frame::Frame::Summary(
                        proto::ExhaustiveSummary {
                            exact_total: result.summary.exact_total,
                            chunk_count: result.summary.chunk_count,
                            checksum_xor: result.summary.checksum.xor,
                            checksum_sum: result.summary.checksum.sum,
                            stats: Some(proto::stats_from_engine(result.stats)),
                            ownership_applied: true,
                            placement_generation,
                            num_shards,
                        },
                    )),
                };
                if let Ok(()) = sink.send_frame(frame) {
                    slot.latency
                        .observe(ShardRpc::PercolateAll, started.elapsed());
                    slot.broad.record(&result.stats);
                } else {
                    let status = sink.terminal_status.take().unwrap_or_else(|| {
                        Status::internal("sending exhaustive completeness summary failed")
                    });
                    drop(sink.tx.try_send(Err(status)));
                }
            }
            Err(error) => {
                let status = sink
                    .terminal_status
                    .take()
                    .unwrap_or_else(|| read_status(&error));
                if matches!(error, ShardError::DeadlineExceeded)
                    || status.code() == tonic::Code::DeadlineExceeded
                {
                    slot.ranked.record_cancellation();
                }
                drop(sink.tx.try_send(Err(status)));
            }
        }
    });
    Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
}

fn validate_broad_owner(
    include_broad: bool,
    shard_id: u32,
    ownership: &crate::ownership::OwnershipContext,
) -> Result<(), Status> {
    if include_broad && ownership.broad_evaluator() != Some(shard_id) {
        return Err(Status::failed_precondition(format!(
            "include_broad requires shard {shard_id} to be the ownership context's broad evaluator"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_channel_stops_at_deadline() {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        tx.try_send(1u8).expect("prefill");
        let started = Instant::now();
        let result = send_before(&tx, 2u8, started + Duration::from_millis(20));
        assert_eq!(result, Err(BoundedSendError::Deadline));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "bounded send remained blocked for {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn broad_stream_requires_this_shard_to_be_the_broad_owner() {
        let missing = crate::ownership::OwnershipContext::new(
            crate::ownership::PlacementGeneration(1),
            2,
            vec![0, 1],
            None,
        )
        .expect("valid standard-scope ownership");
        let wrong = crate::ownership::OwnershipContext::new(
            crate::ownership::PlacementGeneration(1),
            2,
            vec![0, 1],
            Some(1),
        )
        .expect("valid broad ownership");

        assert_eq!(
            validate_broad_owner(true, 0, &missing)
                .expect_err("missing evaluator must fail")
                .code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            validate_broad_owner(true, 0, &wrong)
                .expect_err("different evaluator must fail")
                .code(),
            tonic::Code::FailedPrecondition
        );
        validate_broad_owner(true, 1, &wrong).expect("the evaluator may run the broad lane");
        validate_broad_owner(false, 0, &missing).expect("standard scope needs no evaluator");
    }

    #[tokio::test]
    async fn expired_submission_retains_its_queue_slot_until_scheduled() {
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = Arc::clone(&permits)
            .try_acquire_owned()
            .expect("one permit is available");
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let admission = Arc::new(Mutex::new(Some(QueuedAdmission { tx })));

        drop(take_admission(&admission));

        assert!(
            take_admission(&admission).is_none(),
            "a later blocking worker must observe revoked admission"
        );
        assert_eq!(
            permits.available_permits(),
            0,
            "revocation must not make room for another dormant blocking closure"
        );
        drop(permit);
        assert_eq!(permits.available_permits(), 1);
    }

    #[tokio::test]
    async fn admission_watcher_drops_its_sender_when_the_worker_starts() {
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = Arc::clone(&permits)
            .try_acquire_owned()
            .expect("one permit is available");
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let closed_tx = tx.clone();
        let admission = Arc::new(Mutex::new(Some(QueuedAdmission { tx })));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(watch_queued_admission(
            Arc::clone(&admission),
            closed_tx,
            started_rx,
            Instant::now() + Duration::from_secs(1),
        ));

        let worker = take_admission(&admission).expect("worker takes admission");
        started_tx.send(()).expect("watcher is still waiting");
        drop(worker);
        drop(permit);

        assert!(
            tokio::time::timeout(Duration::from_millis(100), rx.recv())
                .await
                .expect("watcher must not keep a completed stream open")
                .is_none(),
            "all response senders must be gone"
        );
    }
}
