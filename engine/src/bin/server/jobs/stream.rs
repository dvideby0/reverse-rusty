//! Bounded NDJSON sink for exhaustive HTTP jobs.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use serde::Serialize;
use sha2::{Digest, Sha256};

use reverse_rusty::delivery::{
    ChunkSink, ChunkSinkError, DeliveryChecksum, ExhaustiveSummary, MatchChunk,
};

use crate::metrics::PrometheusMetrics;

/// One bounded-channel item. Completion is special: merely enqueueing its
/// bytes does not complete the job. The HTTP body must dequeue the frame and
/// call [`Self::into_bytes`], which atomically attests that the terminal record
/// left the bounded queue. Dropping it first invalidates the attestation.
pub(crate) struct JobFrame {
    bytes: Vec<u8>,
    completion: Option<CompletionState>,
}

impl JobFrame {
    fn data(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            completion: None,
        }
    }

    fn completion(bytes: Vec<u8>, completion: CompletionState) -> Self {
        Self {
            bytes,
            completion: Some(completion),
        }
    }

    /// Consume one dequeued frame. An invalidated completion is suppressed:
    /// cancellation, deadline, or disconnect won the race before delivery.
    pub(crate) fn into_bytes(mut self) -> Option<Vec<u8>> {
        if self
            .completion
            .as_ref()
            .is_some_and(|completion| !completion.deliver())
        {
            return None;
        }
        Some(std::mem::take(&mut self.bytes))
    }
}

impl Drop for JobFrame {
    fn drop(&mut self) {
        if let Some(completion) = &self.completion {
            completion.invalidate();
        }
    }
}

#[derive(Clone)]
pub(super) struct CompletionState {
    gate: Arc<Mutex<CompletionGate>>,
    cancel: Arc<AtomicBool>,
}

impl CompletionState {
    pub(super) fn new(cancel: Arc<AtomicBool>) -> Self {
        Self {
            gate: Arc::new(Mutex::new(CompletionGate {
                phase: CompletionPhase::Pending,
                deadline: None,
            })),
            cancel,
        }
    }

    pub(super) fn set_deadline(&self, deadline: Instant) {
        self.gate.lock().deadline = Some(deadline);
    }

    /// Linearize cancellation against terminal dequeue. Once delivery wins,
    /// the completion is committed and a later DELETE is a no-op. An earlier
    /// invalidation also wins permanently, preserving its original cause.
    pub(super) fn request_cancel(&self) -> bool {
        let mut gate = self.gate.lock();
        if gate.phase != CompletionPhase::Pending {
            return false;
        }
        // Expiry is an absolute event, not merely something the worker notices
        // on its next poll. If DELETE arrives after that instant, record the
        // earlier deadline transition before considering cancellation.
        match gate.deadline {
            Some(deadline) if Instant::now() >= deadline => {
                gate.phase = CompletionPhase::Invalid(CompletionInvalid::Deadline);
                return false;
            }
            None => {
                gate.phase = CompletionPhase::Invalid(CompletionInvalid::MissingDeadline);
                return false;
            }
            Some(_) => {}
        }
        let newly_cancelled = !self.cancel.swap(true, Ordering::AcqRel);
        gate.phase = CompletionPhase::Invalid(CompletionInvalid::Cancelled);
        newly_cancelled
    }

    fn is_cancelled(&self) -> bool {
        self.cancel.load(Ordering::Acquire)
    }

    fn deliver(&self) -> bool {
        let mut gate = self.gate.lock();
        if gate.phase != CompletionPhase::Pending {
            return false;
        }
        if self.cancel.load(Ordering::Acquire) {
            gate.phase = CompletionPhase::Invalid(CompletionInvalid::Cancelled);
            return false;
        }
        match gate.deadline {
            Some(deadline) if Instant::now() < deadline => {
                gate.phase = CompletionPhase::Delivered;
                true
            }
            Some(_) => {
                gate.phase = CompletionPhase::Invalid(CompletionInvalid::Deadline);
                false
            }
            None => {
                gate.phase = CompletionPhase::Invalid(CompletionInvalid::MissingDeadline);
                false
            }
        }
    }

    fn invalidate(&self) -> bool {
        let mut gate = self.gate.lock();
        if gate.phase != CompletionPhase::Pending {
            return false;
        }
        let reason = if self.cancel.load(Ordering::Acquire) {
            CompletionInvalid::Cancelled
        } else {
            match gate.deadline {
                Some(deadline) if Instant::now() >= deadline => CompletionInvalid::Deadline,
                None => CompletionInvalid::MissingDeadline,
                Some(_) => CompletionInvalid::NotConsumed,
            }
        };
        gate.phase = CompletionPhase::Invalid(reason);
        true
    }

    /// Check the wait condition and invalidate it under the same lock used by
    /// [`Self::deliver`]. A consumer can therefore never dequeue completion
    /// between observing cancellation/deadline and committing invalidation.
    fn delivery_status(&self, disconnected: bool) -> Result<bool, ChunkSinkError> {
        let mut gate = self.gate.lock();
        match gate.phase {
            CompletionPhase::Delivered => return Ok(true),
            CompletionPhase::Invalid(reason) => return Err(reason.error()),
            CompletionPhase::Pending => {}
        }
        let invalid = if self.cancel.load(Ordering::Acquire) {
            Some(CompletionInvalid::Cancelled)
        } else if gate
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            Some(CompletionInvalid::Deadline)
        } else if gate.deadline.is_none() {
            Some(CompletionInvalid::MissingDeadline)
        } else if disconnected {
            Some(CompletionInvalid::Disconnected)
        } else {
            None
        };
        if let Some(reason) = invalid {
            gate.phase = CompletionPhase::Invalid(reason);
            return Err(reason.error());
        }
        Ok(false)
    }

    /// Decide the externally visible job terminal state under the same gate
    /// that arbitrates DELETE, deadline invalidation, disconnect, and terminal
    /// dequeue. A stale cancellation load in the worker can therefore never
    /// overwrite a cancellation that won immediately before `JobState` is
    /// published (or vice versa).
    pub(super) fn resolve_terminal(&self, requested: TerminalRequest) -> TerminalResolution {
        let mut gate = self.gate.lock();
        match gate.phase {
            CompletionPhase::Delivered => TerminalResolution::Completed,
            CompletionPhase::Invalid(CompletionInvalid::Cancelled) => TerminalResolution::Cancelled,
            CompletionPhase::Invalid(reason) => {
                TerminalResolution::Failed(Some(reason.error().to_string()))
            }
            CompletionPhase::Pending => {
                // As in `request_cancel`, an already-elapsed absolute deadline
                // precedes whichever terminal condition the worker happens to
                // observe next.
                let timed_out = match gate.deadline {
                    Some(deadline) if Instant::now() >= deadline => {
                        Some(CompletionInvalid::Deadline)
                    }
                    None => Some(CompletionInvalid::MissingDeadline),
                    Some(_) => None,
                };
                if let Some(reason) = timed_out {
                    gate.phase = CompletionPhase::Invalid(reason);
                    return TerminalResolution::Failed(Some(reason.error().to_string()));
                }
                match requested {
                    TerminalRequest::Completed => {
                        gate.phase = CompletionPhase::Invalid(CompletionInvalid::NotConsumed);
                        TerminalResolution::Failed(Some(
                            CompletionInvalid::NotConsumed.error().to_string(),
                        ))
                    }
                    TerminalRequest::Cancelled => {
                        self.cancel.store(true, Ordering::Release);
                        gate.phase = CompletionPhase::Invalid(CompletionInvalid::Cancelled);
                        TerminalResolution::Cancelled
                    }
                    TerminalRequest::Failed => {
                        gate.phase = CompletionPhase::Invalid(CompletionInvalid::ExecutionFailed);
                        TerminalResolution::Failed(None)
                    }
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum TerminalRequest {
    Completed,
    Failed,
    Cancelled,
}

pub(super) enum TerminalResolution {
    Completed,
    Failed(Option<String>),
    Cancelled,
}

struct CompletionGate {
    phase: CompletionPhase,
    deadline: Option<Instant>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CompletionPhase {
    Pending,
    Delivered,
    Invalid(CompletionInvalid),
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CompletionInvalid {
    Cancelled,
    Deadline,
    Disconnected,
    NotConsumed,
    MissingDeadline,
    ExecutionFailed,
}

impl CompletionInvalid {
    fn error(self) -> ChunkSinkError {
        ChunkSinkError::new(match self {
            Self::Cancelled => "exhaustive job cancelled",
            Self::Deadline => "exhaustive job deadline exceeded",
            Self::Disconnected => "exhaustive stream consumer disconnected",
            Self::NotConsumed => "exhaustive completion frame was not consumed",
            Self::MissingDeadline => "exhaustive completion deadline was not initialized",
            Self::ExecutionFailed => "exhaustive job execution failed",
        })
    }
}

pub(super) struct JobChunkSink<'a> {
    tx: tokio::sync::mpsc::Sender<JobFrame>,
    job_id: &'a str,
    event_id: &'a str,
    snapshot_generation: u64,
    deadline: Instant,
    completion: CompletionState,
    prom: &'a PrometheusMetrics,
}

impl<'a> JobChunkSink<'a> {
    pub(super) fn new(
        tx: tokio::sync::mpsc::Sender<JobFrame>,
        job_id: &'a str,
        event_id: &'a str,
        snapshot_generation: u64,
        deadline: Instant,
        completion: CompletionState,
        prom: &'a PrometheusMetrics,
    ) -> Self {
        Self {
            tx,
            job_id,
            event_id,
            snapshot_generation,
            deadline,
            completion,
            prom,
        }
    }

    fn record_backpressure(&self, started: Instant, was_blocked: bool) {
        if was_blocked {
            self.prom
                .exhaustive_backpressure_seconds_total
                .inc_by(started.elapsed().as_secs_f64());
        }
    }

    fn check_active(&self) -> Result<(), ChunkSinkError> {
        if self.completion.is_cancelled() {
            return Err(ChunkSinkError::new("exhaustive job cancelled"));
        }
        if Instant::now() >= self.deadline {
            return Err(ChunkSinkError::new("exhaustive job deadline exceeded"));
        }
        if self.tx.is_closed() {
            return Err(ChunkSinkError::new(
                "exhaustive stream consumer disconnected",
            ));
        }
        Ok(())
    }

    fn send_payload(&self, mut payload: JobFrame, chunk: bool) -> Result<(), ChunkSinkError> {
        let blocked = Instant::now();
        let mut was_blocked = false;
        loop {
            if let Err(error) = self.check_active() {
                self.record_backpressure(blocked, was_blocked);
                return Err(error);
            }
            match self.tx.try_send(payload) {
                Ok(()) => {
                    self.record_backpressure(blocked, was_blocked);
                    if chunk {
                        self.prom.exhaustive_chunks_total.inc();
                    }
                    return Ok(());
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(returned)) => {
                    payload = returned;
                    was_blocked = true;
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    self.record_backpressure(blocked, was_blocked);
                    return Err(ChunkSinkError::new(
                        "exhaustive stream consumer disconnected",
                    ));
                }
            }
        }
    }

    pub(super) fn send_completion(&self, summary: ExhaustiveSummary) -> Result<(), ChunkSinkError> {
        let frame = CompletionFrame {
            kind: "completion",
            job_id: self.job_id,
            exact_total: summary.exact_total,
            snapshot_generation: self.snapshot_generation,
            chunk_count: summary.chunk_count,
            checksum: summary.checksum,
        };
        let bytes = encode_line(&frame)?;
        let len = bytes.len();
        let frame = JobFrame::completion(bytes, self.completion.clone());
        self.send_payload(frame, false)?;

        // Enqueue is not delivery. Stay bounded by the same cancellation and
        // deadline contract until the HTTP stream actually dequeues the
        // completion record. If the response is dropped while the record is
        // still queued, `JobFrame::drop` invalidates this state and the job
        // fails instead of publishing a false completed status.
        loop {
            if self.completion.delivery_status(self.tx.is_closed())? {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        self.prom.exhaustive_bytes_total.inc_by(len as u64);
        Ok(())
    }

    pub(super) fn send_failure_best_effort(&self, code: &'static str, detail: &str) {
        let frame = FailureFrame {
            kind: "failure",
            job_id: self.job_id,
            code,
            detail,
        };
        if let Ok(bytes) = encode_line(&frame) {
            let len = bytes.len();
            if self.tx.try_send(JobFrame::data(bytes)).is_ok() {
                self.prom.exhaustive_bytes_total.inc_by(len as u64);
            }
        }
    }
}

impl ChunkSink for JobChunkSink<'_> {
    fn send_chunk(&mut self, chunk: &MatchChunk) -> Result<(), ChunkSinkError> {
        let members: Vec<StreamMember> = chunk
            .matches
            .iter()
            .map(|member| StreamMember {
                logical_id: member.logical_id,
                score: member.score,
                idempotency_key: idempotency_key(
                    self.event_id,
                    self.snapshot_generation,
                    member.logical_id,
                ),
            })
            .collect();
        let frame = ChunkFrame {
            kind: "match_chunk",
            job_id: self.job_id,
            sequence: chunk.sequence,
            members,
        };
        let bytes = encode_line(&frame)?;
        let len = bytes.len();
        self.send_payload(JobFrame::data(bytes), true)?;
        self.prom.exhaustive_bytes_total.inc_by(len as u64);
        Ok(())
    }

    fn check_cancelled(&mut self) -> Result<(), ChunkSinkError> {
        self.check_active()
    }
}

#[derive(Serialize)]
struct StreamMember {
    logical_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    score: Option<i64>,
    idempotency_key: String,
}

#[derive(Serialize)]
struct ChunkFrame<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    job_id: &'a str,
    sequence: u64,
    members: Vec<StreamMember>,
}

#[derive(Serialize)]
struct CompletionFrame<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    job_id: &'a str,
    exact_total: u64,
    snapshot_generation: u64,
    chunk_count: u64,
    checksum: DeliveryChecksum,
}

#[derive(Serialize)]
struct FailureFrame<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    job_id: &'a str,
    code: &'static str,
    detail: &'a str,
}

fn encode_line(value: &impl Serialize) -> Result<Vec<u8>, ChunkSinkError> {
    let mut bytes = serde_json::to_vec(value)
        .map_err(|error| ChunkSinkError::new(format!("serializing job frame: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn idempotency_key(event_id: &str, snapshot_generation: u64, logical_id: u64) -> String {
    let mut hash = Sha256::new();
    hash.update(event_id.as_bytes());
    hash.update([0]);
    hash.update(snapshot_generation.to_le_bytes());
    hash.update(logical_id.to_le_bytes());
    let digest = hash.finalize();
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    fn completion(deadline: Instant) -> (CompletionState, JobFrame) {
        let cancel = Arc::new(AtomicBool::new(false));
        let completion = CompletionState::new(cancel);
        completion.set_deadline(deadline);
        let frame = JobFrame::completion(b"completion\n".to_vec(), completion.clone());
        (completion, frame)
    }

    #[test]
    fn cancellation_and_dequeue_share_one_terminal_transition() {
        let (completion, frame) = completion(Instant::now() + Duration::from_secs(1));
        assert!(completion.request_cancel());
        assert!(
            frame.into_bytes().is_none(),
            "a dequeue after cancellation must not expose completion bytes"
        );
        assert!(completion.delivery_status(false).is_err());
    }

    #[test]
    fn deadline_and_dequeue_share_one_terminal_transition() {
        let (completion, frame) = completion(Instant::now());
        assert!(
            frame.into_bytes().is_none(),
            "a dequeue after the deadline must not expose completion bytes"
        );
        assert!(completion.delivery_status(false).is_err());
    }

    #[test]
    fn deadline_expiry_precedes_a_later_cancellation_request() {
        let (completion, _frame) = completion(Instant::now());

        assert!(
            !completion.request_cancel(),
            "DELETE after the absolute deadline is not a new cancellation"
        );
        assert!(!completion.is_cancelled());
        match completion.resolve_terminal(TerminalRequest::Cancelled) {
            TerminalResolution::Failed(Some(detail)) => {
                assert!(detail.contains("deadline exceeded"), "{detail}");
            }
            _ => panic!("the earlier deadline transition must remain the terminal cause"),
        }
    }

    #[test]
    fn cancellation_does_not_overwrite_an_earlier_invalidation() {
        let (completion, frame) = completion(Instant::now() + Duration::from_secs(1));
        drop(frame);

        assert!(
            !completion.request_cancel(),
            "an already-invalid completion is not newly cancelled"
        );
        assert!(!completion.is_cancelled());
        let error = completion
            .delivery_status(false)
            .expect_err("the dropped completion must remain invalid");
        assert!(
            error.to_string().contains("not consumed"),
            "the first invalidation cause was overwritten: {error}"
        );
    }

    #[test]
    fn accepted_cancellation_controls_the_published_terminal_state() {
        let (completion, _frame) = completion(Instant::now() + Duration::from_secs(1));
        assert!(completion.request_cancel());
        assert!(matches!(
            completion.resolve_terminal(TerminalRequest::Failed),
            TerminalResolution::Cancelled
        ));
    }

    #[test]
    fn worker_failure_controls_a_later_cancellation_attempt() {
        let (completion, _frame) = completion(Instant::now() + Duration::from_secs(1));
        assert!(matches!(
            completion.resolve_terminal(TerminalRequest::Failed),
            TerminalResolution::Failed(None)
        ));
        assert!(
            !completion.request_cancel(),
            "DELETE must not replace the first terminal transition"
        );
    }
}
