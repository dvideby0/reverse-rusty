//! ADR-114 in-memory exhaustive-job orchestration.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use serde::Serialize;

use reverse_rusty::delivery::{ChunkSink, DeliveryChecksum, ExhaustiveSummary};
use reverse_rusty::QueryScope;

use crate::metrics::PrometheusMetrics;

mod stream;

pub(crate) use stream::JobFrame;
use stream::{CompletionState, JobChunkSink, TerminalRequest, TerminalResolution};

#[derive(Clone, Copy)]
pub(crate) struct ExhaustiveJobConfig {
    pub(crate) threads: usize,
    pub(crate) max_concurrent: usize,
    pub(crate) chunk_size: usize,
    pub(crate) channel_depth: usize,
    pub(crate) max_timeout: Duration,
    pub(crate) max_retained: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum JobPhase {
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl JobPhase {
    fn label(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    fn terminal(self) -> bool {
        self != Self::Running
    }
}

#[derive(Clone, Serialize)]
pub(crate) struct JobView {
    pub(crate) job_id: String,
    pub(crate) event_id: String,
    pub(crate) state: JobPhase,
    pub(crate) query_scope: QueryScope,
    pub(crate) snapshot_generation: u64,
    pub(crate) created_unix_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) completed_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) exact_total: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) chunk_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) checksum: Option<DeliveryChecksum>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) failure: Option<String>,
}

struct JobState {
    phase: JobPhase,
    completed_unix_ms: Option<u64>,
    summary: Option<ExhaustiveSummary>,
    failure: Option<String>,
}

pub(crate) struct JobRecord {
    id: String,
    event_id: String,
    request_fingerprint: [u8; 32],
    query_scope: QueryScope,
    snapshot_generation: u64,
    created_unix_ms: u64,
    sequence: u64,
    state: Mutex<JobState>,
    cancel: Arc<AtomicBool>,
    completion: CompletionState,
    receiver: Mutex<Option<tokio::sync::mpsc::Receiver<JobFrame>>>,
}

impl JobRecord {
    fn view(&self) -> JobView {
        let state = self.state.lock();
        JobView {
            job_id: self.id.clone(),
            event_id: self.event_id.clone(),
            state: state.phase,
            query_scope: self.query_scope,
            snapshot_generation: self.snapshot_generation,
            created_unix_ms: self.created_unix_ms,
            completed_unix_ms: state.completed_unix_ms,
            exact_total: state.summary.map(|summary| summary.exact_total),
            chunk_count: state.summary.map(|summary| summary.chunk_count),
            checksum: state.summary.map(|summary| summary.checksum),
            failure: state.failure.clone(),
        }
    }
}

#[derive(Default)]
struct Registry {
    jobs: HashMap<String, Arc<JobRecord>>,
    by_event: HashMap<String, String>,
    next_sequence: u64,
}

pub(crate) struct ExhaustiveJobs {
    config: ExhaustiveJobConfig,
    pool: rayon::ThreadPool,
    permits: Arc<tokio::sync::Semaphore>,
    registry: Mutex<Registry>,
    next_generation: AtomicU64,
    prom: PrometheusMetrics,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum StartError {
    Busy,
    Capacity,
    EventConflict,
    InvalidTimeout,
}

pub(crate) struct StartOutcome {
    pub(crate) job: JobView,
    pub(crate) reused: bool,
}

#[derive(Debug)]
pub(crate) enum StreamError {
    NotFound,
    AlreadyTaken,
}

struct JobPermit {
    _permit: tokio::sync::OwnedSemaphorePermit,
    gauge: prometheus::IntGauge,
}

impl Drop for JobPermit {
    fn drop(&mut self) {
        self.gauge.dec();
    }
}

impl ExhaustiveJobs {
    pub(crate) fn new(
        config: ExhaustiveJobConfig,
        prom: PrometheusMetrics,
    ) -> Result<Arc<Self>, String> {
        if config.max_concurrent > tokio::sync::Semaphore::MAX_PERMITS
            || config.channel_depth > tokio::sync::Semaphore::MAX_PERMITS
        {
            return Err(format!(
                "exhaustive job concurrency and channel depth must not exceed Tokio's {} permit \
                 maximum",
                tokio::sync::Semaphore::MAX_PERMITS
            ));
        }
        if config.threads == 0
            || config.max_concurrent == 0
            || config.max_concurrent > config.threads
            || config.chunk_size == 0
            || config.chunk_size > reverse_rusty::delivery::MAX_MATCH_CHUNK_SIZE
            || config.channel_depth == 0
            || config.max_timeout.is_zero()
            || config.max_retained == 0
        {
            return Err(
                "exhaustive job threads, concurrency, chunk/channel sizes, timeout, and retention \
                 must be non-zero; concurrency must not exceed worker threads; chunk size must \
                 also fit the engine maximum"
                    .into(),
            );
        }
        if Instant::now().checked_add(config.max_timeout).is_none() {
            return Err("exhaustive job timeout is outside the platform Instant range".to_string());
        }
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(config.threads)
            .thread_name(|index| format!("rr-exhaustive-{index}"))
            .build()
            .map_err(|error| format!("building exhaustive worker pool: {error}"))?;
        Ok(Arc::new(Self {
            config,
            pool,
            permits: Arc::new(tokio::sync::Semaphore::new(config.max_concurrent)),
            registry: Mutex::new(Registry::default()),
            // A random boot namespace followed by monotonic allocation keeps
            // `(event_id, snapshot_generation, logical_id)` idempotency keys
            // distinct when an in-memory job is retried after process restart.
            // Starting at 1 reused the same keys for a potentially different
            // captured view on every fresh server.
            next_generation: AtomicU64::new(generation_seed()),
            prom,
        }))
    }

    #[cfg(test)]
    pub(crate) fn for_tests(prom: PrometheusMetrics) -> Arc<Self> {
        Self::new(
            ExhaustiveJobConfig {
                threads: 1,
                max_concurrent: 1,
                chunk_size: 2,
                channel_depth: 8,
                max_timeout: Duration::from_secs(5),
                max_retained: 32,
            },
            prom,
        )
        .expect("test exhaustive manager")
    }

    pub(crate) fn chunk_size(&self) -> usize {
        self.config.chunk_size
    }

    pub(crate) fn bounded_timeout(&self, requested: Option<Duration>) -> Result<Duration, ()> {
        match requested {
            Some(timeout) if timeout.is_zero() || timeout > self.config.max_timeout => Err(()),
            Some(timeout) => Ok(timeout),
            None => Ok(self.config.max_timeout),
        }
    }

    pub(crate) fn start<F>(
        self: &Arc<Self>,
        event_id: String,
        request_fingerprint: [u8; 32],
        query_scope: QueryScope,
        timeout: Duration,
        execute: F,
    ) -> Result<StartOutcome, StartError>
    where
        F: FnOnce(&mut dyn ChunkSink, Instant) -> Result<ExhaustiveSummary, String>
            + Send
            + 'static,
    {
        let mut registry = self.registry.lock();
        if let Some(id) = registry.by_event.get(&event_id) {
            let record = registry
                .jobs
                .get(id)
                .expect("event index must reference a retained job");
            if record.request_fingerprint != request_fingerprint {
                return Err(StartError::EventConflict);
            }
            return Ok(StartOutcome {
                job: record.view(),
                reused: true,
            });
        }
        // Admission is transactional with respect to retained history: a Busy
        // request must not evict a terminal job when it cannot claim an
        // execution permit and therefore admits no replacement.
        let permit = Arc::clone(&self.permits)
            .try_acquire_owned()
            .map_err(|_| StartError::Busy)?;
        if timeout.is_zero() || timeout > self.config.max_timeout {
            return Err(StartError::InvalidTimeout);
        }
        // Arm the one absolute deadline at successful admission, before any
        // registry work or Rayon scheduling. Dedicated-pool queue time is part
        // of the advertised maximum job lifetime.
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(StartError::InvalidTimeout)?;
        self.prune_for_capacity(&mut registry)?;
        self.prom.exhaustive_permits_in_use.inc();
        let permit = JobPermit {
            _permit: permit,
            gauge: self.prom.exhaustive_permits_in_use.clone(),
        };

        let snapshot_generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        let id = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = tokio::sync::mpsc::channel(self.config.channel_depth);
        let sequence = registry.next_sequence;
        registry.next_sequence = registry.next_sequence.saturating_add(1);
        let cancel = Arc::new(AtomicBool::new(false));
        let completion = CompletionState::new(Arc::clone(&cancel));
        completion.set_deadline(deadline);
        let record = Arc::new(JobRecord {
            id: id.clone(),
            event_id: event_id.clone(),
            request_fingerprint,
            query_scope,
            snapshot_generation,
            created_unix_ms: unix_ms(),
            sequence,
            state: Mutex::new(JobState {
                phase: JobPhase::Running,
                completed_unix_ms: None,
                summary: None,
                failure: None,
            }),
            cancel,
            completion,
            receiver: Mutex::new(Some(rx)),
        });
        registry.by_event.insert(event_id, id.clone());
        registry.jobs.insert(id, Arc::clone(&record));
        self.prom
            .exhaustive_jobs
            .with_label_values(&[JobPhase::Running.label()])
            .inc();
        let initial = record.view();
        drop(registry);

        let manager = Arc::clone(self);
        self.pool.spawn(move || {
            manager.run_job(&record, tx, deadline, permit, execute);
        });
        Ok(StartOutcome {
            job: initial,
            reused: false,
        })
    }

    fn run_job<F>(
        &self,
        record: &JobRecord,
        tx: tokio::sync::mpsc::Sender<JobFrame>,
        deadline: Instant,
        permit: JobPermit,
        execute: F,
    ) where
        F: FnOnce(&mut dyn ChunkSink, Instant) -> Result<ExhaustiveSummary, String>,
    {
        let mut sink = JobChunkSink::new(
            tx,
            &record.id,
            &record.event_id,
            record.snapshot_generation,
            deadline,
            record.completion.clone(),
            &self.prom,
        );
        let result = if record.cancel.load(Ordering::Acquire) {
            Err("job cancelled before execution".to_string())
        } else if Instant::now() >= deadline {
            Err("job deadline exceeded before execution".to_string())
        } else {
            execute(&mut sink, deadline)
        };

        let (phase, summary, failure) = match result {
            Ok(summary) => {
                if record.cancel.load(Ordering::Acquire) {
                    Self::commit_failure(
                        record,
                        &sink,
                        "cancelled",
                        "job cancelled".into(),
                        Some(summary),
                    )
                } else if Instant::now() >= deadline {
                    Self::commit_failure(
                        record,
                        &sink,
                        "deadline_exceeded",
                        "job deadline exceeded".into(),
                        Some(summary),
                    )
                } else {
                    match sink.send_completion(summary) {
                        Ok(()) => (JobPhase::Completed, Some(summary), None),
                        Err(error) => Self::commit_failure(
                            record,
                            &sink,
                            "delivery_failed",
                            error.to_string(),
                            Some(summary),
                        ),
                    }
                }
            }
            Err(error) => {
                let detail = if Instant::now() >= deadline {
                    "job deadline exceeded".to_string()
                } else {
                    error
                };
                let code = if detail.contains("deadline exceeded") {
                    "deadline_exceeded"
                } else {
                    "delivery_failed"
                };
                Self::commit_failure(record, &sink, code, detail, None)
            }
        };
        // Publish the terminal record and release admission capacity as one
        // registry-serialized transition. Otherwise a replacement can acquire
        // this permit while `prune_for_capacity` still sees every retained job
        // as running and spuriously report `exhaustive_registry_full`.
        self.finish(record, phase, summary, failure, permit);
    }

    /// Commit a worker/delivery failure in the shared terminal gate before
    /// publishing its best-effort frame. A concurrent DELETE after detection
    /// can therefore never rewrite that first failure as cancellation.
    fn commit_failure(
        record: &JobRecord,
        sink: &JobChunkSink<'_>,
        code: &'static str,
        detail: String,
        completed_summary: Option<ExhaustiveSummary>,
    ) -> (JobPhase, Option<ExhaustiveSummary>, Option<String>) {
        match record.completion.resolve_terminal(TerminalRequest::Failed) {
            TerminalResolution::Completed => (JobPhase::Completed, completed_summary, None),
            TerminalResolution::Cancelled => {
                sink.send_failure_best_effort("cancelled", "job cancelled");
                (JobPhase::Cancelled, None, Some("job cancelled".into()))
            }
            TerminalResolution::Failed(canonical) => {
                let detail = canonical.unwrap_or(detail);
                let code = if detail.contains("deadline exceeded") {
                    "deadline_exceeded"
                } else {
                    code
                };
                sink.send_failure_best_effort(code, &detail);
                (JobPhase::Failed, None, Some(detail))
            }
        }
    }

    fn finish(
        &self,
        record: &JobRecord,
        phase: JobPhase,
        summary: Option<ExhaustiveSummary>,
        failure: Option<String>,
        permit: JobPermit,
    ) {
        let requested = match phase {
            JobPhase::Completed => TerminalRequest::Completed,
            JobPhase::Cancelled => TerminalRequest::Cancelled,
            JobPhase::Failed | JobPhase::Running => TerminalRequest::Failed,
        };
        let (phase, summary, failure) = match record.completion.resolve_terminal(requested) {
            TerminalResolution::Completed => (JobPhase::Completed, summary, None),
            TerminalResolution::Cancelled => {
                (JobPhase::Cancelled, None, Some("job cancelled".to_string()))
            }
            TerminalResolution::Failed(canonical) => {
                // `commit_failure` has already resolved cancellation/deadline
                // races and returns their canonical detail when they won. On
                // the second resolution below the terminal gate can only
                // reconstruct the generic `ExecutionFailed` text, so prefer
                // the concrete worker/delivery diagnostic carried here.
                (JobPhase::Failed, None, failure.or(canonical))
            }
        };
        // `start` holds this registry lock while it acquires a permit and
        // prunes terminal history. Keep the permit until the terminal state is
        // visible under that same lock, so no admission can observe the
        // impossible combination "permit available, retained job running".
        let _registry = self.registry.lock();
        {
            let mut state = record.state.lock();
            if !state.phase.terminal() {
                self.prom
                    .exhaustive_jobs
                    .with_label_values(&[state.phase.label()])
                    .dec();
                state.phase = phase;
                state.completed_unix_ms = Some(unix_ms());
                state.summary = summary;
                state.failure = failure;
                self.prom
                    .exhaustive_jobs
                    .with_label_values(&[phase.label()])
                    .inc();
                self.prom
                    .exhaustive_jobs_total
                    .with_label_values(&[phase.label()])
                    .inc();
            }
        }
        // Explicitly drop while `_registry` is still held; relying on local
        // drop order here would reopen the admission race this lock closes.
        drop(permit);
    }

    fn prune_for_capacity(&self, registry: &mut Registry) -> Result<(), StartError> {
        while registry.jobs.len() >= self.config.max_retained {
            let oldest = registry
                .jobs
                .values()
                .filter(|record| record.state.lock().phase.terminal())
                .min_by_key(|record| record.sequence)
                .cloned();
            let Some(oldest) = oldest else {
                return Err(StartError::Capacity);
            };
            let phase = oldest.state.lock().phase;
            registry.jobs.remove(&oldest.id);
            registry.by_event.remove(&oldest.event_id);
            self.prom
                .exhaustive_jobs
                .with_label_values(&[phase.label()])
                .dec();
        }
        Ok(())
    }

    pub(crate) fn status(&self, id: &str) -> Option<JobView> {
        self.registry
            .lock()
            .jobs
            .get(id)
            .map(|record| record.view())
    }

    pub(crate) fn take_stream(
        &self,
        id: &str,
    ) -> Result<tokio::sync::mpsc::Receiver<JobFrame>, StreamError> {
        let record = self
            .registry
            .lock()
            .jobs
            .get(id)
            .cloned()
            .ok_or(StreamError::NotFound)?;
        let receiver = record
            .receiver
            .lock()
            .take()
            .ok_or(StreamError::AlreadyTaken);
        receiver
    }

    pub(crate) fn cancel(&self, id: &str) -> Option<JobView> {
        let record = self.registry.lock().jobs.get(id).cloned()?;
        if !record.state.lock().phase.terminal() {
            record.completion.request_cancel();
        }
        Some(record.view())
    }

    /// Request cooperative cancellation of every retained running job. Shutdown
    /// calls this before taking engine/coordinator write locks so a worker
    /// blocked on an unclaimed bounded stream releases those locks promptly.
    pub(crate) fn cancel_all(&self) -> usize {
        let records: Vec<Arc<JobRecord>> = self.registry.lock().jobs.values().cloned().collect();
        let mut cancelled = 0;
        for record in records {
            if !record.state.lock().phase.terminal() && record.completion.request_cancel() {
                cancelled += 1;
            }
        }
        cancelled
    }
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

fn generation_seed() -> u64 {
    let random = uuid::Uuid::new_v4().as_u128();
    let folded = (random as u64) ^ ((random >> 64) as u64);
    folded.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokio_capacity_limits_are_validated_before_constructing_primitives() {
        let over = tokio::sync::Semaphore::MAX_PERMITS + 1;
        let Err(concurrency_error) = ExhaustiveJobs::new(
            ExhaustiveJobConfig {
                threads: over,
                max_concurrent: over,
                chunk_size: 1,
                channel_depth: 1,
                max_timeout: Duration::from_secs(1),
                max_retained: 1,
            },
            PrometheusMetrics::new(),
        ) else {
            panic!("oversized semaphore bound must be a typed startup error");
        };
        assert!(concurrency_error.contains("Tokio"));

        let Err(channel_error) = ExhaustiveJobs::new(
            ExhaustiveJobConfig {
                threads: 1,
                max_concurrent: 1,
                chunk_size: 1,
                channel_depth: over,
                max_timeout: Duration::from_secs(1),
                max_retained: 1,
            },
            PrometheusMetrics::new(),
        ) else {
            panic!("oversized channel bound must be a typed startup error");
        };
        assert!(channel_error.contains("Tokio"));
    }

    #[test]
    fn unrepresentable_timeout_is_a_startup_error() {
        let Err(error) = ExhaustiveJobs::new(
            ExhaustiveJobConfig {
                threads: 1,
                max_concurrent: 1,
                chunk_size: 1,
                channel_depth: 1,
                max_timeout: Duration::MAX,
                max_retained: 1,
            },
            PrometheusMetrics::new(),
        ) else {
            panic!("an Instant-overflowing timeout must fail at startup");
        };
        assert!(error.contains("Instant range"));
    }

    #[test]
    fn job_deadline_includes_worker_scheduling_delay() {
        let jobs = ExhaustiveJobs::new(
            ExhaustiveJobConfig {
                threads: 1,
                max_concurrent: 1,
                chunk_size: 1,
                channel_depth: 4,
                max_timeout: Duration::from_secs(1),
                max_retained: 4,
            },
            PrometheusMetrics::new(),
        )
        .expect("jobs");

        // Occupy the dedicated pool without consuming a job permit, making the
        // admitted job wait in Rayon's queue.
        let (blocker_entered_tx, blocker_entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        jobs.pool.spawn(move || {
            blocker_entered_tx.send(()).expect("signal blocker");
            release_rx.recv().expect("release blocker");
        });
        blocker_entered_rx.recv().expect("pool blocker entered");

        let executed = Arc::new(AtomicBool::new(false));
        let worker_executed = Arc::clone(&executed);
        let started = jobs
            .start(
                "admission-deadline".into(),
                [0xAD; 32],
                QueryScope::Standard,
                Duration::from_millis(25),
                move |_sink, _deadline| {
                    worker_executed.store(true, Ordering::Release);
                    Ok(ExhaustiveSummary::default())
                },
            )
            .expect("job admitted");
        std::thread::sleep(Duration::from_millis(75));
        release_tx.send(()).expect("release pool blocker");

        let wait = Instant::now();
        loop {
            let view = jobs.status(&started.job.job_id).expect("retained job");
            if view.state != JobPhase::Running {
                assert_eq!(view.state, JobPhase::Failed);
                assert!(view
                    .failure
                    .as_deref()
                    .is_some_and(|failure| failure.contains("deadline exceeded")));
                break;
            }
            assert!(
                wait.elapsed() < Duration::from_secs(1),
                "expired queued job did not become terminal"
            );
            std::thread::yield_now();
        }
        assert!(
            !executed.load(Ordering::Acquire),
            "execution started even though the admission-time deadline had expired"
        );
    }

    #[test]
    fn worker_failure_keeps_its_concrete_status_diagnostic() {
        let jobs = ExhaustiveJobs::new(
            ExhaustiveJobConfig {
                threads: 1,
                max_concurrent: 1,
                chunk_size: 1,
                channel_depth: 4,
                max_timeout: Duration::from_secs(1),
                max_retained: 4,
            },
            PrometheusMetrics::new(),
        )
        .expect("jobs");
        let started = jobs
            .start(
                "specific-failure".into(),
                [0x5A; 32],
                QueryScope::Standard,
                Duration::from_secs(1),
                |_sink, _deadline| Err("shard 2 failed exact convergence".to_string()),
            )
            .expect("job admitted");

        let wait = Instant::now();
        loop {
            let view = jobs.status(&started.job.job_id).expect("retained job");
            if view.state != JobPhase::Running {
                assert_eq!(view.state, JobPhase::Failed);
                assert_eq!(
                    view.failure.as_deref(),
                    Some("shard 2 failed exact convergence")
                );
                break;
            }
            assert!(
                wait.elapsed() < Duration::from_secs(1),
                "failed worker did not become terminal"
            );
            std::thread::yield_now();
        }
    }

    #[test]
    fn start_rejects_timeout_outside_the_manager_bound_without_retaining_a_job() {
        let jobs = ExhaustiveJobs::new(
            ExhaustiveJobConfig {
                threads: 1,
                max_concurrent: 1,
                chunk_size: 1,
                channel_depth: 1,
                max_timeout: Duration::from_secs(1),
                max_retained: 1,
            },
            PrometheusMetrics::new(),
        )
        .expect("jobs");
        let result = jobs.start(
            "invalid-timeout".into(),
            [0xEE; 32],
            QueryScope::Standard,
            Duration::from_secs(2),
            |_sink, _deadline| Ok(ExhaustiveSummary::default()),
        );
        assert!(matches!(result, Err(StartError::InvalidTimeout)));
        assert!(jobs.registry.lock().jobs.is_empty());
        assert_eq!(jobs.permits.available_permits(), 1);
    }

    #[test]
    fn generation_namespaces_are_boot_unique_and_nonzero() {
        let first = generation_seed();
        let second = generation_seed();
        assert_ne!(first, 0);
        assert_ne!(second, 0);
        assert_ne!(first, second);
    }

    #[test]
    fn busy_admission_does_not_prune_a_retained_terminal_job() {
        let jobs = ExhaustiveJobs::new(
            ExhaustiveJobConfig {
                threads: 1,
                max_concurrent: 1,
                chunk_size: 1,
                channel_depth: 4,
                max_timeout: Duration::from_secs(5),
                max_retained: 2,
            },
            PrometheusMetrics::new(),
        )
        .expect("jobs");

        let terminal = jobs
            .start(
                "retained-terminal".into(),
                [1; 32],
                QueryScope::Standard,
                Duration::from_secs(5),
                |_sink, _deadline| Ok(ExhaustiveSummary::default()),
            )
            .expect("first admission");
        let completion = jobs
            .take_stream(&terminal.job.job_id)
            .expect("claim first stream")
            .blocking_recv()
            .expect("first completion frame")
            .into_bytes()
            .expect("live completion frame");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&completion).expect("completion JSON")
                ["type"],
            "completion"
        );
        let wait = Instant::now();
        loop {
            if jobs
                .status(&terminal.job.job_id)
                .is_some_and(|view| view.state == JobPhase::Completed)
            {
                break;
            }
            assert!(
                wait.elapsed() < Duration::from_secs(1),
                "first job did not become terminal"
            );
            std::thread::yield_now();
        }

        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let running = jobs
            .start(
                "permit-holder".into(),
                [2; 32],
                QueryScope::Standard,
                Duration::from_secs(5),
                move |_sink, _deadline| {
                    entered_tx.send(()).expect("signal running");
                    release_rx.recv().expect("release running");
                    Ok(ExhaustiveSummary::default())
                },
            )
            .expect("second admission");
        entered_rx.recv().expect("permit holder entered");

        let rejected = jobs.start(
            "must-be-busy".into(),
            [3; 32],
            QueryScope::Standard,
            Duration::from_secs(5),
            |_sink, _deadline| Ok(ExhaustiveSummary::default()),
        );
        assert!(matches!(rejected, Err(StartError::Busy)));
        assert!(
            jobs.status(&terminal.job.job_id).is_some(),
            "a rejected admission pruned retained terminal history"
        );

        release_tx.send(()).expect("release permit holder");
        let completion = jobs
            .take_stream(&running.job.job_id)
            .expect("claim permit-holder stream")
            .blocking_recv()
            .expect("permit-holder completion frame")
            .into_bytes()
            .expect("live completion frame");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&completion).expect("completion JSON")
                ["type"],
            "completion"
        );
        let wait = Instant::now();
        loop {
            if jobs
                .status(&running.job.job_id)
                .is_some_and(|view| view.state == JobPhase::Completed)
            {
                break;
            }
            assert!(
                wait.elapsed() < Duration::from_secs(1),
                "permit holder did not complete"
            );
            std::thread::yield_now();
        }
    }

    #[test]
    fn terminal_publication_precedes_permit_reuse_at_full_retention() {
        let jobs = ExhaustiveJobs::new(
            ExhaustiveJobConfig {
                threads: 1,
                max_concurrent: 1,
                chunk_size: 1,
                channel_depth: 2,
                max_timeout: Duration::from_secs(5),
                max_retained: 1,
            },
            PrometheusMetrics::new(),
        )
        .expect("jobs");

        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let first = jobs
            .start(
                "finishing-at-capacity".into(),
                [0xA1; 32],
                QueryScope::Standard,
                Duration::from_secs(5),
                move |_sink, _deadline| {
                    entered_tx.send(()).expect("signal execution");
                    release_rx.recv().expect("release execution");
                    Ok(ExhaustiveSummary::default())
                },
            )
            .expect("first admission");
        entered_rx.recv().expect("worker entered");
        let mut first_stream = jobs
            .take_stream(&first.job.job_id)
            .expect("claim first stream");
        let first_record = jobs
            .registry
            .lock()
            .jobs
            .get(&first.job.job_id)
            .cloned()
            .expect("retained first job");

        // Freeze terminal publication after the worker has produced completion.
        // The fixed implementation waits for this state lock while holding both
        // the registry lock and its permit. The old ordering released the
        // permit first, exposing the exact admission race under review.
        let state = first_record.state.lock();
        release_tx.send(()).expect("release first execution");
        first_stream
            .blocking_recv()
            .expect("first completion frame")
            .into_bytes()
            .expect("deliver first completion");

        let wait = Instant::now();
        let permit_released_early = loop {
            if jobs.permits.available_permits() == 1 {
                break true;
            }
            if jobs.registry.try_lock().is_none() {
                break false;
            }
            assert!(
                wait.elapsed() < Duration::from_secs(1),
                "worker never reached terminal publication"
            );
            std::thread::yield_now();
        };
        drop(state);
        assert!(
            !permit_released_early,
            "execution capacity became reusable before terminal state publication"
        );

        let wait = Instant::now();
        while !jobs
            .status(&first.job.job_id)
            .is_some_and(|view| view.state == JobPhase::Completed)
        {
            assert!(
                wait.elapsed() < Duration::from_secs(1),
                "first job did not become terminal"
            );
            std::thread::yield_now();
        }

        // With one retained slot, the replacement must now atomically acquire
        // the released permit and prune the terminal predecessor.
        let replacement = jobs
            .start(
                "replacement".into(),
                [0xA2; 32],
                QueryScope::Standard,
                Duration::from_secs(5),
                |_sink, _deadline| Ok(ExhaustiveSummary::default()),
            )
            .expect("terminal predecessor makes room for replacement");
        jobs.take_stream(&replacement.job.job_id)
            .expect("claim replacement stream")
            .blocking_recv()
            .expect("replacement completion frame")
            .into_bytes()
            .expect("deliver replacement completion");
    }

    #[test]
    fn cancel_all_releases_a_lock_held_by_an_unclaimed_backpressured_job() {
        let prom = PrometheusMetrics::new();
        let jobs = ExhaustiveJobs::new(
            ExhaustiveJobConfig {
                threads: 1,
                max_concurrent: 1,
                chunk_size: 1,
                channel_depth: 1,
                max_timeout: Duration::from_secs(30),
                max_retained: 8,
            },
            prom,
        )
        .expect("jobs");
        let held_during_delivery = Arc::new(parking_lot::Mutex::new(()));
        let worker_lock = Arc::clone(&held_during_delivery);
        let started = jobs
            .start(
                "shutdown-cancel".into(),
                [7; 32],
                QueryScope::Standard,
                Duration::from_secs(30),
                move |sink, _deadline| {
                    let _guard = worker_lock.lock();
                    for sequence in 0..3 {
                        sink.send_chunk(&reverse_rusty::MatchChunk {
                            sequence,
                            matches: vec![reverse_rusty::ExhaustiveMatch {
                                logical_id: sequence,
                                score: None,
                            }],
                        })
                        .map_err(|error| error.to_string())?;
                    }
                    Ok(ExhaustiveSummary::default())
                },
            )
            .expect("start");

        let wait_started = Instant::now();
        loop {
            if held_during_delivery.try_lock().is_none() {
                break;
            }
            assert!(
                wait_started.elapsed() < Duration::from_secs(1),
                "job never entered the lock-holding delivery section"
            );
            std::thread::yield_now();
        }

        assert_eq!(jobs.cancel_all(), 1);
        let released = held_during_delivery.try_lock_for(Duration::from_millis(250));
        assert!(
            released.is_some(),
            "shutdown cancellation did not release the worker lock promptly"
        );
        drop(released);

        let terminal_started = Instant::now();
        loop {
            let view = jobs.status(&started.job.job_id).expect("retained");
            if view.state == JobPhase::Cancelled {
                break;
            }
            assert!(
                terminal_started.elapsed() < Duration::from_secs(1),
                "cancelled job did not become terminal"
            );
            std::thread::yield_now();
        }
    }

    #[test]
    fn cancellation_during_completion_backpressure_is_cancelled() {
        let jobs = ExhaustiveJobs::new(
            ExhaustiveJobConfig {
                threads: 1,
                max_concurrent: 1,
                chunk_size: 1,
                channel_depth: 1,
                max_timeout: Duration::from_secs(30),
                max_retained: 8,
            },
            PrometheusMetrics::new(),
        )
        .expect("jobs");
        let (chunk_sent_tx, chunk_sent_rx) = std::sync::mpsc::channel();
        let started = jobs
            .start(
                "cancel-completion".into(),
                [8; 32],
                QueryScope::Standard,
                Duration::from_secs(30),
                move |sink, _deadline| {
                    sink.send_chunk(&reverse_rusty::MatchChunk {
                        sequence: 0,
                        matches: vec![reverse_rusty::ExhaustiveMatch {
                            logical_id: 1,
                            score: None,
                        }],
                    })
                    .map_err(|error| error.to_string())?;
                    chunk_sent_tx.send(()).expect("signal full channel");
                    Ok(ExhaustiveSummary {
                        exact_total: 1,
                        chunk_count: 1,
                        checksum: reverse_rusty::DeliveryChecksum::default(),
                    })
                },
            )
            .expect("start");
        chunk_sent_rx.recv().expect("chunk filled channel");

        // Leave the single-consumer stream unclaimed. The provisional chunk
        // occupies the only channel slot, so the worker advances into the
        // completion-frame backpressure loop.
        std::thread::sleep(Duration::from_millis(20));
        jobs.cancel(&started.job.job_id).expect("retained job");

        let wait = Instant::now();
        loop {
            let view = jobs.status(&started.job.job_id).expect("retained");
            if view.state != JobPhase::Running {
                assert_eq!(view.state, JobPhase::Cancelled);
                assert!(view.exact_total.is_none());
                assert!(view.checksum.is_none());
                break;
            }
            assert!(
                wait.elapsed() < Duration::from_secs(1),
                "cancelled completion send did not become terminal"
            );
            std::thread::yield_now();
        }
    }

    #[test]
    fn completion_requires_terminal_dequeue_and_queued_drop_fails() {
        let jobs = ExhaustiveJobs::new(
            ExhaustiveJobConfig {
                threads: 1,
                max_concurrent: 1,
                chunk_size: 1,
                channel_depth: 1,
                max_timeout: Duration::from_secs(5),
                max_retained: 8,
            },
            PrometheusMetrics::new(),
        )
        .expect("jobs");

        let delivered = jobs
            .start(
                "completion-consumed".into(),
                [9; 32],
                QueryScope::Standard,
                Duration::from_secs(5),
                |_sink, _deadline| Ok(ExhaustiveSummary::default()),
            )
            .expect("start delivered job");
        let mut delivered_stream = jobs
            .take_stream(&delivered.job.job_id)
            .expect("claim delivered stream");
        let queued_at = Instant::now();
        while delivered_stream.is_empty() {
            assert!(
                queued_at.elapsed() < Duration::from_secs(1),
                "completion was not queued"
            );
            std::thread::yield_now();
        }
        assert_eq!(
            jobs.status(&delivered.job.job_id)
                .expect("retained delivered job")
                .state,
            JobPhase::Running,
            "enqueue alone must not publish completed status"
        );
        let completion = delivered_stream
            .blocking_recv()
            .expect("queued completion")
            .into_bytes()
            .expect("completion still valid");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&completion).expect("completion JSON")
                ["type"],
            "completion"
        );
        let completed_at = Instant::now();
        loop {
            let view = jobs
                .status(&delivered.job.job_id)
                .expect("retained delivered job");
            if view.state == JobPhase::Completed {
                assert_eq!(view.exact_total, Some(0));
                break;
            }
            assert!(
                completed_at.elapsed() < Duration::from_secs(1),
                "dequeued completion did not publish completed status"
            );
            std::thread::yield_now();
        }

        let dropped = jobs
            .start(
                "completion-dropped".into(),
                [10; 32],
                QueryScope::Standard,
                Duration::from_secs(5),
                |_sink, _deadline| Ok(ExhaustiveSummary::default()),
            )
            .expect("start dropped job");
        let dropped_stream = jobs
            .take_stream(&dropped.job.job_id)
            .expect("claim dropped stream");
        let queued_at = Instant::now();
        while dropped_stream.is_empty() {
            assert!(
                queued_at.elapsed() < Duration::from_secs(1),
                "dropped completion was not queued"
            );
            std::thread::yield_now();
        }
        drop(dropped_stream);

        let failed_at = Instant::now();
        loop {
            let view = jobs
                .status(&dropped.job.job_id)
                .expect("retained dropped job");
            if view.state != JobPhase::Running {
                assert_eq!(view.state, JobPhase::Failed);
                assert!(view.exact_total.is_none());
                assert!(view.checksum.is_none());
                assert!(view
                    .failure
                    .as_deref()
                    .is_some_and(|detail| detail.contains("not consumed")));
                break;
            }
            assert!(
                failed_at.elapsed() < Duration::from_secs(1),
                "dropped queued completion did not fail the job"
            );
            std::thread::yield_now();
        }
    }
}
