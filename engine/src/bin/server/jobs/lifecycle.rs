//! Exhaustive-job admission, execution, and terminal publication.
//!
//! The permit is intentionally released while the registry lock is still held:
//! admission must never observe reusable capacity before terminal state is visible.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use reverse_rusty::delivery::{ChunkSink, ExhaustiveSummary};
use reverse_rusty::QueryScope;

use crate::metrics::PrometheusMetrics;

use super::stream::{CompletionState, JobChunkSink, JobFrame, TerminalRequest, TerminalResolution};
use super::{
    ExhaustiveJobConfig, ExhaustiveJobs, JobExecutionError, JobPhase, JobRecord, JobState,
    Registry, StartError, StartOutcome,
};

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
        F: FnOnce(&mut dyn ChunkSink, Instant) -> Result<ExhaustiveSummary, JobExecutionError>
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
        let (phase, _) = tokio::sync::watch::channel(JobPhase::Running);
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
            phase,
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
        F: FnOnce(&mut dyn ChunkSink, Instant) -> Result<ExhaustiveSummary, JobExecutionError>,
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
            Err(JobExecutionError::generic("job cancelled before execution"))
        } else if Instant::now() >= deadline {
            Err(JobExecutionError::generic(
                "job deadline exceeded before execution",
            ))
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
                        JobExecutionError::generic("job cancelled"),
                        Some(summary),
                    )
                } else if Instant::now() >= deadline {
                    Self::commit_failure(
                        record,
                        &sink,
                        "deadline_exceeded",
                        JobExecutionError::generic("job deadline exceeded"),
                        Some(summary),
                    )
                } else {
                    match sink.send_completion(summary) {
                        Ok(()) => (JobPhase::Completed, Some(summary), None),
                        Err(error) => Self::commit_failure(
                            record,
                            &sink,
                            "delivery_failed",
                            JobExecutionError::generic(error.to_string()),
                            Some(summary),
                        ),
                    }
                }
            }
            Err(error) => {
                let error = if Instant::now() >= deadline {
                    JobExecutionError::generic("job deadline exceeded")
                } else {
                    error
                };
                let code = if error.detail.contains("deadline exceeded") {
                    "deadline_exceeded"
                } else if error.error_type != "exhaustive_job_failed" {
                    error.error_type
                } else {
                    "delivery_failed"
                };
                Self::commit_failure(record, &sink, code, error, None)
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
        error: JobExecutionError,
        completed_summary: Option<ExhaustiveSummary>,
    ) -> (
        JobPhase,
        Option<ExhaustiveSummary>,
        Option<JobExecutionError>,
    ) {
        match record.completion.resolve_terminal(TerminalRequest::Failed) {
            TerminalResolution::Completed => (JobPhase::Completed, completed_summary, None),
            TerminalResolution::Cancelled => {
                sink.send_failure_best_effort("cancelled", "job cancelled");
                (
                    JobPhase::Cancelled,
                    None,
                    Some(JobExecutionError::generic("job cancelled")),
                )
            }
            TerminalResolution::Failed(canonical) => {
                let error = canonical.map_or(error, JobExecutionError::generic);
                let code = if error.detail.contains("deadline exceeded") {
                    "deadline_exceeded"
                } else {
                    code
                };
                sink.send_failure_best_effort(code, &error.detail);
                (JobPhase::Failed, None, Some(error))
            }
        }
    }

    fn finish(
        &self,
        record: &JobRecord,
        phase: JobPhase,
        summary: Option<ExhaustiveSummary>,
        failure: Option<JobExecutionError>,
        permit: JobPermit,
    ) {
        let requested = match phase {
            JobPhase::Completed => TerminalRequest::Completed,
            JobPhase::Cancelled => TerminalRequest::Cancelled,
            JobPhase::Failed | JobPhase::Running => TerminalRequest::Failed,
        };
        let (phase, summary, failure) = match record.completion.resolve_terminal(requested) {
            TerminalResolution::Completed => (JobPhase::Completed, summary, None),
            TerminalResolution::Cancelled => (
                JobPhase::Cancelled,
                None,
                Some(JobExecutionError::generic("job cancelled")),
            ),
            TerminalResolution::Failed(canonical) => {
                // `commit_failure` has already resolved cancellation/deadline
                // races and returns their canonical detail when they won. On
                // the second resolution below the terminal gate can only
                // reconstruct the generic `ExecutionFailed` text, so prefer
                // the concrete worker/delivery diagnostic carried here.
                (
                    JobPhase::Failed,
                    None,
                    failure.or_else(|| canonical.map(JobExecutionError::generic)),
                )
            }
        };
        // `start` holds this registry lock while it acquires a permit and
        // prunes terminal history. Keep the permit until the terminal state is
        // visible under that same lock, so no admission can observe the
        // impossible combination "permit available, retained job running".
        let _registry = self.registry.lock();
        let mut published = false;
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
                published = true;
            }
        }
        if published {
            record.phase.send_replace(phase);
        }
        // Explicitly drop while `_registry` is still held; relying on local
        // drop order here would reopen the admission race this lock closes.
        drop(permit);
    }
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

pub(super) fn generation_seed() -> u64 {
    let random = uuid::Uuid::new_v4().as_u128();
    let folded = (random as u64) ^ ((random >> 64) as u64);
    folded.max(1)
}
