use super::lifecycle::generation_seed;
use super::*;
use std::sync::atomic::Ordering;
use std::time::Instant;

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
            |_sink, _deadline| {
                Err(JobExecutionError::generic(
                    "shard 2 failed exact convergence",
                ))
            },
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
        serde_json::from_slice::<serde_json::Value>(&completion).expect("completion JSON")["type"],
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
        serde_json::from_slice::<serde_json::Value>(&completion).expect("completion JSON")["type"],
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
        serde_json::from_slice::<serde_json::Value>(&completion).expect("completion JSON")["type"],
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
