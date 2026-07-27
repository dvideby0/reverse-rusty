use super::*;

/// Initial bulk ingest predates the per-logical ADR-047 repair journal. If one
/// shard succeeds and a later shard fails, retaining the coordinator leaves a
/// physically partial corpus with an empty repair map. The logical directory's
/// authority bit must revoke exhaustive completion before any member escapes.
#[test]
fn exhaustive_refuses_after_partial_bulk_ingest_without_a_repair_record() {
    let cfg = ClusterConfig {
        num_shards: 3,
        ..Default::default()
    };
    let seed = vec![(100u64, "1994 topps baseball".to_string())];
    let real = ClusterEngine::build(vocab(), &cfg, &seed).expect("throwaway build");
    let norm = Arc::clone(&real.norm);
    let dict = Arc::clone(&real.dict);
    let tag_dict = Arc::clone(&real.tag_dict);
    let ring = HashRing::new(cfg.num_shards, cfg.vnodes).expect("ring");

    // Find two single-term selective queries on different positions. Enumerating
    // positions gives a successful lower bucket followed by the injected failure.
    let mut by_position = vec![None; cfg.num_shards];
    for index in 0..1_024 {
        let dsl = format!("zzpartialbulk{index}");
        let feature = dict.get_or_synthetic(&format!("term:{dsl}"));
        let position = ring.lookup(feature);
        by_position[position].get_or_insert(dsl);
        if by_position.iter().filter(|dsl| dsl.is_some()).count() >= 2 {
            break;
        }
    }
    let mut selected = by_position
        .into_iter()
        .enumerate()
        .filter_map(|(position, dsl)| dsl.map(|dsl| (position, dsl)));
    let (successful_position, successful_dsl) = selected.next().expect("successful position");
    let (failing_position, failing_dsl) = selected.next().expect("failing position");
    assert!(successful_position < failing_position);

    let failures: Vec<Arc<AtomicBool>> = (0..cfg.num_shards)
        .map(|_| Arc::new(AtomicBool::new(false)))
        .collect();
    let shards: Vec<Box<dyn Shard>> = failures
        .iter()
        .enumerate()
        .map(|(position, fail)| {
            let local = LocalShard::new(
                Arc::clone(&norm),
                Arc::clone(&dict),
                Arc::clone(&tag_dict),
                cfg.per_shard.clone(),
            );
            Box::new(ToggleFailShard::with_position(
                local,
                Arc::clone(fail),
                position as u32,
            )) as Box<dyn Shard>
        })
        .collect();
    let durable = ClusterDurable::in_memory(cfg.num_shards as u32, cfg.vnodes, dict.fingerprint());
    let cluster = ClusterEngine::from_parts(
        norm,
        dict,
        tag_dict,
        ring,
        shards,
        cfg.include_broad,
        1,
        cfg.per_shard,
        durable,
    )
    .expect("from_parts cluster");

    failures[failing_position].store(true, Ordering::Release);
    let error = cluster
        .ingest(&[(1, successful_dsl.clone()), (2, failing_dsl.clone())])
        .expect_err("later shard bulk load must fail");
    assert!(matches!(error, ShardError::Remote(_)));
    assert_eq!(
        cluster.num_queries().expect("partial physical count"),
        1,
        "the lower bucket must have landed before the injected failure"
    );
    assert_eq!(
        cluster.pending_repairs(),
        0,
        "bulk load has no incremental repair record"
    );
    assert!(
        !cluster.logical_ids_authoritative(),
        "partial bulk apply must revoke the convergence attestation"
    );

    let mut sink = RecordingExhaustiveSink::default();
    let error = cluster
        .try_percolate_filtered_all(
            &format!("{successful_dsl} {failing_dsl}"),
            &[],
            crate::result::QueryScope::Standard,
            None,
            1,
            None,
            &mut sink,
        )
        .expect_err("partial bulk corpus must not receive exact completion");
    assert!(
        matches!(
            error,
            ShardError::Protocol(ref detail)
                if detail.contains("authoritative coordinator convergence state")
        ),
        "unexpected exhaustive refusal: {error}"
    );
    assert!(
        sink.chunks.is_empty(),
        "convergence preflight must precede provisional output"
    );
}

struct BlockingExhaustiveSink {
    chunks: Vec<MatchChunk>,
    entered: mpsc::SyncSender<()>,
    release: mpsc::Receiver<()>,
}

impl ChunkSink for BlockingExhaustiveSink {
    fn send_chunk(&mut self, chunk: &MatchChunk) -> Result<(), ChunkSinkError> {
        self.chunks.push(chunk.clone());
        if self.chunks.len() == 1 {
            self.entered
                .send(())
                .map_err(|error| ChunkSinkError::new(error.to_string()))?;
            self.release
                .recv()
                .map_err(|error| ChunkSinkError::new(error.to_string()))?;
        }
        Ok(())
    }
}

/// A successful re-placement is just as dangerous as a queued partial apply if
/// it can interleave between sequential shard reads. The exhaustive API holds
/// the coordinator's mutation barrier for the whole stream, so the upsert
/// cannot move the row until the old coherent view is complete.
#[test]
fn exhaustive_blocks_successful_replacement_until_the_stream_finishes() {
    let cfg = ClusterConfig {
        num_shards: 3,
        ..Default::default()
    };
    let seed = vec![(100u64, "1994 topps baseball".to_string())];
    let cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("cluster");

    let mut by_position = vec![None; cfg.num_shards];
    for index in 0..1_024 {
        let dsl = format!("zzexhaustivemove{index}");
        let feature = cluster.dict.get_or_synthetic(&format!("term:{dsl}"));
        let position = cluster.ring.lookup(feature);
        by_position[position].get_or_insert(dsl);
        if by_position.iter().filter(|value| value.is_some()).count() >= 2 {
            break;
        }
    }
    let mut terms = by_position.into_iter().flatten();
    let old_dsl = terms.next().expect("old position");
    let new_dsl = terms.next().expect("new position");
    cluster.add_query(5, &old_dsl).expect("old add");
    let title = format!("{old_dsl} {new_dsl}");

    let (entered_tx, entered_rx) = mpsc::sync_channel(0);
    let (release_tx, release_rx) = mpsc::sync_channel(0);
    let (write_started_tx, write_started_rx) = mpsc::sync_channel(0);
    let (write_done_tx, write_done_rx) = mpsc::channel();

    std::thread::scope(|scope| {
        let read = scope.spawn(|| {
            let mut sink = BlockingExhaustiveSink {
                chunks: Vec::new(),
                entered: entered_tx,
                release: release_rx,
            };
            let result = cluster.try_percolate_filtered_all(
                &title,
                &[],
                crate::result::QueryScope::Standard,
                None,
                1,
                None,
                &mut sink,
            );
            (result, sink.chunks)
        });
        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("first old-view chunk");

        let write = scope.spawn(|| {
            write_started_tx.send(()).expect("signal writer");
            let result = cluster.upsert_query(5, &new_dsl, 2);
            write_done_tx.send(()).expect("signal completion");
            result
        });
        write_started_rx.recv().expect("writer started");
        assert!(
            write_done_rx
                .recv_timeout(Duration::from_millis(50))
                .is_err(),
            "upsert crossed the exhaustive mutation barrier"
        );

        release_tx.send(()).expect("release stream");
        let (result, chunks) = read.join().expect("read thread");
        let result = result.expect("coherent old-view exhaustive stream");
        let delivered: Vec<u64> = chunks
            .iter()
            .flat_map(|chunk| chunk.matches.iter().map(|member| member.logical_id))
            .collect();
        assert_eq!(delivered, vec![5]);
        assert_eq!(result.summary.exact_total, 1);
        write.join().expect("write thread").expect("upsert");
    });

    assert_eq!(cluster.pending_repairs(), 0);
    assert_eq!(cluster.percolate(&title).expect("new view"), vec![5]);
}

/// Compatibility source enrichment needs the same core fence as exhaustive
/// delivery: a direct library mutation must not replace a source between the
/// match and source-read phases of one request.
#[test]
fn consistent_read_view_blocks_direct_replacement_until_source_is_read() {
    let cfg = ClusterConfig {
        num_shards: 3,
        ..Default::default()
    };
    let seed = vec![(5u64, "1994 topps".to_string())];
    let cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("cluster");
    let view = cluster.consistent_read_view();
    let (ids, _) = view
        .percolate_filtered_with_stats("1994 topps card", &[], false)
        .expect("old-view match");
    assert_eq!(ids, vec![5]);

    let (write_started_tx, write_started_rx) = mpsc::sync_channel(0);
    let (write_done_tx, write_done_rx) = mpsc::channel();
    std::thread::scope(|scope| {
        let write = scope.spawn(|| {
            write_started_tx.send(()).expect("signal writer");
            let result = cluster.upsert_query(5, "1995 fleer", 2);
            write_done_tx.send(()).expect("signal completion");
            result
        });
        write_started_rx.recv().expect("writer started");
        assert!(
            write_done_rx
                .recv_timeout(Duration::from_millis(50))
                .is_err(),
            "direct upsert crossed the consistent read view"
        );
        assert_eq!(
            view.get_source(5).expect("old-view source").as_deref(),
            Some("1994 topps")
        );
        drop(view);
        write.join().expect("writer thread").expect("upsert");
    });

    assert_eq!(
        cluster.get_source(5).expect("new source").as_deref(),
        Some("1995 fleer")
    );
    assert!(!cluster
        .percolate("1994 topps card")
        .expect("new-view match")
        .contains(&5));
}

/// Ranked v2 delivery performs a bounded match and a later winner-source fetch.
/// The public read view must cover both phases so a same-ID replacement cannot
/// pair an old hit with the replacement query's source.
#[test]
fn consistent_read_view_fences_ranked_match_and_source_fetch() {
    let cfg = ClusterConfig {
        num_shards: 3,
        ..Default::default()
    };
    let seed = vec![(5u64, "1994 topps".to_string())];
    let cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("cluster");
    let program = cluster
        .compile_rank_program(&crate::rank::RankProgramSpec::default())
        .expect("rank program");
    let view = cluster.consistent_read_view();
    let ranked = view
        .try_percolate_filtered_top_k(
            "1994 topps card",
            &[],
            crate::result::TopKOptions::default(),
            &program,
            None,
        )
        .expect("old-view ranked match");
    assert_eq!(
        ranked
            .hits
            .iter()
            .map(|hit| hit.logical_id)
            .collect::<Vec<_>>(),
        vec![5]
    );

    let (write_started_tx, write_started_rx) = mpsc::sync_channel(0);
    let (write_done_tx, write_done_rx) = mpsc::channel();
    std::thread::scope(|scope| {
        let write = scope.spawn(|| {
            write_started_tx.send(()).expect("signal writer");
            let result = cluster.upsert_query(5, "1995 fleer", 2);
            write_done_tx.send(()).expect("signal completion");
            result
        });
        write_started_rx.recv().expect("writer started");
        assert!(
            write_done_rx
                .recv_timeout(Duration::from_millis(50))
                .is_err(),
            "direct upsert crossed the ranked consistent read view"
        );
        assert_eq!(
            view.fetch_ranked_sources_bounded(&ranked, 1_024, None)
                .expect("old-view winner sources"),
            vec!["1994 topps".to_string()]
        );
        drop(view);
        write.join().expect("writer thread").expect("upsert");
    });

    assert_eq!(
        cluster.get_source(5).expect("new source").as_deref(),
        Some("1995 fleer")
    );
    assert!(!cluster
        .percolate("1994 topps card")
        .expect("new-view match")
        .contains(&5));
}

/// A partially-applied re-placement can leave the old body live at its prior
/// owner while the new body is already live at a different owner. Both rows
/// legitimately pass ADR-109 ownership for a title containing both bodies, so
/// an O(chunk) exhaustive merge cannot deduplicate them. It must refuse the
/// divergent cluster before emitting provisional members, then succeed after
/// the queued repair converges.
#[test]
fn exhaustive_refuses_pending_repair_overlap_until_resync() {
    let cfg = ClusterConfig {
        num_shards: 3,
        ..Default::default()
    };
    let seed = vec![(100u64, "1994 topps baseball".to_string())];
    let real = ClusterEngine::build(vocab(), &cfg, &seed).expect("throwaway build");
    let norm = Arc::clone(&real.norm);
    let dict = Arc::clone(&real.dict);
    let tag_dict = Arc::clone(&real.tag_dict);
    let ring = HashRing::new(cfg.num_shards, cfg.vnodes).expect("ring");

    // Find two synthetic single-term queries whose selective anchors land on
    // different positions, making an upsert a real cross-owner re-placement.
    let mut by_position = vec![None; cfg.num_shards];
    for index in 0..1_024 {
        let dsl = format!("zzexhaustiveoverlap{index}");
        let feature = dict.get_or_synthetic(&format!("term:{dsl}"));
        let position = ring.lookup(feature);
        by_position[position].get_or_insert(dsl);
        if by_position.iter().filter(|value| value.is_some()).count() >= 2 {
            break;
        }
    }
    let mut terms = by_position
        .into_iter()
        .enumerate()
        .filter_map(|(position, dsl)| dsl.map(|dsl| (position, dsl)));
    let (old_position, old_dsl) = terms.next().expect("first selective position");
    let (new_position, new_dsl) = terms.next().expect("second selective position");
    assert_ne!(old_position, new_position);

    let failures: Vec<Arc<AtomicBool>> = (0..cfg.num_shards)
        .map(|_| Arc::new(AtomicBool::new(false)))
        .collect();
    let shards: Vec<Box<dyn Shard>> = failures
        .iter()
        .enumerate()
        .map(|(position, fail)| {
            let local = LocalShard::new(
                Arc::clone(&norm),
                Arc::clone(&dict),
                Arc::clone(&tag_dict),
                cfg.per_shard.clone(),
            );
            Box::new(ToggleFailShard::with_position(
                local,
                Arc::clone(fail),
                position as u32,
            )) as Box<dyn Shard>
        })
        .collect();
    let durable = ClusterDurable::in_memory(cfg.num_shards as u32, cfg.vnodes, dict.fingerprint());
    let cluster = ClusterEngine::from_parts(
        Arc::clone(&norm),
        Arc::clone(&dict),
        Arc::clone(&tag_dict),
        ring,
        shards,
        cfg.include_broad,
        1,
        cfg.per_shard.clone(),
        durable,
    )
    .expect("from_parts cluster");

    let added = cluster.add_query(5, &old_dsl).expect("healthy old add");
    assert!(
        matches!(added, AddOutcome::Placed { ref shards } if shards == &vec![old_position]),
        "old query must live at the selected old position: {added:?}"
    );

    // Only the old owner fails its delete. The new owner accepts the replacement,
    // leaving two live bodies under logical id 5 until resync.
    failures[old_position].store(true, Ordering::Release);
    let error = cluster
        .upsert_query(5, &new_dsl, 2)
        .expect_err("old-owner delete must partially fail");
    assert!(matches!(
        error,
        ShardError::PartiallyApplied {
            ref applied,
            ref failed,
            ..
        } if applied == &vec![new_position] && failed == &vec![old_position]
    ));
    assert_eq!(cluster.pending_repairs(), 1);

    let title = format!("{old_dsl} {new_dsl}");
    let mut sink = RecordingExhaustiveSink::default();
    let error = cluster
        .try_percolate_filtered_all(
            &title,
            &[],
            crate::result::QueryScope::Standard,
            None,
            1,
            None,
            &mut sink,
        )
        .expect_err("divergent cluster must not certify an exact stream");
    assert!(
        matches!(error, ShardError::Protocol(ref detail) if detail.contains("partial-apply")),
        "unexpected refusal: {error}"
    );
    assert!(
        sink.chunks.is_empty(),
        "the preflight refusal must precede provisional delivery"
    );

    failures[old_position].store(false, Ordering::Release);
    let report = cluster.resync();
    assert_eq!(report.still_pending, 0);
    assert_eq!(cluster.pending_repairs(), 0);
    let result = cluster
        .try_percolate_filtered_all(
            &title,
            &[],
            crate::result::QueryScope::Standard,
            None,
            1,
            None,
            &mut sink,
        )
        .expect("converged cluster streams exactly");
    let delivered: Vec<u64> = sink
        .chunks
        .iter()
        .flat_map(|chunk| chunk.matches.iter().map(|member| member.logical_id))
        .collect();
    assert_eq!(delivered, vec![5]);
    assert_eq!(result.summary.exact_total, 1);
}

/// The mutation barrier and logical-id stripes have one global order:
/// barrier first, stripe second. `resync` necessarily holds the barrier while
/// taking a stripe; if a live mutation took the stripe first, a queued
/// exhaustive writer on a writer-preferring `RwLock` could complete a
/// three-way deadlock. Holding the target stripe here lets the test observe
/// which lock the live add reaches first without ever forming that cycle.
#[test]
fn live_mutation_takes_view_barrier_before_logical_stripe() {
    let cluster = ClusterEngine::build(
        vocab(),
        &ClusterConfig {
            num_shards: 2,
            ..Default::default()
        },
        &[(1, "1994 topps".to_string())],
    )
    .expect("cluster");
    let logical = cluster.logical_write_guard(2);

    std::thread::scope(|scope| {
        let (started_tx, started_rx) = mpsc::channel();
        let cluster_ref = &cluster;
        let mutation = scope.spawn(move || {
            started_tx.send(()).expect("signal mutation");
            cluster_ref.add_query(2, "1994 topps")
        });
        started_rx.recv().expect("mutation started");

        let deadline = Instant::now() + Duration::from_millis(500);
        let mut barrier_held = false;
        while Instant::now() < deadline {
            match cluster.pit_open_barrier.try_write() {
                Ok(guard) => {
                    drop(guard);
                    std::thread::yield_now();
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    barrier_held = true;
                    break;
                }
                Err(std::sync::TryLockError::Poisoned(error)) => {
                    drop(std::sync::PoisonError::into_inner(error));
                    panic!("mutation view barrier was poisoned");
                }
            }
        }

        drop(logical);
        mutation
            .join()
            .expect("mutation thread")
            .expect("mutation succeeds");
        assert!(
            barrier_held,
            "live mutation blocked on the logical stripe before acquiring the view barrier"
        );
    });
}
