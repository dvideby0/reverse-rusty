use super::*;

/// Partial-apply detection + `resync` repair (ADR-047): a selective add whose target shard's
/// write fails returns `PartiallyApplied` (not a swallowed error), emits a `ClusterPartialApply`
/// event, and queues the shard for repair — leaving a transient false-negative window. Once the
/// shard recovers, `resync` re-drives ONLY the failed shard and the query becomes matchable again
/// (zero false negatives restored). Deterministic via a `from_parts` cluster over fault-injecting
/// shards; the gRPC oracle proves the same DETECTION over a real wire.
#[test]
fn partial_apply_is_detected_then_resync_converges() {
    let cfg = ClusterConfig {
        num_shards: 3,
        ..Default::default()
    };
    // A throwaway build gives a frozen norm + dict that already know the query's tokens.
    let seed = vec![(100u64, "1994 acme appliance".to_string())];
    let real = ClusterEngine::build(vocab(), &cfg, &seed).expect("throwaway build");
    let norm = Arc::clone(&real.norm);
    let dict = Arc::clone(&real.dict);
    let tag_dict = Arc::clone(&real.tag_dict);

    // A from_parts cluster over fault-injectable shards sharing that frozen feature space.
    let fail = Arc::new(AtomicBool::new(false));
    let shards: Vec<Box<dyn Shard>> = (0..cfg.num_shards)
        .map(|_| {
            let ls = LocalShard::new(
                Arc::clone(&norm),
                Arc::clone(&dict),
                Arc::clone(&tag_dict),
                cfg.per_shard.clone(),
            );
            Box::new(ToggleFailShard::new(ls, Arc::clone(&fail))) as Box<dyn Shard>
        })
        .collect();
    let ring = HashRing::new(cfg.num_shards, cfg.vnodes).expect("ring");
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

    // Capture emitted events so we can assert the partial-apply event fires.
    let events: Arc<Mutex<Vec<EngineEvent>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let sink = Arc::clone(&events);
        cluster.set_observer(Arc::new(move |ev: &EngineEvent| {
            sink.lock().unwrap().push(ev.clone());
        }));
    }

    // `"zznovelaterm"` is a single out-of-dict required term ⇒ a synthetic (freq-0, never-hot)
    // feature ⇒ class A ⇒ selective placement (one shard). Confirm on a HEALTHY add + that it is
    // matchable, establishing the baseline. (An in-dict term in this tiny corpus would be hot ⇒
    // the replicated lane, never selective — so a synthetic anchor is what forces class A here.)
    let dsl = "zznovelaterm";
    let placed = cluster.add_query(1, dsl).expect("healthy add");
    assert!(
        matches!(placed, AddOutcome::Placed { ref shards } if shards.len() == 1),
        "expected single-shard selective placement, got {placed:?}"
    );
    assert!(
        cluster
            .percolate("zznovelaterm")
            .expect("percolate")
            .contains(&1),
        "healthy selective add must be matchable"
    );

    // Now fail every shard's writes and add a second query with the SAME (selective) placement.
    fail.store(true, Ordering::Release);
    match cluster.add_query(2, dsl) {
        Err(ShardError::PartiallyApplied {
            logical,
            applied,
            failed,
            ..
        }) => {
            assert_eq!(logical, 2);
            assert!(
                applied.is_empty(),
                "the only target shard failed, got applied={applied:?}"
            );
            assert_eq!(
                failed.len(),
                1,
                "exactly the one selective target failed: {failed:?}"
            );
        }
        other => panic!("expected PartiallyApplied, got {other:?}"),
    }
    assert_eq!(
        cluster.pending_repairs(),
        1,
        "the failed mutation must be queued for repair"
    );
    assert!(
        events.lock().unwrap().iter().any(|e| matches!(
            e,
            EngineEvent::DurabilityFailure {
                op: DurabilityOp::ClusterPartialApply,
                ..
            }
        )),
        "a ClusterPartialApply durability event must be emitted"
    );
    // Divergence: query 2 is not yet matchable (the transient false-negative window).
    assert!(
        !cluster
            .percolate("zznovelaterm")
            .expect("percolate")
            .contains(&2),
        "a partially-applied add must not be matchable until repaired"
    );

    // The shard recovers; resync re-drives only the failed shard and converges.
    fail.store(false, Ordering::Release);
    let report = cluster.resync();
    assert_eq!(report.repaired, 1, "the queued mutation must converge");
    assert_eq!(report.still_pending, 0);
    assert_eq!(cluster.pending_repairs(), 0, "the queue must drain");

    // Zero false negatives restored: both queries are matchable again.
    let hits = cluster.percolate("zznovelaterm").expect("percolate");
    assert!(
        hits.contains(&1) && hits.contains(&2),
        "both queries must match after resync: {hits:?}"
    );
}

/// `resync` keeps a mutation queued when its shard is STILL failing (ADR-047): the repair pass
/// is idempotent and only converges what it can, never silently dropping an unrepaired mutation.
#[test]
fn resync_requeues_when_shard_still_failing() {
    let cfg = ClusterConfig {
        num_shards: 3,
        ..Default::default()
    };
    let seed = vec![(100u64, "1994 acme appliance".to_string())];
    let real = ClusterEngine::build(vocab(), &cfg, &seed).expect("throwaway build");
    let norm = Arc::clone(&real.norm);
    let dict = Arc::clone(&real.dict);
    let tag_dict = Arc::clone(&real.tag_dict);

    let make_cluster = || {
        let fail = Arc::new(AtomicBool::new(false));
        let shards: Vec<Box<dyn Shard>> = (0..cfg.num_shards)
            .map(|_| {
                let ls = LocalShard::new(
                    Arc::clone(&norm),
                    Arc::clone(&dict),
                    Arc::clone(&tag_dict),
                    cfg.per_shard.clone(),
                );
                Box::new(ToggleFailShard::new(ls, Arc::clone(&fail))) as Box<dyn Shard>
            })
            .collect();
        let ring = HashRing::new(cfg.num_shards, cfg.vnodes).expect("ring");
        let durable =
            ClusterDurable::in_memory(cfg.num_shards as u32, cfg.vnodes, dict.fingerprint());
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
        (cluster, fail)
    };

    // A failed initial bulk load reserves its ids before the first shard write.
    // Even though this injected failure landed no row, the coordinator cannot
    // generally know whether a remote multi-shard load was partial. The reserved
    // id remains present, and the revoked convergence authority now fails every
    // insert-only add before duplicate admission.
    let (bulk_cluster, bulk_fail) = make_cluster();
    bulk_fail.store(true, Ordering::Release);
    let failed_bulk = vec![(88u64, "zznovelaterm".to_string())];
    assert!(matches!(
        bulk_cluster.ingest(&failed_bulk),
        Err(ShardError::Remote(_))
    ));
    assert!(bulk_cluster.contains_logical_id(88));
    assert!(!bulk_cluster.logical_ids_authoritative());
    assert!(matches!(
        bulk_cluster.add_query(88, "zznovelaterm"),
        Err(ShardError::Config(ref detail)) if detail.contains("upsert_query")
    ));

    // Fail a regular add, then resync while STILL failing — the mutation must stay queued.
    let (cluster, fail) = make_cluster();
    fail.store(true, Ordering::Release);
    assert!(matches!(
        cluster.add_query(7, "zznovelaterm"),
        Err(ShardError::PartiallyApplied { .. })
    ));
    let report = cluster.resync();
    assert_eq!(
        report.repaired, 0,
        "nothing converges while the shard fails"
    );
    assert_eq!(report.still_pending, 1, "the mutation must remain queued");
    assert_eq!(
        cluster.pending_repairs(),
        1,
        "still queued after a failed resync"
    );

    // Recover and resync again — now it converges and the queue drains.
    fail.store(false, Ordering::Release);
    assert_eq!(cluster.resync().repaired, 1);
    assert_eq!(cluster.pending_repairs(), 0);
    assert!(cluster
        .percolate("zznovelaterm")
        .expect("percolate")
        .contains(&7));
}
