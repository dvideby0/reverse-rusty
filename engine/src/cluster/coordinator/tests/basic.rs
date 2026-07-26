use super::*;

#[test]
fn add_query_is_fail_closed_when_log_append_fails() {
    let dir = scratch_dir("failclosed");
    let cfg = ClusterConfig {
        num_shards: 3,
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    // Build over a seed corpus so the frozen dict knows these tokens.
    let seed = vec![(1u64, "1994 topps".to_string())];
    let cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("durable cluster builds");
    let before = cluster.num_queries().expect("count");

    // Break the durable log, then attempt an add of an in-vocabulary query.
    cluster.log.break_writes_for_test();
    let res = cluster.add_query(2, "1995 fleer");
    assert!(
        matches!(res, Err(ShardError::Log(_))),
        "expected Log error, got {res:?}"
    );

    // No shard was mutated: count unchanged and id 2 is not matchable.
    assert_eq!(cluster.num_queries().expect("count"), before);
    let hits = cluster.percolate("1995 fleer").expect("percolate");
    assert!(
        !hits.contains(&2),
        "rejected add must not be matchable: {hits:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A create racing a provisional same-id reservation must wait for the
/// coordinator-log decision. If that append fails and rolls the reservation
/// back, the waiter creates successfully instead of returning a false 409.
#[test]
fn create_waits_for_a_provisional_reservation_to_commit_or_roll_back() {
    let cfg = ClusterConfig {
        num_shards: 3,
        ..Default::default()
    };
    let seed = vec![(100u64, "1994 topps baseball".to_string())];
    let real = ClusterEngine::build(vocab(), &cfg, &seed).expect("throwaway build");
    let norm = Arc::clone(&real.norm);
    let dict = Arc::clone(&real.dict);
    let tag_dict = Arc::clone(&real.tag_dict);
    let shards: Vec<Box<dyn Shard>> = (0..cfg.num_shards)
        .map(|_| {
            Box::new(LocalShard::new(
                Arc::clone(&norm),
                Arc::clone(&dict),
                Arc::clone(&tag_dict),
                cfg.per_shard.clone(),
            )) as Box<dyn Shard>
        })
        .collect();
    let ring = HashRing::new(cfg.num_shards, cfg.vnodes).expect("ring");
    let gate = Arc::new(FirstAppendGate::default());
    let mut durable =
        ClusterDurable::in_memory(cfg.num_shards as u32, cfg.vnodes, dict.fingerprint());
    durable.log = Box::new(FailFirstAppendLog {
        gate: Arc::clone(&gate),
    });
    let cluster = Arc::new(
        ClusterEngine::from_parts(
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
        .expect("from_parts cluster"),
    );

    let first_cluster = Arc::clone(&cluster);
    let first =
        std::thread::spawn(move || first_cluster.create_query_with_tags(91, "1994 topps", 7, &[]));
    gate.wait_until_entered();

    let (started_tx, started_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let second_cluster = Arc::clone(&cluster);
    let second = std::thread::spawn(move || {
        started_tx.send(()).expect("signal second start");
        let result = second_cluster.create_query_with_tags(91, "1995 fleer", 8, &[]);
        result_tx.send(result).expect("send second result");
    });
    started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("second create started");
    let early_result = result_rx.recv_timeout(Duration::from_millis(100));
    gate.release_first();
    assert!(
        matches!(early_result, Err(mpsc::RecvTimeoutError::Timeout)),
        "the second create must wait for the provisional reservation's log decision"
    );

    assert!(
        matches!(
            first.join().expect("first create thread"),
            Err(ShardError::Log(_))
        ),
        "the first provisional write must fail durability"
    );
    let second_result = result_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("second create completes after rollback");
    assert!(
        matches!(
            second_result,
            Ok(AddOutcome::Placed { .. } | AddOutcome::Replicated)
        ),
        "the waiter must create after rollback, got {second_result:?}"
    );
    second.join().expect("second create thread");

    let source = cluster
        .get_document(91)
        .expect("source lookup")
        .expect("second create is live");
    assert_eq!(source.query(), "1995 fleer");
    assert_eq!(source.version(), 8);
}

/// On-disk fingerprint guard: a manifest whose stored `dict_fingerprint` disagrees with
/// the dict it carries must fail `open` loud with `ShardError::DictMismatch` (ADR-030
/// parity for persisted state), never silently opening a divergent feature space. The
/// manifest is rewritten through `write_cluster_manifest` so its trailing CRC stays valid,
/// which exercises the fingerprint check itself — not the CRC check the integration
/// oracle's `corrupt_manifest_*` test already covers.
#[test]
fn open_rejects_manifest_with_divergent_dict_fingerprint() {
    let dir = scratch_dir("fpmismatch");
    let seed = vec![(1u64, "1994 topps".to_string())];
    let cfg = ClusterConfig {
        num_shards: 3,
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    ClusterEngine::build(vocab(), &cfg, &seed).expect("durable cluster builds");

    // Flip only the stored fingerprint, then rewrite with a fresh (valid) CRC. The dict
    // bytes are untouched, so on open the dict's recomputed fingerprint won't match.
    let mpath = dir.join(CLUSTER_MANIFEST_FILE);
    let mut manifest = crate::storage::read_cluster_manifest(&mpath).expect("read manifest");
    manifest.dict_fingerprint ^= 0xDEAD_BEEF_DEAD_BEEF;
    crate::storage::write_cluster_manifest(&manifest, &mpath).expect("rewrite manifest");

    // ClusterEngine isn't Debug, so match explicitly rather than `{:?}`-printing the Ok arm.
    match ClusterEngine::open(dir.clone(), vocab(), None) {
        Err(ShardError::DictMismatch { .. }) => {}
        Err(other) => panic!("expected DictMismatch, got {other:?}"),
        Ok(_) => panic!("expected DictMismatch, but open() succeeded"),
    }

    let _ = std::fs::remove_dir_all(&dir);
}
