use super::*;

/// Distributed local-K/global-K assumes one semantic row per logical id. Query
/// placement is content-derived, so two different rows under one id can have no
/// common routed owner. Reject the invalid state at every cluster load boundary;
/// callers use the existing atomic upsert API to replace an id.
#[test]
fn cluster_load_boundaries_reject_duplicate_logical_ids() {
    let cfg = ClusterConfig {
        num_shards: 5,
        ..Default::default()
    };
    let duplicates = vec![
        (42u64, "1994 topps".to_string()),
        (42u64, "1995 fleer".to_string()),
    ];
    assert!(matches!(
        ClusterEngine::build(vocab(), &cfg, &duplicates),
        Err(ShardError::DuplicateLogicalId(42))
    ));

    let empty = ClusterEngine::build(vocab(), &cfg, &[]).expect("empty cluster");
    assert!(matches!(
        empty.ingest(&duplicates),
        Err(ShardError::DuplicateLogicalId(42))
    ));
    assert_eq!(empty.num_queries().expect("count"), 0);
}

#[test]
fn incremental_add_is_insert_only_and_remove_allows_reuse() {
    let cfg = ClusterConfig {
        num_shards: 8,
        ..Default::default()
    };
    let seed = vec![(42u64, "1994 topps rareplayer0".to_string())];
    let cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("cluster");

    assert!(matches!(
        cluster.add_query(42, "1995 fleer rareplayer1000"),
        Err(ShardError::DuplicateLogicalId(42))
    ));
    assert_eq!(
        cluster.percolate("1994 topps rareplayer0").expect("old"),
        vec![42]
    );
    assert!(cluster
        .percolate("1995 fleer rareplayer1000")
        .expect("rejected new")
        .is_empty());

    cluster.remove_query(42).expect("remove");
    cluster
        .add_query(42, "1995 fleer rareplayer1000")
        .expect("id can be reused after delete");
    assert!(cluster
        .percolate("1994 topps rareplayer0")
        .expect("old removed")
        .is_empty());
    assert_eq!(
        cluster.percolate("1995 fleer rareplayer1000").expect("new"),
        vec![42]
    );
}

#[test]
fn versioned_create_is_insert_only_and_preserves_source_metadata() {
    let cfg = ClusterConfig {
        num_shards: 4,
        ..Default::default()
    };
    let cluster = ClusterEngine::build(vocab(), &cfg, &[]).expect("cluster");
    let tags = vec![("tenant".to_string(), "acme".to_string())];
    cluster
        .create_query_with_tags(43, "1994 topps rareplayer0", 9, &tags)
        .expect("fresh create");

    let source = cluster
        .get_document(43)
        .expect("source lookup")
        .expect("created source");
    assert_eq!(source.version(), 9);
    assert_eq!(source.tags(), tags.as_slice());
    assert!(matches!(
        cluster.create_query_with_tags(43, "1995 fleer rareplayer1000", 10, &[]),
        Err(ShardError::DuplicateLogicalId(43))
    ));
    assert_eq!(
        cluster
            .percolate("1994 topps rareplayer0")
            .expect("original remains"),
        vec![43]
    );
    assert!(cluster
        .percolate("1995 fleer rareplayer1000")
        .expect("conflict body absent")
        .is_empty());
}

#[test]
fn concurrent_same_id_adds_admit_exactly_one_row() {
    use std::sync::{Arc, Barrier};

    let cfg = ClusterConfig {
        num_shards: 8,
        ..Default::default()
    };
    let seed = vec![
        (1u64, "1994 topps rareplayer0".to_string()),
        (2u64, "1995 fleer rareplayer1000".to_string()),
    ];
    let cluster = Arc::new(ClusterEngine::build(vocab(), &cfg, &seed).expect("cluster"));
    let barrier = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    for dsl in ["1994 topps rareplayer0", "1995 fleer rareplayer1000"] {
        let cluster = Arc::clone(&cluster);
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            cluster.add_query(99, dsl)
        }));
    }
    barrier.wait();
    let outcomes: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().expect("worker"))
        .collect();
    assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Err(ShardError::DuplicateLogicalId(99))))
            .count(),
        1
    );

    let matches = [
        cluster
            .percolate("1994 topps rareplayer0")
            .expect("first")
            .contains(&99),
        cluster
            .percolate("1995 fleer rareplayer1000")
            .expect("second")
            .contains(&99),
    ];
    assert_eq!(matches.into_iter().filter(|matched| *matched).count(), 1);
}

#[test]
fn reopened_cluster_rebuilds_unique_id_directory() {
    let dir = scratch_dir("logical_ids");
    let cfg = ClusterConfig {
        num_shards: 4,
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    let seed = vec![(77u64, "1994 topps rareplayer0".to_string())];
    let cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("build");
    cluster.checkpoint().expect("checkpoint");
    drop(cluster);

    let reopened = ClusterEngine::open(dir.clone(), vocab(), Some(&cfg)).expect("open");
    assert!(matches!(
        reopened.add_query(77, "1995 fleer rareplayer1000"),
        Err(ShardError::DuplicateLogicalId(77))
    ));
    reopened.remove_query(77).expect("remove");
    reopened
        .add_query(77, "1995 fleer rareplayer1000")
        .expect("reuse after remove");
    assert_eq!(
        reopened
            .percolate("1995 fleer rareplayer1000")
            .expect("match"),
        vec![77]
    );
    drop(reopened);
    let _ = std::fs::remove_dir_all(dir);
}

/// The three `_owned` read paths share one routed-membership guard
/// (`OwnershipContext::require_routed`): a request naming a position outside the
/// routed set (or the shard space) must fail loud with `LocalPositionMissing`,
/// never silently emit zero rows. The ranked path originally omitted the guard —
/// an unrouted position can never equal `owner()`, so it returned an empty scored
/// set where its siblings error (review finding).
#[test]
fn owned_read_paths_fail_loud_on_unrouted_position() {
    let cfg = ClusterConfig {
        num_shards: 4,
        ..Default::default()
    };
    let seed = vec![(100u64, "1994 topps baseball".to_string())];
    let real = ClusterEngine::build(vocab(), &cfg, &seed).expect("throwaway build");
    let shard = LocalShard::new(
        Arc::clone(&real.norm),
        Arc::clone(&real.dict),
        Arc::clone(&real.tag_dict),
        cfg.per_shard.clone(),
    );
    let context = crate::ownership::OwnershipContext::new(
        crate::ownership::PlacementGeneration::INITIAL,
        4,
        vec![0, 2],
        None,
    )
    .expect("context");
    let pred = TagPredicate::empty();
    let spec = crate::rank::CompiledRankSpec::default();
    let program = crate::rank::CompiledRankProgram::default();
    // Position 1 is in-range but unrouted; position 7 is out of the shard space.
    for position in [1u32, 7] {
        let unrouted = |r: Result<(), ShardError>| {
            assert!(
                matches!(
                    r,
                    Err(ShardError::OwnershipMismatch(
                        crate::ownership::OwnershipError::LocalPositionMissing(p)
                    )) if p == position
                ),
                "position {position} must fail loud"
            );
        };
        unrouted(
            shard
                .percolate_filtered_owned("1994 topps baseball", true, &pred, &context, position)
                .map(|_| ()),
        );
        unrouted(
            shard
                .percolate_filtered_ranked_owned(
                    "1994 topps baseball",
                    true,
                    &pred,
                    &spec,
                    &context,
                    position,
                )
                .map(|_| ()),
        );
        unrouted(
            shard
                .percolate_top_k_owned(
                    "1994 topps baseball",
                    true,
                    &pred,
                    &program,
                    crate::result::TopKOptions::default(),
                    &context,
                    position,
                    None,
                )
                .map(|_| ()),
        );
    }
}

/// A coordinator attached to an already-populated cluster it could not enumerate
/// (the gRPC connect shape — `RemoteShard` has no live-id enumeration RPC) must
/// not run insert-only admission against an empty directory: `add_query` fails
/// closed with a `Config` error directing to `upsert_query`, which stays fully
/// usable because it re-drives replace-by-id on every shard (review finding).
#[test]
fn add_query_fails_closed_when_directory_is_unseeded() {
    let cfg = ClusterConfig {
        num_shards: 3,
        ..Default::default()
    };
    let seed = vec![(1u64, "1994 topps".to_string())];
    let cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("build");
    assert!(cluster.logical_ids_authoritative());

    // Simulate the connect-to-populated-cluster shape: corpus present, directory
    // unseeded.
    cluster.unseed_logical_ids_for_test();
    assert!(!cluster.logical_ids_authoritative());
    let mut sink = RecordingExhaustiveSink::default();
    let exhaustive = cluster
        .try_percolate_filtered_all(
            "1994 topps",
            &[],
            crate::result::QueryScope::Standard,
            None,
            1,
            None,
            &mut sink,
        )
        .expect_err("unattested populated attach must not certify exhaustive completion");
    assert!(
        matches!(
            exhaustive,
            ShardError::Protocol(ref detail)
                if detail.contains("authoritative coordinator convergence state")
        ),
        "unexpected exhaustive refusal: {exhaustive}"
    );
    assert!(
        sink.chunks.is_empty(),
        "reattach refusal must precede provisional output"
    );
    let err = cluster.add_query(2, "1994 topps").unwrap_err();
    assert!(
        matches!(err, ShardError::Config(ref m) if m.contains("upsert_query")),
        "expected the fail-closed Config error, got {err:?}"
    );

    // The replacement path is directory-independent and stays available.
    cluster
        .upsert_query(2, "1994 topps", 1)
        .expect("upsert works unseeded");
    assert!(cluster
        .percolate("1994 topps")
        .expect("percolate")
        .contains(&2));
}

/// A partially-applied remove retains its logical-id reservation (fail-closed),
/// and `resync` — the documented repair path — must RELEASE it once the delete
/// converges everywhere. Without the release the id answered 409
/// `DuplicateLogicalId` to every future `add_query` until a coordinator reopen
/// (review finding).
#[test]
fn resync_releases_reservation_after_repairing_a_remove() {
    let cfg = ClusterConfig {
        num_shards: 3,
        ..Default::default()
    };
    let seed = vec![(100u64, "1994 topps baseball".to_string())];
    let real = ClusterEngine::build(vocab(), &cfg, &seed).expect("throwaway build");
    let norm = Arc::clone(&real.norm);
    let dict = Arc::clone(&real.dict);
    let tag_dict = Arc::clone(&real.tag_dict);

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

    let dsl = "zznovelaterm";
    cluster.add_query(5, dsl).expect("healthy add");

    // Fail the delete: the remove is durably logged, partially applied, and the
    // id stays reserved (fail-closed) — a re-add must refuse.
    fail.store(true, Ordering::Release);
    assert!(
        matches!(
            cluster.remove_query(5),
            Err(ShardError::PartiallyApplied { .. })
        ),
        "the remove must partially apply while the shard is failing"
    );
    assert!(
        matches!(
            cluster.add_query(5, dsl),
            Err(ShardError::DuplicateLogicalId(5))
        ),
        "a partially-removed id must stay reserved"
    );

    // The shard recovers; resync converges the delete and must free the id.
    fail.store(false, Ordering::Release);
    let report = cluster.resync();
    assert_eq!(report.repaired, 1, "the queued remove must converge");
    assert_eq!(cluster.pending_repairs(), 0);
    cluster
        .add_query(5, dsl)
        .expect("a repaired remove must release the id for re-add");
    assert!(
        cluster.percolate(dsl).expect("percolate").contains(&5),
        "the re-added query must be matchable"
    );
}

/// `rebuild_from_live` (resize / set_vocab) must rebuild the logical-id directory
/// from the corpus it actually re-placed, mirroring reopen's live-enumeration
/// seeding. Before the fix the directory survived the rebuild untouched, so a
/// reservation with no surviving row (the leak family: a query dropped by
/// re-placement, or a stale reservation) 409'd every re-add on the LIVE
/// coordinator while a REOPENED one accepted it (review finding).
#[test]
fn rebuild_from_live_reseeds_the_logical_id_directory() {
    let cfg = ClusterConfig {
        num_shards: 3,
        ..Default::default()
    };
    let seed = vec![
        (1u64, "1994 topps baseball".to_string()),
        (2u64, "1995 fleer baseball".to_string()),
    ];
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("build");

    // Plant a stale reservation with no live row (the leak shape).
    assert!(cluster.insert_logical_id(99));
    assert!(matches!(
        cluster.add_query(99, "1994 topps baseball"),
        Err(ShardError::DuplicateLogicalId(99))
    ));

    // The rebuild reseeds the directory from the re-placed corpus: live ids stay
    // reserved, the stale one is healed away.
    let rebuilt = cluster.resize(5).expect("resize rebuilds");
    assert_eq!(rebuilt, 2);
    assert!(matches!(
        cluster.add_query(1, "1994 topps baseball"),
        Err(ShardError::DuplicateLogicalId(1))
    ));
    cluster
        .add_query(99, "1994 topps baseball")
        .expect("stale reservation must be healed by the rebuild");
    assert!(cluster
        .percolate("1994 topps baseball")
        .expect("p")
        .contains(&99));
}

/// The multi-machine-harness wedge: an upsert's DELETE half fans to every
/// shard, so a kill window can queue a repair at a position the placement does
/// not store. Re-driving the full upsert there is refused by ADR-109's
/// shard-side placement validation (`LocalPositionMissing`), so `resync` could
/// never converge that mutation. The repair now re-drives only the delete half
/// on uncovered positions.
#[test]
fn resync_converges_an_upsert_queued_at_a_delete_only_shard() {
    let cfg = ClusterConfig {
        num_shards: 3,
        ..Default::default()
    };
    let seed = vec![(100u64, "1994 topps baseball".to_string())];
    let real = ClusterEngine::build(vocab(), &cfg, &seed).expect("throwaway build");
    let norm = Arc::clone(&real.norm);
    let dict = Arc::clone(&real.dict);
    let tag_dict = Arc::clone(&real.tag_dict);

    let fail = Arc::new(AtomicBool::new(false));
    let shards: Vec<Box<dyn Shard>> = (0..cfg.num_shards)
        .map(|position| {
            let ls = LocalShard::new(
                Arc::clone(&norm),
                Arc::clone(&dict),
                Arc::clone(&tag_dict),
                cfg.per_shard.clone(),
            );
            // Position-aware: reproduce the gRPC server's coverage validation,
            // which is what wedged the harness (LocalShard alone cannot).
            Box::new(ToggleFailShard::with_position(
                ls,
                Arc::clone(&fail),
                position as u32,
            )) as Box<dyn Shard>
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

    // A selective (single-shard) query: its upsert's delete half still fans to
    // ALL THREE shards, so two positions are delete-only.
    let dsl = "zznovelaterm";
    cluster.add_query(5, dsl).expect("healthy add");

    // Fail everything mid-upsert: delete-only positions land in the repair
    // queue holding the FULL upsert mutation.
    fail.store(true, Ordering::Release);
    assert!(
        cluster.upsert_query(5, dsl, 2).is_err(),
        "the upsert must partially apply while shards are failing"
    );
    assert!(cluster.pending_repairs() > 0, "repairs must be queued");

    // Shards recover: resync must converge EVERY queued mutation, including the
    // delete-only positions (previously wedged on LocalPositionMissing).
    fail.store(false, Ordering::Release);
    let report = cluster.resync();
    assert_eq!(
        report.still_pending, 0,
        "no repair may stay wedged: {report:?}"
    );
    assert_eq!(cluster.pending_repairs(), 0);
    assert!(
        cluster.percolate(dsl).expect("percolate").contains(&5),
        "the upserted query must be matchable after convergence"
    );
}
