//! Read failover + write fan-out/ack/aggregation: the in-memory composite behavior
//! (no durability). Reads serve the primary and fail over transport-only to in-sync
//! replicas; writes are primary-authoritative with replicas tolerated-and-flagged;
//! aggregation presents the primary's single-copy view.

use crate::exact::TagPredicate;

use super::super::test_support::*;
use super::super::*;

#[test]
fn read_fails_over_to_in_sync_replica() {
    let (norm, dict, tag_dict, corpus) =
        compile_corpus(&[(1, "alpha bravo"), (2, "charlie delta")]);
    let replica = LocalShard::new(
        Arc::clone(&norm),
        Arc::clone(&dict),
        Arc::clone(&tag_dict),
        EngineConfig::default(),
    );
    seed(&replica, &corpus);
    let rs = ReplicatedShard::new(
        Box::new(FailingShard::reads_remote()) as Box<dyn Shard>,
        vec![Box::new(replica) as Box<dyn Shard>],
    );

    // Primary errors on read (transport); the composite fails over to the in-sync replica.
    let (ids, _) = rs
        .percolate_filtered("alpha bravo zulu", false, &TagPredicate::empty())
        .expect("failover read");
    assert!(
        ids.contains(&1),
        "failover must return the replica's match: {ids:?}"
    );

    // Drop the replica out of sync: with no healthy copy the read must ERR, never return
    // an empty set (that would be a silent false negative).
    rs.replica_handles()[0]
        .in_sync
        .store(false, Ordering::Release);
    assert!(
        matches!(
            rs.percolate_filtered("alpha bravo zulu", false, &TagPredicate::empty()),
            Err(ShardError::Remote(_))
        ),
        "with no in-sync copy the read must surface an error, not an empty set"
    );
}

#[test]
fn read_does_not_fail_over_on_dict_mismatch() {
    let (norm, dict, tag_dict, corpus) = compile_corpus(&[(1, "alpha bravo")]);
    let replica = LocalShard::new(
        Arc::clone(&norm),
        Arc::clone(&dict),
        Arc::clone(&tag_dict),
        EngineConfig::default(),
    );
    seed(&replica, &corpus);
    let rs = ReplicatedShard::new(
        Box::new(FailingShard::reads_dict_mismatch()) as Box<dyn Shard>,
        vec![Box::new(replica) as Box<dyn Shard>],
    );
    // A DictMismatch is structural: it must propagate, not fail over to the (matching)
    // replica — failing over would mask a divergent feature space, itself a silent-FN hazard.
    assert!(
        matches!(
            rs.percolate_filtered("alpha bravo zulu", false, &TagPredicate::empty()),
            Err(ShardError::DictMismatch { .. })
        ),
        "DictMismatch must propagate without failover"
    );
}

#[test]
fn primary_write_failure_propagates() {
    let (norm, dict, tag_dict, corpus) = compile_corpus(&[(1, "alpha bravo")]);
    let healthy = LocalShard::new(
        Arc::clone(&norm),
        Arc::clone(&dict),
        Arc::clone(&tag_dict),
        EngineConfig::default(),
    );
    let rs = ReplicatedShard::new(
        Box::new(FailingShard::writes_fail()) as Box<dyn Shard>,
        vec![Box::new(healthy) as Box<dyn Shard>],
    );
    let (id, ex, dsl) = &corpus[0];
    assert!(
        rs.insert_extracted_with_tags(ex, *id, 1, dsl, &[]).is_err(),
        "a primary write failure must fail the op"
    );
}

#[test]
fn replica_write_failure_is_tolerated_and_flagged() {
    let (norm, dict, tag_dict, corpus) = compile_corpus(&[(1, "alpha bravo")]);
    let primary = LocalShard::new(
        Arc::clone(&norm),
        Arc::clone(&dict),
        Arc::clone(&tag_dict),
        EngineConfig::default(),
    );
    let rs = ReplicatedShard::new(
        Box::new(primary) as Box<dyn Shard>,
        vec![Box::new(FailingShard::writes_fail()) as Box<dyn Shard>],
    );
    // Capture surfaced events (avoid requiring EngineEvent: Clone — record a flag).
    let saw_desync = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&saw_desync);
    rs.set_event_sink(Arc::new(move |ev: &EngineEvent| {
        if matches!(
            ev,
            EngineEvent::DurabilityFailure {
                op: DurabilityOp::ReplicaDesync,
                ..
            }
        ) {
            flag.store(true, Ordering::Release);
        }
    }));

    let (id, ex, dsl) = &corpus[0];
    assert!(
        rs.insert_extracted_with_tags(ex, *id, 1, dsl, &[]).is_ok(),
        "a replica write failure must not fail the op (primary is authoritative)"
    );
    assert!(
        !rs.replica_handles()[0].in_sync.load(Ordering::Acquire),
        "the failed replica must drop out of the in-sync set"
    );
    assert!(
        saw_desync.load(Ordering::Acquire),
        "a ReplicaDesync event must be surfaced"
    );
    assert!(
        rs.percolate_filtered("alpha bravo zulu", true, &TagPredicate::empty())
            .is_ok(),
        "the primary still serves reads after a replica desyncs"
    );
}

#[test]
fn replicas_stay_set_equal_through_op_stream() {
    let (norm, dict, tag_dict, corpus) = compile_corpus(&[
        (1, "alpha bravo"),
        (2, "charlie delta"),
        (3, "echo foxtrot"),
    ]);
    let primary = LocalShard::new(
        Arc::clone(&norm),
        Arc::clone(&dict),
        Arc::clone(&tag_dict),
        EngineConfig::default(),
    );
    let replica = LocalShard::new(
        Arc::clone(&norm),
        Arc::clone(&dict),
        Arc::clone(&tag_dict),
        EngineConfig::default(),
    );
    let rs = ReplicatedShard::new(Box::new(primary), vec![Box::new(replica)]);

    // Drive a mixed op stream through the composite.
    for (id, ex, dsl) in &corpus {
        rs.insert_extracted_with_tags(ex, *id, 1, dsl, &[])
            .expect("insert");
    }
    rs.delete_by_logical_id(2).expect("delete");

    // Primary and replica must hold the same live set.
    assert_eq!(
        rs.primary.num_queries().expect("primary count"),
        rs.replica_handles()[0]
            .shard
            .num_queries()
            .expect("replica count"),
        "primary and replica query counts diverged"
    );
    for title in [
        "alpha bravo zulu",
        "charlie delta zulu",
        "echo foxtrot zulu",
        "nothing here",
    ] {
        let (mut p, _) = rs
            .primary
            .percolate_filtered(title, true, &TagPredicate::empty())
            .expect("primary read");
        let (mut r, _) = rs.replica_handles()[0]
            .shard
            .percolate_filtered(title, true, &TagPredicate::empty())
            .expect("replica read");
        p.sort_unstable();
        r.sort_unstable();
        assert_eq!(p, r, "primary and replica diverged on {title:?}");
    }
    // id 2 was deleted on the primary (and, by fan-out, the replica).
    let (deleted_probe, _) = rs
        .primary
        .percolate_filtered("charlie delta zulu", true, &TagPredicate::empty())
        .expect("read");
    assert!(
        !deleted_probe.contains(&2),
        "the deleted id must be gone on the primary"
    );
}

#[test]
fn aggregation_is_primary_only() {
    // num_queries / class_counts reflect ONE copy (not summed across replicas), so the
    // coordinator's cross-position sums stay correct at RF>1.
    let (norm, dict, tag_dict, corpus) =
        compile_corpus(&[(1, "alpha bravo"), (2, "charlie delta")]);
    let primary = LocalShard::new(
        Arc::clone(&norm),
        Arc::clone(&dict),
        Arc::clone(&tag_dict),
        EngineConfig::default(),
    );
    let replica = LocalShard::new(
        Arc::clone(&norm),
        Arc::clone(&dict),
        Arc::clone(&tag_dict),
        EngineConfig::default(),
    );
    let rs = ReplicatedShard::new(Box::new(primary), vec![Box::new(replica)]);
    for (id, ex, dsl) in &corpus {
        rs.insert_extracted_with_tags(ex, *id, 1, dsl, &[])
            .expect("insert");
    }
    assert_eq!(
        rs.num_queries().expect("count"),
        2,
        "num_queries must be the primary's (2), not summed across copies (4)"
    );
    assert_eq!(
        rs.class_counts().expect("class counts").iter().sum::<u64>(),
        2,
        "class counts must total the primary's queries (2), not summed across copies (4)"
    );
    let removed = rs.delete_by_logical_id(1).expect("delete");
    assert_eq!(
        removed, 1,
        "delete count must be the primary's (1), not summed across copies"
    );
}
