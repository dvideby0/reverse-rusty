//! `ReplicatedShard` unit tests (read failover, write fan-out/ack, peer recovery, durability).
//! Shared fixtures live in [`super::test_support`].

use std::time::{Duration, Instant};

use crate::cluster::clog::ClusterMutation;
use crate::exact::TagPredicate;

use super::test_support::*;
use super::*;

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

#[test]
fn peer_recover_reproduces_primary_set_including_tombstone() {
    let (norm, dict, tag_dict, corpus) = compile_corpus(&[
        (1, "alpha bravo"),
        (2, "charlie delta"),
        (3, "echo foxtrot"),
    ]);
    let tmp = scratch_dir("recover");
    let primary_dir = tmp.join("primary");
    let replica_dir = tmp.join("replica");

    // Durable primary: seed, flush to a base segment, then delete id 2 (a BASE tombstone,
    // so peer recovery's reseal must bake it in — else id 2 would resurrect).
    let pc = EngineConfig {
        data_dir: Some(primary_dir.clone()),
        ..EngineConfig::default()
    };
    let primary = LocalShard::new_durable(
        Arc::clone(&norm),
        Arc::clone(&dict),
        Arc::clone(&tag_dict),
        pc,
    )
    .expect("durable primary");
    seed(&primary, &corpus);
    primary.flush().expect("flush to base");
    primary.delete_by_logical_id(2).expect("delete id 2");

    let (replica, _hwm) = peer_recover(
        &norm,
        &dict,
        &tag_dict,
        EngineConfig::default(),
        &primary,
        &primary_dir,
        &replica_dir,
    )
    .expect("peer recovery");

    for title in [
        "alpha bravo zulu",
        "charlie delta zulu",
        "echo foxtrot zulu",
    ] {
        let (mut p, _) = primary
            .percolate_filtered(title, true, &TagPredicate::empty())
            .expect("primary read");
        let (mut r, _) = replica
            .percolate_filtered(title, true, &TagPredicate::empty())
            .expect("replica read");
        p.sort_unstable();
        r.sort_unstable();
        assert_eq!(p, r, "recovered replica diverged on {title:?}");
    }
    let (probe, _) = replica
        .percolate_filtered("charlie delta zulu", true, &TagPredicate::empty())
        .expect("read");
    assert!(
        !probe.contains(&2),
        "the baked tombstone must not resurrect on the recovered replica"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn peer_recover_replays_tail_without_quiescing() {
    // The headline in-process property (ADR-039): a segment snapshot is taken at position
    // `P`, writes land AFTER it (id 10 added, id 1 removed — in the primary's translog,
    // > P), and the recovering replica catches them up via the TRANSLOG TAIL — no segment
    // re-copy, no quiesce. Ordered (snapshot → write → catch-up) for determinism; it
    // exercises the exact path a concurrent recovery uses for writes that arrive during the
    // copy window. The pre-catch-up staleness assertion proves the writes truly post-date
    // the snapshot (else the test would pass trivially).
    let (norm, dict, tag_dict, corpus) = compile_corpus(&[
        (1, "alpha bravo"),
        (2, "charlie delta"),
        (3, "echo foxtrot"),
        (10, "alpha bravo"),
    ]);
    let tmp = scratch_dir("tail");
    let primary_dir = tmp.join("primary");
    let replica_dir = tmp.join("replica");

    let pc = EngineConfig {
        data_dir: Some(primary_dir.clone()),
        ..EngineConfig::default()
    };
    let primary = LocalShard::new_durable(
        Arc::clone(&norm),
        Arc::clone(&dict),
        Arc::clone(&tag_dict),
        pc,
    )
    .expect("durable primary");
    // The snapshot corpus = ids 1..3 (id 10 is held back for a post-snapshot add).
    for (id, ex, dsl) in corpus.iter().take(3) {
        primary
            .insert_extracted_with_tags(ex, *id, 1, dsl, &[])
            .expect("seed");
    }

    // Snapshot: peer_recover seals the primary at P, copies segments, replays the (empty)
    // tail; `hwm` is the position the replica is caught up to in the primary's log space.
    let (replica, hwm) = peer_recover(
        &norm,
        &dict,
        &tag_dict,
        EngineConfig::default(),
        &primary,
        &primary_dir,
        &replica_dir,
    )
    .expect("peer recovery");

    // Writes that land AFTER the snapshot (into the primary's translog, > hwm).
    let (_, ex10, dsl10) = &corpus[3]; // id 10, "alpha bravo"
    primary
        .insert_extracted_with_tags(ex10, 10, 1, dsl10, &[])
        .expect("post-snapshot add");
    primary
        .delete_by_logical_id(1)
        .expect("post-snapshot delete");

    // Pre-catch-up the replica is STALE (still has id 1, lacks id 10): the writes truly
    // post-date the copied snapshot.
    let (pre, _) = replica
        .percolate_filtered("alpha bravo zulu", true, &TagPredicate::empty())
        .expect("read");
    assert!(
        pre.contains(&1) && !pre.contains(&10),
        "replica must be stale before catch-up (proving writes post-date the snapshot): {pre:?}"
    );

    // Replay the tail (ops > hwm) — the no-quiesce recovery delta.
    catch_up_replica(&replica, &primary, &norm, &dict, hwm).expect("catch up");

    // The replica now equals the primary on every probe: id 10 present, id 1 gone.
    for title in [
        "alpha bravo zulu",
        "charlie delta zulu",
        "echo foxtrot zulu",
    ] {
        let (mut p, _) = primary
            .percolate_filtered(title, true, &TagPredicate::empty())
            .expect("primary");
        let (mut r, _) = replica
            .percolate_filtered(title, true, &TagPredicate::empty())
            .expect("replica");
        p.sort_unstable();
        r.sort_unstable();
        assert_eq!(
            p, r,
            "replica diverged from primary on {title:?} after catch-up"
        );
    }
    let (after, _) = replica
        .percolate_filtered("alpha bravo zulu", true, &TagPredicate::empty())
        .expect("read");
    assert!(
        after.contains(&10) && !after.contains(&1),
        "the translog tail was not applied on catch-up: {after:?}"
    );
    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn durable_shard_self_restarts_from_translog() {
    // ADR-039 §6: a durable data node crashes with un-sealed writes in its translog and
    // restarts from disk — `new_durable` finds the checkpoint sidecar, attaches the committed
    // segments AND replays the translog tail (the ops the last seal had not yet baked). The
    // reopened shard equals the pre-crash live set, with a removed id NOT resurrecting.
    let (norm, dict, tag_dict, corpus) = compile_corpus(&[
        (1, "alpha bravo"),
        (2, "charlie delta"),
        (3, "echo foxtrot"),
        (4, "golf hotel"),
    ]);
    let tmp = scratch_dir("selfrestart");
    let cfg = EngineConfig {
        data_dir: Some(tmp.clone()),
        ..EngineConfig::default()
    };

    {
        let shard = LocalShard::new_durable(
            Arc::clone(&norm),
            Arc::clone(&dict),
            Arc::clone(&tag_dict),
            cfg.clone(),
        )
        .expect("durable shard");
        // Sealed base: ids 1, 2 (flushed into a segment; the sidecar commits at position P).
        shard
            .insert_extracted_with_tags(&corpus[0].1, 1, 1, &corpus[0].2, &[])
            .expect("ins 1");
        shard
            .insert_extracted_with_tags(&corpus[1].1, 2, 1, &corpus[1].2, &[])
            .expect("ins 2");
        shard.seal_for_checkpoint().expect("seal");
        // Un-sealed translog tail (> P): add 3, add 4, remove 1 — only in the translog.
        shard
            .insert_extracted_with_tags(&corpus[2].1, 3, 1, &corpus[2].2, &[])
            .expect("ins 3");
        shard
            .insert_extracted_with_tags(&corpus[3].1, 4, 1, &corpus[3].2, &[])
            .expect("ins 4");
        shard.delete_by_logical_id(1).expect("del 1");
        // "Crash": drop without another seal — the tail lives only in the translog.
    }

    // Restart from the sidecar: attach segments (1, 2) + replay the tail (add 3, add 4,
    // remove 1) → live set {2, 3, 4}.
    let reopened = LocalShard::new_durable(
        Arc::clone(&norm),
        Arc::clone(&dict),
        Arc::clone(&tag_dict),
        cfg,
    )
    .expect("self-restart");
    let probe = |title: &str| -> Vec<u64> {
        let (mut ids, _) = reopened
            .percolate_filtered(title, true, &TagPredicate::empty())
            .expect("read");
        ids.sort_unstable();
        ids
    };
    assert_eq!(
        probe("alpha bravo zulu"),
        Vec::<u64>::new(),
        "id 1 was removed in the tail; it must not resurrect on self-restart"
    );
    assert_eq!(probe("charlie delta zulu"), vec![2], "sealed id 2 survives");
    assert_eq!(
        probe("echo foxtrot zulu"),
        vec![3],
        "tail add id 3 recovered"
    );
    assert_eq!(probe("golf hotel zulu"), vec![4], "tail add id 4 recovered");
    // Physical entry count: 2 sealed (ids 1, 2) + 2 tail adds (ids 3, 4). id 1's sealed entry
    // is tombstoned (the matching probes above prove it is excluded), not yet compacted away —
    // exactly what a non-restarted shard applying the same ops reports.
    assert_eq!(
        reopened.num_queries().expect("count"),
        4,
        "physical count = 2 sealed + 2 tail (id 1 tombstoned, awaiting compaction)"
    );
    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn seal_honors_retention_lease_so_concurrent_seal_keeps_the_recovery_tail() {
    // ADR-040: a peer recovery acquires a retention lease at position `at`, then more writes
    // land and the source SEALS AGAIN (a concurrent checkpoint, or another recovery's
    // FetchSegments). Without the lease that seal would trim the translog to its new `P`,
    // erasing the tail (> at) the in-flight recovery still needs — a silent false negative.
    // With the lease the seal trims only to `at`, so the tail survives; releasing it lets the
    // source GC again. (This is the latent FN ADR-039's no-quiesce path left open.)
    let (norm, dict, tag_dict, corpus) = compile_corpus(&[
        (1, "alpha bravo"),
        (2, "charlie delta"),
        (3, "echo foxtrot"),
    ]);
    let dir = scratch_dir("retain");
    let cfg = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let primary = LocalShard::new_durable(
        Arc::clone(&norm),
        Arc::clone(&dict),
        Arc::clone(&tag_dict),
        cfg,
    )
    .expect("durable primary");
    // Seed id 1 and seal a base — the recovery baseline.
    primary
        .insert_extracted_with_tags(&corpus[0].1, 1, 1, &corpus[0].2, &[])
        .expect("ins 1");
    let at_seal = primary.seal_for_checkpoint().expect("seal 1");

    // The recovery pins the tail at the current high-water.
    let (lease, at) = primary.acquire_retention_lease().expect("lease");
    assert_eq!(at, at_seal, "lease pins the post-seal high-water");

    // Writes land AFTER the snapshot (into the translog, > at).
    primary
        .insert_extracted_with_tags(&corpus[1].1, 2, 1, &corpus[1].2, &[])
        .expect("ins 2");
    primary
        .insert_extracted_with_tags(&corpus[2].1, 3, 1, &corpus[2].2, &[])
        .expect("ins 3");

    // A concurrent seal: WITHOUT the lease it would trim to its new P and drop (at, P]; the
    // lease holds the floor at `at`, so the tail the recovery needs survives.
    let p1 = primary.seal_for_checkpoint().expect("seal 2");
    assert!(
        p1 > at,
        "the second seal advanced the checkpoint past the pinned point"
    );
    let tail = primary.translog_tail(at).expect("tail");
    let ids: Vec<u64> = tail
        .iter()
        .map(|(_, m)| match m {
            ClusterMutation::Add { logical, .. } | ClusterMutation::Remove { logical } => *logical,
        })
        .collect();
    assert_eq!(
        ids,
        vec![2, 3],
        "the lease kept the post-snapshot tail (> at)"
    );

    // Release: the next seal trims freely again (GC), so the pinned tail is now gone.
    primary.release_retention_lease(lease).expect("release");
    primary.seal_for_checkpoint().expect("seal 3");
    assert!(
        primary
            .translog_tail(at)
            .expect("tail after release")
            .is_empty(),
        "a released lease lets the source GC the consumed tail"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ttl_reaps_a_stuck_lease_so_the_seal_reclaims_the_tail_and_emits() {
    // ADR-048: ADR-040's lease keeps a recovery's tail across a concurrent seal, but a CRASHED
    // recovering node would otherwise pin that tail forever. A TTL reaps a lease that has not
    // heartbeated within the window, so the source reclaims the abandoned tail — and surfaces the
    // reap as an event (the recovery was abandoned, not silently dropped). `renew` is the
    // heartbeat, so a live recovery is never reaped (the unit tests cover the renew case).
    let (norm, dict, tag_dict, corpus) = compile_corpus(&[
        (1, "alpha bravo"),
        (2, "charlie delta"),
        (3, "echo foxtrot"),
    ]);
    let dir = scratch_dir("lease_ttl");
    // A small, non-whole-minute TTL: matches the cfg below so the injected `now` (ttl + 1s past
    // acquire) trips the reap, and dodges the `duration_suboptimal_units` lint.
    let ttl = Duration::from_secs(100);
    let cfg = EngineConfig {
        data_dir: Some(dir.clone()),
        retention_lease_ttl_secs: 100,
        ..EngineConfig::default()
    };
    let primary = LocalShard::new_durable(
        Arc::clone(&norm),
        Arc::clone(&dict),
        Arc::clone(&tag_dict),
        cfg,
    )
    .expect("durable primary");

    // Capture the reap event (ADR-021/048): a plain LocalShard now honors the coordinator's sink.
    let saw_reap = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&saw_reap);
    primary.set_event_sink(Arc::new(move |ev: &EngineEvent| {
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

    primary
        .insert_extracted_with_tags(&corpus[0].1, 1, 1, &corpus[0].2, &[])
        .expect("ins 1");
    let at_seal = primary.seal_for_checkpoint().expect("seal 1");

    // A recovery pins the tail; writes land after it (> at).
    let (_lease, at) = primary.acquire_retention_lease().expect("lease");
    assert_eq!(at, at_seal, "lease pins the post-seal high-water");
    primary
        .insert_extracted_with_tags(&corpus[1].1, 2, 1, &corpus[1].2, &[])
        .expect("ins 2");
    primary
        .insert_extracted_with_tags(&corpus[2].1, 3, 1, &corpus[2].2, &[])
        .expect("ins 3");

    // Control: a seal while the lease is LIVE (now within the window) keeps the tail (ADR-040)
    // and reaps nothing.
    let p1 = primary
        .seal_for_checkpoint_at(Instant::now())
        .expect("live seal");
    assert!(
        p1 > at,
        "the seal advanced the checkpoint past the pinned point"
    );
    assert!(
        !primary.translog_tail(at).expect("tail").is_empty(),
        "a live lease still holds the tail across a seal"
    );
    assert!(
        !saw_reap.load(Ordering::Acquire),
        "no reap while the lease is within the TTL window"
    );

    // The recovery crashes: it stops heartbeating. Simulate the TTL elapsing by sealing as of a
    // synthetic `now` past the window (deterministic — no sleep). The stale lease is reaped, the
    // tail is reclaimed, and the reap is surfaced as an event.
    primary
        .seal_for_checkpoint_at(Instant::now() + ttl + Duration::from_secs(1))
        .expect("expiring seal");
    assert!(
        primary
            .translog_tail(at)
            .expect("tail after reap")
            .is_empty(),
        "the reaped lease no longer pins the tail; the seal reclaimed it"
    );
    assert!(
        saw_reap.load(Ordering::Acquire),
        "a stuck-lease reap must surface a ReplicaDesync event"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn add_recovered_replica_promotes_an_in_sync_set_equal_replica() {
    // ADR-040 finalize: add a replica to a live position at runtime — peer-recover + converge +
    // promote under a brief quiesce. The promoted replica is in-sync (a later write fans out to
    // it) and set-equal to the primary.
    let (norm, dict, tag_dict, corpus) = compile_corpus(&[
        (1, "alpha bravo"),
        (2, "charlie delta"),
        (3, "golf hotel"), // written AFTER promotion, so the frozen dict must already know it
    ]);
    let tmp = scratch_dir("addrep");
    let primary_dir = tmp.join("primary");
    let replica_dir = tmp.join("replica");
    let pc = EngineConfig {
        data_dir: Some(primary_dir.clone()),
        ..EngineConfig::default()
    };
    let primary = LocalShard::new_durable(
        Arc::clone(&norm),
        Arc::clone(&dict),
        Arc::clone(&tag_dict),
        pc,
    )
    .expect("durable primary");
    primary
        .insert_extracted_with_tags(&corpus[0].1, 1, 1, &corpus[0].2, &[])
        .expect("ins 1");
    primary
        .insert_extracted_with_tags(&corpus[1].1, 2, 1, &corpus[1].2, &[])
        .expect("ins 2");

    // A composite with the durable primary and NO replicas yet; grow one at runtime.
    let rs = ReplicatedShard::new(Box::new(primary), vec![]);
    rs.add_recovered_replica(
        &norm,
        &dict,
        &tag_dict,
        EngineConfig::default(),
        &primary_dir,
        &replica_dir,
        8,
    )
    .expect("add replica");

    assert_eq!(rs.replica_handles().len(), 1, "one replica promoted");
    assert!(
        rs.replica_handles()[0].in_sync.load(Ordering::Acquire),
        "the promoted replica is in the in-sync set"
    );

    // A write AFTER promotion must fan out to the new replica (proof it is truly in-sync).
    rs.insert_extracted_with_tags(&corpus[2].1, 3, 1, &corpus[2].2, &[])
        .expect("post-promotion write");

    let replica = rs.replica_handles()[0].clone();
    for title in ["alpha bravo zulu", "charlie delta zulu", "golf hotel zulu"] {
        let (mut p, _) = rs
            .primary
            .percolate_filtered(title, true, &TagPredicate::empty())
            .expect("primary");
        let (mut r, _) = replica
            .shard
            .percolate_filtered(title, true, &TagPredicate::empty())
            .expect("replica");
        p.sort_unstable();
        r.sort_unstable();
        assert_eq!(
            p, r,
            "replica diverged from primary on {title:?} after promotion"
        );
    }
    let (probe, _) = replica
        .shard
        .percolate_filtered("golf hotel zulu", true, &TagPredicate::empty())
        .expect("read");
    assert!(
        probe.contains(&3),
        "the post-promotion write must have fanned out to the in-sync replica: {probe:?}"
    );
    let _ = std::fs::remove_dir_all(&tmp);
}
