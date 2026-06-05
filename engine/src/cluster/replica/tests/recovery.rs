//! Peer recovery + durable restart: `peer_recover` reproduces a durable primary's set
//! (tombstones baked), the no-quiesce translog-tail catch-up (ADR-039), a durable shard
//! self-restarting from its translog, and the ADR-040 finalize that promotes a
//! peer-recovered replica into the in-sync set at runtime.

use crate::exact::TagPredicate;

use super::super::test_support::*;
use super::super::*;

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
