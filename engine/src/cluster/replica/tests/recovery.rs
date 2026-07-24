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
fn durable_shard_replays_an_acknowledged_query_above_default_parse_limits() {
    let norm = Arc::new(Normalizer::default_vocab().expect("normalizer"));
    let query = (0..=crate::dsl::MAX_CLAUSES)
        .map(|i| format!("recoveryterm{i}"))
        .collect::<Vec<_>>()
        .join(" ");
    let limits = crate::dsl::ParseLimits {
        max_clauses: crate::dsl::MAX_CLAUSES + 1,
        ..Default::default()
    };
    let ast = crate::dsl::parse_with_limits(&query, &limits).expect("loose front-door parse");
    let mut dict = Dict::new();
    let mut lc = String::new();
    let ex = crate::compile::extract(&ast, &norm, &mut dict, &mut lc);
    dict.finalize_mask();
    let dict = Arc::new(dict);
    let mut tag_dict = TagDict::new();
    tag_dict.mark_finalized();
    let tag_dict = Arc::new(tag_dict);

    let tmp = scratch_dir("selfrestart_structural_parse_limit");
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
        shard
            .insert_extracted_with_tags(&ex, 1, 1, &query, &[])
            .expect("already-validated query is acknowledged");
        // Leave the query only in the translog tail.
    }

    let reopened = LocalShard::new_durable(norm, dict, tag_dict, cfg)
        .expect("self-restart uses durable structural parse limits");
    let (ids, _) = reopened
        .percolate_filtered(&query, true, &TagPredicate::empty())
        .expect("recovered query matches");
    assert!(
        ids.contains(&1),
        "self-restart must not silently skip an acknowledged query above today's defaults"
    );
    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn durable_shard_self_restart_refuses_legacy_clause_boundary_semantics() {
    let mut vocab = crate::vocab::Vocab::new();
    vocab.import_solr_aliases(
        "ny => new york",
        &Normalizer::default_vocab().expect("normalizer"),
        &Dict::new(),
    );
    let norm = Arc::new(vocab.to_normalizer().expect("alias normalizer"));
    let mut dict = Dict::new();
    let mut lc = String::new();
    let ast = crate::dsl::parse("new -used york").expect("query");
    let ex = crate::compile::extract(&ast, &norm, &mut dict, &mut lc);
    let tail_ast = crate::dsl::parse("alpha bravo").expect("tail query");
    let tail_ex = crate::compile::extract(&tail_ast, &norm, &mut dict, &mut lc);
    dict.finalize_mask();
    let dict = Arc::new(dict);
    let mut tag_dict = TagDict::new();
    tag_dict.mark_finalized();
    let tag_dict = Arc::new(tag_dict);

    let tmp = scratch_dir("selfrestart_clause_migration");
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
        shard
            .insert_extracted_with_tags(&ex, 1, 1, "new -used york", &[])
            .expect("base insert");
        shard.seal_for_checkpoint().expect("seal legacy base");
        shard
            .insert_extracted_with_tags(&tail_ex, 2, 1, "alpha bravo", &[])
            .expect("unsealed tail insert");
    }

    let legacy = crate::cluster::translog::read_sidecar(&tmp)
        .expect("read sidecar")
        .expect("sidecar");
    for name in &legacy.segment_files {
        let path = tmp.join("segments").join(name);
        let mut bytes = std::fs::read(&path).expect("read segment");
        bytes[12..16].copy_from_slice(&0u32.to_le_bytes());
        let body = bytes.len() - 4;
        let crc = crate::storage::crc32(&bytes[..body]);
        bytes[body..].copy_from_slice(&crc.to_le_bytes());
        std::fs::write(path, bytes).expect("write legacy compiler stamp");
    }

    let Err(error) = LocalShard::new_durable(norm, dict, tag_dict, cfg) else {
        panic!("one shard cannot safely rewrite and preserve cluster placement");
    };
    assert!(
        error.to_string().contains("legacy compiler semantics")
            && error.to_string().contains("re-placement"),
        "unexpected refusal: {error}"
    );
    assert_eq!(
        crate::cluster::translog::read_sidecar(&tmp)
            .expect("read unchanged sidecar")
            .expect("sidecar"),
        legacy,
        "a refused shard-local restart must not advance its commit point or consume the tail"
    );
    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn durable_shard_self_restart_refuses_legacy_tail_with_empty_base() {
    let (norm, dict, tag_dict, corpus) = compile_corpus(&[(1, "alpha bravo")]);
    let tmp = scratch_dir("selfrestart_legacy_tail_only");
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
        shard
            .insert_extracted_with_tags(&corpus[0].1, 1, 1, &corpus[0].2, &[])
            .expect("unsealed tail insert");
        // Deliberately do not seal: the checkpoint base stays empty and the
        // acknowledged row exists only in the translog tail.
    }

    // Downgrade the v2 checkpoint to the exact v1 body. With no segment
    // filenames the legacy body is the first 28 bytes (three u64s + count).
    let sidecar_path = tmp.join("shard.ckpt");
    let current = std::fs::read(&sidecar_path).expect("read current sidecar");
    let legacy_body = current[12..12 + 28].to_vec();
    let mut legacy_bytes = Vec::new();
    legacy_bytes.extend_from_slice(b"RSCK");
    legacy_bytes.extend_from_slice(&1u32.to_le_bytes());
    legacy_bytes.extend_from_slice(&crate::storage::crc32(&legacy_body).to_le_bytes());
    legacy_bytes.extend_from_slice(&legacy_body);
    std::fs::write(&sidecar_path, &legacy_bytes).expect("write v1 sidecar");
    let translog_before =
        std::fs::read(tmp.join(crate::cluster::translog::TRANSLOG_FILE)).expect("read tail");

    let Err(error) = LocalShard::new_durable(norm, dict, tag_dict, cfg) else {
        panic!("a current binary must not replay a legacy-only tail");
    };
    assert!(
        error.to_string().contains("legacy compiler semantics")
            && error.to_string().contains("re-placement"),
        "unexpected refusal: {error}"
    );
    assert_eq!(
        std::fs::read(&sidecar_path).expect("sidecar remains"),
        legacy_bytes,
        "the refusal must not advance the empty-base checkpoint"
    );
    assert_eq!(
        std::fs::read(tmp.join(crate::cluster::translog::TRANSLOG_FILE)).expect("translog remains"),
        translog_before,
        "the refusal must happen before the legacy tail can be reset or consumed"
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
