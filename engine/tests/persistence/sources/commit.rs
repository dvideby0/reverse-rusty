use super::*;

#[test]
fn bulk_ingest_persists_sources_across_reopen() {
    // P1-15: bulk_ingest now persists both the segment AND the source text
    // (sources.dat) as part of its durable commit. Previously bulk bypassed
    // sources.dat entirely, so source text was lost on reopen.
    let dir = test_dir("bulk_sources_reopen");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let mut engine = Engine::with_config(make_norm(), config.clone());
    engine.build_from_queries(&sample_queries());

    let batch = vec![
        (100u64, "product omega preview".to_string()),
        (101u64, "fernando tatis jr new".to_string()),
    ];
    let report = engine.bulk_ingest(&batch);
    assert_eq!(report.ingested, 2);
    assert_eq!(
        engine.get_query_source(100).as_deref(),
        Some("product omega preview")
    );

    let title = "Product Omega 2019 Summit Chrome Preview";
    let expected = match_ids(&engine, title);
    assert!(
        expected.contains(&100),
        "bulk query should match before reopen"
    );
    drop(engine);

    // Reopen: both the match data AND the bulk source text must survive.
    let engine2 = Engine::open(make_norm(), config).unwrap();
    assert_eq!(
        match_ids(&engine2, title),
        expected,
        "bulk matches lost after reopen"
    );
    assert_eq!(
        engine2.get_query_source(100).as_deref(),
        Some("product omega preview"),
        "bulk-ingested source text lost after reopen (sources.dat not persisted)"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(unix)]
#[test]
fn bulk_ingest_failure_is_all_or_nothing() {
    // P1-15: a persistence failure during bulk ingest must roll the batch back
    // entirely (no segment added, no source committed) and surface as an error,
    // instead of silently degrading to an in-memory segment.
    use std::os::unix::fs::PermissionsExt;

    let dir = test_dir("bulk_all_or_nothing");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let mut engine = Engine::with_config(make_norm(), config);
    engine.build_from_queries(&sample_queries());
    let segs_before = engine.num_segments();

    // Make the segments dir read-only so the next segment write fails.
    let seg_dir = dir.join("segments");
    let orig = std::fs::metadata(&seg_dir).unwrap().permissions();
    std::fs::set_permissions(&seg_dir, std::fs::Permissions::from_mode(0o555)).unwrap();

    let batch = vec![(100u64, "product omega preview".to_string())];
    let failed = engine.try_bulk_ingest(&batch);

    // Restore perms BEFORE asserting so temp-dir cleanup always works.
    std::fs::set_permissions(&seg_dir, orig).unwrap();

    assert!(
        failed.is_err(),
        "bulk ingest into a read-only dir should fail"
    );
    assert_eq!(
        engine.num_segments(),
        segs_before,
        "a failed bulk ingest must not add a segment"
    );
    assert!(
        !engine.persistence_healthy,
        "persistence should be marked unhealthy after a write failure"
    );
    assert!(
        engine.get_query_source(100).is_none(),
        "a rolled-back batch must not commit source text"
    );

    // Once the dir is writable again, a fresh bulk ingest commits cleanly.
    let ok = engine.try_bulk_ingest(&batch);
    assert!(
        ok.is_ok(),
        "bulk ingest should succeed after the dir is writable"
    );
    assert_eq!(engine.num_segments(), segs_before + 1);
    assert_eq!(
        engine.get_query_source(100).as_deref(),
        Some("product omega preview")
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn bulk_source_prepare_failure_rolls_back_match_commit() {
    let dir = test_dir("bulk_source_prepare_rollback");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let mut engine = Engine::with_config(make_norm(), config);
    engine.build_from_queries(&sample_queries());
    let manifest_before = std::fs::read(dir.join("manifest.bin")).expect("committed manifest");
    let segments_before = engine.num_segments();

    // The segment write succeeds first. Poison only the next source candidate's
    // temporary path so the prepare phase fails after that artifact exists.
    std::fs::create_dir(next_source_temp_path(&dir)).expect("poison source candidate tmp");
    let failed = engine.try_bulk_ingest(&[(100, "product omega preview".to_string())]);

    assert!(failed.is_err(), "source preparation must reject the batch");
    assert_eq!(engine.num_segments(), segments_before);
    assert!(engine.get_query_source(100).is_none());
    assert_eq!(
        std::fs::read(dir.join("manifest.bin")).expect("manifest after rejection"),
        manifest_before,
        "the joint commit point must not advance"
    );
    assert!(!engine.persistence_healthy);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn flush_source_prepare_failure_keeps_wal_recovery_authoritative() {
    let dir = test_dir("flush_source_prepare_wal");
    let cfg = || EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    {
        let mut engine = Engine::with_config(make_norm(), cfg());
        engine
            .try_insert_live("product omega preview", 100, 1)
            .expect("WAL-backed insert");
        let poison = dir
            .join("sources_g00000000000000000001.dat")
            .with_extension("sources.tmp");
        std::fs::create_dir(&poison).expect("poison first source candidate tmp");
        engine.flush();
        assert!(!engine.persistence_healthy);
        assert!(
            !dir.join("manifest.bin").exists(),
            "source failure must prevent the first manifest commit"
        );
        std::fs::remove_dir(poison).expect("remove poison before recovery");
    }

    let mut reopened = Engine::open(make_norm(), cfg()).expect("recover complete WAL");
    assert_eq!(
        reopened.get_query_source(100).as_deref(),
        Some("product omega preview")
    );
    assert!(
        match_ids(&reopened, "Product Omega Preview").contains(&100),
        "the unretired WAL must recover matching data too"
    );
    reopened.flush();
    drop(reopened);
    let reopened = Engine::open(make_norm(), cfg()).expect("reopen committed retry");
    assert_eq!(
        reopened.get_query_source(100).as_deref(),
        Some("product omega preview")
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn failed_recompile_never_watermarks_a_memtable_delete_before_segment_commit() {
    let dir = test_dir("recompile_source_prepare_watermark");
    let cfg = || EngineConfig {
        data_dir: Some(dir.clone()),
        memtable_flush_threshold: usize::MAX,
        ..EngineConfig::default()
    };
    let committed_manifest;
    {
        let mut engine = Engine::with_config(make_norm(), cfg());
        engine.build_from_queries(&[(1, "alpha base".to_string())]);
        committed_manifest =
            std::fs::read(dir.join("manifest.bin")).expect("initial committed manifest");

        engine
            .try_insert_live("bravo transient", 2, 1)
            .expect("WAL-backed memtable insert");
        assert_eq!(
            engine
                .delete_by_logical_id(2)
                .expect("WAL-backed logical delete"),
            1
        );

        // Source preparation succeeds, then the replacement segment write fails.
        // The old manifest/WAL must remain the sole recovery authority.
        let poison = next_segment_temp_path(&dir);
        std::fs::create_dir(&poison).expect("poison replacement segment tmp");
        engine
            .set_vocab(reverse_rusty::vocab::Vocab::default())
            .expect("mark the base stale");
        assert_eq!(
            engine.recompile_stale_segments(),
            1,
            "the coherent green row remains live in memory"
        );
        assert!(!engine.persistence_healthy);
        assert_eq!(
            std::fs::read(dir.join("manifest.bin")).expect("manifest after failed recompile"),
            committed_manifest,
            "source preparation must not advance the WAL watermark before the segment commit"
        );
        std::fs::remove_dir(poison).expect("remove segment poison");
    }

    let reopened =
        Engine::open(make_norm(), cfg()).expect("recover from old manifest and full WAL");
    assert_eq!(reopened.num_live_queries(), 1);
    assert!(
        !match_ids(&reopened, "bravo transient").contains(&2),
        "the acknowledged delete must replay instead of being skipped under a premature watermark"
    );
    assert!(reopened.get_query_source(2).is_none());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn degraded_source_recovery_cannot_publish_a_partial_replacement() {
    let dir = test_dir("degraded_source_commit_fence");
    let cfg = || EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    {
        let mut engine = Engine::with_config(make_norm(), cfg());
        engine.build_from_queries(&[(1, "alpha base".to_string())]);
        engine
            .try_bulk_ingest(&[(2, "bravo base".to_string())])
            .expect("second base segment");
    }
    let committed =
        reverse_rusty::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
    let expected_base_segments = committed.segment_files.len();
    assert!(
        expected_base_segments >= 2,
        "fixture needs a compaction range"
    );
    let expected_segments = expected_base_segments + 1; // metrics include the memtable
    let committed_manifest = std::fs::read(dir.join("manifest.bin")).expect("committed manifest");
    std::fs::remove_file(committed_source_path(&dir)).expect("remove selected source sidecar");

    let mut reopened = Engine::open(make_norm(), cfg()).expect("degraded matching-only reopen");
    assert!(!reopened.persistence_healthy);
    assert_eq!(reopened.num_segments(), expected_segments);
    assert!(
        reopened.compact_all().is_none(),
        "compaction must not select an empty recovered source store"
    );
    assert_eq!(
        reopened.num_segments(),
        expected_segments,
        "failed commit rolls back merge"
    );
    assert!(
        reopened
            .try_bulk_ingest(&[(3, "charlie new".to_string())])
            .is_err(),
        "bulk must not legitimize a partial source baseline either"
    );
    assert_eq!(
        std::fs::read(dir.join("manifest.bin")).expect("manifest after refused commits"),
        committed_manifest,
        "the missing source selection remains authoritative until explicit repair"
    );
    assert!(match_ids(&reopened, "alpha base").contains(&1));
    assert!(match_ids(&reopened, "bravo base").contains(&2));
    assert!(!match_ids(&reopened, "charlie new").contains(&3));

    let _ = std::fs::remove_dir_all(&dir);
}
