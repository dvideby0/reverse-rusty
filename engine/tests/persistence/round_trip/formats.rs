use super::*;

#[test]
fn segment_round_trip() {
    // Build an engine in-memory, then write its segment, mmap it back, and
    // verify matches are identical.
    let dir = test_dir("round_trip");
    let norm = make_norm();
    let queries = sample_queries();

    // 1) Build in-memory engine
    let mut mem_engine = Engine::new(norm);
    mem_engine.build_from_queries(&queries);

    // 2) Build persistent engine with same queries
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let mut disk_engine = Engine::with_config(make_norm(), config);
    disk_engine.build_from_queries(&queries);

    // 3) Verify both produce the same matches
    let titles = [
        "1986 Vertex Wireless Mouse New Item #57 PRO",
        "Mechanical Keyboard 2003 Acme Chrome New PKG",
        "Noise Cancelling Headphones 1996 Acme Chrome Premium PRO",
        "Air Purifier 2011 Acme Update PKG US175",
        "Random item that matches nothing specific",
    ];

    for title in &titles {
        let mem_result = match_ids(&mem_engine, title);
        let disk_result = match_ids(&disk_engine, title);
        assert_eq!(
            mem_result, disk_result,
            "Mismatch for title '{title}': in-memory={mem_result:?} vs disk={disk_result:?}"
        );
    }

    // Cleanup
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn persist_and_reopen() {
    // Build, close, reopen, and verify matches survive.
    let dir = test_dir("persist_reopen");
    let norm = make_norm();
    let queries = sample_queries();

    // 1) Build and persist
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let mut engine = Engine::with_config(norm, config.clone());
    engine.build_from_queries(&queries);

    // Record expected matches
    let title = "1986 Vertex Wireless Mouse New Item #57 PRO";
    let expected = match_ids(&engine, title);
    drop(engine); // "close" the engine

    // 2) Reopen
    let engine2 = Engine::open(make_norm(), config).unwrap();
    let actual = match_ids(&engine2, title);
    assert_eq!(expected, actual, "matches differ after reopen");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn compound_anyof_predicates_round_trip_through_v9_mmap() {
    let dir = test_dir("compound_anyof_v9");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let queries = vec![
        (1, "(red shoe,boot)".to_string()),
        (2, "marker -(red shoe,boot)".to_string()),
        (3, "(red shoe,red boot)".to_string()),
    ];
    let cases = [
        ("red shoe marker", vec![1, 3]),
        ("boot marker", vec![1]),
        ("red hat marker", vec![2]),
        ("shoe marker", vec![2]),
        ("red boot", vec![1, 3]),
    ];

    let mut engine = Engine::with_config(make_norm(), config.clone());
    engine.build_from_queries(&queries);
    for (title, expected) in &cases {
        assert_eq!(
            match_ids(&engine, title),
            (*expected).clone(),
            "memory path: {title}"
        );
    }
    let segment = std::fs::read_dir(dir.join("segments"))
        .expect("segment directory")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .find(|path| path.extension().and_then(|ext| ext.to_str()) == Some("seg"))
        .expect("compound segment");
    let bytes = std::fs::read(segment).expect("segment bytes");
    assert_eq!(
        u32::from_le_bytes(bytes[4..8].try_into().expect("format word")),
        9,
        "compound rows must carry the v9 rollback fence"
    );
    drop(engine);

    let reopened = Engine::open(make_norm(), config).expect("reopen v9 segment");
    for (title, expected) in &cases {
        assert_eq!(
            match_ids(&reopened, title),
            (*expected).clone(),
            "mmap path: {title}"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn quoted_phrase_predicates_round_trip_through_v10_mmap() {
    let dir = test_dir("quoted_phrase_v10");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let queries = vec![
        (1, "\"red shoe\"".to_string()),
        (2, "item -\"for parts\"".to_string()),
    ];
    let cases = [
        ("red shoe", vec![1]),
        ("red leather shoe", vec![]),
        ("item for parts", vec![]),
        ("item for spare parts", vec![2]),
    ];

    let mut engine = Engine::with_config(make_norm(), config.clone());
    engine.build_from_queries(&queries);
    for (title, expected) in &cases {
        assert_eq!(
            match_ids(&engine, title),
            expected.clone(),
            "memory: {title}"
        );
    }
    let manifest =
        reverse_rusty::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
    let segment = dir.join("segments").join(&manifest.segment_files[0]);
    let bytes = std::fs::read(&segment).expect("segment bytes");
    assert_eq!(
        u32::from_le_bytes(bytes[4..8].try_into().expect("format")),
        10,
        "quoted rows require the v10 rollback fence"
    );
    assert_eq!(
        u32::from_le_bytes(bytes[12..16].try_into().expect("semantics")),
        6
    );
    drop(engine);

    let reopened = Engine::open(make_norm(), config).expect("reopen v10");
    for (title, expected) in &cases {
        assert_eq!(
            match_ids(&reopened, title),
            expected.clone(),
            "mmap: {title}"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn mmap_tombstone_of_last_phrase_row_restores_columnar_batch_mode() {
    let dir = test_dir("quoted_phrase_live_capability");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let mut engine = Engine::with_config(make_norm(), config.clone());
    engine.build_from_queries(&[(1, "\"red shoe\"".to_string()), (2, "common".to_string())]);
    drop(engine);

    let mut reopened = Engine::open(make_norm(), config).expect("reopen v10 mmap");
    let titles = vec!["common".to_string()];
    let options = reverse_rusty::segment::BatchMatchOptions {
        include_broad: true,
        broad_strategy: reverse_rusty::segment::BroadStrategy::Columnar,
        ..reverse_rusty::segment::BatchMatchOptions::default()
    };
    assert_eq!(
        reopened
            .match_titles_batch_stats(&titles, options)
            .broad_batches,
        0,
        "the live mmap phrase row must force positioned scalar verification"
    );
    assert_eq!(
        reopened
            .delete_by_logical_id(1)
            .expect("delete mmap phrase"),
        1
    );
    assert!(
        reopened
            .match_titles_batch_stats(&titles, options)
            .broad_batches
            > 0,
        "a dead mmap phrase program must not keep phrase-free traffic in scalar mode"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn semantics_two_materialization_is_source_rebuilt_for_quoted_adjacency() {
    let dir = test_dir("semantics_two_phrase_migration");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    {
        let mut engine = Engine::with_config(make_norm(), config.clone());
        engine.build_from_queries(&[(1, "\"red shoe\"".to_string())]);
    }
    let before =
        reverse_rusty::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
    for name in &before.segment_files {
        stamp_compiler_semantics(&dir.join("segments").join(name), 2);
    }

    let reopened = Engine::open(make_norm(), config.clone()).expect("semantics-2 migration");
    assert_eq!(match_ids(&reopened, "red shoe"), vec![1]);
    assert!(
        match_ids(&reopened, "red leather shoe").is_empty(),
        "source rebuild must install adjacency, not retain semantics-2 conjunction"
    );
    let after = reverse_rusty::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
    assert_ne!(before.segment_files, after.segment_files);
    assert!(after.segment_files.iter().all(|name| {
        reverse_rusty::storage::MmapSegment::open(&dir.join("segments").join(name))
            .expect("migrated segment")
            .compiler_semantics_version()
            == 6
    }));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn semantics_one_anyof_materialization_rebuilds_before_serving() {
    let dir = test_dir("semantics_one_anyof_migration");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let queries = vec![
        (1, "(red shoe,boot)".to_string()),
        (2, "marker -(red shoe,boot)".to_string()),
    ];
    let cases = [
        ("red shoe marker", vec![1]),
        ("boot marker", vec![1]),
        ("red hat marker", vec![2]),
        ("shoe marker", vec![2]),
    ];

    {
        let mut engine = Engine::with_config(make_norm(), config.clone());
        engine.build_from_queries(&queries);
    }
    let before =
        reverse_rusty::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
    assert!(!before.segment_files.is_empty(), "persisted segment");
    for name in &before.segment_files {
        // The semantics stamp, independently of the cumulative layout version,
        // decides whether retained source must be recompiled.
        stamp_compiler_semantics(&dir.join("segments").join(name), 1);
    }

    let reopened = Engine::open(make_norm(), config.clone()).expect("semantics-1 migration");
    for (title, expected) in &cases {
        assert_eq!(
            match_ids(&reopened, title),
            (*expected).clone(),
            "migrated semantics: {title}"
        );
    }
    let after = reverse_rusty::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
    assert_ne!(
        before.segment_files, after.segment_files,
        "migration must atomically select a source-recompiled segment"
    );
    assert!(after.segment_files.iter().all(|name| {
        reverse_rusty::storage::MmapSegment::open(&dir.join("segments").join(name))
            .expect("open migrated segment")
            .compiler_semantics_version()
            == 6
    }));

    drop(reopened);
    let reopened = Engine::open(make_norm(), config).expect("idempotent second reopen");
    for (title, expected) in &cases {
        assert_eq!(match_ids(&reopened, title), (*expected).clone());
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn tagged_queries_survive_reopen_and_filter_on_mmap() {
    // The .seg v3 tag column (ADR-049) must survive reopen: build two queries that match
    // the same title but carry different category tags, persist, reopen (now mmap-backed),
    // and confirm a tag filter narrows correctly against the mmap'd tag column.
    let dir = test_dir("tagged_reopen");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let queries = vec![
        (1u64, "acme chrome".to_string()),
        (2u64, "acme chrome".to_string()),
    ];
    let tags = vec![
        vec![("category".to_string(), "items".to_string())],
        vec![("category".to_string(), "coins".to_string())],
    ];
    let mut engine = Engine::with_config(make_norm(), config.clone());
    engine
        .try_build_from_queries_with_tags(&queries, &tags)
        .expect("tagged durable build");
    drop(engine);

    // Reopen — the base segment is now mmap'd, so the tag column is read from the v3 .seg.
    let engine2 = Engine::open(make_norm(), config).unwrap();
    let snap = engine2.snapshot();
    let title = "2020 acme chrome update";
    let source = snap.get_query_document(1).expect("stored source metadata");
    assert_eq!(source.query(), "acme chrome");
    assert_eq!(source.version(), 1);
    assert!(source.tags_known());
    assert_eq!(
        source.tags(),
        [("category".to_string(), "items".to_string())]
    );

    let mut s = reverse_rusty::segment::MatchScratch::new();
    let mut out = Vec::new();

    snap.match_title(title, &mut s, &mut out, true);
    out.sort_unstable();
    assert_eq!(
        out,
        vec![1, 2],
        "both queries match the title unfiltered after reopen"
    );

    let items = snap.compile_tag_predicate(&[("category".to_string(), vec!["items".to_string()])]);
    snap.match_title_filtered(title, &mut s, &mut out, true, &items);
    out.sort_unstable();
    assert_eq!(
        out,
        vec![1],
        "category=items narrows to query 1 on the reopened mmap segment"
    );

    let coins = snap.compile_tag_predicate(&[("category".to_string(), vec!["coins".to_string()])]);
    snap.match_title_filtered(title, &mut s, &mut out, true, &coins);
    out.sort_unstable();
    assert_eq!(out, vec![2], "category=coins narrows to query 2");

    // A value never ingested matches nothing (safe `terms` semantics).
    let none = snap.compile_tag_predicate(&[("category".to_string(), vec!["stamps".to_string()])]);
    snap.match_title_filtered(title, &mut s, &mut out, true, &none);
    assert!(out.is_empty(), "an unseen filter value returns ∅");

    let _ = std::fs::remove_dir_all(&dir);
}
