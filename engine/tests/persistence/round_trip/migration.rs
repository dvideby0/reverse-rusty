use super::*;

#[test]
fn durable_reopen_recompiles_legacy_clause_boundary_semantics() {
    let dir = test_dir("legacy_clause_boundary_compiler");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let mut vocab = Vocab::new();
    vocab.import_solr_aliases(
        "ny => new york",
        &Normalizer::default_vocab().expect("vocab"),
        &reverse_rusty::dict::Dict::new(),
    );

    {
        let mut engine = Engine::with_vocab(vocab.clone(), config.clone()).expect("with_vocab");
        engine.build_from_queries(&[(1, "new -used york".into())]);
        assert!(
            match_ids(&engine, "new vintage product york").contains(&1),
            "current compiler respects the negated-clause boundary"
        );
    }

    let old_manifest =
        reverse_rusty::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
    assert!(!old_manifest.segment_files.is_empty(), "persisted segment");
    for name in &old_manifest.segment_files {
        stamp_legacy_compiler_semantics(&dir.join("segments").join(name));
    }

    // A standalone shard/shared-segment attach has no coordinator manifest that
    // can atomically swap a re-placed corpus, so it must refuse the same legacy
    // materialization rather than publish it.
    let direct_attach = Engine::open_shared_segments(
        std::sync::Arc::new(vocab.to_normalizer().expect("normalizer")),
        std::sync::Arc::new(
            reverse_rusty::storage::deserialize_dict(&old_manifest.dict_data).expect("dict"),
        ),
        std::sync::Arc::new(
            reverse_rusty::storage::deserialize_tagdict(&old_manifest.tag_dict_data)
                .expect("tag dict"),
        ),
        config.clone(),
        &old_manifest.segment_files,
        old_manifest.next_seg_id,
    );
    assert!(
        direct_attach
            .expect_err("direct legacy attach must fail loud")
            .to_string()
            .contains("legacy compiler semantics"),
        "unexpected direct-attach error"
    );

    // Opening with an active multi-word alias detects the legacy lowering,
    // recompiles every live source, and commits a current-stamped replacement
    // before the engine is returned.
    {
        let reopened =
            Engine::open_with_vocab(vocab.clone(), config.clone()).expect("migrating reopen");
        assert!(
            match_ids(&reopened, "new vintage product york").contains(&1),
            "recompiled query must retain the clause-boundary match"
        );
        let current_manifest =
            reverse_rusty::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
        assert!(
            current_manifest.segment_files.iter().all(|name| {
                reverse_rusty::storage::MmapSegment::open(&dir.join("segments").join(name))
                    .expect("open migrated segment")
                    .compiler_semantics_version()
                    > 0
            }),
            "the committed replacement must carry current compiler semantics"
        );
    }

    // The durable header stamp makes the migration one-shot and the next reopen
    // serves the already-rebuilt materialization.
    let reopened = Engine::open_with_vocab(vocab, config).expect("second reopen");
    assert!(
        match_ids(&reopened, "new vintage product york").contains(&1),
        "second reopen keeps the migrated result"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn durable_reopen_migrates_legacy_context_without_aliases() {
    let dir = test_dir("legacy_clause_boundary_number_context");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let mut vocab = Vocab::new();
    vocab.set_number_context_words(&["model"]);

    {
        let mut engine = Engine::with_vocab(vocab.clone(), config.clone()).expect("with vocab");
        engine.build_from_queries(&[(1, "model -used 1994".into())]);
        assert!(
            match_ids(&engine, "model vintage 1994").contains(&1),
            "current compiler isolates number context at the negated clause"
        );
    }

    let legacy =
        reverse_rusty::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
    for name in &legacy.segment_files {
        stamp_legacy_compiler_semantics(&dir.join("segments").join(name));
    }

    // The old joint stream could also leak caller-defined number context across a
    // clause, so semantics-v0 is rebuilt even without any alias vocabulary.
    let reopened = Engine::open_with_vocab(vocab, config).expect("context-sensitive migration");
    assert!(match_ids(&reopened, "model vintage 1994").contains(&1));
    let current =
        reverse_rusty::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
    assert!(current.segment_files.iter().all(|name| {
        reverse_rusty::storage::MmapSegment::open(&dir.join("segments").join(name))
            .expect("open migrated segment")
            .compiler_semantics_version()
            > 0
    }));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn legacy_migration_collapses_a_manifest_captured_wal_insert() {
    let dir = test_dir("legacy_clause_boundary_manifest_wal_window");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        memtable_flush_threshold: usize::MAX,
        ..EngineConfig::default()
    };

    // Preserve the pre-checkpoint WAL bytes, then let flush commit the same
    // mutation into a segment + manifest. Restoring those bytes models SIGKILL
    // after the manifest's atomic rename but before its WAL checkpoint/reset.
    let wal_before_checkpoint = {
        let mut engine = Engine::with_config(make_norm(), config.clone());
        engine
            .try_insert_live("new -used york", 1, 1)
            .expect("insert");
        let wal = std::fs::read(dir.join("wal.log")).expect("pre-flush WAL");
        engine.flush();
        wal
    };
    std::fs::write(dir.join("wal.log"), wal_before_checkpoint).expect("restore captured WAL");
    let manifest =
        reverse_rusty::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
    for name in &manifest.segment_files {
        stamp_legacy_compiler_semantics(&dir.join("segments").join(name));
    }

    let reopened = Engine::open(make_norm(), config).expect("crash-window migration");
    assert_eq!(
        reopened.num_live_queries(),
        1,
        "the segment row and its captured WAL frame are one mutation"
    );
    assert!(
        match_ids(&reopened, "new vintage york").contains(&1),
        "the migrated query remains matchable"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn legacy_migration_collapses_a_manifest_captured_wal_upsert() {
    let dir = test_dir("legacy_clause_boundary_manifest_wal_upsert");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        memtable_flush_threshold: usize::MAX,
        ..EngineConfig::default()
    };

    let wal_before_checkpoint = {
        let mut engine = Engine::with_config(make_norm(), config.clone());
        engine.build_from_queries(&[(1, "alpha old".into())]);
        engine
            .try_upsert_live("new -used york", 1, 2)
            .expect("upsert");
        let wal = std::fs::read(dir.join("wal.log")).expect("pre-flush WAL");
        engine.flush();
        wal
    };
    std::fs::write(dir.join("wal.log"), wal_before_checkpoint).expect("restore captured WAL");
    let manifest =
        reverse_rusty::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
    for name in &manifest.segment_files {
        stamp_legacy_compiler_semantics(&dir.join("segments").join(name));
    }

    let reopened = Engine::open(make_norm(), config).expect("crash-window upsert migration");
    assert_eq!(
        reopened.num_live_queries(),
        1,
        "the committed upsert and its captured WAL frame are one mutation"
    );
    assert!(!match_ids(&reopened, "alpha old").contains(&1));
    assert!(match_ids(&reopened, "new vintage york").contains(&1));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn legacy_migration_replays_a_watermarked_memtable_only_insert() {
    let dir = test_dir("legacy_clause_boundary_watermarked_tail");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        memtable_flush_threshold: usize::MAX,
        ..EngineConfig::default()
    };
    {
        let mut engine = Engine::with_config(make_norm(), config.clone());
        engine
            .try_insert_live("alpha base", 1, 1)
            .expect("first base insert");
        engine.flush();
        engine
            .try_insert_live("bravo base", 2, 1)
            .expect("second base insert");
        engine.flush();
        engine
            .try_insert_live("charlie tail", 3, 1)
            .expect("memtable-only insert");
        engine
            .compact_all()
            .expect("compaction advances the manifest watermark");
        // Crash without flushing: query 3 is covered by the watermark but exists
        // only in the WAL/memtable, not in the committed segment registry.
    }
    let manifest =
        reverse_rusty::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
    for name in &manifest.segment_files {
        stamp_legacy_compiler_semantics(&dir.join("segments").join(name));
    }

    let reopened = Engine::open(make_norm(), config).expect("tail-preserving migration");
    assert!(
        match_ids(&reopened, "charlie tail").contains(&3),
        "watermark alone must not suppress an unmaterialized WAL insert"
    );
    assert_eq!(reopened.num_live_queries(), 3);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn legacy_migration_then_vocab_adoption_recompiles_equivalences() {
    let dir = test_dir("legacy_clause_boundary_adopt_vocab");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let mut vocab = Vocab::new();
    vocab.import_solr_aliases(
        "ny => new york",
        &Normalizer::default_vocab().expect("vocab"),
        &reverse_rusty::dict::Dict::new(),
    );

    {
        let mut engine = Engine::with_vocab(vocab.clone(), config.clone()).expect("with_vocab");
        engine.build_from_queries(&[(1, "new york inventory".into())]);
        assert!(match_ids(&engine, "ny inventory").contains(&1));
    }
    let manifest =
        reverse_rusty::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
    for name in &manifest.segment_files {
        stamp_legacy_compiler_semantics(&dir.join("segments").join(name));
    }

    // The compatibility open path receives only the normalizer, so its mandatory
    // compiler migration cannot reconstruct transient equivalence groups. A later
    // adoption must detect that fact and perform one equivalence-aware rebuild.
    let reopened =
        Engine::open(vocab.to_normalizer().expect("normalizer"), config.clone()).expect("open");
    drop(reopened); // migration commit and adoption may occur in different processes
    let mut reopened = Engine::open(vocab.to_normalizer().expect("normalizer"), config.clone())
        .expect("post-migration open");
    reopened
        .adopt_vocab(vocab.clone())
        .expect("equivalence-aware adoption");
    assert!(
        match_ids(&reopened, "ny inventory").contains(&1),
        "FN: adoption left the migrated predicate without alias expansion"
    );
    drop(reopened);

    let reopened = Engine::open_with_vocab(vocab, config).expect("second reopen");
    assert!(match_ids(&reopened, "ny inventory").contains(&1));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn legacy_migration_refuses_duplicate_live_logical_rows() {
    let dir = test_dir("legacy_clause_boundary_duplicate_rows");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    {
        let mut engine = Engine::with_config(make_norm(), config.clone());
        engine.build_from_queries(&[(1, "alpha unique".into()), (1, "beta distinct".into())]);
        assert!(match_ids(&engine, "alpha unique").contains(&1));
        assert!(match_ids(&engine, "beta distinct").contains(&1));
    }
    let before =
        reverse_rusty::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
    for name in &before.segment_files {
        stamp_legacy_compiler_semantics(&dir.join("segments").join(name));
    }

    let error = Engine::open(make_norm(), config).expect_err("ambiguous source must fail loud");
    assert!(
        error.to_string().contains("multiple physical predicates"),
        "got: {error}"
    );
    let after = reverse_rusty::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
    assert_eq!(
        after.segment_files, before.segment_files,
        "a refused migration must leave the old manifest authoritative"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn degraded_legacy_recovery_never_commits_an_attached_subset() {
    let dir = test_dir("legacy_clause_boundary_degraded");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    {
        let mut engine = Engine::with_config(make_norm(), config.clone());
        engine.build_from_queries(&[(1, "alpha bravo".into())]);
        engine
            .try_bulk_ingest(&[(2, "charlie delta".into())])
            .expect("second segment");
    }
    let before =
        reverse_rusty::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
    assert_eq!(before.segment_files.len(), 2);
    for name in &before.segment_files {
        stamp_legacy_compiler_semantics(&dir.join("segments").join(name));
    }
    let corrupt = dir.join("segments").join(&before.segment_files[1]);
    let mut bytes = std::fs::read(&corrupt).expect("read segment");
    bytes[20] ^= 1; // leave the trailing CRC stale
    std::fs::write(&corrupt, bytes).expect("corrupt segment");

    let error = Engine::open(make_norm(), config).expect_err("degraded migration must fail");
    assert!(
        error.to_string().contains("degraded recovery"),
        "got: {error}"
    );
    let after = reverse_rusty::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
    assert_eq!(
        after.segment_files, before.segment_files,
        "migration must not replace the manifest with only readable segments"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn legacy_migration_persists_replayed_source_before_wal_reset() {
    let dir = test_dir("legacy_clause_boundary_wal_source");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        memtable_flush_threshold: usize::MAX,
        ..EngineConfig::default()
    };
    {
        let mut engine = Engine::with_config(make_norm(), config.clone());
        engine.build_from_queries(&[(1, "seed query".into())]);
        engine
            .try_insert_live("charlie delta", 2, 1)
            .expect("WAL-tail insert");
    }
    let legacy =
        reverse_rusty::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
    for name in &legacy.segment_files {
        stamp_legacy_compiler_semantics(&dir.join("segments").join(name));
    }

    {
        let migrated = Engine::open(make_norm(), config.clone()).expect("migrating reopen");
        assert!(match_ids(&migrated, "charlie delta").contains(&2));
        assert!(migrated.snapshot().get_query_document(2).is_some());
    }
    let reopened = Engine::open(make_norm(), config).expect("second reopen");
    assert!(match_ids(&reopened, "charlie delta").contains(&2));
    assert!(
        reopened.snapshot().get_query_document(2).is_some(),
        "the committed WAL-tail row must retain its canonical source"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn standalone_open_refuses_future_compiler_semantics() {
    let dir = test_dir("future_compiler_semantics");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    {
        let mut engine = Engine::with_config(make_norm(), config.clone());
        engine.build_from_queries(&[(1, "alpha bravo".into())]);
    }
    let manifest =
        reverse_rusty::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
    for name in &manifest.segment_files {
        stamp_compiler_semantics(&dir.join("segments").join(name), u32::MAX);
    }

    let error = Engine::open(make_norm(), config).expect_err("future semantics must fail loud");
    assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
    assert!(
        error
            .to_string()
            .contains("unsupported compiler semantics version"),
        "got: {error}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn legacy_migration_uses_structural_recovery_parse_limits() {
    let dir = test_dir("legacy_clause_boundary_loose_limits");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        max_query_clauses: 300,
        ..EngineConfig::default()
    };
    let query = (0..257)
        .map(|i| format!("term{i}"))
        .collect::<Vec<_>>()
        .join(" ");
    {
        let mut engine = Engine::with_config(make_norm(), config.clone());
        let report = engine
            .try_build_from_queries(&[(1, query.clone())])
            .expect("build");
        assert_eq!(report.ingested, 1);
    }
    let legacy =
        reverse_rusty::storage::read_manifest(&dir.join("manifest.bin")).expect("manifest");
    for name in &legacy.segment_files {
        stamp_legacy_compiler_semantics(&dir.join("segments").join(name));
    }

    let reopened = Engine::open(make_norm(), config).expect("recovery-safe parse");
    assert!(match_ids(&reopened, &query).contains(&1));
    let _ = std::fs::remove_dir_all(&dir);
}
