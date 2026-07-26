use super::*;

#[test]
fn same_version_stale_source_sidecar_fails_loud_after_reopen() {
    let dir = test_dir("sources_same_version_generation");
    let cfg = || EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let stale_sources = {
        let mut engine = Engine::with_config(make_norm(), cfg());
        engine
            .try_upsert_live_with_tags(
                "1994 topps",
                7,
                1,
                &[("status".to_string(), "old".to_string())],
            )
            .expect("first write");
        engine.flush();
        let stale =
            std::fs::read(committed_source_path(&dir)).expect("first committed source sidecar");

        engine
            .try_upsert_live_with_tags(
                "1995 fleer",
                7,
                1,
                &[("status".to_string(), "new".to_string())],
            )
            .expect("same-version replacement");
        engine.flush();
        stale
    };

    // Model post-commit storage damage: replace the manifest-selected immutable
    // sidecar with the first write while the exact row names the second.
    std::fs::write(committed_source_path(&dir), stale_sources)
        .expect("restore stale committed sidecar");
    let engine = Engine::open(make_norm(), cfg()).expect("reopen with stale source");
    let snapshot = engine.snapshot();
    assert!(snapshot.has_live_query(7), "the exact row remains live");
    assert!(
        snapshot.get_query_document(7).is_none(),
        "internal generation mismatch must be source unavailability, never stale _source"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn live_then_bulk_same_id_keeps_newer_source_across_reopen_and_rebuild() {
    let dir = test_dir("sources_live_bulk_reopen");
    let cfg = || EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    {
        let mut engine = Engine::with_config(make_norm(), cfg());
        engine
            .try_insert_live_with_tags(
                "1994 topps",
                7,
                1,
                &[("status".to_string(), "old".to_string())],
            )
            .expect("live insert");
        let rows = [(7, "1995 fleer".to_string())];
        let tags = [vec![("status".to_string(), "new".to_string())]];
        let (report, _) = engine
            .try_bulk_ingest_detailed_with_tags(&rows, &tags)
            .expect("later bulk commit");
        assert_eq!(report.ingested, 1);
        assert_eq!(
            engine
                .snapshot()
                .get_query_document(7)
                .expect("newest live document")
                .query(),
            "1995 fleer"
        );
        // Crash/drop without flushing the older WAL-backed memtable row.
    }

    let mut reopened = Engine::open(make_norm(), cfg()).expect("reopen");
    let document = reopened
        .snapshot()
        .get_query_document(7)
        .expect("bulk source must still pair with the newer base exact row");
    assert_eq!(document.query(), "1995 fleer");
    assert_eq!(document.tags(), [("status".to_string(), "new".to_string())]);

    reopened
        .set_vocab(reverse_rusty::vocab::Vocab::default())
        .expect("coherent reopened corpus remains rebuildable");
    assert_eq!(reopened.recompile_stale_segments(), 1);
    assert!(match_ids(&reopened, "1995 fleer").contains(&7));
    assert!(
        !match_ids(&reopened, "1994 topps").contains(&7),
        "the older replayed source must not replace the bulk document during rebuild"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn missing_source_store_blocks_vocab_change_without_dropping_live_rows() {
    let dir = test_dir("sources_missing_vocab_guard");
    let cfg = || EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    {
        let mut engine = Engine::with_config(make_norm(), cfg());
        engine
            .try_insert_live("1995 fleer", 7, 1)
            .expect("live insert");
        engine.flush();
    }
    std::fs::remove_file(committed_source_path(&dir)).expect("remove source store");

    let mut reopened = Engine::open(make_norm(), cfg()).expect("reopen");
    assert!(match_ids(&reopened, "1995 fleer").contains(&7));
    let epoch = reopened.vocab_epoch();
    let error = reopened
        .set_vocab(reverse_rusty::vocab::Vocab::default())
        .expect_err("a partial rebuild corpus must fail before changing the normalizer");
    assert!(
        error.to_string().contains("persistence is unhealthy"),
        "the missing manifest-selected sidecar must degrade durability before rebuild: {error}"
    );
    assert!(!reopened.persistence_healthy);
    assert_eq!(reopened.vocab_epoch(), epoch);
    assert!(!reopened.has_stale_segments());
    assert!(
        match_ids(&reopened, "1995 fleer").contains(&7),
        "a rejected vocabulary change must leave acknowledged matching state intact"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn source_write_failure_keeps_new_normalizer_and_exact_plans_coherent_in_memory() {
    let dir = test_dir("sources_vocab_write_failure");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let mut engine = Engine::with_config(make_norm(), config);
    engine.build_from_queries(&[(7, "new york".to_string())]);
    let committed_manifest =
        std::fs::read(dir.join("manifest.bin")).expect("initial committed manifest");

    // The next immutable source write stages through the derived `.sources.tmp`
    // path. A directory there makes publication fail deterministically without
    // preventing the green segment itself from being written.
    std::fs::create_dir(next_source_temp_path(&dir)).expect("poison source tmp");

    let mut vocab = reverse_rusty::vocab::Vocab::new();
    vocab.add_phrase(
        &["new", "york"],
        "term:new_york",
        reverse_rusty::dict::FeatureKind::Generic,
    );
    engine
        .set_vocab(vocab)
        .expect("preflight succeeds before the injected write failure");
    assert_eq!(
        engine.recompile_stale_segments(),
        1,
        "the complete green materialization remains live even though it is not durable"
    );
    assert!(!engine.has_stale_segments());
    assert!(
        match_ids(&engine, "new york").contains(&7),
        "publishing the new title normalizer over old exact plans would be a false negative"
    );
    assert!(!engine.persistence_healthy);
    assert_eq!(
        std::fs::read(dir.join("manifest.bin")).expect("old manifest remains"),
        committed_manifest,
        "a failed source publication must not advance the durable commit point"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn lazy_overlay_insert_and_tombstone() {
    use reverse_rusty::storage::SourceStore;
    let dir = test_dir("lazy_overlay");
    let path = dir.join("sources.dat");

    // Absent file → empty lazy store; mutations land in the overlay.
    let s = SourceStore::open(&path, false).unwrap();
    assert!(s.get(1).is_none());
    s.insert(1, "one".to_string());
    s.insert(2, "two".to_string());
    assert_eq!(s.get(1).as_deref(), Some("one"));
    s.remove(1); // overlay tombstone
    assert!(s.get(1).is_none());
    assert_eq!(s.get(2).as_deref(), Some("two"));

    // write_to persists only live entries; reopening reads them and the
    // tombstone is gone (id 1 absent, id 2 present).
    s.write_to(&path).unwrap();
    let s2 = SourceStore::open(&path, false).unwrap();
    assert_eq!(s2.get(2).as_deref(), Some("two"));
    assert!(s2.get(1).is_none());
    let _ = std::fs::remove_dir_all(&dir);
}
