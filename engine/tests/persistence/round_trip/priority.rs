use super::*;

fn ranked_score(engine: &Engine, title: &str, id: u64) -> i64 {
    let snap = engine.snapshot();
    let program = snap
        .compile_rank_program(&reverse_rusty::RankProgramSpec::default())
        .expect("priority program");
    let mut scratch = reverse_rusty::segment::MatchScratch::new();
    let ranked = snap
        .try_match_title_top_k(
            title,
            reverse_rusty::TopKOptions::default(),
            &program,
            &reverse_rusty::exact::TagPredicate::empty(),
            &mut scratch,
            None,
        )
        .expect("ranked match");
    ranked
        .hits
        .iter()
        .find(|hit| hit.logical_id == id)
        .map(|hit| hit.score)
        .expect("ranked id")
}

#[test]
fn typed_priority_v6_mmap_and_wal_tail_round_trip() {
    // Non-zero typed priority forces .seg v6; a live memtable tail exercises
    // WAL v6 replay in the same reopen.
    let dir = test_dir("typed_priority_v6");
    let cfg = || EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    {
        let mut engine = Engine::with_config(make_norm(), cfg());
        engine
            .try_insert_live_ranked(
                "topps chrome",
                1,
                1,
                &[("priority".into(), "50".into())],
                Some(reverse_rusty::RankValues { priority: 50 }),
            )
            .expect("typed insert");
        engine.flush();
        engine
            .try_insert_live_ranked(
                "topps chrome",
                2,
                1,
                &[("priority".into(), "-7".into())],
                Some(reverse_rusty::RankValues { priority: -7 }),
            )
            .expect("typed WAL-tail insert");
        assert_eq!(ranked_score(&engine, "topps chrome", 1), 50);
        assert_eq!(ranked_score(&engine, "topps chrome", 2), -7);
    }
    let engine = Engine::open(make_norm(), cfg()).expect("reopen v6 + WAL tail");
    assert_eq!(ranked_score(&engine, "topps chrome", 1), 50);
    assert_eq!(ranked_score(&engine, "topps chrome", 2), -7);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn legacy_priority_segment_fallback_migrates_through_compaction() {
    let dir = test_dir("legacy_priority_fallback");
    let cfg = || EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    {
        let mut engine = Engine::with_config(make_norm(), cfg());
        engine
            .try_build_from_queries_with_tags(
                &[(1, "topps chrome".to_string())],
                &[vec![("priority".into(), "42".into())]],
            )
            .expect("legacy tagged build");
    }
    // Downgrade the capability word to v3. The section offsets keep the appended
    // bytes unreachable to a pre-v6 reader, exactly reproducing "no stored rank
    // column" while retaining the legacy mirrored tag.
    for entry in std::fs::read_dir(dir.join("segments")).expect("segment dir") {
        let path = entry.expect("entry").path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("seg") {
            continue;
        }
        let mut bytes = std::fs::read(&path).expect("segment bytes");
        bytes[4..8].copy_from_slice(&3u32.to_le_bytes());
        let n = bytes.len();
        let crc = reverse_rusty::storage::crc32(&bytes[..n - 4]);
        bytes[n - 4..].copy_from_slice(&crc.to_le_bytes());
        std::fs::write(path, bytes).expect("downgraded segment");
    }
    let mut engine = Engine::open(make_norm(), cfg()).expect("legacy reopen");
    assert_eq!(ranked_score(&engine, "topps chrome", 1), 42);
    engine.compact_all();
    drop(engine);
    let engine = Engine::open(make_norm(), cfg()).expect("reopen after migration");
    assert_eq!(ranked_score(&engine, "topps chrome", 1), 42);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn typed_priority_survives_compaction_vocab_upsert_delete_and_backup() {
    for reanchor in [false, true] {
        let root = test_dir(if reanchor {
            "typed_priority_lifecycle_reanchor"
        } else {
            "typed_priority_lifecycle_mechanical"
        });
        let source = root.join("source");
        let backup = root.join("backup");
        let config_for = |dir: &std::path::Path| EngineConfig {
            data_dir: Some(dir.to_path_buf()),
            compaction_reanchor: reanchor,
            ..EngineConfig::default()
        };
        let mut engine = Engine::with_config(make_norm(), config_for(&source));
        for (id, priority) in [(1, 50), (2, -5)] {
            engine
                .try_insert_live_ranked(
                    "topps chrome",
                    id,
                    1,
                    &[("priority".into(), priority.to_string())],
                    Some(reverse_rusty::RankValues { priority }),
                )
                .expect("typed insert");
            engine.flush();
        }
        engine.compact_all().expect("two-segment compaction");
        assert_eq!(ranked_score(&engine, "topps chrome", 1), 50);
        assert_eq!(ranked_score(&engine, "topps chrome", 2), -5);

        let vocab = Vocab::new();
        let reopen_norm = vocab.to_normalizer().expect("vocab normalizer");
        engine.set_vocab(vocab).expect("set vocab");
        assert!(engine.recompile_stale_segments() > 0);
        assert_eq!(ranked_score(&engine, "topps chrome", 1), 50);

        engine
            .try_upsert_live_ranked(
                "topps chrome",
                1,
                2,
                &[("priority".into(), "77".into())],
                Some(reverse_rusty::RankValues { priority: 77 }),
            )
            .expect("typed upsert");
        engine.delete_by_logical_id(2).expect("delete second rank");
        engine.flush();
        assert_eq!(ranked_score(&engine, "topps chrome", 1), 77);
        assert!(!match_ids(&engine, "topps chrome").contains(&2));

        engine.backup_to(&backup).expect("ranked backup");
        let restored = Engine::open(reopen_norm, config_for(&backup)).expect("restore backup");
        assert_eq!(ranked_score(&restored, "topps chrome", 1), 77);
        assert!(!match_ids(&restored, "topps chrome").contains(&2));
        let _ = std::fs::remove_dir_all(root);
    }
}
