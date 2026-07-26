use super::*;

fn scratch_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("rr-source-commit-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    dir
}

fn config(dir: &std::path::Path) -> crate::config::EngineConfig {
    crate::config::EngineConfig {
        data_dir: Some(dir.to_path_buf()),
        ..crate::config::EngineConfig::default()
    }
}

#[test]
fn post_manifest_crash_recovers_source_explain_rebuild_checkpoint_and_backup() {
    let dir = scratch_dir("post-manifest");
    let backup = scratch_dir("post-manifest-backup-root").join("backup");
    let mut engine = Engine::with_config(
        crate::normalize::Normalizer::default_vocab().expect("normalizer"),
        config(&dir),
    );
    CRASH_AFTER_SOURCE_MANIFEST_COMMIT.with(|armed| armed.set(true));
    let crash = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let rows = [(7, "1995 fleer".to_string())];
        let _ = engine.try_bulk_ingest(&rows);
    }));
    assert!(crash.is_err(), "the injected post-commit crash must fire");
    assert!(
        engine.get_query_source(7).is_none(),
        "the crash must precede live source publication"
    );
    drop(engine);

    let mut reopened = Engine::open(
        crate::normalize::Normalizer::default_vocab().expect("normalizer"),
        config(&dir),
    )
    .expect("reopen from joint commit");
    let snapshot = reopened.snapshot();
    assert_eq!(
        snapshot
            .get_query_document(7)
            .expect("GET document after recovery")
            .query(),
        "1995 fleer"
    );
    assert!(
        snapshot.explain_hit(7, "1995 Fleer card").is_some(),
        "explain must compile from the recovered canonical source"
    );
    let mut scratch = crate::segment::MatchScratch::new();
    let mut matches = Vec::new();
    snapshot.match_title("1995 Fleer card", &mut scratch, &mut matches, false);
    assert!(
        matches.contains(&7),
        "matching remains available after recovery"
    );

    reopened
        .set_vocab(crate::vocab::Vocab::default())
        .expect("install rebuild trigger");
    assert_eq!(
        reopened.recompile_stale_segments(),
        1,
        "source-driven rebuild must see the recovered document"
    );
    reopened.flush();
    reopened.backup_to(&backup).expect("backup joint commit");
    drop(reopened);

    for recovered_dir in [&dir, &backup] {
        let recovered = Engine::open(
            crate::normalize::Normalizer::default_vocab().expect("normalizer"),
            config(recovered_dir),
        )
        .expect("checkpoint/backup reopen");
        assert_eq!(recovered.get_query_source(7).as_deref(), Some("1995 fleer"));
        assert!(recovered.explain_hit(7, "1995 Fleer card").is_some());
    }

    let _ = std::fs::remove_dir_all(&dir);
    if let Some(root) = backup.parent() {
        let _ = std::fs::remove_dir_all(root);
    }
}
