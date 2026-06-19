//! Backup/restore for the single-node engine (ADR-079, ADR-065 criterion 11).
//!
//! Restore is the existing `Engine::open` pointed at a RELOCATED copy of the backup,
//! so these tests build an engine, `backup_to` a fresh dir, then open that dir and
//! assert matches are preserved. They also prove the snapshot is point-in-time
//! (isolated from post-backup mutations), that a never-flushed (WAL-only) engine
//! round-trips, and that the fail-loud paths hold.

use crate::harness::*;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::segment::Engine;
use reverse_rusty::storage::{self, BackupError};
use std::path::Path;

fn cfg(dir: &Path) -> EngineConfig {
    EngineConfig {
        data_dir: Some(dir.to_path_buf()),
        ..EngineConfig::default()
    }
}

/// Build → mutate (multi-segment + base tombstone + live WAL tail) → backup →
/// open the relocated backup → matches are identical to the source at backup time.
#[test]
fn backup_then_open_matches_source() {
    let root = test_dir("backup_match_source");
    let src = root.join("data");
    let backup = root.join("backup");
    std::fs::create_dir_all(&src).unwrap();

    let mut engine = Engine::with_config(make_norm(), cfg(&src));
    engine.build_from_queries(&sample_queries());
    // Force multiple segments + a base-segment tombstone + an un-flushed WAL tail.
    engine.flush();
    engine
        .try_insert_live("juan soto bowman chrome", 11, 1)
        .unwrap();
    engine.try_insert_live("ronald acuna prizm", 12, 1).unwrap();
    engine.delete_by_logical_id(2).unwrap(); // tombstone an existing (base) query
    engine.compact_all();

    let titles = [
        "1986 Fleer Michael Jordan Rookie Card #57 PSA 10",
        "LeBron James 2003 Topps Chrome Rookie RC", // query 2 — deleted, must not match
        "Juan Soto 2018 Bowman Chrome",
        "Ronald Acuna Jr Prizm Silver",
        "Mike Trout 2011 Topps Update RC US175",
        "a title that matches nothing",
    ];
    let expected: Vec<Vec<u64>> = titles.iter().map(|t| match_ids(&engine, t)).collect();

    engine.backup_to(&backup).expect("backup");

    let restored = Engine::open(make_norm(), cfg(&backup)).expect("open relocated backup");
    for (t, exp) in titles.iter().zip(&expected) {
        assert_eq!(
            &match_ids(&restored, t),
            exp,
            "restore diverged for title {t:?}"
        );
    }
    assert!(
        !match_ids(&restored, "LeBron James 2003 Topps Chrome Rookie RC").contains(&2),
        "the pre-backup delete of query 2 must survive into the restore"
    );

    let _ = std::fs::remove_dir_all(&root);
}

/// The backup is a point-in-time snapshot: mutating the ORIGINAL after the backup
/// must not change what the backup restores to.
#[test]
fn backup_isolated_from_post_backup_mutations() {
    let root = test_dir("backup_isolation");
    let src = root.join("data");
    let backup = root.join("backup");
    std::fs::create_dir_all(&src).unwrap();

    let mut engine = Engine::with_config(make_norm(), cfg(&src));
    engine.build_from_queries(&sample_queries());
    let title_q1 = "1986 Fleer Michael Jordan Rookie Card #57 PSA 10"; // query 1
    let title_new = "Juan Soto 2018 Bowman Chrome"; // not present at backup time
    let expected_q1 = match_ids(&engine, title_q1);
    assert!(expected_q1.contains(&1));

    engine.backup_to(&backup).expect("backup");

    // Churn the original AFTER the backup.
    engine
        .try_insert_live("juan soto bowman chrome", 11, 1)
        .unwrap();
    engine.delete_by_logical_id(1).unwrap();
    engine.flush();
    engine.compact_all();

    let restored = Engine::open(make_norm(), cfg(&backup)).expect("open backup");
    assert_eq!(
        match_ids(&restored, title_q1),
        expected_q1,
        "post-backup delete leaked into the snapshot"
    );
    assert!(
        match_ids(&restored, title_new).is_empty(),
        "post-backup insert leaked into the snapshot"
    );

    let _ = std::fs::remove_dir_all(&root);
}

/// A never-flushed engine (state only in the WAL, no manifest yet) backs up and
/// restores correctly via WAL replay on `open`.
#[test]
fn backup_of_unflushed_engine_recovers_via_wal() {
    let root = test_dir("backup_wal_only");
    let src = root.join("data");
    let backup = root.join("backup");
    std::fs::create_dir_all(&src).unwrap();

    let mut engine = Engine::with_config(make_norm(), cfg(&src));
    engine
        .try_insert_live("michael jordan 1986 fleer", 1, 1)
        .unwrap();
    engine.try_insert_live("lebron james rookie", 2, 1).unwrap();
    assert!(
        !src.join("manifest.bin").exists(),
        "precondition: no manifest yet"
    );

    let title = "1986 Fleer Michael Jordan Rookie";
    let expected = match_ids(&engine, title);
    assert!(expected.contains(&1));

    engine.backup_to(&backup).expect("backup");
    let restored = Engine::open(make_norm(), cfg(&backup)).expect("open backup");
    assert_eq!(
        match_ids(&restored, title),
        expected,
        "WAL-only state was lost in the backup"
    );

    let _ = std::fs::remove_dir_all(&root);
}

/// An in-memory engine has nothing on disk to back up → `NotDurable`.
#[test]
fn backup_refuses_in_memory_engine() {
    let root = test_dir("backup_in_memory");
    let mut engine = Engine::new(make_norm());
    engine.build_from_queries(&sample_queries());
    let dest = root.join("nope");
    match engine.backup_to(&dest) {
        Err(BackupError::NotDurable) => {}
        other => panic!("expected NotDurable, got {other:?}"),
    }
    assert!(!dest.exists());
    let _ = std::fs::remove_dir_all(&root);
}

/// Refuse to overwrite a pre-existing destination.
#[test]
fn backup_refuses_existing_dest() {
    let root = test_dir("backup_dest_exists");
    let src = root.join("data");
    let backup = root.join("backup");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::create_dir_all(&backup).unwrap();

    let mut engine = Engine::with_config(make_norm(), cfg(&src));
    engine.build_from_queries(&sample_queries());
    match engine.backup_to(&backup) {
        Err(BackupError::DestExists(_)) => {}
        other => panic!("expected DestExists, got {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// A failed backup leaves no partial destination behind (atomic via staging+rename).
#[test]
fn backup_failure_leaves_no_partial_dest() {
    let root = test_dir("backup_fail_clean");
    let src = root.join("data");
    std::fs::create_dir_all(&src).unwrap();
    let mut engine = Engine::with_config(make_norm(), cfg(&src));
    engine.build_from_queries(&sample_queries());

    // A regular file where a parent directory is expected → staging create fails.
    let blocker = root.join("blocker");
    std::fs::write(&blocker, b"i am a file").unwrap();
    let dest = blocker.join("backup");
    assert!(
        engine.backup_to(&dest).is_err(),
        "backup under a file path must fail"
    );
    assert!(!dest.exists(), "no partial dest left behind");

    let _ = std::fs::remove_dir_all(&root);
}

/// `verify_backup` catches a corrupted segment in an otherwise-valid backup.
#[test]
fn verify_catches_corrupted_backup_segment() {
    let root = test_dir("backup_verify_corrupt");
    let src = root.join("data");
    let backup = root.join("backup");
    std::fs::create_dir_all(&src).unwrap();
    let mut engine = Engine::with_config(make_norm(), cfg(&src));
    engine.build_from_queries(&sample_queries());
    engine.backup_to(&backup).expect("backup");
    storage::verify_backup(&backup).expect("fresh backup verifies");

    // Flip a byte in a backed-up segment.
    let seg_dir = backup.join("segments");
    let seg = std::fs::read_dir(&seg_dir)
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.path())
        .find(|p| p.extension().and_then(|x| x.to_str()) == Some("seg"))
        .expect("a backed-up segment");
    let mut bytes = std::fs::read(&seg).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xFF;
    std::fs::write(&seg, bytes).unwrap();

    match storage::verify_backup(&backup) {
        Err(BackupError::CorruptSegment { .. }) => {}
        other => panic!("expected CorruptSegment, got {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&root);
}
