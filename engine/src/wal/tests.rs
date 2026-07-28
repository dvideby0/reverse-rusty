use super::*;

fn scratch_path(name: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "reverse_rusty_wal_{}_{}.log",
        name,
        std::process::id()
    ));
    let _ = std::fs::remove_file(&p);
    p
}

#[test]
fn append_surfaces_write_errors_instead_of_swallowing() {
    let path = scratch_path("append_err");
    let mut wal = Wal::open(&path, false).unwrap();
    // A healthy append succeeds.
    assert!(wal.append_insert(1, 1, "wireless mouse", &[]).is_ok());
    // Once the file can no longer be written, the error is returned (not swallowed).
    wal.break_writes_for_test();
    assert!(wal.append_insert(2, 1, "scottie pippen", &[]).is_err());
    assert!(wal.append_tombstone(u32::MAX, 0).is_err());
    let _ = std::fs::remove_file(&path);
}

#[test]
fn fsync_each_write_round_trips_through_recovery() {
    let path = scratch_path("fsync_roundtrip");
    {
        let mut wal = Wal::open(&path, true).unwrap();
        wal.append_insert(7, 2, "product omega", &[]).unwrap();
        wal.append_tombstone(0, 3).unwrap();
    }
    let recovered = Wal::recover(&path).unwrap();
    assert_eq!(recovered.entries.len(), 2);
    assert_eq!(recovered.skipped_bytes, 0);
    match &recovered.entries[0] {
        WalEntry::Insert {
            logical,
            version,
            text,
            ..
        } => {
            assert_eq!(*logical, 7);
            assert_eq!(*version, 2);
            assert_eq!(text, "product omega");
        }
        other => panic!("expected Insert, got {other:?}"),
    }
    let _ = std::fs::remove_file(&path);
}

#[test]
fn insert_tags_round_trip_through_recovery_and_untagged_reads_empty() {
    let path = scratch_path("tags_roundtrip");
    {
        let mut wal = Wal::open(&path, true).unwrap();
        // A tagged insert (the ADR-049 case) and an untagged one.
        wal.append_insert(
            7,
            1,
            "1994 north star",
            &[
                ("category".to_string(), "items".to_string()),
                ("status".to_string(), "active".to_string()),
            ],
        )
        .unwrap();
        wal.append_insert(8, 1, "no tags here", &[]).unwrap();
    }
    let recovered = Wal::recover(&path).unwrap();
    assert_eq!(recovered.entries.len(), 2);
    match &recovered.entries[0] {
        WalEntry::Insert { logical, tags, .. } => {
            assert_eq!(*logical, 7);
            assert_eq!(
                tags,
                &vec![
                    ("category".to_string(), "items".to_string()),
                    ("status".to_string(), "active".to_string()),
                ]
            );
        }
        other => panic!("expected Insert, got {other:?}"),
    }
    match &recovered.entries[1] {
        WalEntry::Insert { logical, tags, .. } => {
            assert_eq!(*logical, 8);
            assert!(tags.is_empty(), "an untagged insert recovers empty tags");
        }
        other => panic!("expected Insert, got {other:?}"),
    }
    let _ = std::fs::remove_file(&path);
}

#[test]
fn typed_priority_extension_round_trips_without_new_opcode() {
    let path = scratch_path("priority_roundtrip");
    {
        let mut wal = Wal::open(&path, true).unwrap();
        wal.append_insert_ranked(
            7,
            3,
            "acme chrome",
            &[("priority".to_string(), "-55".to_string())],
            -55,
        )
        .unwrap();
        wal.append_upsert_ranked(
            7,
            4,
            "acme chrome premium",
            &[("priority".to_string(), "99".to_string())],
            99,
        )
        .unwrap();
    }
    let recovered = Wal::recover(&path).unwrap();
    match &recovered.entries[0] {
        WalEntry::Insert { priority, .. } => assert_eq!(*priority, Some(-55)),
        other => panic!("expected Insert, got {other:?}"),
    }
    match &recovered.entries[1] {
        WalEntry::Upsert { priority, .. } => assert_eq!(*priority, Some(99)),
        other => panic!("expected Upsert, got {other:?}"),
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn source_generation_extension_round_trips_with_optional_priority() {
    let path = scratch_path("source_generation_roundtrip");
    {
        let mut wal = Wal::open(&path, true).unwrap();
        wal.append_insert_with_source_generation(7, 3, "acme chrome", &[], None, 41, false)
            .unwrap();
        wal.append_upsert_with_source_generation(
            7,
            4,
            "acme chrome premium",
            &[("priority".to_string(), "-55".to_string())],
            Some(-55),
            42,
            true,
        )
        .unwrap();
    }

    let recovered = Wal::recover(&path).unwrap();
    match &recovered.entries[0] {
        WalEntry::Insert {
            source_generation,
            priority,
            class_d_accepted,
            ..
        } => {
            assert_eq!(*source_generation, Some(41));
            assert_eq!(*priority, None);
            assert!(!class_d_accepted);
        }
        other => panic!("expected Insert, got {other:?}"),
    }
    match &recovered.entries[1] {
        WalEntry::Upsert {
            source_generation,
            priority,
            class_d_accepted,
            ..
        } => {
            assert_eq!(*source_generation, Some(42));
            assert_eq!(*priority, Some(-55));
            assert!(class_d_accepted);
        }
        other => panic!("expected Upsert, got {other:?}"),
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn delete_by_logical_round_trips_through_recovery() {
    let path = scratch_path("delete_logical_roundtrip");
    {
        let mut wal = Wal::open(&path, true).unwrap();
        wal.append_insert(7, 1, "product omega", &[]).unwrap();
        wal.append_delete_logical(7).unwrap();
        // Old positional frames still coexist in the same file.
        wal.append_tombstone(u32::MAX, 3).unwrap();
    }
    let recovered = Wal::recover(&path).unwrap();
    assert_eq!(recovered.entries.len(), 3);
    assert_eq!(recovered.skipped_bytes, 0);
    match &recovered.entries[1] {
        WalEntry::DeleteByLogical { logical, .. } => assert_eq!(*logical, 7),
        other => panic!("expected DeleteByLogical, got {other:?}"),
    }
    match &recovered.entries[2] {
        WalEntry::Tombstone {
            seg_idx, local_id, ..
        } => {
            assert_eq!(*seg_idx, u32::MAX);
            assert_eq!(*local_id, 3);
        }
        other => panic!("expected Tombstone, got {other:?}"),
    }
    let _ = std::fs::remove_file(&path);
}

#[test]
fn upsert_round_trips_with_tags_and_coexists_with_insert() {
    let path = scratch_path("upsert_roundtrip");
    {
        let mut wal = Wal::open(&path, true).unwrap();
        wal.append_insert(7, 1, "product omega", &[]).unwrap();
        wal.append_upsert(
            7,
            2,
            "product omega pro",
            &[("category".to_string(), "items".to_string())],
        )
        .unwrap();
    }
    let recovered = Wal::recover(&path).unwrap();
    assert_eq!(recovered.entries.len(), 2);
    assert_eq!(recovered.skipped_bytes, 0);
    match &recovered.entries[1] {
        WalEntry::Upsert {
            logical,
            version,
            text,
            tags,
            ..
        } => {
            assert_eq!(*logical, 7);
            assert_eq!(*version, 2);
            assert_eq!(text, "product omega pro");
            assert_eq!(tags, &vec![("category".to_string(), "items".to_string())]);
        }
        other => panic!("expected Upsert, got {other:?}"),
    }
    let _ = std::fs::remove_file(&path);
}

#[test]
fn last_seq_is_monotonic_across_reset() {
    let path = scratch_path("last_seq_monotonic");
    let mut wal = Wal::open(&path, false).unwrap();
    assert_eq!(wal.last_seq(), 0, "no entries yet");
    wal.append_insert(1, 1, "wireless mouse", &[]).unwrap();
    wal.append_delete_logical(1).unwrap();
    assert_eq!(wal.last_seq(), 2);
    wal.reset().unwrap();
    assert_eq!(wal.last_seq(), 2, "reset must not rewind the watermark");
    wal.append_insert(2, 1, "scottie pippen", &[]).unwrap();
    assert_eq!(wal.last_seq(), 3);
    let _ = std::fs::remove_file(&path);
}

/// Micro-benchmark: per-write fsync vs. checkpoint-only. Ignored by default
/// (it does real device flushes). Run with:
///   cargo test --release -p reverse-rusty --lib wal::tests::bench_fsync_cost -- --ignored --nocapture
#[test]
#[ignore = "benchmark: does real device flushes; run with --ignored"]
fn bench_fsync_cost() {
    use std::time::Instant;
    const N: u64 = 5_000;
    for &(label, fsync) in &[
        ("checkpoint-only (fsync=false)", false),
        ("per-write fsync=true", true),
    ] {
        let path = scratch_path(&format!("bench_{fsync}"));
        let mut wal = Wal::open(&path, fsync).unwrap();
        let t = Instant::now();
        for i in 0..N {
            wal.append_insert(i, 1, "1994 north star wireless mouse limited pro", &[])
                .unwrap();
        }
        let per = t.elapsed().as_secs_f64() / N as f64;
        println!(
            "{label:35}: {:.1} us/append   ({:.0} appends/sec)",
            per * 1e6,
            1.0 / per
        );
        let _ = std::fs::remove_file(&path);
    }
}
