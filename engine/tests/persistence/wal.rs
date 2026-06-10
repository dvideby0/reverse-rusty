//! WAL recovery: tagged inserts replayed with their tags, live inserts restored
//! after a simulated crash, and corrupt-tail reporting.

use crate::harness::*;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::segment::Engine;

#[test]
fn tagged_inserts_survive_wal_recovery() {
    // Tags ride the WAL (v2, ADR-049): a live tagged insert that has NOT been flushed is
    // replayed on reopen WITH its tags, so a filter still narrows correctly.
    let dir = test_dir("tagged_wal");
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        memtable_flush_threshold: usize::MAX, // keep live inserts in WAL + memtable
        ..EngineConfig::default()
    };
    {
        let mut engine = Engine::with_config(make_norm(), config.clone());
        // A base build writes the manifest (open replays the WAL only when one exists);
        // the seed query is unrelated to the title/filters below.
        engine.build_from_queries(&[(99, "zzz placeholder seed".to_string())]);
        engine.insert_live_with_tags(
            "topps chrome",
            1,
            1,
            &[("category".to_string(), "cards".to_string())],
        );
        engine.insert_live_with_tags(
            "topps chrome",
            2,
            1,
            &[("category".to_string(), "coins".to_string())],
        );
        // No flush — the tagged inserts live only in the WAL + memtable, so reopen must
        // replay them (with tags) to reconstruct the memtable.
        drop(engine);
    }
    let engine2 = Engine::open(make_norm(), config).unwrap();
    let snap = engine2.snapshot();
    let title = "2020 topps chrome update";

    let mut s = reverse_rusty::segment::MatchScratch::new();
    let mut out = Vec::new();

    let cards = snap.compile_tag_predicate(&[("category".to_string(), vec!["cards".to_string()])]);
    snap.match_title_filtered(title, &mut s, &mut out, true, &cards);
    out.sort_unstable();
    assert_eq!(
        out,
        vec![1],
        "WAL-replayed tags narrow category=cards to query 1"
    );

    let coins = snap.compile_tag_predicate(&[("category".to_string(), vec!["coins".to_string()])]);
    snap.match_title_filtered(title, &mut s, &mut out, true, &coins);
    out.sort_unstable();
    assert_eq!(
        out,
        vec![2],
        "WAL-replayed tags narrow category=coins to query 2"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn wal_recovery_inserts() {
    // Insert via insert_live (goes through WAL), then simulate crash + recovery.
    let dir = test_dir("wal_recovery");
    let norm = make_norm();
    let queries = sample_queries();

    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        memtable_flush_threshold: usize::MAX, // never auto-flush
        ..EngineConfig::default()
    };

    // 1) Build base segment, then add live inserts (not flushed)
    let mut engine = Engine::with_config(norm, config.clone());
    engine.build_from_queries(&queries);

    // These go to the memtable + WAL but are NOT flushed to segments
    engine.insert_live("wander franco prospect", 100, 1);
    engine.insert_live("fernando tatis jr rookie", 101, 1);

    let title_wander = "Wander Franco 2019 Bowman Chrome Prospect";
    let title_tatis = "Fernando Tatis Jr 2019 Topps Chrome Rookie";
    let expected_wander = match_ids(&engine, title_wander);
    let expected_tatis = match_ids(&engine, title_tatis);

    drop(engine); // simulate crash

    // 2) Recover
    let engine2 = Engine::open(make_norm(), config).unwrap();
    let actual_wander = match_ids(&engine2, title_wander);
    let actual_tatis = match_ids(&engine2, title_tatis);

    assert_eq!(
        expected_wander, actual_wander,
        "WAL recovery lost wander insert"
    );
    assert_eq!(
        expected_tatis, actual_tatis,
        "WAL recovery lost tatis insert"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// A bit flipped INSIDE a CRC-valid frame in the MIDDLE of the log: recovery must stop
/// at the corrupt frame and surface everything after it as skipped — never resync past
/// it and replay later entries (which would silently drop one mutation while keeping
/// its successors, a reordering the WAL contract forbids). The junk-at-tail test above
/// cannot distinguish stop-at-first-bad from skip-and-continue; this pins it.
#[test]
fn wal_recovery_stops_at_a_mid_file_bitflip_and_never_resyncs() {
    use reverse_rusty::wal::Wal;

    let dir = test_dir("wal_midfile_bitflip");
    let wal_path = dir.join("wal.log");

    {
        let mut wal = Wal::open(&wal_path, false).unwrap();
        wal.append_insert(1, 1, "first record aaa", &[]).unwrap();
        wal.append_insert(2, 1, "second record bbb", &[]).unwrap();
        wal.append_insert(3, 1, "third record ccc", &[]).unwrap();
    }

    // File layout: 8-byte header, then frames of [len:u32][crc:u32][body:len].
    let mut bytes = std::fs::read(&wal_path).unwrap();
    let frame1_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let frame2_start = 8 + 8 + frame1_len;
    let frame2_len =
        u32::from_le_bytes(bytes[frame2_start..frame2_start + 4].try_into().unwrap()) as usize;
    assert!(frame2_len > 12, "sanity: frame 2 has a body to corrupt");
    // Flip one bit in the middle of frame 2's BODY (its CRC no longer matches).
    let target = frame2_start + 8 + frame2_len / 2;
    bytes[target] ^= 0x01;
    let tail_after_frame1 = bytes.len() - frame2_start;
    std::fs::write(&wal_path, &bytes).unwrap();

    let recovery = Wal::recover(&wal_path).unwrap();
    assert_eq!(
        recovery.entries.len(),
        1,
        "recovery must stop AT the corrupt frame: only the first record is replayable \
         (resyncing to record 3 would silently drop record 2 while keeping its successor)"
    );
    match &recovery.entries[0] {
        reverse_rusty::wal::WalEntry::Insert { logical, text, .. } => {
            assert_eq!(*logical, 1);
            assert_eq!(text, "first record aaa");
        }
        other => panic!("expected the first insert, got {other:?}"),
    }
    assert_eq!(
        recovery.skipped_bytes, tail_after_frame1,
        "everything from the corrupt frame onward must be reported as skipped"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn wal_recovery_reports_corrupt_tail() {
    use reverse_rusty::wal::Wal;
    use std::io::Write;

    let dir = test_dir("wal_corrupt_tail");
    let wal_path = dir.join("wal.log");

    // Write a valid WAL with two inserts
    {
        let mut wal = Wal::open(&wal_path, false).unwrap();
        wal.append_insert(1, 1, "michael jordan card", &[]).unwrap();
        wal.append_insert(2, 1, "lebron james rookie", &[]).unwrap();
    }

    // Append garbage to simulate a torn write
    {
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap();
        f.write_all(&[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF])
            .unwrap();
    }

    // Recover and check that we get the valid entries + skipped bytes reported
    let recovery = Wal::recover(&wal_path).unwrap();
    assert_eq!(
        recovery.entries.len(),
        2,
        "should recover both valid entries"
    );
    assert!(
        recovery.skipped_bytes > 0,
        "should report skipped bytes from corrupt tail"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
