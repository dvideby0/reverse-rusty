//! Tombstone durability at the commit point (ADR-066).
//!
//! Two pre-existing bugs, both found while building the ADR-064 atomic upsert:
//!
//! 1. **Resurrection at flush:** a delete against a BASE segment mutated only the
//!    in-RAM mmap alive-overlay; its WAL frame was the only durable record, and the
//!    flush-time WAL reset dropped it — so the acknowledged delete resurrected on
//!    reopen. Fixed by baking per-segment dead-locals bitmaps into the manifest (the
//!    Lucene `.liv` analogue), applied on open before the WAL tail replays.
//! 2. **Wrong-query tombstone after compaction + crash:** the delete logged positional
//!    `(seg_idx, local)` frames; a compaction commit renumbers that address space, so a
//!    crash replaying a stale frame tombstoned an unrelated query — a silent false
//!    negative. Fixed by logging deletes as ONE address-free `DeleteByLogical` frame
//!    (replayed through the same funnel as the live path) and by skipping base-segment
//!    positional frames at or below the manifest's WAL-seq watermark.

use reverse_rusty::config::EngineConfig;
use reverse_rusty::segment::Engine;
use std::path::Path;

use crate::harness::{make_norm, match_ids, test_dir};

/// Queries where query `i` matches exactly the title "rookie card player{i} unique{i}".
fn distinct_queries(range: std::ops::RangeInclusive<u64>) -> Vec<(u64, String)> {
    range.map(|i| (i, format!("player{i} unique{i}"))).collect()
}

fn title_for(i: u64) -> String {
    format!("rookie card player{i} unique{i}")
}

fn no_compaction_cfg(dir: &Path) -> EngineConfig {
    EngineConfig {
        data_dir: Some(dir.to_path_buf()),
        auto_compact_on_flush: false,
        auto_compact_on_ingest: false,
        holes_ratio_threshold: 1.0,
        ..EngineConfig::default()
    }
}

/// Bug 1, isolated: with every compaction trigger disabled, a base-segment delete
/// must still survive flush (which checkpoints + resets the WAL) and reopen. Before
/// ADR-066 the deleted query resurrected here.
#[test]
fn base_tombstone_survives_flush_and_reopen() {
    let dir = test_dir("tomb_flush_reopen");
    {
        let mut eng = Engine::with_config(make_norm(), no_compaction_cfg(&dir));
        eng.build_from_queries(&distinct_queries(1..=5));
        assert!(match_ids(&eng, &title_for(1)).contains(&1));
        assert!(eng.delete_by_logical_id(1).expect("delete") >= 1);
        // A live insert so the flush has something to seal.
        eng.insert_live("kobe bryant", 100, 1);
        eng.flush();
        assert!(!match_ids(&eng, &title_for(1)).contains(&1));
    }
    let eng = Engine::open(make_norm(), no_compaction_cfg(&dir)).expect("reopen");
    assert!(
        !match_ids(&eng, &title_for(1)).contains(&1),
        "deleted query must NOT resurrect across flush + reopen"
    );
    // Its neighbors are untouched.
    for i in 2..=5 {
        assert!(match_ids(&eng, &title_for(i)).contains(&i), "q{i} intact");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// Bug 1 under DEFAULT config: a delete small enough to stay under the holes-ratio
/// compaction trigger (1/20 = 5% < 30%) used to resurrect — the default-config path a
/// real deployment hits.
#[test]
fn base_tombstone_survives_flush_and_reopen_default_config() {
    let dir = test_dir("tomb_flush_reopen_default");
    let cfg = || EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    {
        let mut eng = Engine::with_config(make_norm(), cfg());
        eng.build_from_queries(&distinct_queries(1..=20));
        assert!(eng.delete_by_logical_id(1).expect("delete") >= 1);
        eng.insert_live("kobe bryant", 100, 1);
        eng.flush();
    }
    let eng = Engine::open(make_norm(), cfg()).expect("reopen");
    assert!(
        !match_ids(&eng, &title_for(1)).contains(&1),
        "deleted query must NOT resurrect under the default config"
    );
    for i in 2..=20 {
        assert!(match_ids(&eng, &title_for(i)).contains(&i), "q{i} intact");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// A delete with NO flush afterwards recovers purely from the WAL tail: the new
/// address-free `DeleteByLogical` frame replays through the same funnel as the live
/// path.
#[test]
fn delete_recovers_from_wal_tail_without_flush() {
    let dir = test_dir("tomb_wal_tail");
    {
        let mut eng = Engine::with_config(make_norm(), no_compaction_cfg(&dir));
        eng.build_from_queries(&distinct_queries(1..=5));
        assert!(eng.delete_by_logical_id(2).expect("delete") >= 1);
        // crash: no flush, no checkpoint — the WAL tail is the only record
    }
    let eng = Engine::open(make_norm(), no_compaction_cfg(&dir)).expect("reopen");
    assert!(!match_ids(&eng, &title_for(2)).contains(&2));
    for i in [1u64, 3, 4, 5] {
        assert!(match_ids(&eng, &title_for(i)).contains(&i), "q{i} intact");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// Bug 2, production path: deletes followed by an explicit compaction (which renumbers
/// the `(seg_idx, local)` address space) and a crash. The address-free delete frame
/// re-derives its targets from the recovered state, so no unrelated query is
/// tombstoned (the pre-fix positional replay silently deleted an innocent neighbor —
/// a false negative) and the deleted ones stay deleted.
#[test]
fn delete_then_compaction_crash_replays_without_misfire() {
    let dir = test_dir("tomb_compact_crash");
    {
        let mut eng = Engine::with_config(make_norm(), no_compaction_cfg(&dir));
        // Two base segments so the compaction has something to merge + renumber.
        eng.build_from_queries(&distinct_queries(1..=10));
        eng.bulk_ingest(&distinct_queries(11..=20));
        assert!(eng.delete_by_logical_id(3).expect("del q3") >= 1);
        assert!(eng.delete_by_logical_id(15).expect("del q15") >= 1);
        let rep = eng.compact_all().expect("compaction ran");
        assert_eq!(rep.segments_merged, 2);
        // crash with the delete frames still in the WAL
    }
    let eng = Engine::open(make_norm(), no_compaction_cfg(&dir)).expect("reopen");
    let mut wrong = Vec::new();
    for i in 1..=20u64 {
        let want = i != 3 && i != 15;
        let got = match_ids(&eng, &title_for(i)).contains(&i);
        if got != want {
            wrong.push((i, want, got));
        }
    }
    assert!(
        wrong.is_empty(),
        "per-query divergence after compaction + crash (id, want, got): {wrong:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Bug 2, positional-API variant: `tombstone_in` still logs a positional frame, but
/// the manifest's WAL-seq watermark marks it as baked at the compaction commit, so
/// the crash replay skips it instead of tombstoning whatever query the renumbered
/// address now points at.
#[test]
fn positional_tombstone_then_compaction_crash_skips_stale_frame() {
    let dir = test_dir("tomb_positional_watermark");
    {
        let mut eng = Engine::with_config(make_norm(), no_compaction_cfg(&dir));
        eng.build_from_queries(&distinct_queries(1..=10));
        eng.bulk_ingest(&distinct_queries(11..=20));
        // Locals are issued in insertion order, so q1 is (seg 0, local 0).
        eng.tombstone_in(0, 0).expect("positional tombstone");
        assert!(!match_ids(&eng, &title_for(1)).contains(&1), "q1 gone live");
        eng.compact_all().expect("compaction ran");
        // crash with the stale positional frame still in the WAL
    }
    let eng = Engine::open(make_norm(), no_compaction_cfg(&dir)).expect("reopen");
    let mut wrong = Vec::new();
    for i in 1..=20u64 {
        let want = i != 1;
        let got = match_ids(&eng, &title_for(i)).contains(&i);
        if got != want {
            wrong.push((i, want, got));
        }
    }
    assert!(
        wrong.is_empty(),
        "stale positional frame must be skipped, not replayed against renumbered \
         addresses (id, want, got): {wrong:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Replay order: delete(X) → manifest commit (bulk ingest bakes the delete) →
/// re-insert(X) → crash. The delete frame sorts at/below the commit's watermark and
/// is skipped (its tombstones are baked); the later insert frame replays and
/// recreates X with its new semantics.
#[test]
fn delete_then_commit_then_reinsert_replays_in_order() {
    let dir = test_dir("tomb_delete_reinsert_order");
    {
        let mut eng = Engine::with_config(make_norm(), no_compaction_cfg(&dir));
        eng.build_from_queries(&distinct_queries(1..=5));
        assert!(eng.delete_by_logical_id(4).expect("delete") >= 1);
        // Manifest commit between the delete and the re-insert (bulk ingest commits).
        eng.bulk_ingest(&distinct_queries(21..=25));
        // Re-insert logical 4 with NEW semantics.
        eng.insert_live("player4b unique4b", 4, 2);
        // crash
    }
    let eng = Engine::open(make_norm(), no_compaction_cfg(&dir)).expect("reopen");
    assert!(
        !match_ids(&eng, &title_for(4)).contains(&4),
        "old q4 semantics must stay deleted"
    );
    assert!(
        match_ids(&eng, "rookie card player4b unique4b").contains(&4),
        "re-inserted q4 must survive via the WAL tail"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Codex P1: bulk ingest bypasses the WAL, so a same-id query bulk-ingested AFTER a
/// delete exists only in the attached segments at recovery time — replaying the older
/// delete frame over it would erase the newer query. The watermark skip (the bulk
/// commit covers the delete's seq) must keep the bulk-ingested copy alive.
#[test]
fn delete_then_bulk_reinsert_same_id_survives_crash() {
    let dir = test_dir("tomb_bulk_reinsert_same_id");
    {
        let mut eng = Engine::with_config(make_norm(), no_compaction_cfg(&dir));
        eng.build_from_queries(&distinct_queries(1..=5));
        assert!(eng.delete_by_logical_id(2).expect("delete") >= 1);
        // Re-add logical 2 with NEW semantics via the WAL-less bulk path (its
        // segment + manifest commit is the durable record, ADR-017).
        eng.bulk_ingest(&[(2, "player2b unique2b".to_string())]);
        assert!(match_ids(&eng, "rookie card player2b unique2b").contains(&2));
        // crash with the delete frame still in the WAL
    }
    let eng = Engine::open(make_norm(), no_compaction_cfg(&dir)).expect("reopen");
    assert!(
        match_ids(&eng, "rookie card player2b unique2b").contains(&2),
        "the bulk-ingested replacement must NOT be erased by the older delete frame"
    );
    assert!(
        !match_ids(&eng, &title_for(2)).contains(&2),
        "old q2 semantics must stay deleted"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Codex P2: a reset (header-only) WAL rescans to seq 1 on reopen while the manifest
/// keeps its watermark — without re-pinning the sequence, deletes issued AFTER the
/// reopen would sort at/below the watermark and be skipped by the NEXT recovery
/// (resurrecting acknowledged deletes). Covers both the logical and positional frames.
#[test]
fn deletes_after_reopen_with_reset_wal_survive_second_crash() {
    let dir = test_dir("tomb_seq_across_reopen");
    // Stage 1: establish a manifest with a non-zero watermark, then a clean close
    // with a RESET (empty) WAL.
    {
        let mut eng = Engine::with_config(make_norm(), no_compaction_cfg(&dir));
        eng.build_from_queries(&distinct_queries(1..=10));
        assert!(eng.delete_by_logical_id(1).expect("delete") >= 1);
        eng.insert_live("filler query", 100, 1);
        eng.flush(); // manifest watermark > 0; WAL checkpoints + resets
    }
    // Stage 2: reopen (the WAL file is header-only), delete more — one logical, one
    // positional — then crash with those frames as the only record.
    {
        let mut eng = Engine::open(make_norm(), no_compaction_cfg(&dir)).expect("reopen 1");
        assert!(eng.delete_by_logical_id(2).expect("delete q2") >= 1);
        // q3 sits at (seg 0, local 2): locals are issued in insertion order.
        eng.tombstone_in(0, 2).expect("positional tombstone");
        assert!(!match_ids(&eng, &title_for(3)).contains(&3), "q3 gone live");
        // crash
    }
    let eng = Engine::open(make_norm(), no_compaction_cfg(&dir)).expect("reopen 2");
    assert!(
        !match_ids(&eng, &title_for(2)).contains(&2),
        "post-reopen logical delete must replay (not be skipped by the stale watermark)"
    );
    assert!(
        !match_ids(&eng, &title_for(3)).contains(&3),
        "post-reopen positional tombstone must replay (not be skipped by the stale watermark)"
    );
    for i in 4..=10 {
        assert!(match_ids(&eng, &title_for(i)).contains(&i), "q{i} intact");
    }
    let _ = std::fs::remove_dir_all(&dir);
}
