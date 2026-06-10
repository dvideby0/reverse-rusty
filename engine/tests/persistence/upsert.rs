//! Atomic upsert — replace-by-id (ADR-067, closing ADR-064 item 1).
//!
//! The divergence being closed: a re-PUT used to insert a second live copy without
//! tombstoning the old one, so the id matched under EITHER version's semantics until
//! an explicit DELETE (live-verified in the ADR-064 audit: re-PUT then DELETE
//! reported `deleted_count: 2`), and the DELETE-then-PUT replace recipe left a
//! no-match window — including in the WAL, where a crash between the two frames
//! recovered the deleted state without the insert.
//!
//! These tests pin the new contract: one frame, one critical section — the old
//! version stops matching exactly when the new one starts, across every crash /
//! flush / compaction / bulk interleaving, and a rejected new version never deletes.

use reverse_rusty::config::EngineConfig;
use reverse_rusty::segment::{Engine, UpsertOutcome};
use std::path::Path;

use crate::harness::{make_norm, match_ids, test_dir};

fn no_compaction_cfg(dir: &Path) -> EngineConfig {
    EngineConfig {
        data_dir: Some(dir.to_path_buf()),
        auto_compact_on_flush: false,
        auto_compact_on_ingest: false,
        holes_ratio_threshold: 1.0,
        ..EngineConfig::default()
    }
}

const OLD_TITLE: &str = "1986 fleer michael jordan rookie";
const NEW_TITLE: &str = "2003 topps lebron james rookie";

/// The ADR-064 acceptance pin: re-PUT a NARROWER query — the old semantics must stop
/// matching immediately (no "matches under either version" window), and a subsequent
/// delete reports ONE live copy, not two.
#[test]
fn reput_replaces_atomically_and_delete_count_is_one() {
    let dir = test_dir("upsert_reput_narrower");
    let mut eng = Engine::with_config(make_norm(), no_compaction_cfg(&dir));

    let first = eng
        .try_upsert_live("michael jordan", 1, 1)
        .expect("first put");
    assert!(
        matches!(first, UpsertOutcome::Created(_)),
        "fresh id ⇒ Created"
    );
    assert!(match_ids(&eng, OLD_TITLE).contains(&1));

    let second = eng.try_upsert_live("lebron james", 1, 2).expect("re-put");
    assert!(
        matches!(second, UpsertOutcome::Updated { replaced: 1, .. }),
        "re-put ⇒ Updated with exactly 1 prior copy, got {second:?}"
    );
    assert!(
        !match_ids(&eng, OLD_TITLE).contains(&1),
        "old semantics must stop matching at the upsert (the audit's divergence)"
    );
    assert!(
        match_ids(&eng, NEW_TITLE).contains(&1),
        "new semantics match"
    );

    let deleted = eng.delete_by_logical_id(1).expect("delete");
    assert_eq!(
        deleted, 1,
        "exactly one live copy after a re-put (was 2 pre-fix)"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Legacy multi-copy state (two additive inserts of the same id) is healed by one
/// upsert: every prior live copy is tombstoned.
#[test]
fn upsert_tombstones_all_prior_copies() {
    let dir = test_dir("upsert_multi_prior");
    let mut eng = Engine::with_config(make_norm(), no_compaction_cfg(&dir));
    // Two additive copies (the pre-ADR-067 re-PUT behavior), one in a base
    // segment, one in the memtable.
    eng.build_from_queries(&[(1, "michael jordan".to_string())]);
    eng.insert_live("michael jordan fleer", 1, 2);

    let out = eng.try_upsert_live("lebron james", 1, 3).expect("upsert");
    assert!(
        matches!(out, UpsertOutcome::Updated { replaced: 2, .. }),
        "both prior copies tombstoned, got {out:?}"
    );
    assert!(!match_ids(&eng, OLD_TITLE).contains(&1));
    assert!(!match_ids(&eng, "1986 fleer michael jordan").contains(&1));
    assert!(match_ids(&eng, NEW_TITLE).contains(&1));
    assert_eq!(eng.delete_by_logical_id(1).expect("delete"), 1);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Crash atomicity, bare WAL tail: the single Upsert frame recovers BOTH halves —
/// never the delete without the insert (the DELETE-then-PUT window) and never the
/// insert without the delete (the additive divergence).
#[test]
fn upsert_survives_wal_tail_crash() {
    let dir = test_dir("upsert_wal_tail");
    {
        let mut eng = Engine::with_config(make_norm(), no_compaction_cfg(&dir));
        eng.build_from_queries(&[(1, "michael jordan".to_string())]);
        eng.try_upsert_live("lebron james", 1, 2).expect("upsert");
        // crash: the upsert exists only as the WAL frame
    }
    let eng = Engine::open(make_norm(), no_compaction_cfg(&dir)).expect("reopen");
    assert!(
        !match_ids(&eng, OLD_TITLE).contains(&1),
        "the tombstone half must recover"
    );
    assert!(
        match_ids(&eng, NEW_TITLE).contains(&1),
        "the insert half must recover"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Replace survives flush + reopen: the flush bakes the prior copy's tombstone into
/// the manifest bitmaps (ADR-066) and the flushed segment carries the new version —
/// the exact restart path that resurrected replaced versions before ADR-066.
#[test]
fn upsert_survives_flush_and_reopen() {
    let dir = test_dir("upsert_flush_reopen");
    {
        let mut eng = Engine::with_config(make_norm(), no_compaction_cfg(&dir));
        eng.build_from_queries(&[(1, "michael jordan".to_string())]);
        eng.try_upsert_live("lebron james", 1, 2).expect("upsert");
        eng.flush(); // manifest bitmaps + WAL reset
    }
    let eng = Engine::open(make_norm(), no_compaction_cfg(&dir)).expect("reopen");
    assert!(
        !match_ids(&eng, OLD_TITLE).contains(&1),
        "old stays replaced"
    );
    assert!(match_ids(&eng, NEW_TITLE).contains(&1), "new survives");
    let _ = std::fs::remove_dir_all(&dir);
}

/// A same-id query bulk-ingested AFTER the upsert (bulk bypasses the WAL, ADR-017)
/// must survive the frame's replay: at/below the watermark the segment-tombstone
/// half is baked + skipped, while the insert half still replays (the memtable copy
/// exists only in the frame).
#[test]
fn upsert_then_bulk_same_id_survives_crash() {
    let dir = test_dir("upsert_bulk_same_id");
    {
        let mut eng = Engine::with_config(make_norm(), no_compaction_cfg(&dir));
        eng.build_from_queries(&[(1, "michael jordan".to_string())]);
        eng.try_upsert_live("lebron james", 1, 2).expect("upsert");
        // Bulk re-add of the same id, with a manifest commit covering the frame.
        eng.bulk_ingest(&[(1, "kobe bryant".to_string())]);
        // crash
    }
    let eng = Engine::open(make_norm(), no_compaction_cfg(&dir)).expect("reopen");
    assert!(
        match_ids(&eng, "2008 topps kobe bryant").contains(&1),
        "the bulk-ingested copy must NOT be erased by the older upsert frame"
    );
    assert!(
        match_ids(&eng, NEW_TITLE).contains(&1),
        "the upsert's own (memtable) copy must still recover"
    );
    assert!(
        !match_ids(&eng, OLD_TITLE).contains(&1),
        "the pre-upsert copy stays replaced (baked in the manifest bitmaps)"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// The memtable is WAL-truth: a prior memtable copy is recreated by its own earlier
/// insert frame, so the upsert frame must re-tombstone it even when a manifest
/// commit covers the frame (the segment/memtable state-domain split).
#[test]
fn upsert_retombstones_memtable_prior_despite_watermark() {
    let dir = test_dir("upsert_memtable_prior");
    {
        let mut eng = Engine::with_config(make_norm(), no_compaction_cfg(&dir));
        eng.insert_live("michael jordan", 1, 1); // memtable copy, WAL frame
        eng.try_upsert_live("lebron james", 1, 2).expect("upsert");
        // A manifest commit (unrelated bulk) covers both frames with its watermark.
        eng.bulk_ingest(&[(50, "wander franco".to_string())]);
        // crash
    }
    let eng = Engine::open(make_norm(), no_compaction_cfg(&dir)).expect("reopen");
    assert!(
        !match_ids(&eng, OLD_TITLE).contains(&1),
        "the replayed insert frame recreates v1; the upsert frame must re-tombstone it"
    );
    assert!(match_ids(&eng, NEW_TITLE).contains(&1), "v2 recovers");
    let _ = std::fs::remove_dir_all(&dir);
}

/// A class-D new version is rejected and must leave the prior version live —
/// matching ES `index` semantics (a failed op leaves the old document) — both live
/// and across a crash (the frame replays to the same rejection).
#[test]
fn upsert_classd_rejection_leaves_old_live() {
    let dir = test_dir("upsert_classd");
    {
        let mut eng = Engine::with_config(make_norm(), no_compaction_cfg(&dir));
        eng.build_from_queries(&[(1, "michael jordan".to_string())]);
        let out = eng
            .try_upsert_live("-graded", 1, 2)
            .expect("a negation-only query parses; it is rejected at classification");
        assert!(matches!(out, UpsertOutcome::RejectedClassD), "got {out:?}");
        assert!(
            match_ids(&eng, OLD_TITLE).contains(&1),
            "a failed replace must never delete"
        );
        // crash with the rejected upsert's frame in the WAL
    }
    let eng = Engine::open(make_norm(), no_compaction_cfg(&dir)).expect("reopen");
    assert!(
        match_ids(&eng, OLD_TITLE).contains(&1),
        "replay reaches the same class-D rejection; the old version stays live"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// The ADR-066 sequence re-pinning covers upsert frames too: an upsert issued after
/// a reopen-with-reset-WAL must not have its segment-tombstone half skipped by the
/// stale watermark on the next recovery.
#[test]
fn upsert_after_reopen_with_reset_wal_survives_second_crash() {
    let dir = test_dir("upsert_seq_across_reopen");
    {
        let mut eng = Engine::with_config(make_norm(), no_compaction_cfg(&dir));
        eng.build_from_queries(&[(1, "michael jordan".to_string())]);
        eng.insert_live("filler", 50, 1);
        eng.flush(); // non-zero watermark; WAL reset
    }
    {
        let mut eng = Engine::open(make_norm(), no_compaction_cfg(&dir)).expect("reopen 1");
        eng.try_upsert_live("lebron james", 1, 2).expect("upsert");
        // crash
    }
    let eng = Engine::open(make_norm(), no_compaction_cfg(&dir)).expect("reopen 2");
    assert!(
        !match_ids(&eng, OLD_TITLE).contains(&1),
        "the post-reopen upsert's tombstone half must replay (not be watermark-skipped)"
    );
    assert!(match_ids(&eng, NEW_TITLE).contains(&1));
    let _ = std::fs::remove_dir_all(&dir);
}

/// The `rejected_class_d` counter is manifest-persisted, so a replayed upsert frame
/// must not re-increment it (codex): one rejected request stays ONE across a
/// manifest commit + restart, not one-per-restart.
#[test]
fn rejected_upsert_counter_does_not_double_count_across_restart() {
    let dir = test_dir("upsert_classd_counter");
    {
        let mut eng = Engine::with_config(make_norm(), no_compaction_cfg(&dir));
        eng.build_from_queries(&[(1, "michael jordan".to_string())]);
        let out = eng.try_upsert_live("-graded", 1, 2).expect("upsert");
        assert!(matches!(out, UpsertOutcome::RejectedClassD));
        assert_eq!(eng.rejected_class_d(), 1, "counted once, live");
        // A manifest commit persists the counter while the frame stays in the WAL.
        eng.bulk_ingest(&[(2, "kobe bryant".to_string())]);
        assert_eq!(eng.rejected_class_d(), 1);
        // crash
    }
    let eng = Engine::open(make_norm(), no_compaction_cfg(&dir)).expect("reopen");
    assert_eq!(
        eng.rejected_class_d(),
        1,
        "replaying the rejected frame must not re-count it"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Tags ride the upsert frame: a replacement's tags survive crash recovery, and the
/// replaced copy's tags die with it (newest-live-copy resolution).
#[test]
fn upsert_tags_survive_recovery() {
    let dir = test_dir("upsert_tags");
    {
        let mut eng = Engine::with_config(make_norm(), no_compaction_cfg(&dir));
        eng.try_upsert_live_with_tags(
            "michael jordan",
            1,
            1,
            &[("category".to_string(), "cards".to_string())],
        )
        .expect("put");
        eng.try_upsert_live_with_tags(
            "lebron james",
            1,
            2,
            &[("category".to_string(), "modern".to_string())],
        )
        .expect("re-put");
        // crash
    }
    let eng = Engine::open(make_norm(), no_compaction_cfg(&dir)).expect("reopen");
    let snap = eng.snapshot();
    let mut s = reverse_rusty::segment::MatchScratch::new();
    let mut out = Vec::new();
    // Filter by the NEW tag: the recovered replacement must be reachable.
    let pred = snap.compile_tag_predicate(&[("category".to_string(), vec!["modern".to_string()])]);
    snap.match_title_filtered(NEW_TITLE, &mut s, &mut out, true, &pred);
    assert!(out.contains(&1), "recovered upsert keeps its tags");
    // The OLD tag no longer reaches anything for this id.
    let mut out_old = Vec::new();
    let pred_old =
        snap.compile_tag_predicate(&[("category".to_string(), vec!["cards".to_string()])]);
    snap.match_title_filtered(NEW_TITLE, &mut s, &mut out_old, true, &pred_old);
    assert!(
        !out_old.contains(&1),
        "the replaced copy's tags must not filter-match the live query"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
