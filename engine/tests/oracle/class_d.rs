//! The class-D always-candidate lane differential — the ADR-068 vacuous-accept
//! oracle. Negation-only queries (no positives, only forbidden features) stored
//! under the `accept_class_d` lane must match exactly the titles bearing none of
//! their forbidden features — per-title AND batch — and must survive every
//! lifecycle edge: tombstones, compaction (plain + re-anchoring), flush → mmap
//! reopen, WAL replay under a flipped knob, and a vocabulary change. Lane off
//! (the default) pins today's loud reject, and the effectively empty query is
//! rejected even with the lane on.

use crate::harness::*;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::gen::{gen_class_d_queries, generate, GenConfig};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{
    BatchMatchOptions, BroadStrategy, Engine, InsertOutcome, MatchScratch,
};
use std::collections::HashSet;

/// Logical-id offset for the negation-only queries, so they are recognizable in
/// result sets (every generated regular id is far below this).
const CLASS_D_ID_BASE: u64 = 1_000_000;

/// A regular corpus with negation-only queries interleaved every `step` slots,
/// so the multi-segment builders spread them across base segments AND the
/// memtable tail.
fn mixed_corpus(
    seed: u64,
    n_regular: usize,
    n_class_d: usize,
) -> (Vec<(u64, String)>, Vec<String>) {
    let data = generate(&GenConfig {
        num_queries: n_regular,
        num_titles: 2_000,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed,
        num_players: 2_000,
        num_sets: 1_000,
    });
    let class_d = gen_class_d_queries(seed ^ 0xD00D, n_class_d);
    let step = (n_regular / n_class_d.max(1)).max(1);
    let mut queries: Vec<(u64, String)> = Vec::with_capacity(n_regular + n_class_d);
    let mut di = 0usize;
    for (i, (id, text)) in data.queries.into_iter().enumerate() {
        if i % step == 0 && di < class_d.len() {
            queries.push((CLASS_D_ID_BASE + di as u64, class_d[di].clone()));
            di += 1;
        }
        queries.push((id, text));
    }
    while di < class_d.len() {
        queries.push((CLASS_D_ID_BASE + di as u64, class_d[di].clone()));
        di += 1;
    }
    (queries, data.titles)
}

fn lane_on() -> EngineConfig {
    EngineConfig {
        accept_class_d: true,
        ..EngineConfig::default()
    }
}

/// Multi-segment engine with the lane on: base segments + bulk + a live memtable
/// tail, mirroring the core oracle's builder.
fn build_multi_lane_on(queries: &[(u64, String)]) -> Engine {
    let mut eng = Engine::with_config(Normalizer::default_vocab().expect("vocab"), lane_on());
    let n = queries.len();
    let c = n / 4;
    eng.build_from_queries(&queries[..c]);
    eng.bulk_ingest(&queries[c..2 * c]);
    eng.bulk_ingest(&queries[2 * c..3 * c]);
    for (id, text) in &queries[3 * c..] {
        eng.insert_live(text, *id, 1);
    }
    eng
}

fn per_title_sets(eng: &Engine, titles: &[String], include_broad: bool) -> Vec<HashSet<u64>> {
    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let mut res = Vec::with_capacity(titles.len());
    for t in titles {
        eng.match_title(t, &mut s, &mut out, include_broad);
        res.push(out.iter().copied().collect());
    }
    res
}

fn assert_no_fn_fp(engine_sets: &[HashSet<u64>], brute: &Brute, titles: &[String], ctx: &str) {
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    let (mut fneg, mut fpos, mut truth_total) = (0usize, 0usize, 0usize);
    for (i, title) in titles.iter().enumerate() {
        let truth = brute.matches(title, &mut blc, &mut bfeats);
        truth_total += truth.len();
        fneg += truth.difference(&engine_sets[i]).count();
        fpos += engine_sets[i].difference(&truth).count();
    }
    assert_eq!(fneg, 0, "{ctx}: FALSE NEGATIVES — contract violated");
    assert_eq!(
        fpos, 0,
        "{ctx}: false positives — exact matcher is not exact"
    );
    assert!(
        truth_total > 0,
        "{ctx}: degenerate corpus, no matches at all"
    );
}

#[test]
fn vacuous_accept_differential_per_title_and_batch() {
    let (queries, titles) = mixed_corpus(0x00C1_A55D, 20_000, 400);
    let eng = build_multi_lane_on(&queries);
    let brute = Brute::build_accepting_class_d(&queries);

    // The corpus must exercise BOTH vacuous outcomes: a class-D query matching a
    // title, and a forbidden feature rejecting one.
    let engine_sets = per_title_sets(&eng, &titles, true);
    let d_matched = engine_sets
        .iter()
        .any(|s| s.iter().any(|&id| id >= CLASS_D_ID_BASE));
    let d_rejected_somewhere = engine_sets
        .iter()
        .any(|s| (0..400u64).any(|d| !s.contains(&(CLASS_D_ID_BASE + d))));
    assert!(
        d_matched,
        "no class-D query ever matched — degenerate corpus"
    );
    assert!(
        d_rejected_somewhere,
        "no forbidden feature ever rejected a class-D query — degenerate corpus"
    );

    // Per-title path ≡ brute.
    assert_no_fn_fp(&engine_sets, &brute, &titles, "per-title");

    // Batch path ≡ brute: columnar (both materialization modes) + the inline
    // kill-switch, across a word-boundary batch size and a degenerate one.
    let snap = eng.snapshot();
    for (strat, mat) in [
        (BroadStrategy::Columnar, true),
        (BroadStrategy::Columnar, false),
        (BroadStrategy::Inline, true),
    ] {
        for bs in [1usize, 64, 256] {
            let mut sets: Vec<HashSet<u64>> = vec![HashSet::new(); titles.len()];
            for (idx, ids) in snap.match_titles_batch(
                &titles,
                BatchMatchOptions {
                    include_broad: true,
                    broad_batch_size: bs,
                    broad_strategy: strat,
                    broad_materialize: mat,
                },
            ) {
                sets[idx] = ids.into_iter().collect();
            }
            assert_no_fn_fp(
                &sets,
                &brute,
                &titles,
                &format!("batch strat={strat:?} mat={mat} bs={bs}"),
            );
        }
    }
}

#[test]
fn broad_off_quarantines_class_d_like_class_c() {
    let (queries, titles) = mixed_corpus(0xD0FF, 8_000, 200);
    let eng = build_multi_lane_on(&queries);
    // With the broad lane excluded, an always-candidate is invisible — the same
    // documented quarantine semantics as class C (the lane it rides).
    for set in per_title_sets(&eng, &titles, false) {
        assert!(
            set.iter().all(|&id| id < CLASS_D_ID_BASE),
            "a class-D query matched with include_broad=false"
        );
    }
}

#[test]
fn lane_off_pins_the_loud_reject() {
    let (queries, titles) = mixed_corpus(0x0FF0, 8_000, 200);
    // Default config: every negation-only query is rejected and counted, none
    // stored, and the engine still satisfies the (rejecting) oracle over the
    // same mixed input.
    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    let report = eng.build_from_queries(&queries);
    assert_eq!(report.rejected_class_d, 200, "every class-D query rejected");
    assert_eq!(
        eng.class_counts()[3],
        0,
        "c[3] counts STORED class-D entries (ADR-068) — none under the default"
    );
    assert_eq!(eng.rejected_class_d(), 200);

    let engine_sets = per_title_sets(&eng, &titles, true);
    for set in &engine_sets {
        assert!(set.iter().all(|&id| id < CLASS_D_ID_BASE));
    }
    let brute = Brute::build(&queries); // the class-D-rejecting reference
    assert_no_fn_fp(&engine_sets, &brute, &titles, "lane-off");
}

#[test]
fn stored_class_d_is_counted_and_introspectable() {
    let (queries, _titles) = mixed_corpus(0xC0DE, 4_000, 100);
    let eng = build_multi_lane_on(&queries);
    assert_eq!(
        eng.class_counts()[3],
        100,
        "every accepted always-candidate counted as stored class D"
    );
    assert_eq!(
        eng.rejected_class_d(),
        0,
        "nothing rejected with the lane on"
    );
}

#[test]
fn effectively_empty_query_rejected_even_with_lane_on() {
    let mut eng = Engine::with_config(Normalizer::default_vocab().expect("vocab"), lane_on());
    // No positives AND no forbidden: parses to an empty AST, classifies D, and is
    // rejected regardless of the lane — storing it would be a match-all.
    for q in ["", "   "] {
        match eng.try_insert_live(q, 9_999, 1) {
            Ok(InsertOutcome::RejectedClassD) => {}
            other => panic!("empty query must reject as class D, got {other:?}"),
        }
    }
    assert_eq!(eng.rejected_class_d(), 2);
    // The bulk path agrees.
    let (report, statuses) = eng
        .try_bulk_ingest_detailed(&[(1, "-auto".into()), (2, String::new())])
        .expect("bulk");
    assert_eq!(report.ingested, 1, "the forbidden-only query is accepted");
    assert_eq!(report.rejected_class_d, 1, "the empty query is not");
    assert!(matches!(
        statuses[1],
        reverse_rusty::segment::IngestItemStatus::RejectedClassD
    ));
}

#[test]
fn tombstones_and_compaction_preserve_always_candidates() {
    for reanchor in [false, true] {
        let (queries, titles) = mixed_corpus(0xDEAD ^ u64::from(reanchor), 8_000, 200);
        let mut eng = Engine::with_config(
            Normalizer::default_vocab().expect("vocab"),
            EngineConfig {
                accept_class_d: true,
                compaction_reanchor: reanchor,
                ..EngineConfig::default()
            },
        );
        let n = queries.len();
        eng.build_from_queries(&queries[..n / 2]);
        eng.bulk_ingest(&queries[n / 2..]);

        // Delete half the class-D queries (and a few regular ones for churn).
        let mut deleted: HashSet<u64> = HashSet::new();
        for d in (0..200u64).step_by(2) {
            let id = CLASS_D_ID_BASE + d;
            eng.delete_by_logical_id(id).expect("delete");
            deleted.insert(id);
        }
        for id in [3u64, 5, 7] {
            eng.delete_by_logical_id(id).expect("delete");
            deleted.insert(id);
        }
        eng.compact_all();

        let kept: Vec<(u64, String)> = queries
            .iter()
            .filter(|(id, _)| !deleted.contains(id))
            .cloned()
            .collect();
        let brute = Brute::build_accepting_class_d(&kept);
        let engine_sets = per_title_sets(&eng, &titles, true);
        for set in &engine_sets {
            assert!(
                set.is_disjoint(&deleted),
                "a deleted query resurfaced (reanchor={reanchor})"
            );
        }
        assert_no_fn_fp(
            &engine_sets,
            &brute,
            &titles,
            &format!("post-compaction reanchor={reanchor}"),
        );
    }
}

#[test]
fn durability_flush_mmap_and_wal_replay_survive_a_knob_flip() {
    let dir = tempdir();
    let titles = ["1990 brand card auto lot", "1990 brand card mint"];
    {
        let mut cfg = lane_on();
        cfg.data_dir = Some(dir.clone());
        let mut eng = Engine::open(Normalizer::default_vocab().expect("vocab"), cfg)
            .expect("open fresh durable engine");
        // d1 sealed into a .seg (the mmap universal-probe path), d2 left in the
        // WAL tail (the replay path), plus one regular query for ballast.
        assert!(matches!(
            eng.try_insert_live("-auto -signed", CLASS_D_ID_BASE, 1),
            Ok(InsertOutcome::Inserted(_))
        ));
        eng.insert_live("1990 brand", 7, 1);
        eng.flush();
        assert!(matches!(
            eng.try_insert_live("-reprint -lot", CLASS_D_ID_BASE + 1, 1),
            Ok(InsertOutcome::Inserted(_))
        ));
    }
    // Reopen with the lane OFF: the knob gates acceptance, never visibility —
    // the sealed entry decodes from the segment, and the WAL-tail entry replays
    // unconditionally (the log holds only accepted mutations).
    let cfg_off = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let mut eng = Engine::open(Normalizer::default_vocab().expect("vocab"), cfg_off)
        .expect("reopen with the lane off");
    // Both the Engine and snapshot counts cover mmap'd segments (the Engine path
    // previously skipped them — closed in this change, a codex catch).
    assert_eq!(
        eng.class_counts()[3],
        2,
        "both always-candidates recovered (one mmap'd, one replayed)"
    );
    assert_eq!(eng.snapshot().class_counts()[3], 2);

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title(titles[1], &mut s, &mut out, true);
    let set: HashSet<u64> = out.iter().copied().collect();
    assert!(
        set.contains(&CLASS_D_ID_BASE),
        "sealed entry lost on reopen"
    );
    assert!(
        set.contains(&(CLASS_D_ID_BASE + 1)),
        "WAL-tail entry dropped by the flipped knob — acknowledged write lost"
    );
    eng.match_title(titles[0], &mut s, &mut out, true);
    let set: HashSet<u64> = out.iter().copied().collect();
    assert!(
        !set.contains(&CLASS_D_ID_BASE),
        "forbidden feature ignored after reopen"
    );

    // ... while NEW negation-only ingest under the reopened engine is rejected:
    // the knob still gates the front door.
    assert!(matches!(
        eng.try_insert_live("-checklist", CLASS_D_ID_BASE + 2, 1),
        Ok(InsertOutcome::RejectedClassD)
    ));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn vocab_change_keeps_always_candidates() {
    let (queries, titles) = mixed_corpus(0x0007_0CAB, 4_000, 100);
    let mut eng = build_multi_lane_on(&queries);
    // A vocabulary change triggers the blue/green recompile; a stored
    // always-candidate must survive it (the recompile passes accept=true
    // unconditionally — a stored query is never dropped by the knob).
    let mut vocab = reverse_rusty::vocab::Vocab::new();
    vocab.add_synonym("rc", "rookie", reverse_rusty::dict::FeatureKind::Category);
    eng.set_vocab(vocab).expect("set_vocab");
    eng.recompile_stale_segments();

    let brute = Brute::build_accepting_class_d(&queries);
    let engine_sets = per_title_sets(&eng, &titles, true);
    // The vocab change can alter REGULAR query/title features (the brute above
    // is vocab-less), so assert only the class-D side: every class-D truth match
    // is still present (zero FN for the always-candidates).
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    let mut d_truth_total = 0usize;
    for (i, title) in titles.iter().enumerate() {
        let truth = brute.matches(title, &mut blc, &mut bfeats);
        for id in truth {
            if id >= CLASS_D_ID_BASE {
                d_truth_total += 1;
                assert!(
                    engine_sets[i].contains(&id),
                    "class-D query {id} lost by the vocab recompile (title {i})"
                );
            }
        }
    }
    assert!(d_truth_total > 0, "degenerate: no class-D truth matches");
}

fn tempdir() -> std::path::PathBuf {
    // A process-wide counter guarantees uniqueness even when two of these tests call
    // tempdir() concurrently under a coarse system clock — `as_nanos()` alone can
    // collide there, and a shared data dir corrupts both tests' segments/manifest
    // (the "0 recovered" flake the parallel `--test oracle` run surfaced).
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "rr-class-d-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
        SEQ.fetch_add(1, Ordering::Relaxed),
    ));
    std::fs::create_dir_all(&dir).expect("create tempdir");
    dir
}

/// The UPGRADE pin (codex P1 on the first cut of ADR-068): a pre-v5 binary logged
/// a frame BEFORE classifying, so its WAL can hold op-0/op-4 frames whose write
/// was acknowledged as `RejectedClassD`. Replay must apply those under the OLD
/// reject gate — accepting the legacy op-4 upsert would not just resurrect the
/// rejected version, it would tombstone the acknowledged-live prior one (a false
/// negative). Only the new op-5/op-6 frames carry the accepted marker.
#[test]
fn legacy_rejected_class_d_frames_replay_under_the_old_gate() {
    use reverse_rusty::wal::Wal;
    let dir = tempdir();
    {
        // Simulate the OLD binary's WAL: a normal insert, then a logged-but-
        // rejected negation-only insert (op 0), then a logged-but-rejected
        // negation-only UPSERT of the live query (op 4). `append_insert` /
        // `append_upsert` still emit the legacy ops — exactly what a pre-v5
        // binary wrote.
        let mut wal = Wal::open(&dir.join("wal.log"), false).expect("wal");
        wal.append_insert(7, 1, "1990 brand", &[]).expect("frame");
        wal.append_insert(8, 1, "-auto", &[]).expect("frame");
        wal.append_upsert(7, 2, "-brand", &[]).expect("frame");
    }
    let cfg = EngineConfig {
        accept_class_d: true, // the knob must NOT resurrect legacy rejected frames
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let eng = Engine::open(Normalizer::default_vocab().expect("vocab"), cfg).expect("recover");

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    eng.match_title("1990 brand", &mut s, &mut out, true);
    let set: HashSet<u64> = out.iter().copied().collect();
    assert!(
        set.contains(&7),
        "the acknowledged-live prior version was tombstoned by a legacy \
         logged-but-rejected upsert frame — a false negative"
    );
    assert!(
        !set.contains(&8),
        "a legacy logged-but-rejected class-D insert resurrected on replay"
    );
    assert_eq!(
        eng.snapshot().class_counts()[3],
        0,
        "no legacy frame may materialize as a stored always-candidate"
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// The ROLLBACK fence (codex P1 on the first cut of ADR-068): a segment holding a
/// class-D always-candidate is written as format v4 (layout-identical to v3), so a
/// pre-ADR-068 reader — which never probes the universal signature — fails loudly
/// ("unsupported format version") instead of serving the file with those queries
/// silently unmatchable. Class-D-free segments keep writing v3 byte-identically.
#[test]
fn class_d_segments_write_the_v4_rollback_fence() {
    use reverse_rusty::storage::MmapSegment;
    let seg_version = |dir: &std::path::Path| -> u32 {
        let seg = std::fs::read_dir(dir.join("segments"))
            .expect("read segments dir")
            .filter_map(Result::ok)
            .map(|e| e.path())
            .find(|p| p.extension().is_some_and(|x| x == "seg"))
            .expect("a sealed segment file");
        let bytes = std::fs::read(&seg).expect("read segment");
        u32::from_le_bytes(bytes[4..8].try_into().expect("version word"))
    };

    // A class-D-bearing segment → v4.
    let dir_d = tempdir();
    {
        let mut cfg = lane_on();
        cfg.data_dir = Some(dir_d.clone());
        let mut eng = Engine::open(Normalizer::default_vocab().expect("vocab"), cfg).expect("open");
        eng.insert_live("1990 brand", 1, 1);
        eng.insert_live("-auto", 2, 1);
        eng.flush();
    }
    assert_eq!(
        seg_version(&dir_d),
        4,
        "class-D segment must carry the fence"
    );
    // The MANIFEST is the loud half of the fence (a pre-ADR-068 binary skips an
    // unreadable segment as corrupt + continues, but an unsupported manifest
    // version fails Engine::open outright): a class-D-bearing commit writes v4.
    let manifest_version = |dir: &std::path::Path| -> u32 {
        let bytes = std::fs::read(dir.join("manifest.bin")).expect("manifest");
        u32::from_le_bytes(bytes[4..8].try_into().expect("version word"))
    };
    assert_eq!(manifest_version(&dir_d), 4, "class-D commit ⇒ manifest v4");

    // A class-D-free segment → v3, byte-for-byte today's format.
    let dir_plain = tempdir();
    {
        let cfg = EngineConfig {
            data_dir: Some(dir_plain.clone()),
            ..EngineConfig::default()
        };
        let mut eng = Engine::open(Normalizer::default_vocab().expect("vocab"), cfg).expect("open");
        eng.insert_live("1990 brand", 1, 1);
        eng.flush();
    }
    assert_eq!(seg_version(&dir_plain), 3, "class-D-free segments stay v3");
    assert_eq!(
        manifest_version(&dir_plain),
        3,
        "class-D-free commit keeps manifest v3 byte-identically"
    );

    // Our own reader applies the same fence to future versions it doesn't know.
    let seg_path = std::fs::read_dir(dir_d.join("segments"))
        .expect("read segments dir")
        .filter_map(Result::ok)
        .map(|e| e.path())
        .find(|p| p.extension().is_some_and(|x| x == "seg"))
        .expect("segment file");
    let mut bytes = std::fs::read(&seg_path).expect("read");
    bytes[4..8].copy_from_slice(&7u32.to_le_bytes());
    // Re-seal the trailing whole-file CRC so the version check (which runs after
    // it) is what fires.
    let body = bytes.len() - 4;
    let crc = reverse_rusty::storage::crc32(&bytes[..body]);
    bytes[body..].copy_from_slice(&crc.to_le_bytes());
    let future = dir_d.join("future.seg");
    std::fs::write(&future, &bytes).expect("write");
    let err = MmapSegment::open(&future).expect_err("future version must fail loud");
    assert!(
        err.to_string().contains("unsupported format version"),
        "got: {err}"
    );

    std::fs::remove_dir_all(&dir_d).ok();
    std::fs::remove_dir_all(&dir_plain).ok();
}
