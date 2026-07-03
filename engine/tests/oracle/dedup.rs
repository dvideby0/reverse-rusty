//! The canonical-body dedup differential (Stage A of the Broad-Query Cost
//! Program's dedup lever).
//!
//! Queries whose SEMANTIC bodies are identical share one posting entry per
//! in-memory segment: the group is reached and verified once and emitted per
//! member — identity (logical id, version, tags) stays per-member. The
//! load-bearing claims pinned here:
//!
//! 1. **Zero FN/FP vs brute** on a heavily-duplicated corpus, per-title AND
//!    batch, both `include_broad` modes — the correctness contract.
//! 2. **A tombstoned LEADER never drops its members** (aliveness gates
//!    emission, never the shared body check), and per-member tags filter
//!    independently of the leader's.
//! 3. **The kill-switch is result-invariant**: `dedup_bodies = false` returns
//!    identical sets while scanning strictly more posting entries.
//! 4. **Flush expands, reopen matches**: the on-disk format carries no group
//!    indirection — a flushed grouped segment reopens (mmap + WAL-replay
//!    paths) and matches brute.
//! 5. **Compaction regroups** — including ACROSS segments (the merge is the
//!    cross-segment dedup mechanism).
//! 6. **The structural pin of the measured defect**: N identical stored
//!    queries cost ONE posting entry to scan, not N (the ADR-104 43,533-entry
//!    shared-posting finding, inverted).

use crate::harness::*;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{BatchMatchOptions, Engine, MatchScratch, MatchStats};
use std::collections::HashSet;

/// Replicate each generated query 0–3 extra times under fresh logical ids —
/// the identical-query concentration the lever targets. Returns the full
/// (duplicated) corpus; brute treats every copy as its own query.
fn duplicate_corpus(seed: u64, n: usize) -> Vec<(u64, String)> {
    let data = generate(&GenConfig {
        num_queries: n,
        num_titles: 1,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed,
        num_players: 800,
        num_sets: 400,
    });
    let mut out = Vec::new();
    let mut next_id = 10_000_000u64;
    for (i, (id, text)) in data.queries.iter().enumerate() {
        out.push((*id, text.clone()));
        for _ in 0..(i % 4) {
            out.push((next_id, text.clone()));
            next_id += 1;
        }
    }
    out
}

fn cfg_dedup(on: bool) -> EngineConfig {
    EngineConfig {
        dedup_bodies: on,
        ..EngineConfig::default()
    }
}

/// Multi-segment engine (base + bulk + live memtable tail) under `cfg`.
fn build_multi(queries: &[(u64, String)], cfg: EngineConfig) -> Engine {
    let mut eng = Engine::with_config(Normalizer::default_vocab().expect("vocab"), cfg);
    let n = queries.len();
    let c = n / 3;
    eng.build_from_queries(&queries[..c]);
    eng.bulk_ingest(&queries[c..2 * c]);
    for (id, text) in &queries[2 * c..] {
        eng.insert_live(text, *id, 1);
    }
    eng
}

fn per_title_sets(
    eng: &Engine,
    titles: &[String],
    include_broad: bool,
) -> (Vec<HashSet<u64>>, MatchStats) {
    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let mut res = Vec::with_capacity(titles.len());
    let mut agg = MatchStats::default();
    for t in titles {
        let st = eng.match_title(t, &mut s, &mut out, include_broad);
        agg.merge(st);
        res.push(out.iter().copied().collect());
    }
    (res, agg)
}

/// Broad-ON vs brute (the full truth set), scalar AND batch. Brute has no
/// broad-off notion; the broad-off legs are pinned engine≡engine by
/// [`assert_on_equals_off`] (the hot-oracle convention).
fn diff_vs_brute(eng: &Engine, brute: &Brute, titles: &[String], ctx: &str) {
    let mut lc = String::new();
    let mut feats = Vec::new();
    let (got, _) = per_title_sets(eng, titles, true);
    for (i, title) in titles.iter().enumerate() {
        let want = brute.matches(title, &mut lc, &mut feats);
        assert_eq!(got[i], want, "{ctx}: mismatch on {title:?}");
    }
    let batch = eng.snapshot().match_titles_batch(
        titles,
        BatchMatchOptions {
            include_broad: true,
            ..BatchMatchOptions::default()
        },
    );
    for (idx, ids) in batch {
        let got_b: HashSet<u64> = ids.iter().copied().collect();
        let want = brute.matches(&titles[idx], &mut lc, &mut feats);
        assert_eq!(got_b, want, "{ctx}: batch mismatch on {:?}", titles[idx]);
    }
}

/// Dedup on ≡ dedup off, BOTH `include_broad` modes, scalar and batch — the
/// kill-switch result-invariance that also covers the broad-off lane brute
/// cannot express.
fn assert_on_equals_off(on: &Engine, off: &Engine, titles: &[String], ctx: &str) {
    for include_broad in [false, true] {
        let (a, _) = per_title_sets(on, titles, include_broad);
        let (b, _) = per_title_sets(off, titles, include_broad);
        assert_eq!(
            a, b,
            "{ctx}: dedup changed results (include_broad={include_broad})"
        );
        let batch = on.snapshot().match_titles_batch(
            titles,
            BatchMatchOptions {
                include_broad,
                ..BatchMatchOptions::default()
            },
        );
        for (idx, ids) in batch {
            let got: HashSet<u64> = ids.iter().copied().collect();
            assert_eq!(
                got, a[idx],
                "{ctx}: batch != scalar under dedup (include_broad={include_broad})"
            );
        }
    }
}

#[test]
fn duplicated_corpus_matches_brute_per_title_and_batch() {
    let queries = duplicate_corpus(0xDED0_0001, 6_000);
    let data = generate(&GenConfig {
        num_queries: 10,
        num_titles: 1_200,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0xDED0_0002,
        num_players: 800,
        num_sets: 400,
    });
    let eng = build_multi(&queries, cfg_dedup(true));
    let snap = eng.snapshot();
    assert!(
        snap.dup_joined() > 0,
        "degenerate: the duplicated corpus formed no body groups"
    );
    assert_eq!(snap.bodies_total(), queries.len() as u64);
    let brute = Brute::build(&queries);
    diff_vs_brute(&eng, &brute, &data.titles, "dedup-on multi-segment");
    let off = build_multi(&queries, cfg_dedup(false));
    assert_on_equals_off(&eng, &off, &data.titles, "dedup-on multi-segment");
}

#[test]
fn dedup_off_is_result_identical_and_scans_more() {
    let queries = duplicate_corpus(0xDED0_0003, 4_000);
    let titles = generate(&GenConfig {
        num_queries: 10,
        num_titles: 600,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0xDED0_0004,
        num_players: 800,
        num_sets: 400,
    })
    .titles;
    let on = build_multi(&queries, cfg_dedup(true));
    let off = build_multi(&queries, cfg_dedup(false));
    assert_eq!(off.snapshot().dup_joined(), 0, "kill-switch must not group");
    let (res_on, stats_on) = per_title_sets(&on, &titles, true);
    let (res_off, stats_off) = per_title_sets(&off, &titles, true);
    assert_eq!(res_on, res_off, "dedup must be result-invariant");
    assert!(
        stats_on.postings_scanned < stats_off.postings_scanned,
        "sharing must scan strictly fewer posting entries on a duplicated \
         corpus (on={}, off={})",
        stats_on.postings_scanned,
        stats_off.postings_scanned
    );
    // The sketch sees THROUGH the duplication identically on both engines.
    let est_on = on.snapshot().distinct_bodies_est();
    let est_off = off.snapshot().distinct_bodies_est();
    assert_eq!(est_on, est_off, "the observe sketch is knob-independent");
}

/// Aliveness gates EMISSION, never the shared body check: killing the group
/// LEADER must keep every member matchable, killing members must keep the
/// leader, and killing the whole group must silence it.
#[test]
fn tombstoned_leader_keeps_members_matchable() {
    let mut eng = Engine::with_config(Normalizer::default_vocab().expect("vocab"), cfg_dedup(true));
    // Insertion order makes id 1 the group leader.
    for id in 1..=4u64 {
        eng.insert_live("psa 10 charizard", id, 1);
    }
    // A same-segment singleton so the segment stays non-trivial.
    eng.insert_live("pikachu holo", 9, 1);
    assert_eq!(eng.snapshot().dup_joined(), 3);

    let hits = |eng: &Engine| -> HashSet<u64> {
        let mut s = MatchScratch::new();
        let mut out = Vec::new();
        eng.match_title("psa 10 charizard slab", &mut s, &mut out, true);
        out.iter().copied().collect()
    };
    assert_eq!(hits(&eng), HashSet::from([1, 2, 3, 4]));

    // Kill the LEADER: members must survive it.
    eng.delete_by_logical_id(1).expect("delete leader");
    assert_eq!(hits(&eng), HashSet::from([2, 3, 4]));

    // Kill two members: the rest are untouched.
    eng.delete_by_logical_id(3).expect("delete member");
    eng.delete_by_logical_id(4).expect("delete member");
    assert_eq!(hits(&eng), HashSet::from([2]));

    // Kill the last member: the group is silent.
    eng.delete_by_logical_id(2).expect("delete last");
    assert_eq!(hits(&eng), HashSet::new());
}

/// Identity stays per-member: tag-divergent duplicates share the body but
/// filter independently — the leader's tags must never veto (or leak) a
/// member's emission, on the scalar AND the columnar-broad paths.
#[test]
fn tag_divergent_duplicates_filter_independently() {
    let mut eng = Engine::with_config(Normalizer::default_vocab().expect("vocab"), cfg_dedup(true));
    let tags = |v: &str| -> Vec<(String, String)> { vec![("store".to_string(), v.to_string())] };
    eng.insert_live_with_tags("psa 10 charizard", 1, 1, &tags("us")); // leader
    eng.insert_live_with_tags("psa 10 charizard", 2, 1, &tags("uk"));
    eng.insert_live_with_tags("psa 10 charizard", 3, 1, &tags("us"));
    assert_eq!(eng.snapshot().dup_joined(), 2);

    let snap = eng.snapshot();
    let filtered = |v: &str| -> HashSet<u64> {
        let pred = snap.compile_tag_predicate(&[("store".to_string(), vec![v.to_string()])]);
        let mut s = MatchScratch::new();
        let mut out = Vec::new();
        snap.match_title_filtered("psa 10 charizard slab", &mut s, &mut out, true, &pred);
        out.iter().copied().collect()
    };
    assert_eq!(filtered("us"), HashSet::from([1, 3]));
    assert_eq!(
        filtered("uk"),
        HashSet::from([2]),
        "the leader's tags must not veto a member"
    );
    assert_eq!(filtered("de"), HashSet::new());
}

/// Flush expands groups into plain on-disk postings; reopen (both the sealed
/// mmap path and the WAL-replay path) matches brute. Also pins that the
/// re-opened engine re-forms groups for NEW live writes.
fn tempdir(tag: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "rr-dedup-{tag}-{}-{}-{}",
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

#[test]
fn flush_reopen_and_wal_replay_match_brute() {
    let dir = tempdir("reopen");
    let queries = duplicate_corpus(0xDED0_0005, 2_500);
    let titles = generate(&GenConfig {
        num_queries: 10,
        num_titles: 400,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0xDED0_0006,
        num_players: 800,
        num_sets: 400,
    })
    .titles;
    let cfg = EngineConfig {
        data_dir: Some(dir.clone()),
        ..cfg_dedup(true)
    };
    let half = queries.len() / 2;
    {
        let mut eng = Engine::with_config(Normalizer::default_vocab().expect("vocab"), cfg.clone());
        for (id, text) in &queries[..half] {
            eng.insert_live(text, *id, 1);
        }
        eng.flush(); // ← groups expand into the .seg here
        for (id, text) in &queries[half..] {
            eng.insert_live(text, *id, 1); // ← stays in the WAL tail
        }
        // Drop WITHOUT flushing the tail: reopen exercises mmap + WAL replay.
    }
    let eng = Engine::open(Normalizer::default_vocab().expect("vocab"), cfg).expect("reopen");
    assert_eq!(eng.num_queries(), queries.len());
    assert!(
        eng.snapshot().dup_joined() > 0,
        "WAL replay must re-form groups in the memtable"
    );
    let brute = Brute::build(&queries);
    diff_vs_brute(&eng, &brute, &titles, "flush+replay reopen");
    drop(eng);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Compaction regroups shared bodies — including ACROSS source segments (the
/// merge is the cross-segment dedup mechanism): after merging two segments
/// that each hold copies of the same body, one posting entry serves them all.
#[test]
fn compaction_regroups_across_segments() {
    let mut eng = Engine::with_config(
        Normalizer::default_vocab().expect("vocab"),
        EngineConfig {
            auto_compact_on_ingest: false,
            auto_compact_on_flush: false,
            ..cfg_dedup(true)
        },
    );
    // Two bulk segments, each with 50 copies of the same body + distinct filler.
    let mut batch_a: Vec<(u64, String)> = (0..50u64)
        .map(|i| (i, "psa 10 charizard".to_string()))
        .collect();
    batch_a.extend((0..30u64).map(|i| (1_000 + i, format!("pikachu holo v{i}"))));
    let mut batch_b: Vec<(u64, String)> = (0..50u64)
        .map(|i| (100 + i, "psa 10 charizard".to_string()))
        .collect();
    batch_b.extend((0..30u64).map(|i| (2_000 + i, format!("blastoise ex n{i}"))));
    eng.build_from_queries(&batch_a);
    eng.bulk_ingest(&batch_b);

    let count_hit = |eng: &Engine| -> (usize, u32) {
        let mut s = MatchScratch::new();
        let mut out = Vec::new();
        let st = eng.match_title("psa 10 charizard slab", &mut s, &mut out, true);
        (out.len(), st.postings_scanned)
    };
    let (hits_before, scanned_before) = count_hit(&eng);
    assert_eq!(hits_before, 100);

    eng.compact_all().expect("compaction ran");
    let (hits_after, scanned_after) = count_hit(&eng);
    assert_eq!(hits_after, 100, "no member lost across the merge");
    assert!(
        scanned_after < scanned_before,
        "cross-segment regroup must collapse the two per-segment leaders \
         (before={scanned_before}, after={scanned_after})"
    );

    // Full differential after the merge for good measure.
    let mut corpus = batch_a;
    corpus.extend(batch_b);
    let brute = Brute::build(&corpus);
    let titles = vec![
        "psa 10 charizard slab".to_string(),
        "pikachu holo v3".to_string(),
        "blastoise ex n7 psa 10".to_string(),
    ];
    diff_vs_brute(&eng, &brute, &titles, "post-compaction regroup");
}

/// The structural pin of the measured defect, inverted: 1000 identical stored
/// queries are ONE posting entry — a hitting title scans O(1) entries and
/// still emits all 1000 matches. (At 20M the same concentration was a single
/// 43,533-entry posting scanned per title — the ADR-104 32×.)
#[test]
fn thousand_identical_queries_scan_one_posting_entry() {
    let mut eng = Engine::with_config(Normalizer::default_vocab().expect("vocab"), cfg_dedup(true));
    let corpus: Vec<(u64, String)> = (0..1_000u64).map(|i| (i, "psa 10".to_string())).collect();
    eng.build_from_queries(&corpus);
    assert_eq!(eng.snapshot().dup_joined(), 999);

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let stats = eng.match_title("psa 10 charizard", &mut s, &mut out, true);
    assert_eq!(out.len(), 1_000, "every member must still match");
    assert!(
        stats.postings_scanned <= 4,
        "1000 identical queries must cost ~one posting entry to scan, got {}",
        stats.postings_scanned
    );
}

/// Upsert interplay: replacing a MEMBER tombstones its copy without touching
/// the group; upserting it back to the same body re-joins as a fresh member.
#[test]
fn upsert_moves_members_between_groups() {
    let mut eng = Engine::with_config(Normalizer::default_vocab().expect("vocab"), cfg_dedup(true));
    for id in 1..=3u64 {
        eng.insert_live("psa 10 charizard", id, 1);
    }
    let hits = |eng: &Engine, title: &str| -> HashSet<u64> {
        let mut s = MatchScratch::new();
        let mut out = Vec::new();
        eng.match_title(title, &mut s, &mut out, true);
        out.iter().copied().collect()
    };
    // Move id 2 to a different body.
    eng.try_upsert_live("pikachu holo", 2, 2).expect("upsert");
    assert_eq!(hits(&eng, "psa 10 charizard slab"), HashSet::from([1, 3]));
    assert_eq!(hits(&eng, "pikachu holo promo"), HashSet::from([2]));
    // And back: it re-joins the original body's group.
    eng.try_upsert_live("psa 10 charizard", 2, 3)
        .expect("upsert back");
    assert_eq!(
        hits(&eng, "psa 10 charizard slab"),
        HashSet::from([1, 2, 3])
    );
    assert_eq!(hits(&eng, "pikachu holo promo"), HashSet::new());
}

/// Counter + sketch sanity on a corpus with EXACTLY known duplication: 400
/// distinct bodies × 4 copies in one segment.
#[test]
fn sketch_estimates_distinct_bodies() {
    let mut eng = Engine::with_config(Normalizer::default_vocab().expect("vocab"), cfg_dedup(true));
    let mut corpus = Vec::new();
    let mut id = 0u64;
    for b in 0..400u64 {
        for _ in 0..4 {
            corpus.push((id, format!("card{b} slab grade")));
            id += 1;
        }
    }
    eng.build_from_queries(&corpus);
    let snap = eng.snapshot();
    assert_eq!(snap.bodies_total(), 1_600);
    assert_eq!(snap.dup_joined(), 1_200, "3 of every 4 copies join a group");
    let est = snap.distinct_bodies_est();
    assert!(
        (380..=420).contains(&est),
        "linear-counting estimate should land near 400, got {est}"
    );
}
