//! The one soak test: durable K-shard build at ≥20M queries, fan-out bands,
//! the single-node differential (the zero-FN reference that scales), planted
//! sentinels, mirrored mutations, and a checkpoint → reopen re-verify.

use crate::harness::*;
use rayon::prelude::*;

/// Result-set compare for one title: cluster vs the single-node reference.
fn set_of(ids: Vec<u64>) -> HashSet<u64> {
    ids.into_iter().collect()
}

#[test]
#[ignore = "large-scale one-off proof (ADR-104) — run explicitly by name; in no gate or CI workflow"]
fn twenty_million_multi_shard_soak() {
    let cfg = SoakConfig::from_env();
    let t0 = Instant::now();
    eprintln!("\n=== CLUSTER SCALE SOAK (ADR-104) ===");
    eprintln!(
        "  queries={} titles={} shards={} dir={}",
        cfg.num_queries,
        cfg.num_titles,
        cfg.num_shards,
        cfg.data_dir.display()
    );
    let _ = std::fs::remove_dir_all(&cfg.data_dir); // stale run with the same pid

    // ── Phase 0: generate + plant sentinels ──
    let gen_start = Instant::now();
    let gen_cfg = GenConfig {
        num_queries: cfg.num_queries,
        num_titles: cfg.num_titles,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x2000_0000,
        // Pool sizes follow the clusterbench convention so the fan-out numbers
        // are comparable (in convention) to the pinned 100k invariants.
        num_players: (cfg.num_queries / 40).max(2_000),
        num_sets: (cfg.num_queries / 100).max(1_000),
    };
    let mut data = generate(&gen_cfg);
    let n_sent = cfg.num_sentinels();
    for i in 0..n_sent {
        data.queries
            .push((SENTINEL_ID_BASE + i as u64, sentinel_query(i)));
    }
    eprintln!(
        "  Phase 0: generated {} queries (+{} sentinels), {} titles in {:.1}s",
        cfg.num_queries,
        n_sent,
        data.titles.len(),
        gen_start.elapsed().as_secs_f64()
    );

    // ── Phase 1: durable multi-shard build ──
    let cluster_cfg = ClusterConfig {
        num_shards: cfg.num_shards,
        per_shard: EngineConfig {
            memtable_flush_threshold: 50_000,
            auto_compact_on_flush: true,
            max_segments: 8,
            hot_anchor_threshold: cfg.hot_theta,
            ..EngineConfig::default()
        },
        include_broad: true,
        data_dir: Some(cfg.data_dir.clone()),
        ..ClusterConfig::default()
    };
    let build_start = Instant::now();
    let cluster =
        ClusterEngine::build(make_norm(), &cluster_cfg, &data.queries).expect("cluster build");
    let build_secs = build_start.elapsed().as_secs_f64();
    let shard_counts = cluster.shard_query_counts().expect("shard counts");
    let classes = cluster.class_counts().expect("class counts");
    let stored = cluster.num_queries().expect("num_queries");
    eprintln!(
        "  Phase 1: built durable K={} cluster in {:.1}s — {} stored, classes A/B/C/D/H = {:?} (θ={})",
        cfg.num_shards, build_secs, stored, classes, cfg.hot_theta
    );
    eprintln!(
        "    per-shard counts: min={} max={} {:?}",
        shard_counts.iter().min().unwrap(),
        shard_counts.iter().max().unwrap(),
        shard_counts
    );
    assert_eq!(classes[3], 0, "class D in generated corpus");
    // θ-conditional band (ADR-105): with the hot tier on, the 20M corpus's
    // θ-hot anchors must actually classify H (volume captured, not banded —
    // the ADR-104 lesson); θ=0 must store none.
    if cfg.hot_theta > 0 {
        assert!(classes[4] > 0, "θ={} stored no class H", cfg.hot_theta);
    } else {
        assert_eq!(classes[4], 0, "class H must be empty with θ off");
    }
    let (min_c, max_c) = (
        *shard_counts.iter().min().unwrap(),
        *shard_counts.iter().max().unwrap(),
    );
    assert!(
        max_c <= 2 * min_c,
        "shard imbalance: max {max_c} > 2x min {min_c}"
    );

    // ── Phase 2: fan-out + candidate structure over every title ──
    let fan_start = Instant::now();
    let per_title: Vec<(u32, u64, u64, u64)> = data
        .titles
        .par_iter()
        .map(|t| {
            let fan = cluster.shard_fanout(t).len() as u32;
            let (_ids, s) = cluster.percolate_with_stats(t).expect("stats percolate");
            // MatchStats fields are u32 — widen before the cross-title sums.
            (
                fan,
                u64::from(s.unique_candidates),
                u64::from(s.main_candidates),
                u64::from(s.broad_candidates),
            )
        })
        .collect();
    let mut fans: Vec<u32> = per_title.iter().map(|x| x.0).collect();
    fans.sort_unstable();
    let n_titles = data.titles.len() as f64;
    let avg_fan = fans.iter().map(|&f| u64::from(f)).sum::<u64>() as f64 / n_titles;
    let (p50_f, p95_f, p99_f, max_f) = (
        pct(&fans, 0.50),
        pct(&fans, 0.95),
        pct(&fans, 0.99),
        *fans.last().unwrap(),
    );
    let avg_unique = per_title.iter().map(|x| x.1).sum::<u64>() as f64 / n_titles;
    let avg_main = per_title.iter().map(|x| x.2).sum::<u64>() as f64 / n_titles;
    let avg_broad = per_title.iter().map(|x| x.3).sum::<u64>() as f64 / n_titles;
    eprintln!(
        "  Phase 2: fan-out over {} titles in {:.1}s",
        data.titles.len(),
        fan_start.elapsed().as_secs_f64()
    );
    eprintln!(
        "    fan-out: avg {avg_fan:.2}  p50 {p50_f}  p95 {p95_f}  p99 {p99_f}  max {max_f}  (of {})",
        cfg.num_shards
    );
    eprintln!(
        "    candidates/title: unique {avg_unique:.2} (selective {avg_main:.2} + broad {avg_broad:.2}, broad share {:.1}%)",
        if avg_unique > 0.0 { 100.0 * avg_broad / avg_unique } else { 0.0 }
    );
    // Bands, not pins: SCALE-INVARIANT structural claims only. Candidate
    // volume is deliberately captured, not banded — with the broad lane ON it
    // grows with corpus size by design (the documented lineage: 85.64 @100k →
    // 682 @1M → this run's capture; that growth is exactly what the ADR-026
    // columnar batch lane amortizes). The broad-OFF selective flatness pin
    // (~54, corpus-size-independent) lives with `bench <Q> <T> 0.0` in
    // docs/performance/benchmark-results.txt. A loose pathological ceiling
    // still trips on a genuine cover regression (candidates heading toward Q).
    assert!(
        (1.0..=5.0).contains(&avg_fan),
        "fan-out avg {avg_fan:.2} outside [1,5]"
    );
    assert!(
        (p99_f as usize) < cfg.num_shards,
        "fan-out p99 {p99_f} reached K={}",
        cfg.num_shards
    );
    assert!(
        avg_unique <= (cfg.num_queries as f64 / 1_000.0).max(1_000.0),
        "candidates/title {avg_unique:.2} above the pathological ceiling (Q/1000)"
    );
    drop(per_title);
    drop(fans);

    // ── Phase 3: single-node reference engine (the zero-FN reference that
    // scales — proven ≡ brute + ≡ the ADR-087 independent matcher elsewhere;
    // it runs NONE of the cluster code) ──
    let ref_start = Instant::now();
    let mut reference = Engine::with_config(make_norm(), EngineConfig::default());
    reference.build_from_queries(&data.queries);
    eprintln!(
        "  Phase 3: built single-node reference in {:.1}s",
        ref_start.elapsed().as_secs_f64()
    );

    // ── Phase 4: differential #1 — cluster ≡ reference over every title ──
    let diff_start = Instant::now();
    let ref_sets = reference_sets(&reference, &data.titles);
    let total_ref_matches: usize = ref_sets.iter().map(HashSet::len).sum();
    let mismatch_idx = differential(&cluster, &data.titles, &ref_sets);
    eprintln!(
        "  Phase 4: differential #1 over {} titles in {:.1}s — {} mismatches, {} reference matches",
        data.titles.len(),
        diff_start.elapsed().as_secs_f64(),
        mismatch_idx.len(),
        total_ref_matches
    );
    report_mismatches(&cluster, &data.titles, &ref_sets, &mismatch_idx);
    assert!(mismatch_idx.is_empty(), "cluster != single-node at scale");
    assert!(total_ref_matches > 0, "no matches at all");
    drop(ref_sets);

    // ── Phase 5: sentinel containment (the absolute FN check) ──
    let sent_misses = sentinel_misses(&cluster, n_sent);
    eprintln!("  Phase 5: sentinel misses: {sent_misses}/{n_sent}");
    assert_eq!(sent_misses, 0, "sentinel query not retrieved by its title");

    // ── Phase 6: live mutations through the cluster, mirrored on the reference ──
    let mut_start = Instant::now();
    let (n_adds, n_upserts, n_deletes) = (cfg.num_adds(), cfg.num_upserts(), cfg.num_deletes());
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();

    for i in 0..n_adds {
        let id = LIVE_ADD_ID_BASE + i as u64;
        let dsl = live_add_query(i);
        let outcome = cluster.add_query(id, &dsl).expect("live add");
        assert!(
            matches!(outcome, AddOutcome::Placed { .. } | AddOutcome::Replicated),
            "live add {i} not stored: {outcome:?}"
        );
        assert!(
            reference.insert_live(&dsl, id, 1).is_some(),
            "reference insert_live {i} rejected"
        );
    }
    let upsert_range = &data.queries[n_deletes..n_deletes + n_upserts];
    for (i, (id, text)) in upsert_range.iter().enumerate() {
        let new_text = format!("{text} upsertvariantxq");
        let (_removed, outcome) = cluster
            .upsert_query(*id, &new_text, 99)
            .expect("live upsert");
        assert!(
            matches!(outcome, AddOutcome::Placed { .. } | AddOutcome::Replicated),
            "upsert {i} new version not stored: {outcome:?}"
        );
        let _ = reference.delete_by_logical_id(*id);
        reference.insert_live(&new_text, *id, 99);
    }
    let deleted_ids: HashSet<u64> = data.queries[..n_deletes]
        .iter()
        .map(|(id, _)| *id)
        .collect();
    let mut removed_total = 0usize;
    for id in &deleted_ids {
        removed_total += cluster.remove_query(*id).expect("live remove");
        let _ = reference.delete_by_logical_id(*id);
    }
    eprintln!(
        "  Phase 6: {} adds + {} upserts + {} removes (removed from {} shard slots) in {:.1}s",
        n_adds,
        n_upserts,
        n_deletes,
        removed_total,
        mut_start.elapsed().as_secs_f64()
    );
    assert!(
        removed_total >= n_deletes,
        "removes hit fewer slots than ids"
    );

    // Live-add retrievability: the frozen-dict synthetic-ID path (ADR-046) at
    // scale — each sampled add must be retrievable via a title carrying its
    // token, identically on both engines.
    let sample = n_adds.clamp(1, 1_000);
    let step = (n_adds / sample).max(1);
    let mut add_failures = 0usize;
    for i in (0..n_adds).step_by(step) {
        let id = LIVE_ADD_ID_BASE + i as u64;
        let title = live_add_title(i);
        let got = set_of(cluster.percolate(&title).expect("percolate live-add"));
        reference.match_title(&title, &mut scratch, &mut out, true);
        let want: HashSet<u64> = out.iter().copied().collect();
        if !got.contains(&id) || got != want {
            add_failures += 1;
        }
    }
    eprintln!("    live-add retrievability: {add_failures} failures over ~{sample} sampled");
    assert_eq!(add_failures, 0, "live add not retrievable / diverged");

    // The query corpus is no longer needed (mutation slices are materialized);
    // free ~GBs before the second differential.
    data.queries = Vec::new();

    // ── Phase 7: differential #2 + ghosts + sentinels, post-mutation ──
    let diff2_start = Instant::now();
    let ref_sets2 = reference_sets(&reference, &data.titles);
    let mismatch2 = differential(&cluster, &data.titles, &ref_sets2);
    let ghosts: usize = ref_sets2
        .par_iter()
        .map(|s| s.intersection(&deleted_ids).count())
        .sum::<usize>()
        + data
            .titles
            .par_iter()
            .map(|t| {
                set_of(cluster.percolate(t).expect("ghost percolate"))
                    .intersection(&deleted_ids)
                    .count()
            })
            .sum::<usize>();
    let sent_misses2 = sentinel_misses(&cluster, n_sent);
    eprintln!(
        "  Phase 7: differential #2 in {:.1}s — {} mismatches, {} ghosts, {} sentinel misses",
        diff2_start.elapsed().as_secs_f64(),
        mismatch2.len(),
        ghosts,
        sent_misses2
    );
    report_mismatches(&cluster, &data.titles, &ref_sets2, &mismatch2);
    assert!(mismatch2.is_empty(), "cluster != single-node post-mutation");
    assert_eq!(ghosts, 0, "deleted ids in results post-mutation");
    assert_eq!(sent_misses2, 0, "sentinel lost after mutations");
    drop(ref_sets2);
    drop(reference); // free the reference before the durability leg

    // ── Phase 8: record → flush → checkpoint → drop ──
    let subset_n = data.titles.len().min(2_000);
    let record = |c: &ClusterEngine| -> (Vec<Vec<u64>>, Vec<Vec<u64>>) {
        let subset: Vec<Vec<u64>> = data.titles[..subset_n]
            .iter()
            .map(|t| {
                let mut ids = c.percolate(t).expect("record percolate");
                ids.sort_unstable();
                ids
            })
            .collect();
        let sentinels: Vec<Vec<u64>> = (0..n_sent)
            .map(|i| {
                let mut ids = c.percolate(&sentinel_title(i)).expect("record sentinel");
                ids.sort_unstable();
                ids
            })
            .collect();
        (subset, sentinels)
    };
    let dur_start = Instant::now();
    let (pre_subset, pre_sentinels) = record(&cluster);
    cluster.flush().expect("flush");
    cluster.checkpoint().expect("checkpoint");
    let disk = dir_size_bytes(&cfg.data_dir);
    eprintln!(
        "  Phase 8: flush + checkpoint in {:.1}s — data_dir {}",
        dur_start.elapsed().as_secs_f64(),
        fmt_mb(disk)
    );
    drop(cluster);

    // ── Phase 9: reopen from disk, re-verify ──
    let reopen_start = Instant::now();
    let reopened = ClusterEngine::open(&cfg.data_dir, make_norm(), Some(&cluster_cfg))
        .expect("reopen durable cluster");
    let reopen_secs = reopen_start.elapsed().as_secs_f64();
    let (post_subset, post_sentinels) = record(&reopened);
    let subset_diffs = pre_subset
        .iter()
        .zip(&post_subset)
        .filter(|(a, b)| a != b)
        .count();
    let sentinel_diffs = pre_sentinels
        .iter()
        .zip(&post_sentinels)
        .filter(|(a, b)| a != b)
        .count();
    let reopened_ghosts: usize = post_subset
        .iter()
        .flatten()
        .filter(|id| deleted_ids.contains(id))
        .count();
    let sent_misses3 = sentinel_misses(&reopened, n_sent);
    eprintln!(
        "  Phase 9: reopened {} stored in {:.1}s — {} subset diffs, {} sentinel diffs, {} ghosts, {} sentinel misses",
        reopened.num_queries().expect("reopened count"),
        reopen_secs,
        subset_diffs,
        sentinel_diffs,
        reopened_ghosts,
        sent_misses3
    );
    assert_eq!(subset_diffs, 0, "reopened cluster diverged on title subset");
    assert_eq!(sentinel_diffs, 0, "reopened cluster diverged on sentinels");
    assert_eq!(reopened_ghosts, 0, "deleted ids resurrected by reopen");
    assert_eq!(sent_misses3, 0, "sentinel lost across reopen");
    drop(reopened);

    // ── Phase 10: summary + cleanup ──
    eprintln!("\n  ══════════════════════════════════════");
    eprintln!("  CLUSTER SCALE SOAK SUMMARY (ADR-104)");
    eprintln!("  ──────────────────────────────────────");
    eprintln!(
        "  corpus:        {} queries (+{} sentinels), {} titles, K={}",
        cfg.num_queries, n_sent, cfg.num_titles, cfg.num_shards
    );
    eprintln!(
        "  build:         {build_secs:.1}s durable ({})",
        fmt_mb(disk)
    );
    eprintln!(
        "  fan-out:       avg {avg_fan:.2}  p50 {p50_f}  p95 {p95_f}  p99 {p99_f}  max {max_f}"
    );
    eprintln!(
        "  cand/title:    unique {avg_unique:.2} (selective {avg_main:.2} + broad {avg_broad:.2})"
    );
    eprintln!("  differential:  0 mismatches pre-mutation, 0 post-mutation");
    eprintln!("  sentinels:     0 misses (pre / post-mutation / reopened)");
    eprintln!(
        "  mutations:     {n_adds} adds + {n_upserts} upserts + {n_deletes} removes, 0 ghosts"
    );
    eprintln!(
        "  reopen:        identical on {subset_n}-title subset + sentinels, {reopen_secs:.1}s"
    );
    eprintln!("  total elapsed: {:.1}s", t0.elapsed().as_secs_f64());
    eprintln!("  ══════════════════════════════════════");
    let _ = std::fs::remove_dir_all(&cfg.data_dir);
}

/// Full per-title match sets from the single-node reference (index-aligned
/// with `titles` — `match_titles_par` tuples carry the title index).
fn reference_sets(reference: &Engine, titles: &[String]) -> Vec<HashSet<u64>> {
    let mut sets: Vec<HashSet<u64>> = vec![HashSet::new(); titles.len()];
    for (idx, ids, _) in reference.match_titles_par(titles, true) {
        sets[idx] = ids.into_iter().collect();
    }
    sets
}

/// Indices of titles whose cluster match set differs from the reference set.
fn differential(
    cluster: &ClusterEngine,
    titles: &[String],
    ref_sets: &[HashSet<u64>],
) -> Vec<usize> {
    titles
        .par_iter()
        .enumerate()
        .filter_map(|(i, t)| {
            let got = set_of(cluster.percolate(t).expect("differential percolate"));
            (got != ref_sets[i]).then_some(i)
        })
        .collect()
}

/// Print the first few mismatching titles with their symmetric differences —
/// diagnostics only, the caller asserts emptiness.
fn report_mismatches(
    cluster: &ClusterEngine,
    titles: &[String],
    ref_sets: &[HashSet<u64>],
    mismatch_idx: &[usize],
) {
    for &i in mismatch_idx.iter().take(5) {
        let got = set_of(cluster.percolate(&titles[i]).expect("report percolate"));
        let missing: Vec<u64> = ref_sets[i].difference(&got).take(8).copied().collect();
        let extra: Vec<u64> = got.difference(&ref_sets[i]).take(8).copied().collect();
        eprintln!(
            "    MISMATCH title[{i}] {:?}: cluster missing {missing:?} extra {extra:?}",
            &titles[i]
        );
    }
}

/// Count sentinel titles whose cluster result does NOT contain the planted id
/// (containment, not equality: extra true matches are irrelevant — this is the
/// zero-FN direction).
fn sentinel_misses(cluster: &ClusterEngine, n_sent: usize) -> usize {
    (0..n_sent)
        .filter(|&i| {
            let got = cluster
                .percolate(&sentinel_title(i))
                .expect("sentinel percolate");
            !got.contains(&(SENTINEL_ID_BASE + i as u64))
        })
        .count()
}
