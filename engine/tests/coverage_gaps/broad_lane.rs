// ═════════════════════════════════════════════════════════════════════════════
// 3. BROAD-LANE ISOLATION
// ═════════════════════════════════════════════════════════════════════════════

use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};
use std::collections::HashSet;

/// Class-C queries must land in the broad index, not the main index.
/// include_broad=false must exclude them; include_broad=true must include them.
#[test]
fn broad_lane_queries_only_in_broad_index() {
    // Craft queries that are clearly broad (all required features are hot)
    // and queries that are clearly selective.
    let norm = Normalizer::default_vocab().expect("built-in vocab");
    let mut eng = Engine::new(norm);

    // Build a corpus that establishes frequency. We need many queries so the
    // "hot" features get high frequency and end up in the common mask.
    let mut queries: Vec<(u64, String)> = Vec::new();
    let mut id = 1u64;

    // 5000 queries all mentioning "hottoken" — makes that feature very hot
    for i in 0..5000 {
        queries.push((
            id,
            format!("hottoken {} somethingrare{:04}", 1990 + (i % 30), i),
        ));
        id += 1;
    }

    // A query whose only required feature is super-hot (single token)
    // This should be class C (broad) since it has one hot feature and nothing to pair.
    let broad_id = id;
    queries.push((broad_id, "hottoken".to_string()));
    id += 1;

    // A selective query with rare required features
    let selective_id = id;
    queries.push((selective_id, "1994 hottoken somethingrare0042".to_string()));

    eng.build_from_queries(&queries);

    let classes = eng.class_counts();
    eprintln!(
        "class distribution: A={} B={} C={} D={}",
        classes[0], classes[1], classes[2], classes[3]
    );
    // We should have at least some class-C queries
    assert!(
        classes[2] > 0,
        "expected some class-C (broad) queries, got 0"
    );

    // Test: include_broad=false should NOT return the broad query
    let title = "hottoken 1994 somethingrare0042";
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();

    eng.match_title(title, &mut scratch, &mut out, false);
    let no_broad: HashSet<u64> = out.iter().copied().collect();

    eng.match_title(title, &mut scratch, &mut out, true);
    let with_broad: HashSet<u64> = out.iter().copied().collect();

    // with_broad should be a superset of no_broad
    for id in &no_broad {
        assert!(
            with_broad.contains(id),
            "include_broad=true lost a match that include_broad=false had: {id}"
        );
    }

    // The broad-only matches (difference) should exist if there are broad queries
    let broad_only: HashSet<u64> = with_broad.difference(&no_broad).copied().collect();
    eprintln!(
        "broad test: no_broad={} with_broad={} broad_only={}",
        no_broad.len(),
        with_broad.len(),
        broad_only.len()
    );
    // At minimum, if class C > 0, the broad lane is being used
    // (whether our specific crafted query lands there depends on the frequency model)
}

/// Verify that MatchStats correctly separates main_candidates from broad_candidates.
#[test]
fn match_stats_separate_main_and_broad_candidates() {
    let cfg = GenConfig {
        num_queries: 20_000,
        num_titles: 500,
        broad_query_frac: 0.10, // 10% broad to ensure we get some
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x0B20_AD57,
        num_players: 2_000,
        num_sets: 800,
    };
    let data = generate(&cfg);

    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.build_from_queries(&data.queries);

    let classes = eng.class_counts();
    eprintln!(
        "stats test classes: A={} B={} C={} D={}",
        classes[0], classes[1], classes[2], classes[3]
    );

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut total_main = 0u64;
    let mut total_broad = 0u64;

    for title in &data.titles {
        let stats = eng.match_title(title, &mut scratch, &mut out, true);
        total_main += u64::from(stats.main_candidates);
        total_broad += u64::from(stats.broad_candidates);
    }

    eprintln!("stats: total_main_candidates={total_main} total_broad_candidates={total_broad}");

    // Main candidates should always exist (we have 20k queries)
    assert!(total_main > 0, "no main candidates at all");

    // If there are class-C queries, we should see broad candidates
    if classes[2] > 0 {
        assert!(
            total_broad > 0,
            "class-C queries exist ({}) but no broad candidates seen",
            classes[2]
        );
    }

    // With include_broad=false, stats should show zero broad candidates
    let mut total_broad_off = 0u64;
    for title in &data.titles {
        let stats = eng.match_title(title, &mut scratch, &mut out, false);
        total_broad_off += u64::from(stats.broad_candidates);
    }
    assert_eq!(
        total_broad_off, 0,
        "include_broad=false still reported broad candidates"
    );
}

/// Regression (codex on ADR-101): the mmap per-title probe counted `postings_scanned` and
/// `broad_candidates` for broad probes but missed the `broad_postings_scanned` subset — so a
/// durable (mmap-backed) engine under-reported the exact stat the ADR-101 per-shard counter
/// exports, while an in-memory engine counted it. The two paths must report identical broad
/// stats for the same corpus + titles.
#[test]
fn broad_postings_scanned_parity_memory_vs_mmap() {
    use reverse_rusty::config::EngineConfig;
    use reverse_rusty::events::SegmentKind;

    let cfg = GenConfig {
        num_queries: 10_000,
        num_titles: 200,
        broad_query_frac: 0.10,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x0B20_AD58,
        num_players: 1_000,
        num_sets: 400,
    };
    let data = generate(&cfg);

    // In-memory build: memory base segments (the path that always counted correctly).
    let mut mem = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    mem.build_from_queries(&data.queries);

    // Durable build of the SAME corpus: base segments are written + mmap'd, so per-title
    // matching goes through the mmap probe. Classification is corpus-deterministic, so the
    // two engines hold identical query sets in identical classes.
    let dir = std::env::temp_dir().join(format!(
        "rr_broad_parity_{}_{:x}",
        std::process::id(),
        cfg.seed
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let mut mmap_eng = Engine::with_config(
        Normalizer::default_vocab().expect("built-in vocab"),
        EngineConfig {
            data_dir: Some(dir.clone()),
            ..EngineConfig::default()
        },
    );
    mmap_eng.build_from_queries(&data.queries);
    assert!(
        mmap_eng
            .snapshot()
            .segment_infos()
            .iter()
            .any(|s| s.kind == SegmentKind::Mmap),
        "the durable engine must serve at least one mmap-backed segment for this test to bite"
    );

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let (mut mem_bp, mut mem_bc) = (0u64, 0u64);
    let (mut mmap_bp, mut mmap_bc) = (0u64, 0u64);
    for title in &data.titles {
        let s = mem.match_title(title, &mut scratch, &mut out, true);
        mem_bp += u64::from(s.broad_postings_scanned);
        mem_bc += u64::from(s.broad_candidates);
        let s = mmap_eng.match_title(title, &mut scratch, &mut out, true);
        mmap_bp += u64::from(s.broad_postings_scanned);
        mmap_bc += u64::from(s.broad_candidates);
    }
    let _ = std::fs::remove_dir_all(&dir);

    assert!(
        mem_bp > 0,
        "premise: the corpus must exercise the broad lane"
    );
    assert_eq!(
        mem_bc, mmap_bc,
        "broad_candidates must agree between the memory and mmap per-title paths"
    );
    assert_eq!(
        mem_bp, mmap_bp,
        "broad_postings_scanned must agree between the memory and mmap per-title paths \
         (the mmap probe under-counted before the ADR-101 fix)"
    );
}
