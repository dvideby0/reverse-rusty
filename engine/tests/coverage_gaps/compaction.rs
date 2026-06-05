// ═════════════════════════════════════════════════════════════════════════════
// 2. REPEATED / INTERLEAVED COMPACTION
// ═════════════════════════════════════════════════════════════════════════════

use crate::harness::*;
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};
use std::collections::HashSet;

/// Build multiple segments, compact some, add more, compact again, verify the
/// final engine matches the brute-force oracle. Stresses the remap logic under
/// repeated merge cycles.
#[test]
fn repeated_compaction_preserves_correctness() {
    let cfg = GenConfig {
        num_queries: 30_000,
        num_titles: 2_000,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x00C0_AC72,
        num_players: 2_500,
        num_sets: 1_000,
    };
    let data = generate(&cfg);
    let q = &data.queries;
    let n = q.len();
    let chunk = n / 6;

    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));

    // Phase 1: build 3 segments
    eng.build_from_queries(&q[..chunk]);
    eng.bulk_ingest(&q[chunk..2 * chunk]);
    eng.bulk_ingest(&q[2 * chunk..3 * chunk]);
    assert!(eng.num_segments() >= 4); // 3 base + memtable

    // Compact all -> 1 base + memtable
    let r1 = eng.compact_all();
    assert!(r1.is_some());
    assert_eq!(eng.num_segments(), 2);

    // Phase 2: add 2 more segments on top of the compacted base
    eng.bulk_ingest(&q[3 * chunk..4 * chunk]);
    eng.bulk_ingest(&q[4 * chunk..5 * chunk]);
    assert!(eng.num_segments() >= 4);

    // Compact range: merge only the 2 newest base segments
    let _ = eng.compact_range(1, 3);

    // Phase 3: add a final segment + live inserts
    eng.bulk_ingest(&q[5 * chunk..]);
    for i in (0..100).step_by(7) {
        let _ = eng.insert_live(&format!("{} extra variant", q[i].1), q[i].0 + 1_000_000, 2);
    }
    eng.flush();

    // Final compact_all
    let r_final = eng.compact_all();
    assert!(r_final.is_some());

    // Verify against oracle over the full query set (including the extra variants)
    let mut all_queries: Vec<(u64, String)> = q.clone();
    for i in (0..100).step_by(7) {
        all_queries.push((q[i].0 + 1_000_000, format!("{} extra variant", q[i].1)));
    }
    let brute = Brute::build(&all_queries);

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    let mut false_neg = 0usize;
    let mut total_truth = 0usize;

    for title in &data.titles {
        eng.match_title(title, &mut scratch, &mut out, true);
        let eng_set: HashSet<u64> = out.iter().copied().collect();
        let truth = brute.matches(title, &mut blc, &mut bfeats);
        total_truth += truth.len();
        for t in &truth {
            if !eng_set.contains(t) {
                false_neg += 1;
            }
        }
    }

    eprintln!(
        "repeated-compaction: truth={} false_neg={} final_segments={}",
        total_truth,
        false_neg,
        eng.num_segments()
    );
    assert_eq!(
        false_neg, 0,
        "repeated compaction introduced FALSE NEGATIVES"
    );
    assert!(total_truth > 0, "degenerate test: no matches");
}

/// Interleave insert_live + tombstone + compact in a tight loop. Verifies that
/// the engine doesn't corrupt state under rapid mutation-compaction cycling.
#[test]
fn compaction_under_churn() {
    let cfg = GenConfig {
        num_queries: 10_000,
        num_titles: 500,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x000C_4020,
        num_players: 1_500,
        num_sets: 600,
    };
    let data = generate(&cfg);

    let mut eng = Engine::new(Normalizer::default_vocab().expect("built-in vocab"));
    eng.build_from_queries(&data.queries);

    // Perform 5 rounds of: add live queries, flush, compact
    let mut extra_id = 2_000_000u64;
    for round in 0..5 {
        // add 50 queries
        for i in 0..50 {
            let text = format!("round {round} item {i} michael jordan upper deck 1994");
            let _ = eng.insert_live(&text, extra_id, (round + 2) as u32);
            extra_id += 1;
        }
        eng.flush();

        // compact
        let _ = eng.compact_all();
    }

    // Verify: engine still runs without panic, matches are non-empty
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut total_matches = 0usize;
    for title in &data.titles {
        eng.match_title(title, &mut scratch, &mut out, true);
        total_matches += out.len();
    }
    eprintln!(
        "churn test: total_matches={} segments={}",
        total_matches,
        eng.num_segments()
    );
    assert!(total_matches > 0, "churn destroyed all matches");
    // Should have compacted down to few segments
    assert!(
        eng.num_segments() <= 3,
        "expected <=3 segments after compact_all, got {}",
        eng.num_segments()
    );
}
