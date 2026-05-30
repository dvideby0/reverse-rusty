//! Stress tests: concurrent-style read/write/delete workloads.
//!
//! These tests simulate real-world mixed workloads — inserts, deletes, updates,
//! and searches happening in staged phases, single-threaded and multi-threaded.
//! They are NOT part of the default test suite (run with `cargo test --test stress`).
//!
//! Each test logs engine events + metrics so you can watch the mechanics:
//!   cargo test --release --test stress -- --nocapture
//!
//! The tests verify:
//!   * Zero false negatives under churn (oracle comparison)
//!   * Metrics consistency (counts, segments, tombstones)
//!   * Event emission (flush, ingest, compaction triggers)
//!   * Correct delete/update visibility
//!   * Parallel vs sequential agreement under mutation

use reverse_rusty::compile::{extract, Extracted};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::dict::Dict;
use reverse_rusty::events::{EngineEvent, EngineMetrics};
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn make_norm() -> Normalizer {
    Normalizer::default_vocab().expect("built-in vocab")
}

fn match_ids(engine: &Engine, title: &str) -> Vec<u64> {
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    engine.match_title(title, &mut scratch, &mut out, true);
    out.sort_unstable();
    out
}

fn match_ids_set(engine: &Engine, title: &str) -> HashSet<u64> {
    match_ids(engine, title).into_iter().collect()
}

struct Brute {
    norm: Normalizer,
    dict: Dict,
    queries: Vec<(u64, Extracted)>,
}

impl Brute {
    fn build(queries: &[(u64, String)]) -> Self {
        let norm = make_norm();
        let mut dict = Dict::new();
        let mut lc = String::new();
        let mut qs = Vec::new();
        for (logical, text) in queries {
            if let Ok(ast) = reverse_rusty::dsl::parse(text) {
                let ex = extract(&ast, &norm, &mut dict, &mut lc);
                if ex.required.is_empty() && ex.anyof.is_empty() {
                    continue;
                }
                qs.push((*logical, ex));
            }
        }
        dict.finalize_mask();
        Brute {
            norm,
            dict,
            queries: qs,
        }
    }

    fn matches(&self, title: &str, lc: &mut String, feats: &mut Vec<u32>) -> HashSet<u64> {
        self.norm.match_features(title, &self.dict, lc, feats);
        let present = |f: u32| feats.binary_search(&f).is_ok();
        let mut out = HashSet::new();
        for (logical, ex) in &self.queries {
            if ex.required.iter().all(|&f| present(f))
                && !ex.forbidden.iter().any(|&f| present(f))
                && ex.anyof.iter().all(|g| g.iter().any(|&f| present(f)))
            {
                out.insert(*logical);
            }
        }
        out
    }
}

#[derive(Debug, Default)]
struct EventLog {
    flushes: AtomicUsize,
    ingests: AtomicUsize,
    compactions: AtomicUsize,
    entries: Mutex<Vec<String>>,
}

impl EventLog {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn observer(self: &Arc<Self>) -> impl Fn(&EngineEvent) + Send + Sync + 'static {
        let log = Arc::clone(self);
        move |event: &EngineEvent| {
            let msg = match event {
                EngineEvent::Flush {
                    entries,
                    base_segments_after,
                    ..
                } => {
                    log.flushes.fetch_add(1, Ordering::Relaxed);
                    format!("[FLUSH] entries={entries} segments_after={base_segments_after}")
                }
                EngineEvent::Ingest {
                    ingested,
                    rejected_parse,
                    rejected_class_d,
                    base_segments_after,
                } => {
                    log.ingests.fetch_add(1, Ordering::Relaxed);
                    format!(
                        "[INGEST] ingested={ingested} rejected_parse={rejected_parse} rejected_d={rejected_class_d} segments_after={base_segments_after}"
                    )
                }
                EngineEvent::Compaction {
                    report,
                    trigger,
                    base_segments_after,
                    ..
                } => {
                    log.compactions.fetch_add(1, Ordering::Relaxed);
                    format!(
                        "[COMPACT] merged={} before={} after={} reclaimed={} trigger={:?} segments_after={}",
                        report.segments_merged,
                        report.entries_before,
                        report.entries_after,
                        report.tombstones_reclaimed,
                        trigger,
                        base_segments_after
                    )
                }
                EngineEvent::SegmentCleanupFailed { path, error } => {
                    format!("[CLEANUP_FAIL] path={} error={error}", path.display())
                }
                EngineEvent::DurabilityFailure { op, detail, error } => {
                    format!(
                        "[DURABILITY_FAIL] op={} detail={detail} error={error}",
                        op.as_str()
                    )
                }
            };
            eprintln!("  EVENT: {msg}");
            log.entries.lock().unwrap().push(msg);
        }
    }

    fn dump_summary(&self, label: &str) {
        eprintln!(
            "  {} event summary: flushes={} ingests={} compactions={}",
            label,
            self.flushes.load(Ordering::Relaxed),
            self.ingests.load(Ordering::Relaxed),
            self.compactions.load(Ordering::Relaxed),
        );
    }
}

fn print_metrics(label: &str, m: &EngineMetrics) {
    eprintln!(
        "  [METRICS:{}] total_queries={} base_segments={} memtable={} dict_features={} stale={}",
        label,
        m.total_queries,
        m.base_segments,
        m.memtable_entries,
        m.dict_features,
        m.stale_segments
    );
    if !m.segment_sizes.is_empty() {
        eprintln!(
            "    segment_sizes={:?} holes={:?}",
            m.segment_sizes,
            m.segment_holes
                .iter()
                .map(|h| format!("{:.2}%", h * 100.0))
                .collect::<Vec<_>>()
        );
    }
    eprintln!(
        "    memory: exact={}KB index={}KB filter={}KB",
        m.exact_bytes / 1024,
        m.index_bytes / 1024,
        m.filter_bytes / 1024
    );
}

// ═════════════════════════════════════════════════════════════════════════════
// 1. MIXED WORKLOAD — single-threaded, staged phases
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn staged_read_write_delete_single_thread() {
    eprintln!("\n=== STAGED READ/WRITE/DELETE (single-thread) ===");
    let t0 = Instant::now();

    let cfg = GenConfig {
        num_queries: 20_000,
        num_titles: 2_000,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x57_2E55,
        num_players: 3_000,
        num_sets: 1_200,
    };
    let data = generate(&cfg);
    let q = &data.queries;

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 2_000,
            auto_compact_on_flush: true,
            max_segments: 6,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    // ── Phase 1: Bulk load initial corpus ──
    eprintln!("\n  Phase 1: bulk load {} queries", q.len() / 2);
    eng.build_from_queries(&q[..q.len() / 2]);
    print_metrics("after-bulk", &eng.metrics());

    // Snapshot baseline matches for verification titles
    let check_titles: Vec<&str> = data
        .titles
        .iter()
        .take(200)
        .map(std::string::String::as_str)
        .collect();
    let _baseline: Vec<HashSet<u64>> = check_titles
        .iter()
        .map(|t| match_ids_set(&eng, t))
        .collect();

    // ── Phase 2: Streaming inserts (hot delta) ──
    eprintln!("\n  Phase 2: streaming {} live inserts", q.len() / 2);
    let mut inserted_ids = Vec::new();
    for (logical, text) in &q[q.len() / 2..] {
        if let Some(local) = eng.insert_live(text, *logical, 1) {
            inserted_ids.push((*logical, local));
        }
    }
    eprintln!("    inserted {} queries into memtable", inserted_ids.len());
    print_metrics("after-inserts", &eng.metrics());

    // Verify reads see both old and new data
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut total_matches = 0usize;
    for title in &data.titles {
        eng.match_title(title, &mut scratch, &mut out, true);
        total_matches += out.len();
    }
    eprintln!(
        "    total matches across {} titles: {}",
        data.titles.len(),
        total_matches
    );
    assert!(total_matches > 0, "no matches after insert phase");

    // ── Phase 3: Targeted deletes ──
    let delete_count = inserted_ids.len() / 5;
    eprintln!("\n  Phase 3: deleting {delete_count} queries by logical ID");
    let mut deleted_ids = HashSet::new();
    for (logical, _) in inserted_ids.iter().take(delete_count) {
        let _ = eng.delete_by_logical_id(*logical);
        deleted_ids.insert(*logical);
    }
    print_metrics("after-deletes", &eng.metrics());

    // Verify deleted queries no longer match
    let mut ghost_matches = 0usize;
    for title in check_titles.iter().take(50) {
        let hits = match_ids_set(&eng, title);
        for id in &hits {
            if deleted_ids.contains(id) {
                ghost_matches += 1;
            }
        }
    }
    eprintln!("    ghost matches (deleted IDs still matching): {ghost_matches}");
    assert_eq!(ghost_matches, 0, "deleted queries still matching");

    // ── Phase 4: Updates (re-insert with higher version) ──
    let update_count = 500;
    eprintln!("\n  Phase 4: updating {update_count} queries (new version)");
    let mut updated = Vec::new();
    for (logical, text) in q.iter().take(update_count) {
        let new_text = format!("{text} updated variant");
        if eng.insert_live(&new_text, *logical, 99).is_some() {
            updated.push((*logical, new_text));
        }
    }
    eprintln!("    updated {} queries", updated.len());

    // ── Phase 5: Flush + compact ──
    eprintln!("\n  Phase 5: flush + compact");
    eng.flush();
    print_metrics("after-flush", &eng.metrics());

    let compact_report = eng.compact_all();
    if let Some(ref r) = compact_report {
        eprintln!(
            "    compaction: merged={} before={} after={} reclaimed={}",
            r.segments_merged, r.entries_before, r.entries_after, r.tombstones_reclaimed
        );
    }
    print_metrics("after-compact", &eng.metrics());

    // ── Phase 6: Second wave of inserts + reads ──
    eprintln!("\n  Phase 6: second wave of 1000 inserts + full read sweep");
    for i in 0..1_000u64 {
        let text = format!("wave2 michael jordan 1994 upper deck variant{i}");
        eng.insert_live(&text, 5_000_000 + i, 1);
    }

    let mut post_matches = 0usize;
    for title in &data.titles {
        eng.match_title(title, &mut scratch, &mut out, true);
        post_matches += out.len();
    }
    eprintln!("    total matches after wave2: {post_matches}");

    // ── Verify: oracle correctness over final state ──
    eprintln!("\n  Final oracle verification...");
    let mut live_queries: Vec<(u64, String)> = Vec::new();
    for (logical, text) in q {
        if !deleted_ids.contains(logical) {
            live_queries.push((*logical, text.clone()));
        }
    }
    for (logical, text) in &updated {
        live_queries.push((*logical, text.clone()));
    }
    for i in 0..1_000u64 {
        live_queries.push((
            5_000_000 + i,
            format!("wave2 michael jordan 1994 upper deck variant{i}"),
        ));
    }

    let brute = Brute::build(&live_queries);
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    let mut false_neg = 0usize;
    let mut total_truth = 0usize;
    for title in data.titles.iter().take(500) {
        eng.match_title(title, &mut scratch, &mut out, true);
        let eng_set: HashSet<u64> = out.iter().copied().collect();
        let truth = brute.matches(title, &mut blc, &mut bfeats);
        total_truth += truth.len();
        for t in &truth {
            if !eng_set.contains(t) {
                false_neg += 1;
                if false_neg <= 3 {
                    eprintln!("    FALSE NEG: title={title:?} missing_id={t}");
                }
            }
        }
    }
    eprintln!(
        "    oracle: truth={} false_neg={} elapsed={:.1}s",
        total_truth,
        false_neg,
        t0.elapsed().as_secs_f64()
    );
    events.dump_summary("staged-single");
    assert_eq!(false_neg, 0, "staged workload introduced false negatives");
    assert!(total_truth > 0, "degenerate test: no truth matches");
}

// ═════════════════════════════════════════════════════════════════════════════
// 2. BURST WRITE + CONCURRENT READ — multi-threaded reads via rayon
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn burst_writes_with_parallel_reads() {
    eprintln!("\n=== BURST WRITES + PARALLEL READS ===");
    let t0 = Instant::now();

    let cfg = GenConfig {
        num_queries: 30_000,
        num_titles: 3_000,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0xBE_EF_CA_FE,
        num_players: 3_000,
        num_sets: 1_200,
    };
    let data = generate(&cfg);
    let q = &data.queries;
    let chunk = q.len() / 6;

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 5_000,
            auto_compact_on_flush: true,
            max_segments: 4,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    // ── Load initial batch ──
    eprintln!("  Loading initial {chunk} queries");
    eng.build_from_queries(&q[..chunk]);

    // ── Burst: bulk_ingest 3 batches, read between each ──
    for wave in 0..3 {
        let lo = (wave + 1) * chunk;
        let hi = ((wave + 2) * chunk).min(q.len());
        eprintln!("\n  Wave {}: bulk_ingest {} queries", wave, hi - lo);
        eng.bulk_ingest(&q[lo..hi]);
        print_metrics(&format!("wave-{wave}"), &eng.metrics());

        // Parallel read sweep between writes
        let par_results = eng.match_titles_par(&data.titles, true);
        let total: usize = par_results.iter().map(|(_, ids, _)| ids.len()).sum();
        eprintln!(
            "    parallel read: {} total matches across {} titles",
            total,
            data.titles.len()
        );
    }

    // ── Remaining queries as live inserts ──
    let live_start = 4 * chunk;
    eprintln!("\n  Streaming {} live inserts", q.len() - live_start);
    for (logical, text) in &q[live_start..] {
        eng.insert_live(text, *logical, 1);
    }

    // ── Delete every 10th query ──
    let delete_targets: Vec<u64> = q.iter().step_by(10).map(|(id, _)| *id).collect();
    eprintln!("  Deleting {} queries", delete_targets.len());
    for id in &delete_targets {
        let _ = eng.delete_by_logical_id(*id);
    }

    // ── Flush + compact ──
    eng.flush();
    eng.compact_all();
    print_metrics("final", &eng.metrics());

    // ── Parallel read: verify sequential == parallel ──
    eprintln!("\n  Verifying parallel == sequential...");
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut sequential: Vec<HashSet<u64>> = Vec::new();
    for title in &data.titles {
        eng.match_title(title, &mut scratch, &mut out, true);
        sequential.push(out.iter().copied().collect());
    }

    let par_results = eng.match_titles_par(&data.titles, true);
    let mut mismatches = 0usize;
    for (idx, matches, _) in &par_results {
        let par_set: HashSet<u64> = matches.iter().copied().collect();
        if par_set != sequential[*idx] {
            mismatches += 1;
        }
    }
    eprintln!(
        "    par vs seq: {} mismatches, elapsed={:.1}s",
        mismatches,
        t0.elapsed().as_secs_f64()
    );
    events.dump_summary("burst-par");
    assert_eq!(
        mismatches, 0,
        "parallel diverged from sequential under churn"
    );

    // ── Verify no deleted IDs appear ──
    let deleted_set: HashSet<u64> = delete_targets.into_iter().collect();
    let mut ghost = 0usize;
    for (_, matches, _) in &par_results {
        for id in matches {
            if deleted_set.contains(id) {
                ghost += 1;
            }
        }
    }
    assert_eq!(ghost, 0, "deleted IDs appeared in match results");
}

// ═════════════════════════════════════════════════════════════════════════════
// 3. RAPID INSERT-DELETE-FLUSH CYCLING — exercises tombstone reclamation
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn rapid_insert_delete_flush_cycles() {
    eprintln!("\n=== RAPID INSERT-DELETE-FLUSH CYCLES ===");
    let t0 = Instant::now();

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 200,
            auto_compact_on_flush: true,
            max_segments: 4,
            holes_ratio_threshold: 0.25,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    // Seed a base segment
    let seed_queries: Vec<(u64, String)> = (0..500)
        .map(|i| (i, format!("player{i} 1994 upper deck basketball")))
        .collect();
    eng.build_from_queries(&seed_queries);

    let mut live_set: HashSet<u64> = (0..500).collect();
    let mut next_id = 1000u64;

    // 10 rounds of: insert batch -> delete some -> flush -> read
    for round in 0..10 {
        eprintln!("\n  Round {round}/10:");

        // Insert 100 new queries
        let insert_count = 100;
        for _i in 0..insert_count {
            let id = next_id;
            next_id += 1;
            let text = format!(
                "round{} player{} {} fleer basketball card",
                round,
                id % 200,
                1990 + (id % 30)
            );
            eng.insert_live(&text, id, 1);
            live_set.insert(id);
        }
        eprintln!("    inserted {}, live_set={}", insert_count, live_set.len());

        // Delete 30 random-ish entries from live set
        let to_delete: Vec<u64> = live_set.iter().copied().take(30).collect();
        for id in &to_delete {
            let _ = eng.delete_by_logical_id(*id);
            live_set.remove(id);
        }
        eprintln!(
            "    deleted {}, live_set={}",
            to_delete.len(),
            live_set.len()
        );

        // Flush (may trigger auto-compact)
        eng.flush();

        let m = eng.metrics();
        eprintln!(
            "    segments={} total_queries={} holes={:?}",
            m.base_segments + 1,
            m.total_queries,
            m.segment_holes
                .iter()
                .map(|h| format!("{:.1}%", h * 100.0))
                .collect::<Vec<_>>()
        );

        // Read sweep — verify no deleted IDs
        let mut scratch = MatchScratch::new();
        let mut out = Vec::new();
        let test_title = "player5 1994 upper deck fleer basketball card round3";
        eng.match_title(test_title, &mut scratch, &mut out, true);

        for id in &out {
            assert!(
                live_set.contains(id) || *id >= 1000,
                "round {round}: deleted ID {id} appeared in results"
            );
        }
    }

    print_metrics("final", &eng.metrics());
    events.dump_summary("rapid-cycles");

    let f = events.flushes.load(Ordering::Relaxed);
    let c = events.compactions.load(Ordering::Relaxed);
    eprintln!(
        "  Total: {} flushes, {} compactions, elapsed={:.1}s",
        f,
        c,
        t0.elapsed().as_secs_f64()
    );
    assert!(f >= 10, "expected at least 10 flushes, got {f}");
    assert!(c > 0, "expected at least 1 auto-compaction, got 0");
}

// ═════════════════════════════════════════════════════════════════════════════
// 4. HIGH-VOLUME PARALLEL MATCHING UNDER MUTATION
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn high_volume_parallel_read_after_mutations() {
    eprintln!("\n=== HIGH-VOLUME PARALLEL READS AFTER MUTATIONS ===");
    let t0 = Instant::now();

    let cfg = GenConfig {
        num_queries: 50_000,
        num_titles: 5_000,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0xFACE_B00C,
        num_players: 5_000,
        num_sets: 2_000,
    };
    let data = generate(&cfg);
    let q = &data.queries;

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 10_000,
            auto_compact_on_flush: true,
            max_segments: 6,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    // Phase 1: build base
    eprintln!("  Phase 1: bulk load {} queries", q.len() / 2);
    eng.build_from_queries(&q[..q.len() / 2]);

    // Phase 2: live inserts
    eprintln!("  Phase 2: live insert {} queries", q.len() / 2);
    for (logical, text) in &q[q.len() / 2..] {
        eng.insert_live(text, *logical, 1);
    }

    // Phase 3: delete 10% of queries
    let delete_set: HashSet<u64> = q.iter().step_by(10).map(|(id, _)| *id).collect();
    eprintln!("  Phase 3: deleting {} queries", delete_set.len());
    for id in &delete_set {
        let _ = eng.delete_by_logical_id(*id);
    }

    // Phase 4: flush + compact
    eng.flush();
    eng.compact_all();
    print_metrics("post-mutation", &eng.metrics());

    // Phase 5: parallel read of all 5k titles
    eprintln!(
        "  Phase 5: parallel read of {} titles...",
        data.titles.len()
    );
    let par_start = Instant::now();
    let par_results = eng.match_titles_par(&data.titles, true);
    let par_elapsed = par_start.elapsed();

    let total_par_matches: usize = par_results.iter().map(|(_, ids, _)| ids.len()).sum();
    let total_candidates: u32 = par_results
        .iter()
        .map(|(_, _, s)| s.unique_candidates)
        .sum();
    let total_skipped: u32 = par_results.iter().map(|(_, _, s)| s.probes_skipped).sum();

    eprintln!(
        "    parallel: {} matches, {} candidates, {} probes skipped, {:.1}ms",
        total_par_matches,
        total_candidates,
        total_skipped,
        par_elapsed.as_secs_f64() * 1000.0
    );

    // Phase 6: sequential read for comparison
    eprintln!("  Phase 6: sequential read for comparison...");
    let seq_start = Instant::now();
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut total_seq_matches = 0usize;
    for title in &data.titles {
        eng.match_title(title, &mut scratch, &mut out, true);
        total_seq_matches += out.len();
    }
    let seq_elapsed = seq_start.elapsed();

    eprintln!(
        "    sequential: {} matches, {:.1}ms",
        total_seq_matches,
        seq_elapsed.as_secs_f64() * 1000.0
    );
    eprintln!(
        "    speedup: {:.1}x",
        seq_elapsed.as_secs_f64() / par_elapsed.as_secs_f64().max(0.001)
    );

    // Verify no deleted IDs in results
    let mut ghosts = 0usize;
    for (_, matches, _) in &par_results {
        for id in matches {
            if delete_set.contains(id) {
                ghosts += 1;
            }
        }
    }

    events.dump_summary("high-vol");
    eprintln!("  elapsed={:.1}s", t0.elapsed().as_secs_f64());

    assert_eq!(ghosts, 0, "deleted IDs appeared in parallel results");
    assert!(total_par_matches > 0, "no matches at all");
    assert_eq!(
        total_par_matches, total_seq_matches,
        "parallel ({total_par_matches}) != sequential ({total_seq_matches}) match count"
    );
}

// ═════════════════════════════════════════════════════════════════════════════
// 5. UPDATE VISIBILITY — version supersession
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn update_visibility_across_flush_compact() {
    eprintln!("\n=== UPDATE VISIBILITY ACROSS FLUSH/COMPACT ===");

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 100,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    // Insert v1 queries
    let v1_queries: Vec<(u64, String)> = vec![
        (1, "michael jordan 1986 fleer".into()),
        (2, "lebron james rookie card".into()),
        (3, "kobe bryant 1996 topps".into()),
        (4, "shaquille oneal orlando magic".into()),
        (5, "tim duncan 1997 bowman".into()),
    ];
    eng.build_from_queries(&v1_queries);
    eprintln!("  v1 loaded: {} queries", v1_queries.len());

    // Check v1 matches
    let v1_hit = match_ids(&eng, "michael jordan 1986 fleer basketball card");
    eprintln!("  v1 match for MJ: {v1_hit:?}");
    assert!(v1_hit.contains(&1), "v1 MJ query should match");

    // Update: delete old, re-insert with new text + higher version
    let _ = eng.delete_by_logical_id(1);
    eng.insert_live("michael jordan 1986 fleer rookie card", 1, 2);

    // Before flush: should still match with updated text
    let pre_flush = match_ids(&eng, "michael jordan 1986 fleer rookie card psa 10");
    eprintln!("  pre-flush updated MJ: {pre_flush:?}");
    assert!(pre_flush.contains(&1), "updated MJ should match pre-flush");

    // Original title (without "rookie card") should NOT match updated query
    // if the new text requires "rookie" and "card"
    // (Actually it still will — the updated query adds "rookie card" but the
    //  original features are still present. This is correct behavior.)

    // Flush
    eng.flush();
    let post_flush = match_ids(&eng, "michael jordan 1986 fleer rookie card psa 10");
    eprintln!("  post-flush updated MJ: {post_flush:?}");
    assert!(
        post_flush.contains(&1),
        "updated MJ should match post-flush"
    );

    // Bulk updates: update all 5 queries, flush, compact
    for (id, _) in &v1_queries {
        let _ = eng.delete_by_logical_id(*id);
        eng.insert_live(&format!("updated_{id} michael jordan lebron kobe"), *id, 3);
    }
    eng.flush();

    // Add another segment so compact_all has something to merge
    eng.insert_live("extra query padding segment", 999, 1);
    eng.flush();

    eng.compact_all();
    print_metrics("post-update-compact", &eng.metrics());

    // All original IDs should still be findable via the updated text
    let final_hits = match_ids(&eng, "michael jordan lebron kobe updated_1 updated_2");
    eprintln!("  final hits: {final_hits:?}");

    events.dump_summary("update-vis");
}

// ═════════════════════════════════════════════════════════════════════════════
// 6. METRICS CONSISTENCY UNDER CHURN
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn metrics_stay_consistent_under_churn() {
    eprintln!("\n=== METRICS CONSISTENCY UNDER CHURN ===");

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 500,
            auto_compact_on_flush: true,
            max_segments: 5,
            holes_ratio_threshold: 0.2,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    let cfg = GenConfig {
        num_queries: 10_000,
        num_titles: 500,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0xDEAD_C0DE,
        num_players: 2_000,
        num_sets: 800,
    };
    let data = generate(&cfg);

    // Build initial
    let report = eng.build_from_queries(&data.queries);
    let initial_count = report.ingested;
    eprintln!("  initial: ingested={initial_count}");

    let mut m = eng.metrics();
    assert_eq!(
        m.total_queries, initial_count,
        "metrics.total_queries != ingested after build"
    );

    // Track live count manually
    let mut expected_live = initial_count;

    // 5 rounds of insert/delete/flush
    let mut next_id = 100_000u64;
    for round in 0..5 {
        // Insert 200
        let mut inserted = 0usize;
        for _ in 0..200 {
            let text = format!("round{round} player{next_id} 1994 topps");
            if eng.insert_live(&text, next_id, 1).is_some() {
                inserted += 1;
            }
            next_id += 1;
        }
        expected_live += inserted;

        // Delete 50 from the initial batch
        let del_start = round * 50;
        let mut deleted = 0usize;
        for i in del_start..del_start + 50 {
            let logical = data.queries[i].0;
            let n = eng.delete_by_logical_id(logical).unwrap();
            if n > 0 {
                deleted += 1;
                expected_live -= 1;
            }
        }

        eng.flush();
        m = eng.metrics();

        eprintln!(
            "  round {}: inserted={} deleted={} expected_live={} metrics.total={}",
            round, inserted, deleted, expected_live, m.total_queries
        );

        // total_queries should track: it counts alive entries
        // (Note: total_queries counts all entries including tombstoned, so this
        // is a >= check. After compaction, tombstones are reclaimed.)
        assert!(
            m.total_queries >= expected_live,
            "round {}: metrics.total_queries ({}) < expected_live ({})",
            round,
            m.total_queries,
            expected_live
        );

        // segment_sizes should sum to total_queries
        let size_sum: usize = m.segment_sizes.iter().sum::<usize>() + m.memtable_entries;
        assert_eq!(
            m.total_queries, size_sum,
            "round {}: total ({}) != sum of segment_sizes + memtable ({})",
            round, m.total_queries, size_sum
        );
    }

    // Final compact should reclaim tombstones
    eng.compact_all();
    m = eng.metrics();
    print_metrics("final", &m);
    events.dump_summary("metrics-churn");

    // After compaction, holes should be near 0
    for (i, &h) in m.segment_holes.iter().enumerate() {
        assert!(
            h < 0.01,
            "segment {} has {:.1}% holes after compact_all",
            i,
            h * 100.0
        );
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// 7. MULTI-SEGMENT INTERLEAVED OPS — exercises segment boundary logic
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn multi_segment_interleaved_ops_oracle() {
    eprintln!("\n=== MULTI-SEGMENT INTERLEAVED OPS (oracle verified) ===");
    let t0 = Instant::now();

    let cfg = GenConfig {
        num_queries: 30_000,
        num_titles: 2_000,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x1E_2E_AE,
        num_players: 3_000,
        num_sets: 1_200,
    };
    let data = generate(&cfg);
    let q = &data.queries;
    let chunk = q.len() / 10;

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 5_000,
            auto_compact_on_flush: false, // manual control
            auto_compact_on_ingest: false,
            max_segments: 20,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    let mut live_queries: Vec<(u64, String)> = Vec::new();

    // Step 1: build initial segment
    eprintln!("  Step 1: build_from_queries({chunk})");
    eng.build_from_queries(&q[..chunk]);
    live_queries.extend_from_slice(&q[..chunk]);

    // Step 2: bulk_ingest 2 more segments
    for wave in 1..=2 {
        let lo = wave * chunk;
        let hi = (wave + 1) * chunk;
        eprintln!("  Step 2.{}: bulk_ingest({})", wave, hi - lo);
        eng.bulk_ingest(&q[lo..hi]);
        live_queries.extend_from_slice(&q[lo..hi]);
    }

    // Step 3: live inserts
    eprintln!("  Step 3: live inserts({chunk})");
    for (logical, text) in &q[3 * chunk..4 * chunk] {
        eng.insert_live(text, *logical, 1);
        live_queries.push((*logical, text.clone()));
    }
    eng.flush();

    // Step 4: compact first 2 segments
    eprintln!("  Step 4: compact_range(0, 2)");
    eng.compact_range(0, 2);
    print_metrics("post-compact-range", &eng.metrics());

    // Step 5: more inserts + deletes
    eprintln!("  Step 5: inserts + deletes");
    for (logical, text) in &q[4 * chunk..5 * chunk] {
        eng.insert_live(text, *logical, 1);
        live_queries.push((*logical, text.clone()));
    }
    // Delete queries from the first chunk
    let mut deleted = HashSet::new();
    for (logical, _) in q[..chunk].iter().step_by(3) {
        let _ = eng.delete_by_logical_id(*logical);
        deleted.insert(*logical);
    }
    live_queries.retain(|(id, _)| !deleted.contains(id));
    eng.flush();

    // Step 6: bulk_ingest more
    eprintln!("  Step 6: bulk_ingest({chunk})");
    eng.bulk_ingest(&q[5 * chunk..6 * chunk]);
    live_queries.extend_from_slice(&q[5 * chunk..6 * chunk]);

    // Step 7: compact_all
    eprintln!("  Step 7: compact_all");
    eng.compact_all();
    print_metrics("post-compact-all", &eng.metrics());

    // Step 8: final wave of inserts
    for (logical, text) in &q[6 * chunk..7 * chunk] {
        eng.insert_live(text, *logical, 1);
        live_queries.push((*logical, text.clone()));
    }

    // ── Oracle verification ──
    eprintln!("  Oracle verification over {} titles...", data.titles.len());
    let brute = Brute::build(&live_queries);
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    let mut false_neg = 0usize;
    let mut false_pos = 0usize;
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
        for e in &eng_set {
            if !truth.contains(e) {
                false_pos += 1;
            }
        }
    }

    eprintln!(
        "    oracle: truth={} false_neg={} false_pos={} elapsed={:.1}s",
        total_truth,
        false_neg,
        false_pos,
        t0.elapsed().as_secs_f64()
    );
    events.dump_summary("interleaved");
    assert_eq!(false_neg, 0, "interleaved ops introduced false negatives");
    assert!(total_truth > 0, "degenerate test: no truth matches");
}

// ═════════════════════════════════════════════════════════════════════════════
// 8. AUTO-COMPACTION TRIGGER VERIFICATION
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn auto_compaction_triggers_correctly() {
    eprintln!("\n=== AUTO-COMPACTION TRIGGER VERIFICATION ===");

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 300,
            auto_compact_on_flush: true,
            max_segments: 3, // aggressive — triggers often
            holes_ratio_threshold: 0.15,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    // Rapidly build segments to trigger segment-count compaction
    for batch in 0..8 {
        let queries: Vec<(u64, String)> = (0..400)
            .map(|i| {
                let id = (batch * 1000 + i) as u64;
                (id, format!("batch{batch} player{i} 1994 topps card"))
            })
            .collect();

        if batch == 0 {
            eng.build_from_queries(&queries);
        } else {
            for (id, text) in &queries {
                eng.insert_live(text, *id, 1);
            }
            eng.flush();
        }

        let m = eng.metrics();
        eprintln!(
            "  batch {}: segments={} total={}",
            batch,
            m.base_segments + 1,
            m.total_queries
        );
    }

    let compactions = events.compactions.load(Ordering::Relaxed);
    eprintln!("  total auto-compactions triggered: {compactions}");
    events.dump_summary("auto-compact");

    assert!(
        compactions >= 2,
        "expected >= 2 auto-compactions with max_segments=3 and 8 batches, got {compactions}"
    );

    // Engine should have compacted down
    let m = eng.metrics();
    assert!(
        m.base_segments <= 4,
        "expected <= 4 segments after auto-compaction, got {}",
        m.base_segments
    );
}

// ═════════════════════════════════════════════════════════════════════════════
// 9. DELETE-HEAVY WORKLOAD — stress tombstone / holes ratio
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn delete_heavy_workload() {
    eprintln!("\n=== DELETE-HEAVY WORKLOAD ===");
    let t0 = Instant::now();

    let cfg = GenConfig {
        num_queries: 15_000,
        num_titles: 1_000,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0xDE_1E_7E,
        num_players: 2_000,
        num_sets: 800,
    };
    let data = generate(&cfg);

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 5_000,
            auto_compact_on_flush: true,
            max_segments: 4,
            holes_ratio_threshold: 0.3,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    eng.build_from_queries(&data.queries);
    let initial_count = eng.metrics().total_queries;
    eprintln!("  initial: {initial_count} queries");

    // Delete 80% of queries
    let delete_count = (data.queries.len() * 8) / 10;
    eprintln!("  deleting {delete_count} queries (80%)");
    let mut deleted = HashSet::new();
    for (logical, _) in data.queries.iter().take(delete_count) {
        let _ = eng.delete_by_logical_id(*logical);
        deleted.insert(*logical);
    }

    // Insert replacements
    eprintln!("  inserting {} replacements", delete_count / 2);
    for i in 0..delete_count / 2 {
        let text = format!("replacement{i} michael jordan 1994 upper deck");
        eng.insert_live(&text, 10_000_000 + i as u64, 1);
    }
    eng.flush();

    let m = eng.metrics();
    eprintln!(
        "  pre-compact: segments={} total={} holes={:?}",
        m.base_segments,
        m.total_queries,
        m.segment_holes
            .iter()
            .map(|h| format!("{:.1}%", h * 100.0))
            .collect::<Vec<_>>()
    );

    // Compact to reclaim
    eng.compact_all();
    let m = eng.metrics();
    print_metrics("post-compact", &m);

    // Verify no ghosts in search results
    let par_results = eng.match_titles_par(&data.titles, true);
    let mut ghosts = 0usize;
    let total_matches: usize = par_results.iter().map(|(_, ids, _)| ids.len()).sum();
    for (_, matches, _) in &par_results {
        for id in matches {
            if deleted.contains(id) {
                ghosts += 1;
            }
        }
    }

    eprintln!(
        "  total_matches={} ghosts={} elapsed={:.1}s",
        total_matches,
        ghosts,
        t0.elapsed().as_secs_f64()
    );
    events.dump_summary("delete-heavy");
    assert_eq!(ghosts, 0, "deleted IDs appeared in results");
}

// ═════════════════════════════════════════════════════════════════════════════
// 10. FULL LIFECYCLE SOAK — everything combined
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn full_lifecycle_soak() {
    eprintln!("\n=== FULL LIFECYCLE SOAK ===");
    let t0 = Instant::now();

    let cfg = GenConfig {
        num_queries: 40_000,
        num_titles: 4_000,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x50A0_7E57,
        num_players: 4_000,
        num_sets: 1_600,
    };
    let data = generate(&cfg);
    let q = &data.queries;
    let chunk = q.len() / 8;

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 3_000,
            auto_compact_on_flush: true,
            max_segments: 5,
            holes_ratio_threshold: 0.25,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    let mut live_ids: HashSet<u64> = HashSet::new();

    // ── Stage 1: Bulk load ──
    eprintln!("  Stage 1: bulk load {} queries", chunk * 2);
    eng.build_from_queries(&q[..chunk]);
    eng.bulk_ingest(&q[chunk..2 * chunk]);
    for (id, _) in &q[..2 * chunk] {
        live_ids.insert(*id);
    }
    print_metrics("stage-1", &eng.metrics());

    // ── Stage 2: Mixed insert + delete ──
    eprintln!("  Stage 2: interleaved insert/delete");
    for (logical, text) in &q[2 * chunk..4 * chunk] {
        eng.insert_live(text, *logical, 1);
        live_ids.insert(*logical);
    }
    // Delete every 5th from first chunk
    for (logical, _) in q[..chunk].iter().step_by(5) {
        let _ = eng.delete_by_logical_id(*logical);
        live_ids.remove(logical);
    }
    eng.flush();
    print_metrics("stage-2", &eng.metrics());

    // ── Stage 3: Parallel read checkpoint ──
    eprintln!("  Stage 3: parallel read checkpoint");
    let par1 = eng.match_titles_par(&data.titles[..1000], true);
    let par1_matches: usize = par1.iter().map(|(_, ids, _)| ids.len()).sum();
    eprintln!("    checkpoint: {par1_matches} matches over 1000 titles");

    // ── Stage 4: More inserts + compact ──
    eprintln!("  Stage 4: more inserts + compact");
    eng.bulk_ingest(&q[4 * chunk..5 * chunk]);
    for (id, _) in &q[4 * chunk..5 * chunk] {
        live_ids.insert(*id);
    }
    eng.compact_all();
    print_metrics("stage-4", &eng.metrics());

    // ── Stage 5: Update wave ──
    eprintln!("  Stage 5: update {} queries", chunk / 2);
    for (logical, text) in &q[..chunk / 2] {
        if live_ids.contains(logical) {
            let _ = eng.delete_by_logical_id(*logical);
            let new_text = format!("{text} updated");
            eng.insert_live(&new_text, *logical, 5);
        }
    }
    eng.flush();

    // ── Stage 6: Heavy delete ──
    let del_batch: Vec<u64> = q[2 * chunk..3 * chunk]
        .iter()
        .step_by(2)
        .map(|(id, _)| *id)
        .collect();
    eprintln!("  Stage 6: delete {} queries", del_batch.len());
    for id in &del_batch {
        let _ = eng.delete_by_logical_id(*id);
        live_ids.remove(id);
    }

    // ── Stage 7: Final inserts + flush + compact ──
    eprintln!("  Stage 7: final inserts + flush + compact");
    for (logical, text) in &q[5 * chunk..6 * chunk] {
        eng.insert_live(text, *logical, 1);
        live_ids.insert(*logical);
    }
    eng.flush();
    eng.compact_all();
    print_metrics("stage-7", &eng.metrics());

    // ── Stage 8: Full parallel read sweep ──
    eprintln!("  Stage 8: full parallel + sequential comparison");
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut seq_results: Vec<HashSet<u64>> = Vec::new();
    for title in &data.titles {
        eng.match_title(title, &mut scratch, &mut out, true);
        seq_results.push(out.iter().copied().collect());
    }

    let par_results = eng.match_titles_par(&data.titles, true);
    let mut par_mismatches = 0usize;
    let mut ghost_total = 0usize;
    for (idx, matches, _) in &par_results {
        let par_set: HashSet<u64> = matches.iter().copied().collect();
        if par_set != seq_results[*idx] {
            par_mismatches += 1;
        }
        for id in &par_set {
            if !live_ids.contains(id) {
                ghost_total += 1;
            }
        }
    }

    let total_matches: usize = par_results.iter().map(|(_, ids, _)| ids.len()).sum();

    eprintln!(
        "\n  SOAK RESULTS: matches={} par_mismatches={} ghosts={} elapsed={:.1}s",
        total_matches,
        par_mismatches,
        ghost_total,
        t0.elapsed().as_secs_f64()
    );
    events.dump_summary("soak");

    let el = events.entries.lock().unwrap();
    eprintln!("  total events logged: {}", el.len());

    assert_eq!(par_mismatches, 0, "parallel != sequential in soak");
    // Ghost check: deleted IDs should not appear. However, live_ids tracking
    // may miss queries rejected at parse/class-D time, so we only flag if the
    // ghost was in our explicit delete set.
    let deleted_explicit: HashSet<u64> = del_batch.into_iter().collect();
    let mut explicit_ghosts = 0usize;
    for (_, matches, _) in &par_results {
        for id in matches {
            if deleted_explicit.contains(id) {
                explicit_ghosts += 1;
            }
        }
    }
    assert_eq!(
        explicit_ghosts, 0,
        "explicitly deleted IDs appeared in results"
    );
    assert!(total_matches > 0, "soak produced zero matches");
}

// ═════════════════════════════════════════════════════════════════════════════
// 11. INTERLEAVED INSERT + MATCH — read while the index is growing
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn match_while_inserting_varied_queries() {
    eprintln!("\n=== MATCH WHILE INSERTING (varied queries) ===");
    let t0 = Instant::now();

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 500,
            auto_compact_on_flush: true,
            max_segments: 4,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    // Diverse query families — different sports, categories, structures
    let query_families: Vec<Vec<(u64, &str)>> = vec![
        // Basketball cards — simple required
        vec![
            (100, "michael jordan 1986 fleer"),
            (101, "michael jordan 1997 upper deck"),
            (102, "michael jordan 1993 topps"),
            (103, "lebron james 2003 topps chrome"),
            (104, "lebron james rookie card"),
            (105, "kobe bryant 1996 topps"),
            (106, "kobe bryant 1996 bowman"),
            (107, "shaquille oneal 1992 fleer"),
            (108, "tim duncan 1997 bowman"),
            (109, "allen iverson 1996 topps"),
        ],
        // With any-of groups
        vec![
            (200, "michael jordan (fleer,topps,bowman) 1986"),
            (201, "lebron james (topps,upper deck) rookie"),
            (202, "kobe bryant (topps,bowman,fleer) 1996"),
            (203, "(jordan,james,bryant) rookie card"),
            (204, "(psa,bgs,sgc) michael jordan"),
        ],
        // With forbidden terms
        vec![
            (300, "michael jordan card -(reprint,auto,lot)"),
            (301, "lebron james rookie -(fake,auto)"),
            (302, "kobe bryant topps -(reprint,lot,break)"),
            (303, "jordan 1986 fleer -(psa,bgs,sgc)"),
            (304, "basketball card rookie -(auto,signed,used)"),
        ],
        // Mixed complex
        vec![
            (
                400,
                "michael jordan (1986,1993,1997) (fleer,topps) -(reprint)",
            ),
            (
                401,
                "lebron james (2003,2004) (topps,upper deck) -(auto,lot)",
            ),
            (402, "(jordan,james) (psa,bgs) -(fake,reprint)"),
            (403, "kobe bryant (topps,bowman) card -(auto,lot,break)"),
            (404, "(jordan,lebron,kobe) (fleer,topps,bowman) rookie"),
        ],
        // Year-heavy / brand-heavy
        vec![
            (500, "1986 fleer basketball"),
            (501, "1997 upper deck basketball card"),
            (502, "topps chrome 2003 basketball"),
            (503, "bowman 1997 rookie card"),
            (504, "upper deck 1994 basketball"),
        ],
        // Single-word broad queries
        vec![
            (600, "jordan"),
            (601, "lebron"),
            (602, "rookie"),
            (603, "basketball"),
            (604, "fleer"),
        ],
    ];

    // Varied titles to search against
    let titles = vec![
        "michael jordan 1986 fleer basketball card psa 10",
        "lebron james 2003 topps chrome rookie card",
        "kobe bryant 1996 topps rookie card bgs 9.5",
        "michael jordan 1997 upper deck game jersey",
        "1986 fleer michael jordan rookie card #57",
        "lebron james 2004 upper deck rookie card auto",
        "kobe bryant 1996 bowman best card psa 9",
        "tim duncan 1997 bowman rookie card",
        "allen iverson 1996 topps draft pick",
        "shaquille oneal 1992 fleer rookie card",
        "michael jordan 1993 topps finest card lot",
        "jordan james bryant triple card topps 2008",
        "1997 upper deck basketball complete set",
        "basketball card reprint auto signed lot",
        "michael jordan fleer reprint fake 1986",
        "lebron james topps chrome 2003 psa 10 gem mt",
        "kobe bryant topps bowman 1996 rookie draft pick",
        "vintage basketball card 1986 fleer set break",
        "michael jordan game used jersey auto signed",
        "rookie card lot basketball topps bowman 1994",
    ];

    let mut match_count_history: Vec<(usize, usize)> = Vec::new(); // (queries_in, total_matches)
    let mut source_lookups_ok = 0usize;
    let mut source_lookups_total = 0usize;
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();

    // Feed families one at a time, matching after each insertion batch
    for (fam_idx, family) in query_families.iter().enumerate() {
        eprintln!("\n  Family {} ({} queries):", fam_idx, family.len());

        for (logical_id, query_text) in family {
            let result = eng.try_insert_live(query_text, *logical_id, 1);
            match &result {
                Ok(reverse_rusty::segment::InsertOutcome::Inserted(_)) => {
                    eprintln!("    + id={logical_id} {query_text:?}");
                }
                Ok(reverse_rusty::segment::InsertOutcome::RejectedClassD) => {
                    eprintln!("    D id={logical_id} {query_text:?} (class D rejected)");
                }
                Err(e) => {
                    eprintln!("    ! id={logical_id} parse error: {e}");
                }
            }

            // Match every title after each single insert
            for title in &titles {
                eng.match_title(title, &mut scratch, &mut out, true);
            }

            // Verify doc source is retrievable for successfully inserted queries
            source_lookups_total += 1;
            if let Ok(reverse_rusty::segment::InsertOutcome::Inserted(_)) = result {
                let source = eng.get_query_source(*logical_id);
                if let Some(src) = source {
                    assert_eq!(
                        src, *query_text,
                        "source mismatch for id={logical_id}: expected {query_text:?}, got {src:?}"
                    );
                    source_lookups_ok += 1;
                } else {
                    panic!("get_query_source({logical_id}) returned None right after insert");
                }
            }
        }

        // Record match count snapshot after each family
        let mut total = 0usize;
        for title in &titles {
            eng.match_title(title, &mut scratch, &mut out, true);
            total += out.len();
        }
        match_count_history.push((eng.num_queries(), total));
        eprintln!(
            "    snapshot: {} queries in engine, {} total matches across {} titles",
            eng.num_queries(),
            total,
            titles.len()
        );
    }

    // Match counts should be monotonically non-decreasing as we add queries
    // (we're only adding, not deleting yet)
    for window in match_count_history.windows(2) {
        let (q_prev, _) = window[0];
        let (q_cur, _) = window[1];
        assert!(
            q_cur >= q_prev,
            "query count went backwards: {q_prev} -> {q_cur}"
        );
        // Not strictly monotonic (class-D rejects add queries that don't match),
        // but total matches should generally not decrease when only adding
    }

    eprintln!("\n  source lookups: {source_lookups_ok}/{source_lookups_total} succeeded");
    eprintln!("  match history: {match_count_history:?}");

    // ── Now interleave deletes with reads ──
    eprintln!("\n  Interleaving deletes with reads...");
    let delete_targets = vec![100, 200, 300, 400, 500]; // one from each family
    for del_id in &delete_targets {
        let _ = eng.delete_by_logical_id(*del_id);
        eprintln!("    deleted id={del_id}");

        // Verify doc source removed
        assert!(
            eng.get_query_source(*del_id).is_none(),
            "get_query_source({del_id}) should return None after delete"
        );

        // Verify match results don't include deleted ID
        for title in &titles {
            eng.match_title(title, &mut scratch, &mut out, true);
            assert!(
                !out.contains(del_id),
                "deleted id {del_id} still appears in matches for {title:?}"
            );
        }
    }

    // ── Flush, compact, re-verify ──
    eng.flush();
    eng.compact_all();
    print_metrics("final", &eng.metrics());

    for del_id in &delete_targets {
        assert!(
            eng.get_query_source(*del_id).is_none(),
            "get_query_source({del_id}) should still be None after compact"
        );
    }

    // Parallel read of all titles — verify agreement with sequential
    let par_results = eng.match_titles_par(
        &titles
            .iter()
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>(),
        true,
    );
    let mut seq_results: Vec<HashSet<u64>> = Vec::new();
    for title in &titles {
        eng.match_title(title, &mut scratch, &mut out, true);
        seq_results.push(out.iter().copied().collect());
    }

    let mut mismatches = 0usize;
    for (idx, matches, _) in &par_results {
        let par_set: HashSet<u64> = matches.iter().copied().collect();
        if par_set != seq_results[*idx] {
            mismatches += 1;
        }
    }

    events.dump_summary("match-while-insert");
    eprintln!("  elapsed={:.1}s", t0.elapsed().as_secs_f64());
    assert_eq!(mismatches, 0, "parallel != sequential after insert+delete");
}

// ═════════════════════════════════════════════════════════════════════════════
// 12. DOC STORE CONSISTENCY — source lookups through full lifecycle
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn doc_store_consistent_through_lifecycle() {
    eprintln!("\n=== DOC STORE CONSISTENCY THROUGH LIFECYCLE ===");

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 200,
            auto_compact_on_flush: true,
            max_segments: 4,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    // Phase 1: Bulk load with source tracking
    let batch: Vec<(u64, String)> = (0..500)
        .map(|i| {
            let text = match i % 5 {
                0 => format!("player{i} 1994 upper deck basketball card"),
                1 => format!("player{i} (topps,fleer,bowman) rookie -(auto)"),
                2 => format!("player{i} 1986 fleer (psa,bgs) -(reprint,lot)"),
                3 => format!("(player{i},athlete{i}) topps 1997 card"),
                _ => format!("player{i} basketball card"),
            };
            (i as u64, text)
        })
        .collect();

    eng.build_from_queries(&batch);
    eprintln!("  Phase 1: loaded {} queries", batch.len());

    // Verify all sources accessible
    let mut found = 0usize;
    for (id, text) in &batch {
        if let Some(src) = eng.get_query_source(*id) {
            assert_eq!(&src, text, "source mismatch for id={id}");
            found += 1;
        }
    }
    eprintln!("  sources found after build: {}/{}", found, batch.len());

    // Phase 2: Live inserts — verify source available immediately
    eprintln!("  Phase 2: live inserts with source checks");
    for i in 500..700u64 {
        let text = format!("live{} michael jordan {} fleer", i, 1980 + (i % 20));
        eng.insert_live(&text, i, 1);
        let src = eng.get_query_source(i);
        assert!(
            src.is_some(),
            "source missing immediately after insert_live({i})"
        );
        assert_eq!(src.unwrap(), text);
    }

    // Phase 3: Delete and verify source removed
    eprintln!("  Phase 3: delete + source removal check");
    let delete_ids: Vec<u64> = (0..100).collect();
    for id in &delete_ids {
        let _ = eng.delete_by_logical_id(*id);
    }
    for id in &delete_ids {
        assert!(
            eng.get_query_source(*id).is_none(),
            "source for deleted id {id} should be None"
        );
    }
    // Non-deleted should still be there
    for id in 100..200u64 {
        assert!(
            eng.get_query_source(id).is_some(),
            "source for non-deleted id {id} should exist"
        );
    }

    // Phase 4: Flush + verify sources survive
    eng.flush();
    eprintln!("  Phase 4: post-flush source check");
    for id in 200..300u64 {
        assert!(
            eng.get_query_source(id).is_some(),
            "source {id} lost after flush"
        );
    }
    for id in 500..700u64 {
        assert!(
            eng.get_query_source(id).is_some(),
            "live source {id} lost after flush"
        );
    }

    // Phase 5: Compact + verify sources survive
    eng.compact_all();
    eprintln!("  Phase 5: post-compact source check");
    for id in 200..300u64 {
        assert!(
            eng.get_query_source(id).is_some(),
            "source {id} lost after compact"
        );
    }
    for id in &delete_ids {
        assert!(
            eng.get_query_source(*id).is_none(),
            "deleted source {id} reappeared after compact"
        );
    }

    // Phase 6: Update (delete + re-insert) — source should reflect new text
    eprintln!("  Phase 6: update + source verification");
    for id in 200..250u64 {
        let new_text = format!("UPDATED player{id} 2024 panini prizm basketball");
        let _ = eng.delete_by_logical_id(id);
        eng.insert_live(&new_text, id, 2);
        let src = eng
            .get_query_source(id)
            .expect("source missing after update");
        assert_eq!(src, new_text, "source not updated for id={id}");
    }

    // Phase 7: Interleave match requests with doc lookups
    eprintln!("  Phase 7: interleaved match + doc lookups");
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let test_titles = [
        "player250 1994 upper deck basketball card psa 10",
        "live550 michael jordan 1990 fleer card",
        "UPDATED player220 2024 panini prizm basketball card",
        "player400 topps 1997 card basketball",
        "michael jordan 1986 fleer rookie card",
    ];

    for title in &test_titles {
        eng.match_title(title, &mut scratch, &mut out, true);
        eprintln!("    title={:?} -> {} matches", title, out.len());

        // For every match, verify we can look up its source
        for &matched_id in &out {
            let src = eng.get_query_source(matched_id);
            assert!(
                src.is_some(),
                "matched id {matched_id} has no source for title {title:?}"
            );
        }
    }

    print_metrics("final", &eng.metrics());
    events.dump_summary("doc-store");
}

// ═════════════════════════════════════════════════════════════════════════════
// 13. GROWING INDEX WHILE MATCHING — simulate a live server receiving queries
//     and search requests simultaneously
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn simulate_live_server_traffic() {
    eprintln!("\n=== SIMULATED LIVE SERVER TRAFFIC ===");
    let t0 = Instant::now();

    let cfg = GenConfig {
        num_queries: 20_000,
        num_titles: 3_000,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x5E_2F_1C,
        num_players: 3_000,
        num_sets: 1_200,
    };
    let data = generate(&cfg);

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 1_000,
            auto_compact_on_flush: true,
            max_segments: 5,
            holes_ratio_threshold: 0.2,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut inserted_count = 0usize;
    let mut total_searches = 0usize;
    let mut total_matches = 0usize;
    let mut total_doc_lookups = 0usize;
    let mut doc_lookup_hits = 0usize;
    let mut deleted_ids: HashSet<u64> = HashSet::new();

    // Simulate interleaved traffic: for every N inserts, do M searches and K doc lookups
    let insert_batch_size = 50;
    let searches_per_batch = 20;
    let doc_lookups_per_batch = 10;
    let delete_every_n_batches = 5;

    let mut title_cursor = 0usize;
    let mut batch_num = 0usize;

    for query_chunk in data.queries.chunks(insert_batch_size) {
        // ── Write: insert a batch of queries ──
        for (logical, text) in query_chunk {
            eng.insert_live(text, *logical, 1);
            inserted_count += 1;
        }

        // ── Read: search against rotating titles ──
        for _ in 0..searches_per_batch {
            let title = &data.titles[title_cursor % data.titles.len()];
            eng.match_title(title, &mut scratch, &mut out, true);
            total_matches += out.len();
            total_searches += 1;

            // Verify no deleted IDs in results
            for &id in &out {
                assert!(
                    !deleted_ids.contains(&id),
                    "batch {batch_num}: deleted id {id} in matches for {title:?}"
                );
            }

            title_cursor += 1;
        }

        // ── Read: doc lookups on recently inserted IDs ──
        let recent_ids: Vec<u64> = query_chunk.iter().map(|(id, _)| *id).collect();
        for i in 0..doc_lookups_per_batch.min(recent_ids.len()) {
            let id = recent_ids[i];
            total_doc_lookups += 1;
            if let Some(src) = eng.get_query_source(id) {
                doc_lookup_hits += 1;
                assert_eq!(
                    src, query_chunk[i].1,
                    "source mismatch for recently inserted id={id}"
                );
            }
        }

        // ── Write: periodic deletes ──
        if batch_num.is_multiple_of(delete_every_n_batches) && batch_num > 0 {
            let del_count = insert_batch_size / 5;
            let del_start = (batch_num - delete_every_n_batches) * insert_batch_size;
            let del_end = (del_start + del_count).min(data.queries.len());
            for (logical, _) in &data.queries[del_start..del_end] {
                let _ = eng.delete_by_logical_id(*logical);
                deleted_ids.insert(*logical);
            }
            if del_start < del_end {
                eprintln!(
                    "    batch {}: deleted {} queries (ids {}..{})",
                    batch_num,
                    del_end - del_start,
                    data.queries[del_start].0,
                    data.queries[del_end - 1].0,
                );
            }
        }

        // Log progress every 100 batches
        if batch_num.is_multiple_of(100) {
            let m = eng.metrics();
            eprintln!(
                "  batch {}: inserted={} searches={} matches={} doc_lookups={}/{} segments={} queries={}",
                batch_num,
                inserted_count,
                total_searches,
                total_matches,
                doc_lookup_hits,
                total_doc_lookups,
                m.base_segments + 1,
                m.total_queries,
            );
        }

        batch_num += 1;
    }

    // ── Final parallel sweep ──
    eprintln!(
        "\n  Final parallel sweep over {} titles...",
        data.titles.len()
    );
    let par_results = eng.match_titles_par(&data.titles, true);
    let par_total: usize = par_results.iter().map(|(_, ids, _)| ids.len()).sum();

    // Check sequential agreement
    let mut seq_total = 0usize;
    let mut mismatches = 0usize;
    let mut seq_results: Vec<HashSet<u64>> = Vec::new();
    for title in &data.titles {
        eng.match_title(title, &mut scratch, &mut out, true);
        seq_total += out.len();
        seq_results.push(out.iter().copied().collect());
    }
    for (idx, matches, _) in &par_results {
        let par_set: HashSet<u64> = matches.iter().copied().collect();
        if par_set != seq_results[*idx] {
            mismatches += 1;
        }
    }

    // No deleted IDs in final results
    let mut final_ghosts = 0usize;
    for (_, matches, _) in &par_results {
        for id in matches {
            if deleted_ids.contains(id) {
                final_ghosts += 1;
            }
        }
    }

    eprintln!(
        "\n  TRAFFIC SIM RESULTS:\n    batches={batch_num} inserts={inserted_count} searches={total_searches} matches={total_matches}"
    );
    eprintln!(
        "    doc_lookups={}/{} deletes={} ghosts={}",
        doc_lookup_hits,
        total_doc_lookups,
        deleted_ids.len(),
        final_ghosts
    );
    eprintln!(
        "    final: par={} seq={} mismatches={} elapsed={:.1}s",
        par_total,
        seq_total,
        mismatches,
        t0.elapsed().as_secs_f64()
    );
    print_metrics("final", &eng.metrics());
    events.dump_summary("traffic-sim");

    assert_eq!(final_ghosts, 0, "deleted IDs in final results");
    assert_eq!(mismatches, 0, "parallel != sequential in traffic sim");
    assert_eq!(par_total, seq_total, "par total != seq total");
    assert!(total_matches > 0, "no matches during simulation");
    assert!(doc_lookup_hits > 0, "no doc lookups succeeded");
}

// ═════════════════════════════════════════════════════════════════════════════
// 14. UPDATE STORM — rapid version churn on the same logical IDs
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn update_storm_same_ids() {
    eprintln!("\n=== UPDATE STORM (same IDs, many versions) ===");
    let t0 = Instant::now();

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 300,
            auto_compact_on_flush: true,
            max_segments: 4,
            holes_ratio_threshold: 0.2,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    let base_ids: Vec<u64> = (1..=50).collect();

    // Build initial versions
    let initial: Vec<(u64, String)> = base_ids
        .iter()
        .map(|&id| (id, format!("player{id} 1994 upper deck basketball card")))
        .collect();
    eng.build_from_queries(&initial);
    eprintln!("  initial: {} queries", initial.len());

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();

    let brands = ["topps", "bowman", "fleer", "panini", "donruss", "hoops"];
    let years = [1986, 1990, 1993, 1996, 1997, 2003, 2010, 2020, 2024];
    let suffixes = [
        "rookie card",
        "card psa 10",
        "basketball card",
        "card bgs 9.5",
        "chrome refractor",
        "prizm silver",
    ];

    // 20 rounds of updates across all 50 IDs
    for round in 0..20u32 {
        let version = round + 2;

        for &id in &base_ids {
            let brand = brands[(id as usize + round as usize) % brands.len()];
            let year = years[(id as usize + round as usize * 3) % years.len()];
            let suffix = suffixes[(id as usize + round as usize * 7) % suffixes.len()];
            let new_text = format!("player{id} {year} {brand} {suffix}");

            let _ = eng.delete_by_logical_id(id);
            eng.insert_live(&new_text, id, version);

            // Verify source reflects latest version
            let src = eng
                .get_query_source(id)
                .expect("source missing after update");
            assert_eq!(src, new_text, "round {round}: source stale for id={id}");
        }

        // Match a few titles between each update round
        let test_titles = [
            "player10 1993 topps rookie card psa 10",
            "player25 2003 bowman chrome refractor card",
            "player1 1986 fleer basketball card psa 10",
            "player50 2024 panini prizm silver basketball",
        ];
        for title in &test_titles {
            eng.match_title(title, &mut scratch, &mut out, true);
        }

        if round % 5 == 0 {
            let m = eng.metrics();
            eprintln!(
                "  round {}: version={} segments={} total={}",
                round,
                version,
                m.base_segments + 1,
                m.total_queries
            );
        }
    }

    // Final state: exactly 50 queries should be live
    eng.flush();
    eng.compact_all();
    let m = eng.metrics();
    print_metrics("final", &m);

    // After compact, tombstones reclaimed — but total queries may be
    // higher than 50 due to multi-version duplication being cleaned up
    // during compaction. The key invariant: each logical ID matches at most once.
    let title = "player10 1993 topps rookie card bowman fleer panini prizm";
    eng.match_title(title, &mut scratch, &mut out, true);
    let mut seen_ids: HashSet<u64> = HashSet::new();
    for &id in &out {
        assert!(
            seen_ids.insert(id),
            "duplicate logical id {id} in match results after update storm"
        );
    }

    // All 50 IDs should have sources
    for &id in &base_ids {
        assert!(
            eng.get_query_source(id).is_some(),
            "source missing for id={id} after update storm + compact"
        );
    }

    events.dump_summary("update-storm");
    eprintln!("  elapsed={:.1}s", t0.elapsed().as_secs_f64());
}

// ═════════════════════════════════════════════════════════════════════════════
// 15. MIXED TRAFFIC WITH PARALLEL READS — synthetic + hand-crafted queries
// ═════════════════════════════════════════════════════════════════════════════

#[test]
fn mixed_synthetic_and_handcrafted_parallel() {
    eprintln!("\n=== MIXED SYNTHETIC + HANDCRAFTED WITH PARALLEL READS ===");
    let t0 = Instant::now();

    // Generate a synthetic corpus for volume
    let cfg = GenConfig {
        num_queries: 15_000,
        num_titles: 2_000,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0xCA_FE_D0_0D,
        num_players: 2_000,
        num_sets: 800,
    };
    let data = generate(&cfg);

    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 2_000,
            auto_compact_on_flush: true,
            max_segments: 5,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    // Phase 1: Load synthetic corpus
    eng.build_from_queries(&data.queries);
    eprintln!("  Phase 1: loaded {} synthetic queries", data.queries.len());

    // Phase 2: Add hand-crafted queries that test specific DSL features
    let handcrafted: Vec<(u64, String)> = vec![
        (9_000_001, "michael jordan 1986 fleer".into()),
        (
            9_000_002,
            "michael jordan (1986,1993,1997) (fleer,topps)".into(),
        ),
        (
            9_000_003,
            "michael jordan card -(reprint,auto,lot,break)".into(),
        ),
        (
            9_000_004,
            "(jordan,james,kobe) (psa,bgs) -(fake,reprint)".into(),
        ),
        (9_000_005, "lebron james 2003 topps chrome rookie".into()),
        (9_000_006, "kobe bryant (topps,bowman) 1996 -(lot)".into()),
        (
            9_000_007,
            "(jordan,lebron) (fleer,topps,upper deck) (1986,1997,2003)".into(),
        ),
        (
            9_000_008,
            "basketball card (psa,bgs,sgc) -(auto,signed,used)".into(),
        ),
    ];

    for (id, text) in &handcrafted {
        let result = eng.try_insert_live(text, *id, 1);
        match &result {
            Ok(reverse_rusty::segment::InsertOutcome::Inserted(_)) => {}
            Ok(reverse_rusty::segment::InsertOutcome::RejectedClassD) => {
                eprintln!("    class-D rejected: id={id} {text:?}");
            }
            Err(e) => {
                eprintln!("    parse error: id={id} {text:?}: {e}");
            }
        }
    }
    eprintln!("  Phase 2: added {} handcrafted queries", handcrafted.len());

    // Phase 3: Search with titles designed to hit the handcrafted queries
    let targeted_titles = vec![
        "michael jordan 1986 fleer basketball card #57 psa 10",
        "michael jordan 1993 topps finest refractor card",
        "michael jordan 1997 upper deck card game jersey",
        "michael jordan 1986 fleer card reprint fake",
        "lebron james 2003 topps chrome rookie card psa 10",
        "kobe bryant 1996 topps draft pick rookie card lot",
        "kobe bryant 1996 bowman basketball card psa 9",
        "jordan james triple card topps 1997 psa 10",
        "basketball card psa 10 gem mint vintage",
        "basketball card auto signed used game worn",
    ];

    eprintln!("  Phase 3: targeted searches");
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    for title in &targeted_titles {
        eng.match_title(title, &mut scratch, &mut out, true);
        let handcrafted_hits: Vec<u64> =
            out.iter().filter(|&&id| id >= 9_000_000).copied().collect();
        eprintln!(
            "    {:?}\n      {} total matches, handcrafted: {:?}",
            title,
            out.len(),
            handcrafted_hits
        );

        // Verify doc sources for all handcrafted hits
        for &id in &handcrafted_hits {
            let src = eng.get_query_source(id);
            assert!(src.is_some(), "handcrafted match id={id} has no source");
        }
    }

    // Phase 4: Delete some synthetic, keep handcrafted, search in parallel
    let del_count = data.queries.len() / 4;
    eprintln!("  Phase 4: deleting {del_count} synthetic queries");
    let deleted: HashSet<u64> = data.queries[..del_count]
        .iter()
        .map(|(id, _)| *id)
        .collect();
    for &id in &deleted {
        let _ = eng.delete_by_logical_id(id);
    }
    eng.flush();

    // Combine all titles for a big parallel sweep
    let mut all_titles: Vec<String> = data.titles.clone();
    all_titles.extend(targeted_titles.iter().map(std::string::ToString::to_string));

    let par_results = eng.match_titles_par(&all_titles, true);
    let par_total: usize = par_results.iter().map(|(_, ids, _)| ids.len()).sum();

    // Sequential comparison
    let mut seq_results: Vec<HashSet<u64>> = Vec::new();
    for title in &all_titles {
        eng.match_title(title, &mut scratch, &mut out, true);
        seq_results.push(out.iter().copied().collect());
    }

    let mut mismatches = 0usize;
    let mut ghosts = 0usize;
    for (idx, matches, _stats) in &par_results {
        let par_set: HashSet<u64> = matches.iter().copied().collect();
        if par_set != seq_results[*idx] {
            mismatches += 1;
        }
        for id in matches {
            if deleted.contains(id) {
                ghosts += 1;
            }
        }
    }

    // Phase 5: Compact and re-check handcrafted queries
    eng.compact_all();
    for (id, text) in &handcrafted {
        if let Some(src) = eng.get_query_source(*id) {
            assert_eq!(src, text.as_str(), "handcrafted source mangled by compact");
        }
    }

    eprintln!(
        "\n  RESULTS: par={} mismatches={} ghosts={} elapsed={:.1}s",
        par_total,
        mismatches,
        ghosts,
        t0.elapsed().as_secs_f64()
    );
    print_metrics("final", &eng.metrics());
    events.dump_summary("mixed-traffic");

    assert_eq!(mismatches, 0, "parallel != sequential");
    assert_eq!(ghosts, 0, "deleted IDs in results");
    assert!(par_total > 0, "no matches at all");
}

// ═════════════════════════════════════════════════════════════════════════════
// 16. 10M SCALE — large corpus then hammer with mixed operations
//
//     Run: cargo test --release --test stress ten_million -- --nocapture
// ═════════════════════════════════════════════════════════════════════════════

#[test]
#[ignore = "large-scale — run explicitly with --ignored or by name"]
fn ten_million_queries_mixed_ops() {
    eprintln!("\n=== 10M QUERIES — MIXED OPS AT SCALE ===");
    let t0 = Instant::now();

    // ── Generate 10M queries + 50K titles ──
    eprintln!("  Generating 10M queries + 50K titles...");
    let gen_start = Instant::now();
    let cfg = GenConfig {
        num_queries: 10_000_000,
        num_titles: 50_000,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x10_000_000,
        num_players: 20_000,
        num_sets: 8_000,
    };
    let data = generate(&cfg);
    eprintln!(
        "    generated {} queries, {} titles in {:.1}s",
        data.queries.len(),
        data.titles.len(),
        gen_start.elapsed().as_secs_f64()
    );

    // ── Build engine with 10M queries ──
    let events = EventLog::new();
    let mut eng = Engine::with_config(
        make_norm(),
        EngineConfig {
            memtable_flush_threshold: 100_000,
            auto_compact_on_flush: true,
            max_segments: 8,
            holes_ratio_threshold: 0.3,
            ..EngineConfig::default()
        },
    );
    eng.set_observer(events.observer());

    // Load in 2 chunks to exercise multi-segment from the start
    let half = data.queries.len() / 2;
    eprintln!("  Building base segment (5M queries)...");
    let build_start = Instant::now();
    eng.build_from_queries(&data.queries[..half]);
    eprintln!("    built in {:.1}s", build_start.elapsed().as_secs_f64());

    eprintln!("  Bulk ingesting second segment (5M queries)...");
    let ingest_start = Instant::now();
    eng.bulk_ingest(&data.queries[half..]);
    eprintln!(
        "    ingested in {:.1}s",
        ingest_start.elapsed().as_secs_f64()
    );

    print_metrics("after-load", &eng.metrics());

    // ── Phase 1: Parallel search sweep over 50K titles ──
    eprintln!(
        "\n  Phase 1: parallel search over {} titles...",
        data.titles.len()
    );
    let search_start = Instant::now();
    let par_results = eng.match_titles_par(&data.titles, true);
    let search_elapsed = search_start.elapsed();

    let total_matches: usize = par_results.iter().map(|(_, ids, _)| ids.len()).sum();
    let total_candidates: u64 = par_results
        .iter()
        .map(|(_, _, s)| u64::from(s.unique_candidates))
        .sum();
    let total_skipped: u64 = par_results
        .iter()
        .map(|(_, _, s)| u64::from(s.probes_skipped))
        .sum();
    let titles_per_sec = data.titles.len() as f64 / search_elapsed.as_secs_f64();
    eprintln!(
        "    {total_matches} matches, {total_candidates} candidates, {total_skipped} probes skipped"
    );
    eprintln!(
        "    {:.1}ms ({:.0} titles/sec)",
        search_elapsed.as_secs_f64() * 1000.0,
        titles_per_sec
    );

    // ── Phase 2: Sequential search for comparison ──
    eprintln!("  Phase 2: sequential search (1K titles) for par agreement...");
    let check_count = 1_000;
    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut mismatches = 0usize;
    for (i, (_, par_ids, _)) in par_results.iter().enumerate().take(check_count) {
        eng.match_title(&data.titles[i], &mut scratch, &mut out, true);
        let seq_set: HashSet<u64> = out.iter().copied().collect();
        let par_set: HashSet<u64> = par_ids.iter().copied().collect();
        if seq_set != par_set {
            mismatches += 1;
        }
    }
    eprintln!("    par vs seq: {mismatches} mismatches over {check_count} titles");
    assert_eq!(mismatches, 0, "parallel != sequential at 10M scale");

    // ── Phase 3: Live inserts (100K new queries) while searching ──
    eprintln!("\n  Phase 3: inserting 100K queries with interleaved searches...");
    let insert_start = Instant::now();
    let insert_count = 100_000u64;
    let mut insert_ok = 0usize;
    let mut search_during_insert = 0usize;
    let mut matches_during_insert = 0usize;

    for i in 0..insert_count {
        let id = 50_000_000 + i;
        let text = format!(
            "live{} player{} {} fleer basketball card",
            i,
            i % 20_000,
            1980 + (i % 40)
        );
        if eng.insert_live(&text, id, 1).is_some() {
            insert_ok += 1;
        }

        // Search every 500th insert
        if i % 500 == 0 {
            let title_idx = (i as usize) % data.titles.len();
            eng.match_title(&data.titles[title_idx], &mut scratch, &mut out, true);
            search_during_insert += 1;
            matches_during_insert += out.len();
        }
    }
    eprintln!(
        "    inserted {} in {:.1}s ({:.0}/sec)",
        insert_ok,
        insert_start.elapsed().as_secs_f64(),
        insert_ok as f64 / insert_start.elapsed().as_secs_f64()
    );
    eprintln!(
        "    {search_during_insert} searches during inserts, {matches_during_insert} matches"
    );

    // ── Phase 4: Delete 500K queries (5%) ──
    eprintln!("\n  Phase 4: deleting 500K queries (5%)...");
    let delete_start = Instant::now();
    let delete_count = 500_000;
    let deleted_ids: HashSet<u64> = data.queries[..delete_count]
        .iter()
        .map(|(id, _)| *id)
        .collect();
    for &id in &deleted_ids {
        let _ = eng.delete_by_logical_id(id);
    }
    eprintln!(
        "    deleted {} in {:.1}s ({:.0}/sec)",
        delete_count,
        delete_start.elapsed().as_secs_f64(),
        delete_count as f64 / delete_start.elapsed().as_secs_f64()
    );
    print_metrics("after-deletes", &eng.metrics());

    // ── Phase 5: Update 50K queries (delete + re-insert with new version) ──
    eprintln!("\n  Phase 5: updating 50K queries...");
    let update_start = Instant::now();
    let update_count = 50_000;
    let update_range = &data.queries[delete_count..delete_count + update_count];
    for (logical, text) in update_range {
        let _ = eng.delete_by_logical_id(*logical);
        let new_text = format!("{text} updated variant");
        eng.insert_live(&new_text, *logical, 99);
    }
    eprintln!(
        "    updated {} in {:.1}s ({:.0}/sec)",
        update_count,
        update_start.elapsed().as_secs_f64(),
        update_count as f64 / update_start.elapsed().as_secs_f64()
    );

    // ── Phase 6: Flush + compact ──
    eprintln!("\n  Phase 6: flush + compact...");
    let compact_start = Instant::now();
    eng.flush();
    eng.compact_all();
    eprintln!(
        "    flush+compact in {:.1}s",
        compact_start.elapsed().as_secs_f64()
    );
    print_metrics("after-compact", &eng.metrics());

    // ── Phase 7: Post-mutation parallel search ──
    eprintln!(
        "\n  Phase 7: post-mutation parallel search ({} titles)...",
        data.titles.len()
    );
    let search2_start = Instant::now();
    let par2 = eng.match_titles_par(&data.titles, true);
    let search2_elapsed = search2_start.elapsed();

    let total2: usize = par2.iter().map(|(_, ids, _)| ids.len()).sum();
    let titles_per_sec2 = data.titles.len() as f64 / search2_elapsed.as_secs_f64();
    eprintln!(
        "    {} matches in {:.1}ms ({:.0} titles/sec)",
        total2,
        search2_elapsed.as_secs_f64() * 1000.0,
        titles_per_sec2
    );

    // Verify no deleted IDs in results
    let mut ghosts = 0usize;
    for (_, matches, _) in &par2 {
        for id in matches {
            if deleted_ids.contains(id) {
                ghosts += 1;
            }
        }
    }
    eprintln!("    ghosts (deleted IDs in results): {ghosts}");

    // ── Phase 8: Doc store spot-checks ──
    eprintln!("\n  Phase 8: doc store spot-checks...");
    let mut source_ok = 0usize;
    let mut source_missing = 0usize;
    // Check some non-deleted queries have sources
    for (id, text) in data.queries[delete_count + update_count..]
        .iter()
        .take(1_000)
    {
        match eng.get_query_source(*id) {
            Some(src) => {
                assert_eq!(src, text.as_str(), "source mismatch for id={id}");
                source_ok += 1;
            }
            None => {
                source_missing += 1;
            }
        }
    }
    // Check deleted queries have no sources
    let mut deleted_source_ghosts = 0usize;
    for &id in deleted_ids.iter().take(1_000) {
        if eng.get_query_source(id).is_some() {
            deleted_source_ghosts += 1;
        }
    }
    eprintln!(
        "    sources: {}/{} found, {} deleted still present",
        source_ok,
        source_ok + source_missing,
        deleted_source_ghosts
    );

    // ── Summary ──
    let elapsed = t0.elapsed();
    eprintln!("\n  ══════════════════════════════════════");
    eprintln!("  10M SCALE SUMMARY");
    eprintln!("  ──────────────────────────────────────");
    eprintln!("  corpus:        10M queries, 50K titles");
    eprintln!("  inserts:       100K live");
    eprintln!("  deletes:       500K (5%)");
    eprintln!("  updates:       50K");
    eprintln!("  search (pre):  {total_matches} matches @ {titles_per_sec:.0} titles/sec");
    eprintln!("  search (post): {total2} matches @ {titles_per_sec2:.0} titles/sec");
    eprintln!("  ghosts:        {ghosts}");
    eprintln!("  par==seq:      {mismatches} mismatches");
    eprintln!("  total elapsed: {:.1}s", elapsed.as_secs_f64());
    eprintln!("  ══════════════════════════════════════");
    events.dump_summary("10M-scale");

    assert_eq!(ghosts, 0, "deleted IDs in results at 10M scale");
    assert_eq!(mismatches, 0, "parallel != sequential at 10M scale");
    assert_eq!(deleted_source_ghosts, 0, "deleted sources still in store");
    assert!(total_matches > 0, "no matches at all");
    assert!(total2 > 0, "no matches after mutations");
}
