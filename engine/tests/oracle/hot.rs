//! The hot-tier differential — the ADR-105 class-H oracle.
//!
//! A θ-hot-anchored query (deciding anchor past `hot_anchor_threshold` with no
//! top-64 mask bit) moves to the per-segment hot index: **probed on every
//! request** (always-visible, like main) but evaluated columnar on the batch
//! path (like broad). The load-bearing claims pinned here:
//!
//! 1. **Zero FN/FP vs brute** with the tier on, per-title AND batch, across
//!    durable reopen (the `.seg` v5 round-trip) — the correctness contract.
//! 2. **θ is visibility-invariant**: a θ-on engine returns byte-identical
//!    result sets to a θ-off engine for BOTH `include_broad` modes (class H
//!    stays default-visible; class C stays opt-in — the two-axis placement
//!    rule). This is also what makes θ-flip WAL replay benign.
//! 3. **Migration** (the ADR-056 re-anchor seam): compaction moves A↔H under
//!    the θ / θ÷2 margin gates and the per-merge work cap, never touching
//!    results, and never crossing C in either direction.
//! 4. **Observe-first ties to enforcement**: `would_be_hot` under θ=0 equals
//!    the stored class-H population once θ enforces.
//! 5. **Hot-empty is free**: with no θ-hot anchor the tier adds zero probes.

use crate::harness::*;
use reverse_rusty::config::{EngineConfig, DEFAULT_HOT_ANCHOR_THETA};
use reverse_rusty::gen::{generate, messify_dataset, Dataset, GenConfig, Rng};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{BatchMatchOptions, BroadStrategy, Engine, MatchScratch};
use std::collections::HashSet;

/// A θ that lands between the generated corpus's long-tail anchors and its
/// Zipf-head players at this scale, so the corpus classifies as a genuine A/H
/// mix (asserted, not assumed — see the non-degeneracy checks).
const THETA: u32 = 64;

fn gen_corpus(seed: u64) -> Dataset {
    generate(&GenConfig {
        num_queries: 20_000,
        num_titles: 2_000,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed,
        num_players: 2_000,
        num_sets: 1_000,
    })
}

fn cfg_theta(theta: u32) -> EngineConfig {
    EngineConfig {
        hot_anchor_threshold: theta,
        ..EngineConfig::default()
    }
}

/// Multi-segment engine: base + two bulks + a live memtable tail (the core
/// oracle's builder shape), under the given config.
fn build_multi(queries: &[(u64, String)], cfg: EngineConfig) -> Engine {
    let mut eng = Engine::with_config(Normalizer::default_vocab().expect("vocab"), cfg);
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

fn batch_sets(
    eng: &Engine,
    titles: &[String],
    include_broad: bool,
    bs: usize,
) -> Vec<HashSet<u64>> {
    let snap = eng.snapshot();
    let mut res = vec![HashSet::new(); titles.len()];
    for (idx, ids) in snap.match_titles_batch(
        titles,
        BatchMatchOptions {
            include_broad,
            broad_batch_size: bs,
            broad_strategy: BroadStrategy::Columnar,
            broad_materialize: true,
            broad_prefilter: true,
        },
    ) {
        res[idx] = ids.into_iter().collect();
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

fn tempdir(tag: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "rr-hot-{tag}-{}-{}-{}",
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

/// A constructed corpus with CONTROLLED anchor frequencies: 70 filler features
/// with descending frequencies own the 64 mask bits, leaving deliberate
/// θ-hot-but-unmasked anchors. Returns `(queries, next_id)`.
fn masked_filler_corpus(reps_base: u64) -> (Vec<(u64, String)>, u64) {
    let mut queries: Vec<(u64, String)> = Vec::new();
    let mut id = 0u64;
    // freq(fillertok i) = reps_base + 4*i; the top 64 (i = 6..=69) take the
    // mask bits, so i = 0..=5 are unmasked with freq ≥ reps_base.
    for i in 0..70u64 {
        for _ in 0..(reps_base + 4 * i) {
            queries.push((id, format!("fillertok{i} uniq{id}")));
            id += 1;
        }
    }
    (queries, id)
}

/// A corpus whose class-H population is deliberate at tiny scale: 70 single-token
/// filler populations with strictly ascending frequencies; the top 64 take the
/// mask (their queries classify C), leaving fillers 0..=5 unmasked — θ-hot at
/// any θ ≤ their frequency. Total ~2.5k queries.
fn tiny_hot_corpus() -> Vec<(u64, String)> {
    let mut queries = Vec::new();
    let mut id = 0u64;
    for i in 0..70u64 {
        for _ in 0..(2 + i) {
            queries.push((id, format!("tinytok{i}")));
            id += 1;
        }
    }
    queries
}

/// The ROLLBACK fence (the ADR-068 idiom, extended by ADR-105): a segment holding
/// class-H entries carries at least format v5 (the hot-index section), and its
/// commit carries at least manifest v5 (+ the recorded θ), so a pre-ADR-105
/// reader — which never probes the hot index — fails loudly instead of silently
/// serving without those queries. Later cumulative formats may outrank v5; a
/// forged/corrupt class byte still fails loud at open.
mod classification;
mod format;
mod migration;
mod routing;
mod safety;
