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

/// Claims 1 + 2 on the generated corpus: θ-on ≡ brute (broad on), and θ-on ≡
/// θ-off byte-identically for BOTH `include_broad` modes, per-title and batch —
/// the visibility-invariance proof (H stays default-visible, C stays opt-in).
#[test]
fn hot_tier_differential_and_visibility_invariance() {
    let data = gen_corpus(0x0407_7E57);
    let eng_hot = build_multi(&data.queries, cfg_theta(THETA));
    let eng_off = build_multi(&data.queries, cfg_theta(0));

    // Non-degenerate: a genuine A/H mix under θ, and NO class H at θ=0.
    let cc = eng_hot.class_counts();
    let cc0 = eng_off.class_counts();
    assert!(
        cc[4] > 0,
        "θ={THETA} produced no class H — degenerate corpus"
    );
    assert!(cc[0] > 0, "θ={THETA} left no class A — pick a larger θ");
    assert_eq!(cc0[4], 0, "θ=0 must never classify class H");
    // θ moves queries ONLY between the always-visible lanes: A+H is conserved
    // and the visibility-affecting boundaries (B pair, C, D) are θ-invariant.
    assert_eq!(cc[0] + cc[4], cc0[0], "A+H must be conserved across θ");
    assert_eq!(cc[1], cc0[1], "class B is θ-invariant");
    assert_eq!(
        cc[2], cc0[2],
        "class C is θ-invariant (visibility contract)"
    );
    assert_eq!(cc[3], cc0[3]);

    // Correctness: θ-on ≡ brute with the broad lane on (the full match set).
    let brute = Brute::build(&data.queries);
    let hot_broad = per_title_sets(&eng_hot, &data.titles, true);
    assert_no_fn_fp(&hot_broad, &brute, &data.titles, "θ-on per-title broad-on");

    // Visibility invariance: θ-on ≡ θ-off for both include_broad modes.
    for include_broad in [false, true] {
        let a = per_title_sets(&eng_hot, &data.titles, include_broad);
        let b = per_title_sets(&eng_off, &data.titles, include_broad);
        assert_eq!(
            a, b,
            "θ changed per-title results (include_broad={include_broad})"
        );
        // Batch ≡ scalar with the hot tier on. include_broad=false is the
        // load-bearing cell: the hot columnar pass must run (and agree) even
        // with the broad lane off.
        for bs in [1usize, 64, 256] {
            let bat = batch_sets(&eng_hot, &data.titles, include_broad, bs);
            assert_eq!(
                bat, a,
                "θ-on batch != scalar (include_broad={include_broad}, bs={bs})"
            );
        }
    }

    // The hot tier actually carries traffic on this corpus (meter proof).
    let stats = eng_hot
        .snapshot()
        .match_titles_batch_stats(&data.titles, BatchMatchOptions::default());
    assert!(
        stats.hot_batches > 0 && stats.hot_postings_scanned > 0,
        "hot columnar pass never ran on an H-bearing corpus"
    );
}

/// Claim 1 across the durable boundary: the `.seg` v5 hot section round-trips —
/// a flushed θ-on corpus reopens (mmap-attached) and still ≡ brute AND ≡ its
/// pre-reopen self on both visibility modes, with identical class counts
/// (classification is a pure function of the persisted dict + config).
#[test]
fn durable_reopen_preserves_hot_tier_exactly() {
    let dir = tempdir("reopen");
    let data = gen_corpus(0x0407_D15C);
    let pre_broad;
    let pre_sel;
    let pre_counts;
    {
        let mut cfg = cfg_theta(THETA);
        cfg.data_dir = Some(dir.clone());
        let mut eng =
            Engine::open(Normalizer::default_vocab().expect("vocab"), cfg).expect("open durable");
        eng.build_from_queries(&data.queries[..data.queries.len() / 2]);
        eng.bulk_ingest(&data.queries[data.queries.len() / 2..]);
        eng.flush();
        pre_counts = eng.class_counts();
        assert!(pre_counts[4] > 0, "degenerate: no class H sealed");
        pre_broad = per_title_sets(&eng, &data.titles, true);
        pre_sel = per_title_sets(&eng, &data.titles, false);
    }
    let mut cfg = cfg_theta(THETA);
    cfg.data_dir = Some(dir.clone());
    let eng =
        Engine::open(Normalizer::default_vocab().expect("vocab"), cfg).expect("reopen durable");
    assert_eq!(eng.class_counts(), pre_counts, "class counts drifted");
    assert_eq!(
        per_title_sets(&eng, &data.titles, true),
        pre_broad,
        "broad-on results drifted across reopen"
    );
    assert_eq!(
        per_title_sets(&eng, &data.titles, false),
        pre_sel,
        "broad-off results drifted across reopen"
    );
    let brute = Brute::build(&data.queries);
    assert_no_fn_fp(
        &per_title_sets(&eng, &data.titles, true),
        &brute,
        &data.titles,
        "reopened θ-on",
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// Claim 2's replay corollary: a WAL tail written θ-on and replayed θ-off is
/// RESULT-identical — the A↔H flip is benign — while the class counts drift
/// exactly by the reclassified tail (asserted explicitly, so the benign
/// divergence is a documented fact, not an accident).
#[test]
fn wal_tail_replay_under_flipped_theta_is_result_identical() {
    let dir = tempdir("flip");
    let data = gen_corpus(0x0407_F11B);
    // The un-flushed WAL tail: 200 identical single-token queries. Their anchor's
    // frequency grows 1..=200 across the inserts, so the early copies classify A
    // and — once freq crosses θ — the later ones classify H: the tail
    // deterministically holds BOTH classes, and a dedicated title below proves
    // every copy stays visible across the θ-flip replay.
    let tail: Vec<(u64, String)> = (0..200u64)
        .map(|i| (2_000_000 + i, "walfliptok".to_string()))
        .collect();
    let mut titles = data.titles.clone();
    titles.push("walfliptok listing".to_string());
    let pre_broad;
    let pre_sel;
    let sealed_h;
    let pre_h;
    {
        let mut cfg = cfg_theta(THETA);
        cfg.data_dir = Some(dir.clone());
        let mut eng =
            Engine::open(Normalizer::default_vocab().expect("vocab"), cfg).expect("open durable");
        eng.build_from_queries(&data.queries);
        eng.flush();
        sealed_h = eng.class_counts()[4];
        assert!(sealed_h > 0, "degenerate: no class H sealed");
        // The live tail stays in the WAL (no flush): these replay on reopen.
        for (id, text) in &tail {
            eng.insert_live(text, *id, 1);
        }
        pre_h = eng.class_counts()[4];
        assert!(pre_h > sealed_h, "degenerate: no class H in the WAL tail");
        pre_broad = per_title_sets(&eng, &titles, true);
        pre_sel = per_title_sets(&eng, &titles, false);
        // The flip-sensitive population actually matches its title.
        let last = pre_sel.last().expect("title present");
        assert!(
            (0..200u64).all(|i| last.contains(&(2_000_000 + i))),
            "the WAL-tail queries must match their constructed title"
        );
    }
    // Reopen θ=0: the sealed v5 segments keep their recorded classes (the knob
    // gates classification of NEW compiles, never visibility of stored entries);
    // the WAL tail re-compiles under θ=0 and lands in class A instead — and the
    // MATCH RESULTS cannot tell the difference.
    let mut cfg = cfg_theta(0);
    cfg.data_dir = Some(dir.clone());
    let eng = Engine::open(Normalizer::default_vocab().expect("vocab"), cfg).expect("reopen θ=0");
    assert_eq!(
        per_title_sets(&eng, &titles, true),
        pre_broad,
        "θ-flip replay changed broad-on results"
    );
    assert_eq!(
        per_title_sets(&eng, &titles, false),
        pre_sel,
        "θ-flip replay changed broad-off results"
    );
    let cc = eng.class_counts();
    assert_eq!(
        cc[4], sealed_h,
        "sealed class-H entries must keep their recorded class; only the \
         replayed tail reclassifies"
    );
    assert!(cc[4] < pre_h, "the tail's H entries replayed as A (benign)");
    std::fs::remove_dir_all(&dir).ok();
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

/// D5: an any-of group with NO top-64 member but ≥1 θ-hot member classifies H —
/// the WHOLE group anchors in the hot index (one index per query) — and stays
/// default-visible.
#[test]
fn mixed_anyof_group_classifies_hot_and_stays_default_visible() {
    let (mut queries, mut id) = masked_filler_corpus(200);
    // Group members: freq 100 each — θ-hot at θ=50, never top-64 (the fillers
    // above hold every mask bit at freq ≥ 200).
    for tok in ["alphax", "betax", "gammax", "deltax"] {
        for _ in 0..100 {
            queries.push((id, format!("{tok} uniq{id}")));
            id += 1;
        }
    }
    // The two-group queries: cover via one group, verify the other.
    let group_base = id;
    for _ in 0..8 {
        queries.push((id, "(alphax,betax) (gammax,deltax)".to_string()));
        id += 1;
    }
    let titles: Vec<String> = vec![
        "alphax gammax listing".into(), // matches (one member of each group)
        "betax deltax listing".into(),  // matches
        "alphax only listing".into(),   // group 2 unsatisfied -> no match
        "gammax only listing".into(),   // group 1 unsatisfied -> no match
        "fillertok3 uniq5 listing".into(),
    ];

    let eng_hot = build_multi(&queries, cfg_theta(50));
    let eng_off = build_multi(&queries, cfg_theta(0));
    assert!(
        eng_hot.class_counts()[4] >= 8,
        "the two-group queries must classify H (θ-hot members, no top-64)"
    );
    let brute = Brute::build(&queries);
    assert_no_fn_fp(
        &per_title_sets(&eng_hot, &titles, true),
        &brute,
        &titles,
        "mixed any-of θ-on",
    );
    for include_broad in [false, true] {
        assert_eq!(
            per_title_sets(&eng_hot, &titles, include_broad),
            per_title_sets(&eng_off, &titles, include_broad),
            "mixed any-of visibility changed under θ (include_broad={include_broad})"
        );
        // The group queries are ALWAYS visible: present with broad off too.
        let sets = per_title_sets(&eng_hot, &titles, include_broad);
        assert!(
            (group_base..group_base + 8).all(|g| sets[0].contains(&g)),
            "a hot any-of query went invisible (include_broad={include_broad})"
        );
    }
}

/// Claim 4: the observe-first counter under θ=0 predicts enforcement exactly —
/// `would_be_hot` (counted against `DEFAULT_HOT_ANCHOR_THETA`) equals the
/// stored class-H population when the default θ is turned on.
#[test]
fn would_be_hot_predicts_enforcement() {
    // SINGLE-token filler queries so each filler IS its query's rarest required
    // anchor. freq(wfiller i) = 1030 + 4i ≥ the default θ (1024) for every i;
    // the top 64 by frequency (i = 6..=69) take the mask bits (their queries
    // classify C — single top-64 anchors), so exactly the i = 0..=5 populations
    // are the would-be-hot class-A anchors.
    let mut queries: Vec<(u64, String)> = Vec::new();
    let mut id = 0u64;
    for i in 0..70u64 {
        for _ in 0..(1030 + 4 * i) {
            queries.push((id, format!("wfiller{i}")));
            id += 1;
        }
    }
    let expected_hot: u64 = (0..6u64).map(|i| 1030 + 4 * i).sum();

    // ONE build pass over the whole corpus: the mask finalizes against the
    // complete frequencies, so exactly fillers 6..=69 (the top 64) are masked —
    // deterministic, unlike a multi-phase build whose mask would freeze on the
    // first quarter's partial counts.
    let build_one = |theta: u32| {
        let mut eng = Engine::with_config(
            Normalizer::default_vocab().expect("vocab"),
            cfg_theta(theta),
        );
        eng.build_from_queries(&queries);
        eng
    };
    let eng_observe = build_one(0);
    let eng_enforce = build_one(DEFAULT_HOT_ANCHOR_THETA);

    assert_eq!(
        eng_observe.would_be_hot(),
        expected_hot,
        "observe counter must flag exactly the sub-mask θ-hot anchors"
    );
    assert_eq!(eng_observe.class_counts()[4], 0);
    assert_eq!(
        eng_enforce.class_counts()[4],
        eng_observe.would_be_hot(),
        "enforcement must move exactly the observed population"
    );
    // With θ on, the counter goes quiet (class H itself is the signal).
    assert_eq!(eng_enforce.would_be_hot(), 0);
}

/// Claim 3 on a CONTROLLED corpus: compaction is the migration seam, with
/// deterministic margins. `margintok` (freq 100, unmasked):
///   - built θ=0 ⇒ class A; a θ=50 re-anchoring merge PROMOTES A→H;
///   - re-merged at θ=150 the plan says A (100 < 150) but 100 > 150/2 = 75 ⇒
///     the margin BLOCKS the demotion (no merge-to-merge thrash);
///   - re-merged at θ=300 (100 ≤ 150) the demotion clears ⇒ H→A.
/// Results are identical at every step, on both visibility modes.
#[test]
fn compaction_migrates_main_to_hot_and_back_margin_gated() {
    let (mut queries, mut id) = masked_filler_corpus(200);
    // SINGLE-token queries so margintok IS the rarest required anchor
    // (freq 100, unmasked — the fillers above hold every mask bit).
    for _ in 0..100 {
        queries.push((id, "margintok".to_string()));
        id += 1;
    }
    let titles: Vec<String> = vec![
        "margintok something".into(),
        "fillertok9 uniq3".into(),
        "unrelated listing".into(),
    ];
    let mk = |theta: u32| EngineConfig {
        hot_anchor_threshold: theta,
        compaction_reanchor: true,
        auto_compact_on_flush: false,
        auto_compact_on_ingest: false,
        ..EngineConfig::default()
    };

    // Built θ=0: two base segments (build + bulk), everything main-lane.
    let mut eng = Engine::with_config(Normalizer::default_vocab().expect("vocab"), mk(0));
    let half = queries.len() / 2;
    eng.build_from_queries(&queries[..half]);
    eng.bulk_ingest(&queries[half..]);
    assert_eq!(eng.class_counts()[4], 0);
    let before_broad = per_title_sets(&eng, &titles, true);
    let before_sel = per_title_sets(&eng, &titles, false);
    // The margintok queries all match title 0 — the population under test.
    assert!(
        before_sel[0].len() >= 100,
        "corpus not matching as designed"
    );

    // ---- A→H at θ=50 ----
    eng.set_config(mk(50));
    let r1 = eng.compact_all().expect("first compaction");
    assert!(r1.hot_promoted >= 100, "margintok population must promote");
    assert_eq!(r1.hot_demoted, 0);
    let h_after_promote = eng.class_counts()[4];
    assert!(h_after_promote >= 100);
    assert_eq!(per_title_sets(&eng, &titles, true), before_broad);
    assert_eq!(per_title_sets(&eng, &titles, false), before_sel);

    // ---- margin band at θ=150: freq 100 ∈ (75, 150) ⇒ nothing moves ----
    eng.set_config(mk(150));
    eng.bulk_ingest(&queries[..8]); // a second segment so a merge happens
    let r2 = eng.compact_all().expect("second compaction");
    assert_eq!(
        r2.hot_demoted, 0,
        "margin band (freq > θ/2) must block demotion"
    );
    assert_eq!(per_title_sets(&eng, &titles, true), before_broad);
    assert_eq!(per_title_sets(&eng, &titles, false), before_sel);
    assert!(
        eng.class_counts()[4] >= h_after_promote,
        "H population shrank inside the margin band"
    );

    // ---- demotion clears at θ=300: freq 100 ≤ 150 ----
    eng.set_config(mk(300));
    eng.bulk_ingest(&queries[..8]);
    let r3 = eng.compact_all().expect("third compaction");
    assert!(r3.hot_demoted >= 100, "demotion must clear past the margin");
    assert_eq!(
        eng.class_counts()[4],
        0,
        "every hot entry demotes once θ dwarfs all frequencies"
    );
    assert_eq!(per_title_sets(&eng, &titles, true), before_broad);
    assert_eq!(per_title_sets(&eng, &titles, false), before_sel);
}

/// The per-merge work cap bounds lane moves, and repeated merges converge to
/// the same end state with results untouched at every intermediate step.
#[test]
fn migration_work_cap_bounds_per_merge_and_converges() {
    let (mut queries, mut id) = masked_filler_corpus(200);
    // SINGLE-token queries so captok IS the rarest required anchor (freq 40).
    for _ in 0..40 {
        queries.push((id, "captok".to_string()));
        id += 1;
    }
    let titles: Vec<String> = vec!["captok anything".into(), "fillertok8 uniq2".into()];
    let capped = |cap: usize| EngineConfig {
        hot_anchor_threshold: 30,
        hot_migration_max_moves: cap,
        compaction_reanchor: true,
        auto_compact_on_flush: false,
        auto_compact_on_ingest: false,
        ..EngineConfig::default()
    };

    let mut eng = Engine::with_config(Normalizer::default_vocab().expect("vocab"), capped(7));
    // Build θ-OFF first (capped(7) has θ=30 — so build under a θ=0 config, then
    // switch): everything starts in main.
    eng.set_config(EngineConfig {
        compaction_reanchor: true,
        auto_compact_on_flush: false,
        auto_compact_on_ingest: false,
        ..EngineConfig::default()
    });
    let half = queries.len() / 2;
    eng.build_from_queries(&queries[..half]);
    eng.bulk_ingest(&queries[half..]);
    assert_eq!(eng.class_counts()[4], 0);
    let before = per_title_sets(&eng, &titles, true);

    eng.set_config(capped(7));
    let mut rounds = 0usize;
    loop {
        eng.bulk_ingest(&queries[..4]); // ensure ≥2 segments each round
        let r = eng.compact_all().expect("capped compaction");
        assert!(
            r.hot_promoted <= 7,
            "work cap exceeded in one merge ({} > 7)",
            r.hot_promoted
        );
        rounds += 1;
        assert!(rounds < 200, "migration failed to converge");
        if r.hot_promoted == 0 {
            break;
        }
        // Intermediate states must be reader-correct throughout.
        assert_eq!(
            per_title_sets(&eng, &titles, true),
            before,
            "capped migration changed results mid-convergence"
        );
    }
    // Exactly the captok population migrates (the two-token filler queries
    // anchor on their unique tail token and never move), across several capped
    // merges, with results untouched.
    assert_eq!(eng.class_counts()[4], 40);
    assert_eq!(per_title_sets(&eng, &titles, true), before);
    assert!(rounds > 1, "cap never actually split the migration");
}

/// The C boundary is untouchable from both sides: a re-anchoring θ-on merge
/// keeps every class-C and class-B count intact (and broad-off results are
/// invariant through it — the extended demote guard's contract).
#[test]
fn compaction_never_moves_class_c_across() {
    let data = gen_corpus(0x0407_C0DE);
    let mut eng = build_multi(
        &data.queries,
        EngineConfig {
            hot_anchor_threshold: THETA,
            compaction_reanchor: true,
            auto_compact_on_flush: false,
            auto_compact_on_ingest: false,
            ..EngineConfig::default()
        },
    );
    let cc_before = eng.class_counts();
    assert!(cc_before[2] > 0, "degenerate: no class C to protect");
    assert!(cc_before[4] > 0, "degenerate: no class H in play");
    let sel_before = per_title_sets(&eng, &data.titles, false);
    let broad_before = per_title_sets(&eng, &data.titles, true);
    eng.flush();
    eng.compact_all().expect("compaction ran");
    let cc_after = eng.class_counts();
    assert_eq!(cc_after[2], cc_before[2], "class C count moved");
    assert_eq!(cc_after[3], cc_before[3], "class D count moved");
    assert_eq!(
        per_title_sets(&eng, &data.titles, false),
        sel_before,
        "broad-off results changed through a θ-on merge (a C-crossing FN)"
    );
    assert_eq!(
        per_title_sets(&eng, &data.titles, true),
        broad_before,
        "broad-on results changed through a θ-on merge"
    );
}

/// The messy-corpus pass (ADR-063 discipline): adversarial surface noise over
/// the θ-on engine still ≡ brute.
#[test]
fn messy_hot_corpus_differential() {
    let mut data = gen_corpus(0x0407_3E55);
    let mut rng = Rng::new(0x0407_3E55 ^ 0xA5A5);
    messify_dataset(&mut rng, &mut data, 0.8, 0.5);
    let eng = build_multi(&data.queries, cfg_theta(THETA));
    assert!(eng.class_counts()[4] > 0, "messy corpus lost its H mix");
    let brute = Brute::build(&data.queries);
    assert_no_fn_fp(
        &per_title_sets(&eng, &data.titles, true),
        &brute,
        &data.titles,
        "messy θ-on",
    );
}

/// Claim 5: a θ so high nothing classifies H leaves the hot tier structurally
/// free — identical probe counts to θ=0 (the skip-when-empty pin) and, of
/// course, identical results.
#[test]
fn hot_empty_is_free() {
    let data = gen_corpus(0x0407_F4EE);
    let eng_off = build_multi(&data.queries, cfg_theta(0));
    let eng_high = build_multi(&data.queries, cfg_theta(u32::MAX));
    assert_eq!(eng_high.class_counts()[4], 0);

    let mut s = MatchScratch::new();
    let mut out_a = Vec::new();
    let mut out_b = Vec::new();
    for include_broad in [false, true] {
        for t in data.titles.iter().take(200) {
            let st_off = eng_off.match_title(t, &mut s, &mut out_a, include_broad);
            let st_high = eng_high.match_title(t, &mut s, &mut out_b, include_broad);
            assert_eq!(out_a, out_b, "hot-empty engine diverged");
            assert_eq!(
                st_high.probes_attempted, st_off.probes_attempted,
                "an empty hot tier must add ZERO probes (include_broad={include_broad})"
            );
            assert_eq!(st_high.hot_postings_scanned, 0);
            assert_eq!(st_high.hot_candidates, 0);
        }
    }
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
/// class-H entries writes format v5 (the hot-index section) and its commit writes
/// manifest v5 (+ the recorded θ), so a pre-ADR-105 reader — which never probes
/// the hot index — fails loudly instead of silently serving without those
/// queries. Hot-free output stays v3 (or v4 under class D) byte-identically, and
/// a forged/corrupt class byte fails loud at open.
#[test]
fn hot_segments_write_the_v5_rollback_fence() {
    use reverse_rusty::storage::MmapSegment;
    let seg_path = |dir: &std::path::Path| -> std::path::PathBuf {
        std::fs::read_dir(dir.join("segments"))
            .expect("read segments dir")
            .filter_map(Result::ok)
            .map(|e| e.path())
            .find(|p| p.extension().is_some_and(|x| x == "seg"))
            .expect("a sealed segment file")
    };
    let seg_version = |dir: &std::path::Path| -> u32 {
        let bytes = std::fs::read(seg_path(dir)).expect("read segment");
        u32::from_le_bytes(bytes[4..8].try_into().expect("version word"))
    };
    let manifest_version = |dir: &std::path::Path| -> u32 {
        let bytes = std::fs::read(dir.join("manifest.bin")).expect("manifest");
        u32::from_le_bytes(bytes[4..8].try_into().expect("version word"))
    };
    let queries = tiny_hot_corpus();

    // ---- hot-bearing: .seg v5 + manifest v5 + the recorded θ ----
    let dir_hot = tempdir("fence-hot");
    {
        let mut cfg = cfg_theta(2);
        cfg.data_dir = Some(dir_hot.clone());
        let mut eng = Engine::open(Normalizer::default_vocab().expect("vocab"), cfg).expect("open");
        eng.build_from_queries(&queries);
        assert!(eng.class_counts()[4] > 0, "degenerate: no class H");
        eng.flush();
    }
    assert_eq!(seg_version(&dir_hot), 5, "hot segment must carry the fence");
    assert_eq!(manifest_version(&dir_hot), 5, "hot commit ⇒ manifest v5");
    let m = reverse_rusty::storage::read_manifest(&dir_hot.join("manifest.bin"))
        .expect("read manifest");
    assert!(m.hot_fence, "v5 reads back with the hot fence set");
    assert_eq!(m.hot_anchor_theta, 2, "the recorded θ round-trips");

    // ---- hot-free under the SAME θ knob: byte-identical v3 output ----
    let dir_plain = tempdir("fence-plain");
    {
        let mut cfg = cfg_theta(1_000_000); // nothing reaches θ
        cfg.data_dir = Some(dir_plain.clone());
        let mut eng = Engine::open(Normalizer::default_vocab().expect("vocab"), cfg).expect("open");
        eng.build_from_queries(&queries);
        assert_eq!(eng.class_counts()[4], 0);
        eng.flush();
    }
    assert_eq!(seg_version(&dir_plain), 3, "hot-free segments stay v3");
    assert_eq!(
        manifest_version(&dir_plain),
        3,
        "hot-free manifest stays v3"
    );

    // ---- the version ladder: hot outranks class D ----
    let dir_both = tempdir("fence-both");
    {
        let mut cfg = cfg_theta(2);
        cfg.accept_class_d = true;
        cfg.data_dir = Some(dir_both.clone());
        let mut eng = Engine::open(Normalizer::default_vocab().expect("vocab"), cfg).expect("open");
        eng.build_from_queries(&queries);
        eng.insert_live("-auto", 900_000, 1);
        eng.flush();
    }
    assert_eq!(seg_version(&dir_both), 5, "hot + class D ⇒ v5 (the ladder)");
    assert_eq!(manifest_version(&dir_both), 5);

    // ---- forged class bytes fail loud at open (never mis-bucketed) ----
    // A class byte above the version's ceiling: 5 in a v5 file…
    let forge = |src: &std::path::Path, dst_name: &str, mutate: &dyn Fn(&mut Vec<u8>)| {
        let mut bytes = std::fs::read(src).expect("read");
        mutate(&mut bytes);
        let body = bytes.len() - 4;
        let crc = reverse_rusty::storage::crc32(&bytes[..body]);
        bytes[body..].copy_from_slice(&crc.to_le_bytes());
        let dst = src.parent().expect("dir").join(dst_name);
        std::fs::write(&dst, &bytes).expect("write");
        dst
    };
    // The class array lives in the meta section: [count: u32][class bytes…],
    // located by the header's meta_off word (bytes 48..56) — forge INSIDE it.
    let forge_class = |bytes: &mut Vec<u8>, from: u8, to: u8| {
        let meta_off = u64::from_le_bytes(bytes[48..56].try_into().expect("meta_off")) as usize;
        let count =
            u32::from_le_bytes(bytes[meta_off..meta_off + 4].try_into().expect("count")) as usize;
        let arr = meta_off + 4;
        let pos = bytes[arr..arr + count]
            .iter()
            .position(|&b| b == from)
            .map(|p| p + arr)
            .expect("a class byte to forge");
        bytes[pos] = to;
    };
    let hot_seg = seg_path(&dir_hot);
    let forged5 = forge(&hot_seg, "forged5.seg", &|bytes: &mut Vec<u8>| {
        forge_class(bytes, 4, 5);
    });
    let err = MmapSegment::open(&forged5).expect_err("class byte 5 must fail loud");
    assert!(err.to_string().contains("cost-class byte"), "got: {err}");
    // …and a class byte 4 smuggled into a v3 file (whose ceiling is 3).
    let plain_seg = seg_path(&dir_plain);
    let forged4 = forge(&plain_seg, "forged4.seg", &|bytes: &mut Vec<u8>| {
        forge_class(bytes, 0, 4);
    });
    let err = MmapSegment::open(&forged4).expect_err("class byte 4 in v3 must fail loud");
    assert!(err.to_string().contains("cost-class byte"), "got: {err}");

    // ---- an unknown FUTURE manifest version fails Engine::open outright ----
    let mpath = dir_hot.join("manifest.bin");
    let mut mbytes = std::fs::read(&mpath).expect("manifest");
    mbytes[4..8].copy_from_slice(&9u32.to_le_bytes());
    let body = mbytes.len() - 4;
    let crc = reverse_rusty::storage::crc32(&mbytes[..body]);
    mbytes[body..].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(&mpath, &mbytes).expect("write");
    let mut cfg = cfg_theta(2);
    cfg.data_dir = Some(dir_hot.clone());
    let err = Engine::open(Normalizer::default_vocab().expect("vocab"), cfg)
        .expect_err("future manifest version must refuse to open");
    assert!(
        err.to_string().contains("unsupported manifest version"),
        "got: {err}"
    );

    for d in [dir_hot, dir_plain, dir_both] {
        std::fs::remove_dir_all(&d).ok();
    }
}
