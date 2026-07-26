use super::*;

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
    // The un-flushed WAL tail: 200 DISTINCT-body any-of queries all carrying
    // `walfliptok`, whose frequency grows 1..=200 across the inserts — the early
    // ones classify A and, once freq crosses θ, the later ones classify H (the
    // mixed-any-of rule: no top-64 member, worst member θ-hot): the tail
    // deterministically holds BOTH classes, and a dedicated title below proves
    // every query stays visible across the θ-flip replay. The unique `wrare{i}`
    // member keeps every body distinct — identical bodies would body-group under
    // dedup (ADR-106) and ADOPT the first insert's class A, leaving no H to flip
    // (that adoption behavior is pinned by `tests/oracle/dedup.rs`; this test
    // pins the θ-flip replay).
    let tail: Vec<(u64, String)> = (0..200u64)
        .map(|i| (2_000_000 + i, format!("(walfliptok,wrare{i})")))
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
