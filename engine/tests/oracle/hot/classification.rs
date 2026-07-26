use super::*;

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
