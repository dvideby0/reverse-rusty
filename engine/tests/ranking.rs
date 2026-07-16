//! Engine-level ranking tests (ADR-059): the post-match `EngineSnapshot::rank`
//! scorer + `compile_rank_spec`, including newest-live-copy tag precedence and the
//! recall guard (ranking reorders the matched set, never changes its membership).
//!
//! The HTTP-surface behavior (response `_score`, `from`/`size` pagination, per-slot
//! truncation, byte-identical unranked path) is covered by the co-located handler
//! tests in `src/bin/server/handlers/search.rs`. The pure scorer arithmetic is unit
//! tested in `src/rank.rs`.

use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty::{
    EngineSnapshot, Normalizer, QueryScope, RankProgramSpec, RankSpec, RankValues, TopKOptions,
    TotalHits,
};

fn norm() -> Normalizer {
    Normalizer::default_vocab().expect("default vocab")
}

fn tag(k: &str, v: &str) -> (String, String) {
    (k.to_string(), v.to_string())
}

fn boost(k: &str, v: &str, w: i64) -> (String, String, i64) {
    (k.to_string(), v.to_string(), w)
}

/// Match a title, returning the matched logical ids (sorted ascending, as the
/// engine dedups them).
fn matched(snap: &EngineSnapshot, title: &str) -> Vec<u64> {
    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    snap.match_title(title, &mut s, &mut out, true);
    out.sort_unstable();
    out
}

/// Score + order ids exactly as the REST handler does: (score desc, id asc).
fn ranked_ids(snap: &EngineSnapshot, ids: &[u64], spec: &RankSpec) -> Vec<(u64, i64)> {
    let compiled = snap.compile_rank_spec(spec);
    let mut scored = snap.rank(ids, &compiled);
    scored.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    scored
}

#[test]
fn ranks_by_priority_then_boost_additively() {
    let mut eng = Engine::new(norm());
    // Three queries that all match the title below, with distinct priority + tier tags.
    eng.insert_live_with_tags(
        "topps chrome",
        1,
        1,
        &[tag("priority", "10"), tag("tier", "gold")],
    );
    eng.insert_live_with_tags("topps chrome", 2, 1, &[tag("priority", "50")]);
    eng.insert_live_with_tags("topps chrome", 3, 1, &[tag("tier", "gold")]);
    let snap = eng.snapshot();
    let ids = matched(&snap, "2020 topps chrome update");
    assert_eq!(ids, vec![1, 2, 3], "all three queries match the title");

    // priority only: 2 (50) > 1 (10) > 3 (0).
    let spec = RankSpec {
        priority_key: Some("priority".into()),
        boosts: vec![],
    };
    assert_eq!(
        ranked_ids(&snap, &ids, &spec),
        vec![(2, 50), (1, 10), (3, 0)]
    );

    // boost tier=gold by 100, no priority: 1 & 3 tie at 100 (id asc breaks the tie), 2 = 0.
    let spec = RankSpec {
        priority_key: None,
        boosts: vec![boost("tier", "gold", 100)],
    };
    assert_eq!(
        ranked_ids(&snap, &ids, &spec),
        vec![(1, 100), (3, 100), (2, 0)]
    );

    // additive priority + boost: 1 = 10+100, 3 = 0+100, 2 = 50 → order 1, 3, 2.
    let spec = RankSpec {
        priority_key: Some("priority".into()),
        boosts: vec![boost("tier", "gold", 100)],
    };
    assert_eq!(
        ranked_ids(&snap, &ids, &spec),
        vec![(1, 110), (3, 100), (2, 50)]
    );
}

#[test]
fn ranking_never_changes_the_matched_set() {
    // The recall guard: ranking only reorders the already-final id set — it may
    // never add or drop a match. Compare the ranked id SET to the raw matched set.
    let mut eng = Engine::new(norm());
    eng.insert_live_with_tags("topps chrome", 1, 1, &[tag("priority", "10")]);
    eng.insert_live_with_tags("topps chrome", 2, 1, &[]); // untagged → priority 0
    eng.insert_live_with_tags("topps chrome", 3, 1, &[tag("priority", "999")]);
    let snap = eng.snapshot();
    let ids = matched(&snap, "2020 topps chrome update");

    let spec = RankSpec {
        priority_key: Some("priority".into()),
        boosts: vec![boost("tier", "gold", 100)],
    };
    let mut got: Vec<u64> = ranked_ids(&snap, &ids, &spec)
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    got.sort_unstable();
    assert_eq!(got, ids, "ranking preserves the exact matched set");
}

#[test]
fn rank_uses_newest_live_copy_tags() {
    // An update is "insert new version + tombstone old", but the low-level insert
    // does NOT tombstone, so after a flush the logical id is alive in BOTH a base
    // segment (v1, priority 1) and the memtable (v2, priority 9). `tags_for_logical`
    // must pick the NEWEST live copy (memtable), so the score is 9, not 1.
    let mut eng = Engine::new(norm());
    eng.insert_live_with_tags("topps chrome", 1, 1, &[tag("priority", "1")]);
    eng.flush(); // bake v1 into a base segment (still alive — no tombstone)
    eng.insert_live_with_tags("topps chrome", 1, 2, &[tag("priority", "9")]);
    let snap = eng.snapshot();
    let ids = matched(&snap, "2020 topps chrome update");
    assert_eq!(ids, vec![1], "the logical id dedups to a single hit");

    let spec = RankSpec {
        priority_key: Some("priority".into()),
        boosts: vec![],
    };
    assert_eq!(
        ranked_ids(&snap, &ids, &spec),
        vec![(1, 9)],
        "the memtable (newest) copy's priority wins over the base copy"
    );
}

#[test]
fn rank_uses_newest_copy_within_one_container() {
    // Two live copies of the SAME logical id inside ONE container — as a re-PUT /
    // re-insert leaves them before the old copy is tombstoned. `locals_for_logical`
    // lists them oldest-first, so ranking must take the LAST (newest) live local,
    // not the first. (This is the common server PUT-update path.)
    let mut eng = Engine::new(norm());
    eng.insert_live_with_tags("topps chrome", 1, 1, &[tag("priority", "1")]);
    eng.insert_live_with_tags("topps chrome", 1, 2, &[tag("priority", "9")]);
    let spec = RankSpec {
        priority_key: Some("priority".into()),
        boosts: vec![],
    };

    // Both copies live in the memtable.
    let snap = eng.snapshot();
    let ids = matched(&snap, "2020 topps chrome update");
    assert_eq!(ids, vec![1], "deduped to a single hit");
    assert_eq!(
        ranked_ids(&snap, &ids, &spec),
        vec![(1, 9)],
        "newest copy in the same memtable wins, not the oldest"
    );

    // After a flush both copies live in ONE base segment — same requirement.
    eng.flush();
    let snap = eng.snapshot();
    assert_eq!(
        ranked_ids(&snap, &matched(&snap, "2020 topps chrome update"), &spec),
        vec![(1, 9)],
        "newest copy in the same base segment wins after flush"
    );
}

#[test]
fn rank_scores_unknown_id_zero() {
    let mut eng = Engine::new(norm());
    eng.insert_live_with_tags("topps chrome", 1, 1, &[tag("priority", "5")]);
    let snap = eng.snapshot();
    let spec = snap.compile_rank_spec(&RankSpec {
        priority_key: Some("priority".into()),
        boosts: vec![],
    });
    // A logical id that was never inserted has no live tags → score 0 (never panics).
    assert_eq!(snap.rank(&[999], &spec), vec![(999, 0)]);
}

#[test]
fn bounded_typed_top_k_matches_collect_all_full_sort() {
    let mut eng = Engine::new(norm());
    let rows = [
        (1, -5, "silver"),
        (2, 50, "gold"),
        (3, 50, "silver"),
        (4, 0, "gold"),
        (5, i64::MAX, "gold"),
    ];
    for (id, priority, tier) in rows {
        let tags = vec![tag("priority", &priority.to_string()), tag("tier", tier)];
        eng.try_insert_live_ranked("topps chrome", id, 1, &tags, Some(RankValues { priority }))
            .expect("typed insert");
    }
    // Two physical copies of logical 2: the newest-live priority must win no
    // matter which row emits the match first.
    eng.flush();
    eng.try_insert_live_ranked(
        "topps chrome",
        2,
        2,
        &[tag("priority", "75"), tag("tier", "gold")],
        Some(RankValues { priority: 75 }),
    )
    .expect("newest copy");

    let snap = eng.snapshot();
    let raw = RankProgramSpec {
        priority_field: Some("priority".into()),
        boosts: vec![boost("tier", "gold", 100)],
    };
    let program = snap.compile_rank_program(&raw).expect("compile typed rank");
    let options = TopKOptions {
        size: 3,
        track_total_hits_up_to: 3,
        query_scope: QueryScope::Standard,
    };
    let mut scratch = MatchScratch::new();
    let got = snap
        .try_match_title_top_k(
            "2020 topps chrome update",
            options,
            &program,
            &reverse_rusty::exact::TagPredicate::empty(),
            &mut scratch,
            None,
        )
        .expect("bounded top-k");

    let ids = matched(&snap, "2020 topps chrome update");
    let compat = RankSpec {
        priority_key: Some("priority".into()),
        boosts: vec![boost("tier", "gold", 100)],
    };
    let mut expected = snap.rank(&ids, &snap.compile_rank_spec(&compat));
    expected.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    expected.truncate(3);
    assert_eq!(
        got.hits
            .iter()
            .map(|hit| (hit.logical_id, hit.score))
            .collect::<Vec<_>>(),
        expected
    );
    assert_eq!(got.total_hits, TotalHits::lower_bound(3));
    assert_eq!(got.stats.matches, 3, "stats carries the threshold value");
    assert_eq!(got.hits[0].score, i64::MAX, "addition saturates");
}

#[test]
fn bounded_top_k_honors_filters_size_zero_and_field_errors() {
    let mut eng = Engine::new(norm());
    eng.insert_live_with_tags(
        "topps chrome",
        1,
        1,
        &[tag("priority", "9"), tag("tenant", "a")],
    );
    eng.insert_live_with_tags(
        "topps chrome",
        2,
        1,
        &[tag("priority", "99"), tag("tenant", "b")],
    );
    let snap = eng.snapshot();
    assert!(snap
        .compile_rank_program(&RankProgramSpec {
            priority_field: Some("unknown".into()),
            boosts: vec![],
        })
        .is_err());
    let program = snap
        .compile_rank_program(&RankProgramSpec::default())
        .expect("program");
    let pred = snap.compile_tag_predicate(&[("tenant".into(), vec!["a".into()])]);
    let mut scratch = MatchScratch::new();
    let got = snap
        .try_match_title_top_k(
            "topps chrome",
            TopKOptions {
                size: 0,
                track_total_hits_up_to: 10,
                query_scope: QueryScope::Standard,
            },
            &program,
            &pred,
            &mut scratch,
            None,
        )
        .expect("count-only top-k");
    assert!(got.hits.is_empty());
    assert_eq!(got.total_hits, TotalHits::exact(1));
    assert_eq!(got.rank_stats.evaluations, 0);
}

#[test]
fn bounded_differential_spans_every_cost_lane_scope_k_and_threshold() {
    fn word(prefix: &str, n: usize) -> String {
        let a = char::from(b'a' + u8::try_from((n / 26) % 26).expect("letter"));
        let b = char::from(b'a' + u8::try_from(n % 26).expect("letter"));
        format!("{prefix}{a}{b}")
    }

    let anchors: Vec<String> = (0..64).map(|i| word("zzcommon", i)).collect();
    let mut queries = Vec::new();
    let mut next_id = 1u64;
    for (anchor_index, anchor) in anchors.iter().enumerate() {
        for repetition in 0..3 {
            let filler = word("zzfiller", anchor_index * 3 + repetition);
            queries.push((next_id, format!("{anchor} {filler}")));
            next_id += 1;
        }
    }
    // Top-64 singleton = C, two top-64 terms = B, θ-hot rank-65 term = H,
    // rare singleton = A, and negation-only = D.
    queries.push((next_id, anchors[0].clone()));
    next_id += 1;
    queries.push((next_id, format!("{} {}", anchors[1], anchors[2])));
    next_id += 1;
    queries.push((next_id, "zzhotterm".into()));
    next_id += 1;
    queries.push((next_id, "zzhotterm".into()));
    next_id += 1;
    queries.push((next_id, "zzrareterm".into()));
    next_id += 1;
    queries.push((next_id, "-zzbanned".into()));

    let tags: Vec<Vec<(String, String)>> = queries
        .iter()
        .map(|(id, _)| {
            vec![
                tag("priority", &((*id as i64 % 17) - 8).to_string()),
                tag("tier", if id % 3 == 0 { "gold" } else { "plain" }),
            ]
        })
        .collect();
    let mut engine = Engine::with_config(
        norm(),
        reverse_rusty::config::EngineConfig {
            accept_class_d: true,
            hot_anchor_threshold: 2,
            ..reverse_rusty::config::EngineConfig::default()
        },
    );
    engine
        .try_build_from_queries_with_tags(&queries, &tags)
        .expect("five-lane build");
    let snapshot = engine.snapshot();
    let counts = snapshot.class_counts();
    assert!(
        counts.iter().all(|&count| count > 0),
        "fixture must span A/B/C/D/H, got {counts:?}"
    );

    let rank_spec = RankSpec {
        priority_key: Some("priority".into()),
        boosts: vec![boost("tier", "gold", 100)],
    };
    let compat = snapshot.compile_rank_spec(&rank_spec);
    let program = snapshot
        .compile_rank_program(&RankProgramSpec {
            priority_field: Some("priority".into()),
            boosts: rank_spec.boosts.clone(),
        })
        .expect("typed program");
    let title = format!(
        "zzrareterm zzhotterm {} {} {}",
        anchors[0], anchors[1], anchors[2]
    );
    for scope in [QueryScope::Standard, QueryScope::WithBroad] {
        let mut scratch = MatchScratch::new();
        let mut ids = Vec::new();
        snapshot.match_title_filtered(
            &title,
            &mut scratch,
            &mut ids,
            scope == QueryScope::WithBroad,
            &reverse_rusty::exact::TagPredicate::empty(),
        );
        let mut fully_ranked = snapshot.rank(&ids, &compat);
        fully_ranked.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        for &size in &[0usize, 1, 3, 10, 100] {
            for &threshold in &[0u64, 1, 3, 100] {
                let mut ranked_scratch = MatchScratch::new();
                let got = snapshot
                    .try_match_title_top_k(
                        &title,
                        TopKOptions {
                            size,
                            track_total_hits_up_to: threshold,
                            query_scope: scope,
                        },
                        &program,
                        &reverse_rusty::exact::TagPredicate::empty(),
                        &mut ranked_scratch,
                        None,
                    )
                    .expect("bounded differential");
                let mut expected = fully_ranked.clone();
                expected.truncate(size);
                assert_eq!(
                    got.hits
                        .iter()
                        .map(|hit| (hit.logical_id, hit.score))
                        .collect::<Vec<_>>(),
                    expected,
                    "scope={scope:?} size={size} threshold={threshold}"
                );
                let expected_total = if u64::try_from(ids.len()).unwrap_or(u64::MAX) > threshold {
                    TotalHits::lower_bound(threshold)
                } else {
                    TotalHits::exact(u64::try_from(ids.len()).unwrap_or(u64::MAX))
                };
                assert_eq!(got.total_hits, expected_total);
            }
        }
    }
}
