//! Cluster ranking (ADR-059/075): the coordinator compiles the `RankSpec` once against
//! the shared frozen tag space, each shard scores its own matched ids, and the merged
//! scored set must equal the single-node engine's `rank` over the same corpus — with
//! the ranked id set IDENTICAL to the unranked one (ranking reorders, never gates).

use crate::harness::*;
use reverse_rusty::cluster::{ClusterConfig, ClusterEngine};
use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty::RankSpec;
use std::collections::HashSet;

/// Corpus tags extended with a numeric `priority` tag on a slice of queries — interned
/// at build (priority scoring reads the tag's VALUE string via `key_value`, which only
/// an interned tag has; the synthetic boundary is pinned by
/// `synthetic_tags_boost_but_do_not_priority_score`).
fn ranked_tags_parallel(queries: &[(u64, String)]) -> Vec<Vec<(String, String)>> {
    queries
        .iter()
        .map(|(l, _)| {
            let mut tags = tags_for(*l);
            if l % 5 == 0 {
                tags.push(("priority".to_string(), (l % 97).to_string()));
            }
            tags
        })
        .collect()
}

fn rank_spec() -> RankSpec {
    RankSpec {
        priority_key: Some("priority".to_string()),
        boosts: vec![
            ("category".to_string(), "cards".to_string(), 1000),
            ("status".to_string(), "active".to_string(), 250),
        ],
    }
}

/// The cluster ranking differential: across K, for a sample of titles, the cluster's
/// scored set ≡ the single-node engine's `rank` over the same tagged corpus (same
/// ids, same scores), the ranked id set ≡ the unranked percolate (recall guard), and
/// the `(score desc, _id asc)` presentation order is byte-identical to single-node.
#[test]
fn cluster_ranking_matches_single_node_and_preserves_recall() {
    let (queries, titles) = build_corpus();
    let tags = ranked_tags_parallel(&queries);
    let spec = rank_spec();

    // Single-node tagged reference: the ranking ground truth.
    let mut reference = Engine::new(vocab());
    reference
        .try_build_from_queries_with_tags(&queries, &tags)
        .expect("tagged single-node build");
    let ref_snap = reference.snapshot();
    let cspec = ref_snap.compile_rank_spec(&spec);

    let mut s = MatchScratch::new();
    let mut out = Vec::new();

    let mut scored_nonzero = 0usize;
    for &k in &[1usize, 3, 8] {
        let cfg = ClusterConfig {
            num_shards: k,
            include_broad: true,
            ..ClusterConfig::default()
        };
        let cluster = ClusterEngine::build_with_tags(vocab(), &cfg, &queries, &tags)
            .expect("tagged cluster build");

        for (ti, title) in titles.iter().take(120).enumerate() {
            // Cluster: scored rows (sorted by id) + the unranked set.
            let (got, _stats) = cluster
                .percolate_filtered_ranked(title, &[], true, &spec)
                .expect("ranked percolate");
            let unranked: HashSet<u64> = cluster.percolate(title).unwrap().into_iter().collect();

            // Recall guard: ranking reorders, never gates — identical id set.
            let ranked_ids: HashSet<u64> = got.iter().map(|&(id, _)| id).collect();
            assert_eq!(
                ranked_ids, unranked,
                "K={k}: ranked id set must equal the unranked set (title {ti})"
            );

            // Score differential: cluster scored rows ≡ single-node rank over the
            // same matched set.
            ref_snap.match_title_filtered(
                title,
                &mut s,
                &mut out,
                true,
                &reverse_rusty::exact::TagPredicate::empty(),
            );
            let mut want = ref_snap.rank(&out, &cspec);
            want.sort_unstable_by_key(|&(id, _)| id);
            assert_eq!(
                got, want,
                "K={k}: cluster scores diverge from single-node (title {ti})"
            );

            // Presentation order: the same (score desc, _id asc) total order.
            let mut got_order = got.clone();
            got_order.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            let mut want_order = want;
            want_order.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            assert_eq!(got_order, want_order, "K={k}: order diverges (title {ti})");
            if got_order.iter().any(|&(_, s)| s != 0) {
                scored_nonzero += 1;
            }
        }
    }
    assert!(
        scored_nonzero > 0,
        "degenerate: no title ever produced a non-zero score"
    );
}

/// The synthetic-tag ranking boundary (ADR-075, pinned not hidden): a post-freeze tag
/// resolves to a synthetic id with NO stored string, so a BOOST on it fires (boost
/// matching is id-equality) but a synthetic `priority` tag contributes 0 (priority
/// needs the value STRING, which only an interned tag has). Single-node behaves
/// identically given the same frozen tag space.
#[test]
fn synthetic_tags_boost_but_do_not_priority_score() {
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..ClusterConfig::default()
    };
    // Untagged build ⇒ empty frozen tag dict ⇒ every live tag below is synthetic.
    let seed = vec![(1u64, "1994 topps".to_string())];
    let cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("build");
    cluster
        .add_query_with_tags(
            100,
            "zzrankalpha",
            &[
                ("region".to_string(), "emea".to_string()),
                ("priority".to_string(), "500".to_string()),
            ],
        )
        .expect("tagged live add");

    let spec = RankSpec {
        priority_key: Some("priority".to_string()),
        boosts: vec![("region".to_string(), "emea".to_string(), 70)],
    };
    let (got, _) = cluster
        .percolate_filtered_ranked("zzrankalpha", &[], true, &spec)
        .expect("ranked percolate");
    assert_eq!(
        got,
        vec![(100, 70)],
        "synthetic boost fires (id-equality); synthetic priority contributes 0 \
         (no recoverable value string)"
    );
}

/// The merge's dedup-keeps-any-copy assumption, pinned (review note): a selective
/// any-of query fans one logical id to SEVERAL shards, and the coordinator merge
/// dedups by id after an UNSTABLE sort — which copy survives is arbitrary, so the
/// invariant that makes that safe (copies are version-identical, hence score-equal)
/// must hold observably. A title containing both any-of members probes both holding
/// shards; the merged row's score must be the exact tag-derived value regardless of
/// which shard's copy won the dedup.
#[test]
fn fanned_out_copies_merge_with_one_exact_score() {
    let (mut queries, _titles) = build_corpus();
    // Pure any-of of two rare members ⇒ Selective placement on BOTH members' shards.
    let q_fan = 9_500_001u64;
    queries.push((q_fan, "(zzfanleft,zzfanright)".into()));
    let mut tags = ranked_tags_parallel(&queries);
    // Deterministic interned tags for the fanned query: boost(1000-eligible) + priority.
    *tags.last_mut().expect("tags for q_fan") = vec![
        ("category".to_string(), "cards".to_string()),
        ("priority".to_string(), "33".to_string()),
    ];

    for &k in &[3usize, 8] {
        let cfg = ClusterConfig {
            num_shards: k,
            include_broad: true,
            ..ClusterConfig::default()
        };
        let cluster = ClusterEngine::build_with_tags(vocab(), &cfg, &queries, &tags)
            .expect("tagged cluster build");

        // Both members in the title ⇒ both holding shards probed ⇒ two copies merge.
        let (scored, _) = cluster
            .percolate_filtered_ranked("zzfanleft zzfanright psa", &[], true, &rank_spec())
            .expect("ranked percolate");
        let rows: Vec<&(u64, i64)> = scored.iter().filter(|&&(id, _)| id == q_fan).collect();
        assert_eq!(
            rows.len(),
            1,
            "K={k}: exactly one merged row for the fanned id"
        );
        assert_eq!(
            rows[0].1,
            1000 + 33,
            "K={k}: the merged score is the exact tag-derived value (cards boost + priority), \
             whichever copy survived the dedup"
        );
    }
}

/// Ranking composes with a tag filter: the scored set is exactly the FILTERED set
/// (filter first, then score) — never a score for a filtered-out query.
#[test]
fn ranking_composes_with_filter() {
    let (queries, titles) = build_corpus();
    let tags = ranked_tags_parallel(&queries);
    let spec = rank_spec();
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build_with_tags(vocab(), &cfg, &queries, &tags)
        .expect("tagged cluster build");

    let filter = vec![("category".to_string(), vec!["cards".to_string()])];
    let mut nonempty = 0usize;
    for title in titles.iter().take(60) {
        let (scored, _) = cluster
            .percolate_filtered_ranked(title, &filter, true, &spec)
            .expect("ranked filtered percolate");
        let filtered: HashSet<u64> = cluster
            .percolate_filtered(title, &filter)
            .unwrap()
            .into_iter()
            .collect();
        let scored_ids: HashSet<u64> = scored.iter().map(|&(id, _)| id).collect();
        assert_eq!(
            scored_ids, filtered,
            "ranked+filtered id set must equal the filtered set"
        );
        nonempty += usize::from(!scored.is_empty());
    }
    assert!(nonempty > 0, "degenerate: filter never matched");
}
