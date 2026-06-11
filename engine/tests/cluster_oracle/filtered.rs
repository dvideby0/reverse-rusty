//! Filtered percolation through the cluster (ADR-049/055): a tag-carrying corpus held to the
//! same frozen-`TagDict` filter semantics as the single-node engine + brute oracle, plus the
//! live-tagged-add guard and the tagged vocabulary rebuild (ADR-074: tags — interned and
//! post-freeze synthetic — are carried through `set_vocab` by stored `TagId`).

use crate::harness::*;
use reverse_rusty::cluster::{ClusterConfig, ClusterEngine};
use reverse_rusty::segment::{Engine, MatchScratch};
use std::collections::HashSet;

/// Filtered percolation through the cluster (ADR-049/055): a tag-carrying corpus, swept across
/// K∈{1,3,8,16} × RF∈{1,2} × a filter set, must agree with BOTH the single-node engine's
/// `compile_tag_predicate` + `match_title_filtered` AND the brute oracle's matches-that-pass-the-
/// filter — zero false negatives, zero false positives — and a filter must only ever REMOVE
/// (filtered ⊆ unfiltered), never drop a wanted match. Proves the shared frozen `TagDict` keeps a
/// stored tag and a request filter resolving to the same `TagId` across every shard.
#[test]
fn filtered_percolation_matches_single_node_and_oracle() {
    let (queries, titles) = build_corpus();
    let tags = tags_parallel(&queries);

    // Single-node reference built WITH tags + the brute oracle (both K-independent).
    let brute = Brute::build(&queries);
    let mut reference = Engine::new(vocab());
    reference
        .try_build_from_queries_with_tags(&queries, &tags)
        .expect("tagged single-node build");
    let ref_snap = reference.snapshot();

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let mut blc = String::new();
    let mut bfeats = Vec::new();

    // Precompute, per title: the unfiltered truth + the single-node filtered set per filter.
    let truth: Vec<HashSet<u64>> = titles
        .iter()
        .map(|t| brute.matches(t, &mut blc, &mut bfeats))
        .collect();

    let mut nonempty = 0usize;
    for &k in &[1usize, 3, 8, 16] {
        for &rf in &[1usize, 2] {
            let cfg = ClusterConfig {
                num_shards: k,
                replication_factor: rf,
                include_broad: true,
                ..ClusterConfig::default()
            };
            let cluster = ClusterEngine::build_with_tags(vocab(), &cfg, &queries, &tags)
                .expect("tagged cluster build");

            for (ti, title) in titles.iter().enumerate() {
                let unfiltered: HashSet<u64> =
                    cluster.percolate(title).unwrap().into_iter().collect();
                for filter in filters_for(ti) {
                    // cluster (the system under test)
                    let got: HashSet<u64> = cluster
                        .percolate_filtered(title, &filter)
                        .unwrap()
                        .into_iter()
                        .collect();

                    // single-node engine filtered (same frozen-tag-space resolution)
                    let pred = ref_snap.compile_tag_predicate(&filter);
                    ref_snap.match_title_filtered(title, &mut s, &mut out, true, &pred);
                    let ref_filtered: HashSet<u64> = out.iter().copied().collect();

                    // brute reference = truth matches that also satisfy the filter
                    let brute_filtered: HashSet<u64> = truth[ti]
                        .iter()
                        .copied()
                        .filter(|l| passes_filter(&tags_for(*l), &filter))
                        .collect();

                    assert_eq!(
                        got, brute_filtered,
                        "K={k} RF={rf}: cluster filtered vs brute oracle (title {ti}, filter {filter:?})"
                    );
                    assert_eq!(
                        got, ref_filtered,
                        "K={k} RF={rf}: cluster filtered vs single-node (title {ti}, filter {filter:?})"
                    );
                    // Monotonicity: filtering only removes; every removed id truly fails the filter.
                    assert!(
                        got.is_subset(&unfiltered),
                        "K={k} RF={rf}: filter added a match not in the unfiltered set"
                    );
                    for removed in unfiltered.difference(&got) {
                        assert!(
                            !passes_filter(&tags_for(*removed), &filter),
                            "K={k} RF={rf}: filter removed id {removed} that satisfies it (false negative)"
                        );
                    }
                    if !got.is_empty() {
                        nonempty += 1;
                    }
                }
            }
        }
    }
    assert!(nonempty > 0, "degenerate: no filter ever matched anything");
}

/// A tagged query added LIVE (post-build, via `add_query_with_tags`) is filterable just like a
/// build-time one — and a tag first seen after the dict froze resolves to a consistent synthetic
/// `TagId` across shards (ADR-046/055), so a filter on it still narrows correctly with zero FN.
#[test]
fn live_tagged_add_is_filterable_with_post_freeze_tag() {
    let cfg = ClusterConfig {
        num_shards: 4,
        include_broad: true,
        ..ClusterConfig::default()
    };
    // Build untagged so the tag space is empty + frozen; the live adds introduce all tags as
    // post-freeze (synthetic) ids — the strongest cross-shard-consistency case.
    let seed = vec![(1u64, "1994 topps".to_string())];
    let cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("build");

    // Two class-A live adds on distinct rare anchors (distinct shards), with distinct categories.
    cluster
        .add_query_with_tags(
            100,
            "zzrarealpha",
            &[("category".to_string(), "cards".to_string())],
        )
        .expect("tagged live add");
    cluster
        .add_query_with_tags(
            200,
            "zzrarebeta",
            &[("category".to_string(), "coins".to_string())],
        )
        .expect("tagged live add");

    let cards = vec![("category".to_string(), vec!["cards".to_string()])];
    let coins = vec![("category".to_string(), vec!["coins".to_string()])];

    // Each title matches its query unfiltered; the category filter keeps only the matching one.
    let a_un: HashSet<u64> = cluster
        .percolate("zzrarealpha")
        .unwrap()
        .into_iter()
        .collect();
    assert!(a_un.contains(&100), "unfiltered must match the live add");
    let a_cards: HashSet<u64> = cluster
        .percolate_filtered("zzrarealpha", &cards)
        .unwrap()
        .into_iter()
        .collect();
    assert!(
        a_cards.contains(&100),
        "the cards filter keeps the cards-tagged live add (zero FN on a synthetic tag)"
    );
    let a_coins: HashSet<u64> = cluster
        .percolate_filtered("zzrarealpha", &coins)
        .unwrap()
        .into_iter()
        .collect();
    assert!(
        !a_coins.contains(&100),
        "the coins filter removes the cards-tagged add"
    );
    // The other query, filtered to its own category.
    assert!(cluster
        .percolate_filtered("zzrarebeta", &coins)
        .unwrap()
        .contains(&200));
}

/// A vocab change on a cluster that received tags ONLY via live adds (post-freeze *synthetic*
/// tags, never interned into `tag_dict` — the ids that have NO recoverable string) carries those
/// tags through the blue/green rebuild verbatim (ADR-074). This is the exact scenario ADR-055
/// deferred by refusing: the rebuild now gathers each query's stored `TagId`s alongside its DSL,
/// so a filter on the synthetic tag still narrows correctly after the rebuild — zero FN.
#[test]
fn set_vocab_carries_live_synthetic_tags_through_rebuild() {
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..ClusterConfig::default()
    };
    // Built UNTAGGED ⇒ tag_dict stays empty + frozen ⇒ every live tag below is synthetic.
    let seed = vec![(1u64, "1994 topps".to_string())];
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("build");
    cluster
        .add_query_with_tags(
            100,
            "zzrarelivetag",
            &[("category".to_string(), "cards".to_string())],
        )
        .expect("tagged live add");
    cluster
        .add_query_with_tags(
            200,
            "zzotherlivetag",
            &[("category".to_string(), "coins".to_string())],
        )
        .expect("tagged live add");

    // The vocab change is unrelated to the tagged queries — the rebuild must not disturb them.
    let mut v = reverse_rusty::vocab::Vocab::new();
    v.add_synonym(
        "zzabbr",
        "term:zzcanon",
        reverse_rusty::dict::FeatureKind::Generic,
    );
    let rebuilt = cluster.set_vocab(v).expect(
        "set_vocab must succeed on a cluster holding live synthetic tags (ADR-074 carry-through)",
    );
    assert_eq!(rebuilt, 3, "the seed + both live adds rebuild");

    let cards = vec![("category".to_string(), vec!["cards".to_string()])];
    let coins = vec![("category".to_string(), vec!["coins".to_string()])];
    // Each query still matches unfiltered, still passes its OWN tag's filter (the synthetic id
    // was carried, not dropped), and is still removed by the other filter (it was carried
    // correctly, not smeared).
    assert!(cluster.percolate("zzrarelivetag").unwrap().contains(&100));
    assert!(
        cluster
            .percolate_filtered("zzrarelivetag", &cards)
            .unwrap()
            .contains(&100),
        "a post-freeze synthetic tag must survive the vocab rebuild (filtered-read recall)"
    );
    assert!(
        !cluster
            .percolate_filtered("zzrarelivetag", &coins)
            .unwrap()
            .contains(&100),
        "the carried tag must still EXCLUDE under a non-matching filter"
    );
    assert!(cluster
        .percolate_filtered("zzotherlivetag", &coins)
        .unwrap()
        .contains(&200));
    assert!(!cluster
        .percolate_filtered("zzotherlivetag", &cards)
        .unwrap()
        .contains(&200));

    // The learn paths delegate to the same rebuild — they too now work on a tagged
    // cluster (a second full rebuild; the carried tags survive again).
    cluster
        .learn_and_apply(2)
        .expect("learn_and_apply must succeed on a tagged cluster (ADR-074)");
    assert!(
        cluster
            .percolate_filtered("zzrarelivetag", &cards)
            .unwrap()
            .contains(&100),
        "tags survive a second (learn-driven) rebuild"
    );
}

/// The tagged vocabulary rebuild, held to the full differential gate (ADR-074): a tag-carrying
/// corpus (interned build-time tags + a post-freeze synthetic live add), swept across K, after a
/// declared alias `set_vocab` — cluster filtered ≡ brute-with-tags (zero FN/FP), filtered ⊆
/// unfiltered, and a tagged query whose extraction CHANGES under the alias keeps its tags on
/// whichever shard re-placement chose.
#[test]
fn set_vocab_preserves_tags_through_tagged_rebuild() {
    let (mut queries, titles) = build_corpus();
    // A tagged query written in the ALIAS surface form: post-alias its extraction (and
    // possibly its anchor/shard) changes, so its tags must travel with the re-placement.
    let q_alias = 9_300_001u64;
    queries.push((q_alias, "1994 fleer zzabbr".into()));
    let tags = tags_parallel(&queries);

    // The live add's tag key never appears in the corpus tags ⇒ guaranteed synthetic.
    let live_id = 9_300_002u64;
    let live_dsl = "zzrareliveq";
    let live_tag = || vec![("region".to_string(), "emea".to_string())];

    let make_vocab = || {
        let mut v = reverse_rusty::vocab::Vocab::new();
        v.add_synonym(
            "zzabbr",
            "term:zzcanon",
            reverse_rusty::dict::FeatureKind::Generic,
        );
        v
    };
    let tag_of = |l: u64| {
        if l == live_id {
            live_tag()
        } else {
            tags_for(l)
        }
    };

    // Alias-aware independent ground truth over the full live set (corpus + live add).
    let mut all = queries.clone();
    all.push((live_id, live_dsl.to_string()));
    let brute = Brute::build_with_vocab(&all, make_vocab().to_normalizer().unwrap());
    let mut blc = String::new();
    let mut bfeats = Vec::new();

    let title_canon = "1994 fleer zzcanon psa 10";
    for &k in &[1usize, 3, 8] {
        let cfg = ClusterConfig {
            num_shards: k,
            include_broad: true,
            ..ClusterConfig::default()
        };
        let mut cluster = ClusterEngine::build_with_tags(vocab(), &cfg, &queries, &tags)
            .expect("tagged cluster build");
        cluster
            .add_query_with_tags(live_id, live_dsl, &live_tag())
            .expect("synthetic-tagged live add");

        let rebuilt = cluster.set_vocab(make_vocab()).expect("tagged set_vocab");
        assert!(
            rebuilt > 100,
            "K={k}: the rebuild covers the whole live corpus (got {rebuilt})"
        );

        // The alias-form tagged query: the canonical-form title now matches it (the vocab
        // change took effect), and its interned tags survived the re-extraction + re-placement.
        let q_alias_filter = vec![("category".to_string(), vec![tag_of(q_alias)[0].1.clone()])];
        let got: HashSet<u64> = cluster
            .percolate_filtered(title_canon, &q_alias_filter)
            .unwrap()
            .into_iter()
            .collect();
        assert!(
            got.contains(&q_alias),
            "K={k}: the re-placed alias query must keep its tags (filtered-read recall)"
        );

        // The synthetic-tagged live add survives the rebuild.
        let region = vec![("region".to_string(), vec!["emea".to_string()])];
        assert!(
            cluster
                .percolate_filtered(live_dsl, &region)
                .unwrap()
                .contains(&live_id),
            "K={k}: a post-freeze synthetic tag must survive the rebuild"
        );

        // Full differential over a title sample: cluster filtered ≡ brute-with-tags,
        // filtered ⊆ unfiltered, every removed id truly fails the filter.
        for (ti, title) in titles
            .iter()
            .map(String::as_str)
            .take(60)
            .chain([title_canon, live_dsl])
            .enumerate()
        {
            let truth = brute.matches(title, &mut blc, &mut bfeats);
            let unfiltered: HashSet<u64> = cluster.percolate(title).unwrap().into_iter().collect();
            assert_eq!(
                unfiltered, truth,
                "K={k}: post-rebuild unfiltered ≠ alias-aware oracle (title {ti})"
            );
            for filter in filters_for(ti) {
                let got: HashSet<u64> = cluster
                    .percolate_filtered(title, &filter)
                    .unwrap()
                    .into_iter()
                    .collect();
                let brute_filtered: HashSet<u64> = truth
                    .iter()
                    .copied()
                    .filter(|l| passes_filter(&tag_of(*l), &filter))
                    .collect();
                assert_eq!(
                    got, brute_filtered,
                    "K={k}: post-rebuild filtered ≠ oracle (title {ti}, filter {filter:?})"
                );
                assert!(
                    got.is_subset(&unfiltered),
                    "K={k}: filter added a match post-rebuild (title {ti})"
                );
            }
        }
    }
}
