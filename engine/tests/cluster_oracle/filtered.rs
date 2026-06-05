//! Filtered percolation through the cluster (ADR-049/055): a tag-carrying corpus held to the
//! same frozen-`TagDict` filter semantics as the single-node engine + brute oracle, plus the
//! live-tagged-add and set_vocab-refusal guards.

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

/// A vocab change on a cluster that received tags ONLY via live adds (post-freeze *synthetic* tags,
/// never interned into `tag_dict`) must still be REFUSED (ADR-055). `tag_dict` emptiness is not a
/// sufficient proxy — the `tags_present` latch catches it. Otherwise the blue/green rebuild (which
/// reconstructs queries from DSL alone) would silently drop those tags → a filtered-read recall loss.
#[test]
fn set_vocab_refused_after_live_synthetic_tagged_add() {
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..ClusterConfig::default()
    };
    // Built UNTAGGED ⇒ tag_dict stays empty + frozen.
    let seed = vec![(1u64, "1994 topps".to_string())];
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("build");
    // A live tagged add whose tag is post-freeze ⇒ a synthetic TagId, NOT interned into tag_dict.
    cluster
        .add_query_with_tags(
            100,
            "zzrarelivetag",
            &[("category".to_string(), "cards".to_string())],
        )
        .expect("tagged live add");
    // The guard must fire even though tag_dict is still empty (the latch saw the tagged write).
    let res = cluster.set_vocab(reverse_rusty::vocab::Vocab::default());
    assert!(
        res.is_err(),
        "set_vocab must be refused on a cluster holding live synthetic tags, got {res:?}"
    );
    // Sanity: an UNTAGGED cluster still allows set_vocab (the latch stays false).
    let mut plain = ClusterEngine::build(vocab(), &cfg, &seed).expect("build plain");
    assert!(
        plain
            .set_vocab(reverse_rusty::vocab::Vocab::default())
            .is_ok(),
        "set_vocab must still work on a genuinely untagged cluster"
    );
}
