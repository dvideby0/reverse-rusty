//! Multi-shard differential oracle — the CONTRACT verification for clustering.
//!
//! The acceptance gate for the in-process multi-shard core. For a synthetic
//! corpus plus hand-injected coverage queries, it asserts that the cluster
//! returns EXACTLY the single-node result set AND exactly the independent
//! brute-force oracle's set, across shard counts {1, 3, 8, 16} and broad on/off:
//!   * ZERO false negatives  (every true match is returned)  <-- the hard requirement
//!   * ZERO false positives  (per-shard exact verify is exact; union dedups)
//!
//! The brute-force matcher uses its own independent Dict/Normalizer so it cannot
//! share a bug with the engine or the cluster. The generated corpus is class A
//! (rare-anchored families) + class C (broad); the generator never emits any-of
//! or all-hot-required queries, so we inject those to exercise class-B any-of
//! (multi-shard placement) and class-B arity-2 (the replicated lane), plus
//! multi-entity titles to exercise multi-shard fan-out.

use reverse_rusty::cluster::{AddOutcome, ClusterConfig, ClusterEngine};
use reverse_rusty::compile::{extract, Extracted};
use reverse_rusty::dict::Dict;
use reverse_rusty::gen::{generate, GenConfig, BRANDS};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};
use std::collections::HashSet;

fn vocab() -> Normalizer {
    Normalizer::default_vocab().expect("built-in vocab")
}

/// Independent ground-truth matcher over extracted queries (copied structure from
/// `tests/oracle.rs` — deliberately shares nothing with the engine or cluster).
struct Brute {
    norm: Normalizer,
    dict: Dict,
    queries: Vec<(u64, Extracted)>,
}

impl Brute {
    fn build(queries: &[(u64, String)]) -> Self {
        let norm = vocab();
        let mut dict = Dict::new();
        let mut lc = String::new();
        let mut qs = Vec::new();
        for (logical, text) in queries {
            if let Ok(ast) = reverse_rusty::dsl::parse(text) {
                let ex = extract(&ast, &norm, &mut dict, &mut lc);
                if ex.required.is_empty() && ex.anyof.is_empty() {
                    continue; // mirror class-D rejection
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

/// Build the test corpus: a generated base (class A + C) plus injected coverage.
/// Returns `(queries, titles)`. Injected logical ids start above the generated
/// range so nothing collides.
fn build_corpus() -> (Vec<(u64, String)>, Vec<String>) {
    let cfg = GenConfig {
        num_queries: 12_000,
        num_titles: 1_200,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x0CEA_5ADE,
        num_players: 2_000,
        num_sets: 800,
    };
    let data = generate(&cfg);
    let mut queries = data.queries;
    let mut titles = data.titles;
    let mut next_id = queries.iter().map(|(id, _)| *id).max().unwrap_or(0) + 1;

    // class-B any-of: pure any-of of two RARE players (no required term, so the
    // any-of cover path fires). "rareplayerN" appears only here -> non-hot.
    for i in 0..150u64 {
        queries.push((next_id, format!("(rareplayer{i},rareplayer{})", i + 1000)));
        next_id += 1;
    }
    // class-B arity-2: all-hot required (year + brand), no rare anchor -> the
    // replicated lane.
    for i in 0..100u64 {
        let year = 1986 + (i % 39);
        let brand = BRANDS[(i % BRANDS.len() as u64) as usize];
        queries.push((next_id, format!("{year} {brand}")));
        next_id += 1;
    }
    // a few class-A queries anchored on the injected rare players, so multi-entity
    // titles below actually match something across shards.
    for i in 0..150u64 {
        let year = 1986 + (i % 39);
        let brand = BRANDS[(i % BRANDS.len() as u64) as usize];
        queries.push((next_id, format!("{year} {brand} rareplayer{i}")));
        next_id += 1;
    }

    // multi-entity titles: two rare players (both in the dict via the any-of
    // queries) -> fan out to two selective shards plus the replicated lane.
    for i in 0..200u64 {
        let year = 1986 + (i % 39);
        let brand = BRANDS[(i % BRANDS.len() as u64) as usize];
        let a = i % 150;
        titles.push(format!(
            "{year} {brand} rareplayer{a} rareplayer{} psa 10",
            a + 1000
        ));
    }

    (queries, titles)
}

#[test]
fn cluster_matches_single_node_and_oracle() {
    let (queries, titles) = build_corpus();

    // Reference (single-node) and oracle are K-independent — build once. The
    // reference uses build_from_queries over the WHOLE corpus in one pass, so its
    // dict mask is finalized over the same feature distribution as the cluster's
    // authoritative dict (otherwise hot-mask divergence could legitimately differ
    // classifications).
    let brute = Brute::build(&queries);
    let mut reference = Engine::new(vocab());
    reference.build_from_queries(&queries);

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    let mut ref_broad: Vec<HashSet<u64>> = Vec::with_capacity(titles.len());
    let mut ref_selective: Vec<HashSet<u64>> = Vec::with_capacity(titles.len());
    let mut oracle: Vec<HashSet<u64>> = Vec::with_capacity(titles.len());
    let mut total_truth = 0usize;
    for title in &titles {
        reference.match_title(title, &mut s, &mut out, true);
        ref_broad.push(out.iter().copied().collect());
        reference.match_title(title, &mut s, &mut out, false);
        ref_selective.push(out.iter().copied().collect());
        let truth = brute.matches(title, &mut blc, &mut bfeats);
        total_truth += truth.len();
        oracle.push(truth);
    }
    assert!(total_truth > 0, "degenerate corpus: no matches at all");
    // The single-node engine itself must satisfy the contract with broad on.
    for (i, _) in titles.iter().enumerate() {
        assert_eq!(ref_broad[i], oracle[i], "single-node disagrees with oracle");
    }

    for &k in &[1usize, 3, 8, 16] {
        let cfg = ClusterConfig {
            num_shards: k,
            include_broad: true,
            ..ClusterConfig::default()
        };
        let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");
        assert_eq!(cluster.num_shards(), k);

        // Every placement branch is exercised (A, B, C all present).
        let cc = cluster.class_counts().unwrap();
        assert!(cc[0] > 0, "K={k}: no class-A queries");
        assert!(
            cc[1] > 0,
            "K={k}: no class-B queries (any-of/arity-2 injection)"
        );
        assert!(cc[2] > 0, "K={k}: no class-C (broad) queries");

        for (i, title) in titles.iter().enumerate() {
            let got: HashSet<u64> = cluster.percolate(title).unwrap().into_iter().collect();
            assert_eq!(
                got, oracle[i],
                "K={k} broad=on: cluster vs brute-force oracle on {title:?}"
            );
            assert_eq!(
                got, ref_broad[i],
                "K={k} broad=on: cluster vs single-node on {title:?}"
            );

            // broad off: cluster must equal the single-node selective path (both
            // exclude class-C broad matches; class-B-arity-2 stays in the main lane).
            let got_sel: HashSet<u64> = cluster
                .percolate_with_broad(title, false)
                .unwrap()
                .into_iter()
                .collect();
            assert_eq!(
                got_sel, ref_selective[i],
                "K={k} broad=off: cluster vs single-node selective on {title:?}"
            );
        }
    }
}

#[test]
fn single_shard_cluster_equals_single_node_engine() {
    let (queries, titles) = build_corpus();
    let cfg = ClusterConfig {
        num_shards: 1,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");
    let mut reference = Engine::new(vocab());
    reference.build_from_queries(&queries);
    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    for title in &titles {
        let got: HashSet<u64> = cluster.percolate(title).unwrap().into_iter().collect();
        reference.match_title(title, &mut s, &mut out, true);
        let want: HashSet<u64> = out.iter().copied().collect();
        assert_eq!(got, want, "K=1 must reduce to the single-node engine");
    }
}

#[test]
fn placement_by_cost_class() {
    let (queries, _titles) = build_corpus();
    let cfg = ClusterConfig {
        num_shards: 8,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");
    let mut id = 9_000_000u64;
    let mut next = || {
        id += 1;
        id
    };

    // class A: a rare anchor -> exactly one selective shard.
    match cluster
        .add_query(next(), "1994 upper deck rareplayer0")
        .unwrap()
    {
        AddOutcome::Placed { shards } => {
            assert_eq!(shards.len(), 1, "class A should hit exactly one shard");
            assert!(shards[0] < 8);
        }
        other => panic!("class A expected Placed, got {other:?}"),
    }

    // class B arity-2: all-hot required, no rare anchor -> replicated lane.
    assert_eq!(
        cluster.add_query(next(), "1994 upper deck").unwrap(),
        AddOutcome::Replicated,
        "all-hot {{year}} {{brand}} should be class-B arity-2 -> replicated lane"
    );

    // class C: a single hot anchor (broad) -> replicated lane.
    assert_eq!(
        cluster.add_query(next(), "rookie").unwrap(),
        AddOutcome::Replicated,
        "broad single-hot anchor should be replicated"
    );

    // class B any-of: pure any-of of two rare players -> selective (1..=2 shards).
    match cluster
        .add_query(next(), "(rareplayer0,rareplayer1000)")
        .unwrap()
    {
        AddOutcome::Placed { shards } => {
            assert!(
                (1..=2).contains(&shards.len()),
                "any-of of two members places on 1..=2 shards, got {shards:?}"
            );
        }
        other => panic!("any-of expected Placed, got {other:?}"),
    }

    // a malformed query is surfaced, not silently dropped.
    assert!(
        matches!(
            cluster.add_query(next(), "(((").unwrap(),
            AddOutcome::RejectedParse(_)
        ),
        "malformed DSL should be RejectedParse"
    );
}

#[test]
fn anyof_query_can_place_on_multiple_shards() {
    let (queries, _titles) = build_corpus();
    let cfg = ClusterConfig {
        num_shards: 16,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");
    // Over many distinct rare-player pairs on a 16-shard ring, at least one
    // any-of query must straddle two shards — the multi-shard placement case.
    let mut id = 8_000_000u64;
    let mut saw_two = false;
    for i in 0..150u64 {
        id += 1;
        if let AddOutcome::Placed { shards } = cluster
            .add_query(id, &format!("(rareplayer{i},rareplayer{})", i + 1000))
            .unwrap()
        {
            if shards.len() == 2 {
                saw_two = true;
                break;
            }
        }
    }
    assert!(
        saw_two,
        "expected at least one any-of query to place on two distinct shards"
    );
}

#[test]
fn fan_out_is_content_routed_not_scatter() {
    let (queries, titles) = build_corpus();
    let k = 16usize;
    let cfg = ClusterConfig {
        num_shards: k,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");

    let mut max_fanout = 0usize;
    let mut saw_multi = false;
    for title in &titles {
        let f = cluster.shard_fanout(title).len();
        max_fanout = max_fanout.max(f);
        if f >= 2 {
            saw_multi = true;
        }
    }
    // Content routing, not scatter-gather: even on 16 shards a title touches only
    // a handful (its rare features + the replicated lane), never all N.
    assert!(saw_multi, "expected some title to fan out to >1 shard");
    assert!(
        max_fanout <= 8,
        "fan-out {max_fanout} on {k} shards is too high — routing is not content-routed"
    );
}

#[test]
fn add_then_percolate_then_remove_roundtrip() {
    // Exercises the incremental write paths the bulk oracle doesn't: a live
    // add_query (memtable insert), that the added query is actually findable via
    // routing, and that remove_query's fan-out delete makes it disappear.
    let (queries, _titles) = build_corpus();
    let cfg = ClusterConfig {
        num_shards: 8,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");

    let qid = 7_777_777u64;
    // class A: rare anchor (rareplayer0 is in the frozen dict via the any-of injection).
    let placed = cluster
        .add_query(qid, "1994 upper deck rareplayer0")
        .unwrap();
    assert!(
        matches!(placed, AddOutcome::Placed { .. }),
        "expected class-A Placed, got {placed:?}"
    );

    let title = "1994 upper deck rareplayer0 psa 10";
    assert!(
        cluster.percolate(title).unwrap().contains(&qid),
        "a live-added query must match a title that satisfies it"
    );

    let removed = cluster.remove_query(qid).unwrap();
    assert!(
        removed >= 1,
        "remove_query should tombstone the holding shard's entry, got {removed}"
    );
    assert!(
        !cluster.percolate(title).unwrap().contains(&qid),
        "a removed query must no longer match"
    );
}

#[test]
fn live_add_with_new_required_term_is_absorbed_not_broadened() {
    // The dynamic-vocabulary contract (ADR-046): a live write whose query has a term
    // absent from the FROZEN dict is ABSORBED (a deterministic synthetic id), so the
    // query keeps its full semantics. Dropping the term would broaden the query (a false
    // positive that survives verification). `zzgloxinia` never appears in build_corpus,
    // so it is not interned in the frozen dict.
    let (queries, _titles) = build_corpus();
    let cfg = ClusterConfig {
        num_shards: 8,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");

    let qid = 9_100_001u64;
    let placed = cluster
        .add_query(qid, "1994 upper deck zzgloxinia")
        .unwrap();
    assert!(
        matches!(placed, AddOutcome::Placed { .. }),
        "a query anchored on a new (hashed) term should place selectively, got {placed:?}"
    );

    // A title containing the new term satisfies the query -> matched (zero false negative).
    let with_term = "1994 upper deck zzgloxinia psa 10";
    assert!(
        cluster.percolate(with_term).unwrap().contains(&qid),
        "a new term must be absorbed so its query still matches a title containing it"
    );

    // A title WITHOUT the new term (but with the query's other required features) must NOT
    // match -> the query did not broaden. (With the old drop-on-miss behavior the query
    // would collapse to "1994 upper deck" and match this title.)
    let without_term = "1994 upper deck rookie psa 10";
    assert!(
        !cluster.percolate(without_term).unwrap().contains(&qid),
        "the query must not broaden: a title lacking the new term must not match"
    );
}

#[test]
fn live_add_with_all_unknown_anyof_group_is_satisfiable() {
    // The false-NEGATIVE case the old behavior risked: an any-of group whose members are
    // ALL absent from the frozen dict would collapse to empty (unsatisfiable) and drop a
    // real match. With hashing each member gets a synthetic id, so the group is
    // satisfiable. Neither `zznovela` nor `zznovelb` appears in build_corpus.
    let (queries, _titles) = build_corpus();
    let cfg = ClusterConfig {
        num_shards: 8,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");

    let qid = 9_200_002u64;
    let placed = cluster.add_query(qid, "(zznovela,zznovelb)").unwrap();
    assert!(
        !matches!(
            placed,
            AddOutcome::RejectedParse(_) | AddOutcome::RejectedClassD
        ),
        "an all-new any-of query must compile + place, not be rejected; got {placed:?}"
    );

    // A title containing either member satisfies the any-of -> matched (no false negative).
    assert!(
        cluster
            .percolate("1994 upper deck zznovela psa 10")
            .unwrap()
            .contains(&qid),
        "an all-new any-of group must be satisfiable, not collapse to a missed match"
    );
}

/// `ingest()` must refuse a non-empty cluster: it re-indexes from scratch, so calling it
/// on an already-populated cluster would silently duplicate entries (the ADR-029 footgun).
/// It returns `ShardError::Config` instead. (The happy path — ingest into a freshly
/// connected empty cluster — is covered by `cluster_grpc_oracle.rs`.)
#[test]
fn ingest_on_a_populated_cluster_is_rejected() {
    let data = generate(&GenConfig {
        num_queries: 500,
        num_titles: 1,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x1234_5678,
        num_players: 200,
        num_sets: 100,
    });
    let cfg = ClusterConfig {
        num_shards: 3,
        ..ClusterConfig::default()
    };
    // build() loads the corpus, so the cluster is already populated.
    let cluster = ClusterEngine::build(vocab(), &cfg, &data.queries).expect("build cluster");
    assert!(
        cluster.num_queries().unwrap() > 0,
        "corpus should populate the cluster"
    );
    assert!(
        matches!(
            cluster.ingest(&data.queries),
            Err(reverse_rusty::cluster::ShardError::Config(_))
        ),
        "ingest() on a populated cluster must error, not silently duplicate"
    );
}

// ---- ADR-035: per-shard replication (replication_factor > 1) ----

/// Replication is answer-invariant: a cluster with replicas returns EXACTLY the single-node
/// engine's set and the independent brute oracle's set, across shard counts × broad on/off.
/// (RF = 1 is covered by `cluster_matches_single_node_and_oracle`.)
#[test]
fn cluster_with_replicas_matches_single_node_and_oracle() {
    let (queries, titles) = build_corpus();
    let brute = Brute::build(&queries);
    let mut reference = Engine::new(vocab());
    reference.build_from_queries(&queries);

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    let mut ref_broad: Vec<HashSet<u64>> = Vec::with_capacity(titles.len());
    let mut ref_selective: Vec<HashSet<u64>> = Vec::with_capacity(titles.len());
    let mut oracle: Vec<HashSet<u64>> = Vec::with_capacity(titles.len());
    for title in &titles {
        reference.match_title(title, &mut s, &mut out, true);
        ref_broad.push(out.iter().copied().collect());
        reference.match_title(title, &mut s, &mut out, false);
        ref_selective.push(out.iter().copied().collect());
        oracle.push(brute.matches(title, &mut blc, &mut bfeats));
    }

    for &rf in &[2usize, 3] {
        for &k in &[1usize, 3, 8] {
            let cfg = ClusterConfig {
                num_shards: k,
                replication_factor: rf,
                include_broad: true,
                ..ClusterConfig::default()
            };
            let cluster =
                ClusterEngine::build(vocab(), &cfg, &queries).expect("build replicated cluster");
            // Replicas must not inflate the per-class tally (it is the primary's view).
            let cc = cluster.class_counts().unwrap();
            assert!(
                cc[0] > 0 && cc[1] > 0 && cc[2] > 0,
                "rf={rf} k={k}: classes {cc:?}"
            );
            for (i, title) in titles.iter().enumerate() {
                let got: HashSet<u64> = cluster.percolate(title).unwrap().into_iter().collect();
                assert_eq!(
                    got, oracle[i],
                    "rf={rf} k={k} broad=on: cluster vs oracle on {title:?}"
                );
                assert_eq!(
                    got, ref_broad[i],
                    "rf={rf} k={k} broad=on: cluster vs single-node on {title:?}"
                );
                let got_sel: HashSet<u64> = cluster
                    .percolate_with_broad(title, false)
                    .unwrap()
                    .into_iter()
                    .collect();
                assert_eq!(
                    got_sel, ref_selective[i],
                    "rf={rf} k={k} broad=off: cluster vs single-node selective on {title:?}"
                );
            }
        }
    }
}

/// Replicas are HA copies, not new logical data: `num_queries` / `class_counts` /
/// per-position counts must equal the RF = 1 cluster's. The composite reports the PRIMARY's
/// view, never a sum across copies — else the coordinator's cross-position sums would
/// multiply by the replication factor.
#[test]
fn replication_does_not_inflate_counts() {
    let (queries, _titles) = build_corpus();
    let mk = |rf: usize| {
        let cfg = ClusterConfig {
            num_shards: 3,
            replication_factor: rf,
            ..ClusterConfig::default()
        };
        ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster")
    };
    let base = mk(1);
    let replicated = mk(3);
    assert_eq!(
        base.num_queries().unwrap(),
        replicated.num_queries().unwrap(),
        "num_queries must not be inflated by replicas"
    );
    assert_eq!(
        base.class_counts().unwrap(),
        replicated.class_counts().unwrap(),
        "class_counts must not be inflated by replicas"
    );
    assert_eq!(
        base.shard_query_counts().unwrap(),
        replicated.shard_query_counts().unwrap(),
        "per-position counts must be the primary's, not summed across copies"
    );
}

/// The live write path at RF > 1: `add_query` fans to the replicas (so the query is findable
/// and copies stay set-equal), and `remove_query` returns the PRIMARY's tombstone count
/// (rf-independent), not a sum across copies.
#[test]
fn replicated_live_add_and_remove() {
    let (queries, _titles) = build_corpus();
    let mk = |rf: usize| {
        let cfg = ClusterConfig {
            num_shards: 8,
            replication_factor: rf,
            include_broad: true,
            ..ClusterConfig::default()
        };
        ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster")
    };
    let c1 = mk(1);
    let c2 = mk(2);
    let qid = 7_777_778u64;
    let dsl = "1994 upper deck rareplayer0";

    assert!(
        matches!(c2.add_query(qid, dsl).unwrap(), AddOutcome::Placed { .. }),
        "class-A live add should be Placed"
    );
    let title = "1994 upper deck rareplayer0 psa 10";
    assert!(
        c2.percolate(title).unwrap().contains(&qid),
        "a live-added query must be findable at rf=2 (write fanned to the replica)"
    );

    // The remove count is the primary's (same as rf=1), not multiplied by replicas.
    c1.add_query(qid, dsl).unwrap();
    let r1 = c1.remove_query(qid).unwrap();
    let r2 = c2.remove_query(qid).unwrap();
    assert_eq!(
        r1, r2,
        "remove count must be primary-only (rf-independent): rf1={r1} rf2={r2}"
    );
    assert!(
        !c2.percolate(title).unwrap().contains(&qid),
        "a removed query must no longer match"
    );
}
