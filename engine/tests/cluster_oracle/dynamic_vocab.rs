//! Live incremental write paths (add → percolate → remove) and the dynamic-vocabulary
//! absorption contract (ADR-046): a new required term / an all-unknown any-of group is absorbed
//! via deterministic synthetic ids, never silently broadened or dropped.

use crate::harness::*;
use reverse_rusty::cluster::{AddOutcome, ClusterConfig, ClusterEngine};

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
