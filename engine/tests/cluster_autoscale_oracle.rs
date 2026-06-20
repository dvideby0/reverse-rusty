//! Autoscale oracle — the acceptance gate for the autoscaler policy/trigger layer (ADR-045,
//! step 6c).
//!
//! The autoscaler is a pure policy (`evaluate`) plus a thin `ClusterEngine` driver
//! (`tick`/`on_node_*`) that drives the already-built `rebalance` on membership/load events.
//! This proves, over a real in-process `ClusterEngine` (in-memory control plane, no
//! `distributed` feature), that:
//!   * `tick` commits the SAME shard→node map a manual `rebalance` would (the driver and the
//!     manual path agree — the policy adds no placement of its own);
//!   * — the load-bearing one — autoscaling NEVER alters matching: an in-process cluster holds
//!     every shard locally, so the map is advisory and `percolate` is byte-identical before
//!     and after a `tick` (the autoscaler cannot introduce a false negative);
//!   * a `tick` does not thrash (a second tick commits nothing — the control-plane epoch does
//!     not advance);
//!   * a disabled config is a true no-op (no actions, the map untouched); and
//!   * the advisory rules (corpus-over-threshold → `RecommendSplit`) fire without mutating the
//!     cluster.

use std::collections::HashSet;

use reverse_rusty::cluster::{
    AutoscaleConfig, ClusterConfig, ClusterEngine, NodeDescriptor, NodeId, NodeRole, ScalingAction,
};
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::Normalizer;

const NUM_SHARDS: usize = 8;
const RF: usize = 2;

fn vocab() -> Normalizer {
    Normalizer::default_vocab().expect("built-in vocab")
}

fn data_node(id: u64) -> NodeDescriptor {
    NodeDescriptor {
        id: NodeId(id),
        addr: Some(format!("http://127.0.0.1:{}", 7000 + id)),
        role: NodeRole::Data,
    }
}

/// The enabled policy used by the rebalance tests: skew + split OFF, so the ONLY rule that can
/// fire is membership drift ⇒ exactly one `Rebalance`.
fn enabled() -> AutoscaleConfig {
    AutoscaleConfig {
        enabled: true,
        target_replication_factor: RF,
        max_node_load_skew: 0.0,
        split_corpus_threshold: 0,
    }
}

/// A small in-process cluster + the title set to probe (verbatim the allocator oracle's setup,
/// so the corpus + seed are identical and the placement is deterministic).
fn build() -> (ClusterEngine, Vec<String>) {
    let cfg = GenConfig {
        num_queries: 1200,
        num_titles: 120,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x5F0C_A11E,
        num_players: 300,
        num_sets: 150,
    };
    let data = generate(&cfg);
    let ccfg = ClusterConfig {
        num_shards: NUM_SHARDS,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(vocab(), &ccfg, &data.queries).expect("build cluster");
    (cluster, data.titles)
}

/// The per-title match sets at an explicit broad toggle — the matching fingerprint we require
/// unchanged across a tick (swept with broad on AND off, like the allocator oracle).
fn sweep(cluster: &ClusterEngine, titles: &[String], include_broad: bool) -> Vec<HashSet<u64>> {
    titles
        .iter()
        .map(|t| {
            cluster
                .percolate_with_broad(t, include_broad)
                .expect("percolate")
                .into_iter()
                .collect()
        })
        .collect()
}

#[test]
fn tick_commits_the_same_map_as_a_manual_rebalance() {
    // Cluster A: register 3 data nodes, then autoscale `tick`.
    let (cluster_a, _) = build();
    for id in 1..=3 {
        cluster_a.register_node(data_node(id)).expect("register");
    }
    let decision = cluster_a.tick(&enabled()).expect("tick");
    assert_eq!(
        decision.actions,
        vec![ScalingAction::Rebalance { rf: RF }],
        "skew/split off ⇒ exactly one membership-driven rebalance: {decision:?}"
    );
    assert!(
        !decision.rationale.is_empty(),
        "rebalance carries a rationale"
    );

    // Cluster B (fresh, identical seed): the same registers, then a MANUAL rebalance.
    let (cluster_b, _) = build();
    for id in 1..=3 {
        cluster_b.register_node(data_node(id)).expect("register");
    }
    cluster_b.rebalance(RF).expect("manual rebalance");

    assert_eq!(
        cluster_a.control_state().expect("state a").assignments,
        cluster_b.control_state().expect("state b").assignments,
        "tick's rebalance and a manual rebalance commit the identical HRW map"
    );
}

#[test]
fn tick_preserves_percolate_byte_identically() {
    let (cluster, titles) = build();
    let base_broad = sweep(&cluster, &titles, true);
    let base_plain = sweep(&cluster, &titles, false);

    for id in 1..=3 {
        cluster.register_node(data_node(id)).expect("register");
    }
    cluster.tick(&enabled()).expect("tick");

    // The load-bearing property: the map moved (rebalance ran) but matching did not.
    assert_eq!(
        sweep(&cluster, &titles, true),
        base_broad,
        "tick must not change any title's match set (broad on)"
    );
    assert_eq!(
        sweep(&cluster, &titles, false),
        base_plain,
        "tick must not change any title's match set (broad off)"
    );

    // No thrash: a second tick commits nothing (the epoch does not advance). Asserting
    // epoch-invariance — NOT action-absence — is the robust check: HRW need not place every
    // registered node, so the coarse drift trigger may stay tripped while `rebalance` is a
    // genuine no-op.
    let epoch_before = cluster.control_state().expect("state").epoch;
    cluster.tick(&enabled()).expect("second tick");
    let epoch_after = cluster.control_state().expect("state").epoch;
    assert_eq!(
        epoch_before, epoch_after,
        "a second tick on unchanged membership commits no reassignment"
    );
}

#[test]
fn disabled_config_is_a_noop() {
    let (cluster, titles) = build();
    let baseline = sweep(&cluster, &titles, true);

    for id in 1..=3 {
        cluster.register_node(data_node(id)).expect("register");
    }
    let decision = cluster.tick(&AutoscaleConfig::default()).expect("tick");
    assert!(
        decision.actions.is_empty(),
        "a disabled autoscaler recommends nothing: {decision:?}"
    );

    // The map is untouched — every position still on the genesis node (no rebalance ran).
    let st = cluster.control_state().expect("state");
    assert!(
        st.assignments
            .iter()
            .all(|a| a.primary == NodeId(0) && a.replicas.is_empty()),
        "disabled tick leaves the genesis map intact: {:?}",
        st.assignments
    );
    assert_eq!(
        sweep(&cluster, &titles, true),
        baseline,
        "a disabled tick changes no match set"
    );
}

#[test]
fn corpus_over_threshold_recommends_split() {
    let (cluster, titles) = build();
    let baseline = sweep(&cluster, &titles, true);

    // Split pressure measures the SELECTIVE (non-replicated) per-shard load (ADR-080): the
    // replicated broad lane (class C + D) is on every shard and splitting won't shrink it, so the
    // autoscaler discounts it. Pick a threshold just below the busiest shard's SELECTIVE load.
    let counts = cluster.shard_query_counts().expect("counts");
    let cc = cluster.class_counts().expect("class counts");
    let num_shards = cluster.num_shards() as u64;
    let replicated = ((cc[2] + cc[3]) / num_shards) as usize;
    let selective: Vec<usize> = counts
        .iter()
        .map(|&c| c.saturating_sub(replicated))
        .collect();
    let (max_pos, &max_sel) = selective
        .iter()
        .enumerate()
        .max_by_key(|(_, &c)| c)
        .expect("non-empty");
    assert!(max_sel > 0, "need some selective load to split on");
    let threshold = max_sel.saturating_sub(1);
    let cfg = AutoscaleConfig {
        enabled: true,
        target_replication_factor: 1,
        max_node_load_skew: 0.0,
        split_corpus_threshold: threshold,
    };

    let epoch_before = cluster.control_state().expect("state").epoch;
    let decision = cluster.tick(&cfg).expect("tick");

    // Every split advisory is well-formed (its corpus matches the live per-shard SELECTIVE count),
    // and the busiest shard is among them.
    let splits: Vec<(u32, usize)> = decision
        .actions
        .iter()
        .filter_map(|a| match a {
            ScalingAction::RecommendSplit { position, corpus } => Some((*position, *corpus)),
            _ => None,
        })
        .collect();
    assert!(
        !splits.is_empty(),
        "a shard over threshold recommends a split"
    );
    for (pos, corpus) in &splits {
        assert_eq!(
            *corpus, selective[*pos as usize],
            "split corpus matches the shard's selective (non-replicated) load"
        );
        assert!(
            *corpus > threshold,
            "only over-threshold shards are reported"
        );
    }
    assert!(
        splits.iter().any(|(p, _)| *p as usize == max_pos),
        "the busiest shard ({max_pos}, selective corpus {max_sel}) is recommended for split"
    );

    // Advisory ⇒ no mutation: the control plane and matching are untouched.
    assert_eq!(
        cluster.control_state().expect("state").epoch,
        epoch_before,
        "a split advisory commits no cluster-state change"
    );
    assert_eq!(
        sweep(&cluster, &titles, true),
        baseline,
        "a split advisory changes no match set"
    );
}

#[test]
fn tick_emits_handoff_under_skew_without_perturbing_matching() {
    // ADR-048: node-skew's advisory `Handoff` (ADR-045) is now WIRED to `execute_handoff`. In a
    // non-`distributed` (in-process) cluster there is no runtime handle and no remote endpoint, so
    // the driver compiles the wiring out and the `Handoff` is RETURNED but never acted on. This
    // proves the policy still emits it under skew AND — the load-bearing property — matching is
    // byte-identical across the tick (the wiring introduces no false negative on the lean path).
    let (cluster, titles) = build();
    for id in 1..=3 {
        cluster.register_node(data_node(id)).expect("register");
    }
    // Spread the corpus across the nodes so per-node load is well-defined and skewed.
    cluster.rebalance(RF).expect("rebalance");
    let base_broad = sweep(&cluster, &titles, true);
    let base_plain = sweep(&cluster, &titles, false);

    // Derive the actual per-node primary load from the committed map + live shard corpus, then set
    // the skew threshold just below the observed max/mean so node-skew fires deterministically on
    // this seeded corpus (no magic constant that could drift with the generator).
    let state = cluster.control_state().expect("state");
    let counts = cluster.shard_query_counts().expect("counts");
    let mut node_load: std::collections::BTreeMap<u64, usize> = std::collections::BTreeMap::new();
    for a in &state.assignments {
        if let Some(&c) = counts.get(a.position as usize) {
            *node_load.entry(a.primary.0).or_default() += c;
        }
    }
    assert!(
        node_load.len() >= 2,
        "need ≥2 loaded nodes to skew between: {node_load:?}"
    );
    let total: usize = node_load.values().sum();
    let mean = total as f64 / node_load.len() as f64;
    let max = *node_load.values().max().expect("non-empty") as f64;
    assert!(
        max > mean,
        "the seeded corpus must distribute unevenly across nodes: {node_load:?}"
    );
    // Strictly between mean and max ⇒ hot_load > skew*mean ⇒ exactly the node-skew rule fires.
    let skew = (max / mean - 0.01).max(1.0 + f64::EPSILON);
    let cfg = AutoscaleConfig {
        enabled: true,
        target_replication_factor: RF,
        max_node_load_skew: skew,
        split_corpus_threshold: 0,
    };

    let decision = cluster.tick(&cfg).expect("tick");
    assert!(
        decision
            .actions
            .iter()
            .any(|a| matches!(a, ScalingAction::Handoff { .. })),
        "node load skewed past the threshold must recommend a handoff: {decision:?}"
    );

    // The crux: the wiring did not perturb matching (in-process ⇒ the handoff is advisory only).
    assert_eq!(
        sweep(&cluster, &titles, true),
        base_broad,
        "matching unchanged across the tick (broad on)"
    );
    assert_eq!(
        sweep(&cluster, &titles, false),
        base_plain,
        "matching unchanged across the tick (broad off)"
    );
}
