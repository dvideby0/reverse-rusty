//! In-process reconcile oracle (ADR-092): the unattended reconciler is a clean NO-OP on an in-process
//! cluster — there are no addr'd data nodes to place on — so the byte-identical-default claim holds and
//! a coordinator that (somehow) runs a pass changes nothing. The data-moving behavior + the
//! restart-zero-FN proof live in the gRPC oracle (`cluster_grpc_oracle::reconcile`), where real shard
//! servers exist; this is the lean companion that guards the no-op / idempotence contract without gRPC.
//!
//! Whole file is gated on `distributed` (the `reconcile` method is `distributed`-only); the default
//! `cargo test` skips it.
#![cfg(feature = "distributed")]

use std::collections::HashSet;

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine};
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::Normalizer;

fn vocab() -> Normalizer {
    Normalizer::default_vocab().expect("built-in vocab")
}

/// A small in-process cluster + the title set to probe (the autoscale oracle's setup, so the corpus +
/// seed are identical and the placement is deterministic).
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
        num_shards: 8,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(vocab(), &ccfg, &data.queries).expect("build cluster");
    (cluster, data.titles)
}

/// The per-title match sets at an explicit broad toggle — the matching fingerprint required unchanged
/// across a reconcile pass (swept with broad on AND off).
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

/// `reconcile` on an in-process cluster is a clean NO-OP (no addr'd data nodes): an empty report, the
/// control-plane epoch invariant, and `percolate` byte-identical (broad on + off) — the byte-identical
/// default guard — and idempotent (a second pass is also a no-op).
#[test]
fn reconcile_in_process_is_noop_and_byte_identical() {
    let (cluster, titles) = build();
    let base_broad = sweep(&cluster, &titles, true);
    let base_plain = sweep(&cluster, &titles, false);
    let epoch_before = cluster.control_state().expect("state").epoch;

    // `reconcile` returns before it ever touches the handle (no targets), so any runtime handle works.
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let report = cluster.reconcile(1, rt.handle()).expect("reconcile");
    assert!(
        report.is_converged() && report.moved_count() == 0,
        "an in-process reconcile is a trivial convergence: {report:?}"
    );
    assert!(
        report.reconciled.is_empty()
            && report.skipped.is_empty()
            && report.uncommitted.is_empty()
            && report.failed.is_empty(),
        "an in-process reconcile report is empty (no addr'd data nodes to move): {report:?}"
    );
    assert_eq!(
        cluster.control_state().expect("state").epoch,
        epoch_before,
        "a no-op reconcile commits nothing (epoch invariant)"
    );
    assert_eq!(
        sweep(&cluster, &titles, true),
        base_broad,
        "reconcile must not change any match set (broad on)"
    );
    assert_eq!(
        sweep(&cluster, &titles, false),
        base_plain,
        "reconcile must not change any match set (broad off)"
    );

    // Idempotent: a second pass is also a clean no-op.
    let report2 = cluster.reconcile(1, rt.handle()).expect("second reconcile");
    assert!(
        report2.is_converged() && report2.moved_count() == 0,
        "a second in-process reconcile is also a no-op: {report2:?}"
    );
    assert_eq!(
        cluster.control_state().expect("state").epoch,
        epoch_before,
        "a second no-op reconcile commits nothing"
    );
}
