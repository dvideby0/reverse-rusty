//! Integration tests for the per-node `/_metrics` surface (ADR-091).
//!
//! The unit tests in `src/cluster/node_metrics.rs` cover the renderers + the HTTP layer with
//! synthetic inputs. These prove the END-TO-END adapter path over the public API: a populated
//! `ShardServer` renders REAL engine numbers (not zeros), a pending shard renders not-ready, and a
//! real (single-node) Raft `ControlServer` renders live leader/term/membership — exercising
//! `control_view` against an actual `RaftMetrics`. `distributed`-only (the metric sources live on the
//! gRPC servers).
#![cfg(feature = "distributed")]

use std::sync::Arc;

use reverse_rusty::cluster::{in_process_cluster, ControlServer, ShardServer};
use reverse_rusty::compile::extract;
use reverse_rusty::config::EngineConfig;
use reverse_rusty::dict::Dict;
use reverse_rusty::normalize::Normalizer;

/// A few queries that compile against a dict built from themselves.
fn corpus() -> Vec<(u64, String)> {
    vec![
        (1, "1994 upper deck ken griffey".to_string()),
        (2, "topps chrome refractor".to_string()),
        (3, "1990 fleer rookie".to_string()),
    ]
}

/// Build a frozen dict over the queries (the same shape `ShardServer::new` expects: a dict already
/// arranged to match the coordinator's).
fn build_dict(norm: &Normalizer, queries: &[(u64, String)]) -> Arc<Dict> {
    let mut d = Dict::new();
    let mut lc = String::new();
    for (_id, text) in queries {
        if let Ok(ast) = reverse_rusty::dsl::parse(text) {
            let _ = extract(&ast, norm, &mut d, &mut lc);
        }
    }
    d.finalize_mask();
    Arc::new(d)
}

#[test]
fn populated_shard_metrics_report_real_numbers() {
    let norm = Arc::new(Normalizer::default_vocab().expect("normalizer"));
    let queries = corpus();
    let dict = build_dict(&norm, &queries);

    let server = ShardServer::new(Arc::clone(&norm), dict, EngineConfig::default());
    server.ingest_dsl(&queries);

    let body = server.metrics_source().render();

    // Adopted/serving, and total_queries reflects every ingested query — the numbers are REAL. This
    // node hosts slot 0, so series carry the ADR-093 per-shard label `shard="0"`.
    assert!(
        body.contains("reverse_rusty_shard_ready{shard=\"0\"} 1"),
        "expected ready; got:\n{body}"
    );
    assert!(
        body.contains(&format!(
            "reverse_rusty_total_queries{{shard=\"0\"}} {}\n",
            queries.len()
        )),
        "expected total_queries={}; got:\n{body}",
        queries.len()
    );
    // Every cost-class series is present (the broad lane is class c).
    assert!(body.contains("reverse_rusty_class_queries{shard=\"0\",class=\"c\"}"));
    // Memory + feature gauges are present and non-trivial.
    assert!(body.contains("reverse_rusty_memory_bytes{shard=\"0\",component=\"exact\"}"));
    assert!(body.contains("reverse_rusty_dict_features{shard=\"0\"}"));
    assert!(body.contains("# TYPE reverse_rusty_total_queries gauge"));
}

#[test]
fn pending_shard_metrics_report_not_ready() {
    let norm = Arc::new(Normalizer::default_vocab().expect("normalizer"));
    let server = ShardServer::pending(norm, EngineConfig::default());
    let body = server.metrics_source().render();
    assert!(
        body.contains("reverse_rusty_shard_ready 0"),
        "expected not-ready; got:\n{body}"
    );
    assert!(
        !body.contains("reverse_rusty_total_queries"),
        "a pending shard has no engine to count; got:\n{body}"
    );
}

#[test]
fn control_metrics_report_leader_state() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    // A single-node in-process cluster elects itself leader before this returns (ADR-038).
    let planes = in_process_cluster(&[0], 8, 128, 0, rt.handle()).expect("raft cluster");
    let server = ControlServer::new(planes[0].raft());

    let body = server.metrics_source().render();
    assert!(
        body.contains("reverse_rusty_control_is_leader 1"),
        "the sole node is the leader; got:\n{body}"
    );
    assert!(
        body.contains("reverse_rusty_control_state{state=\"leader\"} 1"),
        "got:\n{body}"
    );
    assert!(
        body.contains("reverse_rusty_control_voters 1"),
        "one voting member; got:\n{body}"
    );
    assert!(body.contains("reverse_rusty_control_term"), "got:\n{body}");
}
