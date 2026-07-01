//! Unit tests for the per-node metrics renderers + the minimal HTTP listener (ADR-091 + ADR-093).

use super::{
    is_metrics_get, render_control, render_shard_pending, render_shards, serve_metrics,
    ControlMetricsView, ShardSample,
};
use crate::events::{EngineMetrics, SegmentInfo, SegmentKind};
use std::io::{Read, Write};
use std::net::TcpStream;

fn sample(
    shard_id: u32,
    m: EngineMetrics,
    segments: Vec<SegmentInfo>,
    class: [u64; 4],
) -> ShardSample {
    ShardSample {
        shard_id,
        metrics: m,
        segments,
        class,
    }
}

fn sample_metrics() -> EngineMetrics {
    EngineMetrics {
        total_queries: 7,
        base_segments: 2,
        memtable_entries: 3,
        segment_sizes: vec![4, 1],
        segment_holes: vec![0.0, 0.5],
        rejected_parse: 0,
        rejected_class_d: 0,
        dict_features: 99,
        exact_bytes: 11,
        index_bytes: 22,
        filter_bytes: 33,
        stale_segments: 1,
        dict_bytes: 44,
        query_store_bytes: 55,
        logical_index_bytes: 66,
        alive_bytes: 77,
        wal_size_bytes: 0,
        wal_pending_entries: 0,
    }
}

fn seg(deleted: usize) -> SegmentInfo {
    SegmentInfo {
        ordinal: 0,
        kind: SegmentKind::Mmap,
        entries: 10,
        alive: 10 - deleted,
        deleted,
        holes_ratio: 0.0,
        vocab_epoch: 0,
        stale: false,
        resident_bytes: 0,
        overhead_bytes: 0,
    }
}

#[test]
fn render_shard_emits_named_gauges() {
    // A single hosted slot (position 3) renders exactly one `{shard="3"}` series per family.
    let out = render_shards(&[sample(
        3,
        sample_metrics(),
        vec![seg(3), seg(2)],
        [1, 2, 3, 4],
    )]);
    assert!(out.contains("# TYPE reverse_rusty_total_queries gauge"));
    assert!(out.contains("\nreverse_rusty_total_queries{shard=\"3\"} 7\n"));
    assert!(out.contains("reverse_rusty_dict_features{shard=\"3\"} 99"));
    assert!(out.contains("reverse_rusty_memory_bytes{shard=\"3\",component=\"exact\"} 11"));
    assert!(out.contains("reverse_rusty_memory_bytes{shard=\"3\",component=\"filter\"} 33"));
    // c is the broad lane; 3rd class slot.
    assert!(out.contains("reverse_rusty_class_queries{shard=\"3\",class=\"c\"} 3"));
    // tombstoned = sum of segment `deleted` (3 + 2).
    assert!(out.contains("reverse_rusty_tombstoned_entries{shard=\"3\"} 5"));
    assert!(out.contains("reverse_rusty_shard_ready{shard=\"3\"} 1"));
}

#[test]
fn render_shards_labels_all_colocated_slots() {
    // A co-located node hosting slots {1, 4} emits one labeled series per slot under EACH family,
    // with the family header written exactly once (valid grouped exposition — ADR-093 Stage 3).
    let mut m1 = sample_metrics();
    m1.total_queries = 11;
    let mut m4 = sample_metrics();
    m4.total_queries = 44;
    let out = render_shards(&[
        sample(1, m1, vec![seg(1)], [1, 0, 0, 0]),
        sample(4, m4, vec![seg(2)], [0, 2, 0, 0]),
    ]);
    // Header emitted ONCE for the family (duplicating it would be malformed exposition).
    assert_eq!(
        out.matches("# TYPE reverse_rusty_total_queries gauge")
            .count(),
        1,
        "each metric family header must appear exactly once across all slots"
    );
    // One labeled series per slot.
    assert!(out.contains("reverse_rusty_total_queries{shard=\"1\"} 11"));
    assert!(out.contains("reverse_rusty_total_queries{shard=\"4\"} 44"));
    assert!(out.contains("reverse_rusty_shard_ready{shard=\"1\"} 1"));
    assert!(out.contains("reverse_rusty_shard_ready{shard=\"4\"} 1"));
    // Both slots ready ⇒ two ready series.
    assert_eq!(out.matches("reverse_rusty_shard_ready{shard=").count(), 2);
}

#[test]
fn pending_shard_reports_not_ready() {
    // No loaded slots ⇒ the pending body (unlabeled ready 0), also via the empty-slice path.
    let out = render_shard_pending();
    assert!(out.contains("reverse_rusty_shard_ready 0"));
    assert!(!out.contains("reverse_rusty_total_queries"));
    assert_eq!(render_shards(&[]), out, "empty slice ⇒ the pending body");
}

#[test]
fn render_control_emits_state_and_indices() {
    let view = ControlMetricsView {
        term: 5,
        state: "leader",
        is_leader: true,
        leader_known: true,
        last_log_index: Some(42),
        last_applied: Some(40),
        voters: 3,
        snapshot_last_index: Some(30),
    };
    let out = render_control(&view);
    assert!(out.contains("reverse_rusty_control_term 5"));
    assert!(out.contains("reverse_rusty_control_is_leader 1"));
    assert!(out.contains("reverse_rusty_control_state{state=\"leader\"} 1"));
    assert!(out.contains("reverse_rusty_control_last_applied 40"));
    assert!(out.contains("reverse_rusty_control_voters 3"));
}

#[test]
fn request_line_parsing() {
    assert!(is_metrics_get("GET /_metrics HTTP/1.1\r\n"));
    assert!(is_metrics_get("GET /metrics HTTP/1.1\r\n"));
    assert!(is_metrics_get("GET /_metrics?foo=bar HTTP/1.1\r\n"));
    assert!(!is_metrics_get("GET /healthz HTTP/1.1\r\n"));
    assert!(!is_metrics_get("POST /_metrics HTTP/1.1\r\n"));
}

fn http_get(addr: std::net::SocketAddr, path: &str) -> String {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response
}

#[test]
fn http_roundtrip_serves_metrics_and_404s() {
    let handle = serve_metrics("127.0.0.1:0".parse().unwrap(), || {
        "reverse_rusty_test 1\n".to_string()
    })
    .unwrap();
    let addr = handle.addr();

    let ok = http_get(addr, "/_metrics");
    assert!(ok.starts_with("HTTP/1.1 200 OK"), "got: {ok}");
    assert!(ok.contains("text/plain; version=0.0.4"));
    assert!(ok.contains("reverse_rusty_test 1"));

    let alias = http_get(addr, "/metrics");
    assert!(alias.contains("reverse_rusty_test 1"));

    let missing = http_get(addr, "/nope");
    assert!(
        missing.starts_with("HTTP/1.1 404 Not Found"),
        "got: {missing}"
    );

    handle.shutdown();
}

#[test]
fn oversized_request_line_does_not_wedge_the_listener() {
    // A newline-less line far larger than the cap must NOT grow memory unbounded or block the
    // single metrics thread forever — the DoS-hardening regression (codex). The bounded read
    // returns and the connection is closed; we tolerate any outcome on this bad connection (the
    // server closes with bytes still unread, which surfaces as a connection reset). The real
    // assertion is that a NORMAL scrape still succeeds afterward — one bad client didn't wedge it.
    let handle = serve_metrics("127.0.0.1:0".parse().unwrap(), || {
        "reverse_rusty_test 1\n".to_string()
    })
    .unwrap();
    let addr = handle.addr();
    {
        let mut bad = TcpStream::connect(addr).unwrap();
        let huge = vec![b'A'; super::MAX_REQUEST_BYTES as usize + 2000];
        let _ = bad.write_all(&huge);
        let mut sink = Vec::new();
        let _ = bad.read_to_end(&mut sink);
    }
    let ok = http_get(addr, "/_metrics");
    assert!(
        ok.starts_with("HTTP/1.1 200 OK"),
        "listener wedged; got: {ok}"
    );
    assert!(ok.contains("reverse_rusty_test 1"));
    handle.shutdown();
}
