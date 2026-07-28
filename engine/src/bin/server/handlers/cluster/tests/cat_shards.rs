use super::*;

use std::sync::Arc;

use axum::http::{header, Method};
use reverse_rusty::cluster::{
    ClusterState, ClusterStateChange, ControlError, ControlPlane, NodeId, StateVersion,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_table_is_headerless_truthful_and_uncacheable() {
    let state = test_state(&seed());
    let expected = state
        .cluster
        .read()
        .shard_query_counts()
        .expect("shard counts");
    let (status, headers, bytes) = send_raw(&state, req_empty("GET", "/_cat/shards")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
    assert_eq!(
        headers.get(header::CONTENT_TYPE).expect("content type"),
        "text/plain; charset=utf-8"
    );
    let table = String::from_utf8(bytes.to_vec()).expect("UTF-8 table");
    let lines: Vec<&str> = table.lines().collect();
    assert_eq!(lines.len(), expected.len(), "{table}");
    assert!(!lines[0].contains("shard"), "{table}");
    for (position, (line, count)) in lines.iter().zip(expected).enumerate() {
        let cells: Vec<&str> = line.split_whitespace().collect();
        assert_eq!(
            cells,
            [position.to_string(), count.to_string(), "0".to_string()],
            "{table}"
        );
    }
    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["cat_shards", "200"])
            .get(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn common_cat_controls_select_sort_and_preserve_json_key_order() {
    let state = test_state(&seed());
    let (status, _, bytes) = send_raw(
        &state,
        req_empty("GET", "/_cat/shards?v&h=shard,queries,nodes"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let table = String::from_utf8(bytes.to_vec()).expect("UTF-8 table");
    assert_eq!(
        table
            .lines()
            .next()
            .expect("header")
            .split_whitespace()
            .collect::<Vec<_>>(),
        ["shard", "queries", "nodes"]
    );

    let (status, _, bytes) = send_raw(
        &state,
        req_empty(
            "GET",
            "/_cat/shards?format=json&h=q,shard,n&s=queries:desc,shard",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let raw = String::from_utf8(bytes.to_vec()).expect("JSON text");
    assert!(raw.starts_with("[{\"queries\":\""), "{raw}");
    assert!(raw.contains("\",\"shard\":\""), "{raw}");
    assert!(raw.contains("\",\"nodes\":\"0\"}"), "{raw}");
    let rows: serde_json::Value = serde_json::from_str(&raw).expect("JSON rows");
    let rows = rows.as_array().expect("rows");
    assert_eq!(rows.len(), 3);
    for pair in rows.windows(2) {
        let left = pair[0]["queries"]
            .as_str()
            .expect("query string")
            .parse::<u64>()
            .expect("numeric query count");
        let right = pair[1]["queries"]
            .as_str()
            .expect("query string")
            .parse::<u64>()
            .expect("numeric query count");
        assert!(left >= right, "{raw}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn help_bypasses_shared_stats_admission() {
    let state = test_state(&seed());
    let held = Arc::clone(&state.stats_permits)
        .acquire_owned()
        .await
        .expect("permit");
    let (status, _, bytes) = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        send_raw(&state, req_empty("GET", "/_cat/shards?help")),
    )
    .await
    .expect("help must not wait for stats admission");
    assert_eq!(status, StatusCode::OK);
    let help = String::from_utf8(bytes.to_vec()).expect("UTF-8 help");
    assert!(help.contains("shard"), "{help}");
    assert!(help.contains("queries"), "{help}");
    assert!(help.contains("nodes"), "{help}");
    drop(held);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transport_and_controls_fail_loud() {
    let state = test_state(&seed());
    for uri in [
        "/_cat/shards?unknown=true",
        "/_cat/shards?format=yaml",
        "/_cat/shards?v=maybe",
        "/_cat/shards?h=no_such_column",
        "/_cat/shards?s=no_such_column",
        "/_cat/shards?s=queries:sideways",
        "/_cat/shards?bytes=b",
        "/_cat/shards?help&h=shard",
    ] {
        let (status, headers, bytes) = send_raw(&state, req_empty("GET", uri)).await;
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error");
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {body}");
        assert_eq!(body["error"]["type"], "validation_error", "{uri}: {body}");
        assert_eq!(
            headers.get(header::CACHE_CONTROL).expect("cache"),
            "no-store"
        );
    }

    let (status, body) = send(
        &state,
        Request::builder()
            .method(Method::GET)
            .uri("/_cat/shards")
            .body(Body::from("not empty"))
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "validation_error");

    let (status, headers, bytes) = send_raw(&state, req_empty("POST", "/_cat/shards")).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(headers.get(header::ALLOW).expect("allow"), "GET");
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error");
    assert_eq!(body["error"]["type"], "method_not_allowed");

    let oversized = vec![b'x'; crate::handlers::CAT_SHARDS_BODY_LIMIT + 1];
    let (status, _, bytes) = send_raw(
        &state,
        Request::builder()
            .method(Method::GET)
            .uri("/_cat/shards")
            .body(Body::from(oversized))
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error");
    assert_eq!(body["error"]["type"], "payload_too_large");
}

struct BrokenControlPlane;

impl ControlPlane for BrokenControlPlane {
    fn cluster_state(&self) -> Result<Arc<ClusterState>, ControlError> {
        Err(ControlError::Backend("offline".to_string()))
    }

    fn version(&self) -> Result<StateVersion, ControlError> {
        Err(ControlError::Backend("offline".to_string()))
    }

    fn propose(&self, _: ClusterStateChange) -> Result<StateVersion, ControlError> {
        Err(ControlError::Backend("offline".to_string()))
    }

    fn change_membership(&self, _: Vec<NodeId>) -> Result<StateVersion, ControlError> {
        Err(ControlError::Backend("offline".to_string()))
    }

    fn leader(&self) -> Result<Option<NodeId>, ControlError> {
        Err(ControlError::Backend("offline".to_string()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_plane_failure_is_not_rendered_as_empty_assignments() {
    let config = reverse_rusty::cluster::ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..Default::default()
    };
    let cluster = reverse_rusty::cluster::ClusterEngine::build(
        Normalizer::default_vocab().expect("vocab"),
        &config,
        &seed(),
    )
    .expect("cluster")
    .with_control_plane(Box::new(BrokenControlPlane));
    let state = state_from_cluster(cluster);

    let (status, headers, bytes) =
        send_raw(&state, req_empty("GET", "/_cat/shards?format=json")).await;
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON error");
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["error"]["type"], "control_plane_error", "{body}");
    assert!(
        body["error"]["reason"]
            .as_str()
            .expect("reason")
            .contains("offline"),
        "{body}"
    );
    assert_eq!(
        headers.get(header::CACHE_CONTROL).expect("cache"),
        "no-store"
    );
}
