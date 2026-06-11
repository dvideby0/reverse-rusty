//! Handler tests for coordinator mode (ADR-070): drive the cluster router with
//! tower `oneshot` requests over a real in-process multi-shard `ClusterEngine`.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::{get, post};
use axum::Router;
use parking_lot::{Mutex, RwLock};
use tower::ServiceExt;

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine};
use reverse_rusty::Normalizer;

use crate::metrics::PrometheusMetrics;
use crate::state::ClusterAppState;

use super::*;

fn test_state(queries: &[(u64, String)]) -> Arc<ClusterAppState> {
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..Default::default()
    };
    let cluster = ClusterEngine::build(Normalizer::default_vocab().expect("vocab"), &cfg, queries)
        .expect("cluster builds");
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .expect("pool");
    Arc::new(ClusterAppState {
        cluster: RwLock::new(cluster),
        write_serial: Mutex::new(()),
        pool,
        include_broad: true,
        prom: PrometheusMetrics::new(),
        slow_query_threshold_ms: 0,
        auth: None,
    })
}

fn router(state: &Arc<ClusterAppState>) -> Router {
    Router::new()
        .route("/", get(cluster_root))
        .route(
            "/_doc/{id}",
            get(cluster_get_doc)
                .put(cluster_put_doc)
                .delete(cluster_delete_doc),
        )
        .route("/_search", post(cluster_search))
        .route("/_mpercolate", post(cluster_mpercolate))
        .route("/_bulk", post(cluster_bulk))
        .route("/_flush", post(cluster_flush))
        .route("/_checkpoint", post(cluster_checkpoint))
        .route("/_compact", post(cluster_compact))
        .route("/_stats", get(cluster_stats))
        .route("/_cat/shards", get(cluster_cat_shards))
        .route("/_health", get(cluster_health))
        .route("/_metrics", get(cluster_metrics))
        .route("/_vocab", get(cluster_get_vocab).put(cluster_put_vocab))
        .route("/_vocab/learn", post(cluster_learn_vocab))
        .route(
            "/_vocab/learn_and_apply",
            post(cluster_learn_and_apply_vocab),
        )
        .route("/_vocab/aliases", get(cluster_get_aliases))
        .route(
            "/_settings",
            get(cluster_get_settings).put(cluster_put_settings),
        )
        .route("/_cluster/state", get(cluster_state))
        .route("/_cluster/nodes", post(cluster_register_node))
        .route(
            "/_cluster/nodes/{id}",
            axum::routing::delete(cluster_deregister_node),
        )
        .route("/_cluster/rebalance", post(cluster_rebalance))
        .route("/_cluster/resync", post(cluster_resync))
        .with_state(Arc::clone(state))
}

fn req(method: &str, path: &str, body: &serde_json::Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .expect("request")
}

fn req_empty(method: &str, path: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(path)
        .body(Body::empty())
        .expect("request")
}

async fn send(state: &Arc<ClusterAppState>, r: Request<Body>) -> (StatusCode, serde_json::Value) {
    let resp = router(state).oneshot(r).await.expect("router response");
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("body");
    let json = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    };
    (status, json)
}

fn seed() -> Vec<(u64, String)> {
    vec![
        (1, "1994 topps".to_string()),
        (2, "1995 fleer".to_string()),
        (3, "(rarezza,uniquor)".to_string()),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_reports_cluster_mode() {
    let state = test_state(&seed());
    let (status, body) = send(&state, req_empty("GET", "/")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["mode"], "cluster");
    assert_eq!(body["shards"], 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_search_delete_round_trip() {
    let state = test_state(&seed());

    // Create.
    let (status, body) = send(
        &state,
        req(
            "PUT",
            "/_doc/10",
            &serde_json::json!({"query": "1996 skybox"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["result"], "created");

    // Search finds it (with per-request include_broad).
    let (status, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({"document": {"title": "1996 skybox premium"}, "include_broad": true}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ids: Vec<u64> = body["hits"]["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .map(|h| h["_id"].as_u64().expect("id"))
        .collect();
    assert!(ids.contains(&10), "hits: {ids:?}");

    // Replace (upsert): old stops matching, new matches; 200 updated.
    let (status, body) = send(
        &state,
        req(
            "PUT",
            "/_doc/10",
            &serde_json::json!({"query": "1997 metal"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"], "updated");
    let (_, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({"document": {"title": "1996 skybox premium"}}),
        ),
    )
    .await;
    let old_hits: Vec<u64> = body["hits"]["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .map(|h| h["_id"].as_u64().expect("id"))
        .collect();
    assert!(!old_hits.contains(&10), "old version must stop matching");

    // GET returns the new source.
    let (status, body) = send(&state, req_empty("GET", "/_doc/10")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["_source"]["query"], "1997 metal");

    // Delete; then 404.
    let (status, _) = send(&state, req_empty("DELETE", "/_doc/10")).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(&state, req_empty("GET", "/_doc/10")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejections_are_loud_not_silent() {
    let state = test_state(&seed());

    // Class-D upsert → 400 naming the boundary; the prior version (none) untouched.
    let (status, body) = send(
        &state,
        req("PUT", "/_doc/11", &serde_json::json!({"query": "-onlyneg"})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["result"], "rejected");

    // rank → 400 (criterion 5), never silently un-ranked.
    let (status, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({"document": {"title": "x"}, "rank": {"boosts": []}}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"]["reason"]
        .as_str()
        .expect("reason")
        .contains("rank"));

    // /_compact + PUT /_settings → 501 with the alternative named.
    let (status, body) = send(&state, req_empty("POST", "/_compact")).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert!(body["error"]["reason"]
        .as_str()
        .expect("reason")
        .contains("_checkpoint"));
    let (status, _) = send(
        &state,
        req("PUT", "/_settings", &serde_json::json!({"max_segments": 4})),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_mixes_upserts_and_statuses() {
    let state = test_state(&seed());
    let body = concat!(
        "{\"index\":{\"_id\":21}}\n",
        "{\"query\":\"1996 skybox\"}\n",
        "{\"index\":{\"_id\":22}}\n",
        "{\"query\":\"(((\"}\n",
        "{\"index\":{\"_id\":1}}\n",
        "{\"query\":\"1994 topps gold\"}\n",
    );
    let r = Request::builder()
        .method("POST")
        .uri("/_bulk")
        .header("content-type", "application/x-ndjson")
        .body(Body::from(body))
        .expect("request");
    let (status, json) = send(&state, r).await;
    assert_eq!(status, StatusCode::OK, "{json}");
    let items = json["items"].as_array().expect("items");
    assert_eq!(items.len(), 3);
    assert_eq!(items[0]["index"]["status"], 201, "fresh id created");
    assert_eq!(items[1]["index"]["status"], 400, "parse error rejected");
    assert_eq!(items[2]["index"]["status"], 200, "existing id updated");
    assert_eq!(json["errors"], true);

    // The bulk upsert of id 1 replaced its DSL: the old form no longer matches.
    let (_, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({"document": {"title": "1994 topps"}}),
        ),
    )
    .await;
    let ids: Vec<u64> = body["hits"]["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .map(|h| h["_id"].as_u64().expect("id"))
        .collect();
    assert!(
        !ids.contains(&1),
        "replaced query must need the new form: {ids:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mpercolate_returns_per_document_responses() {
    let state = test_state(&seed());
    let (status, body) = send(
        &state,
        req(
            "POST",
            "/_mpercolate",
            &serde_json::json!({"documents": [
                {"title": "1994 topps"},
                {"title": "1995 fleer ultra"},
                {"title": "nothing here"}
            ]}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let responses = body["responses"].as_array().expect("responses");
    assert_eq!(responses.len(), 3);
    assert_eq!(responses[0]["hits"]["total"], 1);
    assert_eq!(responses[1]["hits"]["total"], 1);
    assert_eq!(responses[2]["hits"]["total"], 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_health_shards_and_cluster_ops() {
    let state = test_state(&seed());

    let (status, body) = send(&state, req_empty("GET", "/_stats")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["shards"], 3);
    assert!(body["total_queries"].as_u64().expect("count") >= 3);
    assert_eq!(body["pending_repairs"], 0);

    let (status, body) = send(&state, req_empty("GET", "/_health")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "green");

    let (status, _) = send(&state, req_empty("GET", "/_cat/shards")).await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = send(&state, req_empty("GET", "/_cat/shards?format=json")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_array().expect("rows").len(), 3);

    let (status, body) = send(&state, req_empty("GET", "/_cluster/state")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["num_shards"], 3);

    // Register a node, rebalance, deregister — the control-plane round trip.
    let (status, _) = send(
        &state,
        req(
            "POST",
            "/_cluster/nodes",
            &serde_json::json!({"id": 7, "addr": "http://127.0.0.1:50057"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = send(&state, req_empty("POST", "/_cluster/rebalance")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, _) = send(&state, req_empty("DELETE", "/_cluster/nodes/7")).await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send(&state, req_empty("POST", "/_cluster/resync")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["repaired"], 0);

    // Percolation still correct after the control-plane churn (zero-FN posture).
    let (_, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({"document": {"title": "1994 topps"}}),
        ),
    )
    .await;
    assert_eq!(body["hits"]["total"], 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vocab_alias_makes_both_forms_match() {
    let state = test_state(&seed());
    // Declare an equivalence (ADR-054 expansion): ud ≡ upperdeck.
    let vocab = serde_json::json!({
        "equivalences": [["ud", "upperdeck"]]
    });
    let (status, body) = send(&state, req("PUT", "/_vocab", &vocab)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["acknowledged"], true);

    // A query in one form must now match a title in the other.
    let (status, _) = send(
        &state,
        req(
            "PUT",
            "/_doc/30",
            &serde_json::json!({"query": "upperdeck 1994"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (_, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({"document": {"title": "ud 1994"}}),
        ),
    )
    .await;
    let ids: Vec<u64> = body["hits"]["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .map(|h| h["_id"].as_u64().expect("id"))
        .collect();
    assert!(
        ids.contains(&30),
        "alias must make both forms match: {ids:?}"
    );

    let (status, body) = send(&state, req_empty("GET", "/_vocab")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["equivalences"].as_array().is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn filtered_search_narrows_by_tags() {
    let state = test_state(&[]);
    // Tagged adds (post-build tags resolve synthetically — same TagIds everywhere).
    let (status, _) = send(
        &state,
        req(
            "PUT",
            "/_doc/41",
            &serde_json::json!({"query": "1994 topps", "tags": {"category": "cards"}}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = send(
        &state,
        req(
            "PUT",
            "/_doc/42",
            &serde_json::json!({"query": "1994 topps", "tags": {"category": "comics"}}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Unfiltered: both; filtered: one (filtering only removes).
    let (_, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({"document": {"title": "1994 topps"}}),
        ),
    )
    .await;
    assert_eq!(body["hits"]["total"], 2);
    let (_, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({
                "document": {"title": "1994 topps"},
                "filter": {"category": "cards"}
            }),
        ),
    )
    .await;
    assert_eq!(body["hits"]["total"], 1);
    assert_eq!(body["hits"]["hits"][0]["_id"], 41);

    // A tagged cluster ACCEPTS a vocab change (ADR-074): the rebuild carries each query's
    // stored TagIds, so the filter still narrows identically afterwards.
    let (status, body) = send(
        &state,
        req(
            "PUT",
            "/_vocab",
            &serde_json::json!({"equivalences": [["a","b"]]}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({
                "document": {"title": "1994 topps"},
                "filter": {"category": "cards"}
            }),
        ),
    )
    .await;
    assert_eq!(
        body["hits"]["total"], 1,
        "the synthetic tag must survive the rebuild: {body}"
    );
    assert_eq!(body["hits"]["hits"][0]["_id"], 41);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_acknowledges_and_bumps_epoch() {
    let state = test_state(&seed());
    // In-memory cluster: checkpoint is a no-op but still acknowledged (epoch 0).
    let (status, body) = send(&state, req_empty("POST", "/_checkpoint")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["acknowledged"], true);
    let (status, body) = send(&state, req_empty("POST", "/_flush")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}
