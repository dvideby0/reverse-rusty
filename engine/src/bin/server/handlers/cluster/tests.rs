//! Handler tests for coordinator mode (ADR-070): drive the cluster router with
//! tower `oneshot` requests over a real in-process multi-shard `ClusterEngine`.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::{get, post};
use axum::Router;
use parking_lot::{Mutex, RwLock};
use tower::ServiceExt;

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, ShardError};
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
    state_from_cluster(cluster)
}

fn state_from_cluster(cluster: ClusterEngine) -> Arc<ClusterAppState> {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .expect("pool");
    let prom = PrometheusMetrics::new();
    Arc::new(ClusterAppState {
        cluster: RwLock::new(cluster),
        write_serial: Mutex::new(()),
        pool,
        search_permits: None,
        ranked_search_permits: Arc::new(tokio::sync::Semaphore::new(2)),
        exhaustive_jobs: crate::jobs::ExhaustiveJobs::for_tests(prom.clone()),
        max_ranked_enrichment_bytes: crate::state::DEFAULT_MAX_RANKED_ENRICHMENT_BYTES,
        include_broad: true,
        prom,
        slow_query_threshold_ms: 0,
        auth: None,
        pit_tokens: crate::pit::PitTokens::generate(),
        pit_config: reverse_rusty::PitConfig::default(),
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
        .route("/v2/_search", post(crate::handlers::cluster_v2_search))
        .route(
            "/v2/_pit",
            post(crate::handlers::cluster_open_pit).delete(crate::handlers::cluster_close_pit),
        )
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

#[test]
fn write_error_metrics_and_responses_share_the_same_status_classification() {
    for (error, expected) in [
        (
            ShardError::Config("unseeded directory".into()),
            StatusCode::BAD_REQUEST,
        ),
        (ShardError::Remote("down".into()), StatusCode::BAD_GATEWAY),
        (
            ShardError::Log("unavailable".into()),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
    ] {
        assert_eq!(shard_error_status(&error), expected);
        assert_eq!(
            shard_error_response("document write rejected", &error).status(),
            expected
        );
    }
}

#[tokio::test]
async fn cluster_v2_search_is_exact_enriched_and_reports_routed_positions() {
    let state = test_state(&[
        (3, "topps chrome".to_string()),
        (1, "topps chrome".to_string()),
        (2, "topps chrome".to_string()),
    ]);
    let (status, json) = send(
        &state,
        req(
            "POST",
            "/v2/_search",
            &serde_json::json!({
                "document": {"title": "2020 topps chrome update"},
                "explain": true
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(json["complete"], true);
    assert_eq!(
        json["hits"]["total"],
        serde_json::json!({"value":3,"relation":"eq"})
    );
    let ids: Vec<u64> = json["hits"]["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .map(|hit| hit["_id"].as_u64().expect("id"))
        .collect();
    assert_eq!(ids, vec![1, 2, 3], "score ties break by logical id");
    assert!(json["hits"]["hits"][0]["_source"]["query"].is_string());
    assert!(json["hits"]["hits"][0]["_explanation"].is_object());
    let routed = json["_shards"]["total"].as_u64().expect("routed shards");
    assert!((1..=3).contains(&routed));
    assert_eq!(json["_shards"]["successful"], routed);
    assert_eq!(json["_shards"]["failed"], 0);
}

#[tokio::test]
async fn cluster_v2_search_enforces_validation_deadline_and_enrichment_cap() {
    let state = test_state(&[(1, "topps chrome".to_string())]);
    let (status, json) = send(
        &state,
        req(
            "POST",
            "/v2/_search",
            &serde_json::json!({
                "document": {"title": "topps chrome"},
                "allow_partial_results": true
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");

    let mut capped = test_state(&[(1, "topps chrome".to_string())]);
    Arc::get_mut(&mut capped)
        .expect("unique state")
        .max_ranked_enrichment_bytes = 1;
    let (status, json) = send(
        &capped,
        req(
            "POST",
            "/v2/_search",
            &serde_json::json!({"document": {"title": "topps chrome"}}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{json}");
    assert_eq!(json["error"]["type"], "rank_enrichment_limit");

    let mut queued = test_state(&[(1, "topps chrome".to_string())]);
    Arc::get_mut(&mut queued)
        .expect("unique state")
        .ranked_search_permits = Arc::new(tokio::sync::Semaphore::new(0));
    let (status, json) = send(
        &queued,
        req(
            "POST",
            "/v2/_search",
            &serde_json::json!({
                "document": {"title": "topps chrome"},
                "timeout_ms": 1
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::REQUEST_TIMEOUT, "{json}");
    assert_eq!(queued.prom.ranked_search_permits_in_use.get(), 0);
}

#[tokio::test]
async fn cluster_v2_uses_typed_priority_from_post_freeze_http_writes() {
    let state = test_state(&[]);
    for (id, priority) in [(2u64, -5), (1, 20)] {
        let (status, json) = send(
            &state,
            req(
                "PUT",
                &format!("/_doc/{id}"),
                &serde_json::json!({
                    "query": "zzhttprank",
                    "rank_fields": {"priority": priority}
                }),
            ),
        )
        .await;
        assert!(status.is_success(), "{json}");
    }
    let (status, json) = send(
        &state,
        req(
            "POST",
            "/v2/_search",
            &serde_json::json!({
                "document": {"title": "zzhttprank"},
                "include_source": false
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(json["hits"]["hits"][0]["_id"], 1);
    assert_eq!(json["hits"]["hits"][0]["_score"], 20);
    assert_eq!(json["hits"]["hits"][1]["_id"], 2);
    assert_eq!(json["hits"]["hits"][1]["_score"], -5);
}

#[tokio::test]
async fn cluster_v2_missing_winner_source_is_a_no_partial_502() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "reverse_rusty_cluster_v2_missing_source_{}_{}",
        std::process::id(),
        nonce
    ));
    let cfg = ClusterConfig {
        num_shards: 3,
        data_dir: Some(dir.clone()),
        ..ClusterConfig::default()
    };
    {
        let cluster = ClusterEngine::build(
            Normalizer::default_vocab().expect("vocab"),
            &cfg,
            &[(1, "topps chrome".to_string())],
        )
        .expect("durable cluster");
        cluster.flush().expect("flush");
        cluster.checkpoint().expect("checkpoint");
    }
    for shard in 0..3 {
        let _ = std::fs::remove_file(dir.join(format!("shard_{shard:03}")).join("sources.dat"));
    }
    let cluster = ClusterEngine::open(
        dir.clone(),
        Normalizer::default_vocab().expect("vocab"),
        None,
    )
    .expect("source-less cluster reopen");
    let state = state_from_cluster(cluster);

    let (status, json) = send(&state, req_empty("HEAD", "/_doc/1")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "coordinator HEAD must use exact-index liveness even without sources.dat"
    );
    assert!(json.is_null(), "HEAD must be bodyless");

    let (status, json) = send(&state, req_empty("GET", "/_doc/1")).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{json}");
    assert_eq!(json["error"]["type"], "source_unavailable");

    let (status, json) = send(
        &state,
        req(
            "POST",
            "/v2/_search",
            &serde_json::json!({"document": {"title": "topps chrome"}}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{json}");
    assert_eq!(json["error"]["type"], "source_unavailable");
    assert!(json.get("hits").is_none(), "partial hits must never escape");
    let _ = std::fs::remove_dir_all(dir);
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
    assert_eq!(body["name"], "reverse-rusty");
    assert_eq!(body["cluster_name"], "reverse-rusty");
    assert_eq!(body["cluster_uuid"], "_na_");
    assert_eq!(body["version"]["distribution"], "reverse-rusty");
    assert_eq!(body["version"]["number"], env!("CARGO_PKG_VERSION"));
    assert_eq!(body["mode"], "cluster");
    assert_eq!(body["shards"], 3);

    let response = router(&state)
        .oneshot(req_empty("HEAD", "/"))
        .await
        .expect("router response");
    assert_eq!(response.status(), StatusCode::OK);
    assert!(axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("HEAD body")
        .is_empty());
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
    assert_eq!(body["_index"], "queries");
    assert_eq!(body["_id"], 10);
    assert_eq!(body["_version"], 1);
    assert_eq!(body["result"], "created");
    assert!(body.get("error").is_none());

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
    assert_eq!(body["_index"], "queries");
    assert_eq!(body["_version"], 1);
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
async fn put_doc_create_only_and_query_parameter_contract_match_single_node() {
    let state = test_state(&seed());
    let first = send(
        &state,
        req(
            "PUT",
            "/_doc/70?op_type=create&refresh=wait_for",
            &serde_json::json!({"query":"michael jordan","version":7}),
        ),
    );
    let second = send(
        &state,
        req(
            "PUT",
            "/_doc/70?op_type=create&refresh=true",
            &serde_json::json!({"query":"lebron james","version":8}),
        ),
    );
    let (a, b) = tokio::join!(first, second);
    let mut statuses = [a.0, b.0];
    statuses.sort_by_key(StatusCode::as_u16);
    assert_eq!(statuses, [StatusCode::CREATED, StatusCode::CONFLICT]);
    let (created, conflict) = if a.0 == StatusCode::CREATED {
        (a.1, b.1)
    } else {
        (b.1, a.1)
    };
    assert_eq!(created["_index"], "queries");
    assert!(
        created["_version"] == 7 || created["_version"] == 8,
        "the winning caller's display version is returned"
    );
    assert_eq!(
        conflict["error"]["type"],
        "version_conflict_engine_exception"
    );

    let (status, current) = send(&state, req_empty("GET", "/_doc/70")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        current["_source"]["query"] == "michael jordan"
            || current["_source"]["query"] == "lebron james",
        "one complete create body wins"
    );
    assert_eq!(current["_version"], created["_version"]);

    let (status, malformed_conflict) = send(
        &state,
        req(
            "PUT",
            "/_doc/70?op_type=create",
            &serde_json::json!({"query":"("}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        malformed_conflict["error"]["type"],
        "version_conflict_engine_exception"
    );

    let (status, invalid) = send(
        &state,
        req(
            "PUT",
            "/_doc/71?routing=custom",
            &serde_json::json!({"query":"wayne gretzky"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{invalid}");
    assert_eq!(invalid["error"]["type"], "illegal_argument_exception");
    let (status, _) = send(&state, req_empty("HEAD", "/_doc/71")).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "unsupported parameters reject before mutation"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_doc_reads_back_post_freeze_tags_filters_and_head_status() {
    // The seed freezes an empty tag dictionary. These tags therefore use the
    // synthetic-id path internally; GET must read the canonical raw metadata
    // retained with the source, not attempt an impossible TagId reverse lookup.
    let state = test_state(&seed());
    let (status, _) = send(
        &state,
        req(
            "PUT",
            "/_doc/71",
            &serde_json::json!({
                "query": "topps chrome",
                "version": 9,
                "tags": {"tenant": "acme", "colors": ["red", "blue"]}
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = send(&state, req_empty("GET", "/_doc/71")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_index"], "queries");
    assert_eq!(body["_version"], 9);
    assert_eq!(body["_source"]["query"], "topps chrome");
    assert_eq!(body["_source"]["tags"]["tenant"], "acme");
    assert_eq!(
        body["_source"]["tags"]["colors"],
        serde_json::json!(["blue", "red"])
    );

    let (status, body) = send(
        &state,
        req_empty("GET", "/_doc/71?_source_includes=tags.tenant"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["_source"],
        serde_json::json!({"tags": {"tenant": "acme"}})
    );

    for (path, expected) in [
        ("/_doc/71", StatusCode::OK),
        ("/_doc/72", StatusCode::NOT_FOUND),
    ] {
        let (status, body) = send(&state, req_empty("HEAD", path)).await;
        assert_eq!(status, expected);
        assert!(body.is_null(), "HEAD must be bodyless");
    }
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

    // explain → 400, never silently un-explained. (`rank` is SUPPORTED since ADR-075 —
    // covered by `ranked_search_orders_by_score`.)
    let (status, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({"document": {"title": "x"}, "explain": true}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"]["reason"]
        .as_str()
        .expect("reason")
        .contains("explain"));

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
async fn ranked_search_orders_by_score() {
    // Cluster `rank` (ADR-075): boosts resolve against the shared tag space (synthetic
    // ids included — boost matching is id-equality), hits come back `(score desc, _id
    // asc)` with `_score`, and `from`/`size` slice the RANKED order. The unranked path
    // stays byte-identical (no `_score`, ascending ids).
    let state = test_state(&[]);
    for (id, q, tier) in [
        (41u64, "1994 topps", "gold"),
        (42, "1994 topps", "silver"),
        (43, "1994 topps", "bronze"),
    ] {
        let (status, _) = send(
            &state,
            req(
                "PUT",
                &format!("/_doc/{id}"),
                &serde_json::json!({"query": q, "tags": {"tier": tier}}),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
    }

    let rank = serde_json::json!({"boosts": [
        {"key": "tier", "value": "gold", "boost": 100},
        {"key": "tier", "value": "silver", "boost": 40}
    ]});
    let (status, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({"document": {"title": "1994 topps"}, "rank": rank}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let hits = body["hits"]["hits"].as_array().expect("hits");
    let got: Vec<(u64, i64)> = hits
        .iter()
        .map(|h| {
            (
                h["_id"].as_u64().expect("id"),
                h["_score"].as_i64().expect("ranked hits carry _score"),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![(41, 100), (42, 40), (43, 0)],
        "(score desc, _id asc) with boost scores"
    );

    // from/size slice the RANKED order.
    let (_, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({
                "document": {"title": "1994 topps"},
                "rank": rank, "from": 1, "size": 1
            }),
        ),
    )
    .await;
    assert_eq!(body["hits"]["hits"][0]["_id"], 42);

    // Unranked stays byte-identical: ascending ids, no _score key.
    let (_, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({"document": {"title": "1994 topps"}}),
        ),
    )
    .await;
    let hits = body["hits"]["hits"].as_array().expect("hits");
    assert_eq!(hits[0]["_id"], 41);
    assert!(
        hits[0].get("_score").is_none(),
        "unranked hits must not grow a _score"
    );

    // /_mpercolate honors the same rank block per slot.
    let (status, body) = send(
        &state,
        req(
            "POST",
            "/_mpercolate",
            &serde_json::json!({
                "documents": [{"title": "1994 topps"}],
                "rank": rank
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["responses"][0]["hits"]["hits"][0]["_id"], 41);
    assert_eq!(body["responses"][0]["hits"]["hits"][0]["_score"], 100);

    // A PRESENT-but-empty rank block is a no-op — byte-identical to unranked
    // (single-node parity, review finding): ascending ids, NO `_score` key.
    for noop in [serde_json::json!({}), serde_json::json!({"boosts": []})] {
        let (status, body) = send(
            &state,
            req(
                "POST",
                "/_search",
                &serde_json::json!({"document": {"title": "1994 topps"}, "rank": noop}),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let hits = body["hits"]["hits"].as_array().expect("hits");
        assert_eq!(
            hits[0]["_id"], 41,
            "no-op rank keeps engine (ascending) order"
        );
        assert!(
            hits[0].get("_score").is_none(),
            "a no-op rank block must not grow a _score: {body}"
        );
    }
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

// ---- cooperative cancellation (ADR-099, cluster mode) --------------------------

#[tokio::test]
async fn cluster_search_explicit_zero_timeout_cancels_and_408s() {
    let state = test_state(&[(1, "michael jordan".to_string())]);

    // An explicit timeout_ms arms the per-title cooperative check in
    // percolate_blocking; 0ms is expired before the first title evaluates, so the
    // request 408s deterministically and the cancellation is RECORDED (the counter
    // lives in the blocking closure, so it counts even after the 408 went out).
    let (code, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({
                "documents": [{"title": "michael jordan rookie"}, {"title": "some other"}],
                "include_source": false,
                "timeout_ms": 0,
            }),
        ),
    )
    .await;
    assert_eq!(code, StatusCode::REQUEST_TIMEOUT, "got body: {body}");

    for _ in 0..200 {
        if state
            .prom
            .match_cancellations_total
            .with_label_values(&["search"])
            .get()
            >= 1
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        state
            .prom
            .match_cancellations_total
            .with_label_values(&["search"])
            .get()
            >= 1,
        "the cancelled cluster percolate must record that its work stopped"
    );

    // A shard failure is a 502, never masked by cancellation: the un-armed default
    // path still serves fine (sanity that the seam did not disturb normal serving).
    let (ok_code, ok_body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({
                "document": {"title": "michael jordan rookie"},
                "include_source": false,
            }),
        ),
    )
    .await;
    assert_eq!(ok_code, StatusCode::OK, "unarmed serving intact: {ok_body}");
    assert_eq!(ok_body["hits"]["total"], 1);
}

/// ADR-113 over the coordinator HTTP surface: open a PIT, page a ranked search
/// to exhaustion following `next_cursor` (concat ≡ the one-shot over the same
/// PIT, totals page-invariant), then a `resize` stales the cursor as the one
/// deliberate read-surface 409 — and an explicit DELETE releases the registry.
#[tokio::test]
async fn cluster_v2_pit_pages_concatenate_and_stale_after_resize() {
    let queries: Vec<(u64, String)> = (1..=20).map(|i| (i, "topps chrome".to_string())).collect();
    let tags: Vec<Vec<(String, String)>> = queries
        .iter()
        .map(|(id, _)| vec![("priority".to_string(), ((id % 6) * 10).to_string())])
        .collect();
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build_with_tags(
        Normalizer::default_vocab().expect("vocab"),
        &cfg,
        &queries,
        &tags,
    )
    .expect("tagged cluster");
    let state = state_from_cluster(cluster);

    let (status, opened) = send(&state, req("POST", "/v2/_pit", &serde_json::json!({}))).await;
    assert_eq!(status, StatusCode::OK);
    let pit = opened["pit_id"].as_str().expect("pit token").to_string();

    let body = |extra: serde_json::Value| {
        let mut base = serde_json::json!({
            "document": {"title": "2020 topps chrome update"},
            "include_source": false,
            "rank": {"priority_field": "priority"},
        });
        for (k, v) in extra.as_object().expect("extra") {
            base[k] = v.clone();
        }
        base
    };

    let (status, one_shot) = send(
        &state,
        req(
            "POST",
            "/v2/_search",
            &body(serde_json::json!({"size": 1_000, "pit": {"id": pit}})),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let expected: Vec<(u64, i64)> = one_shot["hits"]["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .map(|hit| {
            (
                hit["_id"].as_u64().unwrap(),
                hit["_score"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(expected.len(), 20);
    assert!(
        one_shot["next_cursor"].is_null(),
        "short page ends the stream"
    );

    let mut pages: Vec<(u64, i64)> = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let page_body = match &cursor {
            None => body(serde_json::json!({"size": 7, "pit": {"id": pit}})),
            Some(token) => body(serde_json::json!({"size": 7, "cursor": token})),
        };
        let (status, page) = send(&state, req("POST", "/v2/_search", &page_body)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            page["hits"]["total"], one_shot["hits"]["total"],
            "pinned totals are page-invariant"
        );
        pages.extend(
            page["hits"]["hits"]
                .as_array()
                .expect("hits")
                .iter()
                .map(|hit| {
                    (
                        hit["_id"].as_u64().unwrap(),
                        hit["_score"].as_i64().unwrap(),
                    )
                }),
        );
        match page["next_cursor"].as_str() {
            Some(token) => cursor = Some(token.to_string()),
            None => break,
        }
    }
    assert_eq!(pages, expected, "concatenated pages equal the one-shot");
    let live_cursor = cursor;

    // A resize (placement-generation bump) stales any further cursor use as
    // the ADR-113 read-surface 409 — never a silently mixed page. (The paging
    // loop above ended with next_cursor null; re-page from the pit instead.)
    state.cluster.write().resize(4).expect("resize");
    let (status, stale) = send(
        &state,
        req(
            "POST",
            "/v2/_search",
            &body(serde_json::json!({"size": 7, "pit": {"id": pit}})),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(stale["error"]["type"], "stale_cursor");
    let _ = live_cursor;

    // Close after staleness: goal state already achieved, closed:false, 200.
    let (status, closed) = send(
        &state,
        req("DELETE", "/v2/_pit", &serde_json::json!({"pit_id": pit})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(closed["closed"], false);
}
