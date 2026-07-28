use super::*;

fn bulk_router(state: &Arc<AppState>) -> Router {
    Router::new()
        .route("/_bulk", axum::routing::post(bulk_route))
        .with_state(Arc::clone(state))
}

async fn send_bulk(
    state: &Arc<AppState>,
    path: &str,
    content_type: Option<&str>,
    body: impl Into<Body>,
) -> (StatusCode, serde_json::Value) {
    let mut request = Request::post(path);
    if let Some(content_type) = content_type {
        request = request.header("content-type", content_type);
    }
    let response = bulk_router(state)
        .oneshot(request.body(body.into()).expect("request"))
        .await
        .expect("response");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    let body = serde_json::from_slice(&bytes).expect("JSON response");
    (status, body)
}

#[tokio::test]
async fn index_replaces_create_conflicts_and_response_is_es_familiar() {
    let mut engine = Engine::new(Normalizer::default_vocab().expect("vocab"));
    engine
        .try_insert_live("wireless mouse", 1, 1)
        .expect("seed");
    let state = state_with_engine(engine);
    let body = concat!(
        "{\"index\":{\"_index\":\"queries\",\"_id\":\"1\",\"require_alias\":false}}\n",
        "{\"query\":\"mechanical keyboard\",\"version\":7,\"rank_fields\":{\"priority\":50}}\n",
        "{\"create\":{\"_id\":2}}\n",
        "{\"query\":\"1996 vertex\",\"version\":3}\n",
        "{\"create\":{\"_id\":1}}\n",
        "{\"query\":\"must not replace\",\"version\":9}\n",
    );
    let (status, response) = send_bulk(
        &state,
        "/_bulk?refresh=wait_for&require_alias=false",
        Some("application/x-ndjson"),
        body,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert!(response["took"].is_u64(), "{response}");
    assert!(response["took_ms"].is_f64(), "{response}");
    assert_eq!(response["errors"], true);
    assert_eq!(response["items"][0]["index"]["status"], 200);
    assert_eq!(response["items"][0]["index"]["result"], "updated");
    assert_eq!(response["items"][0]["index"]["_index"], "queries");
    assert_eq!(response["items"][0]["index"]["_id"], 1);
    assert_eq!(response["items"][0]["index"]["_version"], 7);
    assert_eq!(response["items"][1]["create"]["status"], 201);
    assert_eq!(response["items"][1]["create"]["result"], "created");
    assert_eq!(response["items"][1]["create"]["_version"], 3);
    assert_eq!(response["items"][2]["create"]["status"], 409);
    assert_eq!(
        response["items"][2]["create"]["error"]["type"],
        "version_conflict_engine_exception"
    );

    assert!(
        !matches_in_snapshot(&state, "wireless mouse").contains(&1),
        "index must replace, not retain the old same-id query"
    );
    assert!(matches_in_snapshot(&state, "mechanical keyboard").contains(&1));
    let snapshot = state.snapshot.load();
    assert_eq!(
        snapshot
            .get_query_document(1)
            .expect("updated source")
            .version(),
        7
    );
}

#[tokio::test]
async fn fresh_unique_items_keep_the_direct_segment_fast_path() {
    let state = state();
    let before = state.engine.lock().snapshot().segment_infos().len();
    let body = concat!(
        "{\"index\":{\"_id\":10}}\n",
        "{\"query\":\"acme chrome\",\"rank_fields\":{\"priority\":25}}\n",
        "{\"create\":{\"_id\":11}}\n",
        "{\"query\":\"1995 vertex ultra\"}\n",
    );
    let (status, response) = send_bulk(
        &state,
        "/_bulk",
        Some("application/json; charset=utf-8"),
        body,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["errors"], false, "{response}");
    assert_eq!(response["items"][0]["index"]["status"], 201);
    assert_eq!(response["items"][1]["create"]["status"], 201);
    let after = state.engine.lock().snapshot().segment_infos().len();
    assert_eq!(
        after,
        before + 1,
        "fresh unique bulk should compile one immutable segment"
    );
}

#[tokio::test]
async fn source_failures_are_per_item_and_do_not_desynchronize_later_actions() {
    let state = state();
    let body = concat!(
        "{\"index\":{\"_id\":20}}\n",
        "{\"query\":\"acme chrome\"}\n",
        "{\"index\":{\"_id\":21}}\n",
        "{\n",
        "{\"index\":{\"_id\":22}}\n",
        "{\"tags\":{\"category\":\"items\"}}\n",
        "{\"index\":{\"_id\":23}}\n",
        "{\"query\":\"(((\"}\n",
        "{\"index\":{\"_id\":24}}\n",
        "{\"query\":\"1996 vertex\"}\n",
    );
    let (status, response) = send_bulk(&state, "/_bulk", Some("application/x-ndjson"), body).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["errors"], true, "{response}");
    let items = response["items"].as_array().expect("items");
    assert_eq!(items.len(), 5);
    assert_eq!(items[0]["index"]["status"], 201);
    assert_eq!(
        items[1]["index"]["error"]["type"],
        "document_parsing_exception"
    );
    assert_eq!(items[2]["index"]["status"], 400);
    assert_eq!(items[3]["index"]["error"]["type"], "parse_exception");
    assert_eq!(items[4]["index"]["status"], 201);
    assert!(matches_in_snapshot(&state, "acme chrome").contains(&20));
    assert!(matches_in_snapshot(&state, "1996 vertex").contains(&24));
}

#[tokio::test]
async fn repeated_ids_execute_in_order_and_structural_errors_preflight_the_batch() {
    let state = state();
    let ordered = concat!(
        "{\"index\":{\"_id\":30}}\n",
        "{\"query\":\"acme chrome\"}\n",
        "{\"index\":{\"_id\":30}}\n",
        "{\"query\":\"vertex ultra\"}\n",
        "{\"create\":{\"_id\":30}}\n",
        "{\"query\":\"must not replace\"}\n",
    );
    let (status, response) = send_bulk(
        &state,
        "/_bulk?refresh=false",
        Some("application/x-ndjson"),
        ordered,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["items"][0]["index"]["status"], 201);
    assert_eq!(response["items"][1]["index"]["status"], 200);
    assert_eq!(response["items"][1]["index"]["result"], "updated");
    assert_eq!(response["items"][2]["create"]["status"], 409);
    assert!(!matches_in_snapshot(&state, "acme chrome").contains(&30));
    assert!(matches_in_snapshot(&state, "vertex ultra").contains(&30));
    assert!(!matches_in_snapshot(&state, "must not replace").contains(&30));

    let invalid = concat!(
        "{\"index\":{\"_id\":31}}\n",
        "{\"query\":\"should not commit\"}\n",
        "{\"delete\":{\"_id\":30}}\n",
        "{\"query\":\"unused\"}\n",
    );
    let (status, response) =
        send_bulk(&state, "/_bulk", Some("application/x-ndjson"), invalid).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{response}");
    assert!(
        !matches_in_snapshot(&state, "should not commit").contains(&31),
        "a structural error must reject the request before earlier pairs mutate"
    );
    assert!(
        matches_in_snapshot(&state, "vertex ultra").contains(&30),
        "an unsupported delete must not mutate its target"
    );
}

#[tokio::test]
async fn an_all_rejected_ordered_batch_still_publishes_engine_diagnostics() {
    let state = state();
    let before = state.snapshot.load().rejected_class_d();
    let body = "{\"index\":{\"_id\":40}}\n{\"query\":\"-used\",\"version\":2}\n";
    let (status, response) = send_bulk(&state, "/_bulk", Some("application/x-ndjson"), body).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["errors"], true);
    assert_eq!(response["items"][0]["index"]["status"], 400);
    assert_eq!(
        state.snapshot.load().rejected_class_d(),
        before + 1,
        "the lock-free stats snapshot must see diagnostics from an all-rejected live-write pass"
    );
}

#[tokio::test]
async fn transport_and_action_envelope_are_strict_and_structured() {
    let state = state();
    let valid = "{\"index\":{\"_id\":1}}\n{\"query\":\"acme chrome\"}\n";
    for (label, path, content_type, body, expected) in [
        (
            "missing content type",
            "/_bulk",
            None,
            valid,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
        ),
        (
            "unknown query parameter",
            "/_bulk?routing=one",
            Some("application/x-ndjson"),
            valid,
            StatusCode::BAD_REQUEST,
        ),
        (
            "require alias",
            "/_bulk?require_alias=true",
            Some("application/x-ndjson"),
            valid,
            StatusCode::BAD_REQUEST,
        ),
        (
            "missing final newline",
            "/_bulk",
            Some("application/x-ndjson"),
            "{\"index\":{\"_id\":1}}\n{\"query\":\"x\"}",
            StatusCode::BAD_REQUEST,
        ),
        (
            "blank line",
            "/_bulk",
            Some("application/x-ndjson"),
            "{\"index\":{\"_id\":1}}\n\n{\"query\":\"x\"}\n",
            StatusCode::BAD_REQUEST,
        ),
        (
            "flat action",
            "/_bulk",
            Some("application/x-ndjson"),
            "{\"_id\":1}\n{\"query\":\"x\"}\n",
            StatusCode::BAD_REQUEST,
        ),
        (
            "unsupported operation",
            "/_bulk",
            Some("application/x-ndjson"),
            "{\"update\":{\"_id\":1}}\n{\"doc\":{\"query\":\"x\"}}\n",
            StatusCode::BAD_REQUEST,
        ),
        (
            "wrong index",
            "/_bulk",
            Some("application/x-ndjson"),
            "{\"index\":{\"_index\":\"other\",\"_id\":1}}\n{\"query\":\"x\"}\n",
            StatusCode::BAD_REQUEST,
        ),
        (
            "unsupported action metadata",
            "/_bulk",
            Some("application/x-ndjson"),
            "{\"index\":{\"_id\":1,\"routing\":\"one\"}}\n{\"query\":\"x\"}\n",
            StatusCode::BAD_REQUEST,
        ),
    ] {
        let (status, response) = send_bulk(&state, path, content_type, body).await;
        assert_eq!(status, expected, "{label}: {response}");
        assert_eq!(
            response["status"],
            u64::from(expected.as_u16()),
            "{label}: {response}"
        );
        assert!(response["error"]["type"].is_string(), "{label}: {response}");
    }
}

#[tokio::test]
async fn body_limit_and_post_only_route_preserve_http_statuses() {
    use axum::extract::DefaultBodyLimit;

    let state = state();
    let body = "{\"index\":{\"_id\":1}}\n{\"query\":\"acme chrome\"}\n";
    let response = bulk_router(&state)
        .layer(DefaultBodyLimit::max(16))
        .oneshot(
            Request::post("/_bulk")
                .header("content-type", "application/x-ndjson")
                .body(Body::from(body))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON response");
    assert_eq!(json["status"], 413, "{json}");

    let response = bulk_router(&state)
        .oneshot(Request::get("/_bulk").body(Body::empty()).expect("request"))
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        response
            .headers()
            .get("allow")
            .and_then(|value| value.to_str().ok()),
        Some("POST")
    );
}
