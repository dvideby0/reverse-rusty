use super::*;

fn bulk_request(path: &str, content_type: Option<&str>, body: &str) -> Request<Body> {
    let mut request = Request::post(path);
    if let Some(content_type) = content_type {
        request = request.header("content-type", content_type);
    }
    request.body(Body::from(body.to_string())).expect("request")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_bulk_shares_index_create_version_and_response_contract() {
    let state = test_state(&seed());
    let body = concat!(
        "{\"index\":{\"_index\":\"queries\",\"_id\":\"1\"}}\n",
        "{\"query\":\"1994 acme gold\",\"version\":7,\"rank_fields\":{\"priority\":50}}\n",
        "{\"create\":{\"_id\":10,\"_require_alias\":false}}\n",
        "{\"query\":\"1996 vertex\",\"version\":3}\n",
        "{\"create\":{\"_id\":1}}\n",
        "{\"query\":\"must not replace\",\"version\":9}\n",
    );
    let (status, response) = send(
        &state,
        bulk_request(
            "/_bulk?refresh=true&require_alias=false",
            Some("application/x-ndjson"),
            body,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert!(response["took"].is_u64(), "{response}");
    assert!(response["took_ms"].is_f64(), "{response}");
    assert_eq!(response["errors"], true);
    assert_eq!(response["items"][0]["index"]["status"], 200);
    assert_eq!(response["items"][0]["index"]["result"], "updated");
    assert_eq!(response["items"][0]["index"]["_version"], 7);
    assert_eq!(response["items"][1]["create"]["status"], 201);
    assert_eq!(response["items"][1]["create"]["result"], "created");
    assert_eq!(response["items"][1]["create"]["_version"], 3);
    assert_eq!(response["items"][2]["create"]["status"], 409);
    assert_eq!(
        response["items"][2]["create"]["error"]["type"],
        "version_conflict_engine_exception"
    );

    let (status, document) = send(
        &state,
        Request::get("/_doc/1")
            .body(Body::empty())
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{document}");
    assert_eq!(document["_version"], 7);
    assert_eq!(document["_source"]["query"], "1994 acme gold");

    let (status, search) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({
                "document": {"title": "1994 acme"},
                "rank": {"priority_key": "priority"}
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{search}");
    assert!(
        search["hits"]["hits"]
            .as_array()
            .expect("hits")
            .iter()
            .all(|hit| hit["_id"] != 1),
        "the old query must be replaced: {search}"
    );

    let (status, search) = send(
        &state,
        req(
            "POST",
            "/v2/_search",
            &serde_json::json!({
                "document": {"title": "1994 acme gold"},
                "include_source": false
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{search}");
    let hit = search["hits"]["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .find(|hit| hit["_id"] == 1)
        .expect("updated hit");
    assert_eq!(hit["_score"], 50, "typed priority must survive bulk");
}

#[tokio::test]
async fn cluster_bulk_uses_the_shared_strict_transport_contract() {
    let state = test_state(&seed());
    let valid = "{\"index\":{\"_id\":10}}\n{\"query\":\"1996 vertex\"}\n";
    for (label, request, expected) in [
        (
            "missing content type",
            bulk_request("/_bulk", None, valid),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
        ),
        (
            "unknown query parameter",
            bulk_request("/_bulk?routing=one", Some("application/x-ndjson"), valid),
            StatusCode::BAD_REQUEST,
        ),
        (
            "missing final newline",
            bulk_request(
                "/_bulk",
                Some("application/x-ndjson"),
                "{\"index\":{\"_id\":10}}\n{\"query\":\"x\"}",
            ),
            StatusCode::BAD_REQUEST,
        ),
        (
            "unsupported operation",
            bulk_request(
                "/_bulk",
                Some("application/x-ndjson"),
                "{\"delete\":{\"_id\":1}}\n",
            ),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        let (status, response) = send(&state, request).await;
        assert_eq!(status, expected, "{label}: {response}");
        assert_eq!(
            response["status"],
            u64::from(expected.as_u16()),
            "{label}: {response}"
        );
    }
}
