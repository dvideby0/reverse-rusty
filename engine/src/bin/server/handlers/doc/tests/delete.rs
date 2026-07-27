use super::*;

fn delete_request(path: &str) -> Request<Body> {
    Request::builder()
        .method("DELETE")
        .uri(path)
        .body(Body::empty())
        .expect("DELETE request")
}

async fn route_delete_json(state: &Arc<AppState>, path: &str) -> (StatusCode, serde_json::Value) {
    let (status, bytes) = route_doc(state, delete_request(path)).await;
    let json = serde_json::from_slice(&bytes).expect("JSON response");
    (status, json)
}

#[tokio::test]
async fn delete_doc_is_es_shaped_immediately_visible_and_idempotent() {
    let state = state();
    for (id, refresh) in [(11, "false"), (12, "true"), (13, "wait_for")] {
        let (status, _) = route_put_json(
            &state,
            &format!("/_doc/{id}"),
            &serde_json::json!({"query":format!("topps chrome {id}"), "version": 7}),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);

        let (status, body) =
            route_delete_json(&state, &format!("/_doc/{id}?refresh={refresh}")).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["_index"], "queries");
        assert_eq!(body["_id"], id);
        assert_eq!(body["result"], "deleted");
        assert_eq!(body["deleted_count"], 1);
        assert!(body.get("_version").is_none());
        assert!(body.get("_shards").is_none());
        assert!(
            !matches_in_snapshot(&state, &format!("topps chrome {id}")).contains(&id),
            "every accepted refresh policy must publish the delete before response"
        );
        let get = Request::builder()
            .uri(format!("/_doc/{id}"))
            .body(Body::empty())
            .expect("GET request");
        assert_eq!(
            route_doc(&state, get).await.0,
            StatusCode::NOT_FOUND,
            "the point read must observe the completed delete too"
        );
    }

    let (status, body) = route_delete_json(&state, "/_doc/11").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["_index"], "queries");
    assert_eq!(body["_id"], 11);
    assert_eq!(body["result"], "not_found");
    assert!(body.get("deleted_count").is_none());
}

#[tokio::test]
async fn delete_doc_rejects_unsupported_parameters_before_mutation() {
    let state = state();
    for (id, suffix) in [
        (20, "refresh=immediate"),
        (21, "routing=custom"),
        (22, "version=7"),
        (23, "refresh=true&refresh=false"),
    ] {
        let query = format!("wayne gretzky {id}");
        assert_eq!(do_put(&state, id, &query).await.0, StatusCode::CREATED);

        let (status, body) = route_delete_json(&state, &format!("/_doc/{id}?{suffix}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
        assert!(
            matches_in_snapshot(&state, &query).contains(&id),
            "invalid delete controls must not mutate document {id}"
        );
    }
}
