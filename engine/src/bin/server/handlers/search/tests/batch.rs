use super::*;

#[tokio::test]
async fn v2_mpercolate_per_slot_equals_v2_search_and_shares_winner_sources() {
    let state = state_with(ranked_engine(), false);
    let titles = [
        "2020 topps chrome update",
        "no match at all",
        "2020 topps chrome update",
    ];
    let batch = v2_mpercolate(
        State(Arc::clone(&state)),
        Json(v2_batch_body(serde_json::json!({
            "documents": titles.iter().map(|t| serde_json::json!({"title": t})).collect::<Vec<_>>()
        }))),
    )
    .await
    .expect("batch response");
    let batch_json = serde_json::to_value(batch.0).expect("batch json");
    assert_eq!(batch_json["complete"], true);
    assert_eq!(batch_json["responses"].as_array().map(Vec::len), Some(3));
    for (i, title) in titles.iter().enumerate() {
        let single = v2_search(
            State(Arc::clone(&state)),
            Json(v2_body(serde_json::json!({"document": {"title": title}}))),
        )
        .await
        .expect("single response");
        let single_json = serde_json::to_value(single.0).expect("single json");
        assert_eq!(
            batch_json["responses"][i]["hits"], single_json["hits"],
            "slot {i} must equal its /v2/_search result"
        );
        assert_eq!(
            batch_json["responses"][i]["_shards"], single_json["_shards"],
            "slot {i} shard echo"
        );
    }
}

#[tokio::test]
async fn v2_mpercolate_named_unsupported_shapes_and_empty_batch() {
    let state = state_with(ranked_engine(), false);
    let Err(error) = v2_mpercolate(
        State(Arc::clone(&state)),
        Json(v2_batch_body(serde_json::json!({
            "documents": [{"title": "topps chrome"}],
            "explain": true
        }))),
    )
    .await
    else {
        panic!("explain must be a named 400");
    };
    assert_eq!(error.0, axum::http::StatusCode::BAD_REQUEST);

    let Err(error) = v2_mpercolate(
        State(Arc::clone(&state)),
        Json(v2_batch_body(serde_json::json!({
            "document": {"title": "topps chrome"}
        }))),
    )
    .await
    else {
        panic!("the singular document shape must be a named 400");
    };
    assert_eq!(error.0, axum::http::StatusCode::BAD_REQUEST);

    let Err(error) = v2_mpercolate(
        State(Arc::clone(&state)),
        Json(v2_batch_body(serde_json::json!({}))),
    )
    .await
    else {
        panic!("a MISSING documents field must be a named 400, not an empty 200");
    };
    assert_eq!(error.0, axum::http::StatusCode::BAD_REQUEST);

    let Err(error) = v2_mpercolate(
        State(Arc::clone(&state)),
        Json(v2_batch_body(serde_json::json!({
            "documents": [{"title": "topps chrome", "size": 1}]
        }))),
    )
    .await
    else {
        panic!("a per-document option must be a named 400, never silently discarded");
    };
    assert_eq!(error.0, axum::http::StatusCode::BAD_REQUEST);

    let empty = v2_mpercolate(
        State(Arc::clone(&state)),
        Json(v2_batch_body(serde_json::json!({"documents": []}))),
    )
    .await
    .expect("empty batch is a 200");
    let json = serde_json::to_value(empty.0).expect("empty json");
    assert_eq!(json["responses"], serde_json::json!([]));
    assert_eq!(json["complete"], true);
}
