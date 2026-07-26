use super::*;

#[tokio::test]
async fn put_doc_typed_priority_reaches_bounded_ranker_and_errors_are_structured() {
    let state = state();
    let body: super::super::PutDocBody = serde_json::from_value(serde_json::json!({
        "query": "topps chrome",
        "rank_fields": {"priority": 50}
    }))
    .expect("typed body");
    let response = put_doc(
        State(Arc::clone(&state)),
        Path(77),
        Ok(Query(super::super::PutDocParams::default())),
        Json(body),
    )
    .await
    .into_response();
    assert_eq!(response.status(), StatusCode::CREATED);

    let snap = state.snapshot.load();
    let program = snap
        .compile_rank_program(&reverse_rusty::RankProgramSpec::default())
        .expect("priority program");
    let ranked = snap
        .try_match_title_top_k(
            "2020 topps chrome",
            reverse_rusty::TopKOptions::default(),
            &program,
            &reverse_rusty::exact::TagPredicate::empty(),
            &mut MatchScratch::new(),
            None,
        )
        .expect("ranked match");
    assert_eq!(
        ranked.hits[0],
        reverse_rusty::RankedHit {
            logical_id: 77,
            score: 50
        }
    );

    let invalid: super::super::PutDocBody = serde_json::from_value(serde_json::json!({
        "query": "topps chrome",
        "rank_fields": {"priority": 1.5}
    }))
    .expect("invalid rank still decodes at DTO layer");
    let response = put_doc(
        State(Arc::clone(&state)),
        Path(78),
        Ok(Query(super::super::PutDocParams::default())),
        Json(invalid),
    )
    .await
    .into_response();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
    assert_eq!(json["error"]["type"], "invalid_rank_value");
}

#[test]
fn structured_tag_values_fail_loud() {
    // Pre-fix these were dropped SILENTLY, leaving the query unreachable by any
    // filter on the key (the ADR-064 item-4 finding). Now they are hard errors.
    assert!(
        tags_of(&serde_json::json!({"query": "q", "tags": {"meta": {"x": 1}}})).is_err(),
        "object tag value must error"
    );
    assert!(
        tags_of(&serde_json::json!({"query": "q", "colors": [["nested"]]})).is_err(),
        "nested array tag element must error"
    );
    assert!(
        tags_of(&serde_json::json!({"query": "q", "tags": ["not", "an", "object"]})).is_err(),
        "a non-object `tags` field must error (was silently ignored)"
    );
}

#[tokio::test]
async fn put_doc_rejects_structured_tag_value_with_400() {
    let state = state();
    let body: super::super::PutDocBody = serde_json::from_value(serde_json::json!({
        "query": "michael jordan",
        "tags": {"meta": {"x": 1}},
    }))
    .expect("body deserializes");
    let resp = put_doc(
        State(Arc::clone(&state)),
        Path(7),
        Ok(Query(super::super::PutDocParams::default())),
        Json(body),
    )
    .await
    .into_response();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    // Nothing was ingested: the engine never saw the doc.
    assert!(matches_in_snapshot(&state, "1986 fleer michael jordan rookie").is_empty());
}

#[tokio::test]
async fn rejected_reput_leaves_old_version_live() {
    let state = state();
    do_put(&state, 7, "michael jordan").await;

    // A parse error never reaches the engine; the old version stays live.
    let (status, _) = do_put(&state, 7, "(").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(matches_in_snapshot(&state, "1986 fleer michael jordan rookie").contains(&7));

    // A class-D rejection (negation-only) also leaves the old version live.
    let (status, body) = do_put(&state, 7, "-graded").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["result"], "rejected");
    assert!(
        matches_in_snapshot(&state, "1986 fleer michael jordan rookie").contains(&7),
        "a failed replace must never delete"
    );
}
