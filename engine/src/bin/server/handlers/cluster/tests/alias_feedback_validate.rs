use super::*;

#[tokio::test]
async fn validation_contract_precedes_observed_cluster_boundary() {
    let state = test_state(&seed());
    let request = req_empty(
        "POST",
        "/_vocab/aliases/validate_and_apply?min_titles=1&min_queries=1&activate=true",
    );
    let (status, headers, body) = send_raw(&state, request).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(headers.get("cache-control").unwrap(), "no-store");
    let body: serde_json::Value = serde_json::from_slice(&body).expect("JSON response");
    assert_eq!(
        body["error"]["type"], "not_supported_in_cluster_mode",
        "{body}"
    );
    assert!(body["error"]["reason"]
        .as_str()
        .unwrap()
        .contains("PUT /_vocab"));
    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["vocab_aliases_validate_and_apply", "501"])
            .get(),
        1
    );

    let invalid = req_empty("POST", "/_vocab/aliases/validate_and_apply?min_titles=0");
    let (status, headers, body) = send_raw(&state, invalid).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(headers.get("cache-control").unwrap(), "no-store");
    let body: serde_json::Value = serde_json::from_slice(&body).expect("JSON error");
    assert_eq!(body["error"]["type"], "validation_error", "{body}");
}

#[tokio::test]
async fn validation_method_fallback_is_strict() {
    let state = test_state(&seed());
    let (status, headers, body) = send_raw(
        &state,
        req_empty("GET", "/_vocab/aliases/validate_and_apply"),
    )
    .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(headers.get("allow").unwrap(), "POST");
    assert_eq!(headers.get("cache-control").unwrap(), "no-store");
    let body: serde_json::Value = serde_json::from_slice(&body).expect("JSON error");
    assert_eq!(body["error"]["type"], "method_not_allowed", "{body}");
}
