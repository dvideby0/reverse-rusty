use super::*;

#[tokio::test]
async fn coordinator_feedback_reset_is_strict_uncacheable_observed_and_actionable() {
    let state = test_state(&[]);
    let (status, headers, body) =
        send_raw(&state, req_empty("POST", "/_vocab/aliases/feedback/reset")).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(headers.get("cache-control").unwrap(), "no-store");
    let value: serde_json::Value = serde_json::from_slice(&body).expect("JSON error");
    assert_eq!(
        value["error"]["type"], "not_supported_in_cluster_mode",
        "{value}"
    );
    assert!(
        value["error"]["reason"]
            .as_str()
            .expect("reason")
            .contains("single-node replica"),
        "{value}"
    );

    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["vocab_aliases_feedback_reset_post", "501"])
            .get(),
        1
    );
    assert_eq!(
        state
            .prom
            .http_request_duration
            .with_label_values(&["vocab_aliases_feedback_reset_post"])
            .get_sample_count(),
        1
    );
}

#[tokio::test]
async fn coordinator_validates_feedback_reset_before_its_501_boundary() {
    let state = test_state(&[]);
    for request in [
        req_empty("POST", "/_vocab/aliases/feedback/reset?refresh=true"),
        req(
            "POST",
            "/_vocab/aliases/feedback/reset",
            &serde_json::json!({}),
        ),
    ] {
        let (status, headers, body) = send_raw(&state, request).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(headers.get("cache-control").unwrap(), "no-store");
        let value: serde_json::Value = serde_json::from_slice(&body).expect("JSON error");
        assert_eq!(value["error"]["type"], "validation_error", "{value}");
    }

    let (status, headers, body) = send_raw(
        &state,
        req_empty("DELETE", "/_vocab/aliases/feedback/reset"),
    )
    .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(headers.get("allow").unwrap(), "POST");
    assert_eq!(headers.get("cache-control").unwrap(), "no-store");
    let value: serde_json::Value = serde_json::from_slice(&body).expect("JSON error");
    assert_eq!(value["error"]["type"], "method_not_allowed");
}
