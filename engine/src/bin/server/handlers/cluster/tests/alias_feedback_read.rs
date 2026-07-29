use super::*;

#[tokio::test]
async fn coordinator_feedback_read_is_strict_uncacheable_observed_and_actionable() {
    let state = test_state(&[]);
    for request in [
        req_empty("GET", "/_vocab/aliases/feedback"),
        req_empty(
            "GET",
            "/_vocab/aliases/feedback?min_overlap=0.75&min_titles=10&min_queries=5&from=2&size=3",
        ),
    ] {
        let (status, headers, body) = send_raw(&state, request).await;
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
    }

    let (status, headers, body) =
        send_raw(&state, req_empty("HEAD", "/_vocab/aliases/feedback")).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(headers.get("cache-control").unwrap(), "no-store");
    assert!(body.is_empty());

    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["vocab_aliases_feedback_get", "501"])
            .get(),
        3
    );
    assert_eq!(
        state
            .prom
            .http_request_duration
            .with_label_values(&["vocab_aliases_feedback_get"])
            .get_sample_count(),
        3
    );
}

#[tokio::test]
async fn coordinator_validates_feedback_read_before_its_501_boundary() {
    let state = test_state(&[]);
    for request in [
        req_empty("GET", "/_vocab/aliases/feedback?unknown=true"),
        req_empty("GET", "/_vocab/aliases/feedback?min_overlap=NaN"),
        req_empty("GET", "/_vocab/aliases/feedback?min_titles=0"),
        req_empty("GET", "/_vocab/aliases/feedback?min_queries=0"),
        req("GET", "/_vocab/aliases/feedback", &serde_json::json!({})),
    ] {
        let (status, headers, body) = send_raw(&state, request).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(headers.get("cache-control").unwrap(), "no-store");
        let value: serde_json::Value = serde_json::from_slice(&body).expect("JSON error");
        assert_eq!(value["error"]["type"], "validation_error", "{value}");
    }

    let (status, headers, body) =
        send_raw(&state, req_empty("POST", "/_vocab/aliases/feedback")).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(headers.get("allow").unwrap(), "GET, HEAD");
    assert_eq!(headers.get("cache-control").unwrap(), "no-store");
    let value: serde_json::Value = serde_json::from_slice(&body).expect("JSON error");
    assert_eq!(value["error"]["type"], "method_not_allowed");
}
