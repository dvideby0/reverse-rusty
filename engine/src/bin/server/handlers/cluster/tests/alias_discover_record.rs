use super::*;

#[tokio::test]
async fn coordinator_returns_a_strict_uncacheable_observed_501_with_the_alternative() {
    let state = test_state(&[]);
    for request in [
        req_empty("POST", "/_vocab/aliases/discover_and_record"),
        req(
            "POST",
            "/_vocab/aliases/discover_and_record",
            &serde_json::json!({"min_token_freq": 5, "max_pairs": 10}),
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
                .contains("/_vocab/aliases/discover"),
            "{value}"
        );
    }
    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["vocab_aliases_discover_and_record", "501"])
            .get(),
        2
    );
    assert_eq!(
        state
            .prom
            .http_request_duration
            .with_label_values(&["vocab_aliases_discover_and_record"])
            .get_sample_count(),
        2
    );
}

#[tokio::test]
async fn coordinator_validates_the_shared_request_contract_before_its_501_boundary() {
    let state = test_state(&[]);

    for request in [
        req(
            "POST",
            "/_vocab/aliases/discover_and_record",
            &serde_json::json!({"queries": []}),
        ),
        req(
            "POST",
            "/_vocab/aliases/discover_and_record",
            &serde_json::json!({"min_token_freq": 0}),
        ),
        req(
            "POST",
            "/_vocab/aliases/discover_and_record",
            &serde_json::json!({"unknown": true}),
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
        req_empty("POST", "/_vocab/aliases/discover_and_record?refresh=true"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(headers.get("cache-control").unwrap(), "no-store");
    let value: serde_json::Value = serde_json::from_slice(&body).expect("JSON error");
    assert_eq!(value["error"]["type"], "validation_error");

    let (status, headers, body) = send_raw(
        &state,
        req_empty("GET", "/_vocab/aliases/discover_and_record"),
    )
    .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(headers.get("allow").unwrap(), "POST");
    assert_eq!(headers.get("cache-control").unwrap(), "no-store");
    let value: serde_json::Value = serde_json::from_slice(&body).expect("JSON error");
    assert_eq!(value["error"]["type"], "method_not_allowed");
}
