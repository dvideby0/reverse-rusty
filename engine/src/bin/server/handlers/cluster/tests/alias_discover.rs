use super::*;

fn discovery_corpus() -> Vec<(u64, String)> {
    let mut queries = Vec::new();
    let mut id = 1u64;
    for i in 0..40 {
        queries.push((id, format!("zzns ctxp{} ctxb{}", i % 7, i % 5)));
        id += 1;
        queries.push((id, format!("zznorthstar ctxp{} ctxb{}", i % 7, i % 5)));
        id += 1;
    }
    for i in 0..200 {
        queries.push((id, format!("filler{i} junk{i}")));
        id += 1;
    }
    queries
}

#[tokio::test]
async fn explicit_corpus_discovery_matches_the_standalone_contract() {
    let state = test_state(&[]);
    let request = req(
        "POST",
        "/_vocab/aliases/discover",
        &serde_json::json!({"queries": discovery_corpus()}),
    );
    let (status, headers, bytes) = send_raw(&state, request).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("cache-control").unwrap(), "no-store");
    assert_eq!(headers.get("content-type").unwrap(), "application/json");

    let value: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON response");
    assert!(value["took"].is_u64(), "{value}");
    assert!(value["took_ms"].as_f64().expect("took_ms") >= 0.0);
    assert!(value["count"].as_u64().expect("count") >= 1, "{value}");
    let planted = value["proposals"]
        .as_array()
        .expect("proposals")
        .iter()
        .any(|proposal| {
            let forms: Vec<&str> = proposal["forms"]
                .as_array()
                .expect("forms")
                .iter()
                .map(|form| form.as_str().expect("form"))
                .collect();
            forms.contains(&"zzns") && forms.contains(&"zznorthstar")
        });
    assert!(planted, "{value}");
    assert_eq!(
        state
            .prom
            .http_requests_total
            .with_label_values(&["vocab_aliases_discover", "200"])
            .get(),
        1
    );
}

#[tokio::test]
async fn coordinator_requires_an_explicit_valid_corpus_and_rejects_other_methods() {
    let state = test_state(&[]);

    let (status, headers, body) =
        send_raw(&state, req_empty("POST", "/_vocab/aliases/discover")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(headers.get("cache-control").unwrap(), "no-store");
    let value: serde_json::Value = serde_json::from_slice(&body).expect("JSON error");
    assert_eq!(value["error"]["type"], "validation_error");
    assert!(
        value["error"]["reason"]
            .as_str()
            .expect("reason")
            .contains("explicit-corpus"),
        "{value}"
    );

    let (status, headers, body) = send_raw(
        &state,
        req(
            "POST",
            "/_vocab/aliases/discover",
            &serde_json::json!({"queries": [[1, "a"], [1, "b"]]}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(headers.get("cache-control").unwrap(), "no-store");
    let value: serde_json::Value = serde_json::from_slice(&body).expect("JSON error");
    assert_eq!(value["error"]["type"], "validation_error");

    let (status, headers, body) =
        send_raw(&state, req_empty("GET", "/_vocab/aliases/discover")).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(headers.get("allow").unwrap(), "POST");
    assert_eq!(headers.get("cache-control").unwrap(), "no-store");
    let value: serde_json::Value = serde_json::from_slice(&body).expect("JSON error");
    assert_eq!(value["error"]["type"], "method_not_allowed");
}
