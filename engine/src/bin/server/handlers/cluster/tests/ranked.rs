use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ranked_search_orders_by_score() {
    // Cluster `rank` (ADR-075): boosts resolve against the shared tag space (synthetic
    // ids included — boost matching is id-equality), hits come back `(score desc, _id
    // asc)` with `_score`, and `from`/`size` slice the RANKED order. The unranked path
    // stays byte-identical (no `_score`, ascending ids).
    let state = test_state(&[]);
    for (id, q, tier) in [
        (41u64, "1994 topps", "gold"),
        (42, "1994 topps", "silver"),
        (43, "1994 topps", "bronze"),
    ] {
        let (status, _) = send(
            &state,
            req(
                "PUT",
                &format!("/_doc/{id}"),
                &serde_json::json!({"query": q, "tags": {"tier": tier}}),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
    }

    let rank = serde_json::json!({"boosts": [
        {"key": "tier", "value": "gold", "boost": 100},
        {"key": "tier", "value": "silver", "boost": 40}
    ]});
    let (status, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({"document": {"title": "1994 topps"}, "rank": rank}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let hits = body["hits"]["hits"].as_array().expect("hits");
    let got: Vec<(u64, i64)> = hits
        .iter()
        .map(|h| {
            (
                h["_id"].as_u64().expect("id"),
                h["_score"].as_i64().expect("ranked hits carry _score"),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![(41, 100), (42, 40), (43, 0)],
        "(score desc, _id asc) with boost scores"
    );

    // from/size slice the RANKED order.
    let (_, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({
                "document": {"title": "1994 topps"},
                "rank": rank, "from": 1, "size": 1
            }),
        ),
    )
    .await;
    assert_eq!(body["hits"]["hits"][0]["_id"], 42);

    // Unranked stays byte-identical: ascending ids, no _score key.
    let (_, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({"document": {"title": "1994 topps"}}),
        ),
    )
    .await;
    let hits = body["hits"]["hits"].as_array().expect("hits");
    assert_eq!(hits[0]["_id"], 41);
    assert!(
        hits[0].get("_score").is_none(),
        "unranked hits must not grow a _score"
    );

    // /_mpercolate honors the same rank block per slot.
    let (status, body) = send(
        &state,
        req(
            "POST",
            "/_mpercolate",
            &serde_json::json!({
                "documents": [{"title": "1994 topps"}],
                "rank": rank
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["responses"][0]["hits"]["hits"][0]["_id"], 41);
    assert_eq!(body["responses"][0]["hits"]["hits"][0]["_score"], 100);

    // A PRESENT-but-empty rank block is a no-op — byte-identical to unranked
    // (single-node parity, review finding): ascending ids, NO `_score` key.
    for noop in [serde_json::json!({}), serde_json::json!({"boosts": []})] {
        let (status, body) = send(
            &state,
            req(
                "POST",
                "/_search",
                &serde_json::json!({"document": {"title": "1994 topps"}, "rank": noop}),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let hits = body["hits"]["hits"].as_array().expect("hits");
        assert_eq!(
            hits[0]["_id"], 41,
            "no-op rank keeps engine (ascending) order"
        );
        assert!(
            hits[0].get("_score").is_none(),
            "a no-op rank block must not grow a _score: {body}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_mixes_upserts_and_statuses() {
    let state = test_state(&seed());
    let body = concat!(
        "{\"index\":{\"_id\":21}}\n",
        "{\"query\":\"1996 skybox\"}\n",
        "{\"index\":{\"_id\":22}}\n",
        "{\"query\":\"(((\"}\n",
        "{\"index\":{\"_id\":1}}\n",
        "{\"query\":\"1994 topps gold\"}\n",
    );
    let r = Request::builder()
        .method("POST")
        .uri("/_bulk")
        .header("content-type", "application/x-ndjson")
        .body(Body::from(body))
        .expect("request");
    let (status, json) = send(&state, r).await;
    assert_eq!(status, StatusCode::OK, "{json}");
    let items = json["items"].as_array().expect("items");
    assert_eq!(items.len(), 3);
    assert_eq!(items[0]["index"]["status"], 201, "fresh id created");
    assert_eq!(items[1]["index"]["status"], 400, "parse error rejected");
    assert_eq!(items[2]["index"]["status"], 200, "existing id updated");
    assert_eq!(json["errors"], true);

    // The bulk upsert of id 1 replaced its DSL: the old form no longer matches.
    let (_, body) = send(
        &state,
        req(
            "POST",
            "/_search",
            &serde_json::json!({"document": {"title": "1994 topps"}}),
        ),
    )
    .await;
    let ids: Vec<u64> = body["hits"]["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .map(|h| h["_id"].as_u64().expect("id"))
        .collect();
    assert!(
        !ids.contains(&1),
        "replaced query must need the new form: {ids:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mpercolate_returns_per_document_responses() {
    let state = test_state(&seed());
    let (status, body) = send(
        &state,
        req(
            "POST",
            "/_mpercolate",
            &serde_json::json!({"documents": [
                {"title": "1994 topps"},
                {"title": "1995 fleer ultra"},
                {"title": "nothing here"}
            ]}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let responses = body["responses"].as_array().expect("responses");
    assert_eq!(responses.len(), 3);
    assert_eq!(responses[0]["hits"]["total"], 1);
    assert_eq!(responses[1]["hits"]["total"], 1);
    assert_eq!(responses[2]["hits"]["total"], 0);
}
