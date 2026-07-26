use super::*;

#[tokio::test]
async fn missing_documents_is_400() {
    let (eng, _) = corpus();
    let state = state_with(eng, false);
    let err = mpercolate(State(state), Json(body(None, None, false)))
        .await
        .err()
        .expect("missing documents must error");
    assert_eq!(err.0, axum::http::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn search_rejects_batch_over_max_percolate_batch() {
    // A multi-doc `/_search` must reject an oversized batch with 400 before
    // scheduling work, exactly like `/_mpercolate` (ADR-052) — otherwise it is
    // bounded only by the HTTP body size. A tiny cap keeps the test small.
    use reverse_rusty::config::EngineConfig;
    let cfg = EngineConfig {
        max_percolate_batch: 2,
        ..EngineConfig::default()
    };
    let mut eng = Engine::with_config(Normalizer::default_vocab().expect("vocab"), cfg);
    eng.build_from_queries(&[(1u64, "michael jordan".to_string())]);
    let state = state_with(eng, false);

    // 3 documents > cap of 2 ⇒ 400 before any matching runs.
    let over: SearchBody = serde_json::from_value(serde_json::json!({
        "documents": [{"title": "a"}, {"title": "b"}, {"title": "c"}],
        "include_source": false,
    }))
    .expect("valid SearchBody");
    let err = search(State(Arc::clone(&state)), Json(over))
        .await
        .err()
        .expect("a batch over max_percolate_batch must 400");
    assert_eq!(err.0, axum::http::StatusCode::BAD_REQUEST);

    // A batch AT the cap is accepted (the guard is strictly `>`).
    let at_cap: SearchBody = serde_json::from_value(serde_json::json!({
        "documents": [{"title": "a"}, {"title": "b"}],
        "include_source": false,
    }))
    .expect("valid SearchBody");
    assert!(
        search(State(state), Json(at_cap)).await.is_ok(),
        "a batch at the cap must be accepted"
    );
}

#[tokio::test]
async fn empty_batch_is_noop() {
    let (eng, _) = corpus();
    let state = state_with(eng, true);
    let resp = mpercolate(State(state), Json(body(Some(Vec::new()), None, true)))
        .await
        .expect("empty batch is a valid no-op")
        .0;
    assert!(resp.responses.is_empty());
    assert!(resp.broad.is_none(), "no work => no broad summary");
}

// Reads the ES-convention `_id` field on hits (clippy::used_underscore_binding).
#[allow(clippy::used_underscore_binding)]
#[tokio::test]
async fn responses_are_byte_identical_to_per_title_search() {
    let (eng, titles) = corpus();
    // Capture a snapshot of the same state for the per-title baseline before
    // the engine moves into the AppState.
    let baseline = Arc::new(eng.snapshot());
    let state = state_with(eng, true);

    let batch: Vec<&str> = titles.iter().take(150).map(String::as_str).collect();
    // include_broad=true exercises the columnar broad lane through the endpoint.
    let resp = mpercolate(
        State(Arc::clone(&state)),
        Json(body(Some(batch.clone()), Some(true), true)),
    )
    .await
    .expect("ok")
    .0;

    assert_eq!(
        resp.responses.len(),
        batch.len(),
        "one response per document"
    );

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let mut summed = 0u32;
    for (i, title) in batch.iter().enumerate() {
        out.clear();
        baseline.match_title(title, &mut scratch, &mut out, true);
        let mut expected = out.clone();
        expected.sort_unstable();
        expected.dedup();

        let item = &resp.responses[i];
        let mut got: Vec<u64> = item.hits.hits.iter().map(|h| h._id).collect();
        got.sort_unstable();
        assert_eq!(
            got, expected,
            "document {i} ({title}) diverged from per-title search"
        );
        assert_eq!(item.hits.total, expected.len(), "total mismatch at {i}");
        summed += expected.len() as u32;
    }

    // Top-level broad summary present (profile=true) and internally consistent.
    let broad = resp.broad.expect("profile=true => broad summary");
    assert_eq!(broad.strategy, "columnar");
    assert_eq!(broad.batch_size, 256);
    assert_eq!(
        broad.total_matches, summed,
        "summary total must equal the per-document sum"
    );
}
