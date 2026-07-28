use super::*;

#[tokio::test]
async fn search_honors_per_request_include_broad() {
    // Pre-fix `/_search` honored only the server-wide --include-broad and an
    // `include_broad` body field was SILENTLY ignored (serde unknown-field
    // tolerance) — with broad off, class-C hits read as missing data.
    // `/_mpercolate` and the cluster handlers already had the override.
    let (eng, titles) = corpus();
    let state = state_with(eng, false); // server default: broad OFF

    // A title whose match set differs with the broad lane on — the probe that
    // makes the override observable.
    let title = titles
        .iter()
        .find(|t| expected_ids(&state, t, true).len() > expected_ids(&state, t, false).len())
        .expect("corpus(broad_frac=0.1) has a broad-affected title")
        .clone();
    let with_broad = expected_ids(&state, &title, true);
    let without_broad = expected_ids(&state, &title, false);

    // Absent ⇒ the server default (off).
    let ids = search_ids(&state, serde_json::json!({"document": {"title": title}}))
        .await
        .expect("ok");
    assert_eq!(ids, without_broad);
    // Per-request true overrides the off default.
    let ids = search_ids(
        &state,
        serde_json::json!({"document": {"title": title}, "include_broad": true}),
    )
    .await
    .expect("ok");
    assert_eq!(
        ids, with_broad,
        "include_broad:true must surface class-C hits"
    );

    // And the reverse: on a broad-ON server, per-request false suppresses —
    // through the multi-doc arm, so both handler paths honor the override.
    let (eng2, _) = corpus();
    let state_on = state_with(eng2, true);
    let req = serde_json::json!({"documents": [{"title": title}], "include_broad": false});
    let ids = search_ids(&state_on, req).await.expect("ok");
    assert_eq!(
        ids, without_broad,
        "include_broad:false must suppress broad"
    );
}

// -- Tag-value coercion on the filter path (ADR-073, ADR-064 item 4) --------

/// Run `/_search` with a JSON body, returning the sorted hit ids (Ok) or the
/// HTTP status (Err).
// Reads the ES-convention `_id` field on hits (clippy::used_underscore_binding).
#[allow(clippy::used_underscore_binding)]
async fn search_ids(
    state: &Arc<AppState>,
    body: serde_json::Value,
) -> Result<Vec<u64>, axum::http::StatusCode> {
    let req: SearchBody = serde_json::from_value(body).expect("valid SearchBody");
    match search(State(Arc::clone(state)), Json(req)).await {
        Ok(resp) => {
            let mut ids: Vec<u64> = resp.0.hits.hits.iter().map(|h| h._id).collect();
            ids.sort_unstable();
            Ok(ids)
        }
        Err((status, _)) => Err(status),
    }
}

#[allow(clippy::used_underscore_binding)]
#[tokio::test]
async fn numeric_tag_ingest_meets_numeric_filter() {
    // The load-bearing agreement (ADR-073): ingest and filter coerce through the
    // SAME canonical rule, so a numeric category ingested as `7` is reachable by
    // a filter sending `7` OR `"7"` — pre-fix the ingest side silently dropped
    // the tag, making the query unreachable by ANY filter on that key.
    let state = state_with(
        Engine::new(Normalizer::default_vocab().expect("vocab")),
        false,
    );
    let body: crate::handlers::doc::PutDocBody = serde_json::from_value(serde_json::json!({
        "query": "wireless mouse",
        "tags": {"category": 7, "active": true},
    }))
    .expect("body deserializes");
    let resp = crate::handlers::doc::put_doc(
        axum::extract::State(Arc::clone(&state)),
        axum::extract::Path(1u64),
        Ok(axum::extract::Query(
            crate::handlers::doc::PutDocParams::default(),
        )),
        Json(body),
    )
    .await
    .into_response();
    assert_eq!(resp.status(), axum::http::StatusCode::CREATED);

    let title = serde_json::json!({"title": "1986 vertex wireless mouse new"});
    // Native filter, number and string forms, plus the coerced bool.
    for filter in [
        serde_json::json!({"category": 7}),
        serde_json::json!({"category": "7"}),
        serde_json::json!({"category": [7]}),
        serde_json::json!({"active": true}),
    ] {
        let ids = search_ids(
            &state,
            serde_json::json!({"document": title, "filter": filter}),
        )
        .await
        .expect("filter coerces, not 400");
        assert_eq!(ids, vec![1], "filter {filter} must reach the tagged query");
    }
    // ES envelope: bool.filter terms with a numeric value.
    let ids = search_ids(
        &state,
        serde_json::json!({"query": {"bool": {
            "must": {"percolate": {"field": "query", "document": title}},
            "filter": [{"terms": {"category": [7]}}],
        }}}),
    )
    .await
    .expect("ES terms coerce");
    assert_eq!(ids, vec![1]);
    // A different number does NOT match (coercion is exact, not fuzzy).
    let ids = search_ids(
        &state,
        serde_json::json!({"document": title, "filter": {"category": 8}}),
    )
    .await
    .expect("ok");
    assert!(ids.is_empty(), "category 8 must not match a category-7 tag");
}

#[tokio::test]
async fn unanswerable_filter_values_are_400_not_silently_dropped() {
    // Pre-fix a non-string ARRAY ELEMENT was silently dropped from the filter
    // (widening the predicate); scalars already 400'd. Now everything without a
    // canonical scalar form is a loud 400 on every filter shape.
    let state = state_with(
        Engine::new(Normalizer::default_vocab().expect("vocab")),
        false,
    );
    let title = serde_json::json!({"title": "anything"});
    for (label, body) in [
        (
            "native null",
            serde_json::json!({"document": title, "filter": {"category": null}}),
        ),
        (
            "native object",
            serde_json::json!({"document": title, "filter": {"category": {"x": 1}}}),
        ),
        (
            "native nested array element",
            serde_json::json!({"document": title, "filter": {"category": [["a"]]}}),
        ),
        (
            "native null array element",
            serde_json::json!({"document": title, "filter": {"category": ["a", null]}}),
        ),
        (
            "ES terms null element",
            serde_json::json!({"query": {"bool": {
                "must": {"percolate": {"field": "query", "document": title}},
                "filter": [{"terms": {"category": ["a", null]}}],
            }}}),
        ),
        (
            "ES term null",
            serde_json::json!({"query": {"bool": {
                "must": {"percolate": {"field": "query", "document": title}},
                "filter": [{"term": {"category": null}}],
            }}}),
        ),
        // A clause carrying TWO queries silently dropped the second pre-fix —
        // the widening direction (review catch); ES errors on the shape too.
        (
            "ES clause with both terms and term",
            serde_json::json!({"query": {"bool": {
                "must": {"percolate": {"field": "query", "document": title}},
                "filter": [{"terms": {"a": ["x"]}, "term": {"b": "y"}}],
            }}}),
        ),
        // An empty `terms` object was a silent no-op clause; ES rejects it.
        (
            "ES empty terms clause",
            serde_json::json!({"query": {"bool": {
                "must": {"percolate": {"field": "query", "document": title}},
                "filter": [{"terms": {}}],
            }}}),
        ),
        (
            "mixed native and ES shapes",
            serde_json::json!({
                "document": title,
                "query": {"percolate": {
                    "field": "query",
                    "document": title
                }}
            }),
        ),
        (
            "native document and documents",
            serde_json::json!({
                "document": title,
                "documents": [title]
            }),
        ),
        (
            "missing percolate field",
            serde_json::json!({"query": {"percolate": {"document": title}}}),
        ),
        (
            "wrong percolate field",
            serde_json::json!({"query": {"percolate": {
                "field": "stored_query",
                "document": title
            }}}),
        ),
        (
            "unsupported percolate option",
            serde_json::json!({"query": {"percolate": {
                "field": "query",
                "document": title,
                "name": "ignored-before"
            }}}),
        ),
        (
            "top-level query sibling",
            serde_json::json!({"query": {
                "percolate": {"field": "query", "document": title},
                "match_all": {}
            }}),
        ),
        (
            "unsupported bool sibling",
            serde_json::json!({"query": {"bool": {
                "must": {"percolate": {"field": "query", "document": title}},
                "should": [{"match_all": {}}]
            }}}),
        ),
        (
            "must sibling",
            serde_json::json!({"query": {"bool": {
                "must": {
                    "percolate": {"field": "query", "document": title},
                    "match_all": {}
                }
            }}}),
        ),
        (
            "percolate document and documents",
            serde_json::json!({"query": {"percolate": {
                "field": "query",
                "document": title,
                "documents": [title]
            }}}),
        ),
        (
            "terms scalar",
            serde_json::json!({"query": {"bool": {
                "must": {"percolate": {"field": "query", "document": title}},
                "filter": {"terms": {"category": "one"}}
            }}}),
        ),
        (
            "term with multiple fields",
            serde_json::json!({"query": {"bool": {
                "must": {"percolate": {"field": "query", "document": title}},
                "filter": {"term": {"a": "one", "b": "two"}}
            }}}),
        ),
    ] {
        let err = search_ids(&state, body).await.expect_err(label);
        assert_eq!(err, axum::http::StatusCode::BAD_REQUEST, "{label}");
    }
}

#[tokio::test]
async fn search_enrichment_never_splices_a_newer_source_onto_an_older_match() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    let mut engine = Engine::new(Normalizer::default_vocab().expect("vocab"));
    engine.try_insert_live("acme chrome", 7, 1).expect("insert");
    let mut state = state_with(engine, false);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
    let held = Arc::clone(&semaphore)
        .acquire_owned()
        .await
        .expect("held permit");
    Arc::get_mut(&mut state)
        .expect("sole state owner")
        .search_permits = Some(semaphore);

    let request: SearchBody = serde_json::from_value(serde_json::json!({
        "document": {"title": "2020 acme chrome"}
    }))
    .expect("body");
    let task_state = Arc::clone(&state);
    let mut search_future = Box::pin(search(State(task_state), Json(request)));

    // One manual poll runs synchronously through snapshot capture and stops at
    // the held search permit. This makes the replacement race deterministic.
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    assert!(matches!(
        search_future.as_mut().poll(&mut context),
        Poll::Pending
    ));
    {
        let mut engine = state.engine.lock();
        engine
            .try_upsert_live("wireless mouse", 7, 2)
            .expect("replace query");
        state.snapshot.store(Arc::new(engine.snapshot()));
    }
    drop(held);

    let error = search_future
        .await
        .err()
        .expect("the old snapshot's source generation is no longer available");
    assert_eq!(error.0, axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    let json = serde_json::to_value(error.1 .0).expect("serialize error");
    assert_eq!(json["error"]["type"], "source_unavailable");
    assert!(
        !json.to_string().contains("wireless mouse"),
        "the replacement source must never be attached to the older match"
    );
}

#[tokio::test]
async fn mpercolate_enrichment_never_splices_a_newer_source_onto_an_older_match() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    let mut engine = Engine::new(Normalizer::default_vocab().expect("vocab"));
    engine.try_insert_live("acme chrome", 7, 1).expect("insert");
    let mut state = state_with(engine, false);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
    let held = Arc::clone(&semaphore)
        .acquire_owned()
        .await
        .expect("held permit");
    Arc::get_mut(&mut state)
        .expect("sole state owner")
        .search_permits = Some(semaphore);

    let request: MPercolateBody = serde_json::from_value(serde_json::json!({
        "documents": [{"title": "2020 acme chrome"}]
    }))
    .expect("body");
    let task_state = Arc::clone(&state);
    let mut search_future = Box::pin(mpercolate(State(task_state), Json(request)));

    // Capture the batch snapshot, then hold it at admission while the source row
    // advances. The old match must fail enrichment rather than reading the new text.
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    assert!(matches!(
        search_future.as_mut().poll(&mut context),
        Poll::Pending
    ));
    {
        let mut engine = state.engine.lock();
        engine
            .try_upsert_live("wireless mouse", 7, 2)
            .expect("replace query");
        state.snapshot.store(Arc::new(engine.snapshot()));
    }
    drop(held);

    let error = search_future
        .await
        .err()
        .expect("the old snapshot's source generation is no longer available");
    assert_eq!(error.0, axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    let json = serde_json::to_value(error.1 .0).expect("serialize error");
    assert_eq!(json["error"]["type"], "source_unavailable");
    assert!(
        !json.to_string().contains("wireless mouse"),
        "the replacement source must never be attached to the older match"
    );
}

// ---- cooperative cancellation + bounded concurrency (ADR-099) ----------------

/// Poll a counter until it reaches `want` (the cancellation is recorded inside the
/// blocking closure, which may finish AFTER the handler already answered 408).
async fn wait_for_count(
    counter: &prometheus::core::GenericCounter<prometheus::core::AtomicU64>,
    want: u64,
) {
    for _ in 0..200 {
        if counter.get() >= want {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!(
        "cancellation counter never reached {want} (got {}) — the armed work did not record stopping",
        counter.get()
    );
}

#[tokio::test]
async fn explicit_zero_timeout_cancels_work_and_408s() {
    let (eng, titles) = corpus();
    let state = state_with(eng, false);

    // An explicit timeout_ms arms cooperative cancellation (ADR-099); 0ms is expired
    // by the time the blocking closure runs, so its FIRST deadline check fires —
    // deterministic, no timing sensitivity.
    let req: SearchBody = serde_json::from_value(serde_json::json!({
        "document": {"title": titles[0]},
        "include_source": false,
        "timeout_ms": 0,
    }))
    .expect("valid SearchBody");
    let err = search(State(Arc::clone(&state)), Json(req))
        .await
        .err()
        .expect("a zero timeout must 408");
    assert_eq!(err.0, axum::http::StatusCode::REQUEST_TIMEOUT);

    // The work actually stopped AND recorded it (the closure-side counter).
    let counter = state
        .prom
        .match_cancellations_total
        .with_label_values(&["search"]);
    wait_for_count(&counter, 1).await;
}

#[tokio::test]
async fn mpercolate_explicit_zero_timeout_cancels_and_408s() {
    let (eng, titles) = corpus();
    let state = state_with(eng, false);
    let mut b = body(
        Some(titles.iter().take(8).map(String::as_str).collect()),
        None,
        false,
    );
    b.timeout_ms = Some(0);
    let err = mpercolate(State(Arc::clone(&state)), Json(b))
        .await
        .err()
        .expect("a zero timeout must 408");
    assert_eq!(err.0, axum::http::StatusCode::REQUEST_TIMEOUT);
    let counter = state
        .prom
        .match_cancellations_total
        .with_label_values(&["mpercolate"]);
    wait_for_count(&counter, 1).await;
}

#[tokio::test]
async fn no_explicit_timeout_stays_unarmed() {
    let (eng, titles) = corpus();
    let state = state_with(eng, false);
    let req: SearchBody = serde_json::from_value(serde_json::json!({
        "document": {"title": titles[0]},
        "include_source": false,
    }))
    .expect("valid SearchBody");
    let resp = search(State(Arc::clone(&state)), Json(req)).await;
    assert!(resp.is_ok(), "the unarmed default path must serve normally");
    assert_eq!(
        state
            .prom
            .match_cancellations_total
            .with_label_values(&["search"])
            .get(),
        0,
        "no explicit timeout_ms ⇒ never armed ⇒ never cancelled"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_permit_serializes_but_both_searches_succeed() {
    let (eng, titles) = corpus();
    let mut state_arc = state_with(eng, false);
    {
        // A single permit: two concurrent searches must queue, not fail — the
        // semaphore wait sits inside each request's own timeout budget.
        let state = Arc::get_mut(&mut state_arc).expect("sole owner");
        state.search_permits = Some(std::sync::Arc::new(tokio::sync::Semaphore::new(1)));
    }
    let state = state_arc;

    let mk = |t: &str| -> SearchBody {
        serde_json::from_value(serde_json::json!({
            "document": {"title": t},
            "include_source": false,
        }))
        .expect("valid SearchBody")
    };
    let (a, b) = tokio::join!(
        search(State(Arc::clone(&state)), Json(mk(&titles[0]))),
        search(State(Arc::clone(&state)), Json(mk(&titles[1]))),
    );
    assert!(a.is_ok() && b.is_ok(), "both queued searches must succeed");
    assert_eq!(
        state.prom.search_permits_in_use.get(),
        0,
        "all permits released after the work completed"
    );
}
