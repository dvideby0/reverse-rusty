use super::*;

#[tokio::test]
async fn put_doc_is_created_then_updated_with_replace_semantics() {
    let state = state();

    // First PUT: 201 created.
    let (status, body) = do_put(&state, 7, "michael jordan").await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["_index"], "queries");
    assert_eq!(body["_id"], 7);
    assert_eq!(body["_version"], 1);
    assert_eq!(body["result"], "created");
    assert!(body.get("error").is_none());
    assert!(matches_in_snapshot(&state, "1986 fleer michael jordan rookie").contains(&7));

    // Re-PUT with different semantics: 200 updated, and the snapshot flips
    // atomically — the old version stops matching exactly when the new starts
    // (one lock, one publish; no matches-under-either-version window).
    let (status, body) = do_put(&state, 7, "lebron james").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["_index"], "queries");
    assert_eq!(body["_version"], 1);
    assert_eq!(body["result"], "updated");
    assert!(
        !matches_in_snapshot(&state, "1986 fleer michael jordan rookie").contains(&7),
        "old semantics must stop matching after the re-PUT"
    );
    assert!(matches_in_snapshot(&state, "2003 topps lebron james rookie").contains(&7));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_doc_create_only_is_atomic_and_never_overwrites() {
    let state = state();
    let first_body = serde_json::json!({"query":"michael jordan","version":7});
    let second_body = serde_json::json!({"query":"lebron james","version":8});
    let first = route_put_json(&state, "/_doc/7?op_type=create", &first_body);
    let second = route_put_json(&state, "/_doc/7?op_type=create", &second_body);
    let (a, b) = tokio::join!(first, second);
    let mut statuses = [a.0, b.0];
    statuses.sort_by_key(StatusCode::as_u16);
    assert_eq!(statuses, [StatusCode::CREATED, StatusCode::CONFLICT]);

    let (created, conflict) = if a.0 == StatusCode::CREATED {
        (a.1, b.1)
    } else {
        (b.1, a.1)
    };
    assert_eq!(created["_index"], "queries");
    assert!(
        created["_version"] == 7 || created["_version"] == 8,
        "the winning caller's display version is returned"
    );
    assert_eq!(
        conflict["error"]["type"],
        "version_conflict_engine_exception"
    );

    let jordan = matches_in_snapshot(&state, "1986 fleer michael jordan rookie");
    let lebron = matches_in_snapshot(&state, "2003 topps lebron james rookie");
    assert_ne!(
        jordan.contains(&7),
        lebron.contains(&7),
        "exactly one create-only body must become live"
    );

    let (status, after) = route_put_json(
        &state,
        "/_doc/7?op_type=create",
        &serde_json::json!({"query":"wayne gretzky","version":9}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(after["error"]["type"], "version_conflict_engine_exception");
    assert!(
        !matches_in_snapshot(&state, "1979 opc wayne gretzky rookie").contains(&7),
        "a conflict must not replace the winning document"
    );
    let (status, malformed_conflict) = route_put_json(
        &state,
        "/_doc/7?op_type=create",
        &serde_json::json!({"query":"("}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        malformed_conflict["error"]["type"], "version_conflict_engine_exception",
        "an existing id is the decisive create-only error in both server modes"
    );
}

#[tokio::test]
async fn put_doc_validates_query_parameters_and_accepts_refresh_policies() {
    let state = state();
    for (id, refresh) in [(11, "false"), (12, "true"), (13, "wait_for")] {
        let (status, body) = route_put_json(
            &state,
            &format!("/_doc/{id}?refresh={refresh}&op_type=index"),
            &serde_json::json!({"query":format!("topps chrome {id}")}),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        assert!(
            matches_in_snapshot(&state, &format!("topps chrome {id}")).contains(&id),
            "every accepted refresh policy has immediate visibility"
        );
    }

    for path in [
        "/_doc/20?refresh=immediate",
        "/_doc/21?op_type=overwrite",
        "/_doc/22?routing=custom",
    ] {
        let (status, body) =
            route_put_json(&state, path, &serde_json::json!({"query":"michael jordan"})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}: {body}");
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
    }
    for id in [20, 21, 22] {
        assert!(
            !matches_in_snapshot(&state, "1986 fleer michael jordan rookie").contains(&id),
            "invalid query parameters must reject before mutation"
        );
    }
}

#[tokio::test]
async fn delete_after_reput_reports_one_copy() {
    let state = state();
    do_put(&state, 7, "michael jordan").await;
    do_put(&state, 7, "lebron james").await;

    let resp = delete_doc(
        State(Arc::clone(&state)),
        Path(7),
        Ok(Query(super::DeleteDocParams::default())),
    )
    .await
    .into_response();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("body");
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
    assert_eq!(
        json["deleted_count"], 1,
        "replace-by-id leaves exactly one live copy (the audit observed 2)"
    );
}

// -- memtable_flush_threshold honored by REST PUT (ADR-073, ADR-064 item 5) --

#[tokio::test]
async fn put_doc_honors_memtable_flush_threshold() {
    // Pre-fix the REST PUT path bypassed the only `maybe_flush` call site, so
    // the knob was INERT for single-doc HTTP writes: memtable + WAL grew until
    // a manual /_flush. With threshold 2, the third PUT must have produced at
    // least one sealed segment — and every query must keep matching across the
    // flush boundary.
    use reverse_rusty::config::EngineConfig;
    let cfg = EngineConfig {
        memtable_flush_threshold: 2,
        ..EngineConfig::default()
    };
    let eng = Engine::with_config(Normalizer::default_vocab().expect("vocab"), cfg);
    let state = state_with_engine(eng);

    do_put(&state, 1, "michael jordan").await;
    do_put(&state, 2, "lebron james").await;
    do_put(&state, 3, "wayne gretzky").await;
    // A re-PUT (the upsert path) must honor the threshold too.
    do_put(&state, 2, "mario lemieux").await;

    assert!(
        state.engine.lock().num_segments() > 0,
        "threshold-2 PUTs must auto-flush the memtable into a segment"
    );
    assert!(matches_in_snapshot(&state, "1986 fleer michael jordan rookie").contains(&1));
    assert!(matches_in_snapshot(&state, "1985 opc mario lemieux rookie").contains(&2));
    assert!(matches_in_snapshot(&state, "1979 opc wayne gretzky rookie").contains(&3));
    assert!(
        !matches_in_snapshot(&state, "2003 topps lebron james rookie").contains(&2),
        "the upserted-away version must stay dead across the flush"
    );
}
