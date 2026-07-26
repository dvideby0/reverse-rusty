use super::*;

#[tokio::test]
async fn get_doc_is_es_shaped_filterable_and_head_aware() {
    let state = state();
    let put = Request::builder()
        .method("PUT")
        .uri("/_doc/7")
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({
                "query": "topps chrome",
                "version": 42,
                "tags": {
                    "tenant": "acme",
                    "colors": ["red", "blue"],
                    "active": true,
                    "é": "accent"
                }
            })
            .to_string(),
        ))
        .expect("PUT request");
    assert_eq!(route_doc(&state, put).await.0, StatusCode::CREATED);

    let get = Request::builder()
        .uri("/_doc/7")
        .body(Body::empty())
        .expect("GET request");
    let (status, bytes) = route_doc(&state, get).await;
    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("GET json");
    assert_eq!(body["_index"], "queries");
    assert_eq!(body["_id"], 7);
    assert_eq!(body["_version"], 42);
    assert_eq!(body["found"], true);
    assert_eq!(body["_source"]["query"], "topps chrome");
    assert_eq!(body["_source"]["tags"]["tenant"], "acme");
    assert_eq!(body["_source"]["tags"]["active"], "true");
    assert_eq!(body["_source"]["tags"]["é"], "accent");
    assert_eq!(
        body["_source"]["tags"]["colors"],
        serde_json::json!(["blue", "red"])
    );

    let filtered = Request::builder()
        .uri("/_doc/7?_source_includes=tags.colors")
        .body(Body::empty())
        .expect("filtered GET");
    let (status, bytes) = route_doc(&state, filtered).await;
    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("filtered json");
    assert!(body["_source"].get("query").is_none());
    assert_eq!(
        body["_source"]["tags"],
        serde_json::json!({"colors": ["blue", "red"]})
    );

    let unicode_question = Request::builder()
        .uri("/_doc/7?_source_includes=tags.%3F")
        .body(Body::empty())
        .expect("Unicode wildcard GET");
    let (status, bytes) = route_doc(&state, unicode_question).await;
    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("Unicode wildcard json");
    assert_eq!(
        body["_source"],
        serde_json::json!({"tags": {"é": "accent"}}),
        "`?` must consume one Unicode character, not one UTF-8 byte"
    );

    let excluded = Request::builder()
        .uri("/_doc/7?_source_excludes=tags.col*")
        .body(Body::empty())
        .expect("excluded GET");
    let (status, bytes) = route_doc(&state, excluded).await;
    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("excluded json");
    assert_eq!(body["_source"]["query"], "topps chrome");
    assert!(body["_source"]["tags"].get("colors").is_none());
    assert_eq!(body["_source"]["tags"]["tenant"], "acme");

    let source_disabled = Request::builder()
        .uri("/_doc/7?_source=false")
        .body(Body::empty())
        .expect("source-disabled GET");
    let (status, bytes) = route_doc(&state, source_disabled).await;
    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("source-disabled json");
    assert!(body.get("_source").is_none());
    assert_eq!(body["_version"], 42);

    for (path, expected) in [
        ("/_doc/7", StatusCode::OK),
        ("/_doc/8", StatusCode::NOT_FOUND),
    ] {
        let head = Request::builder()
            .method("HEAD")
            .uri(path)
            .body(Body::empty())
            .expect("HEAD request");
        let (status, bytes) = route_doc(&state, head).await;
        assert_eq!(status, expected);
        assert!(bytes.is_empty(), "HEAD response must be bodyless");
    }

    let missing = Request::builder()
        .uri("/_doc/8")
        .body(Body::empty())
        .expect("missing GET");
    let (status, bytes) = route_doc(&state, missing).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("missing json");
    assert_eq!(
        body,
        serde_json::json!({"_index": "queries", "_id": 8, "found": false})
    );

    let unsupported = Request::builder()
        .uri("/_doc/7?preference=local")
        .body(Body::empty())
        .expect("unsupported query parameter");
    assert_eq!(
        route_doc(&state, unsupported).await.0,
        StatusCode::BAD_REQUEST,
        "unsupported ES parameters must fail instead of being silently ignored"
    );
}

#[tokio::test]
async fn get_doc_does_not_report_a_live_row_as_missing_when_its_source_is_unavailable() {
    use reverse_rusty::config::EngineConfig;

    let dir = std::env::temp_dir().join(format!(
        "reverse-rusty-get-doc-source-guard-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        retain_source: false,
        ..EngineConfig::default()
    };
    {
        let mut engine =
            Engine::with_config(Normalizer::default_vocab().expect("vocab"), config.clone());
        engine
            .try_insert_live("topps chrome", 7, 1)
            .expect("insert");
        engine.flush();
    }
    let source_name = reverse_rusty::storage::read_manifest(&dir.join("manifest.bin"))
        .expect("manifest")
        .source_file_name;
    std::fs::remove_file(dir.join(source_name)).expect("remove source store");
    let engine = Engine::open(Normalizer::default_vocab().expect("vocab"), config).expect("reopen");
    assert!(engine.snapshot().has_live_query(7));
    let state = state_with_engine(engine);

    let request = Request::builder()
        .uri("/_doc/7")
        .body(Body::empty())
        .expect("GET request");
    let (status, bytes) = route_doc(&state, request).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("error json");
    assert_eq!(body["error"]["type"], "source_unavailable");

    let head = Request::builder()
        .method("HEAD")
        .uri("/_doc/7")
        .body(Body::empty())
        .expect("HEAD request");
    let (status, bytes) = route_doc(&state, head).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "HEAD existence comes from the live exact index, not sources.dat"
    );
    assert!(bytes.is_empty(), "HEAD response must be bodyless");

    let _ = std::fs::remove_dir_all(dir);
}
