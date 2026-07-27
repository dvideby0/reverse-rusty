//! Production-boundary regressions for `GET /_percolate/jobs/{id}/stream`.

use super::*;

fn route(state: &Arc<AppState>) -> Router {
    Router::new()
        .route("/_percolate/jobs/{id}/stream", any(get_job_stream))
        .with_state(Arc::clone(state))
}

async fn create(state: &Arc<AppState>, event_id: &str) -> CreateJobResponse {
    let (_, Json(created)) = create_job(State(Arc::clone(state)), Json(request(event_id)))
        .await
        .expect("job accepted");
    created
}

async fn response(app: Router, method: Method, uri: &str) -> axum::response::Response {
    app.oneshot(
        Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::empty())
            .expect("request"),
    )
    .await
    .expect("response")
}

async fn json(response: axum::response::Response) -> serde_json::Value {
    let bytes = to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("response body");
    serde_json::from_slice(&bytes).expect("JSON response")
}

#[tokio::test]
async fn stream_is_newline_delimited_exact_and_no_store() {
    let state = state(5, 8);
    let created = create(&state, "stream-framing").await;
    let uri = format!("/_percolate/jobs/{}/stream", created.job_id);
    let response = response(route(&state), Method::GET, &uri).await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/x-ndjson")
    );
    assert_eq!(
        response
            .headers()
            .get(header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );

    let bytes = to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("stream body");
    assert_eq!(bytes.last(), Some(&b'\n'));
    let frames = std::str::from_utf8(&bytes)
        .expect("UTF-8 stream")
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("JSON frame"))
        .collect::<Vec<_>>();
    assert!(!frames.is_empty());

    let (completion, chunks) = frames.split_last().expect("completion frame");
    assert_eq!(chunks.len(), 3);
    for (sequence, chunk) in chunks.iter().enumerate() {
        assert_eq!(chunk["type"], "match_chunk");
        assert_eq!(chunk["job_id"], created.job_id);
        assert_eq!(chunk["sequence"], sequence);
        for member in chunk["members"].as_array().expect("chunk members") {
            let key = member["idempotency_key"].as_str().expect("idempotency key");
            assert_eq!(key.len(), 64);
            assert!(key.bytes().all(|byte| byte.is_ascii_hexdigit()));
        }
    }
    assert_eq!(completion["type"], "completion");
    assert_eq!(completion["job_id"], created.job_id);
    assert_eq!(completion["exact_total"], 5);
    assert_eq!(completion["chunk_count"], 3);
    assert!(completion["checksum"].is_object());
    assert_eq!(
        wait_terminal(&state, &created.job_id).await.state,
        crate::jobs::JobPhase::Completed
    );
}

#[tokio::test]
async fn invalid_query_and_non_get_methods_fail_before_claiming_the_stream() {
    let state = state(0, 8);
    let created = create(&state, "stream-rejections").await;
    let path = format!("/_percolate/jobs/{}/stream", created.job_id);
    let app = route(&state);

    let invalid = response(
        app.clone(),
        Method::GET,
        &format!("{path}?wait_for_completion_timeout=1s"),
    )
    .await;
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    let invalid = json(invalid).await;
    assert_eq!(invalid["error"]["type"], "validation_error");
    assert!(invalid["error"]["reason"]
        .as_str()
        .is_some_and(|reason| reason.contains("do not accept query parameters")));

    let post = response(
        app.clone(),
        Method::POST,
        &format!("{path}?also_ignored=true"),
    )
    .await;
    assert_eq!(post.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        post.headers()
            .get(header::ALLOW)
            .and_then(|value| value.to_str().ok()),
        Some("GET")
    );
    let post = json(post).await;
    assert_eq!(post["error"]["type"], "method_not_allowed");

    let head = response(
        app.clone(),
        Method::HEAD,
        &format!("{path}?also_ignored=true"),
    )
    .await;
    assert_eq!(head.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        head.headers()
            .get(header::ALLOW)
            .and_then(|value| value.to_str().ok()),
        Some("GET")
    );

    let get = response(app, Method::GET, &path).await;
    assert_eq!(get.status(), StatusCode::OK);
    let bytes = to_bytes(get.into_body(), 64 * 1024)
        .await
        .expect("stream body");
    let completion = std::str::from_utf8(&bytes)
        .expect("UTF-8 stream")
        .lines()
        .last()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("JSON frame"))
        .expect("completion frame");
    assert_eq!(completion["type"], "completion");
}

#[tokio::test]
async fn a_claim_is_exclusive_and_dropping_its_body_fails_the_job() {
    let state = state(20, 1);
    let created = create(&state, "stream-exclusive").await;
    let path = format!("/_percolate/jobs/{}/stream", created.job_id);
    let app = route(&state);

    let claimed = response(app.clone(), Method::GET, &path).await;
    assert_eq!(claimed.status(), StatusCode::OK);

    let duplicate = response(app, Method::GET, &path).await;
    assert_eq!(duplicate.status(), StatusCode::CONFLICT);
    let duplicate = json(duplicate).await;
    assert_eq!(duplicate["error"]["type"], "stream_already_claimed");

    drop(claimed);
    let terminal = wait_terminal(&state, &created.job_id).await;
    assert_eq!(terminal.state, crate::jobs::JobPhase::Failed);
    assert!(terminal.exact_total.is_none());
    assert!(terminal.checksum.is_none());
}

#[tokio::test]
async fn missing_stream_uses_the_standard_not_found_envelope() {
    let state = state(0, 8);
    let response = response(
        route(&state),
        Method::GET,
        "/_percolate/jobs/missing/stream",
    )
    .await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = json(response).await;
    assert_eq!(body["status"], StatusCode::NOT_FOUND.as_u16());
    assert_eq!(body["error"]["type"], "job_not_found");
}
