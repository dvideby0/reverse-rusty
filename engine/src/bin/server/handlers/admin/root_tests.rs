use super::api_root;
use axum::{
    body::{to_bytes, Body},
    http::{header::CONTENT_TYPE, Request, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use tower::ServiceExt;

#[tokio::test]
async fn get_root_returns_the_es_os_shaped_product_contract() {
    let response = api_root().await.into_response();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("application/json")
    );

    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("root body");
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("root JSON");
    assert_eq!(
        body,
        serde_json::json!({
            "name": "reverse-rusty",
            "cluster_name": "reverse-rusty",
            "cluster_uuid": "_na_",
            "version": {
                "distribution": "reverse-rusty",
                "number": env!("CARGO_PKG_VERSION"),
            },
            "tagline": "you know, for matching",
        })
    );
}

#[tokio::test]
async fn head_root_is_a_bodyless_connectivity_probe() {
    let response = Router::new()
        .route("/", get(api_root))
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri("/")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("router response");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("application/json")
    );
    assert!(to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("HEAD body")
        .is_empty());
}
