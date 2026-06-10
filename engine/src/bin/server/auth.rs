//! Opt-in bearer-token authentication for mutating/admin endpoints (ADR-062).
//!
//! When a token is configured (`--auth-token` / `RR_AUTH_TOKEN`), any request
//! that can change engine state must present `Authorization: Bearer <token>`.
//! Read endpoints stay open unless `--auth-protect-reads` extends the gate to
//! everything except the `/_health` liveness probe. With no token configured
//! the middleware is a pass-through and the server behaves exactly as before —
//! the gate is strictly opt-in.
//!
//! The protected set is **default-deny**: every non-GET/HEAD request requires
//! the token unless its path is one of the read-via-POST percolate endpoints
//! (`/_search`, `/_mpercolate`). A future mutating endpoint is therefore
//! covered without anyone remembering to list it here.

use std::sync::Arc;

use axum::{
    body::Body,
    extract::State,
    http::{header, HeaderMap, Method, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use tracing::warn;

use crate::dto::ApiError;
use crate::state::AppState;

/// Resolved auth configuration held in [`AppState`]. Built once at startup by
/// [`AuthConfig::resolve`]; absent (`None` in `AppState.auth`) means the gate
/// is disabled.
pub(crate) struct AuthConfig {
    /// The shared secret. Visible ASCII only, enforced at resolve time.
    token: Vec<u8>,
    /// Gate read endpoints too (everything except `/_health`).
    pub(crate) protect_reads: bool,
}

impl AuthConfig {
    /// Resolve the auth configuration from the CLI flag and the
    /// `RR_AUTH_TOKEN` environment variable (the flag wins). `Ok(None)` means
    /// auth is disabled. Fails loud — instead of silently serving open — on an
    /// empty or non-printable token, or on `--auth-protect-reads` without a
    /// token to enforce it.
    pub(crate) fn resolve(
        flag: Option<String>,
        env: Option<String>,
        protect_reads: bool,
    ) -> Result<Option<Self>, String> {
        let Some(token) = flag.or(env) else {
            if protect_reads {
                return Err(
                    "--auth-protect-reads requires a token (--auth-token or RR_AUTH_TOKEN)"
                        .to_string(),
                );
            }
            return Ok(None);
        };
        if token.is_empty() {
            return Err(
                "auth token must not be empty (unset --auth-token/RR_AUTH_TOKEN to disable auth)"
                    .to_string(),
            );
        }
        if !token.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
            return Err(
                "auth token must be visible ASCII with no spaces or control characters".to_string(),
            );
        }
        Ok(Some(Self {
            token: token.into_bytes(),
            protect_reads,
        }))
    }
}

/// Whether this request must present the bearer token.
///
/// All non-GET `/_vocab*` verbs are protected — including the compute-only
/// `/_vocab/learn` — because they are operator surface. Under `protect_reads`
/// only `/_health` stays open: Kubernetes-style liveness probes cannot send
/// credentials, and the endpoint reveals nothing.
pub(crate) fn requires_auth(method: &Method, path: &str, protect_reads: bool) -> bool {
    if protect_reads {
        return path != "/_health";
    }
    if method == Method::GET || method == Method::HEAD {
        return false;
    }
    // Read-only percolation via POST — the service's primary read path.
    !matches!(path, "/_search" | "/_mpercolate")
}

/// Constant-time byte equality. The fold has no data-dependent branch, so a
/// wrong token costs the same to compare as a nearly-right one. Only the
/// token's *length* is observable (via the early return), which is not secret.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Extract the token from an `Authorization: Bearer <token>` header. The
/// scheme is case-insensitive and may be followed by multiple spaces
/// (RFC 6750 / RFC 7235). `None` for a missing header, a different scheme, or
/// an empty token.
fn bearer_token(headers: &HeaderMap) -> Option<&[u8]> {
    let value = headers.get(header::AUTHORIZATION)?.as_bytes();
    let space = value.iter().position(|&b| b == b' ')?;
    let (scheme, rest) = value.split_at(space);
    if !scheme.eq_ignore_ascii_case(b"Bearer") {
        return None;
    }
    let start = rest.iter().position(|&b| b != b' ')?;
    Some(&rest[start..])
}

/// The auth gate, wired into the router via `middleware::from_fn_with_state`.
/// Pass-through when no token is configured or the route is open; otherwise
/// 401 with an RFC 6750 `WWW-Authenticate` challenge and the standard error
/// envelope (ES-style `security_exception`).
pub(crate) async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let Some(auth) = &state.auth else {
        return next.run(request).await;
    };
    if !requires_auth(request.method(), request.uri().path(), auth.protect_reads) {
        return next.run(request).await;
    }
    let reason = match bearer_token(request.headers()) {
        Some(t) if ct_eq(t, &auth.token) => return next.run(request).await,
        Some(_) => "invalid",
        None => "missing",
    };
    state
        .prom
        .auth_failures_total
        .with_label_values(&[reason])
        .inc();
    warn!(
        method = %request.method(),
        path = request.uri().path(),
        reason,
        "request rejected: authentication required"
    );
    let (detail, challenge) = if reason == "missing" {
        (
            "missing authentication credentials for protected endpoint",
            r#"Bearer realm="reverse-rusty""#,
        )
    } else {
        (
            "invalid authentication token",
            r#"Bearer realm="reverse-rusty", error="invalid_token""#,
        )
    };
    let mut response =
        ApiError::response(StatusCode::UNAUTHORIZED, "security_exception", detail).into_response();
    response.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        header::HeaderValue::from_static(challenge),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::PrometheusMetrics;
    use axum::routing::{get, post};
    use axum::Router;
    use reverse_rusty::segment::Engine;
    use reverse_rusty::Normalizer;
    use tower::ServiceExt;

    // -- requires_auth route table

    #[test]
    fn mutating_routes_require_auth() {
        for (method, path) in [
            (Method::PUT, "/_doc/1"),
            (Method::DELETE, "/_doc/1"),
            (Method::POST, "/_bulk"),
            (Method::POST, "/_flush"),
            (Method::POST, "/_compact"),
            (Method::PUT, "/_vocab"),
            (Method::POST, "/_vocab/learn"),
            (Method::POST, "/_vocab/learn_and_apply"),
            (Method::POST, "/_vocab/aliases/import"),
            (Method::POST, "/_vocab/aliases/learn_and_apply"),
            (Method::PUT, "/_settings"),
            // Default-deny: an unknown future endpoint is protected too.
            (Method::POST, "/_new_admin_thing"),
        ] {
            assert!(
                requires_auth(&method, path, false),
                "{method} {path} must require auth"
            );
        }
    }

    #[test]
    fn read_routes_stay_open() {
        for (method, path) in [
            (Method::GET, "/"),
            (Method::GET, "/_doc/1"),
            (Method::POST, "/_search"),
            (Method::POST, "/_mpercolate"),
            (Method::GET, "/_stats"),
            (Method::GET, "/_cat/segments"),
            (Method::GET, "/_health"),
            (Method::GET, "/_metrics"),
            (Method::GET, "/_vocab"),
            (Method::GET, "/_vocab/aliases"),
            (Method::GET, "/_settings"),
        ] {
            assert!(
                !requires_auth(&method, path, false),
                "{method} {path} must stay open"
            );
        }
    }

    #[test]
    fn protect_reads_gates_everything_but_health() {
        assert!(requires_auth(&Method::GET, "/_stats", true));
        assert!(requires_auth(&Method::POST, "/_search", true));
        assert!(requires_auth(&Method::GET, "/", true));
        assert!(requires_auth(&Method::GET, "/_metrics", true));
        assert!(!requires_auth(&Method::GET, "/_health", true));
    }

    // -- token resolution

    #[test]
    fn resolve_flag_wins_over_env_and_validates() {
        let cfg = AuthConfig::resolve(Some("flag-tok".into()), Some("env-tok".into()), false)
            .expect("valid")
            .expect("enabled");
        assert_eq!(cfg.token, b"flag-tok");

        let cfg = AuthConfig::resolve(None, Some("env-tok".into()), true)
            .expect("valid")
            .expect("enabled");
        assert_eq!(cfg.token, b"env-tok");
        assert!(cfg.protect_reads);

        assert!(AuthConfig::resolve(None, None, false)
            .expect("valid")
            .is_none());
        // Fail-loud cases: empty token, whitespace/control bytes, and
        // protect-reads with nothing to enforce it.
        assert!(AuthConfig::resolve(Some(String::new()), None, false).is_err());
        assert!(AuthConfig::resolve(Some("has space".into()), None, false).is_err());
        assert!(AuthConfig::resolve(Some("ctrl\u{7}".into()), None, false).is_err());
        assert!(AuthConfig::resolve(None, None, true).is_err());
    }

    // -- header parsing + comparison

    #[test]
    fn bearer_parsing() {
        let hdr = |v: &str| {
            let mut h = HeaderMap::new();
            h.insert(header::AUTHORIZATION, v.parse().expect("header"));
            h
        };
        assert_eq!(bearer_token(&hdr("Bearer abc")), Some(b"abc".as_slice()));
        // Scheme is case-insensitive; multiple spaces are allowed (RFC 7235).
        assert_eq!(bearer_token(&hdr("bearer abc")), Some(b"abc".as_slice()));
        assert_eq!(bearer_token(&hdr("Bearer   abc")), Some(b"abc".as_slice()));
        assert_eq!(bearer_token(&hdr("Basic abc")), None);
        assert_eq!(bearer_token(&hdr("Bearer ")), None);
        assert_eq!(bearer_token(&hdr("Bearer")), None);
        assert_eq!(bearer_token(&HeaderMap::new()), None);
    }

    #[test]
    fn ct_eq_semantics() {
        assert!(ct_eq(b"secret", b"secret"));
        assert!(!ct_eq(b"secret", b"secre7"));
        assert!(!ct_eq(b"secret", b"secret2"));
        assert!(!ct_eq(b"", b"x"));
        assert!(ct_eq(b"", b""));
    }

    // -- middleware wired into a router

    fn test_state(auth: Option<AuthConfig>) -> Arc<AppState> {
        let eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
        let snap = Arc::new(eng.snapshot());
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .expect("pool");
        Arc::new(AppState {
            engine: parking_lot::Mutex::new(eng),
            snapshot: arc_swap::ArcSwap::new(snap),
            pool,
            include_broad: false,
            prom: PrometheusMetrics::new(),
            slow_query_threshold_ms: 0,
            auth,
        })
    }

    /// Stub handlers: these tests prove the gate, not the endpoints. Paths
    /// mirror the real router so the route-table classification is exercised
    /// through real URI matching.
    fn router(state: &Arc<AppState>) -> Router {
        Router::new()
            .route(
                "/_doc/{id}",
                get(|| async { "read" }).put(|| async { "ok" }),
            )
            .route("/_search", post(|| async { "read" }))
            .route("/_flush", post(|| async { "ok" }))
            .route("/_health", get(|| async { "ok" }))
            .layer(axum::middleware::from_fn_with_state(
                Arc::clone(state),
                auth_middleware,
            ))
    }

    fn req(method: &str, path: &str, token: Option<&str>) -> Request<Body> {
        let mut b = Request::builder().method(method).uri(path);
        if let Some(t) = token {
            b = b.header("authorization", format!("Bearer {t}"));
        }
        b.body(Body::empty()).expect("request")
    }

    async fn status(state: &Arc<AppState>, r: Request<Body>) -> StatusCode {
        router(state)
            .oneshot(r)
            .await
            .expect("router response")
            .status()
    }

    #[tokio::test]
    async fn no_token_configured_is_passthrough() {
        let state = test_state(None);
        assert_eq!(
            status(&state, req("PUT", "/_doc/1", None)).await,
            StatusCode::OK
        );
        assert_eq!(
            status(&state, req("POST", "/_flush", None)).await,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn mutating_endpoints_enforce_token() {
        let auth = AuthConfig::resolve(Some("s3cr3t".into()), None, false).expect("valid");
        let state = test_state(auth);

        // Missing credentials → 401 with the Bearer challenge + error envelope.
        let resp = router(&state)
            .oneshot(req("PUT", "/_doc/1", None))
            .await
            .expect("response");
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        let challenge = resp
            .headers()
            .get(header::WWW_AUTHENTICATE)
            .expect("WWW-Authenticate present")
            .to_str()
            .expect("ascii");
        assert!(challenge.starts_with("Bearer"));
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .expect("body");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("error envelope");
        assert_eq!(v["status"], 401);
        assert_eq!(v["error"]["type"], "security_exception");

        // Wrong token → 401, challenge flags invalid_token.
        let resp = router(&state)
            .oneshot(req("PUT", "/_doc/1", Some("wrong")))
            .await
            .expect("response");
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert!(resp
            .headers()
            .get(header::WWW_AUTHENTICATE)
            .expect("challenge")
            .to_str()
            .expect("ascii")
            .contains("invalid_token"));

        // Right token → through to the handler. Reads stay open without one.
        assert_eq!(
            status(&state, req("PUT", "/_doc/1", Some("s3cr3t"))).await,
            StatusCode::OK
        );
        assert_eq!(
            status(&state, req("GET", "/_doc/1", None)).await,
            StatusCode::OK
        );
        assert_eq!(
            status(&state, req("POST", "/_search", None)).await,
            StatusCode::OK
        );
        assert_eq!(
            status(&state, req("GET", "/_health", None)).await,
            StatusCode::OK
        );

        // Both failure reasons were counted.
        assert_eq!(
            state
                .prom
                .auth_failures_total
                .with_label_values(&["missing"])
                .get(),
            1
        );
        assert_eq!(
            state
                .prom
                .auth_failures_total
                .with_label_values(&["invalid"])
                .get(),
            1
        );
    }

    #[tokio::test]
    async fn protect_reads_extends_gate_to_reads() {
        let auth = AuthConfig::resolve(Some("s3cr3t".into()), None, true).expect("valid");
        let state = test_state(auth);

        assert_eq!(
            status(&state, req("POST", "/_search", None)).await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            status(&state, req("GET", "/_doc/1", None)).await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            status(&state, req("POST", "/_search", Some("s3cr3t"))).await,
            StatusCode::OK
        );
        // Liveness stays open so probes without credentials keep working.
        assert_eq!(
            status(&state, req("GET", "/_health", None)).await,
            StatusCode::OK
        );
    }
}
