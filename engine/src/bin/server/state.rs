//! Shared server state and request-scoped middleware.
//!
//! [`AppState`] holds the single-node snapshot-based concurrency primitives: a
//! `Mutex<Engine>` for serialized writes and an `ArcSwap<EngineSnapshot>` for
//! lock-free reads. [`ClusterAppState`] is the coordinator-mode analogue (ADR-070):
//! an `RwLock<ClusterEngine>` whose READ side serves both percolates and ordinary
//! writes (cluster reads are `&self` lock-free; writes are `&self`, internally
//! ordered by the cluster log, and serialized across requests by `write_serial` —
//! the `Mutex<Engine>` analogue), while the WRITE side is taken only by the
//! vocabulary paths (`set_vocab` & co., the `&mut self` blue/green rebuilds).
//! [`RequestCtx`] is the seam that lets one auth / request-id middleware serve both
//! backends. [`request_id_middleware`] stamps an `x-request-id` header and tracks
//! the in-flight-request gauge via the RAII [`InFlightGuard`].

use std::sync::Arc;

use arc_swap::ArcSwap;
use axum::{extract::State, http::HeaderValue, middleware::Next, response::Response};
use parking_lot::{Mutex, RwLock};
use prometheus::IntGauge;

use reverse_rusty::cluster::ClusterEngine;
use reverse_rusty::segment::{Engine, EngineSnapshot};

use crate::auth::AuthConfig;
use crate::metrics::PrometheusMetrics;

pub(crate) struct AppState {
    pub(crate) engine: Mutex<Engine>,
    pub(crate) snapshot: ArcSwap<EngineSnapshot>,
    pub(crate) pool: rayon::ThreadPool,
    pub(crate) include_broad: bool,
    pub(crate) prom: PrometheusMetrics,
    pub(crate) slow_query_threshold_ms: u64,
    /// Bearer-token auth (ADR-062). `None` ⇒ the gate is a pass-through.
    pub(crate) auth: Option<AuthConfig>,
}

impl AppState {
    pub(crate) fn publish_snapshot(&self) {
        let engine = self.engine.lock();
        self.snapshot.store(Arc::new(engine.snapshot()));
    }
}

/// Coordinator-mode state (ADR-070): the cluster analogue of [`AppState`].
pub(crate) struct ClusterAppState {
    /// Read lock for percolates AND ordinary writes (both `&self`); write lock only
    /// for the `&mut self` vocabulary rebuilds — so reads are never blocked by
    /// writes, only (briefly) by a vocab change.
    pub(crate) cluster: RwLock<ClusterEngine>,
    /// Serializes mutating requests (the `Mutex<Engine>` analogue), so concurrent
    /// bulk batches don't interleave their per-item apply order. Reads never take it.
    pub(crate) write_serial: Mutex<()>,
    pub(crate) pool: rayon::ThreadPool,
    pub(crate) include_broad: bool,
    pub(crate) prom: PrometheusMetrics,
    pub(crate) slow_query_threshold_ms: u64,
    /// Bearer-token auth (ADR-062), identical to single-node mode.
    pub(crate) auth: Option<AuthConfig>,
}

/// What the request-scoped middleware needs from either backend's state — the seam
/// that lets ONE auth middleware + request-id middleware serve single-node and
/// cluster mode (ADR-070) without duplicating them.
pub(crate) trait RequestCtx: Send + Sync + 'static {
    fn prom(&self) -> &PrometheusMetrics;
    fn auth(&self) -> Option<&AuthConfig>;
}

impl RequestCtx for AppState {
    fn prom(&self) -> &PrometheusMetrics {
        &self.prom
    }
    fn auth(&self) -> Option<&AuthConfig> {
        self.auth.as_ref()
    }
}

impl RequestCtx for ClusterAppState {
    fn prom(&self) -> &PrometheusMetrics {
        &self.prom
    }
    fn auth(&self) -> Option<&AuthConfig> {
        self.auth.as_ref()
    }
}

/// RAII guard for the in-flight request gauge: increments on construction and
/// decrements on drop, so every exit path of the request stays balanced.
struct InFlightGuard<'a>(&'a IntGauge);

impl<'a> InFlightGuard<'a> {
    fn new(gauge: &'a IntGauge) -> Self {
        gauge.inc();
        Self(gauge)
    }
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.0.dec();
    }
}

/// Adds a unique X-Request-Id header to every response, tracks the in-flight
/// request gauge, and includes the request ID in the tracing span for
/// correlation. Generic over the backend state ([`RequestCtx`]).
pub(crate) async fn request_id_middleware<S: RequestCtx>(
    State(state): State<Arc<S>>,
    request: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Response {
    let _in_flight = InFlightGuard::new(&state.prom().in_flight_requests);
    let request_id = uuid::Uuid::new_v4().to_string();
    let span = tracing::info_span!("request", request_id = %request_id);
    let _guard = span.enter();

    let mut response = next.run(request).await;
    if let Ok(val) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert("x-request-id", val);
    }
    response
}
