//! Standard gRPC health-checking service (`grpc.health.v1.Health`) for the deployable
//! `shardserver` / `controlserver` (ADR-084). Behind the `distributed` feature.
//!
//! Served on a SEPARATE, plaintext `--health-addr` port — never the TLS + token mesh
//! data port — so a Kubernetes built-in `grpc` liveness/readiness probe (plaintext, no
//! CA field) reaches it directly, with no extra image binary. The reported status is
//! non-sensitive (SERVING / NOT_SERVING) and pod-local. Two keys:
//!
//!   - `""` (overall) → SERVING once the gRPC server is up = LIVENESS.
//!   - `"ready"` → reflects real readiness (a shard has adopted a dict; a control node sees a leader) = READINESS.
//!
//! Only `Check` (unary) is implemented — Kubernetes and `grpc_health_probe` call it;
//! `Watch` (server-streaming) returns `unimplemented`.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use super::proto::health::health_check_response::ServingStatus;
use super::proto::health::health_server::{Health, HealthServer};
use super::proto::health::{HealthCheckRequest, HealthCheckResponse};

/// The readiness key a Kubernetes readiness probe queries (`grpc: { service: "ready" }`).
/// The empty string is the gRPC-standard "overall server" key, used for liveness.
pub(crate) const READINESS_SERVICE: &str = "ready";

/// How often the readiness watcher re-evaluates its predicate. Coarse on purpose —
/// readiness is off every hot path, and one probe interval of lag is irrelevant to k8s.
const WATCH_INTERVAL: Duration = Duration::from_millis(250);

/// A cheap, lock-free handle to a server's health: the overall (liveness) and readiness
/// statuses as `grpc.health.v1` `ServingStatus` codes. `Clone` shares the same atomics —
/// the watcher writes them, the `Check` handler reads them.
#[derive(Clone)]
pub(crate) struct HealthReporter {
    inner: Arc<HealthInner>,
}

struct HealthInner {
    overall: AtomicI32,
    ready: AtomicI32,
}

impl HealthReporter {
    /// A reporter whose overall (liveness) status is already SERVING — the health server
    /// only starts once the node is serving — and whose readiness is NOT_SERVING until the
    /// watcher flips it on the first poll.
    pub(crate) fn serving() -> Self {
        Self {
            inner: Arc::new(HealthInner {
                overall: AtomicI32::new(ServingStatus::Serving as i32),
                ready: AtomicI32::new(ServingStatus::NotServing as i32),
            }),
        }
    }

    /// Set the readiness status (`true` → SERVING, `false` → NOT_SERVING).
    pub(crate) fn set_ready(&self, ready: bool) {
        let status = if ready {
            ServingStatus::Serving
        } else {
            ServingStatus::NotServing
        };
        self.inner.ready.store(status as i32, Ordering::Release);
    }

    /// The status code for a queried service name: the empty string is the overall
    /// (liveness) key, [`READINESS_SERVICE`] is readiness, anything else is unknown.
    fn status_for(&self, service: &str) -> Option<i32> {
        match service {
            "" => Some(self.inner.overall.load(Ordering::Acquire)),
            READINESS_SERVICE => Some(self.inner.ready.load(Ordering::Acquire)),
            _ => None,
        }
    }
}

/// The `grpc.health.v1.Health` service over a shared [`HealthReporter`].
struct HealthService {
    reporter: HealthReporter,
}

#[tonic::async_trait]
impl Health for HealthService {
    async fn check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let service = request.into_inner().service;
        match self.reporter.status_for(&service) {
            Some(status) => Ok(Response::new(HealthCheckResponse { status })),
            // Per the health spec, an unregistered service name is NOT_FOUND (the probe
            // then fails loud) rather than a silent SERVING.
            None => Err(Status::not_found(format!("unknown service: {service:?}"))),
        }
    }

    type WatchStream = Pin<Box<dyn Stream<Item = Result<HealthCheckResponse, Status>> + Send>>;

    async fn watch(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        Err(Status::unimplemented(
            "Health.Watch is not implemented; use Check (the unary probe Kubernetes and \
             grpc_health_probe use)",
        ))
    }
}

/// Serve the plaintext health service on `addr` until the returned future completes — no
/// TLS, no auth interceptor, so a kubelet probe reaches it directly (ADR-084).
pub(crate) async fn serve_health(
    addr: SocketAddr,
    reporter: HealthReporter,
) -> Result<(), tonic::transport::Error> {
    tonic::transport::Server::builder()
        .add_service(HealthServer::new(HealthService { reporter }))
        .serve(addr)
        .await
}

/// Spawn a background task that keeps `reporter`'s readiness in sync with `is_ready`,
/// re-evaluated at [`WATCH_INTERVAL`]. The first iteration runs immediately, so readiness
/// reflects reality within microseconds of the server starting. Re-evaluating (rather than
/// latching on first-true) handles both monotonic readiness — a shard that adopts a dict
/// stays ready — and transient readiness — a control node briefly without a leader during an
/// election. The task ends when the runtime is dropped (process exit).
pub(crate) fn spawn_readiness_watcher<F>(reporter: HealthReporter, mut is_ready: F)
where
    F: FnMut() -> bool + Send + 'static,
{
    tokio::spawn(async move {
        let mut last: Option<bool> = None;
        loop {
            let now = is_ready();
            if last != Some(now) {
                reporter.set_ready(now);
                last = Some(now);
            }
            tokio::time::sleep(WATCH_INTERVAL).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::{HealthReporter, ServingStatus, READINESS_SERVICE};

    fn serving() -> i32 {
        ServingStatus::Serving as i32
    }
    fn not_serving() -> i32 {
        ServingStatus::NotServing as i32
    }

    #[test]
    fn reporter_starts_live_but_not_ready() {
        // Liveness is SERVING the instant the health server starts; readiness waits.
        let r = HealthReporter::serving();
        assert_eq!(r.status_for(""), Some(serving()));
        assert_eq!(r.status_for(READINESS_SERVICE), Some(not_serving()));
    }

    #[test]
    fn set_ready_toggles_readiness_without_touching_liveness() {
        let r = HealthReporter::serving();
        r.set_ready(true);
        assert_eq!(r.status_for(READINESS_SERVICE), Some(serving()));
        assert_eq!(r.status_for(""), Some(serving()), "liveness stays up");
        r.set_ready(false);
        assert_eq!(r.status_for(READINESS_SERVICE), Some(not_serving()));
        assert_eq!(r.status_for(""), Some(serving()), "liveness still up");
    }

    #[test]
    fn unknown_service_is_not_registered() {
        // An unregistered name must be NOT_FOUND at the handler (None here), never a
        // silent SERVING that would mask a misconfigured probe.
        let r = HealthReporter::serving();
        assert_eq!(r.status_for("bogus"), None);
    }

    #[test]
    fn clone_shares_the_same_status_cell() {
        // The watcher holds one clone, the Check handler another — they must observe the
        // same atomics.
        let writer = HealthReporter::serving();
        let reader = writer.clone();
        writer.set_ready(true);
        assert_eq!(reader.status_for(READINESS_SERVICE), Some(serving()));
    }
}
