//! Translog retention leases (ADR-040) + the live-handoff write fence / un-fence
//! (ADR-044/048) — the `RetentionLease` / `Fence` / `Unfence` RPC bodies. Split out of the
//! [`ShardService`](super) trait impl, which delegates here.

use std::sync::atomic::Ordering;

use tonic::{Request, Response, Status};

use crate::cluster::clog::LogPos;
use crate::cluster::proto;
use crate::cluster::shard::Shard;

use super::super::ShardServer;

/// Body of [`ShardService::retention_lease`](crate::cluster::proto::shard_service_server::ShardService::retention_lease).
pub(super) fn retention_lease(
    server: &ShardServer,
    request: Request<proto::RetentionLeaseRequest>,
) -> Result<Response<proto::RetentionLeaseReply>, Status> {
    let st = server.loaded()?;
    let req = request.into_inner();
    if req.dict_fingerprint != st.dict.fingerprint() {
        return Err(Status::failed_precondition(
            "RetentionLease dict-fingerprint mismatch (divergent feature space)",
        ));
    }
    match req.op {
        0 => {
            let (lease_id, pos) = st
                .shard
                .acquire_retention_lease()
                .map_err(|e| Status::internal(format!("acquire retention lease: {e}")))?;
            Ok(Response::new(proto::RetentionLeaseReply {
                lease_id,
                pos: pos.0,
            }))
        }
        1 => {
            st.shard
                .renew_retention_lease(req.lease_id, LogPos(req.pos))
                .map_err(|e| Status::internal(format!("renew retention lease: {e}")))?;
            Ok(Response::new(proto::RetentionLeaseReply::default()))
        }
        2 => {
            st.shard
                .release_retention_lease(req.lease_id)
                .map_err(|e| Status::internal(format!("release retention lease: {e}")))?;
            Ok(Response::new(proto::RetentionLeaseReply::default()))
        }
        other => Err(Status::invalid_argument(format!(
            "RetentionLease: unknown op {other} (expected 0=acquire, 1=renew, 2=release)"
        ))),
    }
}

/// Body of [`ShardService::fence`](crate::cluster::proto::shard_service_server::ShardService::fence).
pub(super) fn fence(
    server: &ShardServer,
    request: Request<proto::FenceRequest>,
) -> Result<Response<proto::FenceReply>, Status> {
    let st = server.loaded()?;
    let req = request.into_inner();
    if req.dict_fingerprint != st.dict.fingerprint() {
        return Err(Status::failed_precondition(
            "Fence dict-fingerprint mismatch (divergent feature space)",
        ));
    }
    // Monotonic max: a later, lower-generation Fence (a stale/duplicate message) never lowers
    // the fence. `fetch_max` returns the previous value; the stored value becomes the max.
    let prev = server
        .fenced_at_generation
        .fetch_max(req.generation, Ordering::AcqRel);
    Ok(Response::new(proto::FenceReply {
        fenced_at_generation: prev.max(req.generation),
    }))
}

/// Body of [`ShardService::unfence`](crate::cluster::proto::shard_service_server::ShardService::unfence).
pub(super) fn unfence(
    server: &ShardServer,
    request: Request<proto::UnfenceRequest>,
) -> Result<Response<proto::UnfenceReply>, Status> {
    let st = server.loaded()?;
    let req = request.into_inner();
    if req.dict_fingerprint != st.dict.fingerprint() {
        return Err(Status::failed_precondition(
            "Unfence dict-fingerprint mismatch (divergent feature space)",
        ));
    }
    // CAS from the exact generation this handoff fenced at. If the node is at 0 (not fenced)
    // or at a higher generation (a newer handoff re-fenced it), the swap fails and the fence
    // is left as-is — we report its current value.
    let now_gen = match server.fenced_at_generation.compare_exchange(
        req.generation,
        0,
        Ordering::AcqRel,
        Ordering::Acquire,
    ) {
        Ok(_) => 0,
        Err(actual) => actual,
    };
    Ok(Response::new(proto::UnfenceReply {
        fenced_at_generation: now_gen,
    }))
}
