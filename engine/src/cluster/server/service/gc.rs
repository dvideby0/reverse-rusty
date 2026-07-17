//! Orphan-slot GC (ADR-096) — the `ListShards` / `DropShard` RPC bodies. Split out of the
//! [`ShardService`](super) trait impl, which delegates here.
//!
//! `ListShards` is the node-level inventory the coordinator's GC sweep classifies on; `DropShard`
//! is the guarded destructive step. The guard ladder (in order): node fingerprints must match →
//! an absent slot replies `dropped = false` (idempotent) → the slot must be FENCED at exactly the
//! request's generation (> 0 — a cold drop on a live unfenced slot is refused; fences are not
//! durable, so the coordinator ARMS a restarted orphan via the `Fence` probe first) → no
//! unexpired retention lease (an in-flight recovery's pinned source is never destroyed) → the
//! fence is RE-CHECKED under the map write lock at removal (the CAS — an interleaving
//! fence/unfence by a newer handoff fails the drop). Disk reclaim is rename-to-trash first
//! (atomically invisible to a restart), then best-effort delete; the boot sweep finishes an
//! interrupted delete.

use std::sync::atomic::Ordering;

use tonic::{Request, Response, Status};

use crate::cluster::proto;
use crate::cluster::shard::Shard;

use super::super::durable::reclaim_slot_dir;
use super::super::ShardServer;

/// Body of [`ShardService::list_shards`](crate::cluster::proto::shard_service_server::ShardService::list_shards).
pub(super) fn list_shards(
    server: &ShardServer,
    _request: Request<proto::Empty>,
) -> Result<Response<proto::ListShardsReply>, Status> {
    // Node identity: the node-scope adopted space's fingerprints (0/0 when pending — a real
    // fingerprint can never be 0, so a sweeping coordinator fails its identity check and skips a
    // pending node rather than classifying its slots).
    let (dict_fingerprint, tag_dict_fingerprint) = match server.node_dict.load_full() {
        Some(space) => (space.dict.fingerprint(), space.tag_dict.fingerprint()),
        None => (0, 0),
    };
    // Snapshot the slot `Arc`s under the read lock, then introspect lock-free (the handlers'
    // usual discipline — never hold the map lock across engine reads).
    let slots: Vec<(u32, std::sync::Arc<super::super::ShardSlot>)> = {
        let map = server
            .shards
            .read()
            .map_err(|_| Status::internal("shard map lock poisoned"))?;
        map.iter().map(|(&id, s)| (id, s.clone())).collect()
    };
    let mut shards = Vec::with_capacity(slots.len());
    for (shard_id, slot) in slots {
        let (num_queries, retention_leases_held) = match slot.state.load_full() {
            Some(st) => (
                st.shard
                    .num_queries()
                    .map_err(|e| Status::internal(e.to_string()))? as u64,
                st.shard.has_unexpired_retention_leases(),
            ),
            // A pending (never-adopted) slot holds no data and no leases.
            None => (0, false),
        };
        shards.push(proto::ShardListing {
            shard_id,
            fenced_at_generation: slot.fenced_at_generation.load(Ordering::Acquire),
            num_queries,
            retention_leases_held,
        });
    }
    shards.sort_unstable_by_key(|s| s.shard_id);
    Ok(Response::new(proto::ListShardsReply {
        shards,
        dict_fingerprint,
        tag_dict_fingerprint,
    }))
}

/// Body of [`ShardService::drop_shard`](crate::cluster::proto::shard_service_server::ShardService::drop_shard).
pub(super) fn drop_shard(
    server: &ShardServer,
    request: Request<proto::DropShardRequest>,
) -> Result<Response<proto::DropShardReply>, Status> {
    let req = request.into_inner();
    server.validate_placement_config(
        crate::ownership::PlacementGeneration(req.placement_generation),
        req.num_shards,
    )?;
    // Guard 1: node fingerprints (never GC against a divergent feature/tag space).
    let Some(space) = server.node_dict.load_full() else {
        return Err(Status::failed_precondition(
            "DropShard: this node has not adopted a dict (pending) — nothing it hosts can be \
             classified for GC",
        ));
    };
    if req.dict_fingerprint != space.dict.fingerprint() {
        return Err(Status::failed_precondition(
            "DropShard dict-fingerprint mismatch (divergent feature space)",
        ));
    }
    if req.tag_dict_fingerprint != space.tag_dict.fingerprint() {
        return Err(Status::failed_precondition(
            "DropShard tag-dict-fingerprint mismatch (divergent tag space)",
        ));
    }
    // Guard 2: a zero arm is structurally a cold drop — refused before any lookup.
    if req.expected_fence_generation == 0 {
        return Err(Status::invalid_argument(
            "DropShard: expected_fence_generation must be > 0 (fence the slot first — destroying \
             data requires the deliberate fence-then-drop two-step)",
        ));
    }
    // Guard 3: absent slot ⇒ the idempotent re-run.
    let Ok(slot) = server.slot(req.shard_id) else {
        return Ok(Response::new(proto::DropShardReply {
            dropped: false,
            num_queries: 0,
            dir_removed: true,
        }));
    };
    // Guard 4: fenced at exactly the armed generation.
    let gen = slot.fenced_at_generation.load(Ordering::Acquire);
    if gen != req.expected_fence_generation {
        return Err(Status::failed_precondition(format!(
            "DropShard: shard {} is fenced at generation {gen}, not the armed {} — a newer \
             handoff owns this slot; re-plan the drop",
            req.shard_id, req.expected_fence_generation
        )));
    }
    // Guard 5: no unexpired retention lease — the slot may be the pinned SOURCE of an in-flight
    // peer recovery (which deliberately reads through fences). A lease acquired after this check
    // races benignly: the recovery holds the slot `Arc` (reads complete against it) and the
    // renamed dir's already-open files stay readable — data-safe, documented in ADR-096.
    let num_queries = match slot.state.load_full() {
        Some(st) => {
            if st.shard.has_unexpired_retention_leases() {
                return Err(Status::failed_precondition(format!(
                    "DropShard: shard {} holds an unexpired retention lease (an in-flight \
                     recovery is reading from it); retry after it completes/expires",
                    req.shard_id
                )));
            }
            st.shard
                .num_queries()
                .map_err(|e| Status::internal(e.to_string()))? as u64
        }
        None => 0,
    };
    // Remove from the map, re-checking the fence under the WRITE lock (the CAS).
    if server
        .remove_slot_if_fenced_at(req.shard_id, req.expected_fence_generation)?
        .is_none()
    {
        // Raced an earlier drop between guard 3 and here — idempotent.
        return Ok(Response::new(proto::DropShardReply {
            dropped: false,
            num_queries: 0,
            dir_removed: true,
        }));
    }
    // Disk reclaim (durable nodes): rename-to-trash, then best-effort delete.
    let dir_removed = match &server.data_dir {
        None => true,
        Some(root) => reclaim_slot_dir(root, req.shard_id),
    };
    Ok(Response::new(proto::DropShardReply {
        dropped: true,
        num_queries,
        dir_removed,
    }))
}
