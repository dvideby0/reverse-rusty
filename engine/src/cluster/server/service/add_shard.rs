//! The `AddShard` RPC body — create a co-located shard slot on a node that has ALREADY adopted the
//! dict (ADR-093 Stage 2), reusing the node-scope frozen space by `Arc` WITHOUT re-shipping or
//! re-deserializing the (large) dict bytes. Split out of the [`ShardService`](super) trait impl,
//! which delegates here.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::cluster::proto;

use super::super::{ServerState, ShardServer, ShardSlot};

/// Body of [`ShardService::add_shard`](crate::cluster::proto::shard_service_server::ShardService::add_shard).
///
/// Co-location (ADR-093 Stage 2): the dict/tag space are already adopted at NODE scope (a prior
/// `AdoptDict` for an earlier position on this node); this creates the slot named by `req.shard_id`
/// over those SHARED `Arc`s. Contract:
/// - no prior `AdoptDict` on this node (node cell empty) → `failed_precondition` (a fresh node cannot
///   add a slot before it has a dict — in `connect_remote`'s build the first position on each endpoint
///   always adopts, so a co-located position finds the cell);
/// - the request's fingerprints disagree with the node's adopted space → `failed_precondition` (the
///   coordinator's frozen space diverges from what this node adopted — loud, never a silent slot whose
///   `FeatureId`s would not line up with the coordinator's routing);
/// - this slot already serves this exact fingerprint → idempotent no-op (safe on a coordinator
///   reconnect after a node restart self-restored the slot);
/// - otherwise build the slot's shard (in-memory, or durable under `data_dir/shard_<id>/`) over the
///   node-shared space and install it. NO dict deserialize, NO bytes, NO node-space re-persist.
pub(super) fn add_shard(
    server: &ShardServer,
    request: Request<proto::AddShardRequest>,
) -> Result<Response<proto::AddShardReply>, Status> {
    let req = request.into_inner();
    let shard_id = req.shard_id;

    // The node must have adopted a dict already (AddShard ships none).
    let node = server.node_dict.load_full().ok_or_else(|| {
        Status::failed_precondition(
            "AddShard requires a prior AdoptDict on this node (the node has adopted no dict)",
        )
    })?;
    let fp = node.dict.fingerprint();
    let tag_fp = node.tag_dict.fingerprint();

    // Attest the coordinator's frozen space equals the node's adopted one — a divergence would place a
    // slot whose `FeatureId`s do not line up with the coordinator's routing (a silent false negative).
    if fp != req.dict_fingerprint
        || tag_fp != req.tag_dict_fingerprint
        || node.placement_generation.0 != req.placement_generation
        || node.num_shards != req.num_shards
        || shard_id >= req.num_shards
    {
        return Err(Status::failed_precondition(format!(
            "AddShard fingerprint mismatch: node adopted dict {fp:#018x}/tag {tag_fp:#018x} but the \
             request attests {:#018x}/{:#018x} (the coordinator's frozen space diverges from this \
             node's)",
            req.dict_fingerprint, req.tag_dict_fingerprint
        )));
    }

    // Idempotent no-op: this slot already serves exactly this space (e.g. a restart self-restored it,
    // then the coordinator reconnected).
    if let Ok(slot) = server.slot(shard_id) {
        if let Some(st) = slot.state.load_full() {
            if st.dict.fingerprint() == fp && st.tag_dict.fingerprint() == tag_fp {
                return Ok(add_shard_reply(
                    fp,
                    tag_fp,
                    node.placement_generation,
                    node.num_shards,
                ));
            }
        }
    }

    // Build + install the slot over the node-shared `Arc`s — identical to the adopt path's tail.
    let shard =
        super::dict_adopt::build_slot_shard(server, shard_id, &node.dict, &node.tag_dict, fp)?;
    server.insert_slot(
        shard_id,
        ShardSlot::loaded(ServerState {
            dict: Arc::clone(&node.dict),
            tag_dict: Arc::clone(&node.tag_dict),
            shard,
        }),
    )?;

    Ok(add_shard_reply(
        fp,
        tag_fp,
        node.placement_generation,
        node.num_shards,
    ))
}

/// The add-shard reply — the node's frozen-dict + tag-dict fingerprints (equal the request's on
/// success), plus the ADR-080 replicate-to-all attestation (this binary always serves it).
fn add_shard_reply(
    fp: u64,
    tag_fp: u64,
    generation: crate::ownership::PlacementGeneration,
    num_shards: u32,
) -> Response<proto::AddShardReply> {
    Response::new(proto::AddShardReply {
        dict_fingerprint: fp,
        tag_dict_fingerprint: tag_fp,
        broad_replicate_all: true,
        placement_generation: generation.0,
        num_shards,
    })
}
