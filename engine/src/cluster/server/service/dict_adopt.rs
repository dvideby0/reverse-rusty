//! The `AdoptDict` RPC body — adopt a frozen dict + tag space shipped by the coordinator
//! (ADR-034/055) and create the named slot (ADR-093). Split out of the [`ShardService`](super)
//! trait impl, which delegates here.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::cluster::coordinator::shard_dir;
use crate::cluster::proto;
use crate::cluster::shard::LocalShard;

use super::super::{AdoptedSpace, ServerState, ShardServer, ShardSlot};

/// Body of [`ShardService::adopt_dict`](crate::cluster::proto::shard_service_server::ShardService::adopt_dict).
///
/// Multi-shard (ADR-093): the dict/tag space are adopted at NODE scope (deserialized once, shared into
/// every slot by `Arc`); `req.shard_id` names which slot's `LocalShard` to build over that shared space.
/// Contract, per slot / per node:
/// - bad bytes / a fingerprint disagreeing with the deserialized dict → `invalid_argument`;
/// - this slot already serves this exact dict + tag space → idempotent no-op;
/// - a DIVERGENT node dict while ANY slot holds data → `failed_precondition` (the dict is node-shared,
///   so re-basing loaded data onto a divergent feature space would silently corrupt matches);
/// - otherwise build the slot's shard (in-memory, or durable under `data_dir/shard_<id>/`) and install
///   it, (re)placing the node-scope dict when it changed.
pub(super) fn adopt_dict(
    server: &ShardServer,
    request: Request<proto::AdoptDictRequest>,
) -> Result<Response<proto::AdoptDictReply>, Status> {
    let req = request.into_inner();
    let shard_id = req.shard_id;
    let dict = crate::storage::deserialize_dict(&req.dict)
        .map_err(|e| Status::invalid_argument(format!("deserializing shipped dict: {e}")))?;
    let fp = dict.fingerprint();
    if fp != req.fingerprint {
        return Err(Status::invalid_argument(format!(
            "shipped dict integrity check failed: bytes fingerprint to {fp:#018x} but the \
             request claims {:#018x}",
            req.fingerprint
        )));
    }
    // The frozen tag space ships ATOMICALLY with the dict (ADR-055). An empty blob ⇒ an empty
    // (untagged) tag space — back-compatible with a coordinator that ships no tags.
    let tag_dict = crate::storage::deserialize_tagdict(&req.tag_dict)
        .map_err(|e| Status::invalid_argument(format!("deserializing shipped tag dict: {e}")))?;
    let tag_fp = tag_dict.fingerprint();
    if tag_fp != req.tag_dict_fingerprint {
        return Err(Status::invalid_argument(format!(
            "shipped tag-dict integrity check failed: bytes fingerprint to {tag_fp:#018x} but \
             the request claims {:#018x}",
            req.tag_dict_fingerprint
        )));
    }

    // Idempotent no-op: this slot already serves exactly this dict AND tag space.
    if let Ok(slot) = server.slot(shard_id) {
        if let Some(st) = slot.state.load_full() {
            if st.dict.fingerprint() == fp && st.tag_dict.fingerprint() == tag_fp {
                return Ok(adopt_reply(fp, tag_fp));
            }
        }
    }

    // Node-scope adopt (deserialize ONCE per node): reuse the node's `Arc`s when the fingerprints
    // already match (so every slot shares one `Arc<Dict>`), else (re)place the node dict — but only
    // when no slot holds data, since the dict is node-shared.
    let node = server.node_dict.load_full();
    let node_matches = node
        .as_deref()
        .is_some_and(|s| s.dict.fingerprint() == fp && s.tag_dict.fingerprint() == tag_fp);
    let (space_dict, space_tag) = if let (true, Some(s)) = (node_matches, node.as_deref()) {
        (Arc::clone(&s.dict), Arc::clone(&s.tag_dict))
    } else {
        if node.is_some() && server.any_slot_populated()? {
            return Err(Status::failed_precondition(format!(
                "node already hosts loaded shards under a different feature space; refusing to \
                 adopt a divergent dict {fp:#018x} (re-basing loaded data is unsafe)"
            )));
        }
        (Arc::new(dict), Arc::new(tag_dict))
    };

    // Build this slot's shard over the node-shared space.
    let shard = match &server.data_dir {
        Some(root) => {
            // The slot's segments/translog live under a PER-SHARD subdir (ADR-093), matching the
            // coordinator's `shard_<NNN>/` layout.
            let dir = shard_dir(root, shard_id as usize);
            // The DISK is part of the divergence check (ADR-072): the subdir volume may hold
            // segments/translog built under another dict even while the in-RAM slot is pending (a
            // restart racing an adopt). Refuse loud — persisting over a divergent durable state would
            // poison the dict.bin↔sidecar pair and crash-loop every later self-restore.
            if let Some(ckpt) = crate::cluster::translog::read_sidecar(&dir)
                .map_err(|e| Status::internal(format!("reading shard checkpoint: {e}")))?
            {
                if ckpt.dict_fingerprint != fp {
                    return Err(Status::failed_precondition(format!(
                        "durable state under {} was built with dict {:#018x}; refusing to adopt a \
                         divergent dict {fp:#018x} (wipe the data dir to re-seed this node)",
                        dir.display(),
                        ckpt.dict_fingerprint
                    )));
                }
            }
            let mut sc = server.config.clone();
            sc.data_dir = Some(dir);
            let shard = LocalShard::new_durable(
                Arc::clone(&server.norm),
                Arc::clone(&space_dict),
                Arc::clone(&space_tag),
                sc,
            )
            .map_err(|e| Status::internal(format!("durable adopt: {e}")))?;
            // Persist the (verified) shipped bytes at the NODE root, only when the node space changed
            // (write-once / idempotent-on-fp) and only after the durable shard accepted them — so a
            // node that crashes after acknowledging can self-restore without a coordinator (ADR-072),
            // and a failed/refused adopt never overwrites the previously persisted space.
            if !node_matches {
                super::super::persist_adopted_space(root, &req.dict, &req.tag_dict).map_err(
                    |e| {
                        Status::internal(format!(
                            "persisting adopted dict under {}: {e}",
                            root.display()
                        ))
                    },
                )?;
            }
            shard
        }
        None => LocalShard::new(
            Arc::clone(&server.norm),
            Arc::clone(&space_dict),
            Arc::clone(&space_tag),
            server.config.clone(),
        ),
    };

    // Install the node-scope dict (if it changed) then the slot. Order: node dict first, so the
    // `DictFingerprint` handshake never sees a slot referencing a space the node cell lacks.
    if !node_matches {
        server.node_dict.store(Some(Arc::new(AdoptedSpace {
            dict: Arc::clone(&space_dict),
            tag_dict: Arc::clone(&space_tag),
        })));
    }
    server.insert_slot(
        shard_id,
        ShardSlot::loaded(ServerState {
            dict: space_dict,
            tag_dict: space_tag,
            shard,
        }),
    )?;

    Ok(adopt_reply(fp, tag_fp))
}

/// The adopt reply — the server's frozen-dict + tag-dict fingerprints after adoption, plus the
/// ADR-080 replicate-to-all layout attestation (this binary always serves it).
fn adopt_reply(fp: u64, tag_fp: u64) -> Response<proto::AdoptDictReply> {
    Response::new(proto::AdoptDictReply {
        fingerprint: fp,
        tag_dict_fingerprint: tag_fp,
        broad_replicate_all: true,
    })
}
