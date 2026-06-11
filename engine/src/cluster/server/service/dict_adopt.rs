//! The `AdoptDict` RPC body — adopt a frozen dict + tag space shipped by the coordinator
//! (ADR-034/055). Split out of the [`ShardService`](super) trait impl, which delegates here.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::cluster::proto;
use crate::cluster::shard::{LocalShard, Shard};

use super::super::{ServerState, ShardServer};

/// Body of [`ShardService::adopt_dict`](crate::cluster::proto::shard_service_server::ShardService::adopt_dict).
pub(super) fn adopt_dict(
    server: &ShardServer,
    request: Request<proto::AdoptDictRequest>,
) -> Result<Response<proto::AdoptDictReply>, Status> {
    let req = request.into_inner();
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

    let adopt = match server.state.load_full().as_deref() {
        // Already serving this exact dict AND tag space → nothing to do.
        Some(st) if st.dict.fingerprint() == fp && st.tag_dict.fingerprint() == tag_fp => false,
        // A different dict / tag space is already in place; only safe to replace if no data
        // depends on it (re-basing loaded data onto a divergent feature/tag space is unsafe).
        Some(st) => {
            let n = st
                .shard
                .num_queries()
                .map_err(|e| Status::internal(e.to_string()))?;
            if n > 0 {
                return Err(Status::failed_precondition(format!(
                    "shard holds {n} queries under dict {:#018x}; refusing to adopt a \
                     divergent dict {fp:#018x} / tag space (re-basing loaded data is unsafe)",
                    st.dict.fingerprint()
                )));
            }
            true // adopted but empty → safe to re-adopt (e.g. a pre-built `new` server gaining tags)
        }
        // Pending → adopt.
        None => true,
    };

    if adopt {
        let dict = Arc::new(dict);
        let tag_dict = Arc::new(tag_dict);
        // A durable node (data_dir set) builds a segments-only durable shard so its writes
        // persist `.seg` files — required to later serve `FetchSegments` or be a recovering
        // replica (ADR-035/036). An in-memory node keeps today's behavior.
        let shard = match &server.data_dir {
            Some(dir) => {
                // The DISK is part of the divergence check (ADR-072): the volume may hold
                // segments/translog built under another dict even while the in-RAM state
                // is pending (a restart racing an adopt). Refuse loud — persisting the
                // shipped bytes over a divergent durable state would poison the
                // dict.bin↔sidecar pair and crash-loop every later self-restore.
                if let Some(ckpt) = crate::cluster::translog::read_sidecar(dir)
                    .map_err(|e| Status::internal(format!("reading shard checkpoint: {e}")))?
                {
                    // ANY divergence refuses — even with no committed segments the
                    // translog can hold acknowledged writes compiled under the old
                    // space. Re-seeding an intentionally repurposed node = wipe the dir.
                    if ckpt.dict_fingerprint != fp {
                        return Err(Status::failed_precondition(format!(
                            "durable state under {} was built with dict {:#018x}; refusing \
                             to adopt a divergent dict {fp:#018x} (wipe the data dir to re-seed \
                             this node)",
                            dir.display(),
                            ckpt.dict_fingerprint
                        )));
                    }
                }
                let mut sc = server.config.clone();
                sc.data_dir = Some(dir.clone());
                let shard = LocalShard::new_durable(
                    Arc::clone(&server.norm),
                    Arc::clone(&dict),
                    Arc::clone(&tag_dict),
                    sc,
                )
                .map_err(|e| Status::internal(format!("durable adopt: {e}")))?;
                // Persist the (verified) shipped bytes LAST — only after the durable shard
                // accepted them — so a node that crashes after acknowledging can
                // self-restore without a coordinator (ADR-072), and a failed/refused adopt
                // never overwrites the previously persisted space. A crash between the
                // build and this persist just reverts the node to pending (the coordinator
                // re-ships at its next connect; the shard held no data yet).
                super::super::persist_adopted_space(dir, &req.dict, &req.tag_dict).map_err(
                    |e| {
                        Status::internal(format!(
                            "persisting adopted dict under {}: {e}",
                            dir.display()
                        ))
                    },
                )?;
                shard
            }
            None => LocalShard::new(
                Arc::clone(&server.norm),
                Arc::clone(&dict),
                Arc::clone(&tag_dict),
                server.config.clone(),
            ),
        };
        server.state.store(Some(Arc::new(ServerState {
            dict,
            tag_dict,
            shard,
        })));
    }

    Ok(Response::new(proto::AdoptDictReply {
        fingerprint: fp,
        tag_dict_fingerprint: tag_fp,
    }))
}
