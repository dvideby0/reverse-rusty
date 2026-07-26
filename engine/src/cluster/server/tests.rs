//! `ShardServer` unit tests (the dict-adopt state machine + the per-shard write fence).

use std::sync::Arc;

use tonic::{Code, Request};

use super::durable::is_dropped_trash;
use super::ShardServer;
use crate::cluster::proto;
use crate::cluster::proto::shard_service_server::ShardService;
use crate::compile::extract;
use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::normalize::Normalizer;
use crate::storage::serialize_dict;
use crate::tagdict::TagDict;

const TEST_NUM_SHARDS: u32 = 16;

fn placed_at(shard_id: u32, num_shards: u32) -> proto::QueryPlacement {
    proto::placement_to_proto(
        &crate::ownership::QueryPlacement::selective(
            crate::ownership::PlacementGeneration::INITIAL,
            num_shards,
            vec![shard_id],
        )
        .expect("valid test placement"),
    )
}

fn norm() -> Arc<Normalizer> {
    Arc::new(Normalizer::default_vocab().expect("built-in vocab"))
}

/// A frozen dict interned over `snips` in order (mirrors the gRPC oracle helper).
fn frozen_dict(snips: &[&str], norm: &Normalizer) -> Dict {
    let mut d = Dict::new();
    let mut lc = String::new();
    for q in snips {
        if let Ok(ast) = crate::dsl::parse(q) {
            let _ = extract(&ast, norm, &mut d, &mut lc);
        }
    }
    d.finalize_mask();
    d
}

/// The fingerprint an empty (untagged) adopt installs — the empty blob deserializes to an empty
/// `TagDict`, so a `Fence`/`Unfence` must present this exact value.
fn empty_tag_fp() -> u64 {
    TagDict::new().fingerprint()
}

/// An `AdoptDict` request naming slot `shard_id` over `dict`, untagged (an empty tag-dict blob
/// deserializes to an empty `TagDict`, whose fingerprint the request must claim).
fn adopt_req_shard(dict: &Dict, shard_id: u32) -> Request<proto::AdoptDictRequest> {
    Request::new(proto::AdoptDictRequest {
        dict: serialize_dict(dict),
        fingerprint: dict.fingerprint(),
        tag_dict: Vec::new(),
        tag_dict_fingerprint: empty_tag_fp(),
        shard_id,
        placement_generation: crate::ownership::PlacementGeneration::INITIAL.get(),
        num_shards: TEST_NUM_SHARDS,
        compiler_semantics_version: crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION,
    })
}

/// The common single-shard adopt: slot 0.
fn adopt_req(dict: &Dict) -> Request<proto::AdoptDictRequest> {
    adopt_req_shard(dict, 0)
}

fn current_fp(srv: &ShardServer) -> u64 {
    srv.slot(0)
        .expect("slot 0")
        .state
        .load_full()
        .expect("adopted")
        .dict
        .fingerprint()
}

/// An `InsertRequest` targeting `shard_id` — the write-path builder shared by the fence tests.
fn insert_req(shard_id: u32, id: u64, dsl: &str) -> Request<proto::InsertRequest> {
    Request::new(proto::InsertRequest {
        item: Some(proto::AddItem {
            logical_id: id,
            dsl: dsl.to_string(),
            version: 1,
            tags: Vec::new(),
            placement: Some(placed_at(shard_id, TEST_NUM_SHARDS)),
        }),
        shard_id,
    })
}

fn insert_req_single(id: u64, dsl: &str) -> Request<proto::InsertRequest> {
    Request::new(proto::InsertRequest {
        item: Some(proto::AddItem {
            logical_id: id,
            dsl: dsl.to_string(),
            version: 1,
            tags: Vec::new(),
            placement: Some(placed_at(0, 1)),
        }),
        shard_id: 0,
    })
}

/// An `AddShardRequest` naming slot `shard_id`, attesting the node's fingerprints.
fn add_shard_req(shard_id: u32, fp: u64, tag_fp: u64) -> Request<proto::AddShardRequest> {
    Request::new(proto::AddShardRequest {
        shard_id,
        dict_fingerprint: fp,
        tag_dict_fingerprint: tag_fp,
        placement_generation: 1,
        num_shards: TEST_NUM_SHARDS,
        compiler_semantics_version: crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION,
    })
}

fn drop_req(
    shard_id: u32,
    expected_gen: u64,
    fp: u64,
    tag_fp: u64,
) -> Request<proto::DropShardRequest> {
    Request::new(proto::DropShardRequest {
        shard_id,
        expected_fence_generation: expected_gen,
        dict_fingerprint: fp,
        tag_dict_fingerprint: tag_fp,
        placement_generation: 1,
        num_shards: 1,
    })
}

mod add_shard;
mod adopt;
mod fence;
mod gc;
mod limits;
