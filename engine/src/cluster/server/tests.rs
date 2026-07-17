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

/// Exercises every arm of the `AdoptDict` contract through the real async handler:
/// pending-read-fails, empty→adopt, same-fp→no-op, bad-fp→invalid, empty-different→re-adopt,
/// and non-empty-divergent→refuse (the load-bearing silent-FN guard).
#[test]
fn adopt_dict_state_machine() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d1 = frozen_dict(&["1994 upper deck", "psa 10"], &n);
    let d2 = frozen_dict(&["1994 upper deck", "psa 10", "1995 fleer ultra"], &n);
    assert_ne!(
        d1.fingerprint(),
        d2.fingerprint(),
        "test setup: the two dicts must differ"
    );

    let srv = ShardServer::pending(Arc::clone(&n), EngineConfig::default());
    // Pending: no slot exists yet, so a read fails loud (NotFound — the slot is absent) rather than
    // fabricating an empty result (ADR-093: slots are created by AdoptDict).
    assert!(srv.slot(0).is_err(), "a pending node hosts no slot");
    let err = rt
        .block_on(srv.num_queries(Request::new(proto::ShardRef { shard_id: 0 })))
        .expect_err("pending read must fail");
    assert_eq!(err.code(), Code::NotFound);

    // Empty → adopt d1 (creates slot 0).
    let fp = rt
        .block_on(srv.adopt_dict(adopt_req(&d1)))
        .expect("adopt onto empty")
        .into_inner()
        .fingerprint;
    assert_eq!(fp, d1.fingerprint());
    assert_eq!(current_fp(&srv), d1.fingerprint());

    // Same dict again → idempotent no-op.
    rt.block_on(srv.adopt_dict(adopt_req(&d1)))
        .expect("re-adopt same dict is a no-op");
    assert_eq!(current_fp(&srv), d1.fingerprint());

    // Integrity: d2 bytes but d1's claimed fingerprint → invalid_argument.
    let bad = Request::new(proto::AdoptDictRequest {
        dict: serialize_dict(&d2),
        fingerprint: d1.fingerprint(),
        tag_dict: Vec::new(),
        tag_dict_fingerprint: empty_tag_fp(),
        shard_id: 0,
        placement_generation: crate::ownership::PlacementGeneration::INITIAL.get(),
        num_shards: TEST_NUM_SHARDS,
    });
    assert_eq!(
        rt.block_on(srv.adopt_dict(bad))
            .expect_err("fingerprint mismatch must be rejected")
            .code(),
        Code::InvalidArgument
    );

    // Empty shard, different valid dict → re-adopt allowed (no data at risk).
    rt.block_on(srv.adopt_dict(adopt_req(&d2)))
        .expect("re-adopt onto still-empty shard");
    assert_eq!(current_fp(&srv), d2.fingerprint());

    // Load data, then a DIVERGENT dict → refused (the silent-FN guard).
    srv.ingest_dsl(&[(1u64, "1994 upper deck".to_string())]);
    let n_loaded = rt
        .block_on(srv.num_queries(Request::new(proto::ShardRef { shard_id: 0 })))
        .expect("count after load")
        .into_inner()
        .count;
    assert!(n_loaded >= 1, "expected loaded data, got {n_loaded}");
    assert_eq!(
        rt.block_on(srv.adopt_dict(adopt_req(&d1)))
            .expect_err("divergent dict on a non-empty shard must be refused")
            .code(),
        Code::FailedPrecondition
    );
    // The SAME dict on a non-empty shard is still a no-op (not refused).
    rt.block_on(srv.adopt_dict(adopt_req(&d2)))
        .expect("same dict on a populated shard is a no-op");
    assert_eq!(current_fp(&srv), d2.fingerprint());
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

/// The live-handoff write fence (ADR-044): once `Fence` lands, data-mutating writes
/// (`insert`/`delete`/`ingest`) are rejected with `FailedPrecondition`, while reads stay served
/// (serve-then-drop); the fence is monotonic and dict-fingerprint-guarded.
#[test]
fn fence_rejects_writes_but_serves_reads() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["1994 upper deck", "psa 10"], &n);
    let fp = d.fingerprint();
    // ADR-077: `ShardServer::new` starts with the FINALIZED empty tag space; fences
    // must present its fingerprint exactly like the dict's.
    let tag_fp = {
        let mut td = TagDict::new();
        td.mark_finalized();
        td.fingerprint()
    };
    let srv = ShardServer::new(Arc::clone(&n), Arc::new(d), EngineConfig::default());
    srv.ingest_dsl(&[(1u64, "1994 upper deck".to_string())]);

    // Before the fence: a write succeeds.
    rt.block_on(srv.insert_extracted(insert_req_single(2, "psa 10")))
        .expect("insert before fence");

    // Fence at generation 5.
    let fenced = rt
        .block_on(srv.fence(Request::new(proto::FenceRequest {
            generation: 5,
            dict_fingerprint: fp,
            tag_dict_fingerprint: tag_fp,
            shard_id: 0,
            placement_generation: 1,
            num_shards: 1,
        })))
        .expect("fence")
        .into_inner()
        .fenced_at_generation;
    assert_eq!(fenced, 5);

    // After the fence: every data-mutating write is rejected.
    assert_eq!(
        rt.block_on(srv.insert_extracted(insert_req_single(3, "psa 10")))
            .expect_err("insert after fence")
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        rt.block_on(srv.delete(Request::new(proto::DeleteRequest {
            logical_id: 1,
            shard_id: 0,
            placement_generation: 1,
            num_shards: 1,
        })))
        .expect_err("delete after fence")
        .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        rt.block_on(srv.ingest_extracted(Request::new(proto::IngestRequest {
            items: vec![],
            shard_id: 0,
        })))
        .expect_err("ingest after fence")
        .code(),
        Code::FailedPrecondition
    );

    // ...but reads still serve (serve-then-drop): num_queries + percolate keep working.
    let cnt = rt
        .block_on(srv.num_queries(Request::new(proto::ShardRef { shard_id: 0 })))
        .expect("read after fence")
        .into_inner()
        .count;
    assert!(cnt >= 1, "reads stay served while fenced: {cnt}");
    rt.block_on(
        srv.percolate(Request::new(proto::PercolateRequest {
            title: "1994 upper deck".to_string(),
            include_broad: false,
            filter: Vec::new(),
            rank: None,
            shard_id: 0,
            ownership: Some(proto::ownership_to_proto(
                &crate::ownership::OwnershipContext::new(
                    crate::ownership::PlacementGeneration::INITIAL,
                    1,
                    vec![0],
                    None,
                )
                .expect("ownership context"),
            )),
        })),
    )
    .expect("percolate after fence");

    // Monotonic: a stale, lower-generation fence never lowers the fence.
    let after_stale = rt
        .block_on(srv.fence(Request::new(proto::FenceRequest {
            generation: 3,
            dict_fingerprint: fp,
            tag_dict_fingerprint: tag_fp,
            shard_id: 0,
            placement_generation: 1,
            num_shards: 1,
        })))
        .expect("stale fence")
        .into_inner()
        .fenced_at_generation;
    assert_eq!(after_stale, 5, "a lower-gen fence must not lower the fence");
    assert_eq!(
        rt.block_on(srv.insert_extracted(insert_req_single(4, "psa 10")))
            .expect_err("still fenced after a stale fence")
            .code(),
        Code::FailedPrecondition
    );

    // A dict-fingerprint mismatch is refused (never fences across a divergent feature space).
    assert_eq!(
        rt.block_on(srv.fence(Request::new(proto::FenceRequest {
            generation: 9,
            dict_fingerprint: fp ^ 0xDEAD_BEEF,
            tag_dict_fingerprint: tag_fp,
            shard_id: 0,
            placement_generation: 1,
            num_shards: 1,
        })))
        .expect_err("fence fp mismatch")
        .code(),
        Code::FailedPrecondition
    );
}

/// The codex-P1 fix (ADR-093): a `ShardServer` hosting TWO slots keeps their fences INDEPENDENT.
/// Fencing shard 0 for a handoff must NOT write-quiesce a co-located shard 1 on the same node — a
/// single shared `AtomicU64` (the pre-ADR-093 design) could not pass this. A single process CAN host
/// two slots here even though the Stage 1 deployment stays 1:1.
#[test]
fn per_shard_fence_isolation() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["1994 upper deck", "psa 10"], &n);
    let fp = d.fingerprint();
    let tag_fp = empty_tag_fp();

    let srv = ShardServer::pending(Arc::clone(&n), EngineConfig::default());
    // Two adopts over the SAME node dict, different shard-ids → the dict is deserialized ONCE
    // (node-scope), two independent slots created.
    rt.block_on(srv.adopt_dict(adopt_req_shard(&d, 0)))
        .expect("adopt slot 0");
    rt.block_on(srv.adopt_dict(adopt_req_shard(&d, 1)))
        .expect("adopt slot 1");

    // Seed each slot with one query via the insert handler.
    rt.block_on(srv.insert_extracted(insert_req(0, 10, "psa 10")))
        .expect("write slot 0");
    rt.block_on(srv.insert_extracted(insert_req(1, 11, "psa 10")))
        .expect("write slot 1");

    // Fence ONLY shard 0.
    let fenced = rt
        .block_on(srv.fence(Request::new(proto::FenceRequest {
            generation: 5,
            dict_fingerprint: fp,
            tag_dict_fingerprint: tag_fp,
            shard_id: 0,
            placement_generation: 1,
            num_shards: TEST_NUM_SHARDS,
        })))
        .expect("fence slot 0")
        .into_inner()
        .fenced_at_generation;
    assert_eq!(fenced, 5);

    // Slot 0 writes are now rejected...
    assert_eq!(
        rt.block_on(srv.insert_extracted(insert_req(0, 12, "psa 10")))
            .expect_err("slot 0 is fenced")
            .code(),
        Code::FailedPrecondition
    );
    // ...but slot 1 stays writable — THE per-shard-fence isolation (codex P1 fixed).
    rt.block_on(srv.insert_extracted(insert_req(1, 13, "psa 10")))
        .expect("slot 1 must stay writable while slot 0 is fenced");

    // Un-fence slot 0 → both writable again.
    let now = rt
        .block_on(srv.unfence(Request::new(proto::UnfenceRequest {
            generation: 5,
            dict_fingerprint: fp,
            tag_dict_fingerprint: tag_fp,
            shard_id: 0,
            placement_generation: 1,
            num_shards: TEST_NUM_SHARDS,
        })))
        .expect("unfence slot 0")
        .into_inner()
        .fenced_at_generation;
    assert_eq!(now, 0, "slot 0 is un-fenced");
    rt.block_on(srv.insert_extracted(insert_req(0, 14, "psa 10")))
        .expect("slot 0 writable after unfence");
}

/// An `AddShardRequest` naming slot `shard_id`, attesting the node's (untagged) fingerprints.
fn add_shard_req(shard_id: u32, fp: u64, tag_fp: u64) -> Request<proto::AddShardRequest> {
    Request::new(proto::AddShardRequest {
        shard_id,
        dict_fingerprint: fp,
        tag_dict_fingerprint: tag_fp,
        placement_generation: 1,
        num_shards: TEST_NUM_SHARDS,
    })
}

/// AddShard on a node that has adopted NO dict is refused (ADR-093 Stage 2): a co-located slot may
/// only be created once the node holds the frozen space the request attests. In `connect_remote`'s
/// build the first position on each endpoint always adopts first, so this is a guard, not a normal path.
#[test]
fn add_shard_before_adopt_fails() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["psa 10"], &n);
    let srv = ShardServer::pending(Arc::clone(&n), EngineConfig::default());
    assert_eq!(
        rt.block_on(srv.add_shard(add_shard_req(0, d.fingerprint(), empty_tag_fp())))
            .expect_err("AddShard before any AdoptDict must be refused")
            .code(),
        Code::FailedPrecondition
    );
}

/// AddShard creates a co-located slot over the node's ALREADY-adopted dict without re-shipping bytes
/// (ADR-093 Stage 2): after adopting slot 0, AddShard(1) makes slot 1 writable/readable, while an
/// un-created slot 2 is `not_found`.
#[test]
fn add_shard_after_adopt_creates_slot() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["psa 10"], &n);
    let (fp, tag_fp) = (d.fingerprint(), empty_tag_fp());
    let srv = ShardServer::pending(Arc::clone(&n), EngineConfig::default());
    rt.block_on(srv.adopt_dict(adopt_req_shard(&d, 0)))
        .expect("adopt slot 0 (ships the dict)");
    // Co-located slot — NO dict bytes shipped, just the fingerprint attestation.
    rt.block_on(srv.add_shard(add_shard_req(1, fp, tag_fp)))
        .expect("add co-located slot 1");
    rt.block_on(srv.insert_extracted(insert_req(1, 11, "psa 10")))
        .expect("write the co-located slot");
    let n1 = rt
        .block_on(srv.num_queries(Request::new(proto::ShardRef { shard_id: 1 })))
        .expect("count slot 1")
        .into_inner()
        .count;
    assert_eq!(n1, 1, "the co-located slot holds its own query");
    assert_eq!(
        rt.block_on(srv.num_queries(Request::new(proto::ShardRef { shard_id: 2 })))
            .expect_err("slot 2 was never created")
            .code(),
        Code::NotFound
    );
}

/// AddShard whose attested fingerprint disagrees with the node's adopted dict is refused (ADR-093
/// Stage 2): placing a slot under a divergent space would mis-route reads (a silent false negative).
#[test]
fn add_shard_wrong_fingerprint_fails() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["psa 10"], &n);
    let tag_fp = empty_tag_fp();
    let srv = ShardServer::pending(Arc::clone(&n), EngineConfig::default());
    rt.block_on(srv.adopt_dict(adopt_req_shard(&d, 0)))
        .expect("adopt slot 0");
    assert_eq!(
        rt.block_on(srv.add_shard(add_shard_req(1, d.fingerprint() ^ 0xDEAD, tag_fp)))
            .expect_err("a divergent fingerprint must be refused")
            .code(),
        Code::FailedPrecondition
    );
}

/// AddShard is idempotent on the slot's fingerprint (ADR-093 Stage 2): a repeat (e.g. a coordinator
/// reconnect after a restart self-restored the slot) is a safe no-op, not a rebuild that wipes data.
#[test]
fn add_shard_idempotent() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["psa 10"], &n);
    let (fp, tag_fp) = (d.fingerprint(), empty_tag_fp());
    let srv = ShardServer::pending(Arc::clone(&n), EngineConfig::default());
    rt.block_on(srv.adopt_dict(adopt_req_shard(&d, 0)))
        .expect("adopt slot 0");
    rt.block_on(srv.add_shard(add_shard_req(1, fp, tag_fp)))
        .expect("add slot 1");
    rt.block_on(srv.insert_extracted(insert_req(1, 11, "psa 10")))
        .expect("seed slot 1");
    // A second AddShard for the same slot is a no-op — it must NOT wipe the slot's data.
    rt.block_on(srv.add_shard(add_shard_req(1, fp, tag_fp)))
        .expect("repeat add_shard is idempotent");
    let n1 = rt
        .block_on(srv.num_queries(Request::new(proto::ShardRef { shard_id: 1 })))
        .expect("count slot 1")
        .into_inner()
        .count;
    assert_eq!(n1, 1, "idempotent add_shard preserved the slot's query");
}

/// Regression (codex review, ADR-093): a node hosting a NON-ZERO position slot must report its real
/// `/_metrics` — `metrics_source().render()` reads the hosted slot, not slot 0. In the 1:1 deployment a
/// node serving position N hosts ONLY slot N, so a slot-0-specific lookup would show a live position-N
/// node as `reverse_rusty_shard_ready 0` while it serves traffic.
#[test]
fn metrics_render_the_hosted_nonzero_slot() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["1994 upper deck", "psa 10"], &n);

    let srv = ShardServer::pending(Arc::clone(&n), EngineConfig::default());
    // This node hosts ONLY shard-id 2 (a non-zero position) — there is no slot 0.
    rt.block_on(srv.adopt_dict(adopt_req_shard(&d, 2)))
        .expect("adopt slot 2");
    rt.block_on(srv.insert_extracted(insert_req(2, 20, "psa 10")))
        .expect("write slot 2");

    let body = srv.metrics_source().render();
    assert!(
        body.contains("reverse_rusty_shard_ready{shard=\"2\"} 1"),
        "a node hosting a non-zero slot must report ready + real metrics for THAT slot (ADR-093 \
         Stage 3: series are per-shard labeled); got:\n{body}"
    );
}

/// ADR-093 Stage 3: a CO-LOCATED node (many slots) renders one `{shard="<id>"}` series per hosted
/// slot in ONE exposition — each family header written once. A Stage-1 slot-scoped `/_metrics` would
/// have reported only one of the node's shards.
#[test]
fn metrics_aggregate_over_colocated_slots() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["1994 upper deck", "psa 10"], &n);
    let (fp, tag_fp) = (d.fingerprint(), empty_tag_fp());

    // This node hosts slots {0, 5}: slot 0 ships the dict, slot 5 is co-located via AddShard.
    let srv = ShardServer::pending(Arc::clone(&n), EngineConfig::default());
    rt.block_on(srv.adopt_dict(adopt_req_shard(&d, 0)))
        .expect("adopt slot 0");
    rt.block_on(srv.add_shard(add_shard_req(5, fp, tag_fp)))
        .expect("add co-located slot 5");
    rt.block_on(srv.insert_extracted(insert_req(0, 10, "psa 10")))
        .expect("write slot 0");
    rt.block_on(srv.insert_extracted(insert_req(5, 15, "psa 10")))
        .expect("write slot 5");

    let body = srv.metrics_source().render();
    // Both co-located slots report ready + their own series (sorted: 0 then 5).
    assert!(
        body.contains("reverse_rusty_shard_ready{shard=\"0\"} 1"),
        "slot 0 missing; got:\n{body}"
    );
    assert!(
        body.contains("reverse_rusty_shard_ready{shard=\"5\"} 1"),
        "co-located slot 5 missing (a slot-scoped renderer would drop it); got:\n{body}"
    );
    assert_eq!(
        body.matches("reverse_rusty_shard_ready{shard=").count(),
        2,
        "exactly two labeled ready series (one per co-located slot); got:\n{body}"
    );
    // The family header is emitted exactly once across both slots (valid grouped exposition).
    assert_eq!(
        body.matches("# TYPE reverse_rusty_total_queries gauge")
            .count(),
        1,
        "each family header must appear once, not once per slot; got:\n{body}"
    );
}

// ---- orphan-slot GC (ADR-096): ListShards / DropShard ----

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

/// `ListShards` reports every hosted slot's GC-relevant state (fence generation, live count,
/// leases) plus the node's fingerprints — the sweep's classification input.
#[test]
fn list_shards_reports_slots_fence_and_counts() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["psa 10", "1994 upper deck"], &n);
    let fp = d.fingerprint();
    let tag_fp = {
        let mut td = TagDict::new();
        td.mark_finalized();
        td.fingerprint()
    };
    let srv = ShardServer::new(Arc::clone(&n), Arc::new(d), EngineConfig::default());
    rt.block_on(srv.insert_extracted(insert_req_single(10, "psa 10")))
        .expect("write slot 0");
    rt.block_on(srv.fence(Request::new(proto::FenceRequest {
        generation: 7,
        dict_fingerprint: fp,
        tag_dict_fingerprint: tag_fp,
        shard_id: 0,
        placement_generation: 1,
        num_shards: 1,
    })))
    .expect("fence slot 0");

    let reply = rt
        .block_on(srv.list_shards(Request::new(proto::Empty {})))
        .expect("list")
        .into_inner();
    assert_eq!(reply.dict_fingerprint, fp, "node dict fingerprint echoed");
    assert_eq!(
        reply.tag_dict_fingerprint, tag_fp,
        "node tag fingerprint echoed"
    );
    assert_eq!(reply.shards.len(), 1, "one hosted slot: {:?}", reply.shards);
    let s = &reply.shards[0];
    assert_eq!(s.shard_id, 0);
    assert_eq!(s.fenced_at_generation, 7, "the live fence generation");
    assert_eq!(s.num_queries, 1, "the live count");
    assert!(!s.retention_leases_held, "no lease outstanding");
}

/// The `DropShard` guard ladder: a zero arm is `InvalidArgument` (a cold drop is structurally
/// refused); an armed generation that does not match the live fence is `FailedPrecondition`
/// (covers BOTH the unfenced slot and a newer handoff's re-fence); divergent fingerprints are
/// refused before anything else. In every refused case the slot keeps serving.
#[test]
fn drop_shard_guards_refuse_unarmed_mismatched_and_divergent() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["psa 10"], &n);
    let fp = d.fingerprint();
    let tag_fp = {
        let mut td = TagDict::new();
        td.mark_finalized();
        td.fingerprint()
    };
    let srv = ShardServer::new(Arc::clone(&n), Arc::new(d), EngineConfig::default());

    // Zero arm: refused outright.
    let err = rt
        .block_on(srv.drop_shard(drop_req(0, 0, fp, tag_fp)))
        .expect_err("a zero arm is a cold drop");
    assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");

    // Armed-but-unfenced: the generations cannot match (slot is at 0), refused.
    let err = rt
        .block_on(srv.drop_shard(drop_req(0, 5, fp, tag_fp)))
        .expect_err("an unfenced slot never matches an arm");
    assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");

    // Fence at 3, arm with 4: a newer handoff owns the slot — refused.
    rt.block_on(srv.fence(Request::new(proto::FenceRequest {
        generation: 3,
        dict_fingerprint: fp,
        tag_dict_fingerprint: tag_fp,
        shard_id: 0,
        placement_generation: 1,
        num_shards: 1,
    })))
    .expect("fence");
    let err = rt
        .block_on(srv.drop_shard(drop_req(0, 4, fp, tag_fp)))
        .expect_err("a mismatched arm is refused");
    assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");

    // Divergent dict fingerprint: refused before any slot logic.
    let err = rt
        .block_on(srv.drop_shard(drop_req(0, 3, fp ^ 1, tag_fp)))
        .expect_err("a divergent space is refused");
    assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");

    // The slot survived every refusal and still serves.
    let count = rt
        .block_on(srv.num_queries(Request::new(proto::ShardRef { shard_id: 0 })))
        .expect("slot still hosted")
        .into_inner()
        .count;
    assert_eq!(count, 0);
}

/// A slot pinned by an UNEXPIRED retention lease (an in-flight recovery's source) is never
/// dropped; releasing the lease unblocks the drop.
#[test]
fn drop_shard_refuses_held_retention_lease_then_drops_after_release() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["psa 10"], &n);
    let fp = d.fingerprint();
    let tag_fp = {
        let mut td = TagDict::new();
        td.mark_finalized();
        td.fingerprint()
    };
    let srv = ShardServer::new(Arc::clone(&n), Arc::new(d), EngineConfig::default());
    rt.block_on(srv.fence(Request::new(proto::FenceRequest {
        generation: 2,
        dict_fingerprint: fp,
        tag_dict_fingerprint: tag_fp,
        shard_id: 0,
        placement_generation: 1,
        num_shards: 1,
    })))
    .expect("fence");
    // Acquire a lease through the real RPC (the default TTL is 1800s — never expires in-test).
    let lease = rt
        .block_on(
            srv.retention_lease(Request::new(proto::RetentionLeaseRequest {
                op: 0,
                lease_id: 0,
                pos: 0,
                dict_fingerprint: fp,
                tag_dict_fingerprint: tag_fp,
                shard_id: 0,
                placement_generation: 1,
                num_shards: 1,
            })),
        )
        .expect("acquire lease")
        .into_inner()
        .lease_id;

    let err = rt
        .block_on(srv.drop_shard(drop_req(0, 2, fp, tag_fp)))
        .expect_err("a leased source is never dropped");
    assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");

    rt.block_on(
        srv.retention_lease(Request::new(proto::RetentionLeaseRequest {
            op: 2,
            lease_id: lease,
            pos: 0,
            dict_fingerprint: fp,
            tag_dict_fingerprint: tag_fp,
            shard_id: 0,
            placement_generation: 1,
            num_shards: 1,
        })),
    )
    .expect("release lease");
    let reply = rt
        .block_on(srv.drop_shard(drop_req(0, 2, fp, tag_fp)))
        .expect("drop after release")
        .into_inner();
    assert!(reply.dropped, "the released slot drops");
}

/// The durable drop end-to-end: the slot leaves the map, its `shard_<id>/` dir is reclaimed
/// (trash-renamed then deleted — no live-named or trash dir remains), and a re-run is the
/// idempotent `dropped = false`.
#[test]
fn drop_shard_removes_slot_and_dir_and_is_idempotent() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["psa 10"], &n);
    let fp = d.fingerprint();
    let tag_fp = empty_tag_fp();
    let dir = std::env::temp_dir().join(format!("rr_gc_drop_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let srv = ShardServer::new_durable(
        Arc::clone(&n),
        Arc::new(d),
        EngineConfig::default(),
        dir.clone(),
    )
    .expect("durable server");
    rt.block_on(srv.insert_extracted(insert_req_single(10, "psa 10")))
        .expect("write slot 0");
    rt.block_on(srv.flush(Request::new(proto::FlushRequest {
        shard_id: 0,
        placement_generation: 1,
        num_shards: 1,
    })))
    .expect("flush to disk");
    assert!(
        dir.join("shard_000").exists(),
        "the slot dir exists on disk"
    );
    // `new_durable` starts with the FINALIZED empty tag space, whose fingerprint differs from the
    // never-finalized `TagDict::new()` — present the finalized one on the fence.
    let fenced_tag_fp = {
        let mut td = TagDict::new();
        td.mark_finalized();
        td.fingerprint()
    };
    let _ = tag_fp; // documents the distinction above
    rt.block_on(srv.fence(Request::new(proto::FenceRequest {
        generation: 9,
        dict_fingerprint: fp,
        tag_dict_fingerprint: fenced_tag_fp,
        shard_id: 0,
        placement_generation: 1,
        num_shards: 1,
    })))
    .expect("fence");

    let reply = rt
        .block_on(srv.drop_shard(drop_req(0, 9, fp, fenced_tag_fp)))
        .expect("drop")
        .into_inner();
    assert!(reply.dropped, "the armed slot drops");
    assert_eq!(reply.num_queries, 1, "the dropped slot's live count");
    assert!(reply.dir_removed, "the dir is fully reclaimed");
    assert!(!dir.join("shard_000").exists(), "no live-named dir remains");
    let leftovers: Vec<_> = std::fs::read_dir(&dir)
        .expect("scan")
        .filter_map(Result::ok)
        .filter(|e| is_dropped_trash(&e.file_name()))
        .collect();
    assert!(leftovers.is_empty(), "no trash dir remains: {leftovers:?}");
    let err = rt
        .block_on(srv.num_queries(Request::new(proto::ShardRef { shard_id: 0 })))
        .expect_err("the slot left the map");
    assert_eq!(err.code(), Code::NotFound, "{err:?}");

    // Idempotent re-run: absent slot => dropped=false, never an error.
    let reply = rt
        .block_on(srv.drop_shard(drop_req(0, 9, fp, fenced_tag_fp)))
        .expect("re-drop")
        .into_inner();
    assert!(!reply.dropped, "an absent slot is the idempotent no-op");

    let _ = std::fs::remove_dir_all(&dir);
}

/// A trash-renamed slot dir (an interrupted delete) is invisible to a durable restart — never
/// re-attached as a slot — and the boot sweep reclaims it.
#[test]
fn open_durable_sweeps_dropped_trash_and_ignores_it() {
    let n = norm();
    let dir = std::env::temp_dir().join(format!("rr_gc_trash_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let trash = dir.join("shard_000.dropped.12345");
    std::fs::create_dir_all(&trash).expect("plant trash");
    std::fs::write(trash.join("junk.seg"), b"leftover").expect("plant junk");

    let srv =
        super::ShardServer::open_durable(Arc::clone(&n), EngineConfig::default(), dir.clone())
            .expect("open_durable never fails over trash");
    assert!(!srv.is_serving(), "no slot was re-attached from trash");
    assert!(!trash.exists(), "the boot sweep reclaimed the trash dir");

    let _ = std::fs::remove_dir_all(&dir);
}

/// The DropShard tombstone is irrevocable (ADR-096, codex P2): `unfence` refuses to clear a
/// fence at the tombstone value, so a concurrent stale-fence probe (`fence(0)` → observe →
/// `unfence(observed)`) can never resurrect writability on a slot mid-drop.
#[test]
fn unfence_refuses_to_clear_the_drop_tombstone() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["psa 10"], &n);
    let fp = d.fingerprint();
    let tag_fp = {
        let mut td = TagDict::new();
        td.mark_finalized();
        td.fingerprint()
    };
    let srv = ShardServer::new(Arc::clone(&n), Arc::new(d), EngineConfig::default());
    // Drive the fence to the tombstone value through the public monotonic fetch_max (the drop
    // path swaps it in atomically; the wire value is equivalent for the guard under test).
    rt.block_on(srv.fence(Request::new(proto::FenceRequest {
        generation: u64::MAX,
        dict_fingerprint: fp,
        tag_dict_fingerprint: tag_fp,
        shard_id: 0,
        placement_generation: 1,
        num_shards: 1,
    })))
    .expect("fence to the tombstone value");

    // The stale-fence-probe shape: unfence(exactly what a probe would observe) — REFUSED.
    let after = rt
        .block_on(srv.unfence(Request::new(proto::UnfenceRequest {
            generation: u64::MAX,
            dict_fingerprint: fp,
            tag_dict_fingerprint: tag_fp,
            shard_id: 0,
            placement_generation: 1,
            num_shards: 1,
        })))
        .expect("unfence call succeeds (as a no-op)")
        .into_inner()
        .fenced_at_generation;
    assert_eq!(
        after,
        u64::MAX,
        "the tombstone survives an exact-value unfence — a mid-drop slot can never be re-armed"
    );
}
