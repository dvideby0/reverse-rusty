//! `ShardServer` unit tests (the dict-adopt state machine + the per-shard write fence).

use std::sync::Arc;

use tonic::{Code, Request};

use super::ShardServer;
use crate::cluster::proto;
use crate::cluster::proto::shard_service_server::ShardService;
use crate::compile::extract;
use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::normalize::Normalizer;
use crate::storage::serialize_dict;
use crate::tagdict::TagDict;

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
        }),
        shard_id,
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
    rt.block_on(srv.insert_extracted(insert_req(0, 2, "psa 10")))
        .expect("insert before fence");

    // Fence at generation 5.
    let fenced = rt
        .block_on(srv.fence(Request::new(proto::FenceRequest {
            generation: 5,
            dict_fingerprint: fp,
            tag_dict_fingerprint: tag_fp,
            shard_id: 0,
        })))
        .expect("fence")
        .into_inner()
        .fenced_at_generation;
    assert_eq!(fenced, 5);

    // After the fence: every data-mutating write is rejected.
    assert_eq!(
        rt.block_on(srv.insert_extracted(insert_req(0, 3, "psa 10")))
            .expect_err("insert after fence")
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        rt.block_on(srv.delete(Request::new(proto::DeleteRequest {
            logical_id: 1,
            shard_id: 0,
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
    rt.block_on(srv.percolate(Request::new(proto::PercolateRequest {
        title: "1994 upper deck".to_string(),
        include_broad: false,
        filter: Vec::new(),
        rank: None,
        shard_id: 0,
    })))
    .expect("percolate after fence");

    // Monotonic: a stale, lower-generation fence never lowers the fence.
    let after_stale = rt
        .block_on(srv.fence(Request::new(proto::FenceRequest {
            generation: 3,
            dict_fingerprint: fp,
            tag_dict_fingerprint: tag_fp,
            shard_id: 0,
        })))
        .expect("stale fence")
        .into_inner()
        .fenced_at_generation;
    assert_eq!(after_stale, 5, "a lower-gen fence must not lower the fence");
    assert_eq!(
        rt.block_on(srv.insert_extracted(insert_req(0, 4, "psa 10")))
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
        })))
        .expect("unfence slot 0")
        .into_inner()
        .fenced_at_generation;
    assert_eq!(now, 0, "slot 0 is un-fenced");
    rt.block_on(srv.insert_extracted(insert_req(0, 14, "psa 10")))
        .expect("slot 0 writable after unfence");
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
        body.contains("reverse_rusty_shard_ready 1"),
        "a node hosting a non-zero slot must report ready + real metrics; got:\n{body}"
    );
}
