//! `ShardServer` unit tests (the dict-adopt state machine + the write fence).

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

fn adopt_req(dict: &Dict) -> Request<proto::AdoptDictRequest> {
    // Untagged: an empty tag-dict blob deserializes to an empty `TagDict`, whose fingerprint the
    // request must claim (the server's tag-integrity check mirrors the dict one).
    Request::new(proto::AdoptDictRequest {
        dict: serialize_dict(dict),
        fingerprint: dict.fingerprint(),
        tag_dict: Vec::new(),
        tag_dict_fingerprint: TagDict::new().fingerprint(),
    })
}

fn current_fp(srv: &ShardServer) -> u64 {
    srv.state.load_full().expect("adopted").dict.fingerprint()
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
    // Pending: reads fail loud rather than fabricating an empty result.
    assert!(srv.state.load_full().is_none());
    let err = rt
        .block_on(srv.num_queries(Request::new(proto::Empty {})))
        .expect_err("pending read must fail");
    assert_eq!(err.code(), Code::FailedPrecondition);

    // Empty → adopt d1.
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
        tag_dict_fingerprint: TagDict::new().fingerprint(),
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
        .block_on(srv.num_queries(Request::new(proto::Empty {})))
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

/// The live-handoff write fence (ADR-044): once `Fence` lands, data-mutating writes
/// (`insert`/`delete`/`ingest`) are rejected with `FailedPrecondition`, while reads stay served
/// (serve-then-drop); the fence is monotonic and dict-fingerprint-guarded.
#[test]
fn fence_rejects_writes_but_serves_reads() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["1994 upper deck", "psa 10"], &n);
    let fp = d.fingerprint();
    let srv = ShardServer::new(Arc::clone(&n), Arc::new(d), EngineConfig::default());
    srv.ingest_dsl(&[(1u64, "1994 upper deck".to_string())]);

    let insert = |id: u64, dsl: &str| {
        Request::new(proto::InsertRequest {
            item: Some(proto::AddItem {
                logical_id: id,
                dsl: dsl.to_string(),
                version: 1,
                tags: Vec::new(),
            }),
        })
    };

    // Before the fence: a write succeeds.
    rt.block_on(srv.insert_extracted(insert(2, "psa 10")))
        .expect("insert before fence");

    // Fence at generation 5.
    let fenced = rt
        .block_on(srv.fence(Request::new(proto::FenceRequest {
            generation: 5,
            dict_fingerprint: fp,
        })))
        .expect("fence")
        .into_inner()
        .fenced_at_generation;
    assert_eq!(fenced, 5);

    // After the fence: every data-mutating write is rejected.
    assert_eq!(
        rt.block_on(srv.insert_extracted(insert(3, "psa 10")))
            .expect_err("insert after fence")
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        rt.block_on(srv.delete(Request::new(proto::DeleteRequest { logical_id: 1 })))
            .expect_err("delete after fence")
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        rt.block_on(srv.ingest_extracted(Request::new(proto::IngestRequest { items: vec![] })))
            .expect_err("ingest after fence")
            .code(),
        Code::FailedPrecondition
    );

    // ...but reads still serve (serve-then-drop): num_queries + percolate keep working.
    let cnt = rt
        .block_on(srv.num_queries(Request::new(proto::Empty {})))
        .expect("read after fence")
        .into_inner()
        .count;
    assert!(cnt >= 1, "reads stay served while fenced: {cnt}");
    rt.block_on(srv.percolate(Request::new(proto::PercolateRequest {
        title: "1994 upper deck".to_string(),
        include_broad: false,
        filter: Vec::new(),
    })))
    .expect("percolate after fence");

    // Monotonic: a stale, lower-generation fence never lowers the fence.
    let after_stale = rt
        .block_on(srv.fence(Request::new(proto::FenceRequest {
            generation: 3,
            dict_fingerprint: fp,
        })))
        .expect("stale fence")
        .into_inner()
        .fenced_at_generation;
    assert_eq!(after_stale, 5, "a lower-gen fence must not lower the fence");
    assert_eq!(
        rt.block_on(srv.insert_extracted(insert(4, "psa 10")))
            .expect_err("still fenced after a stale fence")
            .code(),
        Code::FailedPrecondition
    );

    // A dict-fingerprint mismatch is refused (never fences across a divergent feature space).
    assert_eq!(
        rt.block_on(srv.fence(Request::new(proto::FenceRequest {
            generation: 9,
            dict_fingerprint: fp ^ 0xDEAD_BEEF,
        })))
        .expect_err("fence fp mismatch")
        .code(),
        Code::FailedPrecondition
    );
}
