use super::*;

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
