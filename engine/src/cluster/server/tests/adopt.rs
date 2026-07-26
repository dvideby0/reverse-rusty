use super::*;

/// Compiler lowering affects both candidate cover and placement. A coordinator
/// on the immediately previous semantics must be refused before its first
/// AdoptDict can create a shard slot; otherwise a syntactically additive
/// protobuf exchange could silently mix incompatible compiler semantics.
#[test]
fn adopt_dict_refuses_previous_compiler_semantics_before_mutation() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["new -used york"], &n);
    let srv = ShardServer::pending(Arc::clone(&n), EngineConfig::default());
    let mut request = adopt_req(&d);
    request.get_mut().compiler_semantics_version =
        crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION - 1;

    let error = rt
        .block_on(srv.adopt_dict(request))
        .expect_err("previous compiler semantics must fail closed");
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(
        srv.slot(0).is_err(),
        "a refused handshake must not create or mutate a shard slot"
    );
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
        compiler_semantics_version: crate::storage::CURRENT_COMPILER_SEMANTICS_VERSION,
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

#[tokio::test]
async fn fingerprint_claim_attests_space_adopted_during_legacy_drain() {
    let n = norm();
    let d1 = frozen_dict(&["1994 upper deck"], &n);
    let d2 = frozen_dict(&["1995 fleer ultra"], &n);
    assert_ne!(
        d1.fingerprint(),
        d2.fingerprint(),
        "test setup requires divergent feature spaces"
    );

    let srv = Arc::new(ShardServer::new(
        Arc::clone(&n),
        Arc::new(d1),
        EngineConfig::default(),
    ));
    // Model an unstamped AdoptDict that the outer lease service admitted before
    // the claim arrived. The handler may replace an empty node's feature space,
    // and the claim must wait for its complete response body to drain.
    let legacy_adopt = srv.coordinator_lease.hold_unstamped_for_test();
    let mut request = Request::new(proto::Empty {});
    request.metadata_mut().insert(
        "x-reverse-rusty-coordinator-id",
        "41".parse().expect("metadata"),
    );
    request.metadata_mut().insert(
        "x-reverse-rusty-coordinator-claim",
        "1".parse().expect("metadata"),
    );

    let claiming_server = Arc::clone(&srv);
    let claiming = tokio::spawn(async move { claiming_server.dict_fingerprint(request).await });
    while !srv.coordinator_lease.is_claiming() {
        tokio::task::yield_now().await;
    }

    srv.adopt_dict(adopt_req(&d2))
        .await
        .expect("legacy adopt completes before its response drains");
    drop(legacy_adopt);

    let reply = claiming
        .await
        .expect("claim task")
        .expect("claim succeeds after legacy drain")
        .into_inner();
    assert_eq!(
        reply.fingerprint,
        d2.fingerprint(),
        "claim handshake must attest the post-drain adopted space"
    );
    assert_eq!(reply.coordinator_id, 41);
}
