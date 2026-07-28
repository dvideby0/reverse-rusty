use super::*;

#[test]
fn grpc_result_cap_can_only_be_lowered_within_static_bounds() {
    let n = norm();
    let server = ShardServer::pending(Arc::clone(&n), EngineConfig::default());
    assert!(server.with_max_grpc_result_bytes(0).is_err());

    let server = ShardServer::pending(Arc::clone(&n), EngineConfig::default());
    assert!(server
        .with_max_grpc_result_bytes(super::super::MAX_GRPC_RESULT_BYTES + 1)
        .is_err());

    let server = ShardServer::pending(n, EngineConfig::default());
    assert!(server.with_max_grpc_result_bytes(1).is_ok());
}

#[test]
fn exhaustive_stream_workers_are_admitted_before_spawn() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let dict = Arc::new(frozen_dict(&["deliveryneedle"], &n));
    let server = ShardServer::new(Arc::clone(&n), dict, EngineConfig::default())
        .with_max_concurrent_exhaustive_streams(1)
        .expect("one exhaustive worker");
    let items: Vec<_> = (0..32)
        .map(|id| (id, "deliveryneedle".to_string()))
        .collect();
    server.ingest_dsl(&items);
    let ownership = crate::ownership::OwnershipContext::new(
        crate::ownership::PlacementGeneration::INITIAL,
        1,
        vec![0],
        None,
    )
    .expect("ownership context");
    let request = || {
        Request::new(proto::PercolateAllRequest {
            title: "deliveryneedle".into(),
            include_broad: false,
            filter: Vec::new(),
            rank: None,
            chunk_size: 1,
            remaining_micros: 5_000_000,
            shard_id: 0,
            ownership: Some(proto::ownership_to_proto(&ownership)),
        })
    };

    // Retain but do not drain the first stream. Its bounded channel fills, so
    // the worker and its admission permit remain live.
    let first = rt
        .block_on(server.percolate_all(request()))
        .expect("first stream admitted");
    let Err(error) = rt.block_on(server.percolate_all(request())) else {
        panic!("second stream bypassed node-local admission");
    };
    assert_eq!(error.code(), Code::ResourceExhausted);
    drop(first);
    rt.block_on(async {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    });
}

#[test]
fn exhaustive_stream_configuration_rejects_invalid_bounds_without_panicking() {
    let server = ShardServer::pending(norm(), EngineConfig::default());
    assert!(server.with_max_concurrent_exhaustive_streams(0).is_err());

    let server = ShardServer::pending(norm(), EngineConfig::default());
    assert!(server
        .with_max_concurrent_exhaustive_streams(tokio::sync::Semaphore::MAX_PERMITS + 1)
        .is_err());

    let server = ShardServer::pending(norm(), EngineConfig::default());
    assert!(server
        .with_max_exhaustive_stream_duration(std::time::Duration::ZERO)
        .is_err());
}

#[test]
fn exhaustive_stream_rejects_a_caller_deadline_above_the_node_ceiling() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let dict = Arc::new(frozen_dict(&["deliveryneedle"], &n));
    let server = ShardServer::new(n, dict, EngineConfig::default())
        .with_max_concurrent_exhaustive_streams(1)
        .expect("one exhaustive worker")
        .with_max_exhaustive_stream_duration(std::time::Duration::from_secs(1))
        .expect("one-second node ceiling");
    let ownership = crate::ownership::OwnershipContext::new(
        crate::ownership::PlacementGeneration::INITIAL,
        1,
        vec![0],
        None,
    )
    .expect("ownership context");
    let request = Request::new(proto::PercolateAllRequest {
        title: "deliveryneedle".into(),
        include_broad: false,
        filter: Vec::new(),
        rank: None,
        chunk_size: 1,
        remaining_micros: 2_000_000,
        shard_id: 0,
        ownership: Some(proto::ownership_to_proto(&ownership)),
    });

    let Err(error) = rt.block_on(server.percolate_all(request)) else {
        panic!("caller-controlled overlong deadline must be refused");
    };
    assert_eq!(error.code(), Code::InvalidArgument);
    assert_eq!(
        server.exhaustive_permits.available_permits(),
        1,
        "deadline validation must happen before node admission"
    );
}

/// `ingest_dsl` preloads must be emitted under ownership-suppressed cluster reads:
/// stamping them `QueryPlacement::standalone()` made `owner()` return `None` for
/// every preloaded row, so the whole preload silently vanished from percolation
/// (OK status, zero ids — a zero-FN violation; review finding). The preload now
/// stamps the node space's real slot-0 selective placement.
#[test]
fn ingest_dsl_preload_is_emitted_under_ownership() {
    use crate::cluster::shard::Shard;

    let norm = norm();
    let dict = Arc::new(frozen_dict(&["1994 acme"], &norm));
    let server = ShardServer::new(Arc::clone(&norm), dict, EngineConfig::default());
    server.ingest_dsl(&[(7u64, "1994 acme".to_string())]);

    let (_, st) = server.loaded_slot(0).expect("slot 0 loaded");
    let context = crate::ownership::OwnershipContext::new(
        crate::ownership::PlacementGeneration::INITIAL,
        1,
        vec![0],
        None,
    )
    .expect("context");
    let (ids, _) = st
        .shard
        .percolate_filtered_owned(
            "1994 acme appliance",
            true,
            &crate::exact::TagPredicate::empty(),
            &context,
            0,
        )
        .expect("owned percolate");
    assert_eq!(
        ids,
        vec![7],
        "preloaded row must be emitted, not suppressed"
    );
}
