use super::*;

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
