use super::*;

// ---- orphan-slot GC (ADR-096): ListShards / DropShard ----

/// `ListShards` reports every hosted slot's GC-relevant state (fence generation, live count,
/// leases) plus the node's fingerprints — the sweep's classification input.
#[test]
fn list_shards_reports_slots_fence_and_counts() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let n = norm();
    let d = frozen_dict(&["pro", "1994 north star"], &n);
    let fp = d.fingerprint();
    let tag_fp = {
        let mut td = TagDict::new();
        td.mark_finalized();
        td.fingerprint()
    };
    let srv = ShardServer::new(Arc::clone(&n), Arc::new(d), EngineConfig::default());
    rt.block_on(srv.insert_extracted(insert_req_single(10, "pro")))
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
    let d = frozen_dict(&["pro"], &n);
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
    let d = frozen_dict(&["pro"], &n);
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
    let d = frozen_dict(&["pro"], &n);
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
    rt.block_on(srv.insert_extracted(insert_req_single(10, "pro")))
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

    let srv = super::super::ShardServer::open_durable(
        Arc::clone(&n),
        EngineConfig::default(),
        dir.clone(),
    )
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
    let d = frozen_dict(&["pro"], &n);
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
