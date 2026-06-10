//! Retention leases (ADR-040/048): a recovery pins the translog tail across a concurrent
//! seal so the no-quiesce copy window stays consistent, and a TTL reaps a crashed
//! recovery's stuck lease so the source reclaims the abandoned tail (surfacing the reap as
//! an event).

use std::time::{Duration, Instant};

use crate::cluster::clog::ClusterMutation;

use super::super::test_support::*;
use super::super::*;

#[test]
fn seal_honors_retention_lease_so_concurrent_seal_keeps_the_recovery_tail() {
    // ADR-040: a peer recovery acquires a retention lease at position `at`, then more writes
    // land and the source SEALS AGAIN (a concurrent checkpoint, or another recovery's
    // FetchSegments). Without the lease that seal would trim the translog to its new `P`,
    // erasing the tail (> at) the in-flight recovery still needs — a silent false negative.
    // With the lease the seal trims only to `at`, so the tail survives; releasing it lets the
    // source GC again. (This is the latent FN ADR-039's no-quiesce path left open.)
    let (norm, dict, tag_dict, corpus) = compile_corpus(&[
        (1, "alpha bravo"),
        (2, "charlie delta"),
        (3, "echo foxtrot"),
    ]);
    let dir = scratch_dir("retain");
    let cfg = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };
    let primary = LocalShard::new_durable(
        Arc::clone(&norm),
        Arc::clone(&dict),
        Arc::clone(&tag_dict),
        cfg,
    )
    .expect("durable primary");
    // Seed id 1 and seal a base — the recovery baseline.
    primary
        .insert_extracted_with_tags(&corpus[0].1, 1, 1, &corpus[0].2, &[])
        .expect("ins 1");
    let at_seal = primary.seal_for_checkpoint().expect("seal 1");

    // The recovery pins the tail at the current high-water.
    let (lease, at) = primary.acquire_retention_lease().expect("lease");
    assert_eq!(at, at_seal, "lease pins the post-seal high-water");

    // Writes land AFTER the snapshot (into the translog, > at).
    primary
        .insert_extracted_with_tags(&corpus[1].1, 2, 1, &corpus[1].2, &[])
        .expect("ins 2");
    primary
        .insert_extracted_with_tags(&corpus[2].1, 3, 1, &corpus[2].2, &[])
        .expect("ins 3");

    // A concurrent seal: WITHOUT the lease it would trim to its new P and drop (at, P]; the
    // lease holds the floor at `at`, so the tail the recovery needs survives.
    let p1 = primary.seal_for_checkpoint().expect("seal 2");
    assert!(
        p1 > at,
        "the second seal advanced the checkpoint past the pinned point"
    );
    let tail = primary.translog_tail(at).expect("tail");
    let ids: Vec<u64> = tail
        .iter()
        .map(|(_, m)| match m {
            ClusterMutation::Add { logical, .. }
            | ClusterMutation::Remove { logical }
            | ClusterMutation::Upsert { logical, .. } => *logical,
        })
        .collect();
    assert_eq!(
        ids,
        vec![2, 3],
        "the lease kept the post-snapshot tail (> at)"
    );

    // Release: the next seal trims freely again (GC), so the pinned tail is now gone.
    primary.release_retention_lease(lease).expect("release");
    primary.seal_for_checkpoint().expect("seal 3");
    assert!(
        primary
            .translog_tail(at)
            .expect("tail after release")
            .is_empty(),
        "a released lease lets the source GC the consumed tail"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ttl_reaps_a_stuck_lease_so_the_seal_reclaims_the_tail_and_emits() {
    // ADR-048: ADR-040's lease keeps a recovery's tail across a concurrent seal, but a CRASHED
    // recovering node would otherwise pin that tail forever. A TTL reaps a lease that has not
    // heartbeated within the window, so the source reclaims the abandoned tail — and surfaces the
    // reap as an event (the recovery was abandoned, not silently dropped). `renew` is the
    // heartbeat, so a live recovery is never reaped (the unit tests cover the renew case).
    let (norm, dict, tag_dict, corpus) = compile_corpus(&[
        (1, "alpha bravo"),
        (2, "charlie delta"),
        (3, "echo foxtrot"),
    ]);
    let dir = scratch_dir("lease_ttl");
    // A small, non-whole-minute TTL: matches the cfg below so the injected `now` (ttl + 1s past
    // acquire) trips the reap, and dodges the `duration_suboptimal_units` lint.
    let ttl = Duration::from_secs(100);
    let cfg = EngineConfig {
        data_dir: Some(dir.clone()),
        retention_lease_ttl_secs: 100,
        ..EngineConfig::default()
    };
    let primary = LocalShard::new_durable(
        Arc::clone(&norm),
        Arc::clone(&dict),
        Arc::clone(&tag_dict),
        cfg,
    )
    .expect("durable primary");

    // Capture the reap event (ADR-021/048): a plain LocalShard now honors the coordinator's sink.
    let saw_reap = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&saw_reap);
    primary.set_event_sink(Arc::new(move |ev: &EngineEvent| {
        if matches!(
            ev,
            EngineEvent::DurabilityFailure {
                op: DurabilityOp::ReplicaDesync,
                ..
            }
        ) {
            flag.store(true, Ordering::Release);
        }
    }));

    primary
        .insert_extracted_with_tags(&corpus[0].1, 1, 1, &corpus[0].2, &[])
        .expect("ins 1");
    let at_seal = primary.seal_for_checkpoint().expect("seal 1");

    // A recovery pins the tail; writes land after it (> at).
    let (_lease, at) = primary.acquire_retention_lease().expect("lease");
    assert_eq!(at, at_seal, "lease pins the post-seal high-water");
    primary
        .insert_extracted_with_tags(&corpus[1].1, 2, 1, &corpus[1].2, &[])
        .expect("ins 2");
    primary
        .insert_extracted_with_tags(&corpus[2].1, 3, 1, &corpus[2].2, &[])
        .expect("ins 3");

    // Control: a seal while the lease is LIVE (now within the window) keeps the tail (ADR-040)
    // and reaps nothing.
    let p1 = primary
        .seal_for_checkpoint_at(Instant::now())
        .expect("live seal");
    assert!(
        p1 > at,
        "the seal advanced the checkpoint past the pinned point"
    );
    assert!(
        !primary.translog_tail(at).expect("tail").is_empty(),
        "a live lease still holds the tail across a seal"
    );
    assert!(
        !saw_reap.load(Ordering::Acquire),
        "no reap while the lease is within the TTL window"
    );

    // The recovery crashes: it stops heartbeating. Simulate the TTL elapsing by sealing as of a
    // synthetic `now` past the window (deterministic — no sleep). The stale lease is reaped, the
    // tail is reclaimed, and the reap is surfaced as an event.
    primary
        .seal_for_checkpoint_at(Instant::now() + ttl + Duration::from_secs(1))
        .expect("expiring seal");
    assert!(
        primary
            .translog_tail(at)
            .expect("tail after reap")
            .is_empty(),
        "the reaped lease no longer pins the tail; the seal reclaimed it"
    );
    assert!(
        saw_reap.load(Ordering::Acquire),
        "a stuck-lease reap must surface a ReplicaDesync event"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
