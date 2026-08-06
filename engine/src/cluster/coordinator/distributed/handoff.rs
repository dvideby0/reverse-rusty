use std::time::Instant;

use crate::cluster::remote::RemoteShard;

use super::{Arc, ClusterEngine, DurabilityOp, EngineEvent, LogPos, Shard, ShardError};

/// Clear a stale fence before a recovered target becomes live again.
///
/// Serve-then-drop deliberately leaves a demoted primary fenced, and `RecoverFrom` preserves that
/// slot fence. Reusing such a node as a later handoff target without clearing it would install a
/// write-rejecting live primary. `fence(0)` is a pure probe because the server fence is monotonic;
/// `unfence(observed)` then CAS-clears exactly the observed generation. The move ledger and the
/// documented single-active-coordinator topology exclude a competing local orchestrator.
pub(in crate::cluster::coordinator) fn clear_stale_fence(
    target: &RemoteShard,
    context: &str,
) -> Result<(), ShardError> {
    let stale = target.fence(0)?;
    if stale == 0 {
        return Ok(());
    }
    let current = target.unfence(stale)?;
    if current != 0 {
        return Err(ShardError::Remote(format!(
            "{context}: clearing stale target fence generation {stale} failed; the slot remains \
             fenced at generation {current} (a concurrent handoff?)"
        )));
    }
    Ok(())
}

/// Terminal result of one raw live-routing handoff request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HandoffOutcome {
    /// The requested target already owns live routing. This makes an exact
    /// retry after a lost HTTP response safe and observable.
    NoChange { generation: u64 },
    /// Recovery, fencing, convergence, and the routing flip completed.
    Moved { generation: u64 },
}

impl HandoffOutcome {
    /// Fence/routing generation serving after the request.
    pub fn generation(self) -> u64 {
        match self {
            Self::NoChange { generation } | Self::Moved { generation } => generation,
        }
    }

    /// Whether this invocation performed the routing flip.
    pub fn moved(self) -> bool {
        matches!(self, Self::Moved { .. })
    }
}

impl ClusterEngine {
    /// Live data-moving handoff (ADR-044, clustering step 6b): move shard `position` from its
    /// current owner (`source_endpoint`) to a new owner (`target_endpoint`) WITHOUT dropping a match
    /// and WITHOUT pausing reads. The byte mover is peer recovery (ADR-036/039); this adds the
    /// **serve-then-drop routing flip** (the 6a [`HandoffShard`]) + a **write fence** on the old
    /// owner. Under one retention lease held on the source for the whole move, the flow is:
    /// peer-recover the target from the source (bulk segments at `P` + drain the translog tail — NO
    /// quiesce: the source keeps serving reads + accepting writes throughout); **fence** the source
    /// (its data-mutating writes now return an error, so a brief write-quiesce for `position` begins,
    /// while reads + the recovery RPCs stay served); **drain to convergence** (the fenced source's
    /// tail is finite + frozen, so looping the catch-up until the high-water stops advancing captures
    /// every op it ever accepted — closing the TOCTOU a single final catch-up would leave); then
    /// **flip** — atomically re-point `position`'s backing at the target (the old source backing is
    /// dropped from routing: in-flight reads complete against it, new reads + writes go to the target,
    /// ending the quiesce). Returns the new fence/handoff generation. Requires a handoff-capable
    /// cluster (built via [`Self::connect_remote`]/[`Self::connect_replicated`]); errors fail-closed —
    /// a write briefly rejected in the fence→flip window is the caller's to retry (it never silently
    /// vanishes), and a source that fails to converge aborts the flip and is auto-unfenced rather
    /// than dropping a write. "Drop the old owner" = drop it from ROUTING, not teardown (its server
    /// keeps running; tearing it down is a separate ops step).
    ///
    /// Reserves `{source, target}` in the busy-endpoint move ledger for the whole move (ADR-095), so
    /// a raw handoff — the REST `POST /_cluster/handoff` path — serializes against every concurrent
    /// data-moving reassign touching either node. (Before the ledger, a raw handoff took NO guard at
    /// all and could race a `reassign_and_move` of the same position — a latent hole ADR-095
    /// closes.) [`reassign_and_move`](Self::reassign_and_move) calls the unguarded `_inner` variant
    /// instead: its own ticket already covers both endpoints, and re-reserving here would
    /// self-deadlock.
    pub fn execute_handoff(
        &self,
        position: usize,
        source_endpoint: &str,
        target_endpoint: &str,
        handle: &tokio::runtime::Handle,
    ) -> Result<u64, ShardError> {
        let _ticket = self
            .move_ledger
            .reserve(&[source_endpoint, target_endpoint]);
        match self.validate_handoff_route(position, source_endpoint, target_endpoint)? {
            HandoffRoute::AlreadyAtTarget { generation } => Ok(generation),
            HandoffRoute::Move => {
                self.execute_handoff_inner(position, source_endpoint, target_endpoint, handle)
            }
        }
    }

    /// Deadline-aware raw handoff admission for an operator boundary.
    ///
    /// The endpoint footprint is reserved before `try_start` is invoked. The
    /// callback must atomically decide whether the request may begin (for
    /// example, by winning a queued-vs-cancelled gate) and may report a start
    /// signal. `None` means no movement began: either the ledger deadline
    /// elapsed or the callback refused the start. Once the callback returns
    /// true, the move is safety-sensitive and runs to a terminal result.
    pub fn execute_handoff_until<F>(
        &self,
        position: usize,
        source_endpoint: &str,
        target_endpoint: &str,
        handle: &tokio::runtime::Handle,
        deadline: Instant,
        try_start: F,
    ) -> Result<Option<HandoffOutcome>, ShardError>
    where
        F: FnOnce() -> bool,
    {
        let Some(_ticket) = self
            .move_ledger
            .reserve_until(&[source_endpoint, target_endpoint], deadline)
        else {
            return Ok(None);
        };
        let route = self.validate_handoff_route(position, source_endpoint, target_endpoint)?;
        if !try_start() {
            return Ok(None);
        }
        match route {
            HandoffRoute::AlreadyAtTarget { generation } => {
                Ok(Some(HandoffOutcome::NoChange { generation }))
            }
            HandoffRoute::Move => self
                .execute_handoff_inner(position, source_endpoint, target_endpoint, handle)
                .map(|generation| Some(HandoffOutcome::Moved { generation })),
        }
    }

    /// Prove that recovery will seed from the position's current primary, not
    /// an arbitrary same-dictionary slot. This check runs while the endpoint
    /// move-ledger reservation is held, so the live backing cannot change
    /// between attestation and the first recovery RPC.
    pub(in crate::cluster::coordinator) fn validate_handoff_route(
        &self,
        position: usize,
        source_endpoint: &str,
        target_endpoint: &str,
    ) -> Result<HandoffRoute, ShardError> {
        let handoff = self.handoffs.get(position).ok_or_else(|| {
            ShardError::Config(format!(
                "execute_handoff: shard position {position} is not handoff-capable (the cluster \
                 was not built via connect_remote/connect_replicated)"
            ))
        })?;
        let Some(primary) = handoff.live_primary_endpoint() else {
            return Err(ShardError::Config(format!(
                "execute_handoff: shard position {position} has no live remote primary"
            )));
        };
        let primary_identity = normalized_endpoint(&primary);
        let replicas: Vec<String> = handoff
            .live_endpoints()
            .into_iter()
            .filter(|endpoint| normalized_endpoint(endpoint) != primary_identity)
            .collect();
        classify_handoff_route(
            position,
            source_endpoint,
            target_endpoint,
            &primary,
            &replicas,
            handoff.generation(),
        )
    }

    /// [`execute_handoff`](Self::execute_handoff) minus the ledger reservation — for callers already
    /// holding a [`MoveTicket`](super::reassign::MoveLedger) covering `{source, target}` (the
    /// data-moving reassign path). Never call this without such a ticket: two unguarded handoffs
    /// sharing a node would interleave their fence windows.
    pub(in crate::cluster::coordinator) fn execute_handoff_inner(
        &self,
        position: usize,
        source_endpoint: &str,
        target_endpoint: &str,
        handle: &tokio::runtime::Handle,
    ) -> Result<u64, ShardError> {
        // Drain caps (ADR-044/048), tunable via `ClusterConfig` and retained on the engine.
        // `drain_passes` bounds the pre-fence drain (best-effort, while writes still flow);
        // correctness rests on the post-fence drain CONVERGING, not on this. `final_drain_cap`
        // bounds the post-fence drain — the fenced source has a finite, frozen tail, so it
        // converges in O(in-flight writes) passes and the cap only bounds a misbehaving source
        // (past it the flip aborts and the source auto-unfences, ADR-048). A test sets the cap to
        // 0 to force the abort deterministically.
        let drain_passes = self.handoff_drain_passes;
        let final_drain_cap = self.handoff_final_drain_cap;
        let handoff = self
            .handoffs
            .get(position)
            .ok_or_else(|| {
                ShardError::Config(format!(
                    "execute_handoff: shard position {position} is not handoff-capable (the cluster \
                     was not built via connect_remote/connect_replicated)"
                ))
            })?
            .clone();
        let new_gen = handoff.generation() + 1;
        let expected = self.dict.fingerprint();
        let expected_tag = self.tag_dict.fingerprint();

        // Connect to the source and pin its un-sealed tail for the WHOLE move, so the segment-copy
        // seal — or any concurrent seal — cannot trim away the tail we still need (ADR-040).
        let source = crate::cluster::remote::RemoteShard::connect_for_coordinator_with_security(
            source_endpoint,
            handle.clone(),
            expected,
            expected_tag,
            // Fence/recover/lease the RIGHT slot: this handoff moves shard `position` (ADR-093).
            position as u32,
            self.coordinator_id,
            &self.client_security,
        )?
        .with_metrics(Arc::clone(&self.transport_metrics));
        let (lease, _pinned) = source.acquire_retention_lease()?;

        let do_move = || -> Result<u64, ShardError> {
            // Ship the dict + frozen tag space + drive the target to pull the source's segments at
            // snapshot `P` (the source keeps serving + writing — no quiesce).
            let dict_bytes = crate::storage::serialize_dict(&self.dict);
            let target = crate::cluster::remote::RemoteShard::
                connect_and_adopt_for_coordinator_with_security(
                target_endpoint,
                handle.clone(),
                dict_bytes,
                expected,
                crate::storage::serialize_tagdict(&self.tag_dict),
                self.tag_dict.fingerprint(),
                position as u32,
                self.placement_generation(),
                self.num_shards() as u32,
                self.coordinator_id,
                &self.client_security,
            )?
            .with_metrics(Arc::clone(&self.transport_metrics));
            let (_segments, _nq, p) = target.recover_from(source_endpoint, expected)?;
            // `RecoverFrom` replaces data but intentionally preserves the slot fence. A target that
            // was a previously demoted primary must be proven writable before it can become live
            // again; otherwise the move could acknowledge a primary that rejects every later write.
            clear_stale_fence(&target, "execute_handoff: recovered target")?;
            // Drain the tail (writes that landed during the copy), renewing the lease each pass.
            let mut hwm = LogPos(p);
            for _ in 0..drain_passes {
                let next = crate::cluster::replica::catch_up_replica(
                    &target, &source, &self.norm, &self.dict, hwm,
                )?;
                source.renew_retention_lease(lease, next)?;
                if next == hwm {
                    break;
                }
                hwm = next;
            }
            // FENCE the source: it stops accepting writes (the write-quiesce for `position` begins).
            // Reads + FetchTranslog stay served, so the catch-up below still works.
            // The fence RPC carries a write-deadline (ADR-085): a lost/slow response can return
            // Err AFTER the server applied the fence. Attempt the CAS-safe unfence(new_gen) on
            // failure (it lifts a fence the server DID apply at new_gen, no-op otherwise) so a
            // failed handoff never strands the source rejecting writes.
            if let Err(e) = source.fence(new_gen) {
                if let Err(ue) = source.unfence(new_gen) {
                    self.emit(EngineEvent::DurabilityFailure {
                        op: DurabilityOp::ReplicaDesync,
                        detail: "fence failed during handoff and the CAS-safe unfence cleanup \
                                 also failed; if the server had applied the fence the source \
                                 remains fenced and needs manual recovery"
                            .into(),
                        error: ue.to_string(),
                    });
                }
                return Err(e);
            }
            // From here the source is write-quiesced. Any failure BEFORE the flip must LIFT the
            // fence (ADR-048) so the source resumes serving — otherwise an aborted handoff leaves it
            // permanently quiesced (a write-rejecting node needing a manual restart). The
            // drain-to-convergence and its cap live in this scope; the flip (the success path) is
            // outside it and deliberately keeps the old owner fenced/dropped (serve-then-drop).
            //
            // Final drain to CONVERGENCE. A write that passed the source's fence check just before
            // the fence took effect can still append AFTER a single catch-up reads the tail (a
            // TOCTOU), so one pass is not enough. But the fenced source accepts no new writes, so its
            // tail is now finite and frozen: loop the catch-up until the high-water stops advancing.
            // Convergence (NOT a fixed pass count) is what guarantees the target holds every op the
            // source ever accepted — the flip below therefore cannot drop a write. The fence
            // guarantees this terminates; the cap only guards a misbehaving (still-accepting) source,
            // in which case we abort fail-closed rather than flip onto a not-yet-converged target.
            let drained = (|| -> Result<(), ShardError> {
                let mut converged = false;
                for _ in 0..final_drain_cap {
                    let next = crate::cluster::replica::catch_up_replica(
                        &target, &source, &self.norm, &self.dict, hwm,
                    )?;
                    source.renew_retention_lease(lease, next)?;
                    if next == hwm {
                        converged = true;
                        break;
                    }
                    hwm = next;
                }
                if !converged {
                    return Err(ShardError::Remote(format!(
                        "execute_handoff: fenced source {source_endpoint} did not converge (tail \
                         still advancing past {}) within {final_drain_cap} passes; aborting the \
                         flip to avoid dropping a write",
                        hwm.0
                    )));
                }
                Ok(())
            })();
            if let Err(e) = drained {
                // AUTO-UNFENCE (ADR-048): lift the fence we set so the source resumes accepting
                // writes instead of staying permanently quiesced. CAS-guarded server-side (only
                // this generation's fence is cleared), so it is safe even under a concurrent
                // handoff. If the unfence RPC ITSELF fails, the source is still fenced and needs
                // manual recovery — surface that as an event, but return the ORIGINAL abort error
                // (don't mask why the handoff failed).
                if let Err(ue) = source.unfence(new_gen) {
                    self.emit(EngineEvent::DurabilityFailure {
                        op: DurabilityOp::ReplicaDesync,
                        detail: "auto-unfence after an aborted handoff failed; the source remains \
                                 fenced at the handoff generation and needs manual recovery"
                            .into(),
                        error: ue.to_string(),
                    });
                }
                return Err(e);
            }
            // FLIP: re-point `position` at the target (reuse the recovery `target` as the new
            // backing). The old source backing is dropped from routing — serve-then-drop — and
            // writes to `position` now reach the target, ending the quiesce.
            handoff.swap_backing(Box::new(target), new_gen);
            Ok(new_gen)
        };
        let out = do_move();
        // Always release the lease (the source keeps serving reads regardless; it may now trim its
        // tail freely). A release failure on an otherwise-successful handoff is surfaced as an event,
        // not conflated with the outcome (the new owner is good; the source may just retain translog).
        if let Err(e) = source.release_retention_lease(lease) {
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::ReplicaDesync,
                detail:
                    "releasing the handoff retention lease on the source failed; the old owner \
                         may retain extra translog until its next successful seal"
                        .into(),
                error: e.to_string(),
            });
        }
        out
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::cluster::coordinator) enum HandoffRoute {
    AlreadyAtTarget { generation: u64 },
    Move,
}

fn classify_handoff_route(
    position: usize,
    source_endpoint: &str,
    target_endpoint: &str,
    primary_endpoint: &str,
    replica_endpoints: &[String],
    generation: u64,
) -> Result<HandoffRoute, ShardError> {
    let source = normalized_endpoint(source_endpoint);
    let target = normalized_endpoint(target_endpoint);
    let primary = normalized_endpoint(primary_endpoint);
    if target == primary {
        return Ok(HandoffRoute::AlreadyAtTarget { generation });
    }
    if source != primary {
        return Err(ShardError::Config(format!(
            "execute_handoff: source endpoint {source_endpoint} is not the current live \
             primary for shard position {position}"
        )));
    }
    if replica_endpoints
        .iter()
        .any(|endpoint| normalized_endpoint(endpoint) == target)
    {
        return Err(ShardError::Config(format!(
            "execute_handoff: target endpoint {target_endpoint} already serves shard \
             position {position}; raw handoff targets must be outside the live replica set"
        )));
    }
    Ok(HandoffRoute::Move)
}

pub(in crate::cluster::coordinator) fn normalized_endpoint(endpoint: &str) -> String {
    endpoint.trim_end_matches('/').to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_must_be_the_current_live_primary() {
        let error = classify_handoff_route(
            3,
            "https://stale:50051",
            "https://target:50051",
            "https://source:50051",
            &[],
            7,
        )
        .expect_err("a stale source must not seed recovery");
        assert!(
            error.to_string().contains("not the current live primary"),
            "{error}"
        );
    }

    #[test]
    fn target_cannot_be_an_existing_replica() {
        let error = classify_handoff_route(
            3,
            "https://source:50051",
            "HTTPS://REPLICA:50051/",
            "https://source:50051/",
            &["https://replica:50051".to_string()],
            7,
        )
        .expect_err("an existing replica is not a fresh raw-handoff target");
        assert!(error.to_string().contains("live replica set"), "{error}");
    }

    #[test]
    fn exact_retry_is_an_idempotent_no_change() {
        assert_eq!(
            classify_handoff_route(
                3,
                "https://old-source:50051",
                "HTTPS://TARGET:50051/",
                "https://target:50051",
                &[],
                11,
            )
            .expect("the requested target already owns routing"),
            HandoffRoute::AlreadyAtTarget { generation: 11 }
        );
    }
}
