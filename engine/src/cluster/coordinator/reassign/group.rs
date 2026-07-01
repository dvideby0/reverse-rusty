//! Group-aware data-moving reassignment (ADR-094, `distributed` feature): move a REPLICATED
//! position's whole group — primary + replicas — to its HRW-desired placement with zero false
//! negatives, generalizing [`ClusterEngine::reassign_and_move`]'s single-shard move-then-commit.
//!
//! ## Why the single-shard move cannot be reused at RF>1
//! [`ClusterEngine::execute_handoff`] swaps the position's backing to a SINGLE `RemoteShard` for the
//! target — de-replicating the position — while an `AssignShard` preserving the old replica list
//! would advertise replicas that no longer receive writes (a stale-failover false negative). Both
//! entry points therefore rejected `rf > 1` / replicated clusters (ADR-090; codex-hardened in the
//! ADR-092 landing).
//!
//! ## The algorithm (one position; C = committed group, D = desired group)
//! Everything below runs under `reassign_serial` (moves stay sequential — the chained-reshuffle
//! constraint) and under ONE retention lease on the source, exactly like `execute_handoff`:
//!
//! 1. **Plan.** Fail-closed endpoint resolution for `cp` (= C.primary, the ONLY supported recovery
//!    source: it is write-authoritative, so it alone provably holds every acked write without
//!    trusting replica in-sync state) and every D member. `C == D` under the SET-compare ⇒
//!    `NoChange` (replica ORDER is failover try-order, not placement).
//! 2. **Pre-fence: establish FRESH members** `F = D ∖ C` (in no composite ⇒ never serving ⇒ safe to
//!    bulk-replace): adopt → `RecoverFrom` the source → bounded pre-fence drain, writes still
//!    flowing (execute_handoff's phase 1, per member).
//! 3. **Fence `cp`'s slot.** The composite write path is primary-first and NEVER falls over for
//!    writes (`replica/shard_impl.rs`: `self.primary.op()?` propagates before any fan-out), so
//!    fencing the one primary slot write-quiesces the WHOLE group; fence-window writes queue as
//!    pending repairs and re-drive post-swap through the swapped backing (`resync`).
//! 4. **Freeze-probe.** Loop `source.translog_tail(cursor)` until an empty pass (bounded): the
//!    fenced source's tail is finite, so a stable read marks the frozen high-water — the same
//!    stops-advancing convergence criterion ADR-044 accepts. Needed as its own step because (a)
//!    promotion / replica-only moves have `F = ∅` (no fresh member whose drain would prove
//!    convergence) and (b) with SEVERAL members, per-member drains alone don't establish a COMMON
//!    frozen point.
//! 5. **Drain F to the frozen tail** (each member's catch-up loops until its cursor stops
//!    advancing; the fenced+probed source cannot outrun it). Abort past the cap ⇒ auto-unfence +
//!    `Err` (the ADR-048 rollback: routing + committed map untouched).
//! 6. **Re-establish RETAINED members** `R = (D ∩ C) ∖ {cp}` — ONLY now, post-freeze: adopt (a
//!    no-op handshake on the existing slot) → `RecoverFrom` → a verify catch-up that must return
//!    no tail. `RecoverFrom` REPLACES the slot's state, which is exactly right here: the source
//!    seals BEFORE streaming and the target install is one atomic per-slot store, so a copy taken
//!    from the fenced-and-frozen source is **complete at install** — a silently-desynced committed
//!    replica is deterministically HEALED, without the coordinator ever reading the composite's
//!    private in-sync state. Doing this PRE-freeze would be unsafe: an R member is live in the OLD
//!    composite (reads may fail over to it), and a segments-at-`P` install would serve a state
//!    missing the tail until its drain converged. `cp ∈ D` is never recovered — it IS the frozen
//!    authority (promotion and demotion fall out of the uniform algorithm, no special case).
//! 7. **Assemble + swap.** Build the new backing in D's shape (a `ReplicatedShard` over the
//!    per-member connections, or a bare `RemoteShard` when D has no replicas — an rf REDUCTION
//!    falls out for free), install the coordinator's observer as its event sink FIRST
//!    (`set_observer` fans sinks only at install time, so a later-swapped composite would
//!    otherwise buffer its `ReplicaDesync` events forever), then `swap_backing` at the new
//!    generation.
//! 8. **Unfence `cp` iff `cp ∈ D`, AFTER the swap** — before it, the write window would reopen on
//!    the old composite; without it, a retained/demoted `cp` would fail its first fan-out and
//!    silently drop out of the new in-sync set. An unfence-RPC failure is degraded-but-zero-FN
//!    (loud event, first fan-out desyncs it). `cp ∉ D` stays fenced forever: serve-then-drop +
//!    stale-coordinator write protection, the ADR-090 posture. Orphan slots on `C ∖ D` nodes are
//!    unrouted post-swap and post-restart (the committed map is what `resolve_topology` reads);
//!    GC stays deferred.
//! 9. **Move-then-commit.** Re-read the committed state and compare the FULL group against the `C`
//!    we planned from (strictly stronger than the RF=1 primary-only compare), then commit the full
//!    `AssignShard(desired)` with bounded retries. Outcomes reuse [`ReassignOutcome`] (`from`/`to`
//!    = the primaries); `MovedButNotCommitted` re-drives idempotently on the next pass.
//!
//! ## Cost (deliberate, recorded in ADR-094)
//! The fence window includes an `O(corpus)` re-copy per RETAINED member — the price of provable
//! completeness without in-sync introspection (a pure promotion re-copies a member that is almost
//! certainly already complete). Writes during the window are never lost: they return
//! `PartiallyApplied` and re-drive via `resync` into the NEW group. Deferred optimizations: an
//! in-sync-snapshot + content-fingerprint protocol to skip provably-complete members, and a
//! server-side staged recovery (shadow install) that moves retained-member copies out of the
//! fence window.

use std::collections::BTreeSet;
use std::sync::{Arc, PoisonError};

use tokio::runtime::Handle;

use crate::cluster::allocator;
use crate::cluster::clog::LogPos;
use crate::cluster::control::{ClusterState, NodeId, NodeRole, ShardAssignment};
use crate::cluster::remote::RemoteShard;
use crate::cluster::replica::{catch_up_replica, ReplicatedShard};
use crate::cluster::shard::{Shard, ShardError};
use crate::events::{DurabilityOp, EngineEvent};

use super::{ClusterEngine, ReassignOutcome, COMMIT_ATTEMPTS};

/// Set-equality of two replica lists. Replica ORDER is composite failover try-order — an artifact
/// of how the group was seeded/planned — never placement, so a target computation that compared
/// `Vec`s would flag every healthy cluster whose seed order differs from HRW rank order and drive
/// K spurious `O(corpus)` moves on its first pass.
fn replica_set_eq(a: &[NodeId], b: &[NodeId]) -> bool {
    let sa: BTreeSet<u64> = a.iter().map(|n| n.0).collect();
    let sb: BTreeSet<u64> = b.iter().map(|n| n.0).collect();
    sa == sb
}

/// Group equality for placement purposes: primary by identity, replicas as a SET.
pub(in crate::cluster::coordinator) fn groups_equal(a: &ShardAssignment, b: &ShardAssignment) -> bool {
    a.primary == b.primary && replica_set_eq(&a.replicas, &b.replicas)
}

/// The group-aware sibling of [`super::rebalance_targets`]: positions whose committed GROUP
/// (primary by identity OR replica set) diverges from the HRW-desired placement at `rf`, each with
/// its full desired [`ShardAssignment`], in position order. Pure over the cluster-state document.
/// A missing committed entry counts as diverged (the move then fails loudly per position, matching
/// `reassign_and_move`'s no-committed-assignment error). Same node filter as the primary-only
/// targets: only addr'd Data nodes are placement candidates. Empty for an in-process / genesis
/// cluster ⇒ the byte-identical no-op.
pub(in crate::cluster::coordinator) fn rebalance_group_targets(
    state: &ClusterState,
    rf: usize,
) -> Vec<(u32, ShardAssignment)> {
    let nodes: Vec<NodeId> = state
        .nodes
        .iter()
        .filter(|n| n.role == NodeRole::Data && n.addr.is_some())
        .map(|n| n.id)
        .collect();
    if nodes.is_empty() {
        return Vec::new();
    }
    // `plan_assignments` clamps rf to the addr'd-node count: an OPERATOR deregistration below rf
    // legitimately shrinks desired groups (a commanded de-replication); a dead-but-registered node
    // keeps its HRW slots (moves to it fail per position and are retried each pass — never a
    // silent de-replication).
    let desired = allocator::plan_assignments(&nodes, state.num_shards, rf);
    desired
        .into_iter()
        .filter(|d| {
            !state
                .assignments
                .iter()
                .find(|a| a.position == d.position)
                .is_some_and(|c| groups_equal(c, d))
        })
        .map(|d| (d.position, d))
        .collect()
}

impl ClusterEngine {
    /// Move a position's whole replica GROUP to `desired` (ADR-094) — data first, commit second.
    /// See the module docs for the phase-by-phase algorithm and its zero-FN argument. Returns the
    /// same [`ReassignOutcome`] contract as [`reassign_and_move`](Self::reassign_and_move)
    /// (`from`/`to` are the old/new primaries); a clean failure rolls the position fully back
    /// (routing + committed map untouched, source auto-unfenced). Requires a handoff-capable
    /// cluster (built via `connect_remote`/`connect_replicated`); the committed primary is the
    /// only supported recovery source — a primary-down position fails loudly (degraded repair is
    /// [`peer_recover_replica`](Self::peer_recover_replica)'s job, a different operation).
    pub fn reassign_group_and_move(
        &self,
        position: usize,
        desired: ShardAssignment,
        handle: &Handle,
    ) -> Result<ReassignOutcome, ShardError> {
        // Serialize against every other data-moving op for the whole move-then-commit (the same
        // guard `reassign_and_move` takes — group and single moves never interleave).
        let _guard = self
            .reassign_serial
            .lock()
            .unwrap_or_else(PoisonError::into_inner);

        let pos = position as u32;
        if desired.position != pos {
            return Err(ShardError::Config(format!(
                "reassign_group_and_move: desired assignment names position {} but the move is \
                 for position {position}",
                desired.position
            )));
        }

        let state = self.control_state()?;
        let committed = state
            .assignments
            .iter()
            .find(|a| a.position == pos)
            .cloned()
            .ok_or_else(|| {
                ShardError::ControlPlane(format!(
                    "reassign_group_and_move: no committed assignment for shard position {position}"
                ))
            })?;

        // The idempotent no-op: the committed group already IS the desired placement.
        if groups_equal(&committed, &desired) {
            return Ok(ReassignOutcome::NoChange { position: pos });
        }

        // Fail-closed endpoint resolution (never silently skip an unroutable member — that would
        // assemble a group that routes a title nowhere). Mirrors `reassign_and_move`.
        let addr_of = |id: NodeId| -> Result<String, ShardError> {
            state
                .nodes
                .iter()
                .find(|n| n.id == id)
                .and_then(|n| n.addr.clone())
                .ok_or_else(|| {
                    ShardError::ControlPlane(format!(
                        "reassign_group_and_move: node {} has no registered endpoint (addr)",
                        id.0
                    ))
                })
        };
        let cp = committed.primary;
        let cp_ep = addr_of(cp)?;
        // D's members in composite order (primary first, then replicas), each with its endpoint.
        let mut d_members: Vec<(NodeId, String)> = Vec::with_capacity(1 + desired.replicas.len());
        d_members.push((desired.primary, addr_of(desired.primary)?));
        for r in &desired.replicas {
            d_members.push((*r, addr_of(*r)?));
        }
        // Distinct endpoints resolving to one address would make "fresh vs retained" ambiguous —
        // and HRW never plans a duplicate node, so treat it as the config error it is.
        {
            let eps: BTreeSet<&str> = d_members.iter().map(|(_, e)| e.as_str()).collect();
            if eps.len() != d_members.len() {
                return Err(ShardError::Config(format!(
                    "reassign_group_and_move: desired group for position {position} resolves two \
                     members to one endpoint ({d_members:?})"
                )));
            }
        }
        let committed_ids: BTreeSet<u64> = std::iter::once(cp.0)
            .chain(committed.replicas.iter().map(|n| n.0))
            .collect();

        let handoff = self
            .handoffs
            .get(position)
            .ok_or_else(|| {
                ShardError::Config(format!(
                    "reassign_group_and_move: shard position {position} is not handoff-capable \
                     (the cluster was not built via connect_remote/connect_replicated)"
                ))
            })?
            .clone();
        let new_gen = handoff.generation() + 1;
        let expected = self.dict.fingerprint();
        let expected_tag = self.tag_dict.fingerprint();
        let drain_passes = self.handoff_drain_passes;
        let final_drain_cap = self.handoff_final_drain_cap;

        // Connect the SOURCE (the committed primary — write-authoritative, so it provably holds
        // every acked write) and pin its un-sealed tail for the whole move (ADR-040).
        let source = RemoteShard::connect_with_security(
            &cp_ep,
            handle.clone(),
            expected,
            expected_tag,
            pos,
            &self.client_security,
        )?
        .with_metrics(Arc::clone(&self.transport_metrics));
        let (lease, pinned) = source.acquire_retention_lease()?;

        let do_move = || -> Result<u64, ShardError> {
            let dict_bytes = crate::storage::serialize_dict(&self.dict);
            let tag_bytes = crate::storage::serialize_tagdict(&self.tag_dict);

            // ---- Phase 2 (pre-fence): establish FRESH members, writes still flowing ----
            // `(node id, connection, member high-water)` for every D member established so far.
            let mut established: Vec<(u64, RemoteShard, LogPos)> = Vec::new();
            for (nid, ep) in d_members.iter().filter(|(nid, _)| {
                !committed_ids.contains(&nid.0)
            }) {
                let t = RemoteShard::connect_and_adopt_with_security(
                    ep,
                    handle.clone(),
                    dict_bytes.clone(),
                    expected,
                    tag_bytes.clone(),
                    expected_tag,
                    pos,
                    &self.client_security,
                )?
                .with_metrics(Arc::clone(&self.transport_metrics));
                let (_segments, _nq, p) = t.recover_from(&cp_ep, expected)?;
                let mut hwm = LogPos(p);
                for _ in 0..drain_passes {
                    let next = catch_up_replica(&t, &source, &self.norm, &self.dict, hwm)?;
                    source.renew_retention_lease(lease, next)?;
                    if next == hwm {
                        break;
                    }
                    hwm = next;
                }
                established.push((nid.0, t, hwm));
            }

            // ---- Phase 3: FENCE the committed primary (write-quiesce for the whole group) ----
            // The CAS-safe cleanup mirrors `execute_handoff`: a lost fence RESPONSE can leave the
            // server fenced, so attempt unfence(new_gen) before propagating the fence error.
            if let Err(e) = source.fence(new_gen) {
                if let Err(ue) = source.unfence(new_gen) {
                    self.emit(EngineEvent::DurabilityFailure {
                        op: DurabilityOp::ReplicaDesync,
                        detail: "fence failed during a group move and the CAS-safe unfence \
                                 cleanup also failed; if the server had applied the fence the \
                                 source remains fenced and needs manual recovery"
                            .into(),
                        error: ue.to_string(),
                    });
                }
                return Err(e);
            }

            // From here every failure before the flip must LIFT the fence (ADR-048) so the source
            // resumes accepting writes. The whole post-fence body runs in one closure so the abort
            // path is single.
            let mut fenced_work = || -> Result<u64, ShardError> {
                // ---- Phase 4: freeze-probe — the fenced tail must stop advancing ----
                // Start from the furthest point already known (the lease pin, or any fresh
                // member's drain cursor) so the probe re-reads as little as possible.
                let mut cursor = established
                    .iter()
                    .map(|(_, _, h)| *h)
                    .max()
                    .unwrap_or(pinned)
                    .max(pinned);
                let mut frozen = false;
                for _ in 0..final_drain_cap {
                    let tail = source.translog_tail(cursor)?;
                    match tail.iter().map(|(p, _)| *p).max() {
                        None => {
                            frozen = true;
                            break;
                        }
                        Some(max_pos) => {
                            source.renew_retention_lease(lease, cursor)?;
                            cursor = max_pos.max(cursor);
                        }
                    }
                }
                if !frozen {
                    return Err(ShardError::Remote(format!(
                        "reassign_group_and_move: fenced source {cp_ep} did not converge (tail \
                         still advancing past {}) within {final_drain_cap} passes; aborting the \
                         flip to avoid dropping a write",
                        cursor.0
                    )));
                }

                // ---- Phase 5: drain FRESH members to the frozen tail ----
                for (_, t, hwm) in established.iter_mut() {
                    let mut converged = false;
                    for _ in 0..final_drain_cap.max(1) {
                        let next = catch_up_replica(t, &source, &self.norm, &self.dict, *hwm)?;
                        source.renew_retention_lease(lease, next)?;
                        if next == *hwm {
                            converged = true;
                            break;
                        }
                        *hwm = next;
                    }
                    if !converged {
                        return Err(ShardError::Remote(format!(
                            "reassign_group_and_move: fresh member did not converge on the \
                             frozen source within {final_drain_cap} passes; aborting the flip",
                        )));
                    }
                }

                // ---- Phase 6: re-establish RETAINED members from the frozen source ----
                // (`D ∩ C` minus the source itself.) The bulk copy is complete-at-install now:
                // the source seals BEFORE streaming and nothing new can land past the probe.
                for (nid, ep) in d_members
                    .iter()
                    .filter(|(nid, _)| committed_ids.contains(&nid.0) && *nid != cp)
                {
                    let t = RemoteShard::connect_and_adopt_with_security(
                        ep,
                        handle.clone(),
                        dict_bytes.clone(),
                        expected,
                        tag_bytes.clone(),
                        expected_tag,
                        pos,
                        &self.client_security,
                    )?
                    .with_metrics(Arc::clone(&self.transport_metrics));
                    let (_segments, _nq, p) = t.recover_from(&cp_ep, expected)?;
                    // Verify: the copy of a frozen source must have NO tail past its seal point.
                    let verify = catch_up_replica(&t, &source, &self.norm, &self.dict, LogPos(p))?;
                    if verify != LogPos(p) {
                        return Err(ShardError::Remote(format!(
                            "reassign_group_and_move: retained member re-established from the \
                             frozen source still had a translog tail past its seal point \
                             ({} > {p}) — the source is not actually frozen; aborting the flip",
                            verify.0
                        )));
                    }
                    established.push((nid.0, t, verify));
                }

                // ---- Phase 7: assemble the new backing in D's shape and swap ----
                // Per-member connections: the just-established ones, plus a dedicated connection
                // to the source for a retained `cp` (its recovery connection stays owned by the
                // outer scope for the lease release + unfence).
                let mut conn_of = |nid: NodeId, ep: &str| -> Result<Box<dyn Shard>, ShardError> {
                    if let Some(i) = established.iter().position(|(id, _, _)| *id == nid.0) {
                        let (_, t, _) = established.remove(i);
                        return Ok(Box::new(t));
                    }
                    debug_assert_eq!(nid, cp, "every non-cp member was established above");
                    let t = RemoteShard::connect_with_security(
                        ep,
                        handle.clone(),
                        expected,
                        expected_tag,
                        pos,
                        &self.client_security,
                    )?
                    .with_metrics(Arc::clone(&self.transport_metrics));
                    Ok(Box::new(t))
                };
                let mut members = d_members.iter();
                let (p_id, p_ep) = members.next().expect("D has a primary");
                let primary_conn = conn_of(*p_id, p_ep)?;
                let mut replica_conns: Vec<Box<dyn Shard>> = Vec::new();
                for (r_id, r_ep) in members {
                    replica_conns.push(conn_of(*r_id, r_ep)?);
                }
                let backing: Box<dyn Shard> = if replica_conns.is_empty() {
                    primary_conn
                } else {
                    Box::new(ReplicatedShard::new(primary_conn, replica_conns))
                };
                // Install the coordinator's observer as the new backing's event sink BEFORE the
                // swap: `set_observer` fans sinks only at install time, so a later-swapped
                // composite would otherwise buffer its `ReplicaDesync` events forever.
                if let Some(obs) = self
                    .observer
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .as_ref()
                {
                    backing.set_event_sink(Arc::clone(obs));
                }
                handoff.swap_backing(backing, new_gen);
                Ok(new_gen)
            };
            match fenced_work() {
                Ok(gen) => {
                    // ---- Phase 8: unfence a RETAINED source, AFTER the swap ----
                    // Before the swap the write window would reopen on the OLD composite; without
                    // it a retained/demoted cp would fail its first fan-out and silently desync.
                    // A cp NOT in D stays fenced forever (serve-then-drop, ADR-090).
                    if d_members.iter().any(|(nid, _)| *nid == cp) {
                        if let Err(e) = source.unfence(new_gen) {
                            self.emit(EngineEvent::DurabilityFailure {
                                op: DurabilityOp::ReplicaDesync,
                                detail: format!(
                                    "group move of shard {position}: unfencing the retained \
                                     source (node {}) after the swap failed; its first \
                                     replicated write will desync it (degraded redundancy, \
                                     zero-FN) until peer re-recovery",
                                    cp.0
                                ),
                                error: e.to_string(),
                            });
                        }
                    }
                    Ok(gen)
                }
                Err(e) => {
                    // AUTO-UNFENCE (ADR-048): the abort path lifts the fence we set so the source
                    // resumes accepting writes; CAS-guarded server-side. Surface an unfence
                    // failure as an event but return the ORIGINAL abort error.
                    if let Err(ue) = source.unfence(new_gen) {
                        self.emit(EngineEvent::DurabilityFailure {
                            op: DurabilityOp::ReplicaDesync,
                            detail: "auto-unfence after an aborted group move failed; the source \
                                     remains fenced at the move generation and needs manual \
                                     recovery"
                                .into(),
                            error: ue.to_string(),
                        });
                    }
                    Err(e)
                }
            }
        };
        let moved = do_move();
        // Always release the lease (success or error) — the source may then trim its tail freely.
        if let Err(e) = source.release_retention_lease(lease) {
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::ReplicaDesync,
                detail: "releasing the group-move retention lease on the source failed; the old \
                         primary may retain extra translog until its next successful seal"
                    .into(),
                error: e.to_string(),
            });
        }
        let generation = moved?;

        // ---- Phase 9: move-then-commit ----
        // COMPARE-AND-SET on the FULL group (strictly stronger than the RF=1 primary-only compare):
        // if a concurrent op re-shaped this position under us (only possible across coordinators —
        // the serial guard covers this one), do NOT overwrite its commit. Either way the data is on
        // D and routing serves it; the durable map just isn't ours to claim.
        let now = self.control_state()?;
        let unchanged = now
            .assignments
            .iter()
            .find(|a| a.position == pos)
            .is_some_and(|a| groups_equal(a, &committed));
        if !unchanged {
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::ReplicaDesync,
                detail: format!(
                    "reassign_group_and_move moved shard {position}'s group to primary node {} \
                     and flipped routing, but the committed group changed under it (a concurrent \
                     reassign); not overwriting the committed map. Re-run to reconcile.",
                    desired.primary.0
                ),
                error: "committed assignment changed during a data-moving group reassign".into(),
            });
            return Ok(ReassignOutcome::MovedButNotCommitted {
                position: pos,
                from: cp,
                to: desired.primary,
                generation,
            });
        }
        let mut last_err: Option<ShardError> = None;
        for attempt in 0..COMMIT_ATTEMPTS {
            match self.reassign_shard(desired.clone()) {
                Ok(()) => {
                    return Ok(ReassignOutcome::Moved {
                        position: pos,
                        from: cp,
                        to: desired.primary,
                        generation,
                    })
                }
                Err(e) => {
                    last_err = Some(e);
                    if attempt + 1 < COMMIT_ATTEMPTS {
                        std::thread::sleep(std::time::Duration::from_millis(50));
                    }
                }
            }
        }
        // Persistent commit failure: the move already succeeded, so D is authoritative — KEEP live
        // routing on it and surface a loud event; the committed map still names the reads-serving
        // old group (zero-FN), and a re-run re-converges + re-commits (idempotent).
        self.emit(EngineEvent::DurabilityFailure {
            op: DurabilityOp::ReplicaDesync,
            detail: format!(
                "reassign_group_and_move moved shard {position}'s group to primary node {} and \
                 flipped routing, but committing the new group failed after {COMMIT_ATTEMPTS} \
                 attempts; live routing stays on the new group (which holds every acked write) \
                 and the committed map still names the reads-serving old group — re-run to \
                 reconcile the durable map (idempotent).",
                desired.primary.0
            ),
            error: last_err.map(|e| e.to_string()).unwrap_or_default(),
        });
        Ok(ReassignOutcome::MovedButNotCommitted {
            position: pos,
            from: cp,
            to: desired.primary,
            generation,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::control::NodeDescriptor;

    fn node(id: u64) -> NodeDescriptor {
        NodeDescriptor {
            id: NodeId(id),
            addr: Some(format!("http://127.0.0.1:{}", 50050 + id)),
            role: NodeRole::Data,
        }
    }

    fn state_with(
        nodes: Vec<NodeDescriptor>,
        num_shards: u32,
        assignments: Vec<ShardAssignment>,
    ) -> ClusterState {
        ClusterState {
            epoch: 0,
            nodes,
            voters: Vec::new(),
            assignments,
            num_shards,
            vnodes: 128,
            dict_fingerprint: 0,
            model_version: 0,
        }
    }

    fn assign(position: u32, primary: u64, replicas: &[u64]) -> ShardAssignment {
        ShardAssignment {
            position,
            primary: NodeId(primary),
            replicas: replicas.iter().map(|&r| NodeId(r)).collect(),
        }
    }

    /// The committed groups exactly match the HRW plan ⇒ no targets — INCLUDING when the committed
    /// replica list is in a different ORDER than the plan emits (seed order is CLI order, plan
    /// order is HRW rank order): a Vec-compare here would flag every healthy cluster as diverged
    /// and drive K spurious O(corpus) moves.
    #[test]
    fn converged_groups_yield_no_targets_regardless_of_replica_order() {
        let nodes = vec![node(1), node(2), node(3)];
        let ids: Vec<NodeId> = nodes.iter().map(|n| n.id).collect();
        let desired = allocator::plan_assignments(&ids, 6, 2);
        // Commit exactly the plan, but with each replica list REVERSED.
        let committed: Vec<ShardAssignment> = desired
            .iter()
            .map(|a| ShardAssignment {
                position: a.position,
                primary: a.primary,
                replicas: a.replicas.iter().rev().copied().collect(),
            })
            .collect();
        let st = state_with(nodes, 6, committed);
        assert!(
            rebalance_group_targets(&st, 2).is_empty(),
            "an HRW-converged map (replicas set-equal) has nothing to move"
        );
    }

    /// Primary-only, replica-only, and both-diverged positions are ALL targets at rf=2 — the
    /// replica-only case is exactly what the primary-only `rebalance_targets` misses (at RF>1
    /// remote, a replica diff IS a data move).
    #[test]
    fn targets_cover_primary_replica_and_both_divergence() {
        let nodes = vec![node(1), node(2), node(3)];
        let ids: Vec<NodeId> = nodes.iter().map(|n| n.id).collect();
        let desired = allocator::plan_assignments(&ids, 6, 2);

        // Start converged, then perturb: position 0 gets a wrong PRIMARY (swap with its replica),
        // position 1 a wrong REPLICA (rotate to the node the plan left out), position 2 both.
        let mut committed: Vec<ShardAssignment> = desired.clone();
        let other = |a: &ShardAssignment| -> NodeId {
            // The one node of {1,2,3} that is in neither the primary nor the replicas.
            ids.iter()
                .copied()
                .find(|n| *n != a.primary && !a.replicas.contains(n))
                .expect("3 nodes, rf=2 ⇒ exactly one left out")
        };
        committed[0] = ShardAssignment {
            position: desired[0].position,
            primary: desired[0].replicas[0],
            replicas: vec![desired[0].primary],
        };
        committed[1] = ShardAssignment {
            position: desired[1].position,
            primary: desired[1].primary,
            replicas: vec![other(&desired[1])],
        };
        committed[2] = ShardAssignment {
            position: desired[2].position,
            primary: other(&desired[2]),
            replicas: vec![desired[2].primary],
        };
        let st = state_with(nodes, 6, committed);
        let targets = rebalance_group_targets(&st, 2);
        let target_positions: Vec<u32> = targets.iter().map(|(p, _)| *p).collect();
        for expect in [desired[0].position, desired[1].position, desired[2].position] {
            assert!(
                target_positions.contains(&expect),
                "position {expect} diverges and must be a target: {target_positions:?}"
            );
        }
        // Every target carries the FULL desired assignment (the plan's group, not a bare primary).
        for (p, d) in &targets {
            let planned = desired.iter().find(|a| a.position == *p).unwrap();
            assert!(groups_equal(d, planned), "target {p} carries the HRW group");
        }
        // Untouched positions are not targets.
        for a in &desired[3..] {
            assert!(
                !target_positions.contains(&a.position),
                "converged position {} must not be a target",
                a.position
            );
        }
    }

    /// A missing committed entry counts as diverged (the move then fails loudly per position),
    /// and only addr'd Data nodes are placement candidates (the addr-less manager is excluded).
    #[test]
    fn missing_assignment_is_diverged_and_manager_is_excluded() {
        let mut manager = node(0);
        manager.addr = None;
        manager.role = NodeRole::Manager;
        let nodes = vec![manager, node(1), node(2)];
        let st = state_with(nodes, 3, Vec::new());
        let targets = rebalance_group_targets(&st, 2);
        assert_eq!(targets.len(), 3, "every unassigned position is a target");
        for (_, d) in &targets {
            assert_ne!(d.primary, NodeId(0), "the manager is never a placement");
            assert!(
                !d.replicas.contains(&NodeId(0)),
                "the manager is never a replica placement"
            );
            assert_eq!(d.replicas.len(), 1, "rf=2 over 2 data nodes ⇒ 1 replica");
        }
    }

    /// rf clamps to the addr'd-node count: over ONE data node, an rf=3 request plans bare
    /// primaries (a commanded de-replication when nodes were deregistered — never silent).
    #[test]
    fn rf_clamps_to_addrd_node_count() {
        let nodes = vec![node(1)];
        let st = state_with(nodes, 2, vec![assign(0, 1, &[]), assign(1, 1, &[])]);
        assert!(
            rebalance_group_targets(&st, 3).is_empty(),
            "one node, rf clamped to 1, both positions already there ⇒ converged"
        );
    }

    /// Group equality is primary-identity + replica-SET equality.
    #[test]
    fn groups_equal_semantics() {
        let a = assign(0, 1, &[2, 3]);
        assert!(groups_equal(&a, &assign(0, 1, &[3, 2])), "order-insensitive");
        assert!(!groups_equal(&a, &assign(0, 2, &[1, 3])), "primary differs");
        assert!(!groups_equal(&a, &assign(0, 1, &[2])), "replica set differs");
        assert!(groups_equal(&assign(0, 1, &[]), &assign(0, 1, &[])), "bare");
    }
}
