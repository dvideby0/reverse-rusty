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
//! Everything below runs under a busy-endpoint ledger reservation of `{cp} ∪ D` (ADR-095 — moves
//! sharing a node serialize, per the chained-reshuffle constraint; disjoint moves may run in
//! parallel) and under ONE retention lease on the source, exactly like `execute_handoff` — with
//! two member-entry disciplines the multi-member shape adds (both codex findings on this ADR):
//! **stale fences are cleared on every member entering the group** (serve-then-drop leaves a
//! dropped primary fenced forever, and `RecoverFrom` preserves the fence — a re-entering member
//! would otherwise reject writes / desync on first fan-out; see [`clear_stale_fence`]), and **the
//! lease is renewed only to the MINIMUM outstanding member cursor** (a later member's
//! `RecoverFrom` re-seals the source and trims to the lease floor, so advancing it past a lagging
//! member would trim tail entries that member still needs — a false negative):
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

use crate::cluster::clog::LogPos;
use crate::cluster::control::{NodeId, ShardAssignment};
use crate::cluster::remote::RemoteShard;
use crate::cluster::replica::{catch_up_replica, ReplicatedShard};
use crate::cluster::shard::{Shard, ShardError};
use crate::events::{DurabilityOp, EngineEvent};

use super::{ClusterEngine, ReassignOutcome, COMMIT_ATTEMPTS, PLAN_ATTEMPTS};

/// The pure planning layer: target computation, group equality, the stale-fence clear, and the
/// validated plan type (split for the <650-line budget).
mod plan;
use plan::{clear_stale_fence, PlannedGroupMove};
pub(in crate::cluster::coordinator) use plan::{groups_equal, rebalance_group_targets};

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
        desired: &ShardAssignment,
        handle: &Handle,
    ) -> Result<ReassignOutcome, ShardError> {
        let pos = position as u32;
        if desired.position != pos {
            return Err(ShardError::Config(format!(
                "reassign_group_and_move: desired assignment names position {} but the move is \
                 for position {position}",
                desired.position
            )));
        }

        // Plan → reserve → revalidate (ADR-095): resolve the move's endpoint footprint
        // (`{cp} ∪ D` — the source we fence + every member we establish/install) from a committed
        // read, reserve it in the busy-endpoint ledger — blocking until every CONFLICTING in-flight
        // move completes — then confirm the position's committed GROUP did not change while we
        // waited. A change re-plans from the fresh state (bounded); the phase-9 CAS stays the
        // final backstop.
        let mut planned: Option<PlannedGroupMove<'_>> = None;
        for _ in 0..PLAN_ATTEMPTS {
            let state = self.control_state()?;
            let committed = state
                .assignments
                .iter()
                .find(|a| a.position == pos)
                .cloned()
                .ok_or_else(|| {
                    ShardError::ControlPlane(format!(
                        "reassign_group_and_move: no committed assignment for shard position \
                         {position}"
                    ))
                })?;

            // The idempotent no-op: the committed group already IS the desired placement.
            if groups_equal(&committed, desired) {
                return Ok(ReassignOutcome::NoChange { position: pos });
            }

            // Fail-closed endpoint resolution (never silently skip an unroutable member — that
            // would assemble a group that routes a title nowhere). Mirrors `reassign_and_move`.
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
            let cp_ep = addr_of(committed.primary)?;
            // D's members in composite order (primary first, then replicas), each with its
            // endpoint.
            let mut d_members: Vec<(NodeId, String)> =
                Vec::with_capacity(1 + desired.replicas.len());
            d_members.push((desired.primary, addr_of(desired.primary)?));
            for r in &desired.replicas {
                d_members.push((*r, addr_of(*r)?));
            }
            // Distinct endpoints resolving to one address would make "fresh vs retained"
            // ambiguous — and HRW never plans a duplicate node, so treat it as the config error
            // it is.
            {
                let eps: BTreeSet<&str> = d_members.iter().map(|(_, e)| e.as_str()).collect();
                if eps.len() != d_members.len() {
                    return Err(ShardError::Config(format!(
                        "reassign_group_and_move: desired group for position {position} resolves \
                         two members to one endpoint ({d_members:?})"
                    )));
                }
            }

            let mut footprint: Vec<&str> = Vec::with_capacity(1 + d_members.len());
            footprint.push(cp_ep.as_str());
            footprint.extend(d_members.iter().map(|(_, e)| e.as_str()));
            let ticket = self.move_ledger.reserve(&footprint);
            // Revalidate BOTH the committed group AND every member's endpoint resolution (codex
            // P2 on this ADR): while we waited on the ledger, a concurrent op may have re-shaped
            // this position, or re-registered a member with a NEW addr (`register_node` replaces
            // by id). Installing/copying over stale endpoints and then committing the NodeIds
            // would leave a route-by-assignments restart resolving to servers that never received
            // the group — a restart false negative.
            let now = self.control_state()?;
            let addr_now = |id: NodeId| {
                now.nodes
                    .iter()
                    .find(|n| n.id == id)
                    .and_then(|n| n.addr.as_deref())
            };
            let group_unchanged = now
                .assignments
                .iter()
                .find(|a| a.position == pos)
                .is_some_and(|a| groups_equal(a, &committed));
            let eps_unchanged = addr_now(committed.primary) == Some(cp_ep.as_str())
                && d_members
                    .iter()
                    .all(|(nid, ep)| addr_now(*nid) == Some(ep.as_str()));
            if group_unchanged && eps_unchanged {
                planned = Some(PlannedGroupMove {
                    committed,
                    cp_ep,
                    d_members,
                    ticket,
                });
                break;
            }
            // The group (or a member's endpoint) changed while we waited on the ledger: the
            // ticket drops here and the next iteration re-plans from the fresh committed state.
        }
        let Some(PlannedGroupMove {
            committed,
            cp_ep,
            d_members,
            ticket: _ticket,
        }) = planned
        else {
            return Err(ShardError::ControlPlane(format!(
                "reassign_group_and_move: the committed group for shard position {position} kept \
                 changing while planning the move ({PLAN_ATTEMPTS} attempts); retry once the map \
                 stops churning"
            )));
        };
        let cp = committed.primary;
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
        // A stale fence on the SOURCE (cp was dropped from this position's group by an earlier
        // move and later became its committed primary again) means the position is ALREADY
        // write-broken; clearing it at move start is the repair, and lets phase 3's fence(new_gen)
        // + the phase-8 unfence CAS operate on OUR generation.
        clear_stale_fence(&source, "reassign_group_and_move: source")?;
        let (lease, pinned) = source.acquire_retention_lease()?;

        let do_move = || -> Result<u64, ShardError> {
            let dict_bytes = crate::storage::serialize_dict(&self.dict);
            let tag_bytes = crate::storage::serialize_tagdict(&self.tag_dict);

            // ---- Phase 2 (pre-fence): establish FRESH members, writes still flowing ----
            // `(node id, connection, member high-water)` for every D member established so far.
            // LEASE DISCIPLINE (codex P2 on this ADR): the ONE source lease is renewed only to the
            // MINIMUM outstanding member cursor — never to the current member's own high-water. A
            // later member's `RecoverFrom` re-SEALS the source (baking the tail into segments and
            // trimming the translog down to the lease floor), so advancing the floor past a
            // lagging member would trim entries that member still needs — an unrecoverable gap ⇒
            // a false negative. Members established LATER never constrain the floor: their bulk
            // copy is taken at a FRESH seal, so they depend only on the tail past their own `P`.
            let mut established: Vec<(u64, RemoteShard, LogPos)> = Vec::new();
            let floor_with = |established: &[(u64, RemoteShard, LogPos)], candidate: LogPos| {
                established
                    .iter()
                    .map(|(_, _, h)| *h)
                    .fold(candidate, std::cmp::Ord::min)
            };
            for (nid, ep) in d_members
                .iter()
                .filter(|(nid, _)| !committed_ids.contains(&nid.0))
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
                // A fresh member may RE-ENTER a group it was dropped from (its slot preserved a
                // stale fence through RecoverFrom) — clear it or the committed member would
                // reject every write / desync on first fan-out.
                clear_stale_fence(&t, "reassign_group_and_move: fresh member")?;
                let (_segments, _nq, p) = t.recover_from(&cp_ep, expected)?;
                let mut hwm = LogPos(p);
                for _ in 0..drain_passes {
                    let next = catch_up_replica(&t, &source, &self.norm, &self.dict, hwm)?;
                    source.renew_retention_lease(lease, floor_with(&established, next))?;
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
                            // Renew at the FLOOR (min outstanding member cursor; the pin when no
                            // member is established — promotion/replica-only) — a TTL refresh that
                            // never lets a trim outrun a lagging member's pending tail.
                            source
                                .renew_retention_lease(lease, floor_with(&established, pinned))?;
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
                // Indexed so the lease floor can be computed over EVERY member (the one being
                // drained at its just-advanced cursor, the others at theirs).
                for i in 0..established.len() {
                    let mut converged = false;
                    for _ in 0..final_drain_cap.max(1) {
                        let hwm = established[i].2;
                        let next = {
                            let (_, t, _) = &established[i];
                            catch_up_replica(t, &source, &self.norm, &self.dict, hwm)?
                        };
                        established[i].2 = next;
                        source.renew_retention_lease(lease, floor_with(&established, next))?;
                        if next == hwm {
                            converged = true;
                            break;
                        }
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
                    // Same stale-fence hazard as a fresh member: a committed replica can carry a
                    // fence from a move that dropped it as this position's PRIMARY long ago (map
                    // edits can re-add it replica-first). Clear it — post-swap it must accept
                    // fan-out. Group writes are quiesced here (cp is fenced), so no write races
                    // the clear.
                    clear_stale_fence(&t, "reassign_group_and_move: retained member")?;
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
                // Structurally non-empty (built primary-first above); fail typed, never panic
                // (the no-`unwrap()`-in-library-code invariant).
                let Some((p_id, p_ep)) = members.next() else {
                    return Err(ShardError::Config(
                        "reassign_group_and_move: desired group has no primary".into(),
                    ));
                };
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
mod tests;
