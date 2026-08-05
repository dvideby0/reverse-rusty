//! `impl ClusterEngine` — data-moving live reassignment (ADR-090, `distributed` feature): tie a
//! committed shard→node assignment change to a physical data move, so a reassignment moves the bytes
//! AND routing follows — live and across a coordinator restart.
//!
//! Design: docs/design/clustering-and-scaling.md §9. Builds on ADR-086 (route by the committed map +
//! the boot guard) and ADR-044/043/048 (`execute_handoff` + `HandoffShard` + auto-unfence).
//!
//! ## The gap this closes
//! [`execute_handoff`](super::ClusterEngine::execute_handoff) moves a shard's data and flips live
//! routing but never touches the committed map; [`reassign_shard`](super::ClusterEngine::reassign_shard)
//! / [`rebalance`](super::ClusterEngine::rebalance) commit a new map but move no data. So on a
//! populated remote cluster routing could not follow a reassignment — the
//! [`route_topology`](super::route_topology) boot guard refuses a non-position-preserving committed
//! map (it would route a position to a node holding different data: a false negative). This module
//! composes the two into ONE operation that keeps committed-map ⟺ live-routing ⟺
//! physical-data-location consistent.
//!
//! ## Move-then-commit
//! [`reassign_and_move`](ClusterEngine::reassign_and_move) runs `execute_handoff` FIRST (peer-recover
//! target → fence source → drain to convergence → flip routing), THEN commits
//! `AssignShard{position, primary: to}`. The order is load-bearing for crash safety: in the window
//! after the flip but before the commit, the committed map still names `from`, which holds the
//! move-time snapshot and still SERVES READS (the source fence is write-only). This avoids ever
//! committing an empty target. It does not make an uncommitted flip restart-safe indefinitely:
//! later writes reach only `to`, so the stale map must be reconciled before a coordinator restart.
//! The opposite order (commit-then-move) is unsafe: a crash after the commit but before the move
//! points routing at an empty `to` — a silent false negative.
//!
//! ## Serialization & supported topology
//! **The supported topology is a single active coordinator** (the v1 deployment — Compose/Helm run
//! one coordinator). Every data-moving op here — plus the autoscaler-driven handoff
//! ([`drive_autoscaled_handoff`](super::ClusterEngine::drive_autoscaled_handoff)) and a raw
//! [`execute_handoff`](super::ClusterEngine::execute_handoff) — reserves its resolved endpoint
//! footprint in the busy-endpoint [`MoveLedger`](ledger::MoveLedger) for the whole move-then-commit
//! (ADR-095, replacing ADR-090's whole-coordinator `reassign_serial` mutex): moves sharing a node
//! serialize exactly as before (so two moves of one position — both reserving its committed
//! primary — cannot interleave their flip + commit and invert the map vs routing), while moves over
//! disjoint node sets may run in parallel (the opt-in
//! [`reconcile_with`](super::ClusterEngine::reconcile_with) /
//! [`rebalance_and_move_with`](super::ClusterEngine::rebalance_and_move_with) waves). A
//! compare-and-set on the committed primary just before the commit is a best-effort guard against a
//! *second* coordinator; making it truly atomic across horizontally-scaled stateless coordinators
//! needs a control-plane **conditional-propose** (compare-and-set `AssignShard`) primitive. ADR-092
//! adds the opt-in unattended reconcile loop, but it does not make the final proposal conditional;
//! the supported deployment therefore remains one active coordinator.
//! The whole module is `distributed`-gated; the in-process/default path never compiles it and is
//! byte-identical.

use std::time::Instant;

use tokio::runtime::Handle;

use crate::cluster::control::{NodeId, ShardAssignment};
use crate::cluster::shard::{Shard, ShardError};
use crate::events::{DurabilityOp, EngineEvent};

use super::distributed::handoff::{normalized_endpoint, HandoffRoute};
use super::ClusterEngine;

/// Group-aware (RF>1) data-moving reassignment — `rebalance_group_targets` +
/// `ClusterEngine::reassign_group_and_move` (ADR-094).
mod group;
pub(in crate::cluster::coordinator) use group::rebalance_group_targets;

/// The busy-endpoint move ledger + RAII ticket (ADR-095) — the per-node concurrency guard every
/// data-moving op reserves its footprint in.
mod ledger;
pub(in crate::cluster::coordinator) use ledger::MoveLedger;

/// Conflict-free wave planning + scoped-thread wave execution for multi-position sweeps
/// (ADR-095). Scheduling-only — safety lives in the ledger.
mod parallel;
pub(in crate::cluster::coordinator) use parallel::plan_waves;

/// Bounded retries of the `AssignShard` commit after a successful move, so a transient control-plane
/// blip (e.g. a real quorum mid-leader-change) doesn't strand a successful move uncommitted. The
/// in-memory control plane commits on the first attempt.
const COMMIT_ATTEMPTS: usize = 3;

/// Bounded plan→reserve→revalidate attempts (ADR-095): a move plans its endpoint footprint from a
/// committed read, reserves it in the ledger (possibly waiting out a conflicting in-flight move),
/// then re-reads to confirm neither the position's committed entry NOR any member's endpoint
/// resolution changed while it waited — a change (e.g. the conflicting move just committed this
/// very position, or a `register_node` replaced a member's addr) re-plans from the fresh state.
/// More than a couple of iterations means the map is churning under a storm of concurrent
/// commits; fail typed rather than spin.
const PLAN_ATTEMPTS: usize = 4;

/// Outcome of a [`ClusterEngine::reassign_and_move`] (ADR-090).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReassignOutcome {
    /// The live and committed authorities already agree on `to`: nothing moved and no new control
    /// proposal was necessary. The requested assignment is already committed.
    NoChange { position: u32, generation: u64 },
    /// The data moved to `to` AND the committed map now names it — fully consistent. `generation` is
    /// the position's new handoff/fence generation (the value
    /// [`handoff_generations`](super::ClusterEngine::handoff_generations) reports).
    Moved {
        position: u32,
        from: NodeId,
        to: NodeId,
        generation: u64,
    },
    /// Live routing already reached `to` because an earlier raw handoff or a
    /// prior move completed without committing. This invocation performed no
    /// second data copy; it reconciled the durable assignment to the attested
    /// live primary.
    Reconciled {
        position: u32,
        from: NodeId,
        to: NodeId,
        generation: u64,
    },
    /// Live routing reaches `to`, but committing the new owner FAILED (a control-plane error or a
    /// concurrent durable change). The live path remains exact, but the durable map is stale and a
    /// restart can resolve back to the old owner after newer writes reached `to`. A loud
    /// [`DurabilityFailure`](EngineEvent::DurabilityFailure) is emitted and the caller should retry
    /// promptly. On the single-target path, the retry attests `to` as the current live primary and
    /// commits it without copying again from the potentially stale committed owner. A durable
    /// intent protocol that generalizes this recovery across group moves is tracked separately.
    MovedButNotCommitted {
        position: u32,
        from: NodeId,
        to: NodeId,
        generation: u64,
        /// Whether this invocation performed the physical routing flip. `false`
        /// means it was retrying a pre-existing uncommitted live move.
        moved: bool,
    },
}

/// Outcome of a [`ClusterEngine::rebalance_and_move`] (ADR-090): which positions converged to their
/// committed targets, the first failure (if any — the sweep stops there, fail-forward / resume), and
/// the changed positions not yet attempted. A converged position may have moved physically or may
/// have reconciled a target that was already live.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RebalanceMoveReport {
    /// Positions whose desired primary committed this pass, including commit-only reconciliation
    /// when that target was already the attested live primary.
    pub moved: Vec<u32>,
    /// The lowest-position failure (with the error message); the sweep stopped at its wave (at the
    /// default `max_parallel_moves = 1` this is exactly "the first position that failed").
    pub failed: Option<(u32, String)>,
    /// Changed positions left for a re-run: everything after the failing wave, plus (at
    /// `max_parallel_moves ≥ 2`) any ADDITIONAL same-wave failure — those were attempted and rolled
    /// back cleanly (each emitted its own event), and a re-run retries them identically.
    pub not_attempted: Vec<u32>,
}

struct PlannedReassign<'a> {
    committed_from: NodeId,
    committed_from_endpoint: String,
    live_from_endpoint: String,
    target_endpoint: String,
    route: HandoffRoute,
    _ticket: ledger::MoveTicket<'a>,
}

impl ClusterEngine {
    /// Move shard `position`'s data to node `to` AND commit the new owner — the data-moving analogue
    /// of [`reassign_shard`](Self::reassign_shard) (ADR-090). Resolves `from` (the current committed
    /// primary) and `to` to endpoints from membership, then **move-then-commit**: run
    /// [`execute_handoff`](Self::execute_handoff) (peer-recover → fence → drain to convergence → flip
    /// routing) and only on success commit `AssignShard{position, primary: to}` (bare — the replica
    /// guard below rejects a replicated position, so the entry this replaces is replica-free).
    ///
    /// Fail-closed before a live flip and exact on the running coordinator after it:
    /// - a failed move propagates `Err` and commits nothing (the source auto-unfenced, routing + the
    ///   committed map untouched — a consistent rollback);
    /// - the commit is bounded-retried (a transient quorum blip self-heals; the in-memory control
    ///   plane commits first try); on persistent failure it returns
    ///   [`ReassignOutcome::MovedButNotCommitted`] and emits a loud durability event, keeping live
    ///   routing on the authoritative target. The durable map is then stale and restart-unsafe after
    ///   newer writes; a prompt re-run attests the live target and commits it without stale recopy.
    ///
    /// **Supported topology: a single active coordinator** (the v1 deployment). The busy-endpoint
    /// ledger (ADR-095) serializes this coordinator's CONFLICTING moves (any shared node — see the
    /// module docs) while disjoint moves may run in parallel; cross-coordinator atomicity of the
    /// primary check + commit needs a control-plane conditional-propose primitive (deferred — see
    /// the module docs).
    /// **A position with committed replicas is rejected** (a single-target move would de-replicate
    /// it) — the group-aware [`reassign_group_and_move`](Self::reassign_group_and_move) (ADR-094)
    /// moves a replicated position. Requires a
    /// handoff-capable cluster (built via [`connect_remote`](Self::connect_remote)); an in-process
    /// cluster has one node owning every position, so `from == to` short-circuits to a no-op.
    pub fn reassign_and_move(
        &self,
        position: usize,
        to: NodeId,
        handle: &Handle,
    ) -> Result<ReassignOutcome, ShardError> {
        match self.reassign_and_move_with_start(position, to, handle, None, || true)? {
            Some(outcome) => Ok(outcome),
            None => Err(ShardError::DeadlineExceeded),
        }
    }

    /// Deadline-aware start admission for one data-moving reassignment.
    ///
    /// The complete committed/live/target endpoint footprint is reserved and
    /// revalidated before `try_start` is called. `None` guarantees that neither
    /// data movement nor a control-state commit began. Once `try_start` returns
    /// true, the operation runs to its exact terminal move-and-commit outcome.
    pub fn reassign_and_move_until<F>(
        &self,
        position: usize,
        to: NodeId,
        handle: &Handle,
        deadline: Instant,
        try_start: F,
    ) -> Result<Option<ReassignOutcome>, ShardError>
    where
        F: FnOnce() -> bool,
    {
        self.reassign_and_move_with_start(position, to, handle, Some(deadline), try_start)
    }

    fn reassign_and_move_with_start<F>(
        &self,
        position: usize,
        to: NodeId,
        handle: &Handle,
        deadline: Option<Instant>,
        try_start: F,
    ) -> Result<Option<ReassignOutcome>, ShardError>
    where
        F: FnOnce() -> bool,
    {
        let pos = u32::try_from(position).map_err(|_| {
            ShardError::Config(format!(
                "reassign_and_move: shard position {position} exceeds the u32 wire/control limit"
            ))
        })?;
        // Plan → reserve → revalidate (ADR-095): resolve the move's endpoint footprint from a
        // committed read, reserve it in the busy-endpoint ledger — blocking until every
        // CONFLICTING in-flight move completes (the ADR-090 serialization, now per-node) — then
        // confirm the committed entry, endpoint resolution, AND current live primary did not change
        // while we waited. The live-primary check is essential after a raw handoff or an earlier
        // move whose commit failed: recovery must seed from the authoritative live owner, never the
        // potentially stale owner still named by the committed map.
        let mut planned: Option<PlannedReassign<'_>> = None;
        for _ in 0..PLAN_ATTEMPTS {
            let state = self.control_state()?;
            let assignment = state
                .assignments
                .iter()
                .find(|a| a.position == pos)
                .ok_or_else(|| {
                    ShardError::ControlPlane(format!(
                        "reassign_and_move: no committed assignment for shard position {position}"
                    ))
                })?;
            let from = assignment.primary;
            // A single-target move of a REPLICATED position is ambiguous and unsafe (ADR-090/094):
            // the move (`execute_handoff`) swaps the position to a SINGLE `RemoteShard` for `to`,
            // dropping the replica group, while the committed map would still advertise the old
            // replicas — so a failover could read a replica that no longer receives writes
            // (stale). The guard is PER-POSITION (the committed entry, not the cluster's
            // replication factor): a bare position on a replicated cluster is a plain single-shard
            // move, and a replicated position has the group-aware
            // [`reassign_group_and_move`](Self::reassign_group_and_move) (ADR-094).
            if !assignment.replicas.is_empty() {
                return Err(ShardError::Config(format!(
                    "reassign_and_move: shard position {position} has {} committed replica(s); a \
                     single-target move would de-replicate it — use reassign_group_and_move (or \
                     rebalance_and_move / reconcile, which dispatch group moves) instead (ADR-094)",
                    assignment.replicas.len()
                )));
            }

            // Resolve node ids → endpoints. Fail-closed (never silently skip an unroutable node —
            // that would route a title nowhere). Mirrors `resolve_topology`'s stance.
            let addr_of = |id: NodeId| -> Result<String, ShardError> {
                state
                    .nodes
                    .iter()
                    .find(|n| n.id == id)
                    .and_then(|n| n.addr.clone())
                    .ok_or_else(|| {
                        ShardError::ControlPlane(format!(
                            "reassign_and_move: node {} has no registered endpoint (addr)",
                            id.0
                        ))
                    })
            };
            let from_ep = addr_of(from)?;
            let tgt_ep = addr_of(to)?;
            let handoff = self.handoffs.get(position).ok_or_else(|| {
                ShardError::Config(format!(
                    "reassign_and_move: shard position {position} is not handoff-capable (the \
                     cluster was not built via connect_remote/connect_replicated)"
                ))
            })?;
            let live_ep = handoff.live_primary_endpoint().ok_or_else(|| {
                ShardError::Config(format!(
                    "reassign_and_move: shard position {position} has no live remote primary"
                ))
            })?;

            // Include the committed endpoint even when it differs from the live primary. Two
            // retries of the same uncommitted move then still share a ledger key, and GC/reassign
            // operations cannot reason about the stale durable owner concurrently.
            let footprint = [from_ep.as_str(), live_ep.as_str(), tgt_ep.as_str()];
            let ticket = match deadline {
                Some(deadline) => self.move_ledger.reserve_until(&footprint, deadline),
                None => Some(self.move_ledger.reserve(&footprint)),
            };
            let Some(ticket) = ticket else {
                return Ok(None);
            };

            // Revalidate the committed entry, both membership resolutions, and live routing while
            // the normalized endpoint reservation is held. Moving over a stale endpoint and then
            // committing the NodeId would make the next assignment-routed restart unsafe.
            let now = self.control_state()?;
            let addr_now = |id: NodeId| {
                now.nodes
                    .iter()
                    .find(|n| n.id == id)
                    .and_then(|n| n.addr.as_deref())
            };
            let entry_unchanged = now
                .assignments
                .iter()
                .find(|a| a.position == pos)
                .is_some_and(|a| a.primary == from && a.replicas.is_empty());
            let endpoint_is = |actual: Option<&str>, planned: &str| {
                actual.is_some_and(|actual| {
                    normalized_endpoint(actual) == normalized_endpoint(planned)
                })
            };
            let eps_unchanged =
                endpoint_is(addr_now(from), &from_ep) && endpoint_is(addr_now(to), &tgt_ep);
            let live_now = handoff.live_primary_endpoint().ok_or_else(|| {
                ShardError::Config(format!(
                    "reassign_and_move: shard position {position} lost its live remote primary"
                ))
            })?;
            let live_unchanged = normalized_endpoint(&live_now) == normalized_endpoint(&live_ep);
            if entry_unchanged && eps_unchanged && live_unchanged {
                let route = self.validate_handoff_route(position, &live_ep, &tgt_ep)?;
                planned = Some(PlannedReassign {
                    committed_from: from,
                    committed_from_endpoint: from_ep,
                    live_from_endpoint: live_ep,
                    target_endpoint: tgt_ep,
                    route,
                    _ticket: ticket,
                });
                break;
            }
            // Durable or live routing changed while waiting: the ticket drops here and the next
            // iteration re-plans from the fresh state.
        }
        let Some(PlannedReassign {
            committed_from: from,
            committed_from_endpoint: from_ep,
            live_from_endpoint: live_ep,
            target_endpoint: tgt_ep,
            route,
            _ticket,
        }) = planned
        else {
            return Err(ShardError::ControlPlane(format!(
                "reassign_and_move: the committed assignment or live primary for shard position \
                 {position} kept changing while planning ({PLAN_ATTEMPTS} attempts); retry once \
                 routing stops churning"
            )));
        };
        if !try_start() {
            return Ok(None);
        }

        let (generation, moved) = match route {
            HandoffRoute::AlreadyAtTarget { generation } => (generation, false),
            HandoffRoute::Move => (
                self.execute_handoff_inner(position, &live_ep, &tgt_ep, handle)?,
                true,
            ),
        };
        // Live routing now reaches `to` (either before this invocation or after its move).
        // COMPARE-AND-SET before committing: confirm the durable primary is still `from`.
        let now = self.control_state()?;
        let still_from = now
            .assignments
            .iter()
            .find(|a| a.position == pos)
            .is_some_and(|a| a.primary == from && a.replicas.is_empty());
        if !still_from {
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::ReplicaDesync,
                detail: format!(
                    "reassign_and_move made shard {position} live on node {} but the committed \
                     primary changed under it (a concurrent reassign); not overwriting the map. \
                     Re-run to reconcile.",
                    to.0
                ),
                error: "committed assignment changed during reassign".into(),
            });
            return Ok(Some(ReassignOutcome::MovedButNotCommitted {
                position: pos,
                from,
                to,
                generation,
                moved,
            }));
        }

        // A committed target still needs a physical repair when raw handoff left live routing
        // elsewhere, but it does not need another control proposal afterward. This return comes
        // only after the durable-primary recheck above, so a second coordinator cannot change the
        // assignment during the move and still receive an acknowledged result. A different NodeId
        // aliasing the same live endpoint DOES need a commit so the requested identity becomes
        // durable.
        if from == to && normalized_endpoint(&from_ep) == normalized_endpoint(&tgt_ep) {
            let outcome = if moved {
                ReassignOutcome::Moved {
                    position: pos,
                    from,
                    to,
                    generation,
                }
            } else {
                ReassignOutcome::NoChange {
                    position: pos,
                    generation,
                }
            };
            return Ok(Some(outcome));
        }

        // COMMIT (move-then-commit): name the new owner. The entry's replica set is provably empty
        // here (the replica guard rejected a replicated position at plan time and the post-reserve
        // revalidation re-checked it), so the committed entry is written bare — an `AssignShard`
        // replaces the whole entry. Bounded-retry the proposal so a transient control-plane blip
        // (e.g. a real quorum mid-leader-change) doesn't strand a successful move uncommitted; the
        // in-memory control plane commits on the first attempt (no behavior change).
        let mut last_err: Option<ShardError> = None;
        for attempt in 0..COMMIT_ATTEMPTS {
            match self.reassign_shard(ShardAssignment {
                position: pos,
                primary: to,
                replicas: Vec::new(),
            }) {
                Ok(()) => {
                    let outcome = if moved {
                        ReassignOutcome::Moved {
                            position: pos,
                            from,
                            to,
                            generation,
                        }
                    } else {
                        ReassignOutcome::Reconciled {
                            position: pos,
                            from,
                            to,
                            generation,
                        }
                    };
                    return Ok(Some(outcome));
                }
                Err(e) => {
                    last_err = Some(e);
                    if attempt + 1 < COMMIT_ATTEMPTS {
                        std::thread::sleep(std::time::Duration::from_millis(50));
                    }
                }
            }
        }
        // Persistent commit failure (only reachable with a real quorum that cannot accept the
        // proposal; the in-memory backend never gets here). The move already succeeded, so `to` is
        // authoritative: KEEP live routing on it and surface a loud event. The durable map remains
        // stale. After any newer write reaches `to`, restarting an assignment-routed coordinator
        // before reconciliation can return to a stale source. A retry therefore attests the current
        // live primary and commits it without copying from the old durable owner. A durable move
        // intent / conditional-propose protocol is the remaining way to close this restart window.
        self.emit(EngineEvent::DurabilityFailure {
            op: DurabilityOp::ReplicaDesync,
            detail: format!(
                "reassign_and_move made shard {position} live on node {} but committing the new \
                 owner failed after {COMMIT_ATTEMPTS} attempts; live routing stays on node {} and \
                 the committed map still names node {}. Re-run promptly to reconcile before a \
                 coordinator restart.",
                to.0, to.0, from.0
            ),
            error: last_err.map(|e| e.to_string()).unwrap_or_default(),
        });
        Ok(Some(ReassignOutcome::MovedButNotCommitted {
            position: pos,
            from,
            to,
            generation,
            moved,
        }))
    }

    /// Data-moving analogue of [`rebalance`](Self::rebalance) (ADR-090/094): recompute the desired
    /// HRW shard→node map at replication factor `rf`, then move each position whose **group**
    /// (primary or replica set) changes — sequentially, in position order (=
    /// [`rebalance_and_move_with`](Self::rebalance_and_move_with) at `max_parallel_moves = 1`, the
    /// byte-identical default). Dispatch is by SHAPE: a bare→bare change runs the proven
    /// single-shard [`reassign_and_move`](Self::reassign_and_move) byte-identically (the RF=1
    /// path); any change touching replicas runs the group-aware
    /// [`reassign_group_and_move`](Self::reassign_group_and_move) (ADR-094), so an `rf > 1` sweep
    /// creates the replica placements it plans — closing the ADR-090 RF>1 deferral.
    pub fn rebalance_and_move(
        &self,
        rf: usize,
        handle: &Handle,
    ) -> Result<RebalanceMoveReport, ShardError> {
        self.rebalance_and_move_with(rf, 1, handle)
    }

    /// [`rebalance_and_move`](Self::rebalance_and_move) with wave parallelism (ADR-095): the
    /// changed positions are partitioned into conflict-free waves
    /// ([`plan_waves`](super::reassign::plan_waves) — moves sharing any node serialize, per the
    /// chained-reshuffle constraint: position `p`: F→T while `q`: T→U would make T a handoff
    /// target and a fenced source at once) and up to `max_parallel_moves` disjoint moves run
    /// concurrently per wave. `max_parallel_moves <= 1` is the sequential path, byte-identical to
    /// the pre-ADR-095 sweep. Safety never rests on the planner: every move still reserves its own
    /// footprint in the busy-endpoint ledger.
    ///
    /// Stops at the first failing WAVE (fail-forward / resume — at the default this is exactly
    /// "stops on the first failure") and returns a [`RebalanceMoveReport`]; already-moved positions
    /// are each consistent, so a partial rebalance is a valid resumable state, never a false
    /// negative. A hard pre-flight error (no nodes, control-plane read failure) is an `Err`;
    /// per-position failures land in the report.
    pub fn rebalance_and_move_with(
        &self,
        rf: usize,
        max_parallel_moves: usize,
        handle: &Handle,
    ) -> Result<RebalanceMoveReport, ShardError> {
        let state = self.control_state()?;
        if state.nodes.is_empty() {
            return Err(ShardError::ControlPlane(
                "rebalance_and_move: the cluster has no nodes to place shards on".into(),
            ));
        }
        // Positions whose GROUP moves (a data move), in deterministic position order, partitioned
        // into conflict-free waves (singletons in target order at the default parallelism).
        let targets = rebalance_group_targets(&state, rf);
        let waves = plan_waves(&state, &targets, max_parallel_moves);

        let mut report = RebalanceMoveReport::default();
        for (wi, wave) in waves.iter().enumerate() {
            let mut wave_failed = false;
            for (pos, outcome) in self.execute_move_wave(&state, &targets, wave, handle) {
                match outcome {
                    Ok(ReassignOutcome::Moved { .. } | ReassignOutcome::Reconciled { .. }) => {
                        report.moved.push(pos);
                    }
                    // Resolved equal under us (a concurrent move already placed it): not a failure.
                    Ok(ReassignOutcome::NoChange { .. }) => {}
                    Ok(ReassignOutcome::MovedButNotCommitted { .. }) => {
                        // The data moved but its commit failed (event already emitted). Stop after
                        // this wave so the durable map stays reconcilable rather than piling more
                        // moves on top.
                        wave_failed = true;
                        if report.failed.is_none() {
                            report.failed = Some((
                                pos,
                                "data moved but committing the new owner failed (see the emitted \
                                 event); stopped the rebalance so the durable map stays \
                                 reconcilable — re-run to resume"
                                    .into(),
                            ));
                        } else {
                            report.not_attempted.push(pos);
                        }
                    }
                    Err(e) => {
                        // A clean move failure rolled this position fully back (routing + map
                        // unchanged); already-moved positions stay consistent. Stop after this
                        // wave and report for a resume.
                        wave_failed = true;
                        if report.failed.is_none() {
                            report.failed = Some((pos, e.to_string()));
                        } else {
                            report.not_attempted.push(pos);
                        }
                    }
                }
            }
            if wave_failed {
                report
                    .not_attempted
                    .extend(waves[wi + 1..].iter().flatten().map(|&i| targets[i].0));
                break;
            }
        }
        // Position-sorted regardless of wave completion order (a no-op at the sequential default,
        // where waves are singletons in target order).
        report.moved.sort_unstable();
        report.not_attempted.sort_unstable();
        Ok(report)
    }
}

#[cfg(test)]
mod tests;
