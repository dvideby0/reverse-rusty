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
//! ## Move-then-commit (the zero-FN ordering)
//! [`reassign_and_move`](ClusterEngine::reassign_and_move) runs `execute_handoff` FIRST (peer-recover
//! target → fence source → drain to convergence → flip routing), THEN commits
//! `AssignShard{position, primary: to}`. The order is load-bearing for crash safety: in the window
//! after the flip but before the commit, the committed map still names `from`, which still holds the
//! data and still SERVES READS (the source fence is write-only). So a coordinator crash → restart
//! resolving the committed map lands on a reads-serving, data-holding node — zero false negatives.
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
//! needs a control-plane **conditional-propose** (compare-and-set `AssignShard`) primitive — which,
//! with an unattended assignment-watch → re-point controller, is the deferred follow-on (ADR-090).
//! The whole module is `distributed`-gated; the in-process/default path never compiles it and is
//! byte-identical.

use tokio::runtime::Handle;

use crate::cluster::control::{NodeId, ShardAssignment};
use crate::cluster::shard::ShardError;
use crate::events::{DurabilityOp, EngineEvent};

use super::ClusterEngine;

/// Group-aware (RF>1) data-moving reassignment — `rebalance_group_targets` +
/// `ClusterEngine::reassign_group_and_move` (ADR-094).
mod group;
pub(in crate::cluster::coordinator) use group::rebalance_group_targets;

/// The busy-endpoint move ledger + RAII ticket (ADR-095) — the per-node concurrency guard every
/// data-moving op reserves its footprint in.
mod ledger;
pub(in crate::cluster::coordinator) use ledger::MoveLedger;

/// Bounded retries of the `AssignShard` commit after a successful move, so a transient control-plane
/// blip (e.g. a real quorum mid-leader-change) doesn't strand a successful move uncommitted. The
/// in-memory control plane commits on the first attempt.
const COMMIT_ATTEMPTS: usize = 3;

/// Bounded plan→reserve→revalidate attempts (ADR-095): a move plans its endpoint footprint from a
/// committed read, reserves it in the ledger (possibly waiting out a conflicting in-flight move),
/// then re-reads to confirm the position's committed entry did not change while it waited — a
/// change (e.g. the conflicting move just committed this very position) re-plans from the fresh
/// state. More than a couple of iterations means the map is churning under a storm of concurrent
/// commits; fail typed rather than spin.
const PLAN_ATTEMPTS: usize = 4;

/// Outcome of a [`ClusterEngine::reassign_and_move`] (ADR-090).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReassignOutcome {
    /// The position already lives on `to` (or `from`/`to` resolve to one endpoint): nothing moved,
    /// nothing committed. The idempotent no-op — e.g. re-running a completed reassign.
    NoChange { position: u32 },
    /// The data moved to `to` AND the committed map now names it — fully consistent. `generation` is
    /// the position's new handoff/fence generation (the value
    /// [`handoff_generations`](super::ClusterEngine::handoff_generations) reports).
    Moved {
        position: u32,
        from: NodeId,
        to: NodeId,
        generation: u64,
    },
    /// The data moved to `to` and live routing flipped, but committing the new owner FAILED (a
    /// control-plane error, or a concurrent change moved the position under us). **Zero-FN safe** —
    /// the committed map still names `from`, which holds the data and serves reads — but the durable
    /// map is stale: a [`DurabilityFailure`](EngineEvent::DurabilityFailure) event was emitted and
    /// the caller should RETRY. Re-running `reassign_and_move` is idempotent (a fenced source still
    /// serves the read-only recovery RPCs, so the retry re-converges the already-populated target and
    /// re-commits).
    MovedButNotCommitted {
        position: u32,
        from: NodeId,
        to: NodeId,
        generation: u64,
    },
}

/// Outcome of a [`ClusterEngine::rebalance_and_move`] (ADR-090): which positions moved + committed,
/// the first failure (if any — the loop stops there, fail-forward / resume), and the changed
/// positions not yet attempted. Each already-moved position is individually consistent, so a partial
/// rebalance is a valid (resumable) state, never a false negative.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RebalanceMoveReport {
    /// Positions whose primary moved AND committed this pass.
    pub moved: Vec<u32>,
    /// The first position that failed (with the error message); the loop stopped here.
    pub failed: Option<(u32, String)>,
    /// Changed positions after the failure — not attempted, left for a re-run.
    pub not_attempted: Vec<u32>,
}

impl ClusterEngine {
    /// Move shard `position`'s data to node `to` AND commit the new owner — the data-moving analogue
    /// of [`reassign_shard`](Self::reassign_shard) (ADR-090). Resolves `from` (the current committed
    /// primary) and `to` to endpoints from membership, then **move-then-commit**: run
    /// [`execute_handoff`](Self::execute_handoff) (peer-recover → fence → drain to convergence → flip
    /// routing) and only on success commit `AssignShard{position, primary: to}` (bare — the replica
    /// guard below rejects a replicated position, so the entry this replaces is replica-free).
    ///
    /// Fail-closed and zero-FN at every step (see the module docs for the crash-window argument):
    /// - a failed move propagates `Err` and commits nothing (the source auto-unfenced, routing + the
    ///   committed map untouched — a consistent rollback);
    /// - the commit is bounded-retried (a transient quorum blip self-heals; the in-memory control
    ///   plane commits first try); on persistent failure it returns
    ///   [`ReassignOutcome::MovedButNotCommitted`] (not `Err` — the data did move) and emits a loud
    ///   durability event, keeping live routing on the authoritative target (no acked write is lost on
    ///   the live path) while the committed map still names the reads-serving source — so a re-run
    ///   reconciles the durable map (idempotent).
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
        let pos = position as u32;
        // Plan → reserve → revalidate (ADR-095): resolve the move's endpoint footprint from a
        // committed read, reserve it in the busy-endpoint ledger — blocking until every
        // CONFLICTING in-flight move completes (the ADR-090 serialization, now per-node) — then
        // confirm the position's committed entry did not change while we waited (the conflicting
        // move may have committed this very position). A change re-plans from the fresh state
        // (bounded); the pre-commit CAS below stays the final backstop.
        let mut planned: Option<(NodeId, String, String, ledger::MoveTicket<'_>)> = None;
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

            // Already in place (same node), or two ids resolving to one endpoint: nothing to move
            // or commit (the idempotent no-op, e.g. re-running a completed reassign).
            if from == to || from_ep == tgt_ep {
                return Ok(ReassignOutcome::NoChange { position: pos });
            }

            let ticket = self.move_ledger.reserve(&[from_ep.as_str(), tgt_ep.as_str()]);
            let now = self.control_state()?;
            let unchanged = now
                .assignments
                .iter()
                .find(|a| a.position == pos)
                .is_some_and(|a| a.primary == from && a.replicas.is_empty());
            if unchanged {
                planned = Some((from, from_ep, tgt_ep, ticket));
                break;
            }
            // The entry changed while we waited on the ledger: the ticket drops here and the next
            // iteration re-plans from the fresh committed state.
        }
        let Some((from, from_ep, tgt_ep, _ticket)) = planned else {
            return Err(ShardError::ControlPlane(format!(
                "reassign_and_move: the committed assignment for shard position {position} kept \
                 changing while planning the move ({PLAN_ATTEMPTS} attempts); retry once the map \
                 stops churning"
            )));
        };

        // MOVE first. On failure this auto-unfences the source and leaves routing + the committed map
        // untouched (consistent rollback) — propagate it; nothing was committed. The `_inner` variant
        // skips the ledger re-reservation (our ticket already covers {from, to}).
        let generation = self.execute_handoff_inner(position, &from_ep, &tgt_ep, handle)?;

        // The move already flipped LIVE routing to `to`. COMPARE-AND-SET before committing: confirm
        // the committed primary is still `from`. If a concurrent op moved this position under us
        // (only possible across coordinators — the guard serializes this one), do NOT overwrite its
        // commit. Either way the data is on `to` and routing serves it; the durable map just isn't
        // ours to claim.
        let now = self.control_state()?;
        let still_from = now
            .assignments
            .iter()
            .find(|a| a.position == pos)
            .is_some_and(|a| a.primary == from);
        if !still_from {
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::ReplicaDesync,
                detail: format!(
                    "reassign_and_move moved shard {position} to node {} and flipped routing, but the \
                     committed primary changed under it (a concurrent reassign); not overwriting the \
                     committed map. Re-run to reconcile.",
                    to.0
                ),
                error: "committed assignment changed during a data-moving reassign".into(),
            });
            return Ok(ReassignOutcome::MovedButNotCommitted {
                position: pos,
                from,
                to,
                generation,
            });
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
                    return Ok(ReassignOutcome::Moved {
                        position: pos,
                        from,
                        to,
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
        // Persistent commit failure (only reachable with a real quorum that has lost majority — a
        // cluster-down condition; the in-memory backend never gets here). The move already succeeded,
        // so `to` is authoritative: KEEP live routing on it (so no acked write is lost on the live
        // path — routing on `to`, which holds every acked write, is never a false negative), and
        // surface a loud event. The committed map still names the reads-serving source, so the v1
        // single-coordinator deployment stays zero-FN; the operator (or the autoscaler's next tick)
        // re-runs to reconcile the durable map (idempotent — a fenced source still serves the
        // read-only recovery RPCs). The narrow residual — a coordinator restart while the quorum is
        // still down, before that reconcile — is the boundary the deferred assignment-watch controller
        // closes (with a conditional-propose / 2-phase commit primitive).
        self.emit(EngineEvent::DurabilityFailure {
            op: DurabilityOp::ReplicaDesync,
            detail: format!(
                "reassign_and_move moved shard {position} to node {} and flipped routing, but \
                 committing the new owner failed after {COMMIT_ATTEMPTS} attempts; live routing stays \
                 on node {} (which holds every acked write) and the committed map still names the \
                 reads-serving source node {} — re-run to reconcile the durable map (idempotent).",
                to.0, to.0, from.0
            ),
            error: last_err.map(|e| e.to_string()).unwrap_or_default(),
        });
        Ok(ReassignOutcome::MovedButNotCommitted {
            position: pos,
            from,
            to,
            generation,
        })
    }

    /// Data-moving analogue of [`rebalance`](Self::rebalance) (ADR-090/094): recompute the desired
    /// HRW shard→node map at replication factor `rf`, then move each position whose **group**
    /// (primary or replica set) changes — **sequentially**, in position order. Dispatch is by
    /// SHAPE: a bare→bare change runs the proven single-shard
    /// [`reassign_and_move`](Self::reassign_and_move) byte-identically (the RF=1 path); any change
    /// touching replicas runs the group-aware
    /// [`reassign_group_and_move`](Self::reassign_group_and_move) (ADR-094), so an `rf > 1` sweep
    /// creates the replica placements it plans — closing the ADR-090 RF>1 deferral.
    ///
    /// Sequential is required: an HRW reshuffle can chain (position `p`: F→T while position `q`: T→U),
    /// and running them concurrently would have T serve as a handoff target and source at once — the
    /// drain-to-convergence proof assumes a quiescent, fenced source. Stops on the first failure and
    /// returns a [`RebalanceMoveReport`] (fail-forward / resume — already-moved positions are each
    /// consistent, so a partial rebalance is a valid resumable state, never a false negative). A hard
    /// pre-flight error (no nodes, control-plane read failure) is an `Err`; per-position failures land
    /// in the report.
    pub fn rebalance_and_move(
        &self,
        rf: usize,
        handle: &Handle,
    ) -> Result<RebalanceMoveReport, ShardError> {
        let state = self.control_state()?;
        if state.nodes.is_empty() {
            return Err(ShardError::ControlPlane(
                "rebalance_and_move: the cluster has no nodes to place shards on".into(),
            ));
        }
        // Positions whose GROUP moves (a data move), in deterministic position order.
        let targets = rebalance_group_targets(&state, rf);

        let mut report = RebalanceMoveReport::default();
        for (i, (pos, desired)) in targets.iter().enumerate() {
            let stop = |report: &mut RebalanceMoveReport, reason: String| {
                report.failed = Some((*pos, reason));
                report.not_attempted = targets[i + 1..].iter().map(|(p, _)| *p).collect();
            };
            // Dispatch by shape: both committed and desired bare ⇒ the proven single-shard path;
            // anything touching replicas ⇒ the group move.
            let committed_bare = state
                .assignments
                .iter()
                .find(|a| a.position == *pos)
                .is_none_or(|a| a.replicas.is_empty());
            let outcome = if committed_bare && desired.replicas.is_empty() {
                self.reassign_and_move(*pos as usize, desired.primary, handle)
            } else {
                self.reassign_group_and_move(*pos as usize, desired, handle)
            };
            match outcome {
                Ok(ReassignOutcome::Moved { .. }) => report.moved.push(*pos),
                // Resolved equal under us (a concurrent move already placed it): not a failure.
                Ok(ReassignOutcome::NoChange { .. }) => {}
                Ok(ReassignOutcome::MovedButNotCommitted { .. }) => {
                    // The data moved but its commit failed (event already emitted). Stop so the
                    // durable map stays reconcilable rather than piling more moves on top.
                    stop(
                        &mut report,
                        "data moved but committing the new owner failed (see the emitted event); \
                         stopped the rebalance so the durable map stays reconcilable — re-run to resume"
                            .into(),
                    );
                    return Ok(report);
                }
                Err(e) => {
                    // A clean move failure rolled this position fully back (routing + map unchanged);
                    // already-moved positions stay consistent. Stop and report for a resume.
                    stop(&mut report, e.to_string());
                    return Ok(report);
                }
            }
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::allocator;
    use crate::cluster::control::{ClusterState, NodeDescriptor, NodeRole};

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

    /// A map already equal to the HRW desired placement moves nothing (the idempotent re-run / the
    /// single-node default ⇒ a no-op rebalance).
    #[test]
    fn no_targets_when_already_balanced() {
        let nodes = vec![node(1), node(2), node(3)];
        let node_ids: Vec<NodeId> = nodes.iter().map(|n| n.id).collect();
        let num_shards = 8u32;
        let desired = allocator::plan_assignments(&node_ids, num_shards, 1);
        let st = state_with(nodes, num_shards, desired);
        assert!(
            rebalance_group_targets(&st, 1).is_empty(),
            "an already-HRW-balanced map needs no moves"
        );
    }

    /// No members ⇒ nothing to place ⇒ no targets (the caller turns this into a fail-closed error).
    #[test]
    fn empty_membership_yields_no_targets() {
        let st = state_with(Vec::new(), 4, Vec::new());
        assert!(rebalance_group_targets(&st, 1).is_empty());
    }

    /// Targets are exactly the positions whose PRIMARY changes, named with the HRW desired owner,
    /// sorted ascending and one per position; unmoved positions keep their current primary.
    #[test]
    fn targets_are_changed_primaries_sorted() {
        let nodes = vec![node(1), node(2), node(3)];
        let num_shards = 8u32;
        // Current: every position on node 1. HRW over {1,2,3} pulls ~2/3 of them off node 1.
        let current: Vec<ShardAssignment> = (0..num_shards)
            .map(|p| ShardAssignment {
                position: p,
                primary: NodeId(1),
                replicas: Vec::new(),
            })
            .collect();
        let st = state_with(nodes.clone(), num_shards, current);
        let targets: Vec<(u32, NodeId)> = rebalance_group_targets(&st, 1)
            .into_iter()
            .map(|(p, d)| (p, d.primary))
            .collect();
        assert!(
            !targets.is_empty(),
            "HRW over 3 nodes must move some positions off node 1"
        );

        // Sorted ascending, one per position.
        let positions: Vec<u32> = targets.iter().map(|(p, _)| *p).collect();
        let mut sorted = positions.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(positions, sorted, "targets sorted by position, no dups");

        // Each target names the HRW desired primary and is a genuine change off node 1.
        let node_ids: Vec<NodeId> = nodes.iter().map(|n| n.id).collect();
        let desired = allocator::plan_assignments(&node_ids, num_shards, 1);
        for (pos, to) in &targets {
            let d = desired.iter().find(|a| a.position == *pos).unwrap();
            assert_eq!(d.primary, *to, "target names the HRW desired primary");
            assert_ne!(*to, NodeId(1), "only changed primaries are targets");
        }
        // Positions absent from targets kept their current primary (node 1).
        for a in &desired {
            if !targets.iter().any(|(p, _)| *p == a.position) {
                assert_eq!(a.primary, NodeId(1), "unmoved positions stayed on node 1");
            }
        }
    }

    /// Targets are planned only over data nodes WITH an address: the addr-less control-plane manager
    /// (`NodeId(0)`) and any addr-less data node are never picked as a move destination (HRW must not
    /// produce a move-to-the-manager that then fails on the missing endpoint).
    #[test]
    fn excludes_manager_and_addrless_nodes() {
        let manager = NodeDescriptor {
            id: NodeId(0),
            addr: None,
            role: NodeRole::Manager,
        };
        let addrless_data = NodeDescriptor {
            id: NodeId(9),
            addr: None,
            role: NodeRole::Data,
        };
        let nodes = vec![manager, node(1), node(2), addrless_data];
        let num_shards = 8u32;
        let current: Vec<ShardAssignment> = (0..num_shards)
            .map(|p| ShardAssignment {
                position: p,
                primary: NodeId(1),
                replicas: Vec::new(),
            })
            .collect();
        let st = state_with(nodes, num_shards, current);
        let targets: Vec<(u32, NodeId)> = rebalance_group_targets(&st, 1)
            .into_iter()
            .map(|(p, d)| (p, d.primary))
            .collect();
        assert!(
            !targets.is_empty(),
            "HRW over the 2 eligible data nodes still moves some positions off node 1"
        );
        for (_pos, to) in &targets {
            assert!(
                *to == NodeId(1) || *to == NodeId(2),
                "only addr'd data nodes are targets, got {to:?}"
            );
        }
    }

    /// An `rf > 1` sweep is no longer rejected up front (ADR-094 replaces the ADR-092-landing
    /// guard): `rebalance_and_move(2, ..)` computes GROUP targets and dispatches each replicated
    /// placement to `reassign_group_and_move`. Here the first group move fails cleanly +
    /// network-free (the committed primary is the addr-less manager) and the sweep stops-on-first
    /// as documented — proving the rf=2 request flows down the group path rather than erroring
    /// the whole call.
    #[test]
    fn rebalance_and_move_rf2_dispatches_group_moves() {
        use crate::cluster::coordinator::{ClusterConfig, ClusterEngine};
        use crate::normalize::Normalizer;

        let queries: Vec<(u64, String)> = vec![(1, "+nike +shoe".into()), (2, "+sony +tv".into())];
        let cluster = ClusterEngine::build(
            Normalizer::default_vocab().expect("vocab"),
            &ClusterConfig {
                num_shards: 2,
                ..ClusterConfig::default()
            },
            &queries,
        )
        .expect("in-process bare cluster");
        for id in [1u64, 2] {
            cluster
                .register_node(NodeDescriptor {
                    id: NodeId(id),
                    addr: Some(format!("http://127.0.0.1:{id}")),
                    role: NodeRole::Data,
                })
                .expect("register node");
        }
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        let report = cluster
            .rebalance_and_move(2, rt.handle())
            .expect("rf=2 sweep runs (per-position failures land in the report)");
        let (pos, msg) = report
            .failed
            .as_ref()
            .expect("the first group move fails loudly");
        assert!(
            msg.contains("reassign_group_and_move"),
            "an rf=2 target with replicas dispatches to the GROUP move: {msg}"
        );
        assert!(
            report.moved.is_empty(),
            "nothing moved (the manager primary has no endpoint): {report:?}"
        );
        assert!(
            report.not_attempted.iter().all(|p| p != pos),
            "stop-on-first: the failed position is not also listed as not-attempted"
        );
    }
}
