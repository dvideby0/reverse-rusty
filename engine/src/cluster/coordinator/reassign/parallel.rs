//! Wave-parallel execution of multi-position moves (ADR-095, `distributed` feature): partition a
//! sweep's targets ([`rebalance_group_targets`](super::rebalance_group_targets) output) into
//! conflict-free WAVES and run each wave's moves on scoped threads, so a large
//! `reconcile`/`rebalance_and_move` sweep is bounded by its longest conflict CHAIN rather than the
//! sum of every move.
//!
//! ## Safety vs scheduling — the load-bearing split
//! The planner here is **scheduling-only**. Every individual move still plans, reserves its own
//! endpoint footprint in the busy-endpoint [`MoveLedger`](super::ledger::MoveLedger), and
//! revalidates under its ticket — so a stale or even WRONG wave plan degrades to two moves
//! briefly blocking each other on the ledger, never to an unguarded conflict. Correctness never
//! rests on this file; wall-clock does.
//!
//! ## The default path is the sequential path
//! `max_parallel = 1` yields singleton waves in target (position) order, and a singleton wave runs
//! INLINE on the calling thread — zero threads spawned, the exact sequential code path (not merely
//! an equivalent one). Parallelism is the opt-in (`max_parallel_moves ≥ 2`).
//!
//! ## Threading model
//! Waves run on **plain scoped `std` threads** (`std::thread::scope`), each calling the same sync
//! move entry points, which bridge async via the cluster's tokio `Handle` — the safe
//! plain-thread `block_on` case. Deliberately NOT rayon: a move is a long-running, blocking
//! operation (an `O(corpus)` copy), and parking it on the shared rayon pool would starve the
//! percolate fan-out (and trip the block_on-in-rayon hazard the gRPC oracle guards against).

use std::collections::HashSet;

use tokio::runtime::Handle;

use crate::cluster::control::{ClusterState, ShardAssignment};
use crate::cluster::shard::ShardError;
use crate::events::{DurabilityOp, EngineEvent};

use super::{ClusterEngine, ReassignOutcome};

/// The endpoints a move of `desired` will touch — the scheduling analogue of the reservation the
/// move itself takes: the position's COMMITTED primary (the fenced recovery source) plus every
/// desired member (fresh and retained; a group move establishes/installs each of them). Dropped
/// members (`C ∖ D` replicas) are never contacted, so they are not part of the footprint. An
/// unresolvable member (no committed entry / no registered addr) is simply omitted — that move
/// fails loudly pre-network inside `reassign_and_move`/`reassign_group_and_move`, exactly like
/// the sequential sweep; omitting it here only affects scheduling.
pub(in crate::cluster::coordinator) fn move_footprint(
    state: &ClusterState,
    desired: &ShardAssignment,
) -> Vec<String> {
    let addr_of = |id| {
        state
            .nodes
            .iter()
            .find(|n| n.id == id)
            .and_then(|n| n.addr.clone())
    };
    let mut eps: Vec<String> = Vec::with_capacity(2 + desired.replicas.len());
    if let Some(committed) = state
        .assignments
        .iter()
        .find(|a| a.position == desired.position)
    {
        eps.extend(addr_of(committed.primary));
    }
    eps.extend(addr_of(desired.primary));
    for r in &desired.replicas {
        eps.extend(addr_of(*r));
    }
    eps.sort_unstable();
    eps.dedup();
    eps
}

/// Greedy conflict-free wave partition over a sweep's targets: scan the not-yet-scheduled targets
/// in position order, admitting each into the current wave iff its footprint is disjoint from
/// every admitted footprint and the wave is under `max_parallel`; the rest defer to later waves.
/// Deterministic (pure over the inputs, position order preserved); `max_parallel <= 1` yields
/// singleton waves in exact target order — the sequential default. Returns waves of INDICES into
/// `targets`.
pub(in crate::cluster::coordinator) fn plan_waves(
    state: &ClusterState,
    targets: &[(u32, ShardAssignment)],
    max_parallel: usize,
) -> Vec<Vec<usize>> {
    let max_parallel = max_parallel.max(1);
    let footprints: Vec<Vec<String>> = targets
        .iter()
        .map(|(_, d)| move_footprint(state, d))
        .collect();
    let mut waves: Vec<Vec<usize>> = Vec::new();
    let mut remaining: Vec<usize> = (0..targets.len()).collect();
    while !remaining.is_empty() {
        let mut wave: Vec<usize> = Vec::new();
        let mut busy: HashSet<&str> = HashSet::new();
        let mut deferred: Vec<usize> = Vec::new();
        for &i in &remaining {
            let fp = &footprints[i];
            if wave.len() < max_parallel && fp.iter().all(|e| !busy.contains(e.as_str())) {
                wave.push(i);
                busy.extend(fp.iter().map(String::as_str));
            } else {
                deferred.push(i);
            }
        }
        waves.push(wave);
        remaining = deferred;
    }
    waves
}

impl ClusterEngine {
    /// Dispatch one sweep target by SHAPE (the rule both sweeps share): committed and desired both
    /// bare ⇒ the proven single-shard [`reassign_and_move`](Self::reassign_and_move)
    /// (byte-identical to the RF=1 sweep); anything touching replicas ⇒ the group-aware
    /// [`reassign_group_and_move`](Self::reassign_group_and_move) (ADR-094). `state` is the
    /// sweep's planning read — the move itself re-reads and revalidates under its ledger ticket,
    /// so a stale shape decision at worst dispatches a move that resolves to `NoChange`/re-plans.
    pub(in crate::cluster::coordinator) fn dispatch_move(
        &self,
        state: &ClusterState,
        pos: u32,
        desired: &ShardAssignment,
        handle: &Handle,
    ) -> Result<ReassignOutcome, ShardError> {
        let committed_bare = state
            .assignments
            .iter()
            .find(|a| a.position == pos)
            .is_none_or(|a| a.replicas.is_empty());
        if committed_bare && desired.replicas.is_empty() {
            self.reassign_and_move(pos as usize, desired.primary, handle)
        } else {
            self.reassign_group_and_move(pos as usize, desired, handle)
        }
    }

    /// Run one planned wave, returning `(position, outcome)` in the wave's (position) order. A
    /// singleton wave runs INLINE on the calling thread — the sequential default spawns no
    /// threads. A multi-move wave runs each move on a named scoped thread; an OS spawn failure
    /// degrades that move to inline execution (narrower wave, same guarantees — surfaced as an
    /// event), and a panicking move thread is contained to a per-position error (its ledger
    /// ticket released by RAII during unwind).
    pub(in crate::cluster::coordinator) fn execute_move_wave(
        &self,
        state: &ClusterState,
        targets: &[(u32, ShardAssignment)],
        wave: &[usize],
        handle: &Handle,
    ) -> Vec<(u32, Result<ReassignOutcome, ShardError>)> {
        if let [i] = wave {
            let (pos, desired) = &targets[*i];
            return vec![(*pos, self.dispatch_move(state, *pos, desired, handle))];
        }
        std::thread::scope(|scope| {
            enum Slot<'s> {
                Spawned(
                    u32,
                    std::thread::ScopedJoinHandle<'s, Result<ReassignOutcome, ShardError>>,
                ),
                Done(u32, Result<ReassignOutcome, ShardError>),
            }
            let mut slots: Vec<Slot<'_>> = Vec::with_capacity(wave.len());
            for &i in wave {
                let (pos, desired) = &targets[i];
                let pos = *pos;
                let spawned = std::thread::Builder::new()
                    .name(format!("rr-move-{pos}"))
                    .spawn_scoped(scope, move || {
                        self.dispatch_move(state, pos, desired, handle)
                    });
                match spawned {
                    Ok(h) => slots.push(Slot::Spawned(pos, h)),
                    Err(e) => {
                        self.emit(EngineEvent::DurabilityFailure {
                            op: DurabilityOp::ReplicaDesync,
                            detail: format!(
                                "spawning a parallel-move thread for shard {pos} failed; running \
                                 the move inline (a narrower wave — same guarantees, less overlap)"
                            ),
                            error: e.to_string(),
                        });
                        slots.push(Slot::Done(
                            pos,
                            self.dispatch_move(state, pos, desired, handle),
                        ));
                    }
                }
            }
            slots
                .into_iter()
                .map(|slot| match slot {
                    Slot::Spawned(pos, h) => match h.join() {
                        Ok(outcome) => (pos, outcome),
                        // Library moves never panic by contract (no `unwrap()` in library code);
                        // contain a violation to this position rather than poisoning the sweep.
                        // The move's ledger ticket was released by RAII during the unwind.
                        Err(_) => (
                            pos,
                            Err(ShardError::Remote(format!(
                                "parallel move of shard {pos} panicked; the position was rolled \
                                 back per the move's own abort path and is retried next pass"
                            ))),
                        ),
                    },
                    Slot::Done(pos, outcome) => (pos, outcome),
                })
                .collect()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::control::{NodeDescriptor, NodeId, NodeRole};

    fn node(id: u64) -> NodeDescriptor {
        NodeDescriptor {
            id: NodeId(id),
            addr: Some(format!("http://127.0.0.1:{}", 50050 + id)),
            role: NodeRole::Data,
        }
    }

    fn addr(id: u64) -> String {
        format!("http://127.0.0.1:{}", 50050 + id)
    }

    fn bare(position: u32, primary: u64) -> ShardAssignment {
        ShardAssignment {
            position,
            primary: NodeId(primary),
            replicas: Vec::new(),
        }
    }

    fn grouped(position: u32, primary: u64, replicas: &[u64]) -> ShardAssignment {
        ShardAssignment {
            position,
            primary: NodeId(primary),
            replicas: replicas.iter().map(|&r| NodeId(r)).collect(),
        }
    }

    fn state_with(nodes: Vec<NodeDescriptor>, assignments: Vec<ShardAssignment>) -> ClusterState {
        ClusterState {
            epoch: 0,
            num_shards: assignments.len().max(8) as u32,
            nodes,
            voters: Vec::new(),
            assignments,
            vnodes: 128,
            dict_fingerprint: 0,
            model_version: 0,
        }
    }

    /// A chained reshuffle (p0: N1→N2 while p1: N2→N3) shares node 2 — the moves land in
    /// DIFFERENT waves (T must not be a handoff target and a fenced source at once).
    #[test]
    fn chained_reshuffle_serializes() {
        let st = state_with(
            vec![node(1), node(2), node(3)],
            vec![bare(0, 1), bare(1, 2)],
        );
        let targets = vec![(0u32, bare(0, 2)), (1u32, bare(1, 3))];
        assert_eq!(plan_waves(&st, &targets, 8), vec![vec![0], vec![1]]);
    }

    /// Two positions flipping off ONE source node serialize.
    #[test]
    fn shared_source_serializes() {
        let st = state_with(
            vec![node(1), node(2), node(3)],
            vec![bare(0, 1), bare(1, 1)],
        );
        let targets = vec![(0u32, bare(0, 2)), (1u32, bare(1, 3))];
        assert_eq!(plan_waves(&st, &targets, 8), vec![vec![0], vec![1]]);
    }

    /// Two positions moving ONTO one destination node serialize (the adopt/AddShard handshake).
    #[test]
    fn shared_destination_serializes() {
        let st = state_with(
            vec![node(1), node(2), node(3)],
            vec![bare(0, 1), bare(1, 2)],
        );
        let targets = vec![(0u32, bare(0, 3)), (1u32, bare(1, 3))];
        assert_eq!(plan_waves(&st, &targets, 8), vec![vec![0], vec![1]]);
    }

    /// Moves over disjoint node sets share a wave — the parallelism the ledger + planner exist
    /// to allow.
    #[test]
    fn disjoint_moves_share_a_wave() {
        let st = state_with(
            vec![node(1), node(2), node(3), node(4)],
            vec![bare(0, 1), bare(1, 3)],
        );
        let targets = vec![(0u32, bare(0, 2)), (1u32, bare(1, 4))];
        assert_eq!(plan_waves(&st, &targets, 8), vec![vec![0, 1]]);
    }

    /// `max_parallel` caps a wave's width even over fully-disjoint moves; deferred targets keep
    /// position order.
    #[test]
    fn cap_bounds_wave_width() {
        let nodes: Vec<NodeDescriptor> = (1..=10).map(node).collect();
        let assignments: Vec<ShardAssignment> = (0..5).map(|p| bare(p, u64::from(p) + 1)).collect();
        let st = state_with(nodes, assignments);
        let targets: Vec<(u32, ShardAssignment)> =
            (0..5u32).map(|p| (p, bare(p, u64::from(p) + 6))).collect();
        assert_eq!(
            plan_waves(&st, &targets, 2),
            vec![vec![0, 1], vec![2, 3], vec![4]]
        );
    }

    /// `max_parallel = 1` (the DEFAULT) is singleton waves in exact target order — the sequential
    /// path's schedule, byte-identical.
    #[test]
    fn cap_one_is_sequential_target_order() {
        let nodes: Vec<NodeDescriptor> = (1..=10).map(node).collect();
        let assignments: Vec<ShardAssignment> = (0..5).map(|p| bare(p, u64::from(p) + 1)).collect();
        let st = state_with(nodes, assignments);
        let targets: Vec<(u32, ShardAssignment)> =
            (0..5u32).map(|p| (p, bare(p, u64::from(p) + 6))).collect();
        assert_eq!(
            plan_waves(&st, &targets, 1),
            vec![vec![0], vec![1], vec![2], vec![3], vec![4]]
        );
        // 0 clamps to 1 (a nonsense cap never means "unbounded").
        assert_eq!(plan_waves(&st, &targets, 0).len(), 5);
    }

    /// A group move's footprint is the committed primary (the fenced source) plus EVERY desired
    /// member — and NOT a dropped `C ∖ D` replica, which the move never contacts.
    #[test]
    fn group_footprint_covers_primary_and_all_desired_members_not_dropped_ones() {
        let st = state_with(
            (1..=4).map(node).collect(),
            vec![grouped(0, 1, &[4])], // committed: primary N1, replica N4
        );
        let desired = grouped(0, 2, &[3]); // desired: primary N2, replica N3
        let fp = move_footprint(&st, &desired);
        assert!(fp.contains(&addr(1)), "committed primary (fenced source)");
        assert!(fp.contains(&addr(2)), "desired primary");
        assert!(fp.contains(&addr(3)), "desired replica");
        assert!(
            !fp.contains(&addr(4)),
            "a dropped C∖D replica is never contacted — not in the footprint"
        );
        assert_eq!(fp.len(), 3);
    }

    /// The partition is deterministic: same inputs, same waves.
    #[test]
    fn waves_are_deterministic() {
        let st = state_with(
            (1..=6).map(node).collect(),
            vec![bare(0, 1), bare(1, 2), bare(2, 3)],
        );
        let targets = vec![(0u32, bare(0, 2)), (1u32, bare(1, 4)), (2u32, bare(2, 5))];
        assert_eq!(plan_waves(&st, &targets, 4), plan_waves(&st, &targets, 4));
    }

    /// A target whose members don't all resolve (an addr-less committed primary — e.g. the
    /// manager) is still SCHEDULED: it fails loudly inside the move (pre-network endpoint
    /// resolution), not silently in the planner.
    #[test]
    fn addrless_member_target_is_still_scheduled() {
        let manager = NodeDescriptor {
            id: NodeId(0),
            addr: None,
            role: NodeRole::Manager,
        };
        let st = state_with(vec![manager, node(1)], vec![bare(0, 0)]);
        let targets = vec![(0u32, bare(0, 1))];
        let fp = move_footprint(&st, &targets[0].1);
        assert_eq!(fp, vec![addr(1)], "only the resolvable member appears");
        assert_eq!(plan_waves(&st, &targets, 2), vec![vec![0]]);
    }
}
