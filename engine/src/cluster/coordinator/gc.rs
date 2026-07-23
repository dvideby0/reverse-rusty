//! `impl ClusterEngine` — the orphan-slot GC sweep (ADR-096, `distributed` feature): reclaim the
//! slots data-moving reassignment strands. Every move leaves its source slot behind — fenced, in
//! the node's slot map, its `shard_<id>/` dir on disk — deliberately (serve-then-drop, ADR-090:
//! the fenced source keeps serving reads through the `MovedButNotCommitted` crash window), but
//! nothing ever reclaimed them, so disk + resident memory grew with every move, forever
//! (a durable restart re-attaches every `shard_<id>/` dir it finds).
//!
//! ## The keep-set (what is NEVER dropped)
//! A hosted slot survives the sweep if ANY of:
//! - its position has **no committed assignment** (the map cannot vouch the data lives elsewhere —
//!   fail-safe, skip + report);
//! - the committed map assigns its position to this node (primary or replica) — including the
//!   `MovedButNotCommitted` window, where the committed map still names the old source;
//! - this node's endpoint is in the position's **live routing**
//!   ([`Shard::live_endpoints`](crate::cluster::shard::Shard::live_endpoints)) — covering every
//!   way routing can point somewhere the map does not name (a raw `execute_handoff` flip, an
//!   uncommitted move) — the oracle-proven flip-without-commit state serves from exactly such a
//!   node. Endpoint comparison is normalized (trailing-slash/case) and ambiguity KEEPS.
//!
//! Everything else the node hosts is an orphan: unrouted by the committed map (what a restart
//! resolves) AND by live routing. Dropping it cannot false-negative any supported read path.
//!
//! ## The drop (defense in depth)
//! The sweep holds a **whole-sweep ledger reservation** of every addr'd data node (ADR-095), so
//! no data move — and no raw handoff, which reserves its endpoints too — can interleave. Server-
//! side, `DropShard` refuses anything not FENCED at exactly the armed generation (fences are not
//! durable, so a restarted orphan is re-armed via the `fence(0)` probe first — the ADR-094
//! `clear_stale_fence` trick), anything holding an unexpired retention lease (an in-flight
//! recovery's pinned source), and any divergent dict/tag space; the fence is re-checked under the
//! slot-map write lock at removal (CAS). Disk reclaim is rename-to-trash then delete — an
//! interrupted delete is invisible to a restart and swept at boot; an in-place delete could brick
//! a restart (a live-named dir whose sidecar lists deleted segments fails the reopen loud).
//!
//! Per-slot failures are recorded and the sweep CONTINUES (the reconcile posture); a second sweep
//! is idempotent (dropped slots list as absent). `distributed`-gated; the in-process/default path
//! never compiles it.

use tokio::runtime::Handle;

use crate::cluster::control::{ClusterState, NodeId, NodeRole};
use crate::cluster::remote::RemoteShard;
use crate::cluster::shard::ShardError;

use super::ClusterEngine;

/// One slot the sweep classified — where it lives and how big it was when listed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanSlot {
    /// The hosting node.
    pub node: NodeId,
    /// The slot (= global shard position).
    pub shard_id: u32,
    /// The slot's live query count at listing time (observability — how much was reclaimed/kept).
    pub num_queries: u64,
}

/// One [`ClusterEngine::gc_orphan_slots`] sweep's outcome (ADR-096). Every slot is independent;
/// per-slot failures are recorded and the sweep continues, so a partial sweep is a valid state a
/// re-run finishes (idempotent — dropped slots are simply absent next time).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GcReport {
    /// Orphans dropped: removed from the node's slot map, their `shard_<id>/` dir reclaimed.
    pub dropped: Vec<OrphanSlot>,
    /// Hosted-but-unassigned slots KEPT because the coordinator's live routing still reaches that
    /// node for that position (a raw handoff flip / an uncommitted move) — the map alone would
    /// have called them orphans.
    pub kept_live_routed: Vec<OrphanSlot>,
    /// Slots whose position has NO committed assignment — fail-safe skipped (the map cannot vouch
    /// the data lives elsewhere).
    pub skipped_unassigned: Vec<OrphanSlot>,
    /// Per-slot drop failures `(slot, error)` — e.g. a lease held by an in-flight recovery, a
    /// fence CAS lost to a concurrent handoff. Retried by the next sweep.
    pub failed: Vec<(OrphanSlot, String)>,
    /// Nodes the sweep could not classify at all `(node, reason)`: unreachable, a pre-ADR-096
    /// server (`Unimplemented`), or a divergent dict/tag fingerprint. Nothing on them is touched.
    pub skipped_nodes: Vec<(NodeId, String)>,
}

impl GcReport {
    /// Did the sweep leave every reachable node fully reclaimed (nothing failed)? Skipped nodes /
    /// unassigned positions are not failures — they are reported so an operator can look.
    pub fn is_clean(&self) -> bool {
        self.failed.is_empty()
    }
}

/// How the sweep classifies one hosted slot. Pure over the committed document + the live-routing
/// endpoint sets, so it is unit-testable with zero network.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::cluster::coordinator) enum SlotClass {
    /// The committed map assigns this position to this node — not an orphan.
    Committed,
    /// Unassigned position — fail-safe skip.
    Unassigned,
    /// Live routing still reaches this node for this position — kept.
    LiveRouted,
    /// Hosted, unassigned-to-this-node, unrouted — the drop candidate.
    Orphan,
}

/// Normalize an endpoint string for the live-routing compare: scheme/host case-insensitivity +
/// a trailing slash are formatting, not identity. Conservative — a compare that still differs
/// after this keeps the slot (the caller treats only EXACT non-membership as orphan).
fn normalized(ep: &str) -> String {
    ep.trim_end_matches('/').to_ascii_lowercase()
}

/// Classify one hosted slot (pure). `live_eps` is the coordinator's live-routing endpoint set for
/// this slot's position (empty when the position is out of range — e.g. a slot beyond the ring).
pub(in crate::cluster::coordinator) fn classify_slot(
    state: &ClusterState,
    node: NodeId,
    node_addr: &str,
    shard_id: u32,
    live_eps: &[String],
) -> SlotClass {
    let Some(assignment) = state.assignments.iter().find(|a| a.position == shard_id) else {
        return SlotClass::Unassigned;
    };
    if assignment.primary == node || assignment.replicas.contains(&node) {
        return SlotClass::Committed;
    }
    let addr = normalized(node_addr);
    if live_eps.iter().any(|e| normalized(e) == addr) {
        return SlotClass::LiveRouted;
    }
    SlotClass::Orphan
}

impl ClusterEngine {
    /// Sweep every addr'd data node for orphan slots and reclaim them (ADR-096) — see the module
    /// docs for the keep-set and the drop's guard ladder. Holds a whole-sweep busy-endpoint
    /// ledger reservation (no move or raw handoff interleaves); per-slot failures land in the
    /// report and the sweep continues. An in-process / genesis cluster (no addr'd data nodes)
    /// returns the clean empty report.
    pub fn gc_orphan_slots(&self, handle: &Handle) -> Result<GcReport, ShardError> {
        let state = self.control_state()?;
        let data_nodes: Vec<(NodeId, String)> = state
            .nodes
            .iter()
            .filter(|n| n.role == NodeRole::Data)
            .filter_map(|n| n.addr.clone().map(|a| (n.id, a)))
            .collect();
        if data_nodes.is_empty() {
            return Ok(GcReport::default());
        }
        // Reserve EVERY data node for the whole sweep (ADR-095): strictly coarser than any move's
        // footprint, so no move-then-commit — and no raw handoff — can interleave a fence/recover
        // with the arm-and-drop below.
        let eps: Vec<&str> = data_nodes.iter().map(|(_, a)| a.as_str()).collect();
        let _ticket = self.move_ledger.reserve(&eps);
        // Re-read under the reservation: the map a concurrent move just committed is the one to
        // classify against (mirrors the moves' own plan → reserve → revalidate).
        let state = self.control_state()?;

        // The live-routing keep-set: each position's endpoints as routing currently reaches them.
        let live_eps: Vec<Vec<String>> = self.shards.iter().map(|s| s.live_endpoints()).collect();

        let expected = self.dict.fingerprint();
        let expected_tag = self.tag_dict.fingerprint();
        let mut report = GcReport::default();
        for (node, addr) in data_nodes {
            // A node-level client (slot binding irrelevant for the LISTING; drops connect their
            // own per-slot client below). Connect refuses a divergent dict — exactly the identity
            // check the sweep wants before classifying anything on the node.
            let lister = match RemoteShard::connect_for_coordinator_with_security(
                &addr,
                handle.clone(),
                expected,
                expected_tag,
                0,
                self.coordinator_id,
                &self.client_security,
            ) {
                Ok(c) => c.with_metrics(std::sync::Arc::clone(&self.transport_metrics)),
                Err(e) => {
                    report.skipped_nodes.push((node, e.to_string()));
                    continue;
                }
            };
            let listing = match lister.list_shards() {
                Ok(l) => l,
                Err(e) => {
                    // Unreachable mid-sweep, or a pre-ADR-096 server (`Unimplemented`).
                    report.skipped_nodes.push((node, e.to_string()));
                    continue;
                }
            };
            if listing.dict_fingerprint != expected || listing.tag_dict_fingerprint != expected_tag
            {
                report.skipped_nodes.push((
                    node,
                    format!(
                        "node fingerprints {:#018x}/{:#018x} diverge from the coordinator's — \
                         not classifying its slots",
                        listing.dict_fingerprint, listing.tag_dict_fingerprint
                    ),
                ));
                continue;
            }
            for s in &listing.shards {
                let slot = OrphanSlot {
                    node,
                    shard_id: s.shard_id,
                    num_queries: s.num_queries,
                };
                let live: &[String] = live_eps.get(s.shard_id as usize).map_or(&[], Vec::as_slice);
                match classify_slot(&state, node, &addr, s.shard_id, live) {
                    SlotClass::Committed => {} // simply not an orphan; unreported
                    SlotClass::Unassigned => report.skipped_unassigned.push(slot),
                    SlotClass::LiveRouted => report.kept_live_routed.push(slot),
                    SlotClass::Orphan => match self.drop_orphan(&addr, s, handle) {
                        Ok(()) => report.dropped.push(slot),
                        Err(e) => report.failed.push((slot, e.to_string())),
                    },
                }
            }
        }
        Ok(report)
    }

    /// Arm-and-drop one classified orphan: connect a per-slot client, probe the fence
    /// (`fence(0)` — a pure probe, the server fence is a monotonic `fetch_max`), arm an unfenced
    /// (e.g. restarted) orphan at `max(epoch, 1)`, then `DropShard` at exactly that generation
    /// (the server re-checks it under its slot-map write lock — the CAS).
    fn drop_orphan(
        &self,
        addr: &str,
        listing: &crate::cluster::proto::ShardListing,
        handle: &Handle,
    ) -> Result<(), ShardError> {
        let client = RemoteShard::connect_for_coordinator_with_security(
            addr,
            handle.clone(),
            self.dict.fingerprint(),
            self.tag_dict.fingerprint(),
            listing.shard_id,
            self.coordinator_id,
            &self.client_security,
        )?
        .with_metrics(std::sync::Arc::clone(&self.transport_metrics));
        let probed = client.fence(0)?;
        let armed = if probed == 0 {
            // A restart cleared the fence (fences are not durable) — re-arm it. The epoch is a
            // fine generation: any concurrent handoff (excluded by the sweep's ledger reservation
            // anyway) would fence higher and the drop's CAS would fail loud.
            let state_epoch = self.control_state()?.epoch.max(1);
            client.fence(state_epoch)?
        } else {
            probed
        };
        let reply = client.drop_shard(armed)?;
        if !reply.dropped {
            // Absent already (an idempotent overlap with an earlier sweep) — nothing reclaimed,
            // nothing wrong.
            return Ok(());
        }
        if !reply.dir_removed {
            // Renamed-to-trash but the delete did not finish; the node's next boot sweeps it.
            // Surface as an event, not a failure — the slot is gone from serving either way.
            self.emit(crate::events::EngineEvent::DurabilityFailure {
                op: crate::events::DurabilityOp::ReplicaDesync,
                detail: format!(
                    "orphan-slot GC dropped shard {} on {} but the dir delete was incomplete \
                     (trash-renamed); the node's next restart sweeps it",
                    listing.shard_id, addr
                ),
                error: "dir_removed = false".into(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::control::{NodeDescriptor, NodeRole, ShardAssignment};

    fn state_with(assignments: Vec<ShardAssignment>) -> ClusterState {
        ClusterState {
            epoch: 3,
            num_shards: 8,
            nodes: vec![
                NodeDescriptor {
                    id: NodeId(1),
                    addr: Some("http://127.0.0.1:50051".into()),
                    role: NodeRole::Data,
                },
                NodeDescriptor {
                    id: NodeId(2),
                    addr: Some("http://127.0.0.1:50052".into()),
                    role: NodeRole::Data,
                },
            ],
            voters: Vec::new(),
            assignments,
            vnodes: 128,
            dict_fingerprint: 0,
            model_version: 0,
            placement_generation: crate::ownership::PlacementGeneration::INITIAL.get(),
        }
    }

    fn assign(position: u32, primary: u64, replicas: &[u64]) -> ShardAssignment {
        ShardAssignment {
            position,
            primary: NodeId(primary),
            replicas: replicas.iter().map(|&r| NodeId(r)).collect(),
        }
    }

    /// The committed map's owners — primary AND replicas — are never orphans.
    #[test]
    fn committed_primary_and_replica_are_kept() {
        let st = state_with(vec![assign(0, 1, &[2])]);
        assert_eq!(
            classify_slot(&st, NodeId(1), "http://127.0.0.1:50051", 0, &[]),
            SlotClass::Committed,
            "the committed primary"
        );
        assert_eq!(
            classify_slot(&st, NodeId(2), "http://127.0.0.1:50052", 0, &[]),
            SlotClass::Committed,
            "a committed replica"
        );
    }

    /// A position with NO committed assignment is fail-safe skipped — the map cannot vouch the
    /// data lives elsewhere, so the sweep never drops it.
    #[test]
    fn unassigned_position_is_skipped() {
        let st = state_with(vec![assign(0, 1, &[])]);
        assert_eq!(
            classify_slot(&st, NodeId(2), "http://127.0.0.1:50052", 5, &[]),
            SlotClass::Unassigned
        );
    }

    /// A hosted-but-unassigned slot whose node LIVE ROUTING still reaches is kept — the
    /// `MovedButNotCommitted` / raw-handoff-flip protection the committed map alone would miss.
    /// The endpoint compare is normalized (trailing slash, case) so a formatting variant still
    /// KEEPS.
    #[test]
    fn live_routed_slot_is_kept_with_normalized_endpoints() {
        let st = state_with(vec![assign(0, 1, &[])]);
        let live = vec!["http://127.0.0.1:50052/".to_string()]; // trailing slash variant
        assert_eq!(
            classify_slot(&st, NodeId(2), "HTTP://127.0.0.1:50052", 0, &live),
            SlotClass::LiveRouted
        );
    }

    /// Hosted, not committed to this node, not live-routed: the one class the sweep drops.
    #[test]
    fn hosted_unrouted_slot_is_the_orphan() {
        let st = state_with(vec![assign(0, 1, &[])]);
        let live = vec!["http://127.0.0.1:50051".to_string()]; // routing reaches node 1, not 2
        assert_eq!(
            classify_slot(&st, NodeId(2), "http://127.0.0.1:50052", 0, &live),
            SlotClass::Orphan
        );
    }

    /// The in-process / genesis sweep is a clean no-op: no addr'd data nodes ⇒ the empty report,
    /// nothing contacted, epoch invariant (the byte-identical-default guard at the unit level).
    #[test]
    fn gc_is_a_clean_no_op_in_process() {
        use crate::cluster::coordinator::{ClusterConfig, ClusterEngine};
        use crate::normalize::Normalizer;

        let cluster = ClusterEngine::build(
            Normalizer::default_vocab().expect("vocab"),
            &ClusterConfig {
                num_shards: 3,
                ..ClusterConfig::default()
            },
            &[(1u64, "+nike +shoe".to_string())],
        )
        .expect("in-process cluster");
        let epoch_before = cluster.control_state().expect("state").epoch;
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        let report = cluster.gc_orphan_slots(rt.handle()).expect("no-op sweep");
        assert_eq!(report, GcReport::default(), "clean empty report");
        assert!(report.is_clean());
        assert_eq!(
            cluster.control_state().expect("state").epoch,
            epoch_before,
            "a no-op sweep commits nothing"
        );
    }
}
