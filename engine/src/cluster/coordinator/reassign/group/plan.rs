//! The group move's PURE planning layer (ADR-094/095): target computation (which positions'
//! committed groups diverge from the HRW-desired placement), group equality (replicas as a SET),
//! the stale-fence clear every member runs on group entry, and the validated plan the
//! plan→reserve→revalidate loop produces. Split from `group.rs` for the <650-line budget; the
//! algorithm itself (the 9 phases) stays there.

use std::collections::BTreeSet;

use crate::cluster::allocator;
use crate::cluster::control::{ClusterState, NodeId, NodeRole, ShardAssignment};
use crate::cluster::remote::RemoteShard;
use crate::cluster::shard::ShardError;

use super::super::ledger::MoveTicket;

/// Clear a STALE fence on a member (re-)entering a group (codex P1 on this ADR): serve-then-drop
/// deliberately leaves a dropped PRIMARY's slot fenced forever, and `RecoverFrom` preserves the
/// slot's fence — so a later move that brings the same node back would commit a member that
/// rejects every write (a fenced new primary write-breaks the position; a fenced new replica
/// silently desyncs on its first fan-out). The server fence is a monotonic `fetch_max`, so
/// `fence(0)` is a pure PROBE reporting the current generation; `unfence(probe)` then CAS-clears
/// exactly that generation. Safe under the documented single-active-coordinator topology (there is
/// no other orchestrator whose live fence this could trample); fails loud if the CAS loses a race.
pub(super) fn clear_stale_fence(member: &RemoteShard, ctx: &str) -> Result<(), ShardError> {
    let stale = member.fence(0)?;
    if stale == 0 {
        return Ok(());
    }
    let now = member.unfence(stale)?;
    if now != 0 {
        return Err(ShardError::Remote(format!(
            "{ctx}: clearing a stale fence (generation {stale}) on a group member failed — the \
             slot is still fenced at generation {now} (a concurrent handoff?)"
        )));
    }
    Ok(())
}

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
pub(in crate::cluster::coordinator) fn groups_equal(
    a: &ShardAssignment,
    b: &ShardAssignment,
) -> bool {
    a.primary == b.primary && replica_set_eq(&a.replicas, &b.replicas)
}

/// The group-aware target computation (it replaced the primary-only `rebalance_targets`): positions
/// whose committed GROUP (primary by identity OR replica set) diverges from the HRW-desired
/// placement at `rf`, each with its full desired [`ShardAssignment`], in position order. Pure over
/// the cluster-state document.
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

/// Whether a RETAINED member provably already holds the frozen source's exact live set
/// (ADR-097): both content fingerprints — computed while BOTH sides are quiescent (the source
/// post-freeze-probe; the member write-quiesced by the primary fence, since composite writes are
/// primary-first) — and equal. Equality covers the match-relevant live multiset
/// `(logical, version, dsl, TagId*)`; sources.dat / segment-layout divergence is deliberately
/// out of scope (never on the match path). `None` source fingerprint (the RPC failed — e.g. a
/// pre-ADR-097 peer) or a member-side error ⇒ NOT provable ⇒ the caller falls back to the
/// proven heal-by-re-copy. False negatives here cost a redundant copy, never correctness.
pub(super) fn retained_member_is_complete(
    source_fp: Option<(u64, u64, u64)>,
    member: &RemoteShard,
) -> bool {
    let Some(src) = source_fp else {
        return false;
    };
    member.content_fingerprint().is_ok_and(|m| m == src)
}

/// The validated output of the group move's plan→reserve→revalidate loop (ADR-095).
pub(super) struct PlannedGroupMove<'a> {
    /// The committed group the plan (and the phase-9 CAS) compares against.
    pub(super) committed: ShardAssignment,
    /// The committed primary's endpoint — the fenced recovery source.
    pub(super) cp_ep: String,
    /// D's members in composite order (primary first), each with its endpoint.
    pub(super) d_members: Vec<(NodeId, String)>,
    /// The held reservation of `{cp} ∪ D` — alive for the whole move-then-commit.
    pub(super) ticket: MoveTicket<'a>,
}
