//! [`ShardMetricsSource`] — the render handle behind a shard node's `/_metrics` endpoint
//! (ADR-091), moved out of the `server.rs` root as a pure extraction (file-size budget).

use std::sync::Arc;

use super::{ServerState, ShardMap, ShardSlot};

/// A render handle for a [`ShardServer`](super::ShardServer)'s `/_metrics` endpoint (ADR-091).
/// Holds a shared clone of the server's swappable state, so it renders live numbers (including the
/// pending→adopted flip) and outlives the `serve` call that consumes the server. `Send + 'static`
/// so the deploy bin can move it into the metrics listener's render closure.
pub struct ShardMetricsSource {
    /// A shared clone of the server's shard map. A node may host many co-located slots (ADR-093), so
    /// `render` emits one `{shard="<id>"}`-labeled series per LOADED slot (Stage 3). A node serving a
    /// single non-zero position renders exactly that slot; a pending node renders the not-ready body.
    pub(super) shards: ShardMap,
}

impl ShardMetricsSource {
    /// Render the current Prometheus exposition body for this shard. Reads ONE lock-free snapshot
    /// (metrics + segment infos + class counts from the same point-in-time) off the engine write
    /// lock; a pending (not-yet-adopted) server reports only `reverse_rusty_shard_ready 0`.
    pub fn render(&self) -> String {
        // ALL loaded slots this node hosts (ADR-093 multi-shard): a co-located node renders one
        // `{shard="<id>"}` series per slot. Collect the slot Arc + an `Arc<ServerState>` handle to
        // each loaded slot under the map read-lock, then DROP the lock before snapshotting —
        // mirroring the RPC handlers, which never hold the map lock across engine work. The slot
        // Arc is kept because the latency histograms (ADR-100) live on the slot. Sorted by
        // shard-id so the exposition is deterministic across scrapes. A poisoned lock ⇒ the
        // pending body.
        let loaded: Vec<(u32, Arc<ShardSlot>, Arc<ServerState>)> = match self.shards.read() {
            Ok(map) => {
                let mut v: Vec<(u32, Arc<ShardSlot>, Arc<ServerState>)> = map
                    .iter()
                    .filter_map(|(&id, slot)| {
                        slot.state.load_full().map(|st| (id, Arc::clone(slot), st))
                    })
                    .collect();
                v.sort_unstable_by_key(|(id, _, _)| *id);
                v
            }
            Err(_) => return crate::cluster::node_metrics::render_shard_pending(),
        };
        let samples: Vec<crate::cluster::node_metrics::ShardSample> = loaded
            .into_iter()
            .map(|(id, slot, st)| {
                let snap = st.shard.metrics_snapshot();
                crate::cluster::node_metrics::ShardSample {
                    shard_id: id,
                    metrics: snap.metrics(),
                    segments: snap.segment_infos(),
                    class: snap.class_counts(),
                    rpc_latency: slot.latency.snapshot(),
                    broad: slot.broad.snapshot(),
                    ranked: slot.ranked.snapshot(),
                }
            })
            .collect();
        crate::cluster::node_metrics::render_shards(&samples)
    }
}
