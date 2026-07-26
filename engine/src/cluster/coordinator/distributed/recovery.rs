use super::{Arc, ClusterEngine, DurabilityOp, EngineEvent, LogPos, Shard, ShardError};

impl ClusterEngine {
    /// Cross-node peer recovery (ADR-036 + ADR-039 + ADR-040): bring a fresh, durable, **pending**
    /// node up as a copy of a shard by streaming a peer's segments AND replaying its translog tail —
    /// so writes to the source need **not** be quiesced for the copy window (ADR-036's gap, closed
    /// by the per-shard translog). The flow: pin the source's un-sealed tail with a **retention
    /// lease** (ADR-040) so the segment-copy seal — and any concurrent seal — cannot trim it away;
    /// ship the frozen dict to `target_endpoint` (adopt); drive its `RecoverFrom`, which pulls
    /// `source_endpoint`'s sealed segments at position `P`, attaches them, and reports `P`; then
    /// replay the source's translog tail (ops > `P`) into the target via the shared apply funnel,
    /// **looping** to drain residual writes until it stops advancing (the finalize — the window a
    /// final external quiesce would cover shrinks toward zero). Releases the lease on completion.
    /// Returns `(num_queries, high_water)`. Correctness never depends on the loop converging — the
    /// lease keeps the tail safe — only the residual size does.
    pub fn peer_recover_replica(
        &self,
        shard_id: u32,
        source_endpoint: &str,
        target_endpoint: &str,
        handle: &tokio::runtime::Handle,
    ) -> Result<(u64, u64), ShardError> {
        // Bound on the convergence loop (a safety cap, not a correctness requirement).
        const FINALIZE_PASSES: usize = 8;
        let expected = self.dict.fingerprint();
        let expected_tag = self.tag_dict.fingerprint();
        // Pin the source's tail BEFORE the segment-copy seal trims it (ADR-040). Held across the
        // whole recovery; released below whether it converges or errors.
        // Recover the source's slot `shard_id` into the target's slot `shard_id` (ADR-093): a
        // relocation/replication keeps the SAME global position (e.g. position 1's primary hosts slot 1).
        let source = crate::cluster::remote::RemoteShard::connect_for_coordinator_with_security(
            source_endpoint,
            handle.clone(),
            expected,
            expected_tag,
            shard_id,
            self.coordinator_id,
            &self.client_security,
        )?
        .with_metrics(Arc::clone(&self.transport_metrics));
        let (lease, _pinned) = source.acquire_retention_lease()?;

        let recover = || -> Result<(u64, u64), ShardError> {
            let dict_bytes = crate::storage::serialize_dict(&self.dict);
            // Ship the dict + frozen tag space so the fresh node attaches segments against the right
            // feature + tag space (ADR-055).
            let target = crate::cluster::remote::RemoteShard::
                connect_and_adopt_for_coordinator_with_security(
                target_endpoint,
                handle.clone(),
                dict_bytes,
                expected,
                crate::storage::serialize_tagdict(&self.tag_dict),
                self.tag_dict.fingerprint(),
                shard_id,
                self.placement_generation(),
                self.num_shards() as u32,
                self.coordinator_id,
                &self.client_security,
            )?
            .with_metrics(Arc::clone(&self.transport_metrics));
            // Bulk copy: segments at snapshot position P (the source keeps serving + writing).
            let (_segments, _nq, p) = target.recover_from(source_endpoint, expected)?;
            // Tail replay + convergence: drain the source tail (> P) through the SAME apply funnel
            // as a live write (re-derived from DSL against the frozen dict), looping until it stops
            // advancing. Renew the lease each pass so the source may GC the consumed prefix.
            let mut hwm = LogPos(p);
            for _ in 0..FINALIZE_PASSES {
                let next = crate::cluster::replica::catch_up_replica(
                    &target, &source, &self.norm, &self.dict, hwm,
                )?;
                source.renew_retention_lease(lease, next)?;
                if next == hwm {
                    break; // tail drained at this instant — converged
                }
                hwm = next;
            }
            let num_queries = target.num_queries()? as u64;
            Ok((num_queries, hwm.0))
        };
        let out = recover();
        // Always release the lease (a held one would pin the source's translog forever). A release
        // failure on an otherwise-successful recovery is surfaced as an event, not conflated with
        // the recovery outcome (the replica is good; the source may just retain extra translog).
        if let Err(e) = source.release_retention_lease(lease) {
            self.emit(EngineEvent::DurabilityFailure {
                op: DurabilityOp::ReplicaDesync,
                detail: "releasing the peer-recovery retention lease on the source failed; the \
                         source may retain extra translog until its next successful seal"
                    .into(),
                error: e.to_string(),
            });
        }
        out
    }

    /// Re-run the translog catch-up (ADR-039): replay `source`'s tail (ops strictly after
    /// `after`) into the already-recovered `target`, returning the new high-water source position.
    /// The brief finalize after [`Self::peer_recover_replica`]'s bulk copy — under sustained
    /// writes, recovery converges by repeating this until the high-water stops advancing (the
    /// window where a final quiesce would shrink to the residual delta).
    pub fn catch_up_recovered_replica(
        &self,
        shard_id: u32,
        source_endpoint: &str,
        target_endpoint: &str,
        after: u64,
        handle: &tokio::runtime::Handle,
    ) -> Result<u64, ShardError> {
        let expected = self.dict.fingerprint();
        let expected_tag = self.tag_dict.fingerprint();
        // Catch up the target's slot `shard_id` from the source's same slot (ADR-093).
        let source = crate::cluster::remote::RemoteShard::connect_for_coordinator_with_security(
            source_endpoint,
            handle.clone(),
            expected,
            expected_tag,
            shard_id,
            self.coordinator_id,
            &self.client_security,
        )?
        .with_metrics(Arc::clone(&self.transport_metrics));
        let target = crate::cluster::remote::RemoteShard::connect_for_coordinator_with_security(
            target_endpoint,
            handle.clone(),
            expected,
            expected_tag,
            shard_id,
            self.coordinator_id,
            &self.client_security,
        )?
        .with_metrics(Arc::clone(&self.transport_metrics));
        let hwm = crate::cluster::replica::catch_up_replica(
            &target,
            &source,
            &self.norm,
            &self.dict,
            LogPos(after),
        )?;
        Ok(hwm.0)
    }
}
