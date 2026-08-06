use super::{
    extract_readonly, Arc, Ordering, PlacedQuery, ServerState, Shard, ShardMetricsSource,
    ShardServer, ShardSlot, Status, DROPPED_TOMBSTONE,
};

impl ShardServer {
    /// Whether this server currently holds an adopted/restored state (false ⇒ pending,
    /// awaiting `AdoptDict`). Introspection for the deployable bin's startup banner.
    pub fn is_serving(&self) -> bool {
        self.shards
            .read()
            .is_ok_and(|m| m.values().any(|s| s.state.load_full().is_some()))
    }

    /// A cloneable handle that renders this shard's `/_metrics` body on demand (ADR-091). The
    /// deploy bin captures it BEFORE `serve` consumes the server, then hands it to
    /// [`serve_metrics`](super::node_metrics::serve_metrics) on the plaintext `--metrics-addr` port.
    /// It shares the server's swappable state, so it reports live numbers across the pending→adopted
    /// flip and never touches the engine write lock.
    pub fn metrics_source(&self) -> ShardMetricsSource {
        ShardMetricsSource {
            shards: Arc::clone(&self.shards),
        }
    }

    /// The slot hosting `shard_id` on this node, or `not_found` (ADR-093). Clones the slot `Arc` out
    /// and DROPS the map read-guard before returning, so no caller (notably the async `recover_from`)
    /// holds the std `RwLock` across an RPC/`await`.
    pub(in crate::cluster::server) fn slot(&self, shard_id: u32) -> Result<Arc<ShardSlot>, Status> {
        let map = self
            .shards
            .read()
            .map_err(|_| Status::internal("shard map lock poisoned"))?;
        map.get(&shard_id).cloned().ok_or_else(|| {
            Status::not_found(format!("shard {shard_id} is not hosted on this node"))
        })
    }

    /// The slot + its adopted [`ServerState`] for `shard_id` — `not_found` if the slot is absent,
    /// `failed_precondition` if present-but-pending. The per-shard handlers' one-line replacement for
    /// the old node-wide `loaded()`.
    pub(in crate::cluster::server) fn loaded_slot(
        &self,
        shard_id: u32,
    ) -> Result<(Arc<ShardSlot>, Arc<ServerState>), Status> {
        let slot = self.slot(shard_id)?;
        let st = slot.loaded_state()?;
        Ok((slot, st))
    }

    // Both failure messages are frozen (a pre-ADR-111 client retypes them by
    // substring); the ADR-111 ownership code rides as metadata alongside.
    pub(in crate::cluster::server) fn validate_placement_config(
        &self,
        generation: crate::ownership::PlacementGeneration,
        num_shards: u32,
    ) -> Result<(), Status> {
        use crate::cluster::ranked_wire::{attach, RankedWireCode};
        let space = self.node_dict.load_full().ok_or_else(|| {
            attach(
                Status::failed_precondition(
                    "node has not adopted an ownership-aware feature space",
                ),
                RankedWireCode::OwnershipMismatch,
                None,
            )
        })?;
        if space.placement_generation != generation || space.num_shards != num_shards {
            return Err(attach(
                Status::failed_precondition(format!(
                    "placement configuration mismatch: node generation {}/{} shards, request generation {}/{} shards",
                    space.placement_generation.0,
                    space.num_shards,
                    generation.0,
                    num_shards
                )),
                RankedWireCode::OwnershipMismatch,
                None,
            ));
        }
        Ok(())
    }

    /// Install (or replace) the slot for `shard_id`; the write-guard is released immediately.
    pub(in crate::cluster::server) fn insert_slot(
        &self,
        shard_id: u32,
        slot: Arc<ShardSlot>,
    ) -> Result<(), Status> {
        self.shards
            .write()
            .map_err(|_| Status::internal("shard map lock poisoned"))?
            .insert(shard_id, slot);
        Ok(())
    }

    /// Remove the slot for `shard_id` iff its fence is EXACTLY `expected_generation` — decided by
    /// a true `compare_exchange` swapping the fence to the irrevocable [`DROPPED_TOMBSTONE`]
    /// (ADR-096, codex P2: `Fence`/`Unfence` mutate the atomic through cloned slot `Arc`s WITHOUT
    /// the map lock, so a plain load-then-remove could race a concurrent fence change; the CAS
    /// makes any interleaving land either before it — the drop is refused — or after it — where
    /// `fetch_max` cannot lower the tombstone and `unfence` refuses to clear it). `Ok(None)` ⇒
    /// the slot was already absent (an idempotent re-run); `Err` ⇒ the fence changed. In-flight
    /// RPCs holding the old `Arc` complete against it (serve-then-drop at micro scale); memory
    /// frees when the last `Arc` drops.
    pub(in crate::cluster::server) fn remove_slot_if_fenced_at_with<T>(
        &self,
        shard_id: u32,
        expected_generation: u64,
        persist_removal: impl FnOnce() -> Result<T, Status>,
    ) -> Result<Option<T>, Status> {
        let mut map = self
            .shards
            .write()
            .map_err(|_| Status::internal("shard map lock poisoned"))?;
        let Some(slot) = map.get(&shard_id).cloned() else {
            return Ok(None);
        };
        if let Err(now) = slot.fenced_at_generation.compare_exchange(
            expected_generation,
            DROPPED_TOMBSTONE,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            return Err(Status::failed_precondition(format!(
                "DropShard: shard {shard_id}'s fence generation changed under the drop \
                ({now} != expected {expected_generation}); re-plan"
            )));
        }
        let persisted = match persist_removal() {
            Ok(persisted) => persisted,
            Err(source) => {
                if slot
                    .fenced_at_generation
                    .compare_exchange(
                        DROPPED_TOMBSTONE,
                        expected_generation,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_err()
                {
                    return Err(Status::internal(format!(
                        "DropShard: failed to restore shard {shard_id}'s fence after durable \
                         quarantine failed"
                    )));
                }
                return Err(source);
            }
        };
        map.remove(&shard_id);
        Ok(Some(persisted))
    }

    /// Whether ANY hosted slot currently holds ≥1 query (ADR-093). The `AdoptDict` divergence guard:
    /// the dict is node-shared, so re-basing onto a divergent feature space is refused while any slot
    /// holds data. Snapshots the slot `Arc`s under the lock then queries them lock-free (no guard held
    /// across the engine reads).
    pub(in crate::cluster::server) fn any_slot_populated(&self) -> Result<bool, Status> {
        let slots: Vec<Arc<ShardSlot>> = {
            let map = self
                .shards
                .read()
                .map_err(|_| Status::internal("shard map lock poisoned"))?;
            map.values().cloned().collect()
        };
        for slot in slots {
            if let Some(st) = slot.state.load_full() {
                if st
                    .shard
                    .num_queries()
                    .map_err(|e| Status::internal(e.to_string()))?
                    > 0
                {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Compile + bulk-load raw `(id, DSL)` queries into this shard before serving —
    /// the server-side preload for standing up a populated node. Read-only against the
    /// adopted frozen dict; parse failures are skipped (like `build`/`ingest`). No-op on a
    /// pending (not-yet-adopted) server.
    pub fn ingest_dsl(&self, items: &[(u64, String)]) {
        // Standalone/pre-built preload path (bin demo, node_metrics, dict-shipping setup, unit tests):
        // targets the sole pre-built slot 0. No-op on a pending (not-yet-adopted) node.
        let Ok((_, st)) = self.loaded_slot(0) else {
            return;
        };
        // Stamp the node space's REAL placement (selective at this slot), never
        // `QueryPlacement::standalone()`: `owner()` returns `None` for standalone
        // rows, so an ownership-suppressed cluster read would silently emit
        // nothing for the whole preload — an OK-status zero-FN violation
        // (review finding). The constructor cannot fail for a loaded slot
        // (`num_shards >= 1`, generation >= INITIAL); skip-on-error mirrors the
        // documented parse-failure behavior rather than panicking in lib code.
        let space = self.node_dict.load();
        let Some(space) = space.as_ref() else {
            return;
        };
        let Ok(placement) = crate::ownership::QueryPlacement::selective(
            space.placement_generation,
            space.num_shards,
            vec![0],
        ) else {
            debug_assert!(false, "slot-0 selective placement is always constructible");
            return;
        };
        let mut lc = String::new();
        let extracted: Vec<PlacedQuery> = items
            .iter()
            .filter_map(|(logical, dsl)| {
                let ast = crate::dsl::parse(dsl).ok()?;
                let ex = extract_readonly(&ast, &self.norm, &st.dict, &mut lc);
                Some(PlacedQuery {
                    logical: *logical,
                    ex,
                    dsl: dsl.clone(),
                    version: 1,
                    source_generation: None,
                    tags: Vec::new(),
                    tag_ids: Vec::new(),
                    rank: crate::rank::RankValues::default(),
                    placement: placement.clone(),
                })
            })
            .collect();
        st.shard.ingest_local(&extracted);
    }
}
