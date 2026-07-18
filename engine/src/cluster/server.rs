//! `ShardServer` — serves the gRPC `ShardService` over ONE in-process `LocalShard`.
//!
//! Construct it over the SAME frozen `Arc<Dict>` / `Arc<Normalizer>` the coordinator
//! uses for placement. The write path carries raw DSL (not pre-extracted feature
//! ids), so the server re-compiles read-only against ITS copy of that dict — a
//! dict-agnostic wire that fails loud on mismatch rather than corrupting matches.
//! Placement + routing stay the coordinator's job; the server is a dumb executor of
//! `percolate` / `ingest` / `insert` / `delete` / `flush`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use arc_swap::ArcSwapOption;
use tonic::Status;

use crate::cluster::coordinator::shard_dir;
use crate::compile::{extract_readonly, Extracted};
use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::normalize::Normalizer;
use crate::segment::PlacedQuery;
use crate::tagdict::TagDict;

use super::proto::shard_service_server::ShardServiceServer;
use super::security::{ClientSecurity, MeshAuthVerify, ServerSecurity, TlsServerIdentity};
use super::shard::{LocalShard, Shard, ShardError};

/// Tonic's default receive-message ceiling. Operators may lower the application
/// cap but ADR-110 deliberately does not permit raising this transport cliff.
pub const DEFAULT_MAX_GRPC_RESULT_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_GRPC_RESULT_BYTES: usize = DEFAULT_MAX_GRPC_RESULT_BYTES;

mod durable;
mod metrics_source;
mod service;

use durable::{read_adopted_space, restore_durable_slots, sweep_dropped_trash};
pub use metrics_source::ShardMetricsSource;

#[cfg(test)]
mod tests;

struct ServerState {
    dict: Arc<Dict>,
    /// The frozen per-query tag space (ADR-049/055), shipped by the coordinator via `AdoptDict`
    /// alongside the dict. Held so the server resolves ingested tags read-only against the same
    /// space the coordinator's filter `TagId`s came from. Empty until adopted (a pre-built `new`
    /// server starts empty; the coordinator's adopt installs the real one). An `Arc` clone of the
    /// node-scope [`AdoptedSpace`] — every slot on the node shares the one deserialized dict/tag pair.
    tag_dict: Arc<TagDict>,
    shard: LocalShard,
}

/// One hosted shard on a multi-shard node (ADR-093): its swappable engine state + its OWN fence
/// generation. Keying the fence PER SLOT is the codex-P1 fix — fencing one shard for a handoff no
/// longer write-quiesces a co-located shard on the same node (a shared `AtomicU64` could not do this).
struct ShardSlot {
    /// `None` until this slot adopts a dict; reads/writes against a pending slot return
    /// `failed_precondition`.
    state: ArcSwapOption<ServerState>,
    /// The fence generation for THIS slot (ADR-044 semantics, now per-shard, ADR-093): `0` ⇒ not
    /// fenced; `> 0` ⇒ this slot has been demoted at that generation, so its data-mutating writes
    /// return `failed_precondition`. Set monotonically by `Fence`, CAS-cleared by `Unfence`.
    fenced_at_generation: AtomicU64,
    /// Per-RPC service-latency histograms (ADR-100), rendered by the `/_metrics` exposition. On
    /// the SLOT (not the swappable state) so an in-place `recover_from` state swap keeps the
    /// series continuous; a whole-slot replacement is an ordinary Prometheus counter reset.
    latency: super::node_metrics::SlotLatency,
    /// Cumulative broad-lane cost counters (ADR-101), accumulated from each percolate's
    /// `MatchStats` at the handler boundary — same slot-lifetime semantics as `latency`.
    broad: super::node_metrics::SlotBroadCost,
    /// Bounded rank-delivery counters (ADR-110), slot-lifetime like latency.
    ranked: super::node_metrics::SlotRankDelivery,
}

impl ShardSlot {
    /// A slot holding an already-built [`ServerState`], not fenced.
    fn loaded(state: ServerState) -> Arc<Self> {
        Arc::new(ShardSlot {
            state: ArcSwapOption::from(Some(Arc::new(state))),
            fenced_at_generation: AtomicU64::new(0),
            latency: super::node_metrics::SlotLatency::new(),
            broad: super::node_metrics::SlotBroadCost::new(),
            ranked: super::node_metrics::SlotRankDelivery::new(),
        })
    }

    /// This slot's adopted state, or `failed_precondition` if the slot has not adopted a dict yet.
    fn loaded_state(&self) -> Result<Arc<ServerState>, Status> {
        self.state
            .load_full()
            .ok_or_else(|| Status::failed_precondition("shard has not adopted a dict yet"))
    }

    /// Reject a data-mutating write if this slot has been fenced (demoted by a live handoff,
    /// ADR-044). Called by `insert`/`delete`/`ingest` only — reads + the recovery RPCs deliberately
    /// do NOT call it, so a demoted owner keeps serving them until the coordinator stops routing to it
    /// (serve-then-drop), and an in-flight read never hits the fence.
    fn check_not_fenced(&self) -> Result<(), Status> {
        let gen = self.fenced_at_generation.load(Ordering::Acquire);
        if gen > 0 {
            return Err(Status::failed_precondition(format!(
                "shard is fenced at generation {gen} (demoted by a handoff); writes are rejected"
            )));
        }
        Ok(())
    }
}

/// The node-scope adopted feature space (ADR-093): ONE frozen dict + tag dict, deserialized once per
/// node and shared by `Arc` into every slot's [`ServerState`], so co-locating N shards on a node never
/// deserializes N dicts. The node-level `DictFingerprint` handshake reads this, independent of any slot.
struct AdoptedSpace {
    dict: Arc<Dict>,
    tag_dict: Arc<TagDict>,
    placement_generation: crate::ownership::PlacementGeneration,
    num_shards: u32,
}

/// The map of shards this node hosts, keyed by `shard_id` (= global position, ADR-093).
type ShardMap = Arc<RwLock<HashMap<u32, Arc<ShardSlot>>>>;

/// The irrevocable fence value a `DropShard` removal swaps in (ADR-096): no legitimate handoff
/// ever fences at `u64::MAX`, `Fence`'s `fetch_max` can never lower it, and `unfence` explicitly
/// refuses to clear it — so once a slot is tombstoned mid-drop, no concurrent fence traffic
/// (e.g. a stale-fence probe's `unfence(probe)`) can resurrect its writability.
pub(in crate::cluster::server) const DROPPED_TOMBSTONE: u64 = u64::MAX;

/// A node-scope adopted-space cell holding the given (already-deserialized) dict + tag space.
fn node_space_cell(dict: Arc<Dict>, tag_dict: Arc<TagDict>) -> Arc<ArcSwapOption<AdoptedSpace>> {
    Arc::new(ArcSwapOption::from(Some(Arc::new(AdoptedSpace {
        dict,
        tag_dict,
        placement_generation: crate::ownership::PlacementGeneration::INITIAL,
        num_shards: 1,
    }))))
}

/// A shard map holding one slot at shard-id 0 — the pre-built / 1:1 deployment.
fn single_slot(slot: Arc<ShardSlot>) -> ShardMap {
    let mut map = HashMap::new();
    map.insert(0, slot);
    Arc::new(RwLock::new(map))
}

/// A gRPC server wrapping ONE in-process shard.
///
/// The (dict, shard) pair is **swappable**: a server may start *pending* (dict-less) via
/// [`ShardServer::pending`] and adopt the coordinator's frozen dict through the `AdoptDict`
/// RPC, so a data node need not rebuild a byte-identical dict from the corpus out-of-band
/// (ADR-034). `norm` + `config` are fixed for the server's life (the normalizer must still
/// match the coordinator's — `default_vocab()` today; see ADR-034 scope note).
pub struct ShardServer {
    norm: Arc<Normalizer>,
    config: EngineConfig,
    /// `Some` ⇒ a **durable** node: its shard persists segments under this dir (ADR-035), so
    /// the node can serve `FetchSegments` (stream its segments to a recovering peer) and accept
    /// `RecoverFrom` (pull a peer's segments + attach). `None` ⇒ in-memory (today's default).
    /// When set, `AdoptDict` builds a durable (segments-only) shard rather than an in-memory one.
    data_dir: Option<PathBuf>,
    /// The shards this node hosts, keyed by `shard_id` (= global position, ADR-093). ONE process can
    /// host many, each independently adopted / fenced / recovered; the 1:1 deployment holds exactly one
    /// slot (its position). A std `RwLock` keeps the lean dependency tree (no `dashmap`); the read path
    /// clones the slot `Arc` out and drops the guard immediately, so it is NEVER held across an
    /// RPC/`await` (the `recover_from` handler dials a peer). Empty ⇒ pending (awaiting `AdoptDict`).
    shards: ShardMap,
    /// The node-scope adopted dict/tag space (ADR-093): deserialized ONCE, its `Arc`s shared into every
    /// slot's [`ServerState`]. `None` until the first adopt (or, for a durable node, until
    /// `open_durable` reads it back). The node-level `DictFingerprint` handshake reads this — the
    /// dict/tag-dict fingerprints are a node-wide content invariant, independent of any slot.
    node_dict: Arc<ArcSwapOption<AdoptedSpace>>,
    /// Mesh security (ADR-071): TLS identity + expected cluster token, applied by the
    /// `serve*` methods. Default (none) ⇒ the historical plaintext/open behavior.
    security: ServerSecurity,
    /// The CLIENT half of the mesh security (ADR-071) — what THIS node presents when it
    /// dials OUT (the `RecoverFrom` handler's pull from a peer source). Default (none) ⇒
    /// plaintext, the historical behavior.
    client_security: ClientSecurity,
    /// `Some` ⇒ also serve the standard `grpc.health.v1.Health` service on this SEPARATE
    /// plaintext port for Kubernetes liveness/readiness probes (ADR-084). `None` (default)
    /// ⇒ no second listener — byte-identical to the historical single-port behavior.
    health_addr: Option<SocketAddr>,
    /// Exact protobuf encoded-result cap for unary result messages and each
    /// `FetchMatches` stream item (ADR-110).
    max_grpc_result_bytes: usize,
}

impl ShardServer {
    /// Build a server over a fresh `LocalShard` sharing the given frozen `norm`/`dict` —
    /// the pre-built path (the dict is already arranged to match the coordinator's).
    pub fn new(norm: Arc<Normalizer>, dict: Arc<Dict>, config: EngineConfig) -> Self {
        // Pre-built path: starts with an empty tag space; a tagged deployment ships the real one
        // via `AdoptDict` (which rebuilds the shard over it). Empty + finalized so the read-only
        // tag-resolution invariant holds even before an adopt. The node hosts its sole slot at
        // shard-id 0 (ADR-093: the pre-built path is the 1:1 position-0 deployment).
        let tag_dict = Arc::new(finalized_empty_tag_dict());
        let shard = LocalShard::new(
            Arc::clone(&norm),
            Arc::clone(&dict),
            Arc::clone(&tag_dict),
            config.clone(),
        );
        let node_dict = node_space_cell(Arc::clone(&dict), Arc::clone(&tag_dict));
        let shards = single_slot(ShardSlot::loaded(ServerState {
            dict,
            tag_dict,
            shard,
        }));
        ShardServer {
            norm,
            config,
            data_dir: None,
            shards,
            node_dict,
            security: ServerSecurity::default(),
            client_security: ClientSecurity::default(),
            health_addr: None,
            max_grpc_result_bytes: DEFAULT_MAX_GRPC_RESULT_BYTES,
        }
    }

    /// Build a **pending** server: no dict yet, awaiting an `AdoptDict` from the coordinator
    /// (ADR-034). Reads return `failed_precondition` until a dict is adopted. This is how a
    /// data node starts in a real multi-node deploy — empty, then handed the frozen dict —
    /// instead of rebuilding a byte-identical dict from the whole corpus out-of-band.
    pub fn pending(norm: Arc<Normalizer>, config: EngineConfig) -> Self {
        ShardServer {
            norm,
            config,
            data_dir: None,
            shards: Arc::new(RwLock::new(HashMap::new())),
            node_dict: Arc::new(ArcSwapOption::from(None)),
            security: ServerSecurity::default(),
            client_security: ClientSecurity::default(),
            health_addr: None,
            max_grpc_result_bytes: DEFAULT_MAX_GRPC_RESULT_BYTES,
        }
    }

    /// Open (or start) a durable data node at `data_dir` (ADR-072): if the node
    /// previously adopted a dict (persisted alongside its shard state by the durable
    /// `AdoptDict` path), **self-restore** — deserialize the persisted dict + tag
    /// space and reopen the shard from its checkpoint sidecar + translog tail
    /// (ADR-039 §6) — so a restarted container/process resumes serving without
    /// waiting for a coordinator. A fresh directory starts **pending** exactly like
    /// [`Self::pending_durable`]. This is what a deployable node should boot through;
    /// `pending_durable` remains the explicit always-start-empty constructor.
    pub fn open_durable(
        norm: Arc<Normalizer>,
        config: EngineConfig,
        data_dir: PathBuf,
    ) -> Result<Self, ShardError> {
        // Boot hygiene (ADR-096): reclaim any trash-renamed dropped-slot dir whose final delete
        // was interrupted. Best-effort — never fails boot (the ADR-078/079 posture) — and runs
        // BEFORE the adoption branch so a pending node's trash is swept too.
        sweep_dropped_trash(&data_dir);
        // The dict + tag space are ONE atomically-written blob (never desynced); absent
        // ⇒ a never-adopted durable node, which starts pending and adopts on connect.
        let Some((dict_bytes, tag_bytes, placement_generation, num_shards)) =
            read_adopted_space(&data_dir)?
        else {
            return Ok(Self::pending_durable(norm, config, data_dir));
        };
        let dict = Arc::new(crate::storage::deserialize_dict(&dict_bytes).map_err(|e| {
            ShardError::Log(format!(
                "deserializing persisted dict under {}: {e}",
                data_dir.display()
            ))
        })?);
        let tag_dict = Arc::new(
            crate::storage::deserialize_tagdict(&tag_bytes).map_err(|e| {
                ShardError::Log(format!(
                    "deserializing persisted tag dict under {}: {e}",
                    data_dir.display()
                ))
            })?,
        );
        // Restore every slot this node previously hosted from its `shard_<id>/` subdir (ADR-093).
        // Each `new_durable` self-restores via that subdir's checkpoint sidecar (segments attached +
        // translog tail replayed, fingerprint-checked). A fingerprint mismatch fails LOUD
        // (DictMismatch): the durable state was built under a dict that no longer matches the
        // persisted one (a corpus/coordinator change across the restart, ADR-034 divergence); the
        // remedy is to wipe this node's data dir and let the coordinator re-seed it.
        let node_dict = node_space_cell(Arc::clone(&dict), Arc::clone(&tag_dict));
        node_dict.store(Some(Arc::new(AdoptedSpace {
            dict: Arc::clone(&dict),
            tag_dict: Arc::clone(&tag_dict),
            placement_generation,
            num_shards,
        })));
        let slots = restore_durable_slots(&data_dir, &norm, &dict, &tag_dict, &config)?;
        for (&position, slot) in &slots {
            if let Some(state) = slot.state.load_full() {
                state
                    .shard
                    .validate_ownership(position, placement_generation, num_shards)?;
            }
        }
        Ok(ShardServer {
            norm,
            config,
            data_dir: Some(data_dir),
            shards: Arc::new(RwLock::new(slots)),
            node_dict,
            security: ServerSecurity::default(),
            client_security: ClientSecurity::default(),
            health_addr: None,
            max_grpc_result_bytes: DEFAULT_MAX_GRPC_RESULT_BYTES,
        })
    }

    /// A **durable, pending** server (ADR-035/036): empty (awaiting `AdoptDict`) but rooted at
    /// `data_dir`, so once it adopts a dict its shard persists segments there. This is the real
    /// recovering/replica node — after adoption it can serve `FetchSegments` and accept
    /// `RecoverFrom`. The durable analogue of [`Self::pending`].
    pub fn pending_durable(norm: Arc<Normalizer>, config: EngineConfig, data_dir: PathBuf) -> Self {
        ShardServer {
            norm,
            config,
            data_dir: Some(data_dir),
            shards: Arc::new(RwLock::new(HashMap::new())),
            node_dict: Arc::new(ArcSwapOption::from(None)),
            security: ServerSecurity::default(),
            client_security: ClientSecurity::default(),
            health_addr: None,
            max_grpc_result_bytes: DEFAULT_MAX_GRPC_RESULT_BYTES,
        }
    }

    /// A **durable, pre-built** server: build a segments-only durable shard over `dict` rooted
    /// at `data_dir`. The durable analogue of [`Self::new`]. Errors if the durable engine cannot
    /// be created (e.g. the dir is unwritable).
    pub fn new_durable(
        norm: Arc<Normalizer>,
        dict: Arc<Dict>,
        config: EngineConfig,
        data_dir: PathBuf,
    ) -> Result<Self, ShardError> {
        // The sole pre-built slot (shard-id 0) roots its segments at `data_dir/shard_000/` (ADR-093:
        // the per-shard subdir the coordinator's durable layout already uses), not the data_dir root.
        let mut sc = config.clone();
        sc.data_dir = Some(shard_dir(&data_dir, 0));
        let tag_dict = Arc::new(finalized_empty_tag_dict());
        let shard = LocalShard::new_durable(
            Arc::clone(&norm),
            Arc::clone(&dict),
            Arc::clone(&tag_dict),
            sc,
        )?;
        let node_dict = node_space_cell(Arc::clone(&dict), Arc::clone(&tag_dict));
        let shards = single_slot(ShardSlot::loaded(ServerState {
            dict,
            tag_dict,
            shard,
        }));
        Ok(ShardServer {
            norm,
            config,
            data_dir: Some(data_dir),
            shards,
            node_dict,
            security: ServerSecurity::default(),
            client_security: ClientSecurity::default(),
            health_addr: None,
            max_grpc_result_bytes: DEFAULT_MAX_GRPC_RESULT_BYTES,
        })
    }

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
    fn slot(&self, shard_id: u32) -> Result<Arc<ShardSlot>, Status> {
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
    fn loaded_slot(&self, shard_id: u32) -> Result<(Arc<ShardSlot>, Arc<ServerState>), Status> {
        let slot = self.slot(shard_id)?;
        let st = slot.loaded_state()?;
        Ok((slot, st))
    }

    // Both failure messages are frozen (a pre-ADR-111 client retypes them by
    // substring); the ADR-111 ownership code rides as metadata alongside.
    fn validate_placement_config(
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
    fn insert_slot(&self, shard_id: u32, slot: Arc<ShardSlot>) -> Result<(), Status> {
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
    fn remove_slot_if_fenced_at(
        &self,
        shard_id: u32,
        expected_generation: u64,
    ) -> Result<Option<Arc<ShardSlot>>, Status> {
        let mut map = self
            .shards
            .write()
            .map_err(|_| Status::internal("shard map lock poisoned"))?;
        let Some(slot) = map.get(&shard_id) else {
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
        Ok(map.remove(&shard_id))
    }

    /// Whether ANY hosted slot currently holds ≥1 query (ADR-093). The `AdoptDict` divergence guard:
    /// the dict is node-shared, so re-basing onto a divergent feature space is refused while any slot
    /// holds data. Snapshots the slot `Arc`s under the lock then queries them lock-free (no guard held
    /// across the engine reads).
    fn any_slot_populated(&self) -> Result<bool, Status> {
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
                    tags: Vec::new(),
                    tag_ids: Vec::new(),
                    rank: crate::rank::RankValues::default(),
                    placement: placement.clone(),
                })
            })
            .collect();
        st.shard.ingest_local(&extracted);
    }

    /// Install mesh security (ADR-071): a TLS identity to present and/or the
    /// expected cluster token, applied by every `serve*` method. Unset ⇒ the
    /// historical plaintext/open behavior, byte-identical.
    #[must_use]
    pub fn with_security(mut self, security: ServerSecurity) -> Self {
        self.security = security;
        self
    }

    /// Install the CLIENT half of the mesh security (ADR-071) — used when this node
    /// dials OUT (the `RecoverFrom` handler pulls segments + translog from the peer
    /// source). Without it a secured source would reject this node's pull; with it the
    /// internal dial rides the same TLS + token as every coordinator connection.
    #[must_use]
    pub fn with_client_security(mut self, security: ClientSecurity) -> Self {
        self.client_security = security;
        self
    }

    /// Also serve the standard `grpc.health.v1.Health` service on `addr` — a SEPARATE
    /// plaintext port for Kubernetes liveness/readiness probes (ADR-084). Liveness
    /// (`Check("")`) is SERVING once the gRPC server is up; readiness (`Check("ready")`)
    /// tracks dict-adoption — a `--pending` shard is live-but-not-ready until `AdoptDict`.
    /// Unset ⇒ no second listener, byte-identical to the historical single-port behavior.
    #[must_use]
    pub fn with_health_addr(mut self, addr: SocketAddr) -> Self {
        self.health_addr = Some(addr);
        self
    }

    /// Set the static exact encoded-result cap. It may be lowered to any
    /// positive byte count but never raised above tonic's 4 MiB default.
    pub fn with_max_grpc_result_bytes(mut self, bytes: usize) -> Result<Self, ShardError> {
        if !(1..=MAX_GRPC_RESULT_BYTES).contains(&bytes) {
            return Err(ShardError::Config(format!(
                "max gRPC result bytes must be within 1..={MAX_GRPC_RESULT_BYTES}, got {bytes}"
            )));
        }
        self.max_grpc_result_bytes = bytes;
        Ok(self)
    }

    pub(in crate::cluster::server) fn check_result_bytes(
        &self,
        encoded: usize,
    ) -> Result<(), Status> {
        if encoded > self.max_grpc_result_bytes {
            Err(Status::resource_exhausted(format!(
                "encoded result is {encoded} bytes; configured maximum is {}",
                self.max_grpc_result_bytes
            )))
        } else {
            Ok(())
        }
    }

    /// Build the tonic server (TLS applied when configured) + the token-verified
    /// service — one assembly shared by every `serve*` flavor so they cannot drift.
    #[allow(clippy::type_complexity)]
    fn secured_router(self) -> Result<tonic::transport::server::Router, tonic::transport::Error> {
        let security = self.security.clone();
        // Server-side HTTP/2 keepalive (ADR-085): PING idle/half-open CLIENT connections and
        // drop the dead ones, so a crashed coordinator/peer can't leak server resources.
        // Off any hot path; default-on via `ServerSecurity::default`.
        let mut builder = tonic::transport::Server::builder()
            .http2_keepalive_interval(Some(security.keepalive_interval))
            .http2_keepalive_timeout(Some(security.keepalive_timeout));
        if let Some(tls) = &security.tls {
            builder = builder.tls_config(server_tls_config(tls))?;
        }
        // The verifier wraps the WHOLE service (pass-through with no token), so every
        // RPC — including a future one — is covered before its handler runs.
        let verify = MeshAuthVerify::new(security.token);
        Ok(builder.add_service(ShardServiceServer::with_interceptor(self, verify)))
    }

    /// Serve `ShardService` on `addr` until the returned future completes. When a
    /// `--health-addr` was configured ([`with_health_addr`](Self::with_health_addr)), the
    /// plaintext health service runs concurrently on its own port and a watcher tracks
    /// readiness (dict-adoption); the two servers are joined fail-loud (ADR-084).
    pub async fn serve(self, addr: SocketAddr) -> Result<(), tonic::transport::Error> {
        let Some(health_addr) = self.health_addr else {
            return self.secured_router()?.serve(addr).await;
        };
        // Capture a shared handle to the shard map BEFORE `secured_router` consumes `self`. The
        // watcher flips `Check("ready")` to SERVING once any slot adopts a dict — no RPC handler is
        // touched (the shared `Arc<RwLock<…>>` shard map is the seam).
        let reporter = super::health::HealthReporter::serving();
        let shards = Arc::clone(&self.shards);
        super::health::spawn_readiness_watcher(reporter.clone(), move || {
            shards
                .read()
                .is_ok_and(|m| m.values().any(|s| s.state.load_full().is_some()))
        });
        let data = self.secured_router()?.serve(addr);
        let health = super::health::serve_health(health_addr, reporter);
        tokio::try_join!(data, health).map(|_| ())
    }

    /// Serve with a graceful-shutdown `signal` future — used by tests to stop cleanly.
    pub async fn serve_with_shutdown<F>(
        self,
        addr: SocketAddr,
        signal: F,
    ) -> Result<(), tonic::transport::Error>
    where
        F: std::future::Future<Output = ()>,
    {
        self.secured_router()?
            .serve_with_shutdown(addr, signal)
            .await
    }

    /// Serve `ShardService` on an already-bound `incoming` listener (no rebind). Lets a
    /// caller bind the socket first and learn its port — an ephemeral `:0` for tests, or
    /// socket activation in production — without the bind→drop→rebind gap that re-binding
    /// by address would open.
    pub async fn serve_with_incoming(
        self,
        incoming: tonic::transport::server::TcpIncoming,
    ) -> Result<(), tonic::transport::Error> {
        self.secured_router()?.serve_with_incoming(incoming).await
    }
}

/// Build the tonic `ServerTlsConfig` from an operator identity — shared with
/// [`ControlServer`](super::control_server::ControlServer) via the same shapes.
pub(crate) fn server_tls_config(tls: &TlsServerIdentity) -> tonic::transport::ServerTlsConfig {
    tonic::transport::ServerTlsConfig::new().identity(tonic::transport::Identity::from_pem(
        &tls.cert_pem,
        &tls.key_pem,
    ))
}

/// Compile one raw query read-only against the shared frozen dict (parse failure →
/// `None`, counted by the caller as a rejected-parse).
fn compile_item(norm: &Normalizer, dict: &Dict, dsl: &str, lc: &mut String) -> Option<Extracted> {
    let ast = crate::dsl::parse(dsl).ok()?;
    Some(extract_readonly(&ast, norm, dict, lc))
}

/// An empty but FINALIZED tag space — the placeholder a pre-built / pending server holds until the
/// coordinator's `AdoptDict` installs the real one (ADR-055). Finalized so the engine's read-only
/// tag-resolution invariant (`debug_assert!(is_finalized())`) holds even before an adopt.
fn finalized_empty_tag_dict() -> TagDict {
    let mut td = TagDict::new();
    td.mark_finalized();
    td
}
