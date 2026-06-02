//! `ShardServer` — serves the gRPC `ShardService` over ONE in-process `LocalShard`.
//!
//! Construct it over the SAME frozen `Arc<Dict>` / `Arc<Normalizer>` the coordinator
//! uses for placement. The write path carries raw DSL (not pre-extracted feature
//! ids), so the server re-compiles read-only against ITS copy of that dict — a
//! dict-agnostic wire that fails loud on mismatch rather than corrupting matches.
//! Placement + routing stay the coordinator's job; the server is a dumb executor of
//! `percolate` / `ingest` / `insert` / `delete` / `flush`.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use tonic::Status;

use crate::compile::{extract_readonly, Extracted};
use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::normalize::Normalizer;

use super::proto::shard_service_server::ShardServiceServer;
use super::shard::{LocalShard, ShardError};

mod service;

#[cfg(test)]
mod tests;

struct ServerState {
    dict: Arc<Dict>,
    shard: LocalShard,
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
    /// `None` until a dict is adopted; reads against a pending server return
    /// `failed_precondition`.
    state: ArcSwapOption<ServerState>,
    /// The fence generation (ADR-044, step 6b): `0` ⇒ not fenced; `> 0` ⇒ this node has been
    /// demoted as the owner of its shard at that generation, so data-mutating writes
    /// (`insert`/`delete`/`ingest`) return `failed_precondition`. Reads + the recovery RPCs stay
    /// served (serve-then-drop). Set monotonically by the `Fence` RPC (a stale lower-gen Fence
    /// never un-fences). A live handoff fences the old owner, drains its tail to the new owner, then
    /// flips routing — the fence holds a brief write-quiesce across that flip.
    fenced_at_generation: AtomicU64,
}

impl ShardServer {
    /// Build a server over a fresh `LocalShard` sharing the given frozen `norm`/`dict` —
    /// the pre-built path (the dict is already arranged to match the coordinator's).
    pub fn new(norm: Arc<Normalizer>, dict: Arc<Dict>, config: EngineConfig) -> Self {
        let shard = LocalShard::new(Arc::clone(&norm), Arc::clone(&dict), config.clone());
        let state = ArcSwapOption::from(Some(Arc::new(ServerState { dict, shard })));
        ShardServer {
            norm,
            config,
            data_dir: None,
            state,
            fenced_at_generation: AtomicU64::new(0),
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
            state: ArcSwapOption::from(None),
            fenced_at_generation: AtomicU64::new(0),
        }
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
            state: ArcSwapOption::from(None),
            fenced_at_generation: AtomicU64::new(0),
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
        let mut sc = config.clone();
        sc.data_dir = Some(data_dir.clone());
        let shard = LocalShard::new_durable(Arc::clone(&norm), Arc::clone(&dict), sc)?;
        let state = ArcSwapOption::from(Some(Arc::new(ServerState { dict, shard })));
        Ok(ShardServer {
            norm,
            config,
            data_dir: Some(data_dir),
            state,
            fenced_at_generation: AtomicU64::new(0),
        })
    }

    /// The adopted state, or `failed_precondition` if the server is still pending.
    fn loaded(&self) -> Result<Arc<ServerState>, Status> {
        self.state
            .load_full()
            .ok_or_else(|| Status::failed_precondition("shard has not adopted a dict yet"))
    }

    /// Reject a data-mutating write if this node has been fenced (demoted by a live handoff,
    /// ADR-044). Called by `insert`/`delete`/`ingest` only — reads + the recovery RPCs deliberately
    /// do NOT call it, so the demoted owner keeps serving them until the coordinator stops routing
    /// to it (serve-then-drop), and an in-flight read never hits the fence.
    fn check_not_fenced(&self) -> Result<(), Status> {
        let gen = self.fenced_at_generation.load(Ordering::Acquire);
        if gen > 0 {
            return Err(Status::failed_precondition(format!(
                "shard is fenced at generation {gen} (demoted by a handoff); writes are rejected"
            )));
        }
        Ok(())
    }

    /// Compile + bulk-load raw `(id, DSL)` queries into this shard before serving —
    /// the server-side preload for standing up a populated node. Read-only against the
    /// adopted frozen dict; parse failures are skipped (like `build`/`ingest`). No-op on a
    /// pending (not-yet-adopted) server.
    pub fn ingest_dsl(&self, items: &[(u64, String)]) {
        let Some(st) = self.state.load_full() else {
            return;
        };
        let mut lc = String::new();
        let extracted: Vec<(u64, Extracted, String, u32)> = items
            .iter()
            .filter_map(|(logical, dsl)| {
                let ast = crate::dsl::parse(dsl).ok()?;
                let ex = extract_readonly(&ast, &self.norm, &st.dict, &mut lc);
                Some((*logical, ex, dsl.clone(), 1))
            })
            .collect();
        st.shard.ingest_local(&extracted);
    }

    /// Serve `ShardService` on `addr` until the returned future completes.
    pub async fn serve(self, addr: SocketAddr) -> Result<(), tonic::transport::Error> {
        tonic::transport::Server::builder()
            .add_service(ShardServiceServer::new(self))
            .serve(addr)
            .await
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
        tonic::transport::Server::builder()
            .add_service(ShardServiceServer::new(self))
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
        tonic::transport::Server::builder()
            .add_service(ShardServiceServer::new(self))
            .serve_with_incoming(incoming)
            .await
    }
}

/// Compile one raw query read-only against the shared frozen dict (parse failure →
/// `None`, counted by the caller as a rejected-parse).
fn compile_item(norm: &Normalizer, dict: &Dict, dsl: &str, lc: &mut String) -> Option<Extracted> {
    let ast = crate::dsl::parse(dsl).ok()?;
    Some(extract_readonly(&ast, norm, dict, lc))
}
