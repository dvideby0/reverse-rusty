//! `ShardServer` — serves the gRPC `ShardService` over ONE in-process `LocalShard`.
//!
//! Construct it over the SAME frozen `Arc<Dict>` / `Arc<Normalizer>` the coordinator
//! uses for placement. The write path carries raw DSL (not pre-extracted feature
//! ids), so the server re-compiles read-only against ITS copy of that dict — a
//! dict-agnostic wire that fails loud on mismatch rather than corrupting matches.
//! Placement + routing stay the coordinator's job; the server is a dumb executor of
//! `percolate` / `ingest` / `insert` / `delete` / `flush`.

use std::net::SocketAddr;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use tonic::{Request, Response, Status};

use crate::compile::{extract_readonly, Extracted};
use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::normalize::Normalizer;

use super::proto;
use super::proto::shard_service_server::{ShardService, ShardServiceServer};
use super::shard::{LocalShard, Shard};

/// The adopted feature space + the shard compiled over it. Held behind an
/// [`ArcSwapOption`] in [`ShardServer`] so a server can start *pending* (no dict) and
/// adopt one shipped by the coordinator at connect (ADR-034).
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
    /// `None` until a dict is adopted; reads against a pending server return
    /// `failed_precondition`.
    state: ArcSwapOption<ServerState>,
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
            state,
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
            state: ArcSwapOption::from(None),
        }
    }

    /// The adopted state, or `failed_precondition` if the server is still pending.
    fn loaded(&self) -> Result<Arc<ServerState>, Status> {
        self.state
            .load_full()
            .ok_or_else(|| Status::failed_precondition("shard has not adopted a dict yet"))
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

#[tonic::async_trait]
impl ShardService for ShardServer {
    async fn percolate(
        &self,
        request: Request<proto::PercolateRequest>,
    ) -> Result<Response<proto::PercolateReply>, Status> {
        let req = request.into_inner();
        let (ids, stats) = self
            .loaded()?
            .shard
            .percolate(&req.title, req.include_broad)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(proto::PercolateReply {
            ids,
            stats: Some(proto::stats_from_engine(stats)),
        }))
    }

    async fn num_queries(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::CountReply>, Status> {
        let count = self
            .loaded()?
            .shard
            .num_queries()
            .map_err(|e| Status::internal(e.to_string()))? as u64;
        Ok(Response::new(proto::CountReply { count }))
    }

    async fn class_counts(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::ClassCountsReply>, Status> {
        let counts = self
            .loaded()?
            .shard
            .class_counts()
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(proto::ClassCountsReply {
            counts: counts.to_vec(),
        }))
    }

    async fn dict_fingerprint(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::DictFingerprintReply>, Status> {
        Ok(Response::new(proto::DictFingerprintReply {
            fingerprint: self.loaded()?.dict.fingerprint(),
        }))
    }

    /// Adopt a frozen dict shipped by the coordinator (ADR-034). The wire carries the
    /// serialized dict (`crate::storage::serialize_dict`) + the coordinator's fingerprint
    /// of it. Contract:
    /// - bad bytes / fingerprint disagreeing with the deserialized dict → `invalid_argument`
    ///   (a corrupt or version-skewed ship, never silently trusted);
    /// - **empty** shard (pending, or adopted-but-no-data) → adopt: build a fresh
    ///   `LocalShard` over the shipped dict;
    /// - already adopted the **same** dict → idempotent no-op;
    /// - **non-empty** shard whose dict **differs** → `failed_precondition`: refuse, because
    ///   re-basing already-loaded data onto a different feature space would silently corrupt
    ///   matches. The coordinator surfaces this as `ShardError::DictMismatch`.
    ///
    /// Single-coordinator / adopt-before-ingest is the intended use; concurrent adopts are
    /// not synchronized beyond the atomic state swap (last writer wins, both with the same
    /// dict in practice).
    async fn adopt_dict(
        &self,
        request: Request<proto::AdoptDictRequest>,
    ) -> Result<Response<proto::AdoptDictReply>, Status> {
        let req = request.into_inner();
        let dict = crate::storage::deserialize_dict(&req.dict)
            .map_err(|e| Status::invalid_argument(format!("deserializing shipped dict: {e}")))?;
        let fp = dict.fingerprint();
        if fp != req.fingerprint {
            return Err(Status::invalid_argument(format!(
                "shipped dict integrity check failed: bytes fingerprint to {fp:#018x} but the \
                 request claims {:#018x}",
                req.fingerprint
            )));
        }

        let adopt = match self.state.load_full().as_deref() {
            // Already serving this exact dict → nothing to do.
            Some(st) if st.dict.fingerprint() == fp => false,
            // A different dict is already in place; only safe to replace if no data depends on it.
            Some(st) => {
                let n = st
                    .shard
                    .num_queries()
                    .map_err(|e| Status::internal(e.to_string()))?;
                if n > 0 {
                    return Err(Status::failed_precondition(format!(
                        "shard holds {n} queries under dict {:#018x}; refusing to adopt a \
                         divergent dict {fp:#018x} (re-basing loaded data is unsafe)",
                        st.dict.fingerprint()
                    )));
                }
                true // adopted but empty → safe to re-adopt
            }
            // Pending → adopt.
            None => true,
        };

        if adopt {
            let dict = Arc::new(dict);
            let shard = LocalShard::new(
                Arc::clone(&self.norm),
                Arc::clone(&dict),
                self.config.clone(),
            );
            self.state
                .store(Some(Arc::new(ServerState { dict, shard })));
        }

        Ok(Response::new(proto::AdoptDictReply { fingerprint: fp }))
    }

    async fn ingest_extracted(
        &self,
        request: Request<proto::IngestRequest>,
    ) -> Result<Response<proto::IngestReply>, Status> {
        let st = self.loaded()?;
        let items = request.into_inner().items;
        let mut lc = String::new();
        let mut rejected_parse = 0u64;
        let mut extracted: Vec<(u64, Extracted, String, u32)> = Vec::with_capacity(items.len());
        for it in items {
            match compile_item(&self.norm, &st.dict, &it.dsl, &mut lc) {
                Some(ex) => extracted.push((it.logical_id, ex, it.dsl, it.version.max(1))),
                None => rejected_parse += 1,
            }
        }
        let report = st.shard.ingest_local(&extracted);
        Ok(Response::new(proto::IngestReply {
            ingested: report.ingested as u64,
            rejected_parse: rejected_parse + report.rejected_parse as u64,
            rejected_class_d: report.rejected_class_d as u64,
        }))
    }

    async fn insert_extracted(
        &self,
        request: Request<proto::InsertRequest>,
    ) -> Result<Response<proto::InsertReply>, Status> {
        let st = self.loaded()?;
        let item = request
            .into_inner()
            .item
            .ok_or_else(|| Status::invalid_argument("InsertRequest.item is required"))?;
        let mut lc = String::new();
        let Some(ex) = compile_item(&self.norm, &st.dict, &item.dsl, &mut lc) else {
            // The coordinator already parsed before placing, so this should not happen;
            // report "not inserted" rather than fabricate a memtable id.
            return Ok(Response::new(proto::InsertReply {
                present: false,
                local_id: 0,
            }));
        };
        let out = st
            .shard
            .insert_extracted(&ex, item.logical_id, item.version.max(1), &item.dsl)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(proto::InsertReply {
            present: out.is_some(),
            local_id: out.unwrap_or(0),
        }))
    }

    async fn delete(
        &self,
        request: Request<proto::DeleteRequest>,
    ) -> Result<Response<proto::DeleteReply>, Status> {
        let removed = self
            .loaded()?
            .shard
            .delete_by_logical_id(request.into_inner().logical_id)
            .map_err(|e| Status::internal(e.to_string()))? as u64;
        Ok(Response::new(proto::DeleteReply { removed }))
    }

    async fn flush(
        &self,
        _request: Request<proto::FlushRequest>,
    ) -> Result<Response<proto::FlushReply>, Status> {
        self.loaded()?
            .shard
            .flush()
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(proto::FlushReply {}))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tonic::{Code, Request};

    use super::proto::shard_service_server::ShardService;
    use super::{proto, ShardServer};
    use crate::compile::extract;
    use crate::config::EngineConfig;
    use crate::dict::Dict;
    use crate::normalize::Normalizer;
    use crate::storage::serialize_dict;

    fn norm() -> Arc<Normalizer> {
        Arc::new(Normalizer::default_vocab().expect("built-in vocab"))
    }

    /// A frozen dict interned over `snips` in order (mirrors the gRPC oracle helper).
    fn frozen_dict(snips: &[&str], norm: &Normalizer) -> Dict {
        let mut d = Dict::new();
        let mut lc = String::new();
        for q in snips {
            if let Ok(ast) = crate::dsl::parse(q) {
                let _ = extract(&ast, norm, &mut d, &mut lc);
            }
        }
        d.finalize_mask();
        d
    }

    fn adopt_req(dict: &Dict) -> Request<proto::AdoptDictRequest> {
        Request::new(proto::AdoptDictRequest {
            dict: serialize_dict(dict),
            fingerprint: dict.fingerprint(),
        })
    }

    fn current_fp(srv: &ShardServer) -> u64 {
        srv.state.load_full().expect("adopted").dict.fingerprint()
    }

    /// Exercises every arm of the `AdoptDict` contract through the real async handler:
    /// pending-read-fails, empty→adopt, same-fp→no-op, bad-fp→invalid, empty-different→re-adopt,
    /// and non-empty-divergent→refuse (the load-bearing silent-FN guard).
    #[test]
    fn adopt_dict_state_machine() {
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        let n = norm();
        let d1 = frozen_dict(&["1994 upper deck", "psa 10"], &n);
        let d2 = frozen_dict(&["1994 upper deck", "psa 10", "1995 fleer ultra"], &n);
        assert_ne!(
            d1.fingerprint(),
            d2.fingerprint(),
            "test setup: the two dicts must differ"
        );

        let srv = ShardServer::pending(Arc::clone(&n), EngineConfig::default());
        // Pending: reads fail loud rather than fabricating an empty result.
        assert!(srv.state.load_full().is_none());
        let err = rt
            .block_on(srv.num_queries(Request::new(proto::Empty {})))
            .expect_err("pending read must fail");
        assert_eq!(err.code(), Code::FailedPrecondition);

        // Empty → adopt d1.
        let fp = rt
            .block_on(srv.adopt_dict(adopt_req(&d1)))
            .expect("adopt onto empty")
            .into_inner()
            .fingerprint;
        assert_eq!(fp, d1.fingerprint());
        assert_eq!(current_fp(&srv), d1.fingerprint());

        // Same dict again → idempotent no-op.
        rt.block_on(srv.adopt_dict(adopt_req(&d1)))
            .expect("re-adopt same dict is a no-op");
        assert_eq!(current_fp(&srv), d1.fingerprint());

        // Integrity: d2 bytes but d1's claimed fingerprint → invalid_argument.
        let bad = Request::new(proto::AdoptDictRequest {
            dict: serialize_dict(&d2),
            fingerprint: d1.fingerprint(),
        });
        assert_eq!(
            rt.block_on(srv.adopt_dict(bad))
                .expect_err("fingerprint mismatch must be rejected")
                .code(),
            Code::InvalidArgument
        );

        // Empty shard, different valid dict → re-adopt allowed (no data at risk).
        rt.block_on(srv.adopt_dict(adopt_req(&d2)))
            .expect("re-adopt onto still-empty shard");
        assert_eq!(current_fp(&srv), d2.fingerprint());

        // Load data, then a DIVERGENT dict → refused (the silent-FN guard).
        srv.ingest_dsl(&[(1u64, "1994 upper deck".to_string())]);
        let n_loaded = rt
            .block_on(srv.num_queries(Request::new(proto::Empty {})))
            .expect("count after load")
            .into_inner()
            .count;
        assert!(n_loaded >= 1, "expected loaded data, got {n_loaded}");
        assert_eq!(
            rt.block_on(srv.adopt_dict(adopt_req(&d1)))
                .expect_err("divergent dict on a non-empty shard must be refused")
                .code(),
            Code::FailedPrecondition
        );
        // The SAME dict on a non-empty shard is still a no-op (not refused).
        rt.block_on(srv.adopt_dict(adopt_req(&d2)))
            .expect("same dict on a populated shard is a no-op");
        assert_eq!(current_fp(&srv), d2.fingerprint());
    }
}
