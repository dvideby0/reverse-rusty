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

use tonic::{Request, Response, Status};

use crate::compile::{extract_readonly, Extracted};
use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::normalize::Normalizer;

use super::proto;
use super::proto::shard_service_server::{ShardService, ShardServiceServer};
use super::shard::{LocalShard, Shard};

/// A gRPC server wrapping ONE in-process shard.
pub struct ShardServer {
    shard: LocalShard,
    norm: Arc<Normalizer>,
    dict: Arc<Dict>,
}

impl ShardServer {
    /// Build a server over a fresh `LocalShard` sharing the given frozen `norm`/`dict`.
    pub fn new(norm: Arc<Normalizer>, dict: Arc<Dict>, config: EngineConfig) -> Self {
        let shard = LocalShard::new(Arc::clone(&norm), Arc::clone(&dict), config);
        ShardServer { shard, norm, dict }
    }

    /// Compile + bulk-load raw `(id, DSL)` queries into this shard before serving —
    /// the server-side preload for standing up a populated node. Read-only against the
    /// shared frozen dict; parse failures are skipped (like `build`/`ingest`).
    pub fn ingest_dsl(&self, items: &[(u64, String)]) {
        let mut lc = String::new();
        let extracted: Vec<(u64, Extracted, String, u32)> = items
            .iter()
            .filter_map(|(logical, dsl)| {
                let ast = crate::dsl::parse(dsl).ok()?;
                let ex = extract_readonly(&ast, &self.norm, &self.dict, &mut lc);
                Some((*logical, ex, dsl.clone(), 1))
            })
            .collect();
        self.shard.ingest_local(&extracted);
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
            .shard
            .class_counts()
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(proto::ClassCountsReply {
            counts: counts.to_vec(),
        }))
    }

    async fn ingest_extracted(
        &self,
        request: Request<proto::IngestRequest>,
    ) -> Result<Response<proto::IngestReply>, Status> {
        let items = request.into_inner().items;
        let mut lc = String::new();
        let mut rejected_parse = 0u64;
        let mut extracted: Vec<(u64, Extracted, String, u32)> = Vec::with_capacity(items.len());
        for it in items {
            match compile_item(&self.norm, &self.dict, &it.dsl, &mut lc) {
                Some(ex) => extracted.push((it.logical_id, ex, it.dsl, it.version.max(1))),
                None => rejected_parse += 1,
            }
        }
        let report = self.shard.ingest_local(&extracted);
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
        let item = request
            .into_inner()
            .item
            .ok_or_else(|| Status::invalid_argument("InsertRequest.item is required"))?;
        let mut lc = String::new();
        let Some(ex) = compile_item(&self.norm, &self.dict, &item.dsl, &mut lc) else {
            // The coordinator already parsed before placing, so this should not happen;
            // report "not inserted" rather than fabricate a memtable id.
            return Ok(Response::new(proto::InsertReply {
                present: false,
                local_id: 0,
            }));
        };
        let out = self
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
            .shard
            .delete_by_logical_id(request.into_inner().logical_id)
            .map_err(|e| Status::internal(e.to_string()))? as u64;
        Ok(Response::new(proto::DeleteReply { removed }))
    }

    async fn flush(
        &self,
        _request: Request<proto::FlushRequest>,
    ) -> Result<Response<proto::FlushReply>, Status> {
        self.shard
            .flush()
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(proto::FlushReply {}))
    }
}
