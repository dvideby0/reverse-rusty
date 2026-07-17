//! ADR-109 ownership handshake: a coordinator must refuse a pre-ADR-109 server that cannot attest
//! its placement generation and shard count, and a server carrying a stale generation must fail
//! closed on adoption. Otherwise the coordinator could trust a reply whose rows use a different
//! emission-owner function. A real `ShardServer` attests both fields, so the happy path is exercised
//! by every other oracle in this suite; this file supplies the old/stale peer controls.

use std::pin::Pin;
use std::sync::Arc;

use raw::shard_service_server::{ShardService, ShardServiceServer};
use reverse_rusty::cluster::{RemoteShard, ShardError, ShardServer};
use reverse_rusty::config::EngineConfig;
use reverse_rusty_shard_proto as raw;
use tokio_stream::Stream;
use tonic::transport::server::TcpIncoming;
use tonic::{Request, Response, Status};

use crate::harness::*;

/// A minimal mock `ShardService` with matching feature-space fingerprints and a configurable
/// ownership attestation. Every other RPC is unimplemented: the connect guard rejects first.
struct LegacyOwnershipServer {
    dict_fp: u64,
    tag_fp: u64,
    placement_generation: u64,
    num_shards: u32,
}

#[tonic::async_trait]
impl ShardService for LegacyOwnershipServer {
    async fn dict_fingerprint(
        &self,
        _req: Request<raw::Empty>,
    ) -> Result<Response<raw::DictFingerprintReply>, Status> {
        Ok(Response::new(raw::DictFingerprintReply {
            fingerprint: self.dict_fp,
            tag_dict_fingerprint: self.tag_fp,
            broad_replicate_all: true,
            placement_generation: self.placement_generation,
            num_shards: self.num_shards,
        }))
    }

    async fn adopt_dict(
        &self,
        req: Request<raw::AdoptDictRequest>,
    ) -> Result<Response<raw::AdoptDictReply>, Status> {
        // Echo the shipped fingerprints so only the ownership attestation decides the result.
        let r = req.into_inner();
        Ok(Response::new(raw::AdoptDictReply {
            fingerprint: r.fingerprint,
            tag_dict_fingerprint: r.tag_dict_fingerprint,
            broad_replicate_all: true,
            placement_generation: self.placement_generation,
            num_shards: self.num_shards,
        }))
    }

    // ---- never reached on the connect path: stub everything else out. ----
    async fn add_shard(
        &self,
        _req: Request<raw::AddShardRequest>,
    ) -> Result<Response<raw::AddShardReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    async fn percolate(
        &self,
        _req: Request<raw::PercolateRequest>,
    ) -> Result<Response<raw::PercolateReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    async fn num_queries(
        &self,
        _req: Request<raw::ShardRef>,
    ) -> Result<Response<raw::CountReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    async fn class_counts(
        &self,
        _req: Request<raw::ShardRef>,
    ) -> Result<Response<raw::ClassCountsReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    async fn ingest_extracted(
        &self,
        _req: Request<raw::IngestRequest>,
    ) -> Result<Response<raw::IngestReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    async fn insert_extracted(
        &self,
        _req: Request<raw::InsertRequest>,
    ) -> Result<Response<raw::InsertReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    async fn delete(
        &self,
        _req: Request<raw::DeleteRequest>,
    ) -> Result<Response<raw::DeleteReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    async fn flush(
        &self,
        _req: Request<raw::FlushRequest>,
    ) -> Result<Response<raw::FlushReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }

    type FetchSegmentsStream =
        Pin<Box<dyn Stream<Item = Result<raw::FetchSegmentsChunk, Status>> + Send>>;
    async fn fetch_segments(
        &self,
        _req: Request<raw::FetchSegmentsRequest>,
    ) -> Result<Response<Self::FetchSegmentsStream>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    async fn recover_from(
        &self,
        _req: Request<raw::RecoverFromRequest>,
    ) -> Result<Response<raw::RecoverFromReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }

    type FetchTranslogStream =
        Pin<Box<dyn Stream<Item = Result<raw::TranslogEntry, Status>> + Send>>;
    async fn fetch_translog(
        &self,
        _req: Request<raw::FetchTranslogRequest>,
    ) -> Result<Response<Self::FetchTranslogStream>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    async fn retention_lease(
        &self,
        _req: Request<raw::RetentionLeaseRequest>,
    ) -> Result<Response<raw::RetentionLeaseReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    async fn fence(
        &self,
        _req: Request<raw::FenceRequest>,
    ) -> Result<Response<raw::FenceReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    async fn unfence(
        &self,
        _req: Request<raw::UnfenceRequest>,
    ) -> Result<Response<raw::UnfenceReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    async fn list_shards(
        &self,
        _req: Request<raw::Empty>,
    ) -> Result<Response<raw::ListShardsReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    async fn drop_shard(
        &self,
        _req: Request<raw::DropShardRequest>,
    ) -> Result<Response<raw::DropShardReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
    async fn content_fingerprint(
        &self,
        _req: Request<raw::ContentFingerprintRequest>,
    ) -> Result<Response<raw::ContentFingerprintReply>, Status> {
        Err(Status::unimplemented("legacy mock"))
    }
}

/// Both connect paths refuse an old peer with no ownership attestation; adoption also refuses a
/// nonzero but stale generation. A real ADR-109 server is the positive control.
#[test]
fn grpc_connect_refuses_missing_or_stale_ownership_attestation() {
    let norm = Arc::new(vocab());
    let dict = frozen_dict_with(&[], &norm);
    let dict_fp = dict.fingerprint();
    let tag_fp = empty_tag_dict().fingerprint(); // matches ShardServer::new's finalized empty space

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    let start_mock = |placement_generation, num_shards| {
        let _enter = rt.enter();
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind");
        let addr = incoming.local_addr().expect("addr");
        let svc = ShardServiceServer::new(LegacyOwnershipServer {
            dict_fp,
            tag_fp,
            placement_generation,
            num_shards,
        });
        rt.spawn(
            tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_incoming(incoming),
        );
        wait_until_listening(addr);
        format!("http://{addr}")
    };
    let legacy_ep = start_mock(0, 0);

    // 1. The bare handshake refuses a proto3-defaulted pre-ADR-109 peer.
    match RemoteShard::connect(&legacy_ep, rt.handle().clone(), dict_fp, tag_fp, 0) {
        Err(ShardError::OwnershipMismatch(_)) => {}
        Err(e) => panic!("expected typed ownership refusal, got {e}"),
        Ok(_) => panic!("connect SUCCEEDED against a pre-ADR-109 peer"),
    }

    // 2. The adopt path refuses the same old peer.
    let dict_bytes = reverse_rusty::storage::serialize_dict(&dict);
    let tag_bytes = reverse_rusty::storage::serialize_tagdict(&empty_tag_dict());
    match RemoteShard::connect_and_adopt(
        &legacy_ep,
        rt.handle().clone(),
        dict_bytes.clone(),
        dict_fp,
        tag_bytes.clone(),
        tag_fp,
        0,
    ) {
        Err(ShardError::OwnershipMismatch(_)) => {}
        Err(e) => panic!("expected typed ownership refusal, got {e}"),
        Ok(_) => panic!("connect_and_adopt SUCCEEDED against a pre-ADR-109 peer"),
    }

    // 3. A nonzero but stale peer is also refused; zero-only checking would miss this.
    let stale_ep = start_mock(2, 1);
    match RemoteShard::connect_and_adopt(
        &stale_ep,
        rt.handle().clone(),
        dict_bytes,
        dict_fp,
        tag_bytes,
        tag_fp,
        0,
    ) {
        Err(ShardError::OwnershipMismatch(_)) => {}
        Err(e) => panic!("expected stale-generation ownership refusal, got {e}"),
        Ok(_) => panic!("connect_and_adopt SUCCEEDED against a stale ownership generation"),
    }

    // Control: a real ADR-109 server attests generation + shard count, so connect succeeds.
    let real_addr = {
        let _enter = rt.enter();
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind");
        let addr = incoming.local_addr().expect("addr");
        let server = ShardServer::new(
            Arc::clone(&norm),
            Arc::clone(&dict),
            EngineConfig::default(),
        );
        rt.spawn(server.serve_with_incoming(incoming));
        addr
    };
    wait_until_listening(real_addr);
    let real_ep = format!("http://{real_addr}");
    RemoteShard::connect(&real_ep, rt.handle().clone(), dict_fp, tag_fp, 0)
        .expect("a real ADR-109 server attests ownership configuration");
}
