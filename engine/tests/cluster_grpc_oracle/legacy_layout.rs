//! ADR-080 layout handshake (P1, codex review): a coordinator that routes the broad lane on a
//! per-title broad-eval shard ASSUMES every shard holds the replicated lane. So it must REFUSE a
//! remote shard server that does not attest the replicate-to-all layout (`broad_replicate_all`
//! false — a pre-ADR-080 server keeps broad only on shard 0), else broad matches off shard 0 are
//! silently dropped (a false negative — the cardinal sin). A real ADR-080 `ShardServer` attests
//! `true`, so the happy path is exercised by EVERY other oracle in this suite (they all connect to
//! real servers); here we stand up a MOCK service that attests `false` — a stand-in for a populated
//! pre-ADR-080 server — and prove BOTH connect paths (`connect` and `connect_and_adopt`) fail loud,
//! with a real server as the control. Mirrors the dict / tag-dict fingerprint handshake refusals.

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

/// A minimal mock `ShardService` that answers the connect-time handshake with MATCHING dict +
/// tag-dict fingerprints but `broad_replicate_all = false` — i.e. it looks like a populated
/// pre-ADR-080 server (the dict identity checks pass; only the layout attestation differs). Every
/// other RPC is `unimplemented`: the connect guard rejects before any of them is reached.
struct LegacyLayoutServer {
    dict_fp: u64,
    tag_fp: u64,
}

#[tonic::async_trait]
impl ShardService for LegacyLayoutServer {
    async fn dict_fingerprint(
        &self,
        _req: Request<raw::Empty>,
    ) -> Result<Response<raw::DictFingerprintReply>, Status> {
        Ok(Response::new(raw::DictFingerprintReply {
            fingerprint: self.dict_fp,
            tag_dict_fingerprint: self.tag_fp,
            // The crux: a pre-ADR-080 server omits the field (proto3 false).
            broad_replicate_all: false,
        }))
    }

    async fn adopt_dict(
        &self,
        req: Request<raw::AdoptDictRequest>,
    ) -> Result<Response<raw::AdoptDictReply>, Status> {
        // Echo the shipped fingerprints (so the fingerprint checks pass) but attest the OLD layout,
        // so connect_and_adopt is rejected by the layout guard, not the fingerprint guard.
        let r = req.into_inner();
        Ok(Response::new(raw::AdoptDictReply {
            fingerprint: r.fingerprint,
            tag_dict_fingerprint: r.tag_dict_fingerprint,
            broad_replicate_all: false,
        }))
    }

    // ---- never reached on the connect path: stub everything else out. ----
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
}

/// Both connect paths refuse a server that does not attest the replicate-to-all broad layout;
/// a real ADR-080 server (the control) connects cleanly.
#[test]
fn grpc_connect_refuses_a_pre_adr080_broad_layout() {
    let norm = Arc::new(vocab());
    let dict = frozen_dict_with(&[], &norm);
    let dict_fp = dict.fingerprint();
    let tag_fp = empty_tag_dict().fingerprint(); // matches ShardServer::new's finalized empty space

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    // The legacy mock: matching fingerprints, but broad_replicate_all = false.
    let legacy_addr = {
        let _enter = rt.enter();
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("bind");
        let addr = incoming.local_addr().expect("addr");
        let svc = ShardServiceServer::new(LegacyLayoutServer { dict_fp, tag_fp });
        rt.spawn(
            tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_incoming(incoming),
        );
        addr
    };
    wait_until_listening(legacy_addr);
    let legacy_ep = format!("http://{legacy_addr}");

    let names_the_layout = |e: &ShardError| {
        let m = e.to_string();
        m.contains("replicate-to-all") || m.contains("broad_replicate_all")
    };

    // 1. The bare `connect` handshake refuses the legacy layout, naming the cause.
    match RemoteShard::connect(&legacy_ep, rt.handle().clone(), dict_fp, tag_fp, 0) {
        Err(e) => assert!(
            names_the_layout(&e),
            "connect refusal must name the layout: {e}"
        ),
        Ok(_) => panic!("connect SUCCEEDED against a pre-ADR-080 broad layout"),
    }

    // 2. The connect_and_adopt path refuses it too — a populated old server whose dict matches ours
    // would otherwise adopt as an idempotent no-op and slip past the fingerprint checks.
    let dict_bytes = reverse_rusty::storage::serialize_dict(&dict);
    let tag_bytes = reverse_rusty::storage::serialize_tagdict(&empty_tag_dict());
    match RemoteShard::connect_and_adopt(
        &legacy_ep,
        rt.handle().clone(),
        dict_bytes,
        dict_fp,
        tag_bytes,
        tag_fp,
        0,
    ) {
        Err(e) => assert!(
            names_the_layout(&e),
            "adopt-path refusal must name the layout: {e}"
        ),
        Ok(_) => panic!("connect_and_adopt SUCCEEDED against a pre-ADR-080 broad layout"),
    }

    // Control: a REAL ADR-080 ShardServer attests the layout, so connect succeeds.
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
        .expect("a real ADR-080 server attests the replicate-to-all layout");
}
