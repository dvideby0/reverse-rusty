//! Generated gRPC `ShardService` for reverse-rusty's distributed shard transport.
//!
//! This crate is intentionally thin: it carries ONLY the code generated from
//! [`proto/shard.proto`](../proto/shard.proto) by `build.rs` (pure-Rust `protox` →
//! `tonic-prost-build`, so no system `protoc` is ever required). The engine pulls it
//! in only under its `distributed` feature; the `RemoteShard` client and
//! `ShardServer` glue live in the engine crate (`src/cluster/{remote,server}.rs`),
//! which import the types re-exported here. See engine ADR-029.
//!
//! Regeneration is automatic at build time from the `.proto` — there is no
//! checked-in generated code to drift.

// Generated code does not follow the workspace lint policy; silence it here so a
// `--workspace` clippy run stays clean without touching the generated output.
#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    missing_docs,
    rustdoc::all
)]

tonic::include_proto!("reverse_rusty.shard.v1");

/// The standard `grpc.health.v1` health-checking service (ADR-084), namespaced so its
/// generic message names (`HealthCheckRequest`, etc.) don't collide with the shard
/// types at the crate root. Served on a separate plaintext port for k8s probes.
pub mod health {
    tonic::include_proto!("grpc.health.v1");
}
