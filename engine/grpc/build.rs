//! Compile `proto/shard.proto` into tonic client + server + prost message code.
//!
//! Uses the pure-Rust `protox` compiler (no system `protoc` dependency, in dev or
//! CI) to produce a `FileDescriptorSet`, then hands it to `tonic-prost-build` for
//! the gRPC codegen. Output lands in `OUT_DIR` and is pulled in by `src/lib.rs` via
//! `tonic::include_proto!`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // shard.proto = the data transport (ADR-029); health.proto = the standard
    // grpc.health.v1 service for k8s probes (ADR-084). One FileDescriptorSet over
    // both → one codegen pass → two package modules in OUT_DIR.
    let fds = protox::compile(["proto/shard.proto", "proto/health.proto"], ["proto"])?;
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_fds(fds)?;
    println!("cargo:rerun-if-changed=proto/shard.proto");
    println!("cargo:rerun-if-changed=proto/health.proto");
    Ok(())
}
