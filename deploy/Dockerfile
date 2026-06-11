# Reverse Rusty node image (ADR-072) — one image, role chosen by command:
#   server        the HTTP API (single-node, or `--cluster` coordinator mode — ADR-070)
#   shardserver   a gRPC data node (durable with --data-dir; mesh TLS+token — ADR-071)
#   controlserver a cluster-manager (openraft) node
#
# Build from the repo root:  docker build -f deploy/Dockerfile .
# Multi-stage: the builder compiles with `--features distributed` (the gRPC stack);
# the runtime carries only the three binaries on a slim base.

# Match engine/rust-toolchain.toml — bump together.
FROM rust:1.95-slim-trixie AS builder
WORKDIR /src
COPY engine/ engine/
RUN cd engine \
    && cargo build --release --features distributed \
       --bin server --bin shardserver --bin controlserver \
    && mkdir -p /out \
    && cp target/release/server target/release/shardserver target/release/controlserver /out/

FROM debian:trixie-slim
# curl: the compose healthchecks + the harness probe through it. ca-certificates:
# operators may front the mesh with real CAs.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/server /out/shardserver /out/controlserver /usr/local/bin/
# Non-root: the bins need only their --data-dir volumes. /data is pre-created and
# rr-owned so a fresh named volume mounted there inherits the ownership (Docker
# initializes new named volumes from the image path) — without this the volume is
# root-owned and the durable stores fail with EACCES.
RUN useradd --system --create-home --uid 10001 rr \
    && mkdir -p /data && chown rr /data
USER rr
ENTRYPOINT []
CMD ["server", "--help"]
