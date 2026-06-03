# ADR-036: gRPC multi-node per-shard replication + peer recovery (clustering step 4b)

> [Back to the decisions index](../DECISIONS.md)


- **Status:** Accepted.
- **Context:** ADR-035 built per-shard replication + peer recovery **in-process** (the `ReplicatedShard`
  composite + the `peer_recover` primitive). This lifts it onto the gRPC transport — replicas on different
  nodes, with cross-node peer recovery — completing build-path step 4, following the ADR-027 (in-process) →
  ADR-029 (gRPC) rhythm. Behind the off-by-default `distributed` feature.
- **Decision:**
  - **Replicas are remote shards.** A new `ClusterEngine::connect_replicated(groups: &[ShardGroup], …)`
    connects + dict-ships (ADR-034) to every endpoint and wraps each position's primary + replica
    `RemoteShard`s in one `ReplicatedShard` — so the coordinator's placement / routing / merge is identical
    to a non-replicated remote cluster, while reads fail over and writes fan out (ADR-035). `connect_remote`
    (RF=1) is unchanged; a `ShardGroup` with no replicas degenerates to a bare `RemoteShard`.
  - **Servers become durable.** `ShardServer` gains a `data_dir` + `pending_durable`/`new_durable` ctors, and
    `AdoptDict` now builds a **segments-only durable** `LocalShard` when a `data_dir` is set — so the node's
    writes persist `.seg` files, the prerequisite for streaming or attaching segments. In-memory servers
    (today's default, and the dict-shipping oracle) are byte-for-byte unchanged.
  - **`FetchSegments` (server-streaming).** The source seals a consistent snapshot (`seal_for_checkpoint` —
    flush + reseal base tombstones), then streams a **manifest frame first** (the complete `.seg` file set +
    `next_seg_id` + dict fingerprint) followed by a chunked run per file (≤256 KiB `FileChunk`s; `sources.dat`
    last if present). The receiver pre-validates the manifest and **rejects a truncated stream rather than
    attaching a subset** (a subset is a silent shard-sized false negative); files land via tmp+rename. The
    request carries the dict fingerprint and the source refuses a mismatch (never ships segments compiled
    against a divergent feature space).
  - **`RecoverFrom` (target-driven — the Elasticsearch model: the recovering node pulls).** Coordinator
    `peer_recover_replica(source, target, handle)` ships the dict to the fresh node (adopt), then drives its
    `RecoverFrom`, which connects to the source peer, drains `FetchSegments`, attaches the segments
    (`open_segments`, fail-loud on missing/corrupt), and swaps in the recovered shard.
  - **One new dependency, distributed-only:** `tokio-stream` (the `ReceiverStream` wrapper for the
    server-streaming response). The lean core and the default server build are untouched.
- **Honest scope (the load-bearing boundary).** Peer recovery **quiesces writes to the position for the copy
  window.** The full ES "stream segments **+ replay the log tail**" needs a *durable / replicated coordinator
  log* for a remote cluster — but a remote cluster uses `NullClusterLog` (ADR-031's durable log is the
  in-process story), so there is no tail to replay. That snapshot-then-delta replay couples to the Raft
  control plane (step 5) and is deferred. Also deferred: an allocator deciding shard→node placement (the
  caller supplies `ShardGroup`s + recovery endpoints by hand — no membership / failure detector yet), TLS/auth
  (plaintext localhost), and true bounded-memory file streaming (the source reads one segment file into memory
  at a time today).
- **Consequence:** A coordinator can run replicas on separate gRPC nodes that fail over, and bring a fresh
  node up by streaming a peer's segments. Proven by `tests/cluster_grpc_oracle.rs`'s new
  `grpc_replicated_failover_and_peer_recovery`: K=3 × RF=2 durable servers, `connect_replicated` ≡ brute;
  stopping a primary still serves correct reads via its replica (failover — which also proves ingest fanned
  out to the replica); and a fresh node peer-recovers a position's segments from a live peer and then serves
  that position correctly inside a verify cluster. Full `check.sh` green (incl. the `clippy (distributed)` +
  `tests (distributed)` lanes).
- **See also:** ADR-035 (the in-process composite + `peer_recover` this lifts onto gRPC), ADR-029 (the
  transport + the DSL-on-wire invariant), ADR-034 (dict shipping — reused per endpoint), ADR-031/032 (the
  coordinator log + per-shard durable segments peer recovery streams), ADR-033 (the shared-nothing model),
  `engine/grpc/proto/shard.proto`, `src/cluster/{server,remote,coordinator}.rs`, `src/bin/shardserver.rs`,
  `tests/cluster_grpc_oracle.rs`.

