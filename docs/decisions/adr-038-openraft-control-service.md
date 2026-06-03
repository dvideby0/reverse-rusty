# ADR-038: openraft backend behind the `ControlPlane` seam + gRPC `ControlService` (clustering step 5b)

> [Back to the decisions index](../DECISIONS.md)


- **Status:** Accepted (increment 5b — the real consensus backend; the durable-query-log quiesce fix is 5c,
  roadmap).
- **Context:** ADR-037 (step 5a) shipped the dependency-free `trait ControlPlane` seam + an in-memory backend
  and froze the seam's shape *for openraft* (membership distinct from `propose`, a `ForwardToLeader` error,
  snapshot-read, app-epoch ≠ Raft term). Step 5b drops the real consensus engine in behind that **unchanged**
  seam, plus the cross-process transport for the managers' own consensus. The engine choice (`openraft`, not
  tikv/raft-rs) was settled with the maintainer in ADR-037: it owns the dangerous parts (joint-consensus
  membership + snapshots) a zero-false-negatives project should not hand-roll, is actively maintained, and is
  `distributed`-gated so the lean core never sees it.
- **Decision (`engine/src/cluster/control_raft.rs` + `control_server.rs`, `distributed`-gated):**
  - **Dependency.** `openraft = "=0.9.24"` (latest STABLE — the 0.10 line is alpha), `optional`, in the
    `distributed` feature only. Features: `serde` (the Raft messages cross the wire), `storage-v2` (the
    non-deprecated split `RaftLogStorage` + `RaftStateMachine` traits — the legacy `RaftStorage` + `Adaptor`
    would trip our `-D warnings` clippy lane), `generic-snapshot-data` (ship the tiny cluster-state snapshot
    whole via `full_snapshot`, no chunked streaming). `cargo deny` accepts the tree (openraft is Apache-2.0;
    its transitives are MIT/Apache/BSD — no allowlist change needed).
  - **Type config.** `declare_raft_types!(TypeConfig: D = ClusterStateChange, R = ClusterStateResponse)` —
    `ClusterStateChange` (ADR-037's "future log-entry payload") IS the Raft log entry; `NodeId = u64`,
    `Node = BasicNode` (its `addr` is the gRPC endpoint the transport already passes).
  - **State machine reuses the ONE apply funnel.** `RaftStateMachine::apply` routes a committed
    `Normal(ClusterStateChange)` through `control::apply` — the SAME function `InMemoryControlPlane` uses
    (made `pub(super)`) — so the two backends are live ≡ replay by construction. A `Membership` entry derives
    `ClusterState::voters` from the Raft voter set (the faithful `change_membership` mapping); a `Blank`
    leader-marker is a no-op. The state machine + an in-memory log store complete the openraft storage traits.
  - **`RaftControlPlane` is the seam impl.** `cluster_state` → `ensure_linearizable` then read the SM;
    `propose` → `client_write`; `change_membership` → `Raft::change_membership`; `leader` →
    `current_leader`. openraft's `ForwardToLeader` maps 1:1 onto `ControlError::ForwardToLeader`, so the
    coordinator changes **no call site**. The sync seam bridges onto async Raft with `handle.block_on` (off
    the runtime's worker threads — exactly the `RemoteShard` bridge; the control plane is never on the
    per-title hot path).
  - **Cross-process transport.** A new `ControlService` (3 RPCs: AppendEntries / Vote / Snapshot) added to the
    **existing** `engine/grpc/proto/shard.proto` (one FDS, no `build.rs` change). The wire is an **opaque
    `bytes` envelope** carrying the serde-encoded Raft message — the proto need not mirror openraft's intricate,
    version-coupled message types; the handler's `Result<_, RaftError>` is encoded *inside* the reply, only
    transport failures surface as a gRPC status. A tonic-backed `RaftNetwork`/`RaftNetworkFactory` (lazy
    per-target clients) is the client; `ControlServer` is the server (relays each RPC to the local Raft
    handler), served via the same port-race-safe `serve_with_incoming` as `ShardServer`. New bin
    `controlserver` (a manager node; `--bootstrap` forms the initial cluster).
  - **No coordinator change.** The seam already accepts any `Box<dyn ControlPlane>` (ADR-037); the default
    backend stays `InMemoryControlPlane`, so every existing oracle is byte-identical and green. The backend is
    exercised through the public `trait ControlPlane` — the exact surface the coordinator depends on.
- **The load-bearing design subtlety.** A faithful Raft proof is inherently **multi-node**: a lone node cannot
  satisfy a voter-set change, and openraft commits its own `Blank`/`Membership` log entries, so the semantic
  `ClusterState::epoch` is **not** comparable to the in-memory backend's under the same script. So the openraft
  backend gets its OWN multi-node differential (3 real nodes converge to the same voters/nodes/assignments/model
  the in-memory backend reaches — NOT epoch), rather than slotting into ADR-037's single-handle
  `control_plane_backends_agree` test. `ClusterState::voters` is openraft-membership-derived in this backend (it
  ends at the same set the in-memory backend reaches via `change_membership`).
- **Honest scope (carried forward from ADR-037).** This does **not** close ADR-036's recovery-quiesce window —
  that needs a durable/replicated *per-shard query log* (the ES translog), a distinct mechanism scheduled as
  **5c**. Also deferred: a durable `RaftLogStorage` (CRC-framed, reusing `storage::crc32` + `durable_rename`) +
  restart-recovery (the in-memory log proves convergence, not crash recovery); TLS/auth on the control
  transport; an allocator that *acts* on the shard→node map (5a/5b commit map-only changes).
- **Consequence:** The cluster-state control plane is now backed by a real, battle-tested consensus engine
  behind the same seam — multi-process elections, leader failover, and committed-state durability across a
  leader death — with the lean core untouched (openraft is absent from the `--no-default-features` dependency
  graph). Proven by `tests/cluster_control_raft_oracle.rs` (a 3-node in-process cluster converges to the
  in-memory document; a follower `propose` returns `ForwardToLeader`; `change_membership` routes to Raft; and —
  over real gRPC `ControlService` servers on localhost — the cluster elects a leader, survives that leader being
  killed, re-elects from quorum, preserves the committed document, and accepts a fresh write). Full `check.sh`
  green (fmt + clippy ×3 incl. lean-core + tests ×2 incl. distributed + audit + deny).
- **See also:** ADR-037 (the seam this fills + the shape choices that made it drop-in), ADR-031 (the
  `ClusterLog` seam + one-`apply`-funnel this mirrors), ADR-029/034 (the gRPC transport + dict shipping the
  `ControlService` sits alongside), ADR-036 (the data-path HA + the 5c quiesce gap), ADR-033 (shared-nothing —
  consensus holds the cluster-state doc only, never query mutations), `src/cluster/control_raft.rs`,
  `src/cluster/control_server.rs`, `src/bin/controlserver.rs`, `engine/grpc/proto/shard.proto`
  (`ControlService`), `tests/cluster_control_raft_oracle.rs`.

