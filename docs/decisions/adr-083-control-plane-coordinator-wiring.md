# ADR-083: Control-plane ↔ coordinator wiring — the `RemoteControlPlane` client

**Status:** Accepted (2026-06-22)

**Context.** [ADR-081](adr-081-deployment-packaging-runbook.md) deferral (a): a deployed coordinator ran
the dependency-free `InMemoryControlPlane`, re-deriving placement deterministically from the frozen
dict + ring on every (re)start. The durable openraft `controlserver` quorum (ADR-038/041) was shipped
but **idle** — never consulted — so runtime membership / assignment / resize decisions were lost on a
coordinator restart and could not be HA across managers. [ADR-082](adr-082-packaging-deploy-correctness.md)
fixed the advertise-URL prerequisite so the quorum can form with routable membership; this ADR wires the
coordinator to it. Two facts shaped the design: the `ControlService` exposed only the raw Raft *peer*
RPCs (`AppendEntries`/`Vote`/`Snapshot`) — **no client-facing surface** for an application to read or
propose — and the coordinator is, by the ADR-070 model, **stateless** (durability lives on the shard
nodes; a restart re-mints the dict and reconnects).

**Decision.** Attach the coordinator as a **thin, stateless gRPC client** of the quorum — it does NOT
join consensus (a learner would make the stateless query router a Raft member and needs an unsolved
self-join protocol). This realizes the seam [ADR-037](adr-037-control-plane-seam.md) designed for:
swap the `Box<dyn ControlPlane>` backend without touching any coordinator call site.

- **`ClientControl` RPC** — one opaque `RaftEnvelope` RPC added to `ControlService`, reusing the
  existing byte-pipe pattern (the request/reply are serde-encoded `control_wire` enums, so protobuf
  carries no control-plane schema). `ControlServer` gains `with_client_plane(Arc<RaftControlPlane>)`
  and serves the op as **native async** (the sync `ControlPlane` methods `block_on` internally, which
  would nest on a gRPC worker), reusing the SAME openraft calls + error mapping the embedded backend
  uses — so the remote path is live ≡ the in-process backend. `ControlServer::new(raft)` stays
  Raft-only (the ADR-038 oracle compiles unchanged); a Raft-only server answers `ClientControl` with
  `unimplemented` — never a silently-wrong reply.
- **`RemoteControlPlane`** — implements the sync `ControlPlane` trait over a `ControlServiceClient`,
  mirroring `RemoteShard`'s `block_on_in_context` sync→async bridge. A follower's `ForwardToLeader`
  (preserved 1:1 on the wire) is followed transparently: redial the named leader, retry once. Any
  RPC/transport failure surfaces as `ControlError::Backend` — never a swallowed stale read of the
  assignment map.
- **Wiring** — `ClusterEngine::with_control_plane` swaps the backend; the cluster-mode server's
  `--control-endpoint <URL>` (remote mode only — fail-loud otherwise) builds a `RemoteControlPlane`
  over the same mesh security as the shard links and injects it before ingest. Absent the flag, the
  in-memory backend stays — byte-identical to before.

**Safety (load-bearing).** The control plane is read ONLY on the admin/introspection path
(`control_state` / `assignment_for` / `register_node` / `reassign_shard` / `rebalance` / `resize`),
**never during percolation** (routing is deterministic from the frozen dict + ring). So this change
**cannot produce a false negative** — the worst failure mode is a contained admin error, surfaced loud.
The oracle pins it: `percolate` is byte-identical across a `register_node` that commits through the
remote quorum.

**Scope boundary.** This makes the cluster-state *document* durable + quorum-HA (membership /
assignment / resize / introspection go through Raft). It does **not** change the routing path to
resolve shard addresses from committed assignments — the remote coordinator still routes by its
`--shard-endpoint` list, as before. Dynamic membership-driven address resolution is a separate, larger
feature, out of scope here. The ADR-082 caveat carries: `--advertise-url` takes at first bootstrap
only, so an existing deployment adopting a wired quorum resets its idle control volumes once.

**Proven.** `tests/cluster_control_wiring_oracle.rs` (feature `distributed`): the `RemoteControlPlane`
round-trips the whole trait against a real `ControlServer` (genesis read / version / propose / leader);
injected into a coordinator, `register_node` commits through the quorum and `percolate` is
byte-identical before/after; and a follower forwards a read + a propose to the leader. The ADR-038
control-raft oracle is unchanged (the Raft-only `ControlServer::new` is preserved).
