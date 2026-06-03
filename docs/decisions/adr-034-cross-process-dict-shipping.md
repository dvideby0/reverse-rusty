# ADR-034: Cross-process dict shipping over gRPC (the first shared-nothing multi-node step)

> [Back to the decisions index](../DECISIONS.md)


- **Status:** Accepted.
- **Context:** ADR-029/030 built the gRPC `ShardServer`/`RemoteShard` transport and a connect-time
  dict-fingerprint *handshake* (a divergent dict fails loud, not silently). But the handshake only *verifies*;
  it never *ships*. So a shard server had to obtain a **byte-identical frozen dict out-of-band** — in practice
  by rebuilding it from the **entire corpus** (`shardserver.rs` ran a full extract pass over the queries just
  to construct the dict). That is the opposite of a data node you can stand up empty, and it was the headline
  caveat on the cross-process transport. Under the shared-nothing realignment (ADR-033), making the existing
  transport actually deployable cross-process is the first concrete multi-node step.
- **Decision:** The coordinator **ships its authoritative frozen dict to each server at connect.**
  - **A new `AdoptDict` RPC.** Payload = the dict serialized by the existing core
    `crate::storage::serialize_dict` + the coordinator's `Dict::fingerprint` of it (an integrity check; the
    server recomputes and rejects a mismatch as `invalid_argument`). Reuses the *exact* bytes the cluster
    manifest already persists — no new serialization surface.
  - **Servers can start *pending* (dict-less).** New `ShardServer::pending(norm, config)` holds its
    `(dict, shard)` behind an `ArcSwapOption` (the codebase's `ArcSwap` snapshot idiom); reads against a
    pending server return `failed_precondition`. `ShardServer::new(norm, dict, config)` (pre-built) is kept,
    signature unchanged.
  - **Adoption contract (the load-bearing part).** On `AdoptDict`: **empty** shard (pending, or zero
    queries) → adopt (build a fresh `LocalShard` over the shipped dict); **same** fingerprint already held →
    idempotent no-op; **non-empty** shard whose dict **differs** → refuse with `failed_precondition`, because
    re-basing already-loaded data onto a different feature space would silently corrupt matches. The client
    (`RemoteShard::connect_and_adopt`) maps that refusal to `ShardError::DictMismatch` (reading back the
    server's actual fingerprint), so the silent-FN guard from ADR-030 is *preserved* — just relocated to where
    it is a real risk (a *committed* server), since adopting onto an empty server is correct, not an error.
  - **`connect_remote` ships by default.** It serializes the dict once and adopts per endpoint. Shipping an
    identical dict to a pre-built server is an idempotent no-op (the fingerprint matches), so existing callers
    (and the gRPC oracle's pre-built-server test) are behavior-preserved; the returned fingerprint *is* the
    handshake. No `ClusterConfig` change.
  - **Scope — dict only.** The fingerprint (and thus shipping) covers the **dict**. The **normalizer** must
    still match on both sides; everything uses `Normalizer::default_vocab()` today, which is corpus-independent
    and reproduced identically on any node, so the default case works end-to-end after shipping. Shipping +
    fingerprinting the vocab→normalizer is the explicit next hardening, **deferred** here.
- **Consequence:** A data node starts **empty** and is handed the frozen dict by the coordinator — no corpus,
  no out-of-band dict coordination. Proven by `tests/cluster_grpc_oracle.rs`: a new
  `grpc_cluster_with_dict_shipping` stands up K **pending** servers, ships the dict via `connect_remote`, and
  asserts the cluster ≡ single-node ≡ brute (broad on/off); the divergence test is updated to load data first
  (an empty server correctly *adopts*, so the guard now fires on a populated server holding a divergent dict);
  a `server.rs` unit test exercises every arm of the adoption contract. All behind the off-by-default
  `distributed` feature (lean core untouched). **Deferred:** normalizer/vocab shipping + fingerprint, TLS/auth
  on the transport, and the per-shard replication / Raft control-plane steps (ADR-033 roadmap).
- **See also:** ADR-029 (the transport + the DSL-on-wire invariant this completes), ADR-030 (the
  dict-fingerprint handshake this turns from verify-only into ship-then-verify), ADR-033 (the shared-nothing
  realignment this is the first step of), ADR-027 (the one-frozen-dict invariant), `src/cluster/server.rs`
  (`pending` + `AdoptDict`), `src/cluster/remote.rs` (`connect_and_adopt`), `src/cluster/coordinator.rs`
  (`connect_remote`), `engine/grpc/proto/shard.proto`, `tests/cluster_grpc_oracle.rs`.

