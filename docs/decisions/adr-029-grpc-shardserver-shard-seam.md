# ADR-029: gRPC `ShardServer` + the local↔remote `trait Shard` seam (clustering step 1, networking)

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted


- **Context:** ADR-027 built clustering steps 1–2 in-process and explicitly deferred step 1's *networking*
  half — "lift the shard behind a `ShardServer` (gRPC) so a shard can be remote (the local↔remote
  `trait Shard` seam)." This ADR builds exactly that: the seam plus a `tonic` transport, behind an
  off-by-default `distributed` feature, proven by a gRPC differential oracle. It builds on ADR-028's
  lean-core feature split (the `distributed` deps slot beside the `server` ones).
- **Decision** — six load-bearing sub-decisions:
  1. **`trait Shard` abstracts the OPERATION, not the data.** A remote shard has no in-process
     `EngineSnapshot`, so the seam exposes `percolate(title, include_broad) -> (ids, MatchStats)` (the
     body of the old `query_shard`), never `snapshot()`. The in-process struct was renamed
     `Shard → LocalShard`; `RemoteShard` (a gRPC client) is the second impl. The coordinator holds
     `Vec<Box<dyn Shard>>` — **dynamic dispatch**, because a cluster of mixed local + remote shards is
     the whole point; one vtable hop is negligible against a match or an RPC. (`assert_send_sync` in
     `lib.rs` still guards `ClusterEngine: Send + Sync`.)
  2. **The seam is fallible** (`Result<_, ShardError>` on every method). A `LocalShard` never errs; a
     `RemoteShard` errs on transport failure. Surfacing that — instead of swallowing it into an empty
     result — is load-bearing for **zero false negatives**: a dropped shard probe would silently shrink
     the union. The coordinator's runtime methods propagate it; `build` stays **infallible** (it only
     ever makes `LocalShard`s and ingests via the inherent infallible `LocalShard::ingest_local`). The
     distributed load path is the new `ClusterEngine::ingest` (the analog of `build`'s pass B, over the
     seam).
  3. **Sync trait + `block_on` bridge.** The coordinator fans probes out via rayon (sync), so the trait
     stays sync; `RemoteShard` holds a `tokio::runtime::Handle` and blocks on its async tonic client
     internally, confining all async to that type and leaving the coordinator + `LocalShard` + the
     in-process oracle untouched. Safe because rayon workers are not tokio workers (no nested-runtime
     panic). Trade-off: a parked rayon worker per in-flight RPC — an async fan-out is the documented
     later optimization.
  4. **The write path ships raw DSL, not pre-extracted `FeatureId`s.** Raw ids are valid only if both
     sides' dicts are byte-identical; sending DSL keeps the wire dict-agnostic and lets the server
     re-compile read-only against ITS frozen dict (exactly what `add_query` does in-process), so a dict
     mismatch fails **loud** rather than corrupting matches. **Key constraint:** every `ShardServer` must
     be built over a byte-identical frozen dict — the ADR-027 shared-dict invariant, extended across the
     wire (in-test the `Arc<Dict>` is literally shared). Placement stays coordinator-only; the server is
     a dumb executor.
  5. **Codegen is isolated in a workspace sub-crate** (`reverse-rusty-shard-proto`, `engine/grpc/`),
     compiled with the **pure-Rust `protox`** compiler so neither dev nor CI needs a system `protoc`. The
     engine depends on it + `tonic` only under `distributed`; the `RemoteShard`/`ShardServer` glue stays
     in `src/cluster/{remote,server}.rs` behind cfg. *Why a sub-crate, not an in-crate `build.rs`:* a
     build script cannot see `#[cfg(feature = "distributed")]` (Cargo passes features to build scripts
     only as runtime env vars), so optional codegen deps can't be conditionally invoked in-crate without
     making them non-optional — which would drag `protox`/`tonic-prost-build` into the lean core (ADR-028).
     No generated code is checked in; it is regenerated from `proto/shard.proto` on every build.
  6. **No TLS** (plaintext localhost) this increment — avoids pulling `rustls`/`ring`/`openssl` into the
     `cargo deny` license surface; transport security + auth are a later step.
- **Why correct:** `tests/cluster_grpc_oracle.rs` stands up K = 3 real `ShardServer`s on localhost,
  assembles a `ClusterEngine` of `RemoteShard`s, loads the corpus over the `IngestExtracted` RPC, and
  asserts the gRPC-backed cluster returns EXACTLY the independent brute-force oracle's set AND the
  single-node engine's set, broad on and off — plus a live add → percolate → remove over the
  Insert/Delete RPCs. The seam refactor is otherwise behavior-preserving: the in-process
  `cluster_oracle.rs` stays green, dependency-free, on the default build.
- **Alternatives considered:**
  - *Infallible seam (swallow or panic on RPC error)* — rejected; swallowing is a false negative,
    panicking violates the no-panic-in-library rule (and `panic = "abort"` would fail-stop the process).
  - *Async trait + async fan-out now* — deferred; large blast radius across the coordinator's public API
    and the synchronous oracle. Sync + `block_on` is correct and contained; revisit for remote-fan-out
    throughput.
  - *In-crate `build.rs` codegen* — rejected; can't gate codegen deps without polluting the lean core.
  - *Commit the generated code* — rejected; checked-in generated noise + manual regen drift. The
    sub-crate auto-regenerates via `protox`.
  - *Send pre-extracted `Extracted` over the wire* — rejected; only valid under byte-identical dicts and
    fails silently if they diverge (4).
- **Consequence:** Clustering build-path **step 1 is complete** (in-process core + gRPC transport).
  Surface (behind `distributed`): `cluster::{ShardServer, RemoteShard}`,
  `ClusterEngine::{connect_remote, ingest}`, the `shardserver` bin, and the gRPC oracle. **Out of scope
  (later steps):** durable externalized log / read-your-writes quorum, Raft cluster-manager, object-store
  segments, multi-process dict shipping (the connect-time dict-hash handshake itself landed — ADR-030), autoscaling, auto-split,
  TLS/auth, async remote fan-out, and production panic-isolation at the RPC boundary.
- **Known sharp edges (live in the shipped surface, distinct from the unbuilt work above):**
  - *Unchecked cross-process dict identity → silent false negatives.* `ShardServer::new` and
    `connect_remote` both take the frozen dict from the caller with NO verification that the coordinator's
    and the servers' dicts match. In-process and the localhost oracle share one `Arc<Dict>`, so it holds;
    across a real process boundary a diverged dict drops matches **silently** — the one false-negative
    path the fallible seam does not catch. The `shardserver` bin builds its own dict and exposes no way to
    ship it, so it is **not yet correctly consumable by a separate coordinator**. *Cheap mitigation before
    full dict-shipping: exchange a dict fingerprint at connect / first RPC and error on mismatch — turns a
    silent FN into a loud failure.* **→ DONE (ADR-030): the handshake landed; a divergent dict now fails
    the connect with `ShardError::DictMismatch`. Full dict-shipping is still deferred, so cross-process use
    still requires matching dicts — but it no longer fails *silently*.** **→ Dict-shipping LANDED (ADR-034):
    `connect_remote` now ships the frozen dict to each server, so a data node need not rebuild it from the
    corpus out-of-band.**
  - *The `MatchStats` wire map is unverified.* `cluster_grpc_oracle.rs` asserts matched-ID sets, not the
    11 round-tripped stats fields, so a transposition in `cluster/proto.rs` would go undetected. *Cheap
    fix: assert a stats round-trip.* **→ DONE (ADR-030): a `proto.rs` round-trip unit test (by field name,
    both directions) + a gRPC-vs-in-process stats equality check in the oracle.**
  - *No transport auth + plaintext:* any client can call `Delete`/`Flush`/`IngestExtracted`. Localhost-only.
  - *`panic = "abort"`* fail-stops a shard process on a handler panic.
  - *Grown audit surface:* the workspace now locks the full tonic tree, so `cargo audit`/`deny` cover
    crates a non-`distributed` build never compiles (the compiled lean core is unchanged).
- **See also:** ADR-027 (the in-process core this extends), ADR-028 (the feature-gating seam `distributed`
  reuses), [`clustering-and-scaling.md`](../design/clustering-and-scaling.md) §10 (step 1),
  `engine/grpc/` (the proto sub-crate), `src/cluster/{shard,remote,server,proto}.rs`,
  `src/bin/shardserver.rs`, `tests/cluster_grpc_oracle.rs`.


