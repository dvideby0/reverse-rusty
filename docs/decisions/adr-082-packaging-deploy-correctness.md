# ADR-082: Packaging follow-on — control-plane advertise-URL + coordinator-gated class-D

**Status:** Accepted (2026-06-22)

**Context.** [ADR-081](adr-081-deployment-packaging-runbook.md) shipped the production packaging and
recorded four deferrals. Two were deploy-time *correctness* gaps small enough to close directly — and
verifying them against the code corrected one outright:

- **The bootstrap node advertises a wildcard address.** `controlserver --bootstrap` committed
  `format!("{scheme}://{bind}")` into Raft membership. With the containerized `0.0.0.0:50061` bind that is
  `https://0.0.0.0:50061`, which peers cannot dial — so a real multi-node control quorum could not form
  (ADR-081 deferral (a), the advertise-URL half).
- **The "class-D on the remote topology" deferral was a phantom.** ADR-081, the coordinator's own
  startup `warn!`, and the runbook §11 all claimed `shardserver` has no `--accept-class-d`, so a
  class-D-accepting coordinator would have its writes *"dropped on a shard that rejects them,"* and told
  operators to set a nonexistent shard flag. Verifying against the code: every remote shard is built
  through `LocalShard::{new,new_durable,open_segments}`, each of which **forces
  `config.accept_class_d = true`** — *"a shard must ACCEPT whatever the coordinator places; the operator's
  front-door knob lives on the coordinator"* (ADR-068/080). `ShardServer` wraps a `LocalShard` on every
  path (`new`, `pending`→`AdoptDict`, `open_durable`, recovery), so class-D already works on the remote
  topology, coordinator-gated. The gap did not exist; the *warning and docs describing it* were the bug.

**Decision.** Two binary + deploy-doc fixes, **no library/hot-path change**:

- **`controlserver --advertise-url <URL>`** — the routable self-URL a bootstrap node commits into Raft
  membership, factored into a pure, unit-tested `bootstrap_self_url`: an explicit advertise URL wins
  verbatim; otherwise the bind address is used **only when routable**; a wildcard bind (`0.0.0.0` / `::`)
  at `--bootstrap` with no advertise-URL **fails loud** (committing an undialable address is refused),
  matching the file's existing fail-loud-on-misconfig stance. An advertise scheme that disagrees with the
  node's TLS identity warns loud. `deploy/compose.cluster.yml`'s `control0` now passes
  `--advertise-url https://control0:50061` (without it, the new guard refuses its wildcard-bind bootstrap).
- **Class-D is coordinator-gated, full stop** — drop the misleading `shardserver --accept-class-d`
  follow-on entirely. The coordinator's `--accept-class-d` (on `server`) is the **sole** gate; remote
  shards force-accept whatever they are placed, so there is no per-shard flag or config parity to
  maintain. Removed the phantom coordinator `warn!`, removed the runbook §11 "not covered" bullet, and
  documented the real contract in runbook §3. Locked by a new gRPC oracle test
  (`tests/cluster_grpc_oracle::class_d`): K=3 servers built with `EngineConfig::default()`
  (`accept_class_d = false`) still store a class-D query on every shard and serve it over the wire —
  matching a title free of the forbidden term, excluding one that carries it, quarantined with broad off.

**Consequences.** A containerized control quorum now forms with routable membership (unblocking — not
completing — the deferred `--control-endpoint` coordinator↔quorum wiring, which remains open as the next
follow-on). `--advertise-url` takes effect at the **first** bootstrap only (`Raft::initialize` is
idempotent), so an existing ADR-081 deployment that already committed a wildcard-URL membership must
reset its idle control-plane volumes to adopt the new URL — documented in the runbook; live membership
repair is deferred to the wiring follow-on, harmless until then because the quorum is not yet consulted.
The class-D operator story collapses to one knob on the coordinator, proven over the wire, with no
per-shard parity. The default (non-`distributed`) build is byte-identical; the only behavior change is
the deliberate, loud refusal of a `--bootstrap` on a wildcard bind without `--advertise-url`.
