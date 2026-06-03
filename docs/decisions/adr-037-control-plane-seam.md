# ADR-037: Cluster-state control-plane seam behind `trait ControlPlane` (clustering step 5a)

> [Back to the decisions index](../DECISIONS.md)


- **Status:** Accepted (increment 5a — the dependency-free seam; the openraft backend is 5b, the quiesce-gap
  fix is 5c, both roadmap).
- **Context:** Build-path step 5 is the **quorum/Raft control plane**: a small, quorum-replicated
  cluster-state document (consistent-hash ring params + the **shard→node map** + membership + feature-model
  version + an epoch) — the Elasticsearch cluster-manager model the shared-nothing design (ADR-033 §4.3)
  commits to. It is also what unblocks the two honest-scope gaps ADR-036 left (shard→node placement /
  membership, and eventually the recovery-quiesce window). Pulling a consensus library is a heavy-dependency
  decision, so two forks were settled with the maintainer first: **(1) seam-first** — build a dependency-free
  `ControlPlane` seam + an in-memory backend now, proven by an oracle, exactly as `trait ClusterLog` +
  `NullClusterLog` (ADR-031) preceded any real durability engine, so the consensus engine drops in behind a
  stable firewall; **(2) control-plane state only** — consensus holds the small, low-rate cluster-state doc,
  **never** the ~750k/sec query mutations (those stay on `ClusterLog` + the per-shard primary→replica path,
  ADR-031/035/036) nor the per-shard segment registry (that stays in the local `ClusterManifest`, ADR-032);
  **(3) target engine = openraft** (step 5b) — it owns the dangerous parts (joint-consensus membership +
  snapshots) a zero-false-negatives project should not hand-roll, is actively maintained (unlike tikv/raft-rs,
  now in maintenance mode), and is async/`distributed`-gated so the **lean core never sees it**. The
  lean-dependency philosophy is real but not absolute — battle-tested consensus that owns the perilous parts
  is worth the (feature-gated) weight.
- **Decision (5a, this increment — `engine/src/cluster/control.rs`, lean core, no new dependency):**
  - **The seam.** `trait ControlPlane: Send + Sync` — sync, fallible (`Result<_, ControlError>`), the
    document-mutation + linearizable-read sibling of `ClusterLog`. Methods: `cluster_state()` (a cheap
    `Arc<ClusterState>` snapshot read), `version()`, `propose(ClusterStateChange)`, `change_membership(voters)`,
    `leader()`. **Not** a log-append seam — a consensus library owns its own log, so the seam abstracts
    *committed state* + *proposals*, not framed bytes.
  - **The document.** `ClusterState { epoch, nodes, voters, assignments, num_shards, vnodes, dict_fingerprint,
    model_version }` (`serde`, self-contained — the future Raft snapshot payload); `ClusterStateChange`
    (`AddNode`/`RemoveNode`/`AssignShard`/`BumpModelVersion`) is the future log-entry payload; `NodeId`
    (newtype), `NodeRole`, `NodeDescriptor`, `ShardAssignment`, `StateVersion`, and a typed `ControlError`.
  - **The backend.** `InMemoryControlPlane` applies every proposal immediately and is always `Ok` (a single
    node trivially has a quorum) — the `NullClusterLog` analogue + the fast differential-test backend.
    `single_node(num_shards, vnodes, dict_fingerprint)` is the default the coordinator builds: one
    `NodeId(0)` owning every position, so the RF=1 / in-process path is **byte-identical** to pre-ADR-037.
  - **Coordinator wiring.** `ClusterEngine` gains `control: Box<dyn ControlPlane>` threaded through the
    existing `ClusterDurable` bundle (no `from_parts` signature change); `build`/`open`/`connect_*` default it
    to `single_node`. New introspection: `control_state()`, `assignment_for(position)` (errors loudly on an
    unassigned live position — never a silent default, the fail-closed stance), `reassign_shard()`. The
    placement/route/apply/percolate hot path is **untouched** — the control plane is read at
    assembly/introspection time only.
  - **Shape choices baked in for openraft (so 5b changes no call site).** `ControlError::ForwardToLeader`
    exists from day one (a follower's `client_write` returns it); `change_membership` is **distinct** from
    `propose` (joint consensus is special in Raft — folding it in would force a re-cut); reads are a snapshot
    *pull*, not a watch (openraft has no watch of an application document); `ClusterState.epoch` (an app
    counter) is kept distinct from the future Raft term **and** from `ClusterManifest.epoch` (the local
    checkpoint generation) — three distinct notions, deliberately not unified.
- **Honest scope (the correction to carry forward).** The roadmap shorthand "the Raft step unblocks the
  quiesce-during-recovery gap" is **imprecise**. The control-plane Raft holds the *cluster-state doc*, which is
  explicitly **not** the query mutations; building it provides membership + epoch fencing (necessary) but does
  **not by itself** lift the ADR-036 quiesce window. Lifting it requires the **per-shard query log** to become
  durable + replicated (the ES translog) so a recovering replica streams segments from a peer **and then
  replays the tail after `snapshot_pos`** — a *distinct* mechanism from the control-plane doc, scheduled as
  **5c**. Also still design-only: the openraft backend itself (5b — a `RaftControlPlane` over `Raft<C>`, a new
  gRPC `ControlService` carrying an opaque-bytes envelope, manager role/bin, multi-node elections); an
  allocator that *acts* on the shard→node map (5a commits a reassignment as a **map-only** change — no physical
  data movement); TLS/auth. **Increment plan: 5a** = the seam (here); **5b** = the openraft backend; **5c** =
  the durable/replicated per-shard query log that closes the quiesce gap.
- **Consequence:** The coordinator now carries a quorum-shaped cluster-state seam with node identity + a
  shard→node map, dependency-free and byte-identical by default. Proven by `tests/cluster_control_plane_oracle.rs`
  (the default control plane ≡ the independent brute oracle across K×RF; the committed document is well-formed;
  a shard reassignment advances the epoch + changes the map while every match set is unchanged; every backend
  driven by one script converges to the identical document — the two-backend differential, openraft-ready) +
  nine `control.rs` unit tests (apply determinism, idempotency, fail-closed). The existing
  `cluster_oracle`/`cluster_grpc_oracle`/`cluster_durability_oracle` are unchanged and stay green — itself the
  byte-identical acceptance signal. Full `check.sh` green (fmt + clippy ×3 incl. lean-core + tests ×2 incl.
  distributed + audit + deny).
- **See also:** ADR-031 (the `ClusterLog` seam + one-`apply`-funnel + manifest-epoch this mirrors and was
  shaped for), ADR-033 (the shared-nothing control-plane model §4.3), ADR-027 (the in-process core + the
  one-frozen-dict invariant), ADR-035/036 (per-shard replication — the data-path HA the control plane sits
  *above*, and the quiesce gap 5c closes), `src/cluster/control.rs`, `src/cluster/coordinator.rs`
  (`control` field, `control_state`/`assignment_for`/`reassign_shard`), `src/cluster/shard.rs`
  (`ShardError::ControlPlane`), `tests/cluster_control_plane_oracle.rs`.

