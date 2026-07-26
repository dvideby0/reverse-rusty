# ADR-065: Distributed v1 — graduation criteria (experimental → release-candidate)

> [Clustering — elasticity & repair decisions](areas/clustering-elasticity-and-repair.md) · [Decision hub](../DECISIONS.md)


- **Status:** **Accepted (2026-06-10) — a program ADR.** Defines the milestone and its acceptance
  checklist; each criterion ships under its own ADR/PR. This ADR preserves the decision-time scope;
  [`CHANGELOG.md`](../CHANGELOG.md) records shipped outcomes and [`roadmap.md`](../roadmap.md) owns
  the remaining acceptance work.
- **Context:** **Cluster v1** — the in-process multi-shard core + durable reopen + dynamic vocabulary —
  is built and oracle-proven (ADR-046). The **distributed multi-node layers**
  (ADR-027, 029, 031–048: the gRPC transport + dict/tag-dict shipping, replication + no-quiesce peer
  recovery + translog/retention, the durable openraft control plane, the HRW allocator, live data-moving
  handoff, the autoscaler, partial-apply repair) are built and proven in-process, over localhost gRPC,
  and through the multi-process single-host container harness. Independent multi-machine evidence
  remains open, so the distributed deployment stays experimental. Decision: drive these layers to
  **Distributed v1**, defined as:

  > *Feature-complete and hardened enough that every advertised feature can be exercised in a real
  > multi-machine deployment — **not yet production-proven** (that takes mileage, not engineering), but
  > with no known untested feature seam and no "experimental" asterisk on the feature list.*

- **Graduation criteria** (the checklist, roughly in build order — the first three unblock testing
  everything else):
  1. **Cluster REST surface.** The HTTP server today fronts a single-node `Engine` only; the cluster is
     a library API plus raw gRPC bins. Build a coordinator-mode server — the existing REST API
     (percolate + filtered percolation, ranking once criterion 5 lands, vocab/alias admin, `/_bulk`,
     stats/health/metrics) over a `ClusterEngine` — so a cluster is operable end-to-end without
     embedding Rust. This is the single biggest usability gap to "testing all features".
  2. **TLS + auth on the gRPC transports** (shard + control plane — both currently plaintext and
     unauthenticated). Reuse the ADR-062 token shape and/or mTLS; fail-loud configuration in the
     ADR-062 style; document the trust model.
  3. **A real multi-machine test harness.** A durable multi-node rolling-restart / kill-and-recover /
     handoff-under-load suite running across a real network boundary (containers or hosts — not
     in-process loopback assumptions): the missing analogue of the localhost oracles, plus a
     CI-runnable (compose-based) variant. Every later criterion lands with harness coverage.
  4. **Tagged-cluster vocabulary change** (the ADR-055 deferral). `set_vocab`/`learn_and_apply` on a
     tagged cluster — persist raw tag strings so the blue/green rebuild can reconstruct synthetic tags
     instead of refusing fail-loud.
  5. **Cluster ranking** (the ADR-059 deferral). The `RankSpec` seam at the coordinator merge —
     cross-shard newest-copy tag resolution + the same `(score desc, _id asc)` total order.
  6. **Cross-process vocabulary/normalizer shipping** + multi-word aliases on a cluster (the
     ADR-046/061 deferrals): ship vocab updates to remote shards' normalizers (P(T)-aware routing, or a
     documented refusal story if it stays out of v1 — but decided, not deferred-by-default).
  7. **Auto-split + `recommended_shard_count`.** The autoscaler's split advisory gains a real mechanism:
     ring re-keying + the data move via the existing live handoff; the ring's `num_shards` stops being
     fixed at construction.
  8. **Replicate-broad-to-all — or an explicit decision.** Today broad (class-C) queries live on the
     shard-0 replicated lane only; either replicate them to all nodes or record the ADR for why the
     RF-replicated single lane suffices at v1.
  9. **Tag-dict fingerprint in the recovery handshakes** (the deferred ADR-055 hardening — the
     feature-dict fingerprint is already validated; the tag dict's is not).
  10. **Deployment packaging + runbook.** Dockerfile / compose for a K-shard + control-plane cluster;
      an operations doc (start/stop/scale/recover/back up). (Closes the long-standing
      "no Dockerfile" backlog line as part of this milestone.)
  11. **Backup/restore, documented + tested** (coordinates with the ADR-064 item-7 single-node
      procedure; the cluster version must cover coordinator manifest + per-shard segments + logs).
  12. **Scale proof at target.** A multi-shard load test at **≥20M stored queries on real hardware**
      (the largest soak to date is 10M, single-node), plus the real-corpus false-negative / throughput
      audit tracked in the
      [`roadmap`](../roadmap.md#real-corpus-correctness-and-throughput-audit) — the two runs that turn
      the headline numbers from design-target evidence into deployment evidence.
- **Explicitly out of scope for v1:** a "production-proven" claim (mileage, not engineering);
  QPS/compute-replica autoscaling (HPA territory — ADR-045 scope note stands); any object-store
  dependency (ADR-033 shared-nothing stands).
- **Why this is safe:** every criterion is additive hardening or surface work over already-oracle-proven
  mechanisms; each PR keeps the cluster oracle suite green, and the single-host container lifecycle
  harness catches network-namespace and process-lifecycle failures localhost structurally cannot.
- **See also:** ADR-027–048 (the layers being graduated), ADR-033 (the shared-nothing model), ADR-064
  (the single-node drop-in-parity program), the roadmap's
  [`real-corpus audit`](../roadmap.md#real-corpus-correctness-and-throughput-audit) and
  [`real Kubernetes exercise`](../roadmap.md#real-kubernetes-failure-and-recovery-exercise).
