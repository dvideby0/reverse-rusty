# Clustering — elasticity & repair decisions

> [Architecture decision hub](../../DECISIONS.md)

Allocation, handoff, autoscaling, resize, reconciliation, and repair after partial distributed writes.

| ADR | Decision | Summary | Status |
|---|---|---|---|
| [042](../adr-042-shard-node-allocator.md) | Shard→node allocator (rendezvous hashing) | HRW hashing computes a balanced, minimal-movement shard→node map; `rebalance` commits only changed positions; in-process the map is advisory. | Accepted |
| [043](../adr-043-swappable-shard-backing.md) | Swappable shard backing (`HandoffShard`) | Make a position's backing atomically swappable (`ArcSwap` + generation fence stamp), serve-then-drop for free — the routing-flip half of a live handoff. | Accepted |
| [044](../adr-044-live-data-moving-handoff.md) | Live data-moving handoff | `execute_handoff` wires decide→move→flip: no-quiesce bulk recover → fence the source (writes only) → drain to convergence → flip routing. A shard moves owners live, zero FN. | Accepted |
| [045](../adr-045-autoscaler.md) | Autoscaler (policy/trigger over rebalance) | A pure `evaluate` policy (membership→rebalance; skew→handoff advisory; corpus→split advisory) + a `tick` driver; idempotence is the hysteresis; disabled by default. | Accepted |
| [047](../adr-047-remote-partial-apply-resync.md) | Remote live-write partial-apply repair (`resync`) | A mid-fan-out remote write failure is detected (typed `PartiallyApplied` + event) and repaired live (`resync`) instead of silently partial; + a safe `block_on` thread-context contract. | Accepted |
| [048](../adr-048-reliability-hardening.md) | Reliability hardening | Auto-unfence-on-abort (`Unfence` CAS), translog-lease TTL reap, and wiring the autoscaler's `Handoff` advisory through to `execute_handoff`. | Accepted |
| [090](../adr-090-data-moving-reassignment.md) | Data-moving live reassignment (`reassign_and_move`/`rebalance_and_move`) | Wire a committed assignment change to a physical move: `reassign_and_move` runs `execute_handoff` (ADR-044) THEN commits `AssignShard` (**move-then-commit** — crash-safe because the fenced source still serves reads, so the committed map always resolves to a data-holding node); `rebalance_and_move` does it per changed primary, sequentially. A reassignment now moves data and routing follows — live under concurrent writes and across a resolve-only restart — closing the ADR-086 deferral. Serialized against concurrent moves (incl. the now-unified autoscaler path) + fail-closed; `distributed`-gated ⇒ default byte-identical. REST `POST /_cluster/reassign` + `rebalance {move:true}`. Deferred: parallel multi-position + an unattended assignment-watch controller. Zero-FN proven (`cluster_grpc_oracle::reassign`). | Accepted |
| [065](../adr-065-distributed-v1-graduation.md) | Distributed v1 — graduation criteria (experimental → release-candidate) | A program ADR defining the milestone that retires the "experimental" label: feature-complete + ready for full-feature **multi-machine** testing (not production-proven). The checklist: a cluster REST surface, TLS/auth on the gRPC transports, a real multi-machine harness, tagged-cluster vocab change, cluster ranking, cross-process vocab shipping, auto-split, replicate-broad-to-all (or decide), tag-dict recovery fingerprint, packaging + runbook, backup/restore, and a ≥20M multi-shard scale proof. Each criterion ships under its own ADR/PR. | Accepted |

---

Each summary links to the canonical ADR record. Implementation status belongs in
[STATUS.md](../../STATUS.md); documentation placement rules belong in
[the documentation hub](../../README.md).
