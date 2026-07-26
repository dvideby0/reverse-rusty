# Architecture Decision Records

Architecture decisions explain *why* Reverse Rusty works the way it does. Use this hub to choose an
area, scan that area's compact catalog, and open only the ADR record that carries the full context,
decision, and consequences.

## Decision areas

| Area catalog | Scope |
|---|---|
| [Matching & verification](decisions/areas/matching-and-verification.md) | Candidate retrieval, lossless signature cover, exact verification, broad-query handling, and query-cost controls. |
| [Normalization & vocabulary](decisions/areas/normalization-and-vocabulary.md) | Shared query/title normalization, dictionaries, learned vocabulary, aliases, and feature-space evolution. |
| [Ingestion, storage & durability](decisions/areas/ingestion-storage-and-durability.md) | Write paths, segments, WAL and source persistence, compaction, recovery, and durable mutation semantics. |
| [Engine, errors, dependencies & ops](decisions/areas/engine-quality-and-operations.md) | Cross-cutting engine APIs, typed failures, dependency boundaries, observability, testing, and delivery mechanics. |
| [Clustering — core & transport](decisions/areas/clustering-core-and-transport.md) | The multi-shard correctness core, remote shard seam, shared feature space, and durable shard topology. |
| [Clustering — replication & control plane](decisions/areas/clustering-replication-and-control-plane.md) | Replication, peer recovery, translogs, cluster state, consensus, and control-plane durability. |
| [Clustering — elasticity & repair](decisions/areas/clustering-elasticity-and-repair.md) | Allocation, handoff, autoscaling, resize, reconciliation, and repair after partial distributed writes. |
| [Percolator parity](decisions/areas/percolator-parity.md) | Metadata filtering, ranking, API compatibility, aliases, and other production percolator semantics. |
| [Distributed v1 — the ADR-065 graduation program](decisions/areas/distributed-v1-graduation.md) | The staged reliability, deployment, security, operability, ranking, and scale work used to graduate distributed v1. |

## Finding and maintaining decisions

- **Know the topic?** Open the matching area catalog above.
- **Know the number?** Search `decisions/adr-NNN-` or use the repository file finder.
- **Need current behavior?** Use the matching [design](design/README.md),
  [reference](reference/api.md), or [operations](operations/deployment-modes.md) page.
- **Need shipped history or future work?** Use [CHANGELOG.md](CHANGELOG.md) or
  [roadmap.md](roadmap.md); ADRs record rationale, not a current-state inventory or backlog.
- **Adding an ADR?** Create `decisions/adr-NNN-slug.md` using the next free number and add one row
  to the appropriate area catalog. Add a new area here only when no existing catalog is coherent.

ADRs are **append-only and never renumbered**. Mark a superseded, reversed, or declined decision in
place; never delete the record that explains the old constraint.

---

*Documentation placement and single-source-of-truth rules → [README.md](README.md).*
