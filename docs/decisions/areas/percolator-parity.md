# Percolator parity decisions

> [Architecture decision hub](../../DECISIONS.md)

Metadata filtering, ranking, API compatibility, aliases, and other production percolator semantics.

| ADR | Decision | Summary | Status |
|---|---|---|---|
| [049](../adr-049-percolator-parity-tags.md) | Metadata + filtered percolation | Stores integer tag columns and applies request filters during verification, never candidate gating. | Accepted |
| [055](../adr-055-cluster-tags-filtered-percolation.md) | Cluster tags + filtering | Shares one tag dictionary across shards and resolves filters once before fan-out. | Accepted |
| [059](../adr-059-percolate-ranking-pagination.md) | Ranking + pagination | Reorders the final match set by request boosts and priority, then applies `from` and `size`. | Accepted |
| [060](../adr-060-learned-alias-evolution.md) | Learned-alias governance | Tracks provenance and confidence, auto-activating only structurally safe single-token aliases. | Accepted |
| [061](../adr-061-token-graph-multiword-aliases.md) | Multi-word alias title views | Uses a positive alias superset for required matching and a canonical view for forbidden checks. | Accepted |
| [062](../adr-062-server-bearer-auth.md) | HTTP bearer authentication | Protects mutation and administration endpoints with an opt-in constant-time bearer-token gate. | Accepted |
| [063](../adr-063-adversarial-test-hardening.md) | Adversarial test hardening | Adds messy corpora, reference-free properties, boundary tests, and corruption pins. | Accepted |
| [064](../adr-064-percolator-drop-in-parity-audit.md) | Drop-in parity audit | Maps the reference workload, proves pinned-pair recall, and defines the remaining parity work package. | Accepted |
| [067](../adr-067-atomic-upsert-put.md) | Atomic upsert | Replaces every prior live copy under one writer lock and one recoverable WAL frame. | Accepted |
| [068](../adr-068-class-d-always-candidate-lane.md) | Class-D always-candidate lane | Optionally accepts negation-only queries under a universal broad signature with exact negative verification. | Accepted |
| [069](../adr-069-parity-number-context-words.md) | Configurable number context | Makes context-sensitive number typing configurable, including the position-insensitive parity mode. | Accepted |
| [073](../adr-073-rest-parity-hardening.md) | REST parity hardening | Makes tag coercion explicit, wires live flush thresholds, and exposes per-request broad scope. | Accepted |
| [126](../adr-126-search-api-contract.md) | Search API contract | Makes compatibility search strict, generation-consistent, and ES/OS-shaped where semantics align. | Accepted |
| [127](../adr-127-v2-search-api-contract.md) | V2 search API contract | Makes exact bounded search strict, ES/OS-familiar, and mutation-consistent through winner enrichment. | Accepted |
| [128](../adr-128-v2-mpercolate-api-contract.md) | V2 batch percolate API contract | Makes exact bounded batches strict, compatibly controlled, and mutation-consistent through union enrichment. | Accepted |

---

Shipped changes are recorded in [CHANGELOG.md](../../CHANGELOG.md); unfinished work belongs in
[roadmap.md](../../roadmap.md). Documentation placement rules live in
[the documentation hub](../../README.md).
