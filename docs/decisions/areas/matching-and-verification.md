# Matching & verification decisions

> [Architecture decision hub](../../DECISIONS.md)

Candidate retrieval, lossless signature cover, exact verification, broad-query handling, and query-cost controls.

| ADR | Decision | Summary | Status |
|---|---|---|---|
| [001](../adr-001-semantic-signatures.md) | Semantic signatures | Gates on small semantic feature combinations instead of raw terms to keep retrieval selective and lossless. | Accepted |
| [002](../adr-002-integer-exact-verification.md) | Integer-only exact verification | Pushes parsing and interpretation to compile time so matching uses only integer masks and sorted IDs. | Accepted |
| [003](../adr-003-broad-query-quarantine.md) | Broad-query cost classes | Classifies non-selective queries into explicit lanes so broad work cannot dominate selective matching. | Accepted |
| [006](../adr-006-forbidden-features-never-gate.md) | Forbidden features never gate | Keeps negative features out of candidate retrieval and checks them only during exact verification. | Accepted |
| [011](../adr-011-cache-line-blocked-bloom.md) | Cache-line blocked bloom filter | Skips impossible segment probes with one cache-line-sized bloom lookup. | Accepted |
| [019](../adr-019-query-family-factoring-declined.md) | Query-family factoring | Declines a shared-prefix DAG because it targets a non-bottleneck at high format and rebuild cost. | **Declined** |
| [025](../adr-025-query-complexity-limits.md) | Query-complexity limits | Enforces front-door policy limits while keeping durable recovery governed by format ceilings. | Accepted |
| [026](../adr-026-broad-lane-batch-evaluation.md) | Columnar broad-lane evaluation | Evaluates broad queries once per title batch through bitmap algebra while preserving scalar results. | Accepted |

---

Shipped changes are recorded in [CHANGELOG.md](../../CHANGELOG.md); unfinished work belongs in
[roadmap.md](../../roadmap.md). Documentation placement rules live in
[the documentation hub](../../README.md).
