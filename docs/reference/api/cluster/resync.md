# `POST /_cluster/resync` — Re-drive partial-apply repairs

> [Cluster control APIs](../cluster.md) · [REST API hub](../../api.md)

Re-drive the coordinator's queued partial-apply repairs and return:

```json
{"repaired": 3, "still_pending": 1}
```

Use this after a cluster write reports a durably logged partial apply. A partial `PUT /_doc/{id}`
must not be repeated because that would append a second write; resync converges the existing logged
operation. An idempotent partial DELETE may instead be repeated. Resync can converge its in-memory
repair queue only while the same coordinator remains running; a stateless remote coordinator
restart does not preserve that queue. See ADR-047, ADR-067, and ADR-125.

Cross-topology assembly rules and mesh configuration are documented in
[coordinator mode](../server/coordinator-mode.md). Cluster design and failure invariants are
canonical in [clustering and scaling](../../../design/clustering-and-scaling.md).
