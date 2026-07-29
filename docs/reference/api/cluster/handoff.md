# `POST /_cluster/handoff` — Move one position between endpoints

> [Cluster control APIs](../cluster.md) · [REST API hub](../../api.md)

The raw-endpoint handoff primitive accepts:

```json
{"position": 2, "source": "https://source.example:50051", "target": "https://target.example:50051"}
```

It peer-recovers the target, fences and drains the source, flips live routing, and returns the new
placement `generation`. Failure is fail-closed: an aborted move automatically unfences the source.
The endpoint requires a `--features distributed` build and otherwise returns 501. Prefer
`/_cluster/reassign` when the durable membership map should resolve the target and record the new
assignment. See ADR-044, ADR-048, and ADR-072.

Cross-topology assembly rules and mesh configuration are documented in
[coordinator mode](../server/coordinator-mode.md). Cluster design and failure invariants are
canonical in [clustering and scaling](../../../design/clustering-and-scaling.md).
