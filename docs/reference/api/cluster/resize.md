# `POST /_cluster/resize` — Resize an in-process cluster

> [Cluster control APIs](../cluster.md) · [REST API hub](../../api.md)

Supply `{"num_shards": N}` to rebuild the cluster under a fresh ring and place every live query
again. A successful response is:

```json
{"acknowledged": true, "num_shards": 16, "rebuilt": 1200000}
```

The operation is in-process only; a non-local cluster returns 400. Vocabulary and tags are
preserved. The rebuild is `O(corpus)` and holds the write lock like `PUT /_vocab`. See ADR-078.

Cross-topology assembly rules and mesh configuration are documented in
[coordinator mode](../server/coordinator-mode.md). Cluster design and failure invariants are
canonical in [clustering and scaling](../../../design/clustering-and-scaling.md).
