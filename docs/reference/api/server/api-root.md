# `GET /` / `HEAD /` — API root

> [Server & shared behavior](../server.md) · [REST API hub](../../api.md)

```bash
curl localhost:9200/
```

```json
{
  "name": "reverse-rusty",
  "cluster_name": "reverse-rusty",
  "cluster_uuid": "_na_",
  "version": {
    "distribution": "reverse-rusty",
    "number": "0.1.0"
  },
  "tagline": "you know, for matching"
}
```

The shape follows the familiar Elasticsearch/OpenSearch cluster-information response while staying
honest about Reverse Rusty's own capabilities:

- `version.number` is the crate's `CARGO_PKG_VERSION` (from `engine/Cargo.toml`), not a pinned
  literal — the `"0.1.0"` above is illustrative and tracks the package version as it bumps.
- `cluster_uuid` is `_na_` because Reverse Rusty does not currently persist an externally visible
  cluster identity. The response omits Lucene, wire-compatibility, and index-compatibility fields
  because they do not apply.
- Coordinator mode adds `mode: "cluster"`, `shards`, `replication_factor`, and `durable`.
- `HEAD /` is the lightweight connectivity form: it returns the same `200` and response headers as
  `GET /`, with no body.
