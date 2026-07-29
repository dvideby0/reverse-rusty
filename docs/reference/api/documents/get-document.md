# `GET /_doc/{id}` — Retrieve a query

> [Documents APIs](../documents.md) · [REST API hub](../../api.md)

Reference shapes: [Elasticsearch get document](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-get)
and [OpenSearch get document](https://docs.opensearch.org/latest/api-reference/document-apis/get-documents/).

```bash
curl localhost:9200/_doc/1
```

```json
{
  "_index": "queries",
  "_id": 1,
  "_version": 7,
  "found": true,
  "_source": {
    "query": "dell laptop",
    "tags": {
      "category": "electronics",
      "status": ["active", "reviewed"]
    }
  }
}
```

The response follows the Elasticsearch/OpenSearch get-document envelope while retaining Reverse
Rusty's numeric `u64` identity: `_index` is the implicit `"queries"` index, `_version` is the
version supplied on the successful write (default `1`), and `_source.query` is the original DSL.
Tags are read back in a canonical `tags` object: scalar-coerced values are strings, one value is a
string, and multiple values are a sorted array. This canonical form intentionally does not preserve
whether a tag arrived under `tags` or as an ES-style sibling field.

Reads are real-time: every acknowledged write publishes a new snapshot before returning. Reverse
Rusty has no REST-visible equivalent of Elasticsearch's `_seq_no` or `_primary_term`, so it omits
those fields instead of inventing concurrency tokens. Internally, a separate persisted source
generation changes on every accepted write; it is not a concurrency API and is never returned.

The common ES/OS source projection parameters are supported (comma-separated values and `*`/`?`
wildcards):

| Query parameter | Behavior |
|---|---|
| `_source=false` | Omit `_source` while retaining identity, version, and `found` |
| `_source_includes=query,tags.category` | Return only matching source fields |
| `_source_excludes=tags.internal_*` | Remove matching source fields |

The singular aliases `_source_include` and `_source_exclude` are accepted too. Unsupported query
parameters fail with **400** rather than being silently ignored.

Use `HEAD /_doc/{id}` for a bodyless existence check. It reads liveness from the exact index—not
the display-only source sidecar—and returns **200** when the query exists and **404** otherwise.

If the query ID doesn't exist, `GET` returns **404** with:

```json
{"_index": "queries", "_id": 1, "found": false}
```

Both single-node and in-process coordinator modes provide this contract. A coordinator backed by
remote shards returns **501** because the v1 gRPC transport does not expose source lookup; it never
misreports an unavailable lookup as `found: false`.

If the match index has a live row but its source sidecar is missing, the request fails with
`source_unavailable` (**500** in single-node mode, **502** through a local coordinator) rather than
misreporting the live document as a 404. The same fail-loud rule applies when footer-backed source
metadata disagrees with the greatest live exact generation. Resolution uses that greatest generation
across the memtable and base segments, so a supported additive bulk write after a live insert still
reads the newer source even though it resides in a base segment. The generation check catches a stale
sidecar even when both writes used the default version `1`. WAL v7 preserves that ordering across
restart: replay reinstalls the frame's original generation and cannot overwrite a later same-id bulk
source. A vocabulary update also refuses before changing the live normalizer when the source store
does not cover every distinct live exact id, so a partial sidecar cannot turn a rebuild into data
loss. `HEAD` still returns **200** in either source-error case because existence does not require
source materialization.

`sources.dat` v2 now appends a backward-readable metadata footer while leaving the original query
index/blob intact, so query-only hit enrichment remains lazy and old binaries still read query text.
Metadata-footer v2 stores the internal generation; footer v1, source-file v1, and original-v2 files
open automatically as generation-zero legacy data and can pair only with a generation-zero pre-v8
segment. Dense legacy tags are reconstructed from the persisted tag dictionary; the rare pre-footer
document carrying only post-freeze synthetic tags cannot be reversed, so its response includes
`"_source_metadata":{"complete":false,...}` until the document is re-PUT. This is explicit rather
than silently presenting incomplete metadata as the original source.
