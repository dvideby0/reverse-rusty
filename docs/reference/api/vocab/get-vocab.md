# `GET` / `HEAD /_vocab` — Current vocabulary

> [Vocabulary & alias APIs](../vocab.md) · [REST API hub](../../api.md)

```bash
curl localhost:9200/_vocab
curl -I localhost:9200/_vocab
```

```json
{
  "synonyms": [
    {"token": "pkg", "canonical": "term:package", "kind": "generic"}
  ],
  "phrases": [
    {"tokens": ["north", "star"], "canonical": "brand:north_star", "kind": "brand"},
    {"tokens": ["wireless", "mouse"], "canonical": "entity:wireless_mouse", "kind": "entity"}
  ],
  "equivalences": [["ns", "north star"]],
  "punctuation": [{"ch": "'", "class": "fold"}, {"ch": "-", "class": "fold"}],
  "number_context": ["model"],
  "aliases": {"entries": []}
}
```

The GET response is the one complete installed `Vocab` document. It can be saved as the
single-node `--vocab-file` or sent back to `PUT /_vocab` without projection or reconstruction.
`HEAD` performs the same snapshot capture and serialization but returns no body. Every outcome
reached through the read route includes `Cache-Control: no-store`; success is
`Content-Type: application/json`.

The read is strict: it accepts no query parameters or request body. GET/HEAD body extraction has a
64 KiB ceiling and a 250 ms read deadline, independent of the write operation's 16 MiB allowance.
Errors use the standard JSON envelope: invalid query/body is 400, a stalled body is 408, oversized
input is 413, closed read admission is 503, and serialization/worker failure is 500. Other methods
are 405 with `Allow: GET, HEAD, PUT`.

Standalone mode captures one immutable lock-free engine snapshot. Coordinator mode clones the
installed vocabulary while briefly holding the cluster read guard on a blocking worker, releases
the guard, and serializes afterward. Both share the server's single bounded administrative-read
slot, so concurrent large documents cannot multiply clone/serialization work; waiting for that
slot is asynchronous.

This is deliberately a native API. Elasticsearch
[`GET /_synonyms/{id}`](https://www.elastic.co/guide/en/elasticsearch/reference/current/get-synonyms-set.html)
and
[`PUT /_synonyms/{id}`](https://www.elastic.co/guide/en/elasticsearch/reference/current/put-synonyms-set.html)
operate on one named, pageable Solr-rule set, while OpenSearch exposes synonyms through
[analyzer token-filter configuration](https://docs.opensearch.org/latest/analyzers/token-filters/synonym/).
Reverse Rusty's document also owns phrases, equivalences, punctuation, numeric context, and the
governed alias registry, so neither the standard path nor its response shape is an honest alias.
