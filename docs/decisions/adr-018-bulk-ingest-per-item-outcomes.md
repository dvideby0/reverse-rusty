# ADR-018: Bulk ingest reports per-item outcomes (ES-style)

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted


- **Context:** `POST /_bulk` reported only an aggregate `IngestReport` (counts of ingested / parse-
  rejected / class-D-rejected). Every item that parsed as NDJSON was stamped status 201 — even when
  the engine subsequently dropped its query (a DSL parse error inside the query, or a cost-class-D
  quarantine, ADR-003). The caller saw the batch-level `errors: true` flag but had no way to learn
  *which* items were dropped or why. This diverged from the single-doc `PUT /_doc` path, which
  already returns a per-item 400 with a reason. (audit P1-8)
- **Research:** Elasticsearch's `_bulk` is the reference contract: the batch returns HTTP 200 with
  an `items[]` array in which each item carries its *own* `status` and, on failure, an `error`
  object; a top-level `errors` boolean flags that at least one item failed. The audit suggested two
  options: (a) insert one-by-one via `try_insert_live` for natural per-item results, or (b) have the
  bulk path return per-item outcomes. Option (a) was rejected — it would route bulk through the
  memtable + WAL live-insert path, destroying the all-or-nothing durable-segment commit and the
  single-segment build efficiency (ADR-017) and WAL-ing every entry (the redundant double-write
  ADR-017 explicitly avoids). The two-pass bulk compiler already decides each query's fate per item;
  only the mapping back to input position was being thrown away.
- **Decision:** Option (b). Add a public `IngestItemStatus { Ingested, RejectedParse(ParseError),
  RejectedClassD }` and `try_bulk_ingest_detailed`, returning `(IngestReport, Vec<IngestItemStatus>)`
  with one entry per input query in submission order (`items[i]` describes `queries[i]`).
  `try_bulk_ingest` stays as a thin wrapper that discards the per-item vec, so its other callers
  (infallible wrappers, bench, persistence tests) are untouched. The `/_bulk` handler tracks each
  pair's response slot and maps the engine outcome back onto it: parse and class-D rejections become
  per-item 400s mirroring `PUT /_doc` — parse echoes the typed `ParseError` detail (position + kind),
  class-D uses "query has no anchorable feature (cost class D)". Durability is unchanged
  (all-or-nothing, ADR-017); per-item statuses are reported only once the batch has durably committed.
- **Consequence:** Bulk callers get ES-parity per-item visibility — a partially-bad batch durably
  commits its good items *and* reports exactly which were dropped and why, instead of a silent 201.
  `IngestItemStatus` carries the typed `ParseError` (not a stringified message), keeping the
  diagnostic inspectable end-to-end (ADR-005). The aggregate `IngestReport` is retained and stays
  consistent with the per-item tallies. Cost is one `Vec<IngestItemStatus>` allocation on the cold
  bulk write path (never the match hot path).
- **See also:** ADR-017 (durable bulk ingest), ADR-005 (typed errors), ADR-003 (cost-class-D
  quarantine), the single-doc `PUT /_doc` path it now matches.

