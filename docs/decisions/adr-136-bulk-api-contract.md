# ADR-136: Bulk REST API contract — strict NDJSON, index/create semantics, and fast fresh ingest

> [Ingestion, storage & durability decisions](areas/ingestion-storage-and-durability.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

- **Context.** `POST /_bulk` borrowed Elasticsearch's alternating action/source NDJSON shape, but
  its boundary and behavior were permissive. It accepted missing or unrelated media types, ignored
  every query parameter, discarded blank lines before pairing records, accepted an unterminated
  final line, and silently treated an empty body as success. Action metadata accepted nonstandard
  flat IDs and arbitrary fields. Only numeric IDs worked. The standalone `index` operation appended
  another matchable body for an existing logical ID, while the coordinator replaced it; the two
  deployment modes therefore implemented different document semantics. Source `version` was
  ignored, the response lacked familiar identity and result fields, and the endpoint had no
  dedicated standalone HTTP tests.

- **Compatibility boundary.** Elasticsearch and OpenSearch bulk APIs use newline-delimited action
  and optional source records, require a terminating newline, define `index` as replace-or-create
  and `create` as create-if-absent, and report ordered per-item results
  ([Elasticsearch Bulk API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-bulk),
  [OpenSearch Bulk API](https://docs.opensearch.org/latest/api-reference/document-apis/bulk/)).
  Reverse Rusty adopts that client-familiar core for its one implicit `queries` index. It remains a
  strict subset: only source-bearing `index` and `create` are implemented; logical IDs must fit
  `u64`; and there are no aliases, routing, ingest pipelines, sequence numbers, primary terms,
  ES/OS internal versioning, update scripts, or bulk deletes. Unsupported controls fail before
  mutation instead of being acknowledged and ignored.

- **Decision — strict request framing.** The preferred media type is `application/x-ndjson`; the
  existing `application/json` media type remains accepted as a compatibility allowance. The body must be
  nonempty, end in a newline, contain no blank records, and alternate one action with one source.
  Every action is an object with exactly one `index` or `create` key. Its metadata accepts `_id`,
  optional `_index: "queries"`, and false `require_alias` / `_require_alias`; `_id` may be a JSON
  unsigned integer or decimal string. The whole action structure is validated before any write, so
  malformed framing, unknown operations or metadata, unsupported indices, alias requirements, and
  unknown query parameters produce one structured request error without partial mutation.
  `refresh=false|true|wait_for` is accepted because every successful Reverse Rusty write is already
  visible before response. The 100 MiB server body limit remains in force.

- **Decision — source and ordered item semantics.** Every source requires a string `query` and may
  carry the same `version`, metadata tags, ES-style scalar tag siblings, and
  `rank_fields.priority` as `PUT /_doc/{id}`. The version is unsigned application metadata
  (default 1), not ES/OS concurrency state. Source JSON, source-field, DSL, and class-D failures are
  per-item 400s when the request is still structurally pairable; later pairs retain their original
  slots. `index` atomically creates or replaces the live logical ID and returns 201 `created` or
  200 `updated`. `create` returns 201 only when the logical ID is absent and otherwise returns 409
  `version_conflict_engine_exception`. Items execute in request order, so repeated IDs observe
  earlier successful items in the same batch.

- **Decision — preserve the bulk-build fast path without changing meaning.** A standalone batch in
  which every structurally valid source has default version 1, every ID is unique, and every ID is
  absent from the captured engine snapshot still compiles directly into one immutable base segment
  and uses ADR-017's atomic segment-plus-source manifest commit. Any existing ID, repeated ID, or
  source version other than 1 selects the ordered WAL-backed live-write path instead; the engine
  writer lock covers the entire batch and one snapshot is published after the ordered pass. The
  public REST `index` operation is therefore a real upsert in both cases. The lower-level
  `Engine::bulk_ingest*` API remains an additive expert primitive and must not be mistaken for
  replace-by-ID. The coordinator keeps its existing ordered logged upsert/create path under one
  batch writer guard.

- **Decision — truthful response and durability.** A structurally valid batch returns HTTP 200 with
  integer `took`, precise native `took_ms`, aggregate `errors`, and one ordered action-keyed item.
  Successful items include `_index: "queries"`, numeric `_id`, the stored application `_version`,
  `result`, and status. Failed items carry a structured `{type, reason}` error and their own status.
  A standalone fresh-segment commit failure still rejects the whole request with 503 because that
  path is all-or-nothing under ADR-017. A WAL failure is a per-item 503 after any earlier successful
  ordered items. A distributed partially applied write remains durably logged for repair and is
  reported as an error-bearing `partial` item; the response never presents it as a clean success.

- **Safety and proof.** The change selects only between existing accepted-write funnels: the direct
  immutable-segment compiler, standalone WAL-backed upsert/create, and coordinator logged
  upsert/create. It does not change query compilation, signature construction, exact verification,
  or durable formats. Standalone route tests pin replacement, create conflicts, in-batch ordering,
  source versions, response identity, source/DSL error alignment, the fresh-segment fast path,
  media type and body framing, strict query/action controls, 413 handling, and POST-only routing.
  Coordinator tests pin the same shared boundary, replacement and conflict behavior, source
  readback, and typed-priority preservation. Existing durable bulk, WAL/reopen, cluster repair, and
  oracle suites remain the semantic and persistence backstop.
