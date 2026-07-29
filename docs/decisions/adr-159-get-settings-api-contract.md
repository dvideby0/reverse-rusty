# ADR-159: Settings read REST API contract

> [Engine, errors, dependencies & ops decisions](areas/engine-quality-and-operations.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

`GET /_settings` exposes the live `EngineConfig` in standalone mode and the cluster topology plus
assembled per-shard configuration in coordinator mode. The original route accepted unknown query
parameters and arbitrary request bodies under the server-wide 100 MiB limit, had no body deadline,
method fallback, cache policy, fixed telemetry, or response ceiling, and serialized on a Tokio
request worker. Coordinator mode ignored `include_defaults` and could block that worker while
waiting for the cluster lock.

Elasticsearch and OpenSearch expose cluster settings through
[`GET /_cluster/settings`](https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-get-settings.html)
and the
[OpenSearch Cluster Settings API](https://docs.opensearch.org/latest/api-reference/cluster-api/cluster-settings/).
Those contracts distinguish persistent and transient overrides and return only explicitly
configured values unless defaults are requested. Elasticsearch's
[bare `/_settings` path](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-settings)
instead means index settings. Reverse Rusty returns one effective native engine configuration and
does not implement either persistence tier or an Elasticsearch index resource, so neither familiar
path is an honest alias.

## Decision

- Keep native `GET`/`HEAD /_settings`; do not add `/_cluster/settings` or claim Elasticsearch index
  settings compatibility.
- Accept the familiar Boolean `include_defaults` and `flat_settings` controls. Defaults mean
  `EngineConfig::default()`. Reverse Rusty's setting keys are already flat, so `flat_settings` is an
  honest representation-preserving no-op. Reject unknown, duplicate, and malformed controls.
- Keep the operation bodyless. Bound GET/HEAD transport at 64 KiB and 250 milliseconds, reject a
  non-empty body, and return the standard error envelope plus `Allow: GET, HEAD, PUT` for unsupported
  methods. The existing PUT body allowance remains independent.
- Preserve the standalone response shape: `settings`, plus `defaults` only when requested. Preserve
  the coordinator topology fields and `per_shard`; add the same optional per-shard `defaults`.
- Wait asynchronously for the shared one-slot administrative permit. Move standalone snapshot
  serialization and coordinator lock wait, cloning, and serialization onto a blocking worker that
  owns the permit until completion.
- Bound serialized responses at 64 KiB. Fail with typed `500 settings_unavailable` on worker,
  serialization, or ceiling failure, and with `503 settings_unavailable` if admission is closed.
- Mark every route-reached response `Cache-Control: no-store` and count/time all outcomes under the
  fixed `settings_get` metric label. HEAD executes the same read and preserves the GET
  representation headers while Axum removes the body.

## Consequences

Settings reads are strict, cache-safe, bounded, observable, and consistent across standalone and
coordinator transport. Coordinator reads no longer block Tokio workers and now honor
`include_defaults`. Existing clients retain the native response fields; the only additive response
change is coordinator `defaults` when explicitly requested.

Clients written specifically for Elasticsearch/OpenSearch settings still need a translation layer:
Reverse Rusty deliberately does not fabricate an index name, stringify typed values, or report
nonexistent persistent/transient override tiers.

## Safety and proof

The route remains a read of immutable standalone configuration or a coherent cluster read guard; it
cannot change matching, visibility, persistence, or placement. Standalone tests cover exact live
and default values, the flat control, GET/HEAD parity, cache and telemetry policy, strict
query/body/size/deadline/method handling, PUT limit isolation, asynchronous admission, and closed
admission. Coordinator tests cover topology/per-shard/default values, shared transport,
off-runtime lock contention, permit ownership during that wait, and closed admission.
