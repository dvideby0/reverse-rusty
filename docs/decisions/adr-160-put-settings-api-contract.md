# ADR-160: Settings write REST API contract

> [Engine, errors, dependencies & ops decisions](areas/engine-quality-and-operations.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

`PUT /_settings` updates the dynamic subset of the standalone `EngineConfig`. The original handler
used Axum's generic JSON extractor under the server-wide 100 MiB allowance, ignored query
parameters, silently collapsed duplicate JSON keys, had no body or operation deadline, cache
policy, fixed telemetry, response ceiling, or bounded admission, and waited on the parking-lot
engine mutex from a Tokio request worker. It changed the engine config, dropped the lock, and then
reacquired it to publish the snapshot, so concurrent settings writes could acknowledge one config
while publishing another.

Elasticsearch's
[cluster settings update](https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-update-settings.html)
and the
[OpenSearch Cluster Settings API](https://docs.opensearch.org/latest/api-reference/cluster-api/cluster-settings/)
write persistent or transient override tiers through `PUT /_cluster/settings`. Elasticsearch's
[index settings update](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-settings)
instead targets indices and data streams, including through the bare `PUT /_settings` path.
Reverse Rusty has neither an index resource nor a persisted override registry: it updates one
effective typed config in memory, and startup configuration becomes authoritative again after a
restart. Neither familiar resource contract is therefore an honest alias.

## Decision

- Keep native `PUT /_settings`; do not add `PUT /_cluster/settings` or claim index-settings
  compatibility.
- Require one non-empty flat JSON object of native setting keys. Reject malformed or non-object
  JSON, duplicate keys, unknown keys, static settings, wrapper objects, wrong types, and invalid
  ranges before mutation. Keep the complete patch all-or-nothing.
- Do not accept `persistent`/`transient` tiers or `null` reset. The server does not retain an
  explicit override registry or startup baseline, so it cannot honestly implement their
  persistence, precedence, or reset semantics. Return a specific error for familiar wrappers
  instead of silently treating them as native settings.
- Accept familiar `flat_settings` and `timeout` query controls. Native keys and output are already
  flat, so `flat_settings` is representation-preserving. `timeout` accepts the shared
  Elasticsearch/OpenSearch duration units plus `0`, defaults to 30 seconds, is capped at 30
  seconds, and bounds administrative admission plus the pre-commit engine-lock wait. Reject
  unknown, duplicate, and malformed controls.
- Require `application/json` or an `application/*+json` media type. Cap the body at 64 KiB with a
  five-second read deadline and return the standard JSON error envelope for every transport
  failure.
- Wait asynchronously for the shared one-slot administrative permit, then move the engine-lock
  wait, validation, response serialization, mutation, and publication to a blocking worker that
  owns the permit. Serialize and enforce the 64 KiB response ceiling before changing state.
- Set the config and publish its immutable snapshot from the same engine guard, making the
  acknowledgement and lock-free GET view one coherent commit. A timeout before that commit changes
  nothing; request cancellation after admission cannot leave a completed update hidden.
- Preserve the successful response fields (`acknowledged`, `persistent: false`, and the complete
  typed `settings`). Mark every route-reached response `Cache-Control: no-store` and count/time all
  outcomes under the fixed `settings_put` metric label.
- Keep coordinator settings static. Coordinator mode validates the same query, media, JSON, size,
  and patch contract before returning a no-store, observed `501` that tells operators to restart
  the coordinator and consistently configured shard nodes.

## Consequences

Runtime settings updates are strict, bounded, observable, and off the async runtime. Duplicate or
oversized input cannot be misinterpreted, lock contention has a caller-visible bound, and a
successful response names exactly the configuration published to lock-free readers. Existing
successful clients keep the same flat request and response shapes.

Clients written for Elasticsearch/OpenSearch settings still need a translation layer. Reverse
Rusty deliberately does not fabricate an index target, override persistence tier, wildcard reset,
or cluster-manager acknowledgement.

## Safety and proof

The pure patch tests cover the complete dynamic/static policy, type/range checks, and
all-or-nothing failure. Standalone route tests cover exact mutation and snapshot visibility,
familiar controls, media and JSON strictness, duplicate keys, body size/deadline, no-store
telemetry, bounded admission, closed admission, and off-runtime engine-lock timeout with no
mutation. Coordinator tests prove the shared contract precedes the explicit unsupported-mode
boundary. Existing query-limit, class-D, hot-tier, broad-lane, deduplication, feedback, and
durability suites continue to own the behavior of each dynamic setting after it is applied.
