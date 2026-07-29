# ADR-155: Alias discover-and-record REST API contract

> [Engine, errors, dependencies & ops decisions](areas/engine-quality-and-operations.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

`POST /_vocab/aliases/discover_and_record` mines the standalone engine's stored query corpus and
files every proposal in the governed alias registry as a review-only candidate. ADR-102 guarantees
that this provenance never auto-activates and uses a metadata-only vocabulary install, so matching
and the vocabulary epoch do not change.

The original REST route accepted ignored query parameters, inherited the server-wide 100 MiB body
limit, decoded optional JSON without a read deadline or unknown-field rejection, accepted
unbounded and nonsensical discovery controls, and performed source collection, O(corpus)
discovery, registry mutation, and snapshot publication while holding the engine mutex on a Tokio
request worker. It had no bounded administrative admission, timing, no-store handling, or fixed
endpoint telemetry. It also published a snapshot after request-level failure and returned
`acknowledged: true` without stating that the standalone vocabulary file was not updated.

[Elasticsearch synonym APIs](https://www.elastic.co/guide/en/elasticsearch/reference/current/put-synonym-rule.html)
manage explicit rules in named synonym sets. OpenSearch configures explicit rules through a
[synonym token filter](https://docs.opensearch.org/latest/analyzers/token-filters/synonym/) and can
[refresh search analyzers](https://docs.opensearch.org/latest/im-plugin/refresh-analyzer/index/).
None of those operations discovers candidates from stored reverse queries or records them without
activation, so exposing their paths or shapes would misstate both the input and the mutation.

## Decision

- Keep the native `POST /_vocab/aliases/discover_and_record` path. It accepts no query parameters
  and never accepts a caller-supplied `queries` corpus; use the compute-only discovery endpoint for
  explicit evidence.
- Accept an empty body for default controls. A non-empty body must be `application/json` or
  `application/*+json`, reject unknown fields, fit within 64 KiB, and complete within five seconds.
- Reuse the compute-only endpoint's control validation: `min_token_freq` is positive;
  `min_similarity` and `max_cooccurrence_rate` are finite within `[0,1]`; `max_pairs` is at most
  100,000; and `max_vocab` is within `1..=4096`.
- Return whole-route `took` and `took_ms`, `acknowledged: true`, the proposal/registry counters,
  `recompiled: 0`, and the resulting registry summary. Return `persisted: false` explicitly because
  the route changes the live standalone vocabulary document but does not write the operator's
  startup vocabulary file.
- Wait asynchronously for the shared one-slot administrative permit. A blocking worker owns that
  permit through parsing, source capture, discovery, candidate installation, snapshot publication,
  and serialization. The worker briefly locks the engine to clone live sources, releases it for
  O(corpus) discovery, and reacquires it only to classify and install the precomputed proposals.
  Add `Engine::record_discovered_aliases` as the mutation half of the existing combined embedded
  API so the REST handler does not recompute under the writer guard.
- Publish the snapshot only after a successful metadata install. A disconnected request does not
  cancel admitted mutation work. There is no execution timeout after admission.
- Keep coordinator mode fail-loud with 501 and the explicit dry-run/review/`PUT /_vocab`
  alternative. It still applies the shared method, query, media, JSON, body, and control validation
  before returning that capability boundary.
- Use the standard JSON error envelope, `Allow: POST` for unsupported methods,
  `Cache-Control: no-store` for every route-reached result, and the fixed
  `vocab_aliases_discover_and_record` timing/counter label.

## Consequences

The mutation is now a strict, bounded, observable stored-corpus operation. Candidate recording can
no longer monopolize a Tokio worker or hold the engine mutex during distributional analysis, and a
request error cannot trigger snapshot publication. The response distinguishes live acknowledgement
from persistence: operators must save the resulting `GET /_vocab` document to their startup
vocabulary file before restart.

Cluster-side record-only installation remains unavailable because coordinator vocabulary changes
use a full blue/green re-placement and checkpoint path. Operators can run explicit compute-only
discovery, review the proposals, and install an edited vocabulary through `PUT /_vocab`.

## Safety and proof

The ADR-102 distributional signal and governance rules are unchanged. `record_discovered_aliases`
classifies proposals against the current normalizer and dictionary, and the existing metadata-only
install guard verifies that matching-relevant alias projections and all non-registry vocabulary
fields are unchanged before skipping an epoch bump and recompile.

Standalone route tests cover timed no-store success, the explicit non-persistence field, published
candidate visibility, byte-identical match results and vocabulary epoch, idempotent rediscovery,
explicit-corpus refusal, strict method/query/media/JSON transport, body size and deadline, all
control bounds, fixed telemetry, asynchronous admission, closed admission, and off-runtime engine
lock waiting. Coordinator tests cover the observed no-store 501 alternative, shared request
validation, and method fallback. The existing differential oracle continues to prove that combined
discovery and recording changes no match set.
