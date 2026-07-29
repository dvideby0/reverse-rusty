# `POST /_vocab/aliases/discover_and_record` — Discover and record review candidates

> [Vocabulary & alias APIs](../vocab.md) · [REST API hub](../../api.md)

Run discovery over the standalone engine's own stored queries and record every proposal as a review
`candidate`. Nothing activates, matching and the vocabulary epoch do not change, and `recompiled`
is always zero:

```json
{
  "took": 12,
  "took_ms": 12.34,
  "acknowledged": true,
  "persisted": false,
  "proposed": 12,
  "new_candidates": 8,
  "rediscovered": 3,
  "rejected_sticky": 1,
  "recompiled": 0,
  "summary": {"active": 2, "candidate": 8, "rejected": 1}
}
```

`acknowledged` means the live engine and published snapshot contain the registry update.
`persisted: false` is deliberate: like other standalone runtime vocabulary changes, this endpoint
does not write the operator's startup vocabulary file. Save the resulting `GET /_vocab` document to
that file before restart if the candidates must survive.

The controls are the same bounded knobs documented for
[alias discovery](alias-discover.md), but `queries` is never accepted:
recording is specifically over this engine's stored source snapshot. An empty body uses defaults. A
non-empty body must be `application/json` or `application/*+json`, reject unknown fields, fit within
64 KiB, and complete within five seconds. Query parameters are rejected; unsupported methods return
405 with `Allow: POST`.

The request waits asynchronously for the shared one-at-a-time administrative-work slot. Parsing,
source capture, discovery, registry installation, snapshot publication, and serialization run on a
blocking worker that owns admission through completion. The engine guard is held only to clone live
sources and later to install the precomputed review metadata; O(corpus) discovery runs without it.
Admitted work has no execution timeout and completes coherently if the client disconnects. Invalid
transport or controls return 400/408/413/415 as applicable, closed admission returns 503, and an
internal mutation or worker failure returns 500. Every response is no-store and uses fixed
`vocab_aliases_discover_and_record` telemetry.

Coordinator mode validates that same request contract, then returns a no-store 501. Run dry
`/_vocab/aliases/discover` against an explicit corpus, review the proposals, and install the edited
registry with `PUT /_vocab`; recording review metadata alone does not justify the coordinator's
full blue/green vocabulary rebuild.

This is a native governance operation, not an Elasticsearch/OpenSearch synonym-management alias:
those systems accept explicit synonym rules rather than mining stored reverse queries and recording
never-active review candidates. The contract and persistence boundary are recorded in
[ADR-155](../../../decisions/adr-155-alias-discover-record-api-contract.md).
