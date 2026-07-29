# Alias feedback and validation APIs

> [Vocabulary & alias APIs](../vocab.md) · [REST API hub](../../api.md)

ADR-103 can passively compare which queries match titles containing each form of a tracked
two-form candidate. Capture is single-node, in memory, default off, and applies to compatibility
`/_search` and `/_mpercolate` traffic. Enable it with the dynamic settings
`alias_feedback_capture=true`; `alias_feedback_max_pairs` bounds the tracked candidate set.

## Read feedback evidence

`GET` and `HEAD /_vocab/aliases/feedback` return the rolling evidence. This is a native behavioral
report, not an Elasticsearch/OpenSearch synonym-rule API: those systems manage explicit rules rather
than compare passive reverse-query match populations. The route adopts Elasticsearch-familiar
`from`/`size` paging and a total `count` without claiming synonym-set semantics.

Threshold query parameters default to `min_overlap=0.5`, `min_titles=50`, and `min_queries=20`.
`min_overlap` must be finite within `[0,1]`; title and query thresholds must be positive. `from`
defaults to zero and `size` defaults to 256, with a maximum of 256. Unknown, duplicate, malformed,
or out-of-range parameters fail with 400. Out-of-range `from` and `size=0` return an empty page:

```json
{
  "took": 3,
  "took_ms": 3.42,
  "capture_enabled": true,
  "count": 1,
  "tracked_pairs": 1,
  "min_overlap": 0.5,
  "min_titles": 50,
  "min_queries": 20,
  "pairs": [{
    "forms": ["ns", "north star"],
    "titles_a": 75,
    "titles_b": 81,
    "titles_both": 2,
    "sampled_a": 43,
    "sampled_b": 46,
    "excluded": 4,
    "overlap": 0.78,
    "validated": true
  }]
}
```

`count` is the total tracked-pair cardinality before paging. `tracked_pairs` is retained as an equal
compatibility spelling; `pairs` contains the selected page. Every response is no-store and uses the
fixed `vocab_aliases_feedback_get` telemetry label.

The request body must be empty, with a 64 KiB extraction ceiling and 250 ms read deadline. The
serialized page is capped at 1 MiB. The request waits asynchronously for the shared administrative
slot; a blocking worker clones only the page under the feedback mutex, captures the corresponding
engine snapshot, and releases the mutex before source lookup, exclusion filtering, overlap
calculation, and serialization. Closed admission returns 503 and internal worker/serialization
failure returns 500.

## Validate and optionally activate

`POST /_vocab/aliases/validate_and_apply` accepts the same three evidence thresholds and no body.
`min_overlap` must be finite within `[0,1]`; `min_titles` and `min_queries` must be positive.
Defaults are `0.5`, 50, and 20. Unknown, duplicate, malformed, or out-of-range parameters fail with
400 rather than being clamped. The default operation stamps changed evidence and raises confidence
without changing matching:

```json
{
  "took": 4,
  "took_ms": 4.31,
  "acknowledged": true,
  "result": "updated",
  "persisted": false,
  "min_overlap": 0.5,
  "min_titles": 50,
  "min_queries": 20,
  "activate": false,
  "validated": 1,
  "stamped": 1,
  "activated": 0,
  "recompiled": 0,
  "summary": {"active": 0, "candidate": 1, "rejected": 0}
}
```

An identical retry is `result: "noop"` with `stamped: 0`. Add `activate=true` to explicitly promote
eligible validated candidates through a complete query recompile. Rejected or mixed-kind entries
are never resurrected by automation. Activation refuses unhealthy durable state, verifies that
every live source was recompiled with no stale segment left, and returns 503 if a coherent live
rebuild could not be committed. `persisted: false` means the live standalone vocabulary document
was not written back to the startup vocabulary file; save `GET /_vocab` there before restart.

The bodyless request is capped at 64 KiB and 250 ms. It waits asynchronously for the shared
administrative slot, then evidence snapshotting/reporting, source lookup, engine-lock waiting,
metadata mutation, optional O(corpus) recompile, and publication run on a blocking worker. Every
response is no-store and uses fixed `vocab_aliases_validate_and_apply` telemetry.
Other methods return 405 with `Allow: POST`; invalid input is 400/408/413, closed admission is 503,
and internal/incomplete work is 500.

## Reset the measurement window

`POST /_vocab/aliases/feedback/reset` starts a new process-local evidence window. It accepts no
query parameters or body; extraction is capped at 64 KiB with a 250 ms deadline. The operation
waits asynchronously for the shared administrative slot and clears counters and sketches on a
blocking worker. The feedback mutex is the exact window boundary: observations before the clear are
removed and observations after it enter the new window. Candidate pairs remain tracked and
pre-tokenized, so reset neither republishes an unchanged engine snapshot nor creates a capture gap.

```json
{
  "took": 0,
  "took_ms": 0.084,
  "acknowledged": true,
  "capture_enabled": true,
  "tracked_pairs": 2
}
```

Every reset outcome is no-store and uses the fixed `vocab_aliases_feedback_reset_post` telemetry
label. Other methods return 405 with `Allow: POST`; invalid query/body is 400, a stalled body is
408, oversized input is 413, closed admission is 503, and a blocking-worker failure is 500.

All three feedback endpoints return 501 in cluster mode after validating their shared transport
contracts and return observed no-store alternatives. Run capture and validation on a single-node
replica of the title stream and install reviewed activations through cluster `PUT /_vocab`. The
contracts are recorded in
[ADR-156](../../../decisions/adr-156-alias-feedback-read-api-contract.md) and
[ADR-157](../../../decisions/adr-157-alias-feedback-reset-api-contract.md), with application in
[ADR-158](../../../decisions/adr-158-alias-feedback-validate-apply-api-contract.md).
