# ADR-148: Vocabulary learning REST API contract

> [Engine, errors, dependencies & ops decisions](areas/engine-quality-and-operations.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

`POST /_vocab/learn` correctly composed the any-of synonym/equivalence learner with optional NPMI
phrase induction and returned a reviewable `Vocab` without applying it. Its HTTP boundary remained a
prototype. It silently accepted unknown JSON fields and query parameters, inherited the global
100 MiB bulk-ingest ceiling, exposed framework JSON rejections, and had no body deadline, cache
policy, route telemetry, compute admission, or blocking-worker boundary.

The learner also accepted zero or unbounded counts and phrase-growth passes, silently ignored NPMI
controls when phrase induction was disabled, skipped invalid DSL in the any-of learner while still
mining its raw tokens for phrases, and counted duplicate IDs as independent query evidence.
Coordinator mode had a second undocumented contract: omitting `queries` gathered its stored corpus,
whereas standalone mode required the field. That made the same request mode-dependent and blurred
the dry-run endpoint with `POST /_vocab/learn_and_apply`.

[Elasticsearch's create-or-update synonym-set API](https://www.elastic.co/guide/en/elasticsearch/reference/current/put-synonyms-set.html)
stores one named set of explicit Solr-format rules. OpenSearch exposes explicit synonym rules through
its [synonym token filter](https://docs.opensearch.org/latest/analyzers/token-filters/synonym/).
Neither API learns Reverse Rusty's complete vocabulary document from reverse-query DSL, and neither
models its optional NPMI phrases or expansion equivalences. Reusing `/_synonyms/{id}` would therefore
misstate both the input and product.

## Decision

- Keep `POST /_vocab/learn` native. Do not add a `/_synonyms/{id}` alias or accept analyzer controls.
  Success remains one complete bare `Vocab`, so it can be inspected, edited, and submitted directly
  to `PUT /_vocab`.
- Require the caller-supplied `queries` corpus in standalone and coordinator modes. Never substitute
  stored queries. The separate `learn_and_apply` operation owns learning from live stored state.
- Accept only query-free POST with `application/json` or an `application/*+json` vendor media type.
  Reject unknown fields, malformed tuple entries, duplicate IDs, and invalid DSL through the
  standard JSON error envelope. An explicit empty array is a valid deterministic dry run.
- Bound the request at 16 MiB, five seconds of body-read time, and 100,000 unique query entries.
  Parse every entry with the public DSL ceilings of 10,240 bytes, 256 clauses, and 64 any-of members
  before learning. Also cap potential any-of relationship observations and NPMI corpus tokens at
  100,000 each. These are admission limits, not a change to embedded learner behavior.
- Require `min_count >= 1`. Accept NPMI controls only when `corpus_phrases` is true; require finite
  `npmi_tau` in `[-1, 1]`, `npmi_min_count >= 1`, and `npmi_iterations` in `1..=8`. The iteration
  ceiling bounds deliberate growth passes while leaving ample room beyond the default of two.
- Share the one-per-server administrative permit with stats and vocabulary reads/replacements.
  Wait for admission asynchronously, then move the permit, JSON decoding, duplicate/DSL validation,
  corpus learning, and JSON serialization into a blocking worker. This prevents CPU-heavy corpus
  jobs and large validation/serialization work from blocking Tokio workers or multiplying in the
  blocking pool.
- Count a learned relationship at most once per query even if its any-of group is repeated. The
  threshold therefore measures distinct query evidence as documented, for both collapse synonyms
  and expansion equivalences.
- Refuse a result above 100,000 vocabulary entries or 16 MiB and ask the caller to raise learning
  thresholds. The byte ceiling matches `PUT /_vocab`, so every successful dry-run document can be
  submitted back to the advertised apply path.
- Attach `Cache-Control: no-store` to every route-reached outcome. Count and time every outcome
  under the fixed `vocab_learn` endpoint label, beginning before transport validation. Return a
  sanitized 500 if the worker or serialization fails and 503 if administrative admission is closed.

## Consequences

The dry run now means the same thing in both server modes: exactly the submitted, uniquely
identified, syntactically valid corpus produces exactly one reviewable vocabulary document. Invalid
text cannot contribute to only one learner, ignored controls fail loudly, and clients get stable
structured transport errors and explicit resource limits.

Large accepted bodies are buffered before the administrative permit is acquired. The 16 MiB
transport ceiling bounds that memory; decoding happens only after admission. There is no execution
deadline after admission: an accepted CPU job is allowed to finish and release its owned permit even
if the client disconnects.

## Safety and proof

This endpoint does not mutate engine, cluster, durable, or feature-dictionary state. Its output uses
the existing deterministic learners and remains subject to `Vocab::to_normalizer` validation when a
client applies it. Rejecting invalid DSL and duplicate IDs makes the documented "different queries"
threshold truthful without altering the lossless signature-cover contract.

Standalone route tests pin strict method/query/media/JSON handling, vendor JSON, body size/deadline,
configuration and DSL/work bounds, unique IDs, distinct-query thresholds, corpus cardinality,
round-trippable no-store output, whole-route telemetry, asynchronous admission, and closed
admission. Coordinator tests pin the same required caller-corpus semantics and response/telemetry
contract.
