# Ranking profiles

This page owns the user-facing ranking contract: score semantics, the profile-file format and
limits, feature meanings, loading rules, and topology behavior. Request and response shapes remain
canonical in the [percolate API reference](api/percolate.md); deployment commands remain in the
[operations runbooks](../operations/deployment-modes.md); implementation details remain in the
[matching design](../design/matching.md#54-ranking--an-optional-layer-over-the-boolean-correct-set).

## 1. Correctness boundary and score

Reverse Rusty determines the complete Boolean match set before ranking. A profile can reorder,
truncate, or paginate confirmed matches; it cannot make a query match or stop one from matching.
The native ranked APIs use saturating signed 64-bit arithmetic:

```text
score = profile relevance + typed priority + sum(matching tag boosts)
order = score descending, then logical query ID ascending
```

The built-in `static_v1` profile contributes zero relevance and preserves the historical
priority-plus-boost score. Operator-loaded profiles can add title-dependent relevance:

- `linear` evaluates an intercept plus a weighted sum over the fixed feature schema.
- `tree_ensemble` evaluates bounded integer decision trees. It is the serving shape for a
  conventional LambdaMART-style model; training is deliberately outside the engine.

Every confirmed match is scored. `size` or K bounds collector memory and returned rows, not the
number of profile evaluations. Boolean membership is therefore identical across `static_v1`,
linear, and tree profiles.

## 2. Selecting a profile

Named profiles are available on the strict native ranking surfaces:

- `POST /v2/_search`;
- `POST /v2/_mpercolate`;
- `POST /_percolate/jobs` with exhaustive delivery.

The native `rank` object accepts `profile`, `priority_field`, and additive tag `boosts`:

```json
{
  "rank": {
    "profile": "linear_v1",
    "priority_field": "priority",
    "boosts": [
      {"key": "tier", "value": "gold", "boost": 1000}
    ]
  }
}
```

Within a native rank program, omitting `profile` selects `static_v1`. The bounded v2 endpoints
always compile that default program; an exhaustive job with no `rank` object emits unscored
members. Native HTTP rank programs default `priority_field` to `"priority"`; an absent or
non-numeric stored priority contributes zero. Unknown profiles return `400 unknown_rank_profile`,
and an unsupported priority field returns `400 unsupported_rank_field`. The complete endpoint
controls, defaults, and response envelopes are in the
[percolate API reference](api/percolate.md).

The compatibility `/_search` and `/_mpercolate` ranking block remains a separate static-policy
contract: it accepts `priority_key` plus tag boosts and does not select named relevance profiles.
Use the native surfaces when title-dependent relevance or bounded top-K collection is required.

Library callers load [`RankProfiles`](../../engine/src/rank.rs), then compile a
`RankProgramSpec` against that registry. A default registry contains only `static_v1`.

## 3. Profile file

`--ranking-profiles-file <path>` loads one immutable registry before the server binds.
`RR_RANKING_PROFILES_FILE` is the environment equivalent; an explicit flag takes precedence.
The registry is serving configuration rather than corpus state: it is not stored in the engine data
directory, included in backups, exposed by `GET /_settings`, or mutable through `PUT /_settings`.
Changing it requires a process restart.

The checked-in
[`deploy/ranking-profiles.example.json`](../../deploy/ranking-profiles.example.json) is the
executable format example and is exercised by tests and benchmarks. Its weights illustrate the
schema; they are not trained relevance evidence.

The root object is strict JSON:

```json
{
  "version": 1,
  "profiles": {
    "linear_v1": {
      "kind": "linear",
      "expected_fingerprint": "fnv1a64:b79de9ce5a0231d6",
      "intercept": 0,
      "weights": [
        {"feature": "query_positive_terms", "weight": 120}
      ]
    }
  }
}
```

Profile names contain 1–64 lowercase ASCII letters, digits, `_`, or `-`. Duplicate names and
unknown fields are rejected. Supported kinds are:

- `static`: zero relevance. `static_v1` is always built in and cannot be replaced with different
  semantics.
- `linear`: optional `intercept` plus unique `{feature, weight}` entries.
- `tree_ensemble`: optional `base_score` plus one or more flat trees. Each tree is rooted at node
  zero. A split contains `feature`, inclusive `threshold`, `left`, and `right`; a leaf contains
  `value`. Every non-root node has exactly one parent, every node is reachable, and cycles are
  rejected.

`expected_fingerprint` is optional. When present it must be
`fnv1a64:<16 lowercase hexadecimal digits>` and equal the semantic fingerprint computed after
validation. Every process logs its loaded `name@fingerprint` identities at startup and refuses a
configured mismatch.

### Admission limits

| Scope | Limit |
|---|---:|
| Registry file | 16 MiB |
| Operator-defined profiles | 64 |
| Linear terms per profile | 64 |
| Trees per ensemble | 1–256 |
| Total nodes per ensemble | 16,384 |
| Depth per tree | 16 |
| Sum of maximum tree depths per ensemble | 1,024 |

These bounds are startup admission limits. Scoring itself remains integer-only and allocation-free.

## 4. Fixed feature schema

Feature names and meanings are versioned semantics. Changing an existing meaning would require a new
feature name rather than silently changing a deployed model.

| Feature | Integer value |
|---|---|
| `query_positive_terms` | Required positive features plus phrase positions and the shortest satisfiable member of each positive any-of group |
| `query_negative_terms` | Forbidden features plus forbidden phrase positions and features in forbidden conjunctions |
| `query_any_of_groups` | Semantic positive any-of groups, counted before retrieval proxies or repeated exact predicates are deduplicated |
| `query_tag_count` | Metadata tags on the newest live query row |
| `title_tokens` | Runs of Unicode alphanumeric characters in the incoming title |
| `title_bytes` | UTF-8 byte length of the incoming title |
| `title_digits` | Unicode numeric characters in the incoming title |
| `positive_coverage_milli` | `query_positive_terms * 1000 / max(title_tokens, 1)` using saturating integer arithmetic |
| `unmatched_title_tokens` | `max(title_tokens - query_positive_terms, 0)` |

Query-side evidence comes from the persisted exact-verification representation, not source parsing
on the hot path. Phrase graphs count analyzer positions; compound any-of groups use their shortest
satisfiable branch. Source-driven migration rebuilds older durable rows before rich profiles can
observe semantics introduced by a newer compiler.

## 5. Topologies and distribution

Named profiles work in all supported modes:

| Mode | Registry loading |
|---|---|
| Single node | Load the file in `server` |
| In-process cluster | Load the file in the coordinator `server`; compiled programs are shared with its local shards |
| Remote Compose | Mount the same file into the coordinator and every `shardserver` with `compose.ranking-profiles.yml` |
| Remote Helm/Kubernetes | Mount one operator-owned ConfigMap, PVC, or CSI-backed volume into the coordinator and every shard pod |
| Direct gRPC/library assembly | Install the same registry on each `ShardServer` with `with_rank_profiles` and compile requests against the coordinator registry |

Remote requests carry the selected profile name and semantic fingerprint, not model bytes. Each
shard resolves that exact identity before scoring, then echoes it in a scalar reply or terminal
batch/exhaustive summary. The coordinator accepts results only after the echo matches. Unknown
profiles, drifted model content, missing terminal attestations, and pre-attestation peers fail the
whole ranked operation; Reverse Rusty never silently falls back to `static_v1`.

The [Compose runbook](../operations/cluster-deployment.md#25-optional-named-ranking-profiles) and
[Kubernetes runbook](../operations/kubernetes-deployment.md#21-optional-named-ranking-profiles)
own the exact mount and rollout commands. A ConfigMap is limited by Kubernetes object size; the
chart's generic `volumeSource` supports larger engine-valid registries from PVC or CSI storage.

## 6. Rollout and compatibility

Profiles are startup-loaded and immutable. For uninterrupted remote ranked traffic:

1. distribute the same immutable, fingerprint-pinned file to every node;
2. restart and verify all shards;
3. restart the coordinator;
4. select the newly installed profile only after the mesh is green.

A new shard accepts a legacy request without identity only as `static_v1`. A new coordinator
rejects an old shard that omits the terminal identity, including for `static_v1`; a mixed rollout
may therefore fail requests but cannot return silently inconsistent scores. The full procedure and
rollback rules are in the [rolling-upgrade runbook](../operations/rolling-upgrade.md).

## 7. Quality and performance evaluation

The shipped profiles are serving mechanisms, not trained models. Use a title-grouped,
time-separated corpus and report ranking quality (for example NDCG@K and precision@K) separately
from Boolean recall. Historical matches are positive-but-incomplete labels; unlabeled pairs should
not automatically become negatives.

Latency must be segmented by confirmed-match count and query cost class because every confirmed
match is evaluated. Current measured CPU costs and reproduction commands live only in the
[performance results](../performance/results.md#cpu-rank-profile-cost) and
[benchmark runbook](../performance/benchmark-results.txt). Training and model-selection rationale
live in the [learned-ranking research note](../research/learned-ranking.md); architecture rationale
and compatibility history remain in [ADR-162](../decisions/adr-162-versioned-cpu-ranking-profiles.md)
and [ADR-163](../decisions/adr-163-distributed-ranking-profile-attestation.md).
