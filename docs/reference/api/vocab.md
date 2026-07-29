# Vocabulary & alias APIs

> [REST API hub](../api.md) · [Normalization design](../../design/normalization.md)

Read, replace, learn, and govern the feature vocabulary and alias registry.

## Vocabulary

| API | What it does | Availability |
|---|---|---|
| [`GET\|HEAD /_vocab`](vocab/get-vocab.md) | Return the round-trippable active vocabulary or bodyless metadata. | Single-node and coordinator modes |
| [`PUT /_vocab`](vocab/replace-vocab.md) | Replace and synchronously activate a complete vocabulary. | Local engine/in-process cluster; remote coordinator refuses normalizer divergence |
| [`POST /_vocab/learn`](vocab/learn-vocab.md) | Compute review-first synonyms and optional corpus phrases from supplied query text. | Single-node and coordinator compute modes |
| [`POST /_vocab/learn_and_apply`](vocab/learn-and-apply.md) | Learn from stored queries and synchronously activate the result. | Local engine/in-process cluster |

## Governed aliases

Learned aliases carry provenance, confidence, activation state, and optional feedback evidence.
Multi-word aliases use the positive alias view for required matching while forbidden checks retain
the canonical view (ADR-060/061/102/103).

| API | What it does | Availability |
|---|---|---|
| [`GET\|HEAD /_vocab/aliases`](vocab/alias-registry.md) | Page through governed alias entries and registry-wide summary counts. | Single-node and coordinator modes |
| [`POST /_vocab/aliases/import`](vocab/alias-import.md) | Strictly import and apply native or Solr-style alias rules. | Local engine/in-process cluster |
| [`POST /_vocab/aliases/learn_and_apply`](vocab/alias-learn-and-apply.md) | Learn aliases from stored queries and synchronously apply eligible rules. | Local engine/in-process cluster |
| [`POST /_vocab/aliases/discover`](vocab/alias-discover.md) | Compute review-only distributional alias proposals. | Single-node or explicit-corpus coordinator compute |
| [`POST /_vocab/aliases/discover_and_record`](vocab/alias-discover-and-record.md) | Discover from the local stored corpus and record review candidates without changing matching. | Single-node |
| [`GET\|HEAD /_vocab/aliases/feedback`](vocab/alias-feedback.md#read-feedback-evidence) | Page through captured behavioral evidence. | Single-node when capture is enabled |
| [`POST /_vocab/aliases/feedback/reset`](vocab/alias-feedback.md#reset-the-measurement-window) | Reset evidence counters while preserving tracked candidate pairs. | Single-node |
| [`POST /_vocab/aliases/validate_and_apply`](vocab/alias-feedback.md#validate-and-optionally-activate) | Stamp review evidence and optionally activate eligible candidates. | Single-node |
