# Learned ranking for confirmed reverse-query matches

Reverse Rusty has an unusually clean ranking boundary: retrieval and exact verification determine
the complete Boolean match set first. Ranking therefore optimizes the order of known matches rather
than compensating for an approximate first-stage retriever.

## Practical serving ladder

1. **Static policy:** priority plus tag boosts. This remains the deterministic control and business
   override layer.
2. **Linear relevance:** a weighted integer feature sum. It is inexpensive, easy to inspect, and a
   useful first learned baseline.
3. **Boosted decision trees:** LambdaMART-style learning usually provides the strongest conventional
   CPU ranker for heterogeneous tabular relevance features. Microsoft describes LambdaMART as
   LambdaRank gradients applied to boosted regression trees
   ([overview](https://www.microsoft.com/en-us/research/publication/from-ranknet-to-lambdarank-to-lambdamart-an-overview/));
   LightGBM provides efficient histogram-based tree learning and ranking objectives
   ([paper](https://proceedings.neurips.cc/paper_files/paper/2017/hash/6449f44a102fde848669bdd9eb6b76fa-Abstract.html)).
4. **Neural reranking:** a cross-encoder can capture interactions the compact feature schema misses,
   but should score only a bounded first-stage window. Vespa's phased-ranking design makes the same
   cost boundary explicit: later ranking phases run only on selected top hits
   ([documentation](https://docs.vespa.ai/en/ranking/phased-ranking.html)).

OpenSearch's learning-to-rank plugin follows the same broad pattern: define features, log them for
training, upload a model, and apply it after initial retrieval
([overview](https://docs.opensearch.org/latest/search-plugins/ltr/index/),
[feature logging](https://docs.opensearch.org/latest/search-plugins/ltr/working-with-features/),
[model upload](https://docs.opensearch.org/docs/latest/search-plugins/ltr/training-models/)).
Reverse Rusty does not copy that runtime; it adopts the separations that matter here.

## Evaluation shape

The deployment-owned corpus should group rows by incoming title and retain:

- the stored query ID or stable query identity;
- the title;
- the observed matched/not-matched label and, where available, a stronger quality label;
- business metadata used by priority or boosts;
- a time split so near-duplicate listings do not leak across training and evaluation.

The observed production matches are positive-but-incomplete labels: the old matcher may have missed
valid pairs. Treat unlabeled pairs as unknown, not automatically negative. Audit a stratified sample
of disagreements, report Boolean recall separately from ranking quality, and use ranking metrics
such as NDCG@K, precision@K, and reciprocal rank on groups with reviewed judgments.

Latency evaluation must segment titles by confirmed-match count and query cost class. Static,
linear, and tree profiles all evaluate every confirmed match; response K bounds retained hits but
does not cap scoring work. Report median and tail latency plus evaluations/title, especially for
broad-heavy traffic. Neural work, if justified later, needs its own bounded rerank window, batching
policy, queue deadline, and explicit fail behavior.

## Shipped boundary

[ADR-162](../decisions/adr-162-versioned-cpu-ranking-profiles.md) implements the first three CPU
steps as named profiles. The checked-in example demonstrates and fingerprints the format; it is not
trained evidence. The next decision should be driven by representative-corpus quality and cost
measurements, not by adding model complexity in advance.
