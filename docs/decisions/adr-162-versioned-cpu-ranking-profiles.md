# ADR-162: Versioned CPU ranking profiles

> [Percolator parity decisions](areas/percolator-parity.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

Reverse Rusty's Boolean matcher determines which stored queries match an incoming title. Its
bounded delivery path historically ordered those confirmed matches only by stored priority and
request tag boosts. Those business signals are useful but title-independent: two matching queries
with the same metadata tie even when one describes the title much more specifically.

A production matched-title corpus can support an offline ranking audit and later supervised
training. Serving must remain CPU-friendly, deterministic, bounded, and separate from the
lossless candidate-cover proof. The engine must also preserve the existing static score by default,
avoid a dependency on a training runtime, and never silently run a different model on one shard.

[LambdaMART](https://www.microsoft.com/en-us/research/publication/from-ranknet-to-lambdarank-to-lambdamart-an-overview/)
combines LambdaRank's ranking gradients with boosted regression trees. LightGBM demonstrates an
efficient histogram-based implementation and includes learning-to-rank objectives
([paper](https://proceedings.neurips.cc/paper_files/paper/2017/hash/6449f44a102fde848669bdd9eb6b76fa-Abstract.html)).
Search systems commonly separate candidate retrieval from increasingly expensive reranking:
[Vespa phased ranking](https://docs.vespa.ai/en/ranking/phased-ranking.html) applies later phases
only to top candidates, while
[OpenSearch learning to rank](https://docs.opensearch.org/latest/search-plugins/ltr/index/)
extracts features and applies an uploaded model after retrieval.

## Decision

- Add a startup-loaded registry of named, versioned CPU profiles. `static_v1` is built in and
  preserves the historical score. A strict JSON file may add `linear` and `tree_ensemble` profiles;
  the server loads it with `--ranking-profiles-file`.
- Let native bounded and exhaustive requests select a profile with `rank.profile`. Omission means
  `static_v1`. Unknown profiles fail with `400 unknown_rank_profile`.
- Keep Boolean truth authoritative. Every selected profile runs only after exact verification and
  may reorder confirmed matches but cannot add or remove one. The final score is:

  ```text
  profile relevance + typed priority + matching tag boosts
  ```

  Integer addition and multiplication saturate at the `i64` bounds; ties remain
  `(score desc, logical_id asc)`.
- Define one immutable v1 feature schema derived from existing exact columns and the incoming title:
  required-term count, forbidden-term count, any-of-group count, tag count, title token/byte/digit
  counts, positive-term coverage per thousand, and unmatched title-token count. Title features are
  computed once per title. Query term counts include predicate-backed semantics: quoted graphs count
  analyzer positions, forbidden conjunctions count their features, and compound any-of groups count
  the shortest satisfiable member. Distinct semantic any-of clauses remain distinct even when their
  retrieval proxies or exact predicates deduplicate. Query features are reconstructed from existing
  memory or mmap columns. No segment column is added; ranking-aware predicate-program versions
  preserve both the pre-dedup group count and shortest-member term total. Compiler semantics 6
  certifies that metadata: standalone recovery source-rebuilds semantics 0–5 before serving, while a
  shard that cannot coordinate that rebuild fails loud. Older predicate programs remain readable
  during migration, and older binaries reject the new program version rather than misinterpret it.
  The ranking-only prefix is excluded from canonical Boolean body identity, so equivalent queries
  can still share candidate retrieval and exact verification while each member keeps its own score.
- Compile linear weights and flat quantized tree ensembles once at startup. Scoring is allocation-free
  and integer-only. Admission bounds the file at 16 MiB, profiles at 64, linear terms at 64, trees
  at 256, total tree nodes at 16,384, depth at 16, and aggregate tree-evaluation steps at 1,024.
  Invalid graphs, cycles, unreachable nodes, duplicate profile names or linear features, and
  unknown JSON fields fail startup.
- Give each compiled profile a stable semantic FNV-1a 64 fingerprint. Configurations may pin an
  `expected_fingerprint`; mismatch fails startup. Startup logs sorted `name@fingerprint` identities.
  PIT and exhaustive-job request identities include the effective profile name.
- Support rich profiles in single-node and in-process cluster modes. Keep the existing gRPC static
  rank wire unchanged; a remote shard rejects a non-static profile before making the RPC with
  `501 rank_profile_transport_unsupported`. Model distribution plus fingerprint attestation is a
  separate wire-compatibility decision.
- Extend `rankbench` with optional profile-file and profile-name arguments. Learned profiles still
  score every confirmed match; K bounds retained results and response volume, not model evaluations.
  The supplied profile file is illustrative and fingerprint-pinned, not a trained production model.

## Alternatives considered

- **Keep only static business ranking.** Cheapest, but it cannot learn title-dependent relevance
  from the matched-title corpus.
- **Implement BM25-style scoring.** It is a useful feature, not the best complete ranker for this
  workload: exact Boolean matching already establishes term presence, marketplace titles are short,
  and a supervised tree model can combine specificity, length, numeric, and business features.
- **Embed a LightGBM training/runtime dependency.** Training stays offline. A bounded native
  interpreter is smaller, deterministic, and easier to attest across shards.
- **Expose scorer switches as independent flags.** Named immutable profiles avoid combinatorial
  behavior, make requests reproducible, and let model identity participate in cursor/job semantics.
- **Add a neural cross-encoder now.** A GPU/accelerator reranker belongs in a later bounded cascade
  over a small first-stage candidate set. It would introduce batching, queueing, timeout, fallback,
  and model-serving semantics that this CPU increment does not need.

## Consequences

The serving path now covers the practical CPU progression from static policy, through a cheap
linear baseline, to nonlinear LambdaMART-style inference without changing matching correctness.
The feature set is deliberately small; representative-corpus NDCG/precision evaluation must decide
whether it is sufficient before expanding it. Profile score units must be calibrated against
priority and boost ranges because the layers are additive.

Top K prevents unbounded result retention and delivery, but scoring cost remains proportional to
the number of confirmed matches. Broad queries with large match sets therefore need benchmark and
admission scrutiny even with a small K. A future expensive reranker must operate on a bounded
first-stage window rather than on every match.

Remote multi-process deployments remain on `static_v1` until the rank wire can distribute or
reference a model and attest the same semantic fingerprint on every shard.

## Safety and proof

Unit tests validate feature extraction, linear/tree scores, graph rejection, reserved static
semantics, fingerprint pins, and the deployed example. Ranking integration tests prove that
title-dependent profiles change order and scores without changing membership, and that batched
titles preserve the correct feature row across parallel chunks. Regressions cover shared proxies,
deduplicated compound predicates, flush/mmap parity, and source-driven migration from semantics 5.
REST tests cover profile selection and unknown-profile rejection. Existing top-K, PIT, exhaustive,
distributed, oracle, and durability suites continue to exercise `static_v1` as the
backward-compatible default.
