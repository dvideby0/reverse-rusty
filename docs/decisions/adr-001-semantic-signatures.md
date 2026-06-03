# ADR-001: Semantic signatures over term-level gating

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted


- **Context:** Generic percolators (Lucene Monitor, ES/OS) gate on raw terms extracted from
  queries. This works for full-text search, but product queries have structure — the same word
  means different things in different positions ("jordan" = player, brand, or year subset).
  Term-level gating retrieves too many false-positive candidates.
- **Decision:** Gate on 2–3 *semantic* feature combinations (e.g., `player:jordan +
  year:1994 + grader_grade:psa10`) produced by a domain-aware normalizer, rather than raw
  terms.
- **Consequence:** Flat ~54 candidates/title regardless of corpus size (measured 1M–5M).
  Requires a shared normalizer that maps both queries and titles into the same feature space.
  Makes the system domain-specific rather than generic.

