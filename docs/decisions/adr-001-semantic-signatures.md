# ADR-001: Semantic signatures over term-level gating

> [Matching & verification decisions](areas/matching-and-verification.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted


- **Context:** Generic percolators (Lucene Monitor, ES/OS) gate on raw terms extracted from
  queries. Product-intent queries often contain several positive requirements, and probing a common
  raw term alone retrieves too many false-positive candidates.
- **Decision:** Gate on lossless combinations of positive features (for example
  `entity:wireless_mouse + year:2024 + brand:north_star`) produced by the shared generic
  normalizer and caller-supplied vocabulary.
- **Consequence:** Flat ~54 candidates/title regardless of corpus size (measured 1M–5M).
  Requires a shared normalizer that maps both queries and titles into the same feature space.
  Domain semantics remain data supplied through vocabulary; candidate planning itself is generic.
