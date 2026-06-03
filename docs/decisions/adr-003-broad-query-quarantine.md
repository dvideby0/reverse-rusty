# ADR-003: Broad-query quarantine via cost classes

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted


- **Context:** Some queries are inherently non-selective (e.g., a bare "jordan" with no
  year/grade). In a flat index these poison the hot path — one broad posting list can
  dominate match time.
- **Decision:** Classify queries at compile time into cost classes A/B/C/D. Class C (too
  common to be selective) is routed to a separate batch/columnar lane. Class D (effectively
  unconstrained) is rejected with rewrite suggestions.
- **Consequence:** The selective (A/B) path stays fast and predictable. Broad lane is ~9×
  slower but isolated. Class D rejection forces query authors to add specificity.

