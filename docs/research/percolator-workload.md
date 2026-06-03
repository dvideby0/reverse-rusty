# Reference workload — a metadata-filtered percolator deployment

*Scope: an abstract description of how production percolator deployments are **actually used** — the
read/write patterns Reverse Rusty aims to serve. Distilled from a real deployment but stated in generic
terms (stored queries + structured tags + filter predicates; no domain specifics). This is the reference
workload behind the Tier-4 "percolator parity" roadmap ([`../STATUS.md`](../STATUS.md) Tier 4), the
gap analysis in [`prior-art.md`](prior-art.md) §2, and the design in [`../design/matching.md`](../design/matching.md)
§5 / [`../design/ingestion-and-updates.md`](../design/ingestion-and-updates.md) §11 ([`../DECISIONS.md`](../DECISIONS.md)
ADR-049). Companion to [`prior-art.md`](prior-art.md) §2, which surveys the percolator's internals; this
file describes how one is *operated*.*

## The shape

A percolator stores *queries* and matches an incoming *document* against them — "which stored queries
does this document satisfy?", the reverse of normal search. In a production deployment a stored query is
more than a matching expression: it is attached to a business entity and carries **structured metadata**.
A representative stored-query record:

| Field | Role |
|---|---|
| **identity** | a stable foreign key to the owning entity (the thing the query represents) |
| **matching expression** | the query DSL, compiled into the percolator |
| **`category` tag** | a coarse type/class used to *partition* the query set — the dominant filter |
| **`status` tag** | a lifecycle/visibility enum used both to filter and to prioritize |
| **secondary key(s)** | optional finer filters (a sub-type, a region, a grade band, …) |
| **display text** | the human-readable source expression, returned alongside hits |

The matching expression is the familiar boolean shape — **include terms (AND), exclude terms (NOT), and
OR-groups** — compiled into the percolator's term-extraction / candidate-gating machinery.

## Write path

Stored queries are kept in sync by an idempotent **upsert keyed by identity** (so a retried write repairs
rather than duplicates), plus delete-by-identity:

- **Create / update / delete** of an entity re-derives its matching expression and re-indexes it (with
  its tags). A **bulk reindex** job periodically re-asserts the whole set.
- The percolator only needs rewriting when the **matching expression could change** (the entity's name,
  query, or a key that feeds the expression). **Metadata- or status-only changes are cheaper** and, in
  practice, often skip the percolator rewrite entirely — a useful split: the expression and its tags have
  different change rates.

## Read path (the load-bearing pattern)

1. **Normalize** the incoming document (e.g. a listing title) with the *same* normalizer the stored
   queries were compiled under (feature spaces must line up).
2. **Percolate** to retrieve candidate stored queries.
3. **Filter the candidates by tag predicates** — `category ∈ {…}` (almost always supplied), optionally
   `status ∈ {…}` and a secondary key. **This narrowing is the dominant read pattern:** nearly every
   caller percolates and then immediately restricts to one category, sometimes one status.
4. **Optionally rank** the survivors — sort or boost by a priority attribute (e.g. a status priority),
   then **paginate** (limit / offset).
5. Return hits carrying **identity + display text** (and a score where ranking is used).

### Two-stage matching — and why a generic percolator needs it

A generic percolator selects candidates from *extracted terms*, which **over-matches**: the candidate
set is a *superset* of the true matches (the extraction can't perfectly capture every expression's
semantics). Production therefore runs a **second, exact re-test in application code** — re-evaluating
each candidate's expression against the document — before trusting a match. The percolator is the
**recall** stage; the application is the **precision** stage.

### Where ranking is — and isn't — used

In the reference deployment the **core matching jobs do not rank**: they take the tag-filtered candidate
set, exact-re-test it, and emit the surviving entity ids. Ranking / boosting appears only on a
**public, human-facing search surface** (ordering results for a person to read). The lesson for an engine
whose output is *already precise*: **filtering is the high-value capability; ranking is a lower-priority,
presentation-layer concern** that never needs to touch the matching core.

## How Reverse Rusty maps to this

| Workload need | Reverse Rusty today | Disposition |
|---|---|---|
| query identity / foreign key | `logical_id: u64` (caller-supplied) | ✅ have — identity maps 1:1 |
| include / exclude / OR-group DSL | required / forbidden / any-of | ✅ have (full parity) |
| compile-time extract + match-time select | signature-cover optimizer | ✅ have |
| over-match → app re-test (two stages) | integer-exact verifier ⇒ output is false-positive-free | ✅ **subsumes** — RR returns *final* matches, no second pass |
| create / update / delete / bulk | memtable + tombstones + `/_bulk` + per-query `version` | ✅ have |
| **per-query structured tags** | interned `(key,value)` tags, SoA column | ✅ **built (single-node)** — ADR-049 |
| **filter candidates by tag** | ES `bool`/`terms` + native filter, pushed into verify | ✅ **built (single-node)** — ADR-049 |
| rank / boost / `_score` | pure boolean (`Vec<u64>`, no score) | ❌ gap → Tier 4 (lower priority — see above) |
| pagination (limit / offset) | `/_search` has both; `/_mpercolate` is size-only | ◑ partial → Tier 4 |

**Already aligned (or better).** Identity, the boolean DSL, the compile-time-extract / match-time-select
architecture, and the write/update/bulk model all map directly. The **two-stage recall+verify** pattern
is *subsumed*: because RR verifies candidates with an integer-only exact matcher, its output already has
**zero false positives**, so the application-side re-test that a generic percolator forces is unnecessary
— RR returns final matches, not candidates.

**The dominant-read-pattern needs** — **per-query metadata** and **filtered percolation** — are now
**built and oracle-proven on the single-node engine** (specified in
[`../design/matching.md`](../design/matching.md) §5 and
[`../design/ingestion-and-updates.md`](../design/ingestion-and-updates.md) §11, decided in
[`../DECISIONS.md`](../DECISIONS.md) ADR-049, tracked in [`../STATUS.md`](../STATUS.md) Tier 4). Scoring and
`/_mpercolate` pagination remain smaller, lower-priority items in the same tier; threading tags through the
experimental cluster path is a separate follow-on.

> **Validation still owed.** This file specifies the *workload*; it is not a correctness audit. Running RR
> against this workload's **real corpus** (a false-negative / false-positive / throughput pass over messy
> real titles) remains the open, highest-leverage validation step flagged in
> [`../STATUS.md`](../STATUS.md) "Current limitations."
