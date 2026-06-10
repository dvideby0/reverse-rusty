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
| query identity / foreign key | `logical_id: u64` (caller-supplied) | ✅ have — identity maps 1:1 (composite/string keys are the caller's encoding, e.g. a type ordinal in the high bits) |
| include / exclude / OR-group DSL | required / forbidden / any-of | ✅ have (translation contract verified — §Drop-in parity below) |
| compile-time extract + match-time select | signature-cover optimizer | ✅ have |
| over-match → app re-test (two stages) | integer-exact verifier ⇒ output is false-positive-free *under RR's semantics* | ✅ subsumes under RR semantics; fronting a **foreign** precision stage instead requires the superset contract — **verified** (ADR-064 PoC, §Drop-in parity) |
| create / **update** / delete / bulk | memtable + tombstones + `/_bulk` per-item statuses (ADR-018) + per-query `version` | ◑ divergence — a REST re-`PUT` is *additive* (old copy matches until DELETE); atomic upsert = [ADR-064](../DECISIONS.md) item 1 |
| **exclude-only (pure-negative) queries** | rejected at ingest (cost class D) | ❌ divergence — ES/OS rewrites them to *match-all-except* (`fixNegativeQueryIfNeeded`); opt-in always-candidate lane = [ADR-064](../DECISIONS.md) item 2 (interim: caller side-list) |
| write visibility / read-your-writes | every write publishes a fresh snapshot before responding | ✅ **better** — immediate, no refresh interval |
| **per-query structured tags** | interned `(key,value)` tags, SoA column | ✅ **built** — ADR-049 (single-node) + ADR-055 (cluster); values are strings — non-string ingest values currently dropped silently ([ADR-064](../DECISIONS.md) item 4) |
| **filter candidates by tag** | ES `bool`/`terms` + native filter, pushed into verify | ✅ **built** — ADR-049/055. AND-across-keys / OR-within-a-key only (no cross-key OR / `must_not` — covered by two calls + client union) |
| rank / boost / `_score` | additive request-boosts + priority tag, `(score desc, _id asc)` | ✅ **built single-node** — ADR-059. *Ordering* parity (e.g. boosted-status-first banding) reproduces; multiplicative score arithmetic does not (and is not needed by the workload) |
| pagination (limit / offset) | `from`/`size` on `/_search` + `/_mpercolate`, untruncated `total` | ✅ **built** — ADR-059 |

**Already aligned (or better).** Identity, the boolean DSL, the compile-time-extract / match-time-select
architecture, and the write/bulk model map directly. The **two-stage recall+verify** pattern is *subsumed*
**under RR's own semantics**: RR verifies candidates with an integer-only exact matcher, so its output has
zero false positives *relative to its own compiled query semantics*. An important honesty note the
drop-in audit added: when RR fronts a deployment that **keeps its own precision stage** (a different
implementation with its own tokenization/regex semantics), subsumption is not automatic — the requirement
becomes **RR candidates ⊇ that stage's accepts**, which is exactly what the §Drop-in parity contract below
establishes and the ADR-064 PoC verified empirically.

**The dominant-read-pattern needs** — **per-query metadata**, **filtered percolation**, and **ranking +
pagination** — are **built and oracle-proven** (ADR-049 single-node, ADR-055 through the cluster, ADR-059
ranking/pagination single-node; specified in [`../design/matching.md`](../design/matching.md) §5 and
[`../design/ingestion-and-updates.md`](../design/ingestion-and-updates.md) §11, tracked in
[`../STATUS.md`](../STATUS.md) Tier 4). The remaining operational divergences form the
[ADR-064](../DECISIONS.md) work package.

## Drop-in parity: the verified configuration + translation contract (ADR-064, 2026-06)

A drop-in-replacement audit translated **real stored queries** from the reference deployment's query
grammar into the RR DSL and verified the recall contract empirically — ground truth computed by
*executing the reference deployment's own application-side precision matcher* on pinned (query, title)
pairs. Result: **zero false negatives on every precision-stage-accepting pair** (38/38, plus the
exclude-only side-list case below), with every false positive predicted in advance. The contract:

**The parity configuration.** Run RR with an **empty vocabulary** (no graders, grade words, phrases,
synonyms, or equivalences — the reference deployment canonicalizes titles client-side before percolating,
so RR's vocab machinery is a *later recall upgrade*, not a migration requirement) plus punctuation
overrides **`.` `#` `/` → `split`** (defaults already split the rest). `.`→split is load-bearing: the
reference matcher tolerates trailing `.`/`,` on a token, so keeping `.` would turn `card.`-style title
tokens into distinct features — a real FN; splitting makes decimals like `9.5` ≈ `{9, 5}` instead, an
FP-only loosening the precision stage re-filters. Run with the **broad lane enabled** (`--include-broad`
or per-request on `/_mpercolate`) — single-hot-term queries are class C and *silently unmatchable*
otherwise — and raise `ParseLimits` to envelope the corpus (side-listing anything still rejected).

**Translation rules** (each preserves semantics or makes RR strictly *more* permissive — FP-only, which
the precision stage re-filters):
1. Bare terms and OR-groups translate 1:1; quoted phrases translate 1:1 but compile to a **bag of
   required tokens** (no adjacency) — a recall superset of phrase semantics.
2. **Drop all negations.** Single-token negations *look* 1:1 but have demonstrated FN edges (the
   reference matcher's end-of-string regex quirks, multi-feature negated tokens like `-9.5`, query-side
   diacritic asymmetry); forbidden clauses only ever *narrow*, and the precision stage re-applies every
   NOT exactly — so dropping them is the only zero-FN translation. (Cost: candidate volume, not
   correctness.)
3. A term with inner `-`/`/`/`'` becomes the space-split quoted phrase of its parts (the only variant
   that is live title-side after the deployment's client normalization).
4. Trailing `*` needs no text mutation — RR's punctuation table splits it away at compile time
   (requiring the bare token) while the reference precision stage literalizes it; the stored text keeps
   the original form.
5. Translate from the reference grammar's **parsed structure**, not raw text (inheriting its parser's
   quirky state machine for free), and render the RR query as text valid under **both** grammars.
   **Round-trip caveat:** hits return `_source` and a consumer's precision stage re-parses it — but
   rules 2 and 7 deliberately *drop* clauses from the stored text, so re-parsing `_source` yields the
   widened parse, not the original (the dropped negations/groups would silently vanish from the
   precision stage too — over-accepting). A consumer that keeps its own precision stage must therefore
   resolve the **original** expression by id from its own source of truth, or use the opaque
   original-expression passthrough once [ADR-064](../DECISIONS.md) item 7 lands. The round-trip
   property (rendered text re-parses to the original) holds — and is property-testable — for every
   clause the translation *preserves*.
6. A repeated-term occurrence-count requirement (a reference-matcher-only feature) survives by
   rendering the repeated terms verbatim — RR dedups them at compile time (set semantics, no widening)
   while the reference re-parse keeps the counts for its precision stage.
7. A positive group containing a member that normalizes to **zero features** (non-Latin scripts,
   symbol-only members) drops the *whole group clause* (vacuous = wider = FN-safe).
8. **Exclude-only queries** (no positive clause after translation) are class-D-rejected — keep their ids
   in an **always-candidate side list**: their *positive* semantics is satisfied by every title, so every
   title takes them as **candidates** (never append them to final results directly — the precision stage
   applies the excludes) — until the ADR-064 item-2 lane lands.

**Known residual FP classes** (all predicted, all re-filtered by a precision stage): phrase-as-bag
adjacency loss, dropped negations, decimal splits under `.`→split, occurrence counts unenforced at
stage one (deduped at compile; preserved in the text per rule 6), and the always-candidate side list. **Known residual FN class:** exactly one — the `pop` number-context
position-sensitivity (ADR-064 item 3) — until its parity knob lands.

> **Validation still owed.** The PoC above verifies the *translation contract* on adversarial pinned
> pairs; it is not the full-corpus audit. Running RR against this workload's **real corpus at scale** (a
> false-negative / false-positive / throughput pass over messy real titles — e.g. a shadow-read
> comparison during a dual-write migration window) remains the open, highest-leverage validation step
> flagged in [`../STATUS.md`](../STATUS.md) "Current limitations" — and is Distributed-v1 criterion 12
> ([ADR-065](../DECISIONS.md)).
