# Normalization — DSL, shared normalizer, feature dictionary

*Scope: how stored query text and incoming document text become dense integer feature IDs. Siblings:
[`matching.md`](matching.md), [`ingestion-and-updates.md`](ingestion-and-updates.md), and
[`clustering-and-scaling.md`](clustering-and-scaling.md). See the
[overview](README.md) for the correctness contract.*

> **Implementation status:** Fully implemented and tested.

**TL;DR (for agents)**

- **Owns:** DSL parsing (`dsl.rs`), shared normalization (`normalize.rs`), feature dictionary
  (`dict.rs`), and runtime vocabulary (`vocab.rs`).
- **Key invariant:** queries and documents must use the same normalizer and vocabulary.
- **Default behavior:** generic tokens plus four-digit year recognition. There is no built-in
  product taxonomy, named-entity list, category policy, or domain composite.
- **Configured behavior:** phrases, synonyms, equivalences, aliases, punctuation rules, and numeric
  context arrive through `NormalizerBuilder`, `Vocab`, or the vocabulary REST APIs.
- **Quoted clauses:** zero-slop contiguous paths through analyzed token graphs (ADR-120).

---

## 1. Query DSL

The DSL is deliberately constrained so every query can compile to an integer predicate and the
compiler can identify queries with no selective positive gate.

```text
Grammar (EBNF-ish):
  query        := clause+
  clause       := positive | negative
  positive     := term | phrase | anyof
  negative     := '-' term | '-' phrase | '-' anyof
  anyof        := '(' member (',' member)* ')'
  member       := term+
  phrase       := '"' term+ '"'
  term         := word | normalized-entity-literal

Semantics:
  bare term / phrase            → MUST
  ( a b , c )                   → MUST ((a AND b) OR c)
  -term                         → MUST_NOT
  -( a b , c )                  → MUST_NOT ((a AND b) OR c)
```

The compiler jointly normalizes each maximal consecutive run of positive bare terms. This lets a
configured phrase such as `wireless mouse` become one entity without joining across another clause:
`wireless -used mouse`, `wireless "compact" mouse`, and `wireless (black,white) mouse` remain
separate runs (ADR-118).

An unquoted multi-token any-of member is one conjunctive branch, not a bag of interchangeable
features. `(red shoe,boot)` means `(red AND shoe) OR boot`. Candidate retrieval may use one necessary
feature as a proxy for a branch, but exact verification preserves the complete branch (ADR-119).

A quoted clause retains the analyzer's position graph. Each edge is
`(start, end, FeatureId alternatives)`. A normal token spans one position; a configured collapsed
phrase can span several. `"red shoe"` therefore rejects both `red leather shoe` and `shoe red`.
Required phrase edges may be widened by active equivalences. Forbidden phrases use the canonical
leftmost-longest title view and are never widened (ADR-120).

Worked example with caller-supplied vocabulary:

```text
2024 (north star,ns) wireless mouse (package,pkg)
-(used,damaged) -refurbished
```

Given:

```text
phrase:  north star      → brand:north_star
phrase:  wireless mouse  → entity:wireless_mouse
synonym: ns              → brand:north_star
synonym: pkg             → term:package
```

the query compiles to:

```text
REQUIRED:   year:2024, brand:north_star, entity:wireless_mouse, term:package
FORBIDDEN:  term:used, term:damaged, term:refurbished
```

Each any-of group collapses to one canonical feature and is therefore promoted to a required
feature by the compiler.

The AST is compile-time only; matching never interprets source strings.

---

## 2. Shared query and document normalizer

The same `Normalizer` processes stored queries and incoming titles. The pipeline uses caller-owned
scratch buffers:

1. **Byte cleaning.** ASCII is lowercased, supported diacritics fold to ASCII, and the punctuation
   table classifies each character as `split`, `fold`, `keep`, or `marker`. By default `.` is kept,
   `#` and `/` are marker tokens, and other non-alphanumeric characters split words. Operators may,
   for example, fold apostrophes and hyphens so `O'Brien`, `O-Brien`, and `OBrien` converge.
2. **Tokenization.** Cleaned text becomes spans into the reusable buffer, not owned strings.
3. **Phrase and alias scan.** A daachorse Aho-Corasick automaton emits configured multi-token
   features. Collapse, additive, and alias modes control whether component tokens remain visible.
4. **Number typing.** Four-digit values in `1900..=2099` emit `year:N`. Other numbers remain generic.
   A caller-supplied `number_context` word makes an immediately following number generic too:
   with `["model"]`, `model 1995` emits `term:1995`, while `series 1995` emits `year:1995`.
   The default context list is empty.
5. **Synonyms and fallback.** A configured single-token synonym emits its canonical feature.
   Everything else emits `term:<normalized-token>`.
6. **Dense IDs.** Feature names resolve to `FeatureId(u32)`. Strings do not enter candidate
   retrieval or exact verification.

There is intentionally no inference for product categories, brands, model names, conditions,
or other business concepts. A caller can submit those semantics as phrases and synonyms,
declare equivalences, import an alias file, learn repeated any-of relationships, or opt into corpus
phrase induction. The same vocabulary is then applied to both query compilation and title analysis.

`match_features_dual` writes canonical `N(T)` and positive-superset `P(T)` views. `P(T)` includes
overlapping alias paths so a positive requirement is not hidden by a longer leftmost-longest phrase.
`N(T)` remains canonical so forbidden predicates are not accidentally widened. When quoted
predicates exist, `match_phrase_views` also writes reusable position-arc buffers.

---

## 3. Feature dictionary

- One `Dict` belongs to an engine. A cluster shares one frozen dictionary across shards so a
  `FeatureId` has one meaning everywhere.
- Interned IDs are dense in first-seen order below the reserved synthetic-ID range. Parallel arrays
  hold names, kinds, frequencies, and top-64 mask positions.
- Query-document frequency drives anchor selection independently of ID order. Finalization freezes
  the 64 highest-frequency features used by the exact verifier's common mask.
- Read-only paths resolve an absent name to a deterministic synthetic ID. A collision may
  over-retrieve, but cannot remove a true candidate.
- `FeatureKind` is descriptive vocabulary metadata: `year`, `brand`, `entity`, `category`, `flag`,
  or `generic`. Candidate choice is frequency-based; it does not contain category-specific rules.
- Active equivalences widen a positive requirement to an any-of group. Expansion can add matches,
  but cannot remove an existing match (ADR-054).

Multi-word aliases are asymmetric by design (ADR-061). On the query side they collapse to an entity
that equivalence expansion can widen. On the title side they are additive, and an overlapping scan
adds nested alias entities to `P(T)`. With no active multi-word alias, the flat title paths are
identical.

---

## 4. Vocabulary sources and lifecycle

`Normalizer::default_vocab()` is domain-neutral and empty apart from generic normalization rules.
Semantics can be supplied through:

- `NormalizerBuilder` for embedded callers;
- a serialized `Vocab` loaded at startup;
- `PUT /_vocab` for explicit runtime replacement;
- `POST /_vocab/aliases/import` for operator-declared surface forms;
- any-of learning from query text;
- opt-in NPMI phrase induction; and
- review-first distributional alias discovery.

The review-first REST learner requires one explicit caller corpus in both local modes. It rejects
duplicate IDs and invalid DSL before counting cross-query evidence, bounds corpus cardinality,
relationship expansion, phrase tokens and growth passes, body/result size, and body time, and runs
validation, learning, and serialization on the shared administrative blocking slot. It does not
inspect stored queries or apply its result.

The REST replacement, alias-import, and stored-corpus learn-and-apply paths perform any required
O(corpus) rebuild through the same one-slot blocking-work boundary and recompile stored queries
before publishing the new normalizer. Alias imports parse atomically, bound rules and forms, and
skip installation entirely when an identical registry declaration is retried. The mutating learner
accepts only bounded, bodyless, validated query controls; both mutations return the same timed
`recompiled` result in standalone and coordinator modes. A successful durable response means the
rebuilt query state committed; a coherent live rebuild whose storage commit fails is published but
explicitly not acknowledged. Retrying the identical coordinator import in that state recommits the
live vocabulary generation before it returns a no-op acknowledgement.
Alias-registry review shares that administrative slot for potentially large JSON snapshots. A
standalone read captures one immutable engine snapshot; a coordinator read clones the registry
under a brief cluster guard inside the blocking worker and releases the guard before paging and
serialization. Optional `from`/`size` controls page stored order without changing the total
registry `count` or whole-registry lifecycle summary.
Embedded callers use the deliberately split `set_vocab()` then `recompile_stale_segments()` sequence
and must not publish a snapshot between those calls. Single-node durable deployments must persist
the same vocabulary file used on reopen; clusters checkpoint the vocabulary in coordinator state. See
[`../reference/api/vocab.md`](../reference/api/vocab.md).

Compiler semantics version 5 removed earlier special-purpose feature categories. This project has
no compatibility requirement for prototype data: persisted query state written with an earlier
compiler semantics version must be rebuilt from source rather than upgraded in place.
