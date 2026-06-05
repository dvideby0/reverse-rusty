# ADR-061: Alias-entity phrases — the Elasticsearch `synonym_graph` equivalent for multi-word aliases

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted

- **Context.** [ADR-060](adr-060-bulk-synonym-alias-registration.md) shipped the Solr-format synonym
  loader, mapping rules onto FN-safe equivalence expansion (ADR-054). Single-token aliases
  (`auto ≡ {autograph, signature}`) are provably perfect — fully bidirectional, zero-drop. **Multi-word**
  aliases (`ud ≡ upper deck`, `nyc ≡ new york`) are the hard case: two rounds of local Codex review showed
  that gluing a multi-word form with a *collapse* phrase loses component-token matches (a `deck` query
  stops matching an `upper deck` title — a false negative), while gluing with a plain *additive* phrase to
  a single-token canonical is **one-way** (a query *phrased* `upper deck` keeps `upper`+`deck` required, so
  it never matches a `ny`-style sibling title). This is the classic multi-word-synonym problem. We checked
  how Elasticsearch solves it: the **`synonym_graph`** token filter builds a *token graph* (multi-word
  synonyms get a `positionLength` spanning their tokens), applied **at search time only** — "indexing
  ignores `positionLength`" so the **index keeps the component tokens**, while the **query** is expanded
  into a graph that becomes *"the phrase `upper deck` OR the term `ud`"*. The result is an **asymmetry**:
  the document keeps components; the query collapses the multi-word form into an entity alternative.
- **Decision.** Introduce an **alias-entity phrase** — the RR equivalent of an ES `synonym_graph` entry —
  with exactly that asymmetry, plus a tiny resolver tweak:
  1. **Asymmetric emission.** A phrase carries an `alias` flag. The shared normalizer `emit` takes a
     `query_side: bool`; an alias phrase **consumes its component tokens only on the query/compile side**
     (collapse → emit just the entity feature) and **keeps them on the title/match side** (additive →
     entity feature + components). Collapse and additive phrases are unchanged
     (`consume = if alias { query_side } else { !additive }`). The three entry points pass the flag:
     `compile_features` / `compile_features_readonly` → `true` (query), `match_features` → `false` (title).
  2. **The loader uses alias phrases.** A multi-word form in a synonym table is registered as an alias
     phrase to a `term:`-prefixed entity (`["upper","deck"] -> "term:upperdeck"`); the equivalence **member
     is the raw multi-word form**. `resolve_equivalences` runs the query/compile path, so the alias phrase
     collapses the member to the single entity feature — and equivalence expansion (ADR-054) then widens
     the query's entity feature to its synonyms. So a query phrased `upper deck` requires
     `any-of(term:upperdeck, term:ud)`, and a title bearing either `upper deck` (adjacent → emits the
     entity additively) or `ud` matches. **Bidirectional.**
  3. **Resolver robustness.** If a multi-word member resolves to >1 feature because an *existing additive*
     phrase (ADR-053) already covers those tokens, `resolve_equivalences` uses that phrase's entity
     (canonical) feature instead of silently dropping the link (`phrase_entity_in`).
- **Why this is safe (lossless cover holds; default byte-identical).** The asymmetry is strictly in the
  **safe direction**: the title side emits a **superset** (entity + components) of what the query side
  requires (just the entity, or just a component for a component query). So for any title that *could*
  satisfy a query, the title generates the feature the query's signature needs — the lossless-cover
  contract ([`design/README.md`](../design/README.md) §2) holds. A multi-word-form query becoming
  **phrasal** (it no longer matches non-adjacent components) is the *same* behavior ES has (the graph
  query is a phrase alternative), and it is a tightening (fewer matches than loose AND), never a
  spurious one. The default path is **byte-identical**: with no alias phrase, `consume = !additive`
  exactly as before, so every prior oracle is unchanged.
- **The "same normalizer" invariant — a narrow, safe-direction exception.** RR's invariant "same
  normalizer for queries and titles" exists to keep feature spaces aligned so a title can always retrieve
  a query it satisfies. The alias phrase is the **one** deliberate query/title asymmetry, and it is in the
  alignment-*preserving* direction (titles emit a superset). The invariant wording is updated to record
  this single exception; correctness is proven by the oracle, not assumed.
- **Scope.** Library + REST (single-node) and the in-process cluster share the `Vocab`/`set_vocab` path,
  so alias phrases work in both. A multi-word-form query is phrasal (ES-equivalent). Cross-process
  shipping of a learned normalizer to a remote shard stays the same deferral as cross-process vocab
  shipping (ADR-046).
- **Alternatives declined.** *Collapse phrase for multi-word* — loses component-token matches (a real FN);
  rejected (Codex round 1). *Additive phrase to a single-token canonical* — one-way + drops non-adjacent
  matches via the required canonical; rejected (Codex round 2). *Single-token only, error/skip multi-word*
  — correct but cuts a feature ES provides; rejected in favor of true equivalence. *A full token-graph
  query model* (Lucene's actual implementation) — far larger than RR's integer-feature matcher needs; the
  alias-entity asymmetry captures the same observable behavior for the percolation use case.
- **Consequence.** RR now provides genuinely ES-`synonym_graph`-equivalent multi-word aliases —
  bidirectional, component-preserving, adjacency-correct — loadable from a plain Solr synonym file, with
  the zero-false-negative contract intact and the default path byte-identical.
- **See also:** [ADR-060](adr-060-bulk-synonym-alias-registration.md) (the synonym-file loader this
  completes), [ADR-054](adr-054-equivalence-expansion.md) (the expansion mechanism alias entities feed),
  [ADR-053](adr-053-corpus-phrase-vocab-source.md) (additive phrases — the other phrase kind),
  [ADR-006](adr-006-forbidden-features-never-gate.md) (the never-gate invariant). ES reference:
  [`synonym_graph` token filter](https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-synonym-graph-tokenfilter.html),
  [token graphs](https://www.elastic.co/guide/en/elasticsearch/reference/current/token-graphs.html).
  Code: `src/normalize/core.rs` (`emit` `query_side`), `src/normalize.rs` (`PhraseEntry.alias`),
  `src/normalize/builder.rs` (`add_phrase_alias`), `src/vocab.rs` + `vocab/methods.rs`
  (`PhraseEntry.alias`, `add_phrase_alias`, `resolve_equivalences` + `phrase_entity_in`),
  `src/vocab/synonyms.rs` (the loader). Tests: `src/vocab/synonyms.rs` units,
  `tests/oracle/synonyms.rs` (bidirectional + phrasal-adjacency + engine ≡ equivalence-aware brute).
