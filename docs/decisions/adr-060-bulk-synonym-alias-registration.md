# ADR-060: Bulk synonym / alias registration (Solr-format file loader)

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted

- **Context.** The vocabulary surface registered aliases **one at a time** — `Vocab::add_synonym` /
  `add_equivalence`, `NormalizerBuilder::synonym`, or a hand-authored `Vocab` JSON via `PUT /_vocab`.
  Real percolator deployments maintain *hundreds* of equivalences (abbreviation→canonical, variant
  spellings, term expansions like `auto ≡ {autograph, autographed, signature, signed}`) and keep them
  in a file **outside of code**, edited by domain operators. Hand-writing the `Vocab` JSON (nested
  objects, `term:`-prefixed canonicals, explicit `kind`s) for that is error-prone and not how the rest
  of the search world does it. This was the last open [Tier 4](../roadmap.md) percolator-parity item.
- **Decision.** Parse the **Solr/Lucene synonym-file format** — the de-facto interchange format that
  Elasticsearch/OpenSearch's `synonyms_path` already consumes — and map it onto RR's existing FN-safe
  equivalence mechanism.
  1. **Format (two line shapes, plus `#` comments / blank lines).** An *equivalent set*
     `auto, autograph, signature` (comma-separated, no arrow) declares every form interchangeable; a
     *mapping* `ud, upperdeck => upper deck` (with `=>`) is accepted for Solr-file compatibility.
  2. **Everything becomes an FN-safe equivalence group (the load-bearing choice).** Each rule is applied
     through [`Vocab::add_equivalence`](adr-054-equivalence-expansion.md) — **expansion, not collapse**:
     a query requiring one form is widened to an any-of over the whole group, so it matches a title
     bearing any form. The `=>` arrow's two sides are simply **unioned into one group** (RR is
     expansion-based, so direction is immaterial to recall). We deliberately do **not** implement Solr's
     directional token-*collapse*: collapsing interacts badly with forbidden terms (a collapsed `c -a`
     becomes the contradiction `term:c -term:c`, silently killing a query) and expansion is strictly
     recall-safe — a wrong alias can only add a (cheap) false-positive candidate, never drop a match.
  3. **Multi-token forms are glued as alias entities ([ADR-061](adr-061-alias-entity-phrases.md)).** A
     form like `upper deck` / `i-pod` can't be a single feature on its own, so it is registered as an
     **alias-entity phrase** to a `term:`-prefixed entity (`["upper","deck"] -> "term:upperdeck"`) and the
     **raw multi-word form** joins the equivalence group. The alias-entity is the ES `synonym_graph`
     equivalent — additive on the title side (entity + components, so a component query like `deck` still
     matches) but collapse on the query side (a query phrased `upper deck` requires just the entity, which
     equivalence expansion widens to its synonyms) — so the alias is **bidirectional**, not one-way.
     `resolve_equivalences` runs the query path, collapsing the raw member to the single entity feature.
     Form tokenization mirrors the default normalizer (`.` kept inside a token, so `st.` stays `st.`).
  4. **Surfaces.** Lean-core library: `vocab::parse_synonyms(text) -> Result<Vocab, SynonymParseError>`,
     `Vocab::extend_from_synonyms[_file]`, and bulk `Vocab::add_equivalences` / `add_synonyms` /
     `NormalizerBuilder::add_synonyms`. REST: **`POST /_vocab/synonyms`** takes the raw table as the
     request body, merges it into the live vocab, and recompiles every stored query (the ADR-046
     `set_vocab` + `recompile_stale_segments` path) so it takes effect immediately with zero false
     negatives. Malformed input is rejected **fail-loud with the 1-based line number** — no silent skip.
- **Why this is safe (no false negative + byte-identical default).** The loader only *adds* equivalence
  groups and gluing phrases, then applies them through the already-oracle-proven ADR-046/054 expansion
  path. Expansion only grows a query's match set (structurally FN-safe — proven independently of any
  test by ADR-054), and the same normalizer runs over queries and titles, so the lossless-cover contract
  ([`design/README.md`](../design/README.md) §2) is untouched. An empty/comment-only table is a no-op.
- **Scope.** Single-node + cluster share the `Vocab`/`set_vocab` path, so the library loader works in
  both; the `POST /_vocab/synonyms` endpoint runs against the single-node server. Cross-process shipping
  of a learned/loaded normalizer to a remote shard remains the same deferral as cross-process vocab
  shipping (the distributed-layer residue, [ADR-046](adr-046-dynamic-vocabulary.md)). Escaped commas
  (`\,`) and Solr's `\u` escapes are not parsed (commas separate forms); inline comments are not parsed
  (a `#` must start the line).
- **Alternatives declined.** *A new bespoke JSON/CSV synonym schema* — rejected: operators already have
  Solr-format files and tooling; inventing a format adds friction for no gain. *Implement true Solr
  directional collapse (`=>`)* — rejected for the forbidden-term footgun above; expansion is the
  recall-first model and RR's native mechanism. *Only a `NormalizerBuilder` bulk method (no file loader)*
  — rejected: the headline need is maintaining the table *outside of code*, which the in-code builder
  doesn't address (the bulk builder method is still added, as a convenience).
- **Consequence.** A domain team maintains a plain-text synonym file and `curl --data-binary
  @synonyms.txt …/_vocab/synonyms` applies it live, recall-safe, with a line-numbered error on a typo —
  closing the last Tier 4 percolator-parity item.
- **See also:** [ADR-054](adr-054-equivalence-expansion.md) (the FN-safe expansion mechanism this drives),
  [ADR-053](adr-053-corpus-phrase-vocab-source.md) (the multi-token adjacency residual),
  [ADR-046](adr-046-dynamic-vocabulary.md) (`set_vocab` + recompile — the live-apply path),
  [ADR-015](adr-015-runtime-vocabulary-learning.md) (the `Vocab` system). Design: [`reference/dsl.md`](../reference/dsl.md)
  §Vocabulary, [`reference/api/vocab.md`](../reference/api/vocab.md). Code: `src/vocab/synonyms.rs`
  (`parse_synonyms`, `SynonymParseError`/`SynonymLoadError`/`SynonymLoadStats`, the `Vocab` bulk +
  `extend_from_synonyms[_file]` methods), `src/normalize/builder.rs` (`add_synonyms`/`synonyms`),
  `src/bin/server/handlers/vocab.rs` (`load_synonyms`). Tests: `src/vocab/synonyms.rs` units,
  `tests/oracle/synonyms.rs` (runtime FN-safe recall-grows + engine ≡ equivalence-aware brute, zero
  FN/FP), `tests/oracle/harness.rs::build_with_vocab` (the equivalence-aware brute reference).
