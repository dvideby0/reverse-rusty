# Corpus-driven feature learning

*Can the feature vocabulary be built from supplied queries instead of embedded product knowledge?
Largely yes. Phrase induction and any-of learning are implemented; alias activation remains
review-governed because equivalence changes match semantics.*

## 1. Why the matching core does not need a taxonomy

Candidate selection uses observed query-document frequency, not `FeatureKind`. The compiler asks
which positive requirement or required any-of branch gives the cheapest lossless cover. It does not
need to know whether that requirement represents a brand, model, entity, category, or ordinary word.

Vocabulary improves two independent things:

1. **Phrase gluing** turns a recurring multi-token expression such as `wireless mouse` into one
   feature, often producing a shorter candidate posting.
2. **Surface-form relationships** let `north star`, `northstar`, and `ns` express one operator-approved
   concept.

The first can be proposed from corpus statistics. The second needs governance because an apparent
substitute may instead be a related but distinct category.

## 2. Implemented learning sources

### Any-of relationships

Repeated query groups such as `(package,pkg)` provide evidence that two forms are alternatives.
The free function `reverse_rusty::vocab::learn_from_queries` can emit collapse synonyms, while
`reverse_rusty::vocab::learn_equivalences_from_queries` can emit widening equivalence groups.
`min_count` bounds one-off noise.

An any-of group is still only a disjunction, not proof of identity. Clear single-token spelling or
abbreviation variants may auto-activate under the alias policy; distinct words and multi-word forms
remain review candidates.

### NPMI phrase induction

The corpus learner counts unigrams and adjacent n-grams, then proposes phrases whose normalized
pointwise mutual information and count exceed configured thresholds:

```text
NPMI(a,b) = ln(P(ab) / (P(a)·P(b))) / -ln(P(ab))
```

It can iterate from bigrams to longer expressions by rewriting the corpus with accepted phrases.
The runtime surface is `CorpusLearnConfig` and
`POST /_vocab/learn[/_and_apply]?corpus_phrases=true`.

Learned phrases are additive: a match emits the phrase feature and keeps its component features.
That protects queries which require a component. A query written in the learned phrase form still
adopts adjacency semantics, so phrase induction is opt-in, reviewable, and evaluated against a
labeled corpus rather than assumed universally correct.

### Distributional alias proposals

Tokens with similar query contexts can be proposed as aliases. This is a noisy signal: substitutes
and co-categories often have similar neighbors. Distributional discovery therefore never activates
matching by itself. It records ranked candidates for review (ADR-102).

### Match-feedback evidence

For a tracked two-form candidate, the feedback loop compares sampled query-match sets for titles
containing either form. Strong overlap is useful evidence but still does not mutate semantics unless
an operator explicitly activates the candidate (ADR-103).

## 3. Correctness boundaries

- Token cleaning, punctuation, and numeric context are safe only when the same configuration runs on
  both queries and titles.
- Additive phrase features can widen candidate retrieval; exact verification remains authoritative.
- Equivalence expansion widens positive requirements and cannot remove an existing match, but a bad
  equivalence can add false-positive results. It is therefore governed and reversible.
- Destructive one-sided canonicalization is forbidden because it can create false negatives.
- Query frequency chooses anchors but never changes Boolean truth.

These boundaries are why the engine exposes vocabulary as data and recompiles stored queries when it
changes.

## 4. Operational workflow

1. Ingest representative stored queries.
2. Preview any-of and NPMI results with `POST /_vocab/learn`.
3. Run alias discovery for additional review candidates.
4. Validate proposals against labeled query/title pairs and, optionally, match-feedback evidence.
5. Install reviewed vocabulary with `PUT /_vocab` or `learn_and_apply`.
6. Persist the resulting `GET /_vocab` document for single-node reopen; clusters checkpoint it.
7. Re-run the independent oracle and workload benchmarks after every semantic vocabulary change.

The engine can therefore reach selective, category-aware matching without category-specific code.
The caller supplies structured knowledge directly or lets the generic learners propose it through
the same vocabulary API.

Run the standalone learner:

```bash
cargo run --release --bin learn -- 500000 50 0.30
```

No checked-in learner capture is treated as a production baseline; phrase quality depends on the
deployment corpus.
