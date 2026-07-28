# Marketplace title findings

This note records the general title-shape risks that informed the normalizer and the recommended
ingestion boundary. It deliberately avoids product-category assumptions: the engine must behave the
same way whether the corpus contains electronics, apparel, parts, books, or another catalog.

The historical exploratory study sampled about 20 representative result titles from a marketplace
search and ran them through the actual feature-printing normalizer. Roughly 30–40% of observed tokens
were promotional, decorative, or otherwise absent from the query corpus. Source and category labels
were sanitized during the domain-neutral reset, but these measured quantities are preserved. This
was a small diagnostic sample, not a production-quality estimate.

## What free-text titles reliably provide

Marketplace titles commonly contain:

- brand and model surface forms;
- compact abbreviations;
- a year or model number;
- condition and variant words;
- seller-added punctuation and emoji;
- repeated promotional language; and
- dimensions, quantities, serial-like values, and compatibility identifiers.

The shared normalizer already handles the reusable mechanics: case folding, selected diacritic
folding, configurable punctuation, generic year recognition, caller-declared numeric context,
phrases, synonyms, aliases, and exact quoted adjacency.

What it intentionally does **not** do is guess business meaning from a number or keyword. `10`,
`pro`, `limited`, and `series` are generic tokens unless the caller supplies vocabulary that gives
them a canonical meaning. This avoids a category-specific interpretation silently becoming part of
the engine.

## Recurring title hazards

| Title shape | Risk | Generic treatment |
|---|---|---|
| `Café`, `Jalapeño` | surface forms split or lose letters | supported diacritics fold to ASCII |
| `O'Brien`, `O-Brien`, `OBrien` | punctuation-only spelling drift | optionally classify apostrophe and hyphen as `fold` |
| `#866`, `3/10`, `/5` | marker and number ambiguity | markers are separate tokens; numbers remain generic unless they are four-digit years |
| `model 1995` | a model number looks like a year | declare `model` in `number_context` |
| `North-Star`, `North Star`, `NS` | one brand has several forms | declare a phrase, synonym, or equivalence |
| `wireless mouse` inside `wireless mouse bundle` | a longer phrase can hide a nested positive form | positive title analysis emits overlapping alias paths |
| seller promotion and accessory language | lexical overlap without product identity | use structured metadata and exact Boolean requirements |

These rules are symmetric: every punctuation, phrase, synonym, equivalence, and numeric-context
decision must be used for both query compilation and title analysis.

## Structured fields should remain structured

Free text is incomplete and ambiguous. If the source feed provides category, brand, model,
condition, dimensions, identifiers, seller, or other item-specific fields, preserve them in the
source system rather than pretending the title reliably contains them.

Reverse Rusty's current percolation document is strict and title-only. Stored-query metadata tags
can filter confirmed matches, but they are not incoming-document features and cannot satisfy a
positive query clause. Direct typed/aspects feature ingestion remains
[roadmap work](../roadmap.md#aspects-first-ingestion). If structured values must participate today,
the caller must deliberately compose them into the submitted text and author queries against that
same convention; the engine has no separate structured-field channel.

The intended future boundary is:

```text
source record
  ├─ title text ───────── shared generic normalizer ───────┐
  └─ structured fields ─ typed/aspects ingestion ─────────┤
                                                          ▼
                                            integer feature document
                                                          │
                                     candidate retrieval + exact verify
```

Metadata filters run after Boolean matching and therefore must not be used to weaken candidate
cover.

## Vocabulary should be supplied or learned

The entity and alias long tail is unbounded. Reverse Rusty therefore starts without a product
dictionary and offers several generic ways to build one:

1. submit reviewed phrases, synonyms, and equivalences through `Vocab` or `PUT /_vocab`;
2. import operator-managed aliases;
3. learn repeated any-of relationships already present in stored queries;
4. opt into NPMI phrase discovery for recurring multi-token entities; and
5. run distributional discovery to produce review candidates.

The matcher does not need category-specific code to reach the desired result. It needs a consistent
feature model, a representative vocabulary, and query patterns selective enough to avoid the broad
lanes. Quality should be evaluated with labeled query/title pairs from the target workload; title
shape alone cannot establish whether a match is commercially correct.
