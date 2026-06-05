# Query DSL & vocabulary reference

How to *write* queries and configure the vocabulary that drives matching. This is the user-facing
language reference; for the compile-time internals (parser → AST → normalizer → feature dictionary)
see [`../design/normalization.md`](../design/normalization.md). To register queries and manage
vocabulary over HTTP, see [`api.md`](api.md).

## Operators

Queries are written in a simple DSL that supports required terms, phrases, any-of groups, and
negations. **All top-level clauses are implicitly ANDed together.**

| Syntax | Meaning | Example |
|---|---|---|
| `word` | Required term (AND) | `laptop` |
| `"a b"` | Required phrase (AND) | `"running shoes"` |
| `(a,b,c)` | Any-of group (OR — at least one must match) | `(red,blue,green)` |
| `-word` | Must not contain (NOT) | `-refurbished` |
| `-"a b"` | Must not contain phrase (NOT) | `-"for parts"` |
| `-(a,b,c)` | Must not contain any of (NOT + OR) | `-(used,open box,returned)` |

## Combining operators

Every top-level element is required (AND logic). Use groups for OR within that structure, and prefix
with `-` for exclusion.

```
# All of these terms are required (AND):
vintage leather jacket

# At least one color required (OR), plus a required term:
(brown,tan,cognac) leather jacket

# Required terms with exclusions (AND + NOT):
vintage leather jacket -wallet -belt

# Full example using all operators:
vintage (leather,suede) "bomber jacket" (brown,tan,black) -womens -(replica,faux,vegan)
```

This last query matches titles that contain: `vintage`, either `leather` or `suede`, the phrase
`bomber jacket`, at least one of `brown`/`tan`/`black` — but rejects any title containing `womens`,
`replica`, `faux`, or `vegan`.

> Negations (`-`) are **never** used to retrieve candidates — they're checked only during exact
> verification. This is a core correctness invariant (see [`../../CLAUDE.md`](../../CLAUDE.md) and
> [`../design/README.md`](../design/README.md) §2); it's why an absent forbidden feature can never
> drop a real match.

## Normalization

Both queries and titles pass through the **same** normalization pipeline before matching — that
shared pipeline is what makes synonyms and aliases work automatically:

- **Case folding and diacritic removal** — `Café` becomes `cafe`, `Jokić` becomes `jokic`.
- **Number disambiguation** — years, quantities, model numbers, and other numeric types are
  classified separately based on context.
- **Domain-agnostic by default** — the normalizer ships with no hardcoded vocabulary. All domain
  knowledge (phrases, synonyms, graders) is supplied via vocabulary configuration.

Because the same normalizer processes both sides, a query containing `sneakers` will match a title
containing `running shoes` if those are configured as equivalent in the vocabulary. The normalizer
hardening derived from real eBay data (diacritics, card numbers, serials, populations) is documented
in [`../research/real-data-findings.md`](../research/real-data-findings.md) and
[`../design/normalization.md`](../design/normalization.md) §4.

## Vocabulary

The engine's domain knowledge is managed through a **vocabulary** — a JSON-serializable collection of
phrases, synonyms, grader keywords, and grade words. Vocabulary can come from three sources:

1. **Learned from queries** — the engine scans any-of groups in your query corpus to discover synonym
   relationships. If many queries contain `(rookie,rc)`, the engine learns that `rookie ≈ rc` and maps
   both to the same canonical feature (ADR-015). Use [`POST /_vocab/learn`](api/vocab.md#post-_vocablearn--learn-vocabulary-from-queries)
   to preview learned vocabulary.

2. **Manual configuration** — add phrases, synonyms, graders, and grade words through the `Vocab` API
   or the [`PUT /_vocab`](api/vocab.md#put-_vocab--replace-vocabulary) REST endpoint.

3. **File-based** — load a vocabulary JSON file at startup with `--vocab-file`, or save/load at
   runtime. Vocabularies are composable via `merge()`.

```json
{
  "synonyms": [
    {"token": "rc", "canonical": "term:rookie", "kind": "category"},
    {"token": "ud", "canonical": "term:upper_deck", "kind": "generic"}
  ],
  "phrases": [
    {"tokens": ["upper", "deck"], "canonical": "term:upper_deck", "kind": "generic"}
  ],
  "graders": ["psa", "bgs", "sgc"],
  "grade_words": ["gem", "mint", "pristine"],
  "punctuation": [
    {"ch": "'", "class": "fold"},
    {"ch": "-", "class": "fold"}
  ]
}
```

The optional `punctuation` array (ADR-058) reclassifies how individual characters are handled in
byte-cleaning, so punctuation-only spelling differences stop dropping candidates:

- `"fold"` — delete the character so its neighbors **join** into one token (`O'Brien`, `O-Brien`, and
  `OBrien` all become `obrien`). Declare a corpus's mid-word `'` (and the curly apostrophe `'`) and `-`
  here.
- `"split"` — make the character a word boundary.
- `"keep"` — leave it literally in place inside the token (`9.5` stays `9.5`).
- `"marker"` — emit it as its own standalone token.

By default `.` is `keep`, `#`/`/` are `marker`, and every other non-alphanumeric character is `split`;
omit the array (as older vocab files do) to get exactly that historical behavior. The same table applies
to **both** queries and titles, so a query and a title that differ only in punctuation match.

The `NormalizerBuilder` API remains available for programmatic vocabulary construction when you need
fine-grained control (`fold_punctuation` / `set_punct_class`).

### Bulk alias/synonym files (Solr format)

Real deployments maintain large alias tables — abbreviation→canonical, variant spellings, term
expansions like `auto ≡ {autograph, autographed, signature, signed}` — in a plain-text file edited
outside of code. RR loads the **Solr/Lucene synonym-file format** directly (the same format ES/OS's
`synonyms_path` consumes, ADR-060). Two line shapes, plus `#` comments and blank lines:

```text
# equivalent set (comma-separated, no arrow): every form is interchangeable
auto, autograph, autographed, signature, signed
rc, rookie, rookie card

# mapping (=>): accepted for Solr-file compatibility — both sides become one equivalent set
ud, upperdeck => upper deck
```

Every rule is applied as an **equivalence group via FN-safe expansion** (ADR-054, see above): a query
requiring one form is widened to an any-of over the group, so it matches a title bearing any form, and a
wrong alias can only add bounded false positives — never drop a match (recall-first). The `=>` arrow's
sides are unioned into one group (RR is expansion-based; direction is immaterial to recall) — RR does
**not** perform Solr's directional token-collapse. A multi-token form (`upper deck`) is glued to a single
feature as a phrase. Load a table over HTTP with [`POST /_vocab/synonyms`](api/vocab.md#post-_vocabsynonyms--load-a-solr-format-synonymalias-table)
(raw text body — merged + recompiled live, with a line-numbered error on a malformed table), or from the
library via `Vocab::extend_from_synonyms` / `extend_from_synonyms_file` (plus bulk
`Vocab::add_equivalences` / `add_synonyms` and `NormalizerBuilder::add_synonyms`).
