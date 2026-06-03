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
  "grade_words": ["gem", "mint", "pristine"]
}
```

The `NormalizerBuilder` API remains available for programmatic vocabulary construction when you need
fine-grained control.
