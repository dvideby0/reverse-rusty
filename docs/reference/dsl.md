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
| `(a,b,c)` | Any-of group (OR — at least one complete member must match) | `(red,blue,green)` |
| `-word` | Must not contain (NOT) | `-refurbished` |
| `-"a b"` | Must not contain phrase (NOT) | `-"for parts"` |
| `-(a,b,c)` | Must not contain any complete member (NOT + OR) | `-(used,open box,returned)` |

## Combining operators

Every top-level element is required (AND logic). Use groups for OR within that structure, and prefix
with `-` for exclusion.

Negation applies to the complete analyzed clause. A negated any-of member with multiple terms is
rejected only when the complete member matches; seeing one component is not enough.

Consecutive positive bare terms are normalized together only within one uninterrupted run, so a
configured multi-word entity can be recognized (`new york`). Every phrase, any-of group, or negated
clause is a boundary. For example, `new -used york` means required `new` AND required `york` AND NOT
`used`; it never manufactures a contiguous `new york` entity across the negation (ADR-118).

An unquoted any-of member may contain multiple tokens. Tokens are ANDed **within** that member, while
members are ORed **across** the group (ADR-119):

```
(red shoe,boot) marker
    = ((red AND shoe) OR boot) AND marker

marker -(red shoe,boot)
    = marker AND NOT ((red AND shoe) OR boot)
```

| Title | Positive query | Negated query |
|---|---:|---:|
| `red shoe marker` | match | reject |
| `boot marker` | match | reject |
| `red hat marker` | reject | match |
| `shoe marker` | reject | match |

The compiler may choose one required feature from each member as a candidate-retrieval proxy, but
that proxy never replaces the member's full exact predicate. Quoted-phrase adjacency is a separate
language rule and is not implied by this unquoted-member contract.

### Quoted phrases

A quoted clause is an **analyzed, ordered, contiguous path** (ADR-120). It uses the same normalizer as
titles and has zero slop: every analyzed edge must connect directly to the next one, although a
configured synonym or multi-word alias may represent one alternate analyzer path. Runs of
whitespace still delimit the same token positions, so `"north  star"` is equivalent to
`"north star"`.

| Query | Title | Result |
|---|---|---|
| `"red shoe"` | `red shoe` | match |
| `"red shoe"` | `red-shoe` | match with the default `Split` punctuation |
| `"red shoe"` | `red leather shoe` | no match |
| `"red shoe"` | `shoe red` | no match |
| `item -"for parts"` | `item for parts` | reject |
| `item -"for parts"` | `item for spare parts` | match |

Adjacency is over normalized positions, not raw bytes. Case/diacritic folding, number typing, and the
configured punctuation table therefore apply before the phrase check. For example, declaring `-` as
`Fold` turns `red-shoe` into the single token `redshoe`; it no longer has the two-position path
`red → shoe`. A declared `ny ↔ new york` alias lets `"new york" inventory` match `ny inventory`
without allowing `new vintage york inventory`.

There is currently no slop parameter or transposition syntax. DSL quotes are also distinct from a
vocabulary `phrases` entry: quotes constrain a stored query to adjacency, while vocabulary phrases
define analyzer entity edges used by both quoted and unquoted clauses.

Required quoted phrases remain in the standard/default-visible query scope. Their analyzer labels
are candidate hints only; individually common labels do not move a phrase into the opt-in broad
scope.

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
> verification. This is a core correctness invariant (see [`../../AGENTS.md`](../../AGENTS.md) and
> [`../design/README.md`](../design/README.md) §2); it's why an absent forbidden feature can never
> drop a real match.

## Normalization

Both queries and titles pass through the **same** normalization pipeline before matching — that
shared pipeline is what makes synonyms and aliases work automatically:

- **Case folding and diacritic removal** — `Café` becomes `cafe`.
- **Generic number handling** — four-digit values from 1900 through 2099 are years; other values are
  ordinary terms. A caller-supplied `number_context` list can keep a following value generic.
- **No built-in product semantics** — named phrases, synonyms, aliases, categories, brands, and
  entities come entirely from vocabulary configuration. `number_context` is empty by default.

Because the same normalizer processes both sides, a query containing `sneakers` will match a title
containing `running shoes` if those are configured as equivalent in the vocabulary. The normalizer
hardening derived from marketplace title shapes is documented
in [`../research/real-data-findings.md`](../research/real-data-findings.md) and
[`../design/normalization.md`](../design/normalization.md) §2.

## Vocabulary

The engine's domain knowledge is managed through a **vocabulary** — a JSON-serializable collection
of phrases, synonyms, equivalences, aliases, punctuation rules, and numeric context. Vocabulary can
come from three sources:

1. **Learned from queries** — the engine scans any-of groups in your query corpus to discover synonym
   relationships. If many queries contain `(package,pkg)`, the engine can learn the relationship
   (ADR-015). Use [`POST /_vocab/learn`](api/vocab/learn-vocab.md)
   to preview learned vocabulary.

2. **Manual configuration** — add phrases, synonyms, equivalences, aliases, punctuation, and numeric
   context through `Vocab` or
   [`PUT /_vocab`](api/vocab/replace-vocab.md).

3. **File-based** — load a vocabulary JSON file at startup with `--vocab-file`, or save/load at
   runtime. Vocabularies are composable via `merge()`.

```json
{
  "synonyms": [
    {"token": "pkg", "canonical": "term:package", "kind": "generic"},
    {"token": "ns", "canonical": "brand:north_star", "kind": "brand"}
  ],
  "phrases": [
    {"tokens": ["north", "star"], "canonical": "brand:north_star", "kind": "brand"},
    {"tokens": ["wireless", "mouse"], "canonical": "entity:wireless_mouse", "kind": "entity"}
  ],
  "equivalences": [["ns", "north star"]],
  "punctuation": [
    {"ch": "'", "class": "fold"},
    {"ch": "-", "class": "fold"}
  ],
  "number_context": ["model"]
}
```

The optional `punctuation` array (ADR-058) reclassifies how individual characters are handled in
byte-cleaning, so punctuation-only spelling differences stop dropping candidates:

- `"fold"` — delete the character so its neighbors **join** into one token (`O'Brien`, `O-Brien`, and
  `OBrien` all become `obrien`). Declare a corpus's mid-word `'` (and the curly apostrophe `’`) and `-`
  here.
- `"split"` — make the character a word boundary.
- `"keep"` — leave it literally in place inside the token (`9.5` stays `9.5`).
- `"marker"` — emit it as its own standalone token.

By default `.` is `keep`, `#`/`/` are `marker`, and every other non-alphanumeric character is `split`;
omit the array to use that behavior. The same table applies
to **both** queries and titles, so a query and a title that differ only in punctuation match.

The `NormalizerBuilder` API remains available for programmatic vocabulary construction when you need
fine-grained control (`fold_punctuation` / `set_punct_class`). In a single-node server, a REST
vocabulary change remains process-local metadata until the resulting `GET /_vocab` document is saved
to the configured `--vocab-file`; a durable cluster checkpoints its vocabulary in the coordinator
manifest.
