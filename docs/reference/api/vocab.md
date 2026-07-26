# Vocabulary — REST API

> Part of the [REST API reference](../api.md). Query language: [`dsl.md`](../dsl.md).

## `GET /_vocab` — Current vocabulary

```bash
curl localhost:9200/_vocab
```

```json
{
  "synonyms": [
    {"token": "rc", "canonical": "term:rookie", "kind": "generic"}
  ],
  "phrases": [
    {"tokens": ["upper", "deck"], "canonical": "term:upper_deck", "kind": "generic"}
  ],
  "graders": ["psa"],
  "grade_words": ["gem"],
  "equivalences": [["ud", "upper deck"]],
  "punctuation": [{"ch": "'", "class": "fold"}, {"ch": "-", "class": "fold"}],
  "number_context": []
}
```

## `PUT /_vocab` — Replace vocabulary

Replace the engine's vocabulary. Existing stored queries are **automatically recompiled** under the
new normalizer — under the same lock, before the new snapshot is published — so the change takes
effect immediately with zero false negatives. `recompiled` reports how many queries were rebuilt.

> **Durability:** the recompiled queries persist (the recompile commits like a flush), but the
> vocabulary **object** itself lives in memory — single-node vocab persistence is the `--vocab-file`
> the server loads at startup (ADR-015). After changing the vocabulary over REST on a durable
> server, save the same JSON to your `--vocab-file` (e.g. capture `GET /_vocab`) so a restart
> reopens under the matching normalizer; restarting with a stale/absent vocab file desyncs title
> normalization from the persisted queries. (A cluster persists its vocab in the coordinator
> manifest and does not have this caveat.)

```bash
curl -X PUT localhost:9200/_vocab \
  -H 'Content-Type: application/json' \
  -d '{"synonyms": [{"token": "rc", "canonical": "term:rookie", "kind": "category"}], "phrases": [], "graders": [], "grade_words": []}'
```

```json
{
  "acknowledged": true,
  "recompiled": 1280
}
```

**Declaring equivalences (ADR-054).** The optional `equivalences` block is a list of groups of
surface forms treated as the same entity (e.g. `[["ud", "upper deck"], ["rc", "rookie"]]`). Unlike
`synonyms` (which *collapse* a form to a canonical via the normalizer), equivalences are applied by
**expansion**: a query requiring one form is widened to an any-of over the group, so it matches a
title bearing any form. Expansion only grows a query's match set, so it is **false-negative-safe** —
a wrong/uncertain equivalence can only add bounded false positives, never drop a true match. Each form
should resolve to a single entity (glue a multi-token form as a phrase first); a form that doesn't is
skipped. Applying the change recompiles existing queries through the expansion.

**Declaring punctuation rules (ADR-058).** The optional `punctuation` block reclassifies how individual
characters are handled in byte-cleaning. Each rule is `{"ch": "<char>", "class": "<fold|split|keep|marker>"}`:
`fold` deletes the character so its neighbors **join** into one token (so `O'Brien`, `O-Brien`, and
`OBrien` all become `obrien` — closing a recall gap for punctuation-only spelling differences), `split`
makes it a word boundary, `keep` leaves it literally in place, and `marker` emits it as its own token. The
default — `.` is `keep`, `#`/`/` are `marker`, everything else is `split` — is reproduced exactly when the
block is omitted (so older vocab payloads are unchanged). The same table applies to both queries and
titles, so the lossless-cover contract is preserved under any configuration.

**Number-context words (ADR-069).** The optional `number_context` array lists tokens that demote an
immediately-following number to a generic term (`pop 1995` → `term:1995`, never `year:1995`). When the
field is **omitted** the built-in default `["pop"]` applies — the historical population rule,
byte-identical for older payloads. An explicit **empty array disables the rule** — the
percolator-parity mode: number typing becomes position-insensitive, so a 4-digit year is `year:N` in
every position. A custom list substitutes other context words. Like every vocab change, applying it
recompiles stored queries under the new typing; the same list runs over queries and titles.

## `POST /_vocab/learn` — Learn vocabulary from queries

Send raw query text to discover synonym relationships from any-of groups. Returns the learned
vocabulary without applying it — review and then `PUT /_vocab` to use it.

```bash
curl -X POST localhost:9200/_vocab/learn \
  -H 'Content-Type: application/json' \
  -d '{
    "queries": [[1, "(rookie,rc) 2024"], [2, "(rookie,rc) 2023"]],
    "min_count": 2
  }'
```

```json
{
  "synonyms": [
    {"token": "rc", "canonical": "term:rookie", "kind": "generic"}
  ],
  "phrases": [],
  "graders": [],
  "grade_words": []
}
```

The `min_count` parameter (default: 2) controls how many times a synonym pair must appear across
different queries before it's included. Higher values reduce noise. See [`dsl.md`](../dsl.md#vocabulary)
for how vocabulary affects matching.

**Opt-in NPMI corpus phrase induction (ADR-053).** Add `"corpus_phrases": true` to ALSO induce
multi-token entity **phrases** (e.g. `upper deck` → `upper_deck`) from the supplied query text via NPMI
collocation mining, on top of the any-of synonyms. Phrases only — never aliases. They are applied
**additively** (a match emits the phrase feature AND keeps the component features), so a query
referencing a component never loses a candidate — important because this is a recall-first
candidate generator. A phrase-*form* query does tighten to requiring the adjacent phrase; for genuine
entities, which appear adjacent in real titles, that is negligible — but it is why this is opt-in and
reviewable. Explicit DSL quotes use the same analyzed adjacency contract; see
[`dsl.md#quoted-phrases`](../dsl.md#quoted-phrases) and ADR-120. Tunable:
`npmi_min_count` (min adjacent co-occurrence, default 3), `npmi_tau` (binding-strength threshold,
default 0.30), `npmi_iterations` (bigram→trigram passes, default 2). Absent ⇒ any-of learning only,
exactly as before. Add `"learn_equivalences": true` to instead learn the any-of groups as
**equivalence groups** applied via FN-safe expansion (ADR-054) rather than collapse synonyms.

```bash
curl -X POST localhost:9200/_vocab/learn \
  -H 'Content-Type: application/json' \
  -d '{"queries": [[1,"upper deck 1994"],[2,"upper deck rookie"]],
       "corpus_phrases": true, "npmi_min_count": 2}'
```

## `POST /_vocab/learn_and_apply` — Learn from stored queries and apply

Learn synonyms from the engine's **own** already-ingested queries and apply them in one step (unlike
`POST /_vocab/learn`, which only returns synonyms learned from caller-supplied queries for review). The
engine re-mints its vocabulary, recompiles every stored query under the new normalizer, and atomically
swaps — so both surface forms of each learned alias match immediately, with zero false negatives
(ADR-046). It has the same persistence boundary as `PUT /_vocab`: a durable cluster checkpoints the
vocabulary in its coordinator manifest; a single-node operator must save `GET /_vocab` to the
configured `--vocab-file` before restart.

```bash
curl -X POST 'localhost:9200/_vocab/learn_and_apply?min_count=2'
```

```json
{
  "acknowledged": true,
  "recompiled": 1280
}
```

`min_count` (query parameter, default: 2) is the minimum any-of occurrences before a synonym pair is
learned; `recompiled` is the number of stored queries rebuilt under the new vocabulary.

Add `?corpus_phrases=true` to ALSO self-derive entity **phrases** from the engine's own live query text
via NPMI corpus phrase induction (ADR-053), applied through the same recompile/blue-green rebuild with
zero false negatives. Tunable via `npmi_min_count` (default 3), `npmi_tau` (default 0.30), and
`npmi_iterations` (default 2). Add `?learn_equivalences=true` to learn the any-of groups as
**equivalence groups** applied via FN-safe expansion (ADR-054) instead of collapse synonyms.
Absent ⇒ any-of synonym learning only (byte-identical to before).

```bash
curl -X POST 'localhost:9200/_vocab/learn_and_apply?corpus_phrases=true&npmi_min_count=3'
```


---

## Learned-alias registry (ADR-060/061/102/103)

The registry governs equivalence expansion with provenance, a structural **kind**, confidence,
optional feedback evidence, and a lifecycle **status** (`candidate`, `active`, or `rejected`).
Candidates and rejected entries are metadata only. Active, expressible groups widen positive
matching through the false-negative-safe equivalence path.

Current activation policy:

| Source and kind | Default status |
|---|---|
| Operator-imported or manually edited single-token or multi-word group | `active` |
| Any-of-learned clear single-token spelling/abbreviation variant | `active` |
| Any-of-learned distinct-token or multi-word group | `candidate` |
| Distributionally discovered group, of any kind | `candidate` |
| Mixed-feature-kind or otherwise unexpressible group | `candidate`; it cannot affect matching |

Multi-word aliases are implemented, not deferred: ADR-061 supplies query-side collapse plus the
two title feature views, and ADR-076 makes cluster routing positive-view-aware. Import,
learn-and-apply, and an edited registry installed through `PUT /_vocab` work in single-node and
cluster modes. The lower-confidence discovery-record and match-feedback workflows remain
single-node where noted below.

### `GET /_vocab/aliases`

Returns the full registry (for review) plus a status summary. Lock-free (reads the `ArcSwap`
snapshot, ADR-016).

```bash
curl 'localhost:9200/_vocab/aliases'
```

```json
{
  "aliases": {
    "entries": [
      { "forms": ["autograph", "autographs"], "provenance": "learned_from_queries",
        "kind": "single_token_variant", "status": "active", "confidence": 0.6 },
      { "forms": ["bgs", "psa", "sgc"], "provenance": "learned_from_queries",
        "kind": "single_token_distinct", "status": "candidate", "confidence": 0.5 }
    ]
  },
  "summary": { "active": 1, "candidate": 1, "rejected": 0 }
}
```

### `POST /_vocab/aliases/import`

Import a Solr/Lucene synonym file (the format ES's `synonyms_path` consumes) into the registry and
apply it live. Comma lists are one equivalence group; `a, b => c, d` mappings are unioned into one
**bidirectional** group (Reverse Rusty equivalences are bidirectional); `#` comments and `\,`
escapes are honored. An imported single-token or multi-word group activates because the file is an
operator declaration. An unexpressible or mixed-feature-kind group remains a candidate.

```bash
curl -X POST localhost:9200/_vocab/aliases/import \
  -H 'Content-Type: application/json' \
  -d '{"synonyms": "autograph, autographs\nrc => rookie card"}'
```

```json
{ "acknowledged": true, "activated": 1, "recompiled": 1280,
  "summary": { "active": 1, "candidate": 1, "rejected": 0 } }
```

`activated` is the number of groups switched to active; `recompiled` is the number of stored queries
rebuilt in place so the change takes effect immediately (no restart), with zero false negatives.

### `POST /_vocab/aliases/learn_and_apply`

Learn alias candidates from the engine's own stored queries (any-of co-occurrence) into the registry
and apply. Only clear single-token variants auto-activate; distinct-token and multi-word groups land
as candidates. Inspect them with `GET /_vocab/aliases`, then make an operator declaration through an
import or an edited `PUT /_vocab` document if appropriate. `?min_count=N` defaults to 2. The response
shape matches `import`.

### `POST /_vocab/aliases/discover`

Run deterministic, distributional discovery (ADR-102) and return review proposals without recording
or activating anything. With no body, single-node mode analyzes its live stored query sources. An
explicit corpus is accepted in either mode:

```json
{
  "queries": [[1, "upper deck rookie"], [2, "ud rookie"]],
  "min_token_freq": 5,
  "min_similarity": 0.60,
  "max_pairs": 100,
  "max_vocab": 4096,
  "max_cooccurrence_rate": 0.05,
  "glue_phrases": true,
  "include_numeric": false
}
```

The fields shown are the defaults. The response is
`{"count":N,"proposals":[{"forms":["ud","upper deck"],"similarity":0.91,
"cooccurrence_rate":0.0}]}` in best-first deterministic order. Cluster mode requires the explicit
`queries` corpus because the coordinator has no cross-shard source-gather operation; omitting it is
a 400.

### `POST /_vocab/aliases/discover_and_record`

Single-node only. Run discovery over this engine's own stored queries and record every proposal as a
review `candidate`. The body may override the same knobs as `discover`, but may not supply
`queries`. Nothing activates, matching does not change, and `recompiled` is always zero:

```json
{
  "acknowledged": true,
  "proposed": 12,
  "new_candidates": 8,
  "rediscovered": 3,
  "rejected_sticky": 1,
  "recompiled": 0,
  "summary": {"active": 2, "candidate": 8, "rejected": 1}
}
```

Cluster mode returns 501. Run the dry discovery against an explicit corpus, review it, and install
the resulting registry with `PUT /_vocab` instead.

### Match-feedback validation

ADR-103 can passively compare which queries match titles containing each form of a tracked
two-form candidate. Capture is single-node, in memory, default off, and applies to compatibility
`/_search` and `/_mpercolate` traffic. Enable it with the dynamic settings
`alias_feedback_capture=true`; `alias_feedback_max_pairs` bounds the tracked candidate set.

`GET /_vocab/aliases/feedback` returns the rolling evidence. Threshold query parameters default to
`min_overlap=0.5`, `min_titles=50`, and `min_queries=20`:

```json
{
  "capture_enabled": true,
  "tracked_pairs": 1,
  "min_overlap": 0.5,
  "min_titles": 50,
  "min_queries": 20,
  "pairs": [{
    "forms": ["ud", "upper deck"],
    "titles_a": 75,
    "titles_b": 81,
    "titles_both": 2,
    "sampled_a": 43,
    "sampled_b": 46,
    "excluded": 4,
    "overlap": 0.78,
    "validated": true
  }]
}
```

`POST /_vocab/aliases/validate_and_apply` with the same thresholds stamps evidence and raises
confidence for validated candidates without changing matching. Add `?activate=true` to explicitly
promote eligible validated candidates through the full recompile path; rejected or mixed-kind
entries are never resurrected by automation. The response reports `validated`, `stamped`,
`activated`, `recompiled`, and the status `summary`.

`POST /_vocab/aliases/feedback/reset` clears the process-local evidence window and returns
`{"acknowledged":true}`. All three feedback endpoints return 501 in cluster mode; run capture on a
single-node replica of the title stream and install reviewed activations through cluster
`PUT /_vocab`.
