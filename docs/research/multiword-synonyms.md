# Multi-word synonyms — design learnings (why the first attempt was scrapped)

> **Status:** research note. An attempt to add ES-`synonym_graph`-equivalent multi-word aliases
> (branch `feat/adr-060-bulk-synonym-alias-registration`, PR #37) was **abandoned after 5 local
> Codex-review rounds** surfaced a *fundamental* conflict, not a patchable bug. This note records
> what we learned so the next attempt starts from the right design. Single-token alias loading was
> sound and can be re-landed independently; the hard part is **multi-word**.

## The goal

Let operators bulk-load an alias table (Solr/Lucene synonym-file format — what ES's `synonyms_path`
consumes) so a query phrased one way matches a title phrased another. RR is a **recall-first**
stage-one candidate generator, so the cardinal rule is **zero false negatives** (false positives are
cheap — the exact verifier and stage-two filter them).

## What is easy: single-token aliases

`auto, autograph, signature` (all single-token) maps cleanly onto RR's existing **equivalence
expansion** (a required feature widens to an any-of over the group): fully bidirectional, zero-drop,
no caveats. This is the roadmap's headline case and is genuinely simple. **Re-land it on its own.**

## What is hard: multi-word aliases (`ny ≡ new york`)

This is the genuine **token-graph problem** — the reason Lucene needs a whole `SynonymGraphFilter`
subsystem. RR matches on a flat *set* of integer features per title; ES builds a *graph* of
overlapping token paths. Bridging the two is where every approach broke. ES's actual model:
multi-word synonyms are applied **at search time** as a graph (the index keeps component tokens; the
query becomes "the adjacent phrase OR the synonym"), i.e. an **asymmetry** between how the document
and the query are analyzed.

### Approaches tried, and why each failed

1. **Collapse phrase** (`upper deck` → one entity feature, consume components). Bidirectional, but a
   title `upper deck` no longer emits `deck`, so a pre-existing **component-token query** (`deck`)
   stops matching → **false negative.**
2. **Additive phrase to a single-token canonical** (emit the entity *and* keep components). Fixes the
   component query, but a query *phrased* with the multi-word form keeps its components required, so
   it only matches one direction (`ud` query → `upper deck` title, never the reverse) → **one-way, not
   the advertised bidirectional alias.**
3. **Alias-entity phrase** — the asymmetry ES uses: **collapse on the query side** (a multi-word query
   collapses to the entity, which expansion widens to its synonyms) but **additive on the title side**
   (entity + components). This *worked* for a single, non-overlapping alias: bidirectional,
   component-preserving, and phrasal-adjacency-correct like ES. But two deeper conflicts remained:
   - **Overlapping/nested aliases** (`new york` and `new york city` both loaded). RR's automaton is
     leftmost-longest (non-overlapping), so a `new york city` title emits only the longer entity and a
     `new york` query stops matching it → **false negative.** Rescuing it with a second, overlapping
     (`MatchKind::Standard`) automaton that emits *every* phrase entity on the title side then caused…
   - **The fundamental conflict (the wall):** that title-side **superset is safe for positive matching
     but unsafe for negation.** A query `foo -"new york"` that matched `foo new york city` now gets
     **rejected**, because the overlap pass emits the hidden `new york` feature and trips the forbidden
     check. **RR uses one feature set per title for both required *and* forbidden checks**, so a
     superset cannot be correct for both at once. This is a false negative in the most sacred area
     (forbidden features) and cannot be patched without architectural change.

### A second, independent conflict: equivalence-id timing vs dynamic vocab

Equivalences are resolved **once** (at vocab-install time) against the current dict. A form not yet
interned resolves to a deterministic **synthetic** id (dynamic vocab); a *later* `PUT /_doc` interns
the same feature as a **dense** id via the mutating compile path, so the installed equivalence map no
longer matches it — the alias silently goes inactive for queries inserted after the table is loaded
on a fresh index. This affects **single-token aliases too** and must be solved by keeping equivalence
resolution consistent with future interning (e.g. intern the equivalence forms at install time, or
compile later inserts against the installed ids).

## The core lesson

Multi-word synonyms are not a vocabulary-loader feature; they are a **matching-model** feature. The
two walls are:

1. **One feature set, two polarities.** Positive (required/any-of) matching wants the *overlapping
   superset* of phrase entities a title contains; negative (forbidden) matching needs the
   *canonical, non-overlapping* set. A single per-title feature set cannot serve both.
2. **Phrase matching is leftmost-longest, not a graph.** Correct overlapping/nested behavior needs a
   graph (or an equivalent overlapping representation), not a single best match per span.

## Recommended direction for the next attempt

- **Ship single-token alias loading on its own** (Solr-format file → equivalence expansion). It is
  correct today and delivers the roadmap's headline case; don't couple it to multi-word.
- For multi-word, design a **token-graph-aware match model** before writing code. The most promising
  shape is **two title-side feature sets**: an overlap-aware *positive-retrieval* set (so nested /
  overlapping aliases are found) and the canonical leftmost-longest *negation* set (so forbidden
  checks stay correct). Spell out, up front, how it interacts with: forbidden verification, the
  dynamic-vocab synthetic/dense id boundary, the broad lane, the cluster's frozen shared dict, and the
  match hot-path budget. Write the differential oracle to include **forbidden-feature** queries over
  multi-word-alias titles from day one — that is the case the flat-set approach silently broke.
- Treat overlapping multi-word aliases (`new york` ⊂ `new york city`) as a **first-class** requirement
  of the design, not an afterthought — it is where the flat-set model fails.

## Pointers

- Process learning: this took 5 review rounds because each fix created a new interaction. For a
  matching-model feature, **design the model (and its oracle) first**; don't iterate it in as loader
  patches.
- ES references: [`synonym_graph` token filter](https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-synonym-graph-tokenfilter.html),
  [token graphs](https://www.elastic.co/guide/en/elasticsearch/reference/current/token-graphs.html).
- Related existing mechanisms: equivalence expansion (the alias mechanism for single tokens),
  additive phrases (corpus-learned, recall-preserving), and the forbidden-never-gates invariant
  ([`../design/README.md`](../design/README.md) §2) that the flat-set superset violated.
