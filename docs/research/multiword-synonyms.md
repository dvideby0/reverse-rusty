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






# Feedback from review

The right single PR should **not** be “PR #37 but less buggy,” and it also should not be “PR #38 docs-only and punt.” It should be a **real vocabulary-evolution PR** with a deliberately safe first implementation.

I would make the PR something like:

```text id="1z643w"
feat(vocab): learned alias evolution with safe single-token activation and multi-word design guardrails
```

Or, if you want the ambitious version:

```text id="uxqeln"
feat(vocab): token-graph-aware alias model with separate positive/negative title features
```

But I would personally ship the first one unless you are ready to touch the matcher deeply.

## The right single-PR direction

The PR should say:

> Reverse Rusty already supports live query writes and evolving vocabulary. This PR turns query-authored any-of groups and optional alias files into a versioned alias vocabulary that can be learned, applied, and recompiled live, without pretending multi-word aliases are the same as single-token aliases.

The core idea:

```text id="tq7ycu"
Queries teach the system candidate aliases.
Explicit files can also provide aliases.
Safe aliases become active vocabulary.
Active vocabulary is applied through set_vocab + recompile.
Everything is published by epoch/snapshot swap.
```

That matches how the repo is already built: vocab changes use `set_vocab`, which builds a new normalizer, resolves equivalences, bumps a vocab epoch, and marks old segments stale.  Existing live queries can then be recompiled under the current normalizer and atomically swapped back in. 

## What I would include in the PR

### 1. Alias registry, not just synonym parsing

Add a first-class registry:

```rust
AliasClass {
    id: AliasClassId,
    forms: Vec<AliasForm>,
    provenance: ExplicitFile | LearnedFromQueries,
    confidence: f32,
    status: Candidate | Active | Rejected,
    kind: Generic | Brand | Player | Category | ...
}
```

Each form should know whether it is:

```text id="e6qncg"
single token:     ud
single token:     udeck
multi token:      upper deck
```

This matters because `ud == udeck` is easy; `ud == upper deck` changes span semantics.

### 2. Learn candidates from query any-of groups

When users write:

```text id="gomkmq"
(ud, upper deck, uDeck) rookie
```

the learner should record:

```text id="xh5uqo"
candidate alias group:
  ud
  upper deck
  udeck
```

But do not blindly learn every group. Some any-of groups are alternatives, not synonyms:

```text id="v3xzo6"
(psa, bgs, sgc)       // any grader, not same thing
(red, blue, green)    // alternatives
(jordan, kobe, lebron)// alternatives
```

So the learner should have conservative rules:

```text id="r09jcx"
Auto-activate:
  repeated single-token spelling/abbreviation variants

Candidate/review only:
  multi-word aliases
  broad category alternatives
  mixed entity-kind groups
```

### 3. Activate single-token aliases now

This should be the safe win.

If the system learns or imports:

```text id="8bej3i"
auto == autograph == signature
ud == udeck
```

then compile queries with equivalence expansion:

```text id="z0k42s"
required auto
→ required any-of {auto, autograph, signature}
```

That fits the current engine. PR #38 itself says single-token aliases map cleanly onto existing equivalence expansion. 

This should be in the PR.

### 4. Fix the synthetic/dense ID problem in the same PR

This is the underrated real bug.

Right now, equivalences can be resolved against a frozen dict using synthetic IDs, while later live inserts can use the mutating compile path and intern the same feature as a dense ID. Read-only extraction explicitly falls back to synthetic IDs.  But live insert calls the mutating `extract` path against `Arc::make_mut(&mut self.dict)`. 

So this PR should ensure alias features have stable IDs.

I would do:

```text id="kgplyd"
When activating vocab:
  normalize every alias form
  intern/reserve every alias feature into the dict
  then resolve equivalence groups
  never let the same active alias form later become a different ID
```

This makes aliases survive future writes.

### 5. Runtime apply path

Expose one explicit operation:

```text id="2p3lft"
learn_aliases_from_queries()
apply_alias_vocab()
```

or:

```text id="kpjj5t"
POST /_vocab/aliases/learn
POST /_vocab/aliases/apply
```

Internally:

```text id="r1sugu"
scan live query source
extract any-of groups
update AliasRegistry
activate safe groups
set_vocab(new_vocab)
recompile_stale_segments()
publish snapshot
```

The important product behavior:

```text id="l1a8q3"
New queries keep flowing.
Match requests keep serving.
Vocabulary improves at safe apply points.
No restart.
No full rebuild unless major tokenizer/model version changes.
```

## What I would not include, unless you go ambitious

I would **not** fully activate multi-word aliases in the first version unless the PR also changes the matcher.

This is the trap PR #37 fell into.

For multi-word learned aliases like:

```text id="mkn4iw"
ud == upper deck
ny == new york
nyc == new york city
```

the PR can absolutely **learn and store** them. But either:

```text id="4wjo05"
A. keep them as candidates / explain-only / operator review
```

or:

```text id="7td9nr"
B. implement the real matcher change
```

Do not do the half-measure where multi-word aliases are shoved into one flat title feature set.

## If you want full multi-word support in one PR

Then the correct PR is bigger and should be explicitly matcher-level.

The design should be:

```rust
TitleFeatureViews {
    positive: FeatureSet,
    negative: FeatureSet,
}
```

Use them differently:

```text id="rg42a7"
Required checks:
  use positive features

Any-of checks:
  use positive features

Candidate signatures:
  generated from positive features

Forbidden checks:
  use negative features
```

Today the verifier checks required and forbidden features against the same title mask/features.  That is exactly what needs to change.

Then multi-word aliases can work like:

```text id="su0yyo"
Title: new york city

positive features:
  new
  york
  city
  new_york
  york_city
  new_york_city

negative features:
  whatever the chosen forbidden policy says
```

And you must choose the policy.

For example:

```text id="4hfvk2"
Surface-span negative:
  -"new york" rejects "new york city"

Entity negative:
  -"new york" does not reject "new york city" unless new_york is the selected entity

Alias-expanded negative:
  -"upper deck" also rejects "ud"
```

The PR must pick this intentionally. Otherwise you are back in PR #37 land.

## My preferred single PR

I would ship this:

```text id="gn5zar"
feat(vocab): live learned aliases v1
```

Scope:

```text id="g81sj9"
1. AliasRegistry data model.
2. Learn alias candidates from query any-of groups.
3. Parse/import explicit alias files into the same registry.
4. Auto-activate safe single-token equivalence groups.
5. Store multi-word groups as candidates, not active matcher semantics.
6. Fix alias ID stability across synthetic/dense boundary.
7. Apply vocab live through set_vocab + recompile_stale_segments.
8. Add oracle tests proving no false negatives.
9. Add metrics/explain output showing learned candidates and active aliases.
```

That is a strong PR. It moves the product forward without lying about multi-word support.

## Tests I would require

Minimum tests:

```text id="3ivnsc"
learns_single_token_alias_from_anyof_group
```

Example:

```text id="xz2hoj"
(auto, autograph, signature)
```

Then prove `auto` matches `signature`.

```text id="tw82pg"
does_not_auto_activate_category_alternatives
```

Example:

```text id="l8tyln"
(psa, bgs, sgc)
```

Should become candidate or ignored, not global synonym.

```text id="w1thlu"
alias_ids_are_stable_after_future_insert
```

Load alias before terms are interned, then insert new queries later, and prove alias still works.

```text id="o4kcnt"
vocab_apply_recompiles_existing_queries_without_restart
```

Old query starts not matching; apply learned vocab; same title now matches.

```text id="w2i5zw"
multiword_alias_candidate_is_recorded_but_not_activated
```

Example:

```text id="nfossg"
(ud, upper deck)
```

Learner sees it, stores it, explains it, but does not silently change matcher behavior unless the multi-word feature flag/model exists.

## Then the next PR

After that lands, the next PR is clean:

```text id="bbg09k"
feat(match): token-graph multi-word aliases with positive/negative feature views
```

That PR can activate the stored multi-word candidates.

But if you want it truly as one PR, then combine both:

```text id="ko5l0a"
feat(vocab+match): learned aliases with positive/negative feature views
```

And the acceptance bar is much higher.

## My bottom-line recommendation

Do **not** ship PR #37.

Do **not** stop at PR #38 if the goal is product progress.

Ship one PR that says:

```text id="r6s56x"
We learn aliases from how users write queries.
We safely activate single-token aliases now.
We preserve and expose multi-word alias candidates.
We fix live-vocab ID stability.
We apply changes without restart via vocab epoch + recompile.
We explicitly do not pretend multi-word synonym_graph parity is done.
```

That is the right single PR because it gives you the real-time evolving system you’re describing, while avoiding the exact correctness trap that killed the previous attempt.

