# Normalization — DSL, shared normalizer, feature dictionary

*Scope: how query text and title text are turned into dense integer feature IDs — the front end of the
pipeline. Covers the query DSL, the shared normalizer, the feature dictionary, and the normalizer
hardening forced by real eBay data. Siblings: [`matching.md`](matching.md) (what happens to those
features), [`ingestion-and-updates.md`](ingestion-and-updates.md), [`clustering-and-scaling.md`](clustering-and-scaling.md).
See the [overview](README.md) for the mental model and correctness contract.*

> **Implementation status:** Fully implemented and tested.

**TL;DR (for agents)**
- **Owns:** DSL parser (`dsl.rs`), shared normalizer (`normalize.rs`), feature dictionary (`dict.rs`)
- **Key invariant:** The same normalizer must process both queries and titles — feature spaces must align
- **DSL:** `word` = MUST, `"phrase"` = MUST, `(a,b,c)` = required any-of, `-x` = MUST_NOT (user-facing syntax + vocabulary reference: [`../reference/dsl.md`](../reference/dsl.md))
- **Normalizer pipeline:** clean bytes → daachorse multiword alias scan → tokenize → grader/grade/year patterns → synonyms → generic features
- **Quoted clauses:** zero-slop contiguous paths through analyzed token graphs (ADR-120), not unordered feature conjunctions
- **Status:** Fully implemented; daachorse v3 Aho-Corasick replaced the original token-trie
- **Gotchas:** Grade detection is context-aware (§3.2); diacritic folding is lossy by design; `#`-prefixed card numbers need disambiguation from serial numbers

---

## 1. Query DSL

Constrained on purpose — the compiler can reduce every query to an integer predicate and can identify
queries with no selective positive gate explicitly. A negation-only query is class D: rejected by
default or stored deliberately in the opt-in universal broad lane.

```
Grammar (EBNF-ish):
  query        := clause+
  clause       := positive | negative
  positive     := term | phrase | anyof
  negative     := '-' term | '-' phrase | '-' anyof
  anyof        := '(' member (',' member)* ')'    // OR across complete members
  member       := term+                           // AND within one unquoted member
  phrase       := '"' term+ '"'
  term         := word | normalized-entity-literal

Semantics:
  bare term / phrase            → MUST (required)
  ( a b , c )                   → MUST ((a AND b) OR c)
  -term                         → MUST_NOT
  -( a b , c )                  → MUST_NOT ((a AND b) OR c)
```

The compiler jointly normalizes only each **maximal consecutive run of positive bare terms**. This
lets `new york` recognize a configured multi-word entity without joining terms across a clause that
was not contiguous in the source: `new -used york`, `new "collectible" york`, and
`new (vintage,modern) york` all split the two bare-term runs. Mutable extraction, frozen-dict
extraction, and the reference matcher share this clause-boundary contract (ADR-118).

An unquoted multi-token any-of member is one conjunctive branch, not a bag of interchangeable
features: `(red shoe,boot)` means `(red AND shoe) OR boot`; its negation rejects only a title that
satisfies a complete branch. Normalization may collapse a configured multi-word entity to one feature.
Otherwise the compiler preserves every normalized requirement through exact verification. A
rarest-feature member proxy may be used for lossless candidate retrieval, but never as the member's
exact truth condition (ADR-119).

A quoted clause retains the analyzer's **position graph** instead of flattening it into the ordinary
feature columns (ADR-120). Each edge is `(start, end, FeatureId alternatives)`: a normal token spans
one position, while a collapsed entity or multi-word alias may span several. A phrase matches only
when its complete graph is a connected path through the title graph; `"red shoe"` therefore rejects
`red leather shoe` and `shoe red`. Required phrase edges are widened by active equivalences and use
the overlapping positive title graph `P(T)`. Forbidden phrases are not widened and use canonical
leftmost-longest `N(T)`, preserving ADR-061's negative policy. Analyzer-silent marker/context tokens
receive a raw normalized term edge only when no semantic edge covers that position, so quoting does
not accidentally remove a lexical position. The windowed `grader_grade` feature remains available
to flat matching, but its quoted-graph shortcut is limited to fused or adjacent forms (`psa10` /
`psa 10`) so it cannot jump over an explicit quoted token. Graph-only labels live in a separate
candidate-probe view and never widen the flat `P(T)` used to verify ordinary rows. The user-facing
truth table is in [`../reference/dsl.md`](../reference/dsl.md#quoted-phrases).

Worked example (from the spec):

```
1994 (upper deck,UD) michael jordan sp (preview,previews)
-(next,checklist,checklists,heroes,long,count)
-(minor,minors,top,classic,alumni)
-(auto,autograph,autographs,autographed,signed,dna,signature)
PSA 10 -(sgc,bgs)
```

compiles to:

```
REQUIRED:   year:1994, player:michael_jordan, card_term:sp, grader:psa, grade:10,
            grader_grade:psa10
REQUIRED any-of:  { brand:upper_deck }            (both "upper deck" and "UD" normalize to it)
REQUIRED any-of:  { card_term:preview }           ("preview"/"previews" normalize to one feature)
FORBIDDEN:  next, checklist, heroes, long, count, minor, top, classic, alumni,
            auto, signed, dna, signature, grader:sgc, grader:bgs
```

Note how normalization collapses `(preview,previews)` and `(upper deck, UD)` into single features, so
several DSL "OR groups" become singletons — strictly improving selectivity. The AST exists only at
compile time; it is never walked on the hot path.

---

## 2. Title & query normalizer (shared)

The same normalizer runs over stored-query terms (compile time) and titles (match time). Sharing it is
what makes the feature spaces line up. Pipeline, all over a reusable scratch buffer:

1. **Byte normalization:** lowercase ASCII, strip diacritics, and apply the per-character **punctuation
   table** over a reusable scratch buffer. By default `.` is kept in place (so half-grades like `9.5`
   survive), `#`/`/` become standalone marker tokens (so the number logic tells `#2`/`/199` from grades),
   and every other non-alphanumeric byte becomes a space (a word boundary). The table is **configurable**
   ([ADR-058](../DECISIONS.md)): a character can instead be declared **folding** — deleted, so its neighbors
   join into one token (`O'Brien`/`O-Brien`/`OBrien` → `obrien`), closing a recall gap where a
   punctuation-only spelling difference would drop a candidate. The default is byte-identical to the old
   hardcoded behavior; the same table runs over queries and titles, so any reclassification applies to
   both sides (the §2 shared-normalizer invariant).
2. **Tokenization:** split on spaces into token spans (offsets into scratch), not owned `String`s.
3. **Alias / entity extraction (Aho-Corasick / daachorse):** one pass over the token stream emits
   multi-token entities with leftmost-longest semantics:
   - `upper deck` / `ud` → `brand:upper_deck`
   - a configured `michael jordan` / `mj` alias → `player:michael_jordan`
   - `psa gem mt 10` / `psa 10` / `psa10` → `grader:psa` + `grade:10` + `grader_grade:psa10`
4. **Pattern features:** regex-free scanners for `year` (19xx/20xx), `grade` (0–10, half-grades),
   `lot/bulk/count`, set numbers, autograph/signed flags, reprint/custom/proxy flags. Number typing
   consults a configurable **number-context word list** ([ADR-069](../DECISIONS.md)): a number
   immediately after a listed token is demoted to a generic term (default `["pop"]` — the population
   rule, §4). An **empty** list disables the demotion — the percolator-parity mode, making number
   typing position-insensitive (a 4-digit year is `year:N` everywhere); like the punctuation table,
   the list is vocab-persisted and runs over both sides.
5. **Dense feature IDs:** every feature → a `u32` from a global **feature dictionary** (§3). Strings die
   here; downstream is integers only.

`match_features` writes a sorted, deduplicated `Vec<FeatureId>` supplied by the caller.
`match_features_dual` writes the canonical negative view `N(T)` and positive superset `P(T)` into two
caller-owned reusable vectors; the exact matcher consumes those integer slices directly—there is no
typed entity-slot result structure. When a snapshot contains a quoted predicate,
`match_phrase_views` additionally materializes sorted canonical/positive `PositionArc` buffers in
reusable match scratch. Phrase-free snapshots skip that work. The positive graph unions canonical,
force-additive, positioned raw-token, and overlapping-entity paths, so a collapsed phrase cannot hide
a valid quoted path. Whitespace runs produce the same positions as one space.

Aliases are explicit semantic configuration. The normalizer does not infer contextual corroboration
for an ambiguous short form such as `mj`; operators should activate such an alias only when that
meaning is appropriate for the corpus.

---

## 3. Feature dictionary

- One `Dict` belongs to an engine; a cluster shares one frozen dictionary across every shard so a
  `FeatureId` has the same meaning everywhere. Segments persist/validate that feature space rather
  than owning independent dictionaries.
- Interned `FeatureId(u32)` values are dense in **first-seen order**, below the reserved synthetic-ID
  range. Parallel `names`, `kinds`, `freq`, and `mask_bit` vectors carry metadata; kinds are not packed
  into the ID and the exact matcher does not use typed entity slots.
- `freq[]` is query-document frequency and drives `anchor_plan` independently of ID order. On
  finalization the 64 highest-frequency interned features receive fixed common-mask bits. That mapping
  must remain frozen because existing exact rows store those bits; compaction never reorders IDs or
  re-ranks the mask.
- Mutable compile paths may intern a newly seen feature. Read-only/frozen paths resolve an absent name
  to a deterministic synthetic ID in the reserved high-bit range. Coordinator and shards therefore
  agree on post-freeze vocabulary without mutating the shared dictionary (ADR-046); a synthetic hash
  collision can over-retrieve but cannot cause a false negative.
- **Equivalences (aliases).** The dict carries a transient `EquivMap` (member `FeatureId` → its group)
  consulted by the compile-time expansion pass (`Extracted::expand_equivalences`): a required feature in
  an equivalence group widens to an any-of over the group, so a query phrased with one surface form
  retrieves a title bearing another — applied by **expansion, not collapse**, so it can only widen the
  match set (structurally FN-safe; [DECISIONS.md](../DECISIONS.md) ADR-054). The map is re-derived from
  the `Vocab` at apply time, never serialized, and not part of `Dict::fingerprint`. **Governed by the
  `AliasRegistry`** (provenance / kind / confidence / status; ADR-060): only *active* single-token
  groups feed the map (`Vocab::effective_equivalence_groups`), and on the mutable single-node dict the
  active forms are interned **before** resolving (`intern_equivalence_forms`) so a later insert cannot
  flip a form's synthetic id to a dense one and silently drop the alias (the ID-stability fix).
- **Multi-word aliases (ADR-061).** An active *multi-word* alias form (`new york`) is registered as an
  asymmetric **alias-mode phrase** (`PhraseMode::Alias`), so it resolves to a single entity feature
  (`term:new_york`) and the equivalence map above treats it exactly like a single token. The asymmetry is
  by [`Side`]: on the **query** side the phrase **collapses** to its entity (so expansion widens it); on
  the **title** side it is **additive** (entity + components) and a second, *overlapping* automaton emits
  every nested alias entity. `match_features_dual` thus yields two title views — the canonical
  leftmost-longest `N(T)` and the overlapping superset `P(T)` — consumed by the two-view verifier
  ([`matching.md`](matching.md) §3). No active multi-word alias ⇒ the flat path does not consult the
  overlap automaton and the two flat views are identical (byte-identical to before ADR-061);
  phrase-aware ADR-120 analysis still consults it for ordinary declared entities.

---

## 4. Normalizer hardening (from real eBay data)

Testing the normalizer against ~20 real eBay "PSA 10" titles exposed defects that synthetic data hid;
all are now fixed in `normalize.rs` (the oracle/test suite still passes — zero FN/FP). Full evidence
and the architectural implications are in
[`../research/real-data-findings.md`](../research/real-data-findings.md); the shipped normalizer
behaviour is:

| Defect (real title) | Before | After fix |
|---|---|---|
| **Diacritics** `Nikola Jokić` | `term:joki` (ć dropped) | `term:jokic` |
| **Diacritics** `Ronald Acuña` | `term:acu, term:a` (ñ split the name!) | `term:acuna` |
| **Card number** `#2 BULLS` | `grade:2` | `term:2` |
| **Population** `(Pop 1)` | `grade:1` | `term:1` |
| **Serial** `3/10`, `/5`, `5/23` | `grade:3, grade:10, grade:5` | `term:3, term:10, term:5` (serials) |
| **Accessory** `…5000 10,000` (card sleeves) | `grade:10` (a non-card matched a grade anchor!) | no grade emitted |
| **Grade w/o grader** `Graded Gem Mint 10`, `1st Graded 10` | already ok-ish | `grade:10` via context, no false grader |

The three hardening rules: (a) **diacritic folding** to ASCII; (b) keep `#` and `/` as marker tokens
so **card-numbers, serials, and "pop N" are never read as grades**; (c) require a **grader or a
gem/mint/graded context** before a bare number becomes a grade (kills `10,000` → `grade:10`).

Every one of these context rules is configuration today: `#`/`/` ride the punctuation table
(ADR-058 — the parity configuration declares them `split`), and the `pop` rule is the default entry
of the **number-context word list** (ADR-069 — set it empty to disable the demotion entirely, the
parity mode). Defaults reproduce the table above byte-identically. Note the documented trade: with
the list emptied in a graders-configured vocabulary, `psa pop 7` reads as a PSA grade (the population
count is no longer shielded); the parity mode targets the empty-vocabulary configuration where this
cannot arise.

**Two architectural conclusions from that real-data study** (detailed in
[`../research/real-data-findings.md`](../research/real-data-findings.md)), both affecting *where
features come from* rather than the matching core:

- **Aspects-first ingestion.** The grade is often stated *without* the grader in the title; eBay returns
  such listings via structured item-specifics (aspects). The right *document* is the title **plus**
  eBay's `(field,value)` aspects (`grade=10, grader=psa, player=…, set=…`); the title normalizer becomes
  the *fallback* path for free-text gaps. This remains proposed; see
  [`Aspects-first ingestion`](../roadmap.md#aspects-first-ingestion).
- **Learned entity vocabulary.** The player/set/parallel vocabulary is unbounded and multi-word, so the
  hand-built vocab must be replaced by the corpus learner — see
  [`../research/corpus-feature-learning.md`](../research/corpus-feature-learning.md). As of ADR-010,
  `Normalizer::default_vocab()` builds an **empty** normalizer (no hard-coded card vocabulary); domain
  vocabulary is supplied at runtime via the `NormalizerBuilder` fluent API or the `Vocab` system
  (learned from query any-of groups, ADR-015). The NPMI corpus learner is wired into that runtime
  source through `CorpusLearnConfig` / `learn_vocab_from_corpus` (ADR-053); per-range reruns remain
  part of the roadmap's
  [`self-tuning recommendations`](../roadmap.md#self-tuning-cost-and-placement-recommendations).
