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
- **Status:** Fully implemented; daachorse v3 Aho-Corasick replaced the original token-trie
- **Gotchas:** Grade detection is context-aware (§3.2); diacritic folding is lossy by design; `#`-prefixed card numbers need disambiguation from serial numbers

---

## 1. Query DSL

Constrained on purpose — a smaller language means every query is gateable by construction.

```
Grammar (EBNF-ish):
  query        := clause+
  clause       := positive | negative
  positive     := term | phrase | anyof
  negative     := '-' term | '-' phrase | '-' anyof
  anyof        := '(' term (',' term)* ')'        // OR group
  phrase       := '"' term+ '"'
  term         := word | normalized-entity-literal

Semantics:
  bare term / phrase            → MUST (required)
  ( a , b , c )                 → MUST (a OR b OR c)   (required any-of group)
  -term                         → MUST_NOT
  -( a , b , c )                → MUST_NOT a AND MUST_NOT b AND MUST_NOT c
```

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
   - `michael jordan` / `mj`(only if safely disambiguated) → `player:michael_jordan`
   - `psa gem mt 10` / `psa 10` / `psa10` → `grader:psa` + `grade:10` + `grader_grade:psa10`
4. **Pattern features:** regex-free scanners for `year` (19xx/20xx), `grade` (0–10, half-grades),
   `lot/bulk/count`, set numbers, autograph/signed flags, reprint/custom/proxy flags.
5. **Dense feature IDs:** every feature → a `u32` from a global **feature dictionary** (§3). Strings die
   here; downstream is integers only.

Output is a `TitleFeatureSet`: a sorted, deduped `&[u32]` of feature IDs plus typed entity slots
(year, grader, grade, ...) packed into a fixed-size struct for slot checks. Reused across titles.

**MJ disambiguation note.** Ambiguous aliases (`MJ`) only fire when corroborated (e.g. co-occurring
`bulls`, a basketball set, or another Jordan-specific token), otherwise they are dropped. Dropping is
safe for recall *of the alias* but we must ensure queries written as `MJ` are themselves normalized the
same way at compile time — they are, because the normalizer is shared. Determinism is the invariant.

---

## 3. Feature dictionary

- `FeatureId(u32)` assigned densely, **ordered by global query-document frequency** (rarest = lowest
  IDs is one option; we keep an explicit `freq[]` table rather than relying on ID order so we can
  re-rank on compaction). Frequency drives anchor selection (see [`matching.md`](matching.md) §1).
- Feature *kinds* are encoded in high bits or a side table: `Year`, `Brand`, `Player`, `CardTerm`,
  `Grader`, `Grade`, `GraderGrade`, `Flag`, `Generic`. Kinds let the exact matcher do slot checks and
  let the optimizer reason about selectivity per kind.
- The dictionary is immutable per segment (compaction can re-rank); the hot delta uses an append-only
  overlay so new features get IDs without rewriting segments.
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

**Two architectural conclusions from that real-data study** (detailed in
[`../research/real-data-findings.md`](../research/real-data-findings.md)), both affecting *where
features come from* rather than the matching core:

- **Aspects-first ingestion.** The grade is often stated *without* the grader in the title; eBay returns
  such listings via structured item-specifics (aspects). The right *document* is the title **plus**
  eBay's `(field,value)` aspects (`grade=10, grader=psa, player=…, set=…`); the title normalizer becomes
  the *fallback* path for free-text gaps. (Design-only; see [`../STATUS.md`](../STATUS.md).)
- **Learned entity vocabulary.** The player/set/parallel vocabulary is unbounded and multi-word, so the
  hand-built vocab must be replaced by the corpus learner — see
  [`../research/corpus-feature-learning.md`](../research/corpus-feature-learning.md). As of ADR-010,
  `Normalizer::default_vocab()` builds an **empty** normalizer (no hard-coded card vocabulary); domain
  vocabulary is supplied at runtime via the `NormalizerBuilder` fluent API or the `Vocab` system
  (learned from query any-of groups, ADR-015). The NPMI corpus learner (`src/bin/learn.rs`) remains a
  separate analysis binary, not yet wired in as a runtime vocabulary source.
