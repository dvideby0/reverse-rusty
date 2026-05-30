# Real-Data Findings — testing assumptions against live eBay "PSA 10" titles

*We searched eBay for `PSA 10` and ran ~20 representative real result titles through the actual
normalizer (`engine/src/bin/norm.rs`, which prints the features it extracts), testing the design's
assumptions against messy reality instead of synthetic data. Findings, the fixes we shipped, and the
architectural implications follow. The normalizer hardening below is reflected in the
[normalization design](../design/normalization.md).*

---

## 1. What held up

- **Year, brand, grader+grade, refractor, rookie/RC extract correctly** on the majority of titles:
  `2019 Topps Chrome … Refractor PSA 10` → `year:2019, brand:topps, term:chrome, card_term:refractor,
  grader:psa, grade:10, grader_grade:psa10`. The core pipeline works on real text.
- **Multiple grader surface forms** are handled: `PSA 10`, `PSA GEM MT 10`, `PSA10`, and `BGS 9 Mint`
  all resolve to the right `grader`/`grade`/`grader_grade`.
- **Unknown tokens are harmlessly dropped at match time** — emojis (🔥🚨), hype (`*READ*`, `QTY`,
  `LOW POP`), team/city tokens become `term:*` that no query references and are ignored. Confirmed
  the "title tokens absent from the query corpus are irrelevant" assumption holds, and the volume of
  such noise is high (~30–40% of tokens) but cheap.

## 2. What broke — and the fixes we shipped (with before/after evidence)

These were real defects exposed only by real titles. All are now fixed in `normalize.rs` and the
oracle/test suite still passes (zero false negatives/positives).

| Defect (real title) | Before | After fix |
|---|---|---|
| **Diacritics** `Nikola Jokić` | `term:joki` (ć dropped) | `term:jokic` |
| **Diacritics** `Ronald Acuña` | `term:acu, term:a` (ñ split the name!) | `term:acuna` |
| **Card number** `#2 BULLS` | `grade:2` | `term:2` |
| **Population** `(Pop 1)` | `grade:1` | `term:1` |
| **Serial** `3/10`, `/5`, `5/23` | `grade:3, grade:10, grade:5` | `term:3, term:10, term:5` (serials) |
| **Accessory** `…5000 10,000` (card sleeves) | `grade:10` (a non-card matched a grade anchor!) | no grade emitted |
| **Grade w/o grader** `Graded Gem Mint 10`, `1st Graded 10` | already ok-ish | `grade:10` via context, no false grader |

Fixes: (a) **diacritic folding** to ASCII; (b) keep `#` and `/` as marker tokens so **card-numbers,
serials, and "pop N" are never read as grades**; (c) require a **grader or a gem/mint/graded
context** before a bare number becomes a grade (kills `10,000` → `grade:10`).

## 3. What the normalizer *can't* fix — and what it tells us about the architecture

These are the important findings; they reshape the design rather than the code.

### 3.1 The grade is frequently stated WITHOUT the grader in the title
Real PSA-10 results include `Victor Wembanyama … Graded Gem Mint 10`, `… 1st Graded 10`,
`GMA 10 Gem Mint`, `Gem Mint 10` — titles that **do not contain "PSA"**. eBay still returns them
under a `PSA 10` search because it matches on **structured item-specifics (aspects)**: `Grade=10`,
`Professional Grader=PSA`, `Player`, `Set`, `Parallel`, `Card Number`, `Year`. A title-text-only
matcher requiring `grader:psa` would miss these.

**Implication (the big one):** the right *document* for the percolator is not raw title text — it's
the title **plus eBay's structured aspects**. This points to a `(field,value)` document model. The
refined architecture is **aspects-first**: ingest
`grade=10, grader=psa, player=…, set=…, year=…, parallel=…` as authoritative `(field,value)` features
when present, and fall back to the title normalizer for free-text gaps and sellers who don't fill
aspects. Our normalizer becomes the *fallback path*, not the only path.

### 3.2 The player/set/parallel vocabulary is unbounded — corpus learning is mandatory
Every real athlete fell through to generic tokens (`term:scottie term:pippen`, `term:victor
term:wembanyama`, `term:cooper term:flagg` …); only `ken_griffey` matched, because it was hand-coded.
Sets and parallels are multi-word and endless: `Topps Chrome`, `Bowman Draft Chrome`, `Skybox E-X
Century`, `Silver Prizm`, `Black Refractor`, `Purple Pattern /199`, `Yellow Wave Refractor`. **You
cannot enumerate these** — which is precisely the case for the corpus-driven learner in
[`corpus-feature-learning.md`](corpus-feature-learning.md). Real data confirms that doc's thesis: the
entity vocabulary must be *learned* from the query corpus (NPMI gluing `victor_wembanyama`,
`topps_chrome`, `silver_prizm`), not coded. Those learned multi-word entities are the **real selective
anchors**.

### 3.3 Keyword-stuffing and accessories create genuine title ambiguity → broad lane matters
`🚨CHASER PACK🚨 … PSA 10 Chaser Pack` (a raw card pack, not a graded card) and the card-sleeves
accessory both put "PSA 10" in the title for SEO. Title text alone cannot always tell a real PSA-10
card from a listing that merely mentions it. Two defenses, both already in the design: (a) **aspects**
(a real graded card has `Graded=Yes, Grade=10`; a sleeve does not), and (b) the **broad-query lane** —
`grade:10` is a terrible anchor (it matched a sleeves accessory before our fix), exactly the class-C
quarantine case. Real data validates the cost-class design.

### 3.4 Smaller, noted limitations
- **Misspellings**: a real title says `MOSIAC` (Mosaic). A query for `mosaic` won't match `mosiac` —
  a recall miss, not a contract violation (our zero-FN guarantee is relative to supported semantics).
  Optional future work: high-precision fuzzy/typo expansion (edit-distance ≤1 on rare tokens).
- **Hyphenated set names**: `E-X Century` → `e, x, century`; `All-Rookie` → `all, rookie`. The corpus
  learner recovers these as glued phrases; no normalizer rule needed.
- **Card numbers** (`#866`, `#BDC-85`) are currently generic terms. They are highly selective and
  could be modeled as a `card_number` field (a great anchor) — a cheap future win.
- **Season years** `2025-26` → leading `year:2025` captured (the trailing `26` is harmless noise).

---

## 4. The refined architecture this evidence points to

```
incoming listing
  ├─ eBay structured aspects (Grade, Grader, Player, Set, Year, Parallel, Card#)  ── authoritative ──┐
  └─ title free-text ── normalizer (diacritics, grader/grade, patterns) ── fallback for gaps ────────┤
                                                                                                     ▼
                                          (field,value) feature document  (dense IDs)
                                                                                                     │
   feature vocabulary + multi-word entities (players/sets/parallels)  ── LEARNED from query corpus ──┘
                                                                                                     ▼
                          signature cover  →  candidate index  →  integer exact verify  →  matches
                          (broad anchors like grade:10 quarantined to the broad lane)
```

Three evidence-backed changes from the original design:

1. **Aspects-first ingestion.** Treat eBay's structured item-specifics as the primary feature source;
   the title normalizer is the fallback. This directly solves the "grade without grader in title"
   problem and the chaser-pack/accessory ambiguity, and it makes the `(field,value)` model the
   natural substrate.
2. **Learned entity vocabulary.** The hand vocab is replaced by the corpus learner
   ([`corpus-feature-learning.md`](corpus-feature-learning.md)); real data shows the entity space is
   unbounded and multi-word.
3. **Normalizer hardening** (shipped): diacritics, serial/card-number/pop disambiguation, grade-context
   — all driven by concrete real-title failures.

None of this changes the matching core (signatures, integer verification, broad-lane, zero-FN
contract). It changes the **front end** — where features come from — which is exactly where real data
showed the assumptions were thin.

---

## 5. Reproduce

```bash
cd engine
export CARGO_TARGET_DIR=/tmp/perc-target
cargo run --release --bin norm -- /path/to/titles.txt   # prints extracted features per title
```
The sample of real titles used here is in the conversation; drop any set of titles into a file to
re-test. Normalizer fixes are in `src/normalize.rs`; correctness is still guarded by `tests/oracle.rs`.
