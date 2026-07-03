# Prior Art — Broad Queries, Duplicates & Self-Tuning Matching Cost

*Scope: what the systems that are excellent at boolean-expression matching at scale — ad-targeting
engines, content-based publish/subscribe, continuous-query processors, the production percolators,
and self-tuning databases — actually do about the three problems [ADR-104](../decisions/adr-104-cluster-scale-soak.md)'s
20M soak put numbers on: **hot/broad predicates whose candidate volume grows with the corpus**,
**duplicate stored queries**, and **classification that must not depend on corpus luck**. Complements
[`prior-art.md`](prior-art.md) (which answers "how to gate selectively"; this doc answers "what to do
when gating can't be selective"). Every claim below was verified against the primary paper, the
production documentation, or — for Vespa — the shipped source code; measured numbers are quoted from
those sources. Feeds a future roadmap program (deliberately **not** yet added to
[`../roadmap.md`](../roadmap.md) — maintainer review first).*

## The problem in one paragraph, and the field's verdict up front

With the broad lane ON, Reverse Rusty's candidates/title grows with corpus size (measured lineage:
85.64 @100k → 682 @1M → 10,036 @20M; ~99% of those are *true matches*), and single-token queries
whose anchor sits just below the top-64 commonness cliff ride the selective lane with fat postings
(the measured 32× realtime-lane hit at 20M, ADR-104). The research question: can per-title cost be
bounded by construction? **The field's unanimous verdict: no system bounds *emitted volume* under a
full-recall contract — they bound *wasted work*** (probes, false candidates, per-candidate
verification cost) and make the irreducible true-match volume cheap to produce. The only volume cap
that exists anywhere is top-N scoring, which sacrifices recall (k-index ranking, BE\*-Tree, WAND) and
is unusable under the zero-FN contract. Everything below is about the five levers the field *does*
use: duplicate interning, better anchors (pairs, measured selectivity), cheap quarantine lanes,
shared/factored evaluation, and statistics-driven re-optimization done off the hot path.

---

## 1. The Yahoo lineage: k-index → interval evaluation → Vespa predicate fields

The one documented industrial lineage that matches millions of boolean targeting expressions per
impression, and the production system (Vespa) whose source verifiably implements both papers
(`ConjunctionIndex.java`'s javadoc cites the k-index paper by name; its interval annotator is
line-for-line Fontoura §4).

**Whang et al., "Indexing Boolean Expressions" (VLDB 2009).** Conjunctions are partitioned by
**K = number of positive (∈) predicates** — negations deliberately never count toward K, so they can
never gate retrieval; a conjunction matches iff exactly K posting lists corroborate it with no ∉
violation (a WAND-style sorted merge with skipping). Pure-negation conjunctions (K=0) ride a single
always-probed **Z list** — the always-candidate lane, independently identical to our class-D
universal signature. **The probe set is bounded by the event's key count, never the corpus** (~91
keys/assignment in their workload). 1M expressions = 35 MB, built offline in 7 min; 11–20× faster
than SIFT counting. Two findings matter most here: (a) **deduplicating identical conjunctions before
loading is called out as "a significant factor of the good performance"** — each distinct conjunction
carries an ID list mapping back to expressions; (b) for genuinely broad (K=1, hot-key) expressions
their only volume cap is top-N ranking by inverse-frequency upper bounds — i.e., **at full recall the
k-index accepts O(#matches), exactly where we are**.

**Fontoura et al. (SIGMOD 2010)** evaluates *arbitrarily nested* expressions without normalization
(DNF expansion measured exploding 55→501 MB at depth 2): leaf conjunctions retrieve via the k-index,
then fixed-size **interval annotations** verify the whole tree in O(matching leaves) with two array
probes per interval. Their stated future work — "common subexpression elimination across indexed
BEs" — is duplicate-handling promoted to subexpression granularity, later built by Vespa and A-Tree.

**Vespa predicate fields (production, source-verified).** The pipeline is *counting prefilter →
cheap exact verification*: per stored expression, ingest computes **`min_feature`** — a lower bound
on how many query-side features must hit before the expression can possibly match (AND sums
children, OR takes the min — a per-expression generalization of K); at match time posting-list hits
are **counted per document** and only documents with `count ≥ min_feature` reach interval
verification. Negation rides **z-star**, a single shared always-probed synthetic feature (their
third-way implementation of "negation must never gate"). Shared conjunctions are interned in the
**ConjunctionIndex** (a literal k-index) and fan out to documents afterward — duplicate conjunctions
across stored expressions are matched once per event. Hot posting lists are **auto-promoted to dense
bit-vector form** past `dense-posting-list-threshold` (default 0.40; documented optimum 0.15–0.50,
with failure modes called out on both sides). Ranges decompose into arity-power bucket features
(`arity` knob: "low arity → smaller indexes, more query terms; high arity → larger indexes, fewer
query terms"). Updates in this lineage are a delta tail + periodic offline rebuild-and-swap — our
LSM path is strictly ahead there.

**Borrow:** conjunction/expression dedup with ID-list fan-out (measured first-order on real ad
corpora, productized twice); `min_feature` as a *pre-verification integer count-gate* for the broad
lane (weaker than our signature cover, so useful only where the cover is weakest — class C/D
candidates); dense-posting promotion past a relative-size threshold (with Vespa's measured-safe
default); the third independent confirmation that negation rides an always-probed lane.
**Reject:** top-N as a volume cap (violates zero-FN); arity/range machinery (our DSL has no range
predicates today — if ranges ever land, this is the proven design, and the k-index's 7.32 GB
range-expansion blow-up measured by PS-Tree is the cautionary tale for naive enumeration).

---

## 2. Boolean-expression indexes: BE-Tree, PS-Tree, A-Tree

**BE-Tree (Sadoghi & Jacobsen, SIGMOD 2011 / TODS 2013).** A self-adjusting two-phase
space-partitioning tree. Its transferable core is not the tree (pointer-chasing multi-path descent —
its own authors later added bitmap evaluation to fight it) but the **feedback loop**: partition keys
are chosen and *re-chosen* by a cost-based rank `(1−α)·Gain − α·Loss`, where **Loss = measured
false-candidate rate over a sliding window of events**, with explore/exploit and node recycling when
rank decays. Negations map to full-domain intervals — structurally excluded from ever being keys
(cost-motivated arrival at our correctness invariant). Its guarantee bounds *structure* (height
O(k·log N), corpus-independent), never per-event candidate volume.

**PS-Tree / PSTHash (Ji & Jacobsen, PVLDB 2018).** Independent convergence on our core bet: on a
**real display-ads dataset** (3M subscriptions, dense/hot attributes), single-anchor approaches
collapse and **PSTHash — subscriptions gated on a conjunction of N=2 access predicates, ranked by
measured selectivity, negations structurally last** — wins at 0.24 ms/event (vs SCAN 435 ms, k-index
616 ms/7.32 GB, BE-Tree 2.26 ms). Multi-feature conjunction signatures are what bound candidates
under hot terms; our signature cover is the general form.

**A-Tree (Ji & Jacobsen, SIGMOD 2021).** Current academic state of the art for arbitrary
expressions; its core design axis is **whole-subexpression interning shared across expressions**
(the realization of Fontoura's future-work line) with lazy propagation and cost-based ordering.
Notably there is a production-grade **Rust implementation** (the `a-tree` crate, RTB-flavored) — the
closest existing Rust artifact to this problem, worth a source read if we ever build expression-level
sharing.

**Borrow:** the false-candidate-driven re-ranking loop (BE-Tree's Gain/Loss window) as the *policy*
for compaction-time re-anchoring — it composes with our ADR-103 match-feedback machinery and ADR-056
re-anchoring seam; PSTHash's evidence that pair-anchors are the dense-workload winner; A-Tree as the
reference design if subexpression sharing is ever pursued. **Reject:** the mutable in-place adaptive
tree (violates the immutable-segment model — adaptation must ride compaction); structural-height
guarantees marketed as cost bounds.

---

## 3. What the production percolators actually do about hot terms

Lucene Monitor and the ES percolator have exactly **four moves**, and we already ship stronger forms
of three:

1. **Pick the least-bad anchor.** Monitor's `TermWeightor` (default: longer-term-is-rarer heuristic;
   optionally `termFreqWeightor` with a supplied corpus-frequency map; manual per-term/field
   penalties). This is the only *proactive* hot-term lever either system has. Our signature optimizer
   already selects by frequency — but against the top-64 cliff rather than a real threshold (the
   ADR-104 finding).
2. **Buy candidate precision with multi-term covers.** Monitor's `MultipassTermFilteredPresearcher`
   (k passes → a conjunction of k disjunctions; documented RAM/index-time cost; `minWeight` floor so
   later passes skip stopwords) and ES 6.1's `CoveringQuery` + minimum-should-match counting are the
   same idea from two directions — require more than one extracted term before a conjunction becomes
   a candidate. Rescues conjunctions containing common terms; does nothing for pure disjunctions of
   common terms (every branch must be indexed to stay lossless).
3. **Quarantine the un-gateable.** Monitor's `ANYTOKEN`, ES's `extraction_result: failed`, Whang's
   Z list: all are our class-D lane. In all three the bucket is **unbounded and uncapped**; ES is
   alone in making it *measurable* (operators are told to audit the failed flag).
4. **Make broad candidates cheap instead of rare.** ES's **verified-candidate skip** (PR #18696):
   for pure disjunctions and single-term queries with complete extraction, a candidate-selection hit
   *is* the proof of match — verification is skipped entirely. Retrieval-is-verification is our
   pure-anchor vacuous accept; our columnar batch evaluator generalizes it (ES's skip dies the moment
   a must clause or msm appears).

**The war stories are the real lesson.** Documented field failures: a CoveringQuery-dominated
percolate (ES 7.6) and extraction failing on simple bool+range queries — both forum threads
auto-closed with zero vendor replies; and OpenSearch issue #16285 measures the modern
extraction-based percolator at **~20× slower than ES 1.x's brute-force-over-cached-queries** for a
term+range disjunctive workload — when candidate selection fails to prune, you pay extraction
overhead *plus* per-candidate MemoryIndex verification with none of the old caching. The reporter's
only effective mitigation was hand-rolled metadata pre-filtering — user-space tag routing (our
ADR-049/055, built in). Flax's own benchmark: Monitor 6–40× over the ES percolator *with* the
presearcher, ES-level without it; Bloomberg runs this lineage at ~5.5M alerts/day.

**Borrow:** the principle that **pruning must degrade to a *cheap* brute force** — our columnar broad
lane is that design, and the 20M bench numbers (inline 3.5k titles/s vs batched 69.9k on the broad-on
corpus) confirm it; frequency-aware anchor choice as a *threshold*, not a rank cliff.
**Reject:** nothing new to adopt — this family validates the architecture and shows where its
ceiling is; the gap it leaves (no cost model, no bounded waste, no dedup) is what §§1–2 and 4–6 fill.

---

## 4. Content-based pub/sub & shared evaluation: SIFT, Le Subscribe, Gryphon, NiagaraCQ, YFilter

The literature that makes evaluation cost scale with **distinct** work. Four moves recur everywhere:
predicate interning, structural sharing of conjunction cores, covering, and FP-tolerated merging
(§5). Identity dedup is universal and structural: Gryphon *assumes* identical subscriptions are
coalesced (a footnote!), YFilter gives "identical (and structurally equivalent) queries … the same
accepting state" with an ID list, Rebeca's first routing rung is identity suppression.

**SIFT (Yan & Garcia-Molina, TODS 1994/1999)** — the ancestor: invert the *queries*; per document,
walk each distinct word's posting and count per-profile hits; match iff `Count == Total`. Its
ranked-key variant (index only the most selective words, verify the residual) is the seed of
signature gating. Production service: 40k+ profiles, 80k docs/day in 1996; the 2009 TOIS
re-implementation (BestFitTrie) filters ~150 KB/s of text against **3M stored queries** by clustering
shared word-sets in a trie forest — and its **ReTrie** background reorganizer (relocate
"underclustered" sets when a clustering-ratio threshold trips) is compaction-that-improves, arrived
at independently: clustering quality is insert-order-dependent and gets repaired off the hot path.

**Le Subscribe (Fabret et al., SIGMOD 2001)** — the closest sibling to our design, and the paper to
steal the *cost model* from. Phase 1: every **distinct** predicate is evaluated exactly once per
event into a global bit vector (measured: 1.3 ms/event at 6M subscriptions, **independent of N**).
Phase 2: subscriptions cluster under an **access predicate** they all imply (their statement of our
lossless-cover obligation — and a negation can never satisfy it); clusters are **columnwise
(SoA!) fixed-size groups by predicate-count** evaluated with size-specialized unrolled kernels +
explicit prefetch. Multi-attribute (conjunction) clustering + a **greedy benefit-per-space
optimizer** over selectivity statistics ν(p), with **dynamic re-clustering driven by benefit-margin
thresholds** measured from live event stats. Headline: **602 events/s at 6M subscriptions** (2001
hardware) vs 1.1 for SIFT-style counting; phase 2 cost 0.1 ms vs 3.53 ms for the
single-predicate-index alternative — **a 35× gap purely from grouping**; matching time flat
100k→3M subscriptions; insertion cost ≈ one match; 50 add + 50 delete/sec sustained while serving.

**Gryphon (Aguilera et al., PODC 1999)** — shared matching trees with don't-care edges; sub-linear
*expected* matching but **worst-case linear** (the standing warning against banking on
distributional bounds under a zero-FN contract); its "factoring on attributes subscriptions rarely
wildcard" is entity-anchor sharding by another name.

**NiagaraCQ (SIGMOD 2000)** — group standing queries by **expression signature** (structure with
constants abstracted); N selections become one join against a **constant table** (constant →
query-id) — structurally *our candidate index*, discovered from the relational side. Its lasting
lessons: incremental grouping (hash the new query into an existing group, never re-optimize the
group — our append-only postings), and **"regrouping is an expensive operation that cannot be
performed frequently"** — re-optimization is a background job with a trigger heuristic, not a
write-path activity.

**YFilter (Diao et al., TODS 2003)** — thousands of XPath subscriptions in one shared NFA; common
prefixes represented and processed once; **order-of-magnitude** win over per-query FSMs, to the
point path matching left the critical path. Its decisive experiment: **Selection Postponed beats
Inline** — pushing per-query value predicates into the shared structure "would destroy the sharing"
and prunes nothing; evaluate them only after the shared gate has pruned. That is independent
empirical confirmation of our invariant that signatures are built from required features only, with
value/negation semantics living in verification.

**Borrow:** identity dedup at the verify layer (one plan record + ID list per distinct compiled
query — every system made this structural); residual factoring *within a posting list* — group
candidates by residual plan shape, subtract the signature-implied features from each member's verify
plan (they are proven by the probe), evaluate shared cores once (Fabret's 35×; our pure-anchor
derivation is the degenerate case); Fabret's greedy benefit-per-space signature selection fed by
selectivity stats as the compaction-time policy; the unanimous churn story (insert ≈ one match into
shared structure; repair quality periodically off the hot path). **Reject:** SIFT-style global
per-profile counters on the hot path (an N-wide mutable array per title violates the allocation-free
budget — counting only pays *inside* an already-gated candidate set, which is what our verifier is);
Gryphon-style global test-order trees (degenerate under sparse set-membership features where
everything is don't-care).

---

## 5. Covering & merging: Siena, Rebeca, MBDs — direction matters

Siena maintains a **covering poset** (filter f₁ covers f₂ ⟺ every notification matching f₂ matches
f₁) so only root filters generate network traffic — but that drops covered subscriptions *for
forwarding*, which an emit-all-matches engine can never do. What transfers is the **contrapositive**,
which Siena itself uses when matching: evaluate the more-general gate first; **if it fails, skip
everything it covers**. For our model the positive-side covering test is a cheap subset test
(required(A) ⊆ required(B), each any-of group of A a superset of some group of B) and skipping
verification of a query whose positive gate failed can never create a false negative — MUST_NOT
only ever *narrows*, so it is excluded from the covering test and stays in per-query verification.

Rebeca's ladder (identity → covering → **merging**) adds the second transferable idea: **imperfect
mergers** — replace k near-duplicate filters with one broader gate that over-matches. The
literature's cost is false-positive *traffic*; under our contract false-positive *candidates* are
explicitly cheap (the exact verifier rejects), so aggressive imperfect merging of near-duplicate
posting entries — one broader gate entry fanning out to k member queries — is a zero-FN-risk index
compression lever. Our class-D universal signature is the degenerate maximal merger; the graded
version merges within a signature's posting by shared residual shape.

The hazards are equally well measured (Li/Hou/Jacobsen, ICDCS 2005): **maintenance dominates**
(insertion the most expensive op; unmerge-on-delete needs recovery protocols; optimal n-filter
merging is NP-complete), and **covering benefit collapses as the distinct-predicate space grows —
75% of 200k subscriptions covered at 2k distinct predicates, 45% at 5k**. On a high-cardinality
feature space the lattice may be flat. Two consequences: (a) build any covering/merge structure
**per immutable segment at flush/compaction** (no online poset churn; a delete is a tombstone, never
an unmerge — the LSM answers the problem the pub/sub papers couldn't); (b) **measure duplicate and
subset structure on the real corpus before building anything** — the payoff ranges from 85.7%
(their measured matching-time cut with covering+merging) to nothing.

---

## 6. Self-tuning machinery: LEO, CE feedback, sketches, cracking, merge-time policy

The "doesn't rely on luck" layer. The defining difference from every system here: in a DBMS a wrong
statistic yields a slow plan; **in Reverse Rusty a compile-side/match-side classification
disagreement yields a false negative**. The prior art splits cleanly into patterns that keep
statistics on the cost side of that line, and the discipline for the one place they can't stay there.

- **Statistics stay advisory, in a separate store, applied at a defined boundary.** DB2 LEO stores
  learned adjustment factors *beside* untouched base statistics, applied only at recompile; SQL
  Server 2022 CE feedback **verifies a hypothesis on a subsequent execution before persisting it and
  backs off permanently on regression**. Template: pair/frequency stats live beside the frozen dict,
  never inside it; reclassification applies only at compaction; the manifest commit is the plan swap.
- **Sketch error points the safe way.** Count-min **never underestimates**, so "this pair is rare"
  can never conceal a hot pair — the cheap misclassification direction is conservative. Heavy-hitter
  sketches offer a `NO_FALSE_NEGATIVES` query mode (DataSketches). Pair-scale feasibility is proven
  (all word-pair counts from 90 GB of text in 2B counters/8 GB with PMI-faithful accuracy), but no
  shipped optimizer sketches *all* pairs — the production pattern is **nominate-then-count**
  (CORDS nominates by sampling; PG/MSSQL humans declare groups). We hold a structural shortcut: **the
  stored queries themselves nominate the pairs** — only feature pairs co-occurring inside some
  query's required/any-of set can ever serve as anchors, so the "which pairs?" problem that defeats
  DBMSs does not exist here.
- **Compaction-as-optimizer has a settled safety contract** (RocksDB compaction filters, Cassandra
  tombstone purge, ClickHouse merge trees, Lucene index sorting): merge output is a **pure function
  of its inputs + a stats snapshot taken at merge start**; **segment-local evidence only justifies
  segment-local actions** (Cassandra refuses a tombstone purge it cannot prove against overlapping
  SSTables — a global-frequency decision needs the coordinator's snapshot passed in); every
  intermediate state must be reader-correct (both our lanes are correct, only differently priced, so
  a half-migrated corpus is safe by construction); synopses must be **mergeable monoids** unioned at
  compaction (the Druid/AggregatingMergeTree pattern — CM/heavy-hitter sketches are entrywise-addable).
  Lucene's cautionary number: merge-time re-sorting **halved indexing throughput** until the work
  moved to flush — re-derive classification from already-compiled per-query metadata at merge; don't
  re-run the expensive optimizer per query per merge.
- **Anti-thrash policy, with shipped defaults to steal.** Trigger families: churn-based (SQL Server's
  `MIN(500 + 0.20n, √(1000n))` — sub-linear so large corpora still re-tune), schedule-based, and
  error-feedback-based (LEO). Dampers: **monotone or margin-gated moves** (database cracking never
  un-cracks — demotion into an *always-visible* cost-quarantine tier can be eager and promotion back
  conservative, since a mis-demotion there costs only latency while both placements stay lossless
  and default-visible; see the visibility obligation below); **verify-then-persist**;
  **observe-first** (OpenSearch search-backpressure ships `monitor_only` as the default mode — the
  classifier should log its lane decisions before it makes them); **per-pass work caps** (stochastic
  cracking's "at most one reorganization per query"; OpenSearch's cancellation ratio ≤10% — bound the
  fraction of a segment reclassified per merge).
- **Admission control precedent.** ES circuit breakers refuse (429) rather than degrade;
  `allow_expensive_queries` is a class-based refusal taxonomy; Vespa's match-phase degradation
  honestly *reports* its downgrade in the response. Our class-D loud-reject + opt-in lane and the
  backlogged class-C ingest warning are the same posture.

**The one genuinely novel obligation** (nothing in this literature has it, because their mistakes
are only slow): the **agreement fence**. Any classification whose match-side behavior differs (a
pair-anchored query needs the title side to *generate* that pair signature) must be a pure function
of a **durably recorded stats snapshot**, shipped with the dict like the common mask, and consumed
at match time as the *recorded decision* — never the live sketch. The closest prior art is RocksDB's
determinism discipline and our own manifest-version fences (ADR-068/080).

**And one obligation this repo already learned the hard way: cost lane ≠ visibility lane.** The
broad lane is not just "cheaper elsewhere" — it is **request-gated** (`include_broad`, default OFF
on the engine's per-title path), so moving an A/B query into it silently hides that query from
default requests: a user-visible false negative even though the index can still retrieve it. The
ADR-056 re-anchoring pass carries an explicit CORRECTNESS GUARD refusing exactly this (`segment/seg.rs`
— "never demote a main-lane (A/B) query into the broad lane … a hotness reclassification is a
major-version blue/green concern, NOT a silent compaction change"; originally a review-caught FN in
PR #31). Any frequency-threshold reclassification (lever 2 below) must therefore keep visibility
invariant: either the demoted-hot queries land in an **always-probed cost-quarantine** (batched
/columnar evaluation like the broad lane, but probed on every request like main — a third tier that
separates the cost axis from the visibility axis), or reclassification rides the documented
blue/green major-version path — never a silent lane move.

---

## 7. The top-k contingency (surveyed, not applicable)

WAND (CIKM 2003) and Block-Max WAND (SIGIR 2011) provide **safe** early termination — identical
top-k set, order, and scores — given monotone-additive scores and true per-list/per-block upper
bounds; measured on GOV2: exhaustive OR 225.7 ms/query → WAND 77.6 → BMW 27.9 (→ 8.9 with docID
reassignment), evaluating 21.9k docs where exhaustive touches 3.82M. Lucene 8 / ES 7 ship BMW
(disjunctions 40%–13× faster) at the documented price that total hit counts become lower bounds. A
static per-query priority is the *ideal* impact function (exact per-posting, append-friendly
layering). But every one of these techniques exists precisely to avoid enumerating all matches:
"safe" means safe-for-the-top-k, i.e., a **controlled, ranked false negative** against the
recall-first contract. Filed as the design if the product contract ever becomes "top-k alerts per
title"; not applicable while stage two consumes the full match set.

---

## 8. Synthesis — the five levers, their evidence, and their FN-safety arguments

Where we already sit: the quarantined broad lane (ADR-003), the columnar batch evaluator (ADR-026),
vacuous accept for pure anchors, tag-filtered percolation (ADR-049/055), batching (`/_mpercolate`),
the class-D loud reject/opt-in lane (ADR-068/080), and never-gate-on-MUST_NOT are all independently
re-invented across this literature (the negation rule three separate times: Z list, full-domain
transform, z-star). The engine is at or past the production percolators on every axis they document.
The levers below are what the stronger families add, ranked by evidence strength for *our* workload:

| # | Lever | Prior art & evidence | Reverse Rusty seam | FN-safety argument |
|---|---|---|---|---|
| 1 | **Identity/conjunction dedup with ID-list fan-out** | Whang: "significant factor" of k-index performance; Vespa `ConjunctionIndex`; A-Tree's design axis; Gryphon coalescing; YFilter shared accepting states; Rebeca identity routing — *unanimous and measured* | Canonical hash of the compiled plan → one posting entry + one verify row per distinct plan, ID fan-out at emit; per-segment at flush/compaction (no online poset) | Emission is identical by construction — duplicates share one evaluation whose verdict applies to every ID |
| 2 | **Frequency-threshold cost reclassification** (replaces the top-64 cliff) | The ADR-104 measured defect (32×); Monitor's `termFreqWeightor` (threshold, not rank); BE-Tree's false-candidate Gain/Loss loop; LEO/CE-feedback discipline; cracking's monotone-moves; OpenSearch observe-first | `is_hot_anchor = top-64 ∨ freq ≥ θ` at classification (mask unchanged); migration at compaction under work caps; stats snapshot pinned in the manifest | **Visibility must stay invariant** — the broad lane is `include_broad`-gated, so a silent A/B→broad move is a user-visible FN (the ADR-056 guard in `segment/seg.rs` exists because a review caught exactly this, PR #31). FN-safe forms only: an **always-probed cost-quarantine tier** (batch-evaluated like broad, default-visible like main), or the blue/green major-version reclassification path (`matching.md` §8). Retrievability inside either structure is arity-1-probe-preserved; both forms lossless ⇒ half-migrated states correct |
| 3 | **Pair-anchor escalation with measured joint frequencies** | PSTHash (N=2 access predicates wins dense real-ads data, 0.24 ms @3M); Fabret multi-attribute clustering (46,600→26,500 checks); CM-sketch one-sided error; nominate-then-count with query-side auto-nomination | Extend the pairing predicate on **both** sides in lockstep, persisted with the dict (RDCT-additive) — the **agreement fence**; title-side pair probes already exist for hot×other | The fence: pair classification is a pure function of a durably recorded snapshot; CM overestimation ⇒ a truly-rare pair may miss the optimization, never the reverse |
| 4 | **Residual factoring inside posting lists** | Fabret's 35× phase-2 gap (columnwise size-grouped clusters, signature-implied predicates subtracted); NiagaraCQ constant tables; SIFT tries | Group posting-list members by residual plan shape; subtract probe-implied features from verify plans; size-specialized kernels (our SoA verify is halfway there) | Verify-layer only — gating untouched |
| 5 | **Broad-lane counting pre-reject + dense-posting promotion** | Vespa `min_feature` (count-gate before verification) + `dense-posting-list-threshold` 0.40 | A conservative per-query lower bound on required title-feature hits, checked in the columnar pass before full verification; promote hot broad postings to bitvectors | The bound must be provably *necessary* (≤ true minimum); a conservative bound only ever under-rejects |

Cross-cutting obligations: **cost lane ≠ visibility lane** — every lever must leave which requests
see a query invariant (the §6 obligation; the in-repo ADR-056 guard is the precedent); the
**agreement fence** for any lever that changes match-side behavior (only #3); **measure first** —
duplicate rate, subset/covering structure, and the class mix on the *real* corpus decide whether #1
is a structural win or a no-op (ICDCS: covering 75%→45% as predicate diversity grew; the generator's
single-token broad queries are unrepresentative by construction — this is the same real-corpus
dependency as ADR-065 criterion 12's open half); and **observe-first** (ship the classifier logging
its decisions before enforcement, the OpenSearch pattern).

What no lever changes: true matches are output, not overhead. The recall-first contract makes
emission volume the product's choice (rank/size caps, filters — built); the engine's job, per this
entire literature, is to make wasted work bounded and true matches cheap.

---

## Sources

**Ad-targeting / boolean-expression indexing:** Whang et al., "Indexing Boolean Expressions," VLDB 2009 — <https://theory.stanford.edu/~sergei/papers/vldb09-indexing.pdf> · Fontoura et al., "Efficiently Evaluating Complex Boolean Expressions," SIGMOD 2010 — <https://theory.stanford.edu/~sergei/papers/sigmod10-index.pdf> · Sadoghi & Jacobsen, "BE-Tree," SIGMOD 2011 — <https://dl.acm.org/doi/10.1145/1989323.1989390> (TODS 2013 — <https://dl.acm.org/doi/10.1145/2487259.2487260>; thesis — <https://tspace.library.utoronto.ca/bitstream/1807/65515/13/Sadoghi_Hamedani_Mohammad_201306_PhD_thesis.pdf>) · Ji & Jacobsen, "PS-Tree," PVLDB 12(3) 2018 — <http://www.vldb.org/pvldb/vol12/p251-ji.pdf> · Ji & Jacobsen, "A-Tree," SIGMOD 2021 — <https://dl.acm.org/doi/10.1145/3448016.3457266> (Rust crate — <https://github.com/AntoineGagne/a-tree>) · Zhang, Chan, Tan, "OpIndex," PVLDB 7(8) 2014 · Vespa predicate fields — <https://docs.vespa.ai/en/schemas/predicate-fields.html>; verified sources: `predicate_tree_annotator.cpp`, `predicate_zstar_compressed_posting_list.h`, `PredicateIndex.java`, `ConjunctionIndex.java` in <https://github.com/vespa-engine/vespa>.

**Percolators & top-k:** Lucene Monitor javadocs — `TermFilteredPresearcher`, `MultipassTermFilteredPresearcher`, `TermWeightor`, `QueryTree` (<https://lucene.apache.org/core/10_2_0/monitor/>) · Luwak — <https://github.com/flaxsearch/luwak> · Flax streamed-search comparison — <https://www.flax.co.uk/blog/2015/07/27/a-performance-comparison-of-streamed-search-implementations/> · Kleppmann, Luwak+Samza — <https://www.confluent.io/blog/real-time-full-text-search-with-luwak-and-samza/> · ES percolator evolution — <https://www.elastic.co/fr/blog/elasticsearch-percolator-continues-to-evolve> · ES PR #18696 (verified skip), issue #25445, 6.1 release notes · percolate docs — <https://www.elastic.co/docs/reference/query-languages/query-dsl/query-dsl-percolate-query> · OpenSearch #16285 — <https://github.com/opensearch-project/OpenSearch/issues/16285> · ES discuss threads 224173, 246495 · GAE Prospective Search — <http://blog.notdot.net/2011/06/Using-the-Prospective-Search-API-on-App-Engine-for-instant-traffic-analysis> (shut down 2015-12-01) · Broder et al., WAND, CIKM 2003 — <https://dl.acm.org/doi/10.1145/956863.956944> · Ding & Suel, Block-Max WAND, SIGIR 2011 — <https://dl.acm.org/doi/10.1145/2009916.2010048>.

**Pub/sub & shared evaluation:** Yan & Garcia-Molina, SIFT — TODS 19(2) 1994 <https://dl.acm.org/doi/abs/10.1145/176567.176573>, TODS 24(4) 1999 <https://dl.acm.org/doi/10.1145/331983.331992> · Tryfonopoulos et al., TOIS 27(2) 2009 (BestFitTrie/ReTrie) — <https://cgi.di.uoa.gr/~koubarak/publications/2009/tryfonopoulos-tois2009.pdf> · Fabret et al., "Filtering Algorithms … for Very Fast Publish/Subscribe," SIGMOD 2001 — <https://www.eecg.toronto.edu/~jacobsen/papers/sigmod01.pdf> · Aguilera et al., Gryphon matching, PODC 1999 — <https://users.ece.utexas.edu/~garg/dist/p53-aguilera.pdf> · Carzaniga, Rosenblum, Wolf, Siena, TOCS 19(3) 2001 — <https://dl.acm.org/doi/10.1145/380749.380767> · Mühl, Rebeca thesis, TU Darmstadt 2002 — <https://tuprints.ulb.tu-darmstadt.de/274/> · Li, Hou, Jacobsen, MBD covering/merging, ICDCS 2005 — <https://www.cecs.uci.edu/~papers/icdcs05/43_lig_unified.pdf> · Chen et al., NiagaraCQ, SIGMOD 2000 — <https://dl.acm.org/doi/10.1145/342009.335432> · Madden et al., CACQ, SIGMOD 2002 — <https://db.csail.mit.edu/madden/html/maddencacqdraftjan2402.pdf> · Diao & Franklin, YFilter overview — <https://yfilter.cs.umass.edu/publications/filtering-overview.pdf> (TODS 2003 — <https://dl.acm.org/doi/10.1145/958942.958947>).

**Self-tuning & statistics:** Stillger et al., DB2 LEO, VLDB 2001 — <http://www.vldb.org/conf/2001/P019.pdf> · Ilyas et al., CORDS, SIGMOD 2004 — <https://cs.uwaterloo.ca/~ilyas/papers/cords.pdf> · PostgreSQL extended statistics — <https://www.postgresql.org/docs/current/planner-stats.html> · SQL Server statistics + CE feedback — <https://learn.microsoft.com/en-us/sql/relational-databases/statistics/statistics> · Goyal, Daumé, Cormode, NLP sketches, EMNLP-CoNLL 2012 — <https://dimacs.rutgers.edu/~graham/pubs/papers/nlpsketch.pdf> · Correlated heavy hitters — <https://arxiv.org/abs/1310.1161> · Halim et al., Stochastic Cracking, PVLDB 5(6) 2012 — <http://vldb.org/pvldb/vol5/p502_felixhalim_vldb2012.pdf> · Graefe & Kuno, Adaptive Merging, EDBT 2010 — <https://openproceedings.org/2010/conf/edbt/GraefeK10.pdf> · RocksDB compaction filter — <https://github.com/facebook/rocksdb/wiki/Compaction-Filter> · Cassandra tombstones — <https://cassandra.apache.org/doc/latest/cassandra/managing/operating/compaction/tombstones.html> · ClickHouse MergeTree — <https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree> · ClickHouse topK — <https://clickhouse.com/docs/sql-reference/aggregate-functions/reference/topk> · Apache DataSketches Frequent Items — <https://datasketches.apache.org/docs/Frequency/FrequentItemsOverview.html> · Elastic index sorting — <https://www.elastic.co/blog/index-sorting-elasticsearch-6-0> · ES circuit breakers — <https://www.elastic.co/docs/reference/elasticsearch/configuration-reference/circuit-breaker-settings> · OpenSearch search backpressure — <https://docs.opensearch.org/latest/tuning-your-cluster/availability-and-recovery/search-backpressure/> · Vespa graceful degradation — <https://docs.vespa.ai/en/performance/graceful-degradation.html>.
