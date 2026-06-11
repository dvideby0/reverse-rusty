# Roadmap — what's next

The prioritized roadmap for Reverse Rusty: design-only future work, the (now-complete) **Cluster v1**
acceptance gate that framed it, the operational-polish backlog, and what was evaluated-and-declined.

This is the canonical home for **next steps**. The sibling gateway [`STATUS.md`](STATUS.md) is the
canonical home for **what's already built vs design-only** (and keeps a one-line tier glance that links
back here). Component design lives in the [design docs](design/README.md); decision rationale in the
[ADRs](DECISIONS.md).

---

Priority follows the bottleneck analysis ([`performance/results.md`](performance/results.md) §9): the
selective match path is already ~255× the spec target with a flat ~54 candidates/title, so the leverage
is in the **broad lane**, **memory/footprint**, and the **durability + scale** story — not in shaving
the selective candidate count further.

### Tier 0 — Cluster v1 acceptance gate (**complete** — the shippable milestone)

The one tier here that was **active build work, not design-only research.** It makes **Cluster v1** —
the in-process multi-shard core + durable local reopen + dynamic vocabulary — a defensible, shippable
milestone before the broader distributed cluster roadmap (Tier 3) resumes. These were the critical-core
items from an external review, re-ranked to the top; **all are now done:**

- **Dynamic vocabulary — "it just works" (built + oracle-proven, ADR-046).** The headline v1 correctness
  item. **Before ADR-046**, a live write whose query introduced a term absent from the frozen shared dict
  **silently dropped** it (`coordinator/ingest.rs` + `server.rs` compile read-only against the frozen
  dict), so the query broadened and an all-unknown any-of group risked a *false negative*. v1 now
  **absorbs** the new term with matching still correct (**zero false negatives**). The research spike
  ([`research/dynamic-vocabulary.md`](research/dynamic-vocabulary.md)) is complete and the approach is
  **decided ([ADR-046](DECISIONS.md))** — two complementary mechanisms: **(1) new tokens →
  deterministic feature-hashing** into a reserved `FeatureId` range (every shard computes the same id
  with no coordination — in-process ≡ cross-process for free; collisions = bounded over-match, *never*
  a missed match), and **(2) new alias/synonym rules → runtime normalizer learning** (reuse the ADR-015
  `Vocab` machinery + an atomic in-process `Arc<Normalizer>` swap). Both land in the in-process v1 core;
  the *cross-process shipping* of alias updates is deferred to the experimental distributed layers.
  **Both mechanisms are built and oracle-proven.** (1) feature-hashing — `dict::synthetic_id`/`get_or_synthetic`
  (a reserved high-`u32` range) + both readonly paths (`normalize::compile_features_readonly` + `match_features`)
  hash unknown terms instead of dropping them; **additive** (synthetic ids are disjoint from interned ids, so
  every prior oracle is byte-identical). (2) alias learning — a synchronous **recompile pass**
  (`Engine::recompile_stale_segments`, the single-engine path that also fixes the server's `PUT /_vocab`) + a
  cluster **blue/green rebuild** (`ClusterEngine::set_vocab` — re-mint the dict, **re-place** every query since
  an alias can change a query's anchor/shard, atomic swap; durable via a manifest `vocab_data` blob, manifest
  **v3**, so an alias survives reopen) + **auto-learning** (`Engine`/`ClusterEngine::learn_and_apply` wire the
  ADR-015 any-of learner; `POST /_vocab/learn_and_apply`). In-process only — `set_vocab` refuses a non-local
  cluster (cross-process normalizer shipping stays in the experimental distributed layers). Proven by
  `tests/cluster_oracle.rs` (absorb-without-broadening, satisfiable all-unknown any-of, **declared alias makes
  both surface forms match**, auto-learn) + `tests/cluster_durability_oracle.rs` (alias survives reopen +
  rebind) + `tests/hardening_fixes.rs`. **Remaining (deferred, not v1-blocking):** the background re-materialize
  that consolidates hashed terms / learned synonyms on compaction (the *vocab-consolidation* slice of the
  "improve" phase — distinct from, and not addressed by, the frequency-drift re-anchoring shipped in
  ADR-056), and cross-process normalizer shipping. *(Absorbed the former Tier-3 "normalizer/vocab
  shipping" residue.)*
- **`block_on` regression guard test — done.** `RemoteShard`'s sync→async bridge is *safe by design*
  (rayon workers aren't tokio runtime threads — `remote.rs:9-14`). A guard test now drives a
  multi-shard (fan-out ≥ 2) `RemoteShard` percolate so the bridge runs `block_on` on rayon workers,
  asserting no nested-runtime panic + correctness vs the brute oracle
  (`tests/cluster_grpc_oracle.rs::remote_fanout_block_on_does_not_panic_on_rayon_workers`). A future
  refactor that drove the fan-out from inside an async context would fail loudly here. (A test, not a fix.)
- **Name + lock the Cluster-v1 acceptance gate — done.** `tests/cluster_oracle.rs` (cluster ≡
  single-node ≡ brute, K∈{1,3,8,16} × broad × RF∈{1,2,3}) + `tests/cluster_durability_oracle.rs`
  (reopen ≡ pre-crash ≡ brute) are named the explicit Cluster-v1 gate in [`testing.md`](testing.md)
  (+ a comment in `check.sh`) — both already run on default `cargo test --release`; this names them
  the contract and keeps them green. The dynamic-vocab absorb-correctly assertions are present in both
  (declared-alias both-forms-match + auto-learn in `cluster_oracle`; alias-survives-reopen + rebind in
  `cluster_durability_oracle`).
- **Cluster fan-out / broad-lane benchmark output — done.** New `src/bin/clusterbench.rs` emits
  aggregate shards-probed-per-title (avg/p50/p95/p99/max), a fan-out-vs-K sweep, and the broad-lane
  candidate share; a CLUSTER section is in
  [`performance/benchmark-results.txt`](performance/benchmark-results.txt) (HOW TO RUN + INVARIANTS +
  capture log) and CI runs it. Machine-independent invariants: fan-out is bounded ~2–5 (never → N) and
  candidates/title is identical at every K (the cluster distributes selectivity without inflating it).
  (Observability, not correctness.)
- **Stop overclaiming — done.** The v1/experimental reframe is applied across this doc, the design
  doc, `CLAUDE.md`, and PR #18 — dynamic vocab marked **built + oracle-proven**, and the distributed
  layers consistently framed "oracle-proven *in-process / on localhost*," not "production
  multi-node."

### Tier 1 — highest leverage (the measured bottlenecks)

- ~~**Broad-lane batch / columnar evaluation.**~~ **✅ Shipped (ADR-026).** The broad lane now runs
  once per title-batch (columnar): per-batch feature→title inverted index, one probe per broad anchor
  per batch, bitmap-algebra verification, and a pure-anchor skip-verify fast path (the
  materialized-subscription analog). Exposed as `match_titles_batch` + `POST /_mpercolate`; byte-identical
  to the per-title path; broad postings scanned amortize ~1/batch_size (29× at batch 256, ~2.4× end-to-end
  throughput over the inline path). The "metered to a higher cost class" intent is satisfied by the new
  broad `MatchStats`/Prometheus meters. The single biggest matching-performance lever — now resolved.
  Remaining follow-ups: class-C ingest warnings/rewrite suggestions (its own feature), SIMD posting
  intersection. ([`design/matching.md`](design/matching.md) §4; details in the Implemented section above.)
- ~~**Memory: resident-footprint reduction.**~~ **✅ Shipped (ADR-020).** Phase-0 measurement showed
  resident RAM (once the SoA/index are mmap'd) is dominated by the **source store** (91 B/q) and the
  **reverse index** (53 B/q), *not* the dict. Both are now off-heap — lazy on-disk source store +
  flat mmap'd logical-index columns — dropping resident from **~148 → ~4.5 B/query** (~33×; ~14.5 GB →
  ~0.45 GB at 100M). Deferred as not worth it *for memory*: dict arena/mmap (bounded, ~3.5 B/q — its
  separate un-versioned-manifest correctness hazard is future work) and tighter SoA packing (paged —
  helps disk/throughput, not resident RAM).

### Tier 2 — feature-model quality & self-tuning

- ~~**Compaction-that-improves.**~~ **✅ Shipped (ADR-056).** The "improve" phase: an **opt-in**
  `compaction_reanchor` makes a merge re-derive each alive query's cover with the *current*
  frequencies (decoding the stored exact-store SoA, reusing `anchor_plan`/`build_signatures`) instead
  of carrying old anchors forward — so a query whose anchor drifted to a more-common feature moves
  onto its now-most-selective anchor, shrinking hot postings and per-title candidate fan-out, all
  amortized into a merge that was happening anyway. FN-safe by construction (the new cover is built by
  the same optimizer the title side is matched against; the SoA — masks/forbidden/any-of/tags — is
  copied verbatim, so only postings + class are re-derived); a **no-op in a cluster** (the shared dict
  is frozen, so frequencies never drift) and default-off ⇒ byte-identical. Works *within* the frozen
  64-hot mask (it repairs frequency-ordering drift, incl. A→B arity-2 escalation; **re-ranking the hot
  set itself stays a major-version blue/green concern**, §8). Oracle-proven: a controlled drift forces
  a guaranteed flip (pre ≡ post ≡ brute across all shapes), a 30k realistically-drifted corpus
  re-anchors ~15% of queries with zero FN (per-title + columnar batch), and a frozen-dict no-op test.
  ([`design/ingestion-and-updates.md`](design/ingestion-and-updates.md) §7.3.) **Deferred (the rest of
  §7's "improve" menu):** candidate-survival telemetry, `recommended_shard_count`/`recommended_arity`,
  feature-ID re-ranking for locality, re-running the corpus learner per range.
- ~~**Wire the NPMI learner as the runtime vocab source.**~~ **✅ Shipped (ADR-053).** The `learn.rs`
  NPMI collocation core is now a library module (`src/corpus.rs::learn_phrases_from_text`) that induces
  multi-token entity **phrases** from the live query text and returns them as a `Vocab`, composed UNDER
  the ADR-015 any-of learner via an **opt-in** `CorpusLearnConfig` threaded through
  `Engine`/`ClusterEngine::learn_and_apply_with` (+ the `corpus_phrases` REST params on
  `/_vocab/learn[/_and_apply]`). Phrases only — never aliases. Recall-first: corpus phrases are applied
  **additively** (emit the phrase feature AND keep the component features), so a query referencing a
  component never loses a candidate; engine ≡ brute under the learned normalizer (oracle-proven,
  single-engine + cluster). Honest residual: a phrase-form query tightens to adjacency (re-tokenization)
  — opt-in/reviewable/reversible, pinned by a characterization test. Default-off ⇒ byte-identical.
  ([`research/corpus-feature-learning.md`](research/corpus-feature-learning.md).)
- **Alias / equivalence learning** (e.g. `UD` ≡ `Upper Deck`) — the precision-first safety rail. **The
  mechanism + high-precision sources are ✅ shipped (ADR-054):** a first-class `Vocab.equivalences`
  applied via **expansion, not collapse** (`Extracted::expand_equivalences` — a required feature widens
  to an any-of over its group, structurally FN-safe; a wrong alias degrades to a bounded false positive,
  never a false negative), sourced from operator-**declared** groups (`PUT /_vocab`) and **any-of-learned**
  groups (opt-in `learn_equivalences`), reversible + oracle-proven (incl. a wrong-equivalence-never-drops-a-match
  proof + survives-reopen). **Still deferred behind the same seam (precision order):** **distributional
  discovery** (context-similarity candidates — noisy, conflates substitutes with co-hyponyms, so
  review-first) and **match-feedback validation** (the highest-precision *automated* signal, needs an
  operational title→query loop). Both feed the shipped mechanism when built.

### Tier 3 — scale & production maturity (larger builds)

- **Feature-model versioning + blue/green re-materialize.** Frozen common-mask across minor versions;
  a major model change is replayed from the log into a parallel index, then an atomic alias/epoch swap.
- **Clustering — the 100M horizontal-scale story** (built on the **shared-nothing** model: local segments +
  per-node/coordinator WAL + replication + a quorum control plane — **no object store, no cloud dependency**;
  ADR-033). **Scope frame:** **Cluster v1** = the in-process multi-shard core + durable local reopen +
  dynamic vocabulary (Roadmap **Tier 0**) — oracle-proven and shippable. The **distributed multi-node
  layers** — the gRPC transport + dict shipping, the durable coordinator log + per-shard local segments,
  replication + peer recovery, a durable openraft control plane, a per-shard translog with no-quiesce
  recovery + retention/finalize, a rendezvous-hash shard→node allocator, a runtime-swappable shard backing
  (the live-handoff routing-flip mechanism), the **live data-moving handoff** (peer-recover → fence →
  drain-to-convergence → flip, under concurrent writes), and the **autoscaler** — are **built and
  oracle-proven _in-process / on localhost_, but experimental: not yet hardened for real multi-machine
  deployment** (ADR-027, 029, 031–045). **Tier 0 (the v1 acceptance gate) is complete**; the buildout now
  targets **Distributed v1 ([ADR-065](DECISIONS.md)) — graduating these layers from experimental to
  release-candidate**: feature-complete and hardened enough to *test every feature on real machines* — not
  yet production-proven (that takes mileage), but with no known untested feature seam.
  **Per-ADR detail is
  in [Implemented](#implemented-working-tested) above**; the build path + cross-shard correctness argument are
  in [`design/clustering-and-scaling.md`](design/clustering-and-scaling.md) §10 (hashing-variant survey:
  [`research/clustering-prior-art.md`](research/clustering-prior-art.md)). *(Reliability hardening —
  auto-unfence-on-abort, the translog-lease TTL, and wiring the autoscaler's handoff to `execute_handoff` —
  **landed in ADR-048**; see Implemented above.)*

  **The Distributed-v1 checklist ([ADR-065](DECISIONS.md)) — work top-down; items 1–3 unblock testing
  everything else:**
  1. ~~**Cluster REST surface**~~ **✅ Shipped ([ADR-070](DECISIONS.md)).** A coordinator mode inside
     the existing `server` binary (`--cluster`): the same REST dialect over a `ClusterEngine` —
     in-process (`--shards K`, durable build/reopen) on the default build, remote
     (`--shard-endpoint primary[,replica…]`, dict/tag-space shipped at connect) under `distributed`.
     `PUT /_doc` is a **cluster-atomic upsert** (a new single-frame `ClusterMutation::Upsert`, clog
     v3 — closing the "cluster upsert rides ADR-065" deferral from ADR-067); filtered percolation +
     per-request `include_broad` on `/_search`/`/_mpercolate`; vocab/alias admin over `set_vocab`
     (refusals surface verbatim); `/_checkpoint` + `_cluster/*` ops (state/nodes/rebalance/resync);
     no-analogue surfaces answer 501 with the alternative, unsupported request features (`rank`,
     `explain`) are loud 400s. Oracle-proven (upsert durability: log-tail + checkpoint reopen ≡
     pre-crash ≡ brute) + handler tests over a real in-process cluster.
  2. ~~**TLS + auth on the gRPC transports**~~ **✅ Shipped ([ADR-071](DECISIONS.md)).** Both gRPC
     surfaces (shard + control plane) take opt-in **TLS** (tonic `tls-ring`; PEM identity + client
     CA, `--tls-cert`/`--tls-key`/`--tls-ca`) and an opt-in **mesh token**
     (`--cluster-token`/`RR_CLUSTER_TOKEN`, ADR-062 rules, constant-time verify before any handler —
     default-deny over every RPC incl. future ones). One shared `cluster::security` module both
     sides/planes; additive `_with_security` constructors keep unset paths byte-identical; the
     coordinator retains its client security for internal connects (peer recovery/handoff).
     Fail-loud config; documented trust model (token = mesh admission, TLS = server auth + wire
     privacy; mTLS deferred). Oracle-proven: secured cluster ≡ brute, wrong/missing-token +
     plaintext-to-TLS fail loud, secured 3-node control plane elects + commits.
  3. ~~**A real multi-machine test harness**~~ **✅ Shipped ([ADR-072](DECISIONS.md)).** A
     compose-based harness (`deploy/harness.sh` + `compose.harness.yml` + the `Dockerfile` criterion
     10 will ship) driving a **fully secured** containerized cluster (3 durable shards + handoff
     target + REST coordinator + 3-node control plane) through kill-and-recover (dead shard ⇒ 502
     fail-loud, never silent truncation; restart ⇒ durable self-restore ≡ baseline), rolling
     restarts, a coordinator restart, and a **live handoff under load** via the new
     `POST /_cluster/handoff` (zero FN for acknowledged writes across the move) — every link
     crossing a real container network boundary. Runs in CI on every PR (the `multi-machine
     harness` job). Later criteria land with harness coverage here.
  4. **Tagged-cluster vocabulary change** (the ADR-055 deferral) — persist raw tag strings so the
     blue/green rebuild can reconstruct synthetic tags instead of refusing fail-loud.
  5. **Cluster ranking** (the ADR-059 deferral) — the `RankSpec` seam at the coordinator merge.
  6. **Cross-process vocab/normalizer shipping** + multi-word aliases on a cluster (the ADR-046/061
     deferrals; per the [research spike](research/dynamic-vocabulary.md)) — ship it, or record the
     decided refusal story.
  7. **Auto-split + `recommended_shard_count`** — ring re-keying + the data move via the existing live
     handoff (the autoscaler's split advisory gains a real mechanism).
  8. **Replicate-broad-to-all** — or the explicit ADR for why the RF-replicated shard-0 lane suffices
     at v1.
  9. **Tag-dict fingerprint in the recovery handshakes** (the deferred ADR-055 hardening — the
     feature-dict fingerprint is already validated).
  10. **Deployment packaging + runbook** — Dockerfile/compose for a K-shard + control-plane cluster +
      an ops doc (closes the backlog "no Dockerfile" line).
  11. **Backup/restore documented + tested** (coordinates with ADR-064 item 7; the cluster version must
      cover coordinator manifest + per-shard segments + logs).
  12. **Scale proof at target** — a multi-shard load test at ≥20M stored queries on real hardware (the
      largest soak to date is 10M, single-node) + the real-corpus FN/throughput audit owed in STATUS
      "Current limitations".
- **Aspects-first ingestion.** Use eBay structured item-specifics as features instead of relying only
  on title parsing — higher feature quality, but a larger domain integration.

### Tier 4 — ES/OS percolator parity (verified against a documented reference workload)

These items close the gaps between Reverse Rusty and how production percolator deployments are actually
*operated* — now **verified against a documented reference workload**
([`research/percolator-workload.md`](research/percolator-workload.md)), not just an initial guess. That
write-up also records what already **aligns** (entity identity ↔ `logical_id`, the
include/exclude/OR-group DSL, create/delete + bulk — *update* is the ADR-064 atomic-upsert divergence:
a re-PUT is additive today, not replace-by-id) and what RR **subsumes under its own
semantics** (the two-stage recall→verify pattern — RR's integer-exact verifier makes output
false-positive-free; fronting a deployment that keeps a *foreign* precision stage instead requires the
verified superset contract, §Drop-in parity in that write-up); the capability-by-capability mapping is
[`research/prior-art.md`](research/prior-art.md) §2. The **dominant read pattern** — *"percolate, then
narrow to one category"* — makes the **metadata + filtering pair the high-value work**; scoring and batch
pagination are smaller, lower-priority items. *(A 2026-06 **drop-in parity audit** —
[ADR-064](DECISIONS.md) — re-verified this workload at the semantic level, including an empirical
pinned-pair PoC against the reference deployment's own precision matcher: **zero false negatives** under
the documented parity configuration, now recorded in
[`research/percolator-workload.md`](research/percolator-workload.md) §Drop-in parity. The full
**real-corpus** false-negative / throughput audit remains the open validation step in STATUS "Current
limitations".)*

- **Drop-in operational parity — the [ADR-064](DECISIONS.md) work package. ✅ COMPLETE** (2026-06-10):
  the audit's six decided items are all shipped — items 1–3 under their own ADRs (067/068/069), items
  4–6 batched in [ADR-073](DECISIONS.md) (the ADR-052 precedent); only the program ADR's smaller
  item-7 residue stays in the polish backlog (restart-time measurement, backup/restore procedure,
  original-expression passthrough, tag read-back). Full detail + decision-time behavior citations in
  the program ADR:
  1. ~~**Atomic-upsert `PUT /_doc`**~~ **✅ Shipped ([ADR-067](DECISIONS.md)).** `PUT /_doc/{id}` is now
     replace-by-id (ES `index` semantics): the new version is inserted and every prior live copy
     tombstoned under one writer critical section, one WAL frame (`Upsert`, WAL v4), and one snapshot
     publish — 201-created / 200-updated, `deleted_count` back to 1 after a re-PUT, and a failed
     replace (parse / class-D / WAL) never deletes. Crash-atomic on the ADR-066 substrate (replay
     splits by state domain over the watermark). Also fixed en route: `Engine::open`'s fresh
     (no-manifest-yet) path now replays the WAL tail, so a start-empty server no longer loses
     acknowledged writes on its first crash. Single-node; cluster upsert rides ADR-065.
  2. ~~**Class-D always-candidate lane (opt-in)**~~ **✅ Shipped ([ADR-068](DECISIONS.md)).** The
     `accept_class_d` knob (dynamic, default off = the loud reject) stores a negation-only query as an
     **always-candidate**: `anchor_plan` derives the cover of an empty positive set as the **universal
     signature** (one empty broad-anchor group, `sig_key(&[])`), probed once per segment — once per
     *batch* on the columnar path — in the broad lane; forbidden features enforced only in exact
     verification (never-gate-on-MUST_NOT extended naturally; `is_pure_anchor` structurally false ⇒
     verify always runs). Live writes gate before the WAL (the log holds only accepted mutations ⇒
     replay unconditional, knob-flip-safe), the vocab recompile keeps stored entries (closing a
     pre-existing silent-drop hazard), the effectively empty query still rejects. Oracle-proven
     (vacuous-accept differential: per-title + batch + durability + knob-flip replay). Single-node;
     the cluster lane rides the ADR-065 replicate-broad-to-all criterion.
  3. ~~**Parity-mode normalizer knob**~~ **✅ Shipped ([ADR-069](DECISIONS.md)).** The hard-coded
     `pop` number-context demotion is now a configurable **number-context word list** on the shared
     normalizer (the ADR-058 move: hard-coded → configuration). Default `["pop"]` = byte-identical;
     an **empty list disables the rule** — the parity mode, making number typing position-insensitive
     (a 4-digit year is `year:N` everywhere) and closing the audit's one residual FN class in both
     directions. Vocab-persisted (`Vocab.number_context`, old JSON untouched): applies live via the
     `PUT /_vocab` `set_vocab` recompile (flip-proven, reversible) and survives reopen through the
     standard vocab channels (`--vocab-file` single-node, the cluster manifest's vocab blob). The
     "emit both typings title-side" variant was evaluated + declined in the ADR (mixed typing is
     unrepresentable under the shared normalizer + recompile machinery; both-typings isn't FP-only
     in the single-view path). Oracle-proven: engine ≡ brute under the knob, both audit directions
     asserted closed.
  4. ~~**Loud non-string tag values**~~ **✅ Shipped ([ADR-073](DECISIONS.md)).** Decided **canonical
     scalar coercion** (number/bool → JSON text, the ES keyword behavior — the reference workload's
     dominant filter key is a numeric category, so reject-everything would break drop-in) through ONE
     shared function on ingest + both filter parsers, so the sides can never disagree; `null` = the
     ES "no value" (skipped on ingest, **400 in a filter**); objects/nested arrays/non-object `tags`
     are loud 400s everywhere they were silently dropped (`/_bulk` per-item; cluster mode shares the
     helpers). Proven by the ingest-meets-filter agreement test.
  5. ~~**Wire `maybe_flush` into the REST PUT path**~~ **✅ Shipped ([ADR-073](DECISIONS.md)).**
     `maybe_flush` moved to the success tail of both fallible live-write paths
     (`try_insert_live_with_tags` / `try_upsert_live_with_tags`), so every live write honors
     `memtable_flush_threshold` (rejected writes never flush; replay/bulk/cluster funnels untouched).
  6. ~~**Per-request `include_broad` on `/_search`**~~ **✅ Shipped ([ADR-073](DECISIONS.md)).** The
     same per-request override `/_mpercolate` and the cluster handlers already had, both single- and
     multi-doc arms; absent ⇒ the server default, byte-identical. (`deny_unknown_fields` declined —
     ES-style sibling-tag ingest depends on tolerance.)
- **Per-query metadata + filtered percolation — the lead item. ✅ BUILT (single-node) + oracle-proven
  (2026-06-03, [ADR-049](DECISIONS.md)).** The dominant read pattern: stored queries carry structured tags
  (a category, a status, secondary keys) and callers percolate, then **narrow the candidates by those
  tags**. Tags are interned to integer `TagId`s (`tagdict.rs`, a space disjoint from `FeatureId`) and held
  as an exact-match **SoA column** (`exact.rs`); the tag filter is **pushed into verification** (a hot-path
  sorted-slice intersection); and — load-bearing — tags are **checked only post-candidate, never in
  signature gating**, so the lossless-cover contract is untouched (structurally ADR-006's "forbidden
  features never gate"). Persisted in `.seg` **v3** + WAL **v2** (survive reopen/recovery; v1/v2 read back
  untagged). Exposed over REST as the ES `bool`/`terms`/`percolate` envelope **and** a native `filter`
  block, with ES-style sibling-tag ingest on `PUT /_doc` + `/_bulk`. **Proven:** `tests/oracle.rs`
  (filtered differential — zero false negatives/positives + "filtering only removes" monotonicity),
  `tests/broad_batch.rs` (batch≡scalar under filter, incl. pure-anchor materialization),
  `tests/persistence.rs` (tagged `.seg`/WAL reopen). **Cluster follow-on ✅ BUILT + oracle-proven
  (2026-06-04, [ADR-055](DECISIONS.md)):** tags + filtered percolation now thread end-to-end through the
  in-process multi-shard core AND the experimental gRPC path — one shared frozen `TagDict` (like the
  `Dict`), raw tags in the log + read-only `get_or_synthetic` resolution (never `intern`), the filter
  resolved once at the coordinator + fanned as `TagId` groups, tag-dict shipping via `AdoptDict` +
  fingerprint handshake; additive APIs (`build_with_tags`/`add_query_with_tags`/`ingest_with_tags`/
  `percolate_filtered`) keep the untagged path byte-identical. Proven by `tests/cluster_oracle.rs`
  (filtered ≡ single-node ≡ brute across K×RF + synthetic-tag cross-shard consistency),
  `tests/cluster_durability_oracle.rs` (tags survive checkpoint/reopen), and `tests/cluster_grpc_oracle.rs`
  (filtered percolate + tag-dict shipping over the wire). **Remaining:** a runtime **vocab change on a
  tagged cluster** is currently refused fail-loud (a deferred follow-on — the blue/green rebuild can't
  reconstruct a synthetic tag's string). (Ranking + `/_mpercolate` `from` pagination — decision point 4,
  below — is now ✅ shipped single-node, ADR-059.) Full design:
  [`design/matching.md`](design/matching.md) §5 and
  [`design/ingestion-and-updates.md`](design/ingestion-and-updates.md) §11.
- ~~**Match scoring / ranking + `/_mpercolate` pagination.**~~ **✅ Shipped single-node (ADR-059).** An
  optional layer *over* the boolean-correct result set: a new lean-core `src/rank.rs` +
  `EngineSnapshot::rank` score the already-final matched set as `Σ request-boosts + priority-tag value`
  (additive; priority reuses the tag mechanism, resolved to the newest live copy), and the `/_search` +
  `/_mpercolate` handlers sort by `(score desc, _id asc)`, apply `from`/`size`, and emit `_score`. Also
  adds `from` to `/_mpercolate` and per-slot truncation to multi-doc `/_search` (closing the ADR-052 #3
  tail). Opt-in ⇒ the no-rank path is byte-identical; it runs after verification and never touches the
  candidate index or verifier, so it only reorders + paginates (zero-FN intact). Oracle-/test-proven
  (`tests/ranking.rs` + handler tests). **Deferred:** **cluster** (multi-shard) ranking — cross-shard
  priority fetch at the coordinator merge, behind the same `RankSpec` seam
  ([ADR-049](DECISIONS.md)/[ADR-059](DECISIONS.md); [`design/matching.md`](design/matching.md) §5.4).
- ~~**Byte-cleaning: punctuation-equivalence rules.**~~ **✅ Shipped (ADR-058).** `clean_into`'s
  per-character behavior is now a configurable `PunctClass` table (`Split`/`Fold`/`Keep`/`Marker`) on the
  shared normalizer — set via `NormalizerBuilder` (`fold_punctuation`/`set_punct_class`), persisted through
  `Vocab` (so it survives reopen and rides `PUT /_vocab`). Declaring a character as **`Fold`** deletes it
  so its neighbors join, collapsing `O'Brien`, `O'Brien` (curly U+2019), `O-Brien`, and `OBrien` onto one
  token — stopping a punctuation-only spelling difference from dropping a candidate (the recall-first win).
  The same table runs over queries and titles, so the lossless cover holds under any config (oracle-proven:
  engine ≡ brute, zero FN/FP, under a folding normalizer); the **default reproduces the historical behavior
  byte-identically** (`.` kept, `#`/`/` markers, everything else split), opt-in / default-off.
  ([`normalization.md`](design/normalization.md) §2.) **Deferred behind the same `PunctClass` seam:** an
  *additive* fold (emit the joined form AND the split components — a pure recall gain à la Lucene's
  `WordDelimiterGraphFilter`), and cross-process shipping of the table to a remote shard's normalizer (the
  same deferral as cross-process vocab shipping).
- **Bulk synonym / alias registration → learned alias evolution (2 phases / 2 PRs).** Real
  deployments need to register hundreds of equivalences (abbreviation → canonical, variant spellings,
  expansions like `auto` ≡ `{autograph, autographed, signature, signed}`) and have them evolve live. RR
  already ships the *core* mechanism — equivalence **expansion** (required → any-of, structurally FN-safe;
  declared via `PUT /_vocab` + any-of-learned, [ADR-054](DECISIONS.md)), the any-of synonym learner
  (ADR-015), corpus phrase induction (ADR-053), and the live `set_vocab` + `recompile_stale_segments`
  apply path. The remaining work is to **govern, persist, and safely activate** aliases — and it splits
  along the exact line that killed the first attempt (PR #37, abandoned): **single-token aliases are a
  vocabulary feature; multi-word aliases are a matching-model feature.** Design learnings:
  [`research/multiword-synonyms.md`](research/multiword-synonyms.md). **Phase 1 is now ✅ BUILT +
  oracle-proven ([ADR-060](DECISIONS.md));** Phase 2 (the multi-word matcher feature) is now **✅ BUILT +
  oracle-proven ([ADR-061](DECISIONS.md))**, single-node.

  - **Phase 1 — `feat(vocab): learned alias evolution (safe single-token activation)`. ✅ BUILT +
    oracle-proven ([ADR-060](DECISIONS.md)), single-node.** A *real* vocabulary-evolution PR — not "PR
    #37 with fewer bugs," not docs-only — all nine scope items shipped: **(1)** a first-class
    **`AliasRegistry`** (`forms`, `provenance` = declared-file |
    learned-from-queries, `confidence`, `status` = candidate | active | rejected, `kind`); **(2) learn
    candidates from query any-of groups** with *conservative* rules — auto-activate only repeated
    single-token spelling/abbreviation variants; keep multi-word aliases, broad category alternatives
    (`(psa, bgs, sgc)`), and mixed-entity-kind groups as **candidates for review, never silently active**;
    **(3) import explicit Solr/Lucene synonym files** into the same registry; **(4) auto-activate safe
    single-token groups** through the existing equivalence-expansion path (required → any-of); **(5)
    store multi-word groups as candidates only** (explain/review-surfaced, *not* active matcher
    semantics — this is the half-measure PR #37 must not repeat); **(6) fix the alias-ID-stability bug**
    (see the callout below); **(7) apply live** via `set_vocab` + `recompile_stale_segments` + snapshot
    swap (no restart, no full rebuild); **(8) oracle tests** proving zero false negatives
    (`learns_single_token_alias_from_anyof_group`, `does_not_auto_activate_category_alternatives`,
    `alias_ids_are_stable_after_future_insert`, `vocab_apply_recompiles_existing_queries_without_restart`);
    **(9) metrics/explain** surfacing learned
    candidates vs active aliases. *(Single-node first, like ADR-054 — `set_vocab` already refuses a
    non-local or tagged cluster, verified.)*

    > **Embedded real bug — alias ID stability across the synthetic/dense boundary. ✅ FIXED in
    > [ADR-060](DECISIONS.md)** (`Vocab::intern_equivalence_forms` interns every active form into the
    > mutable single-node dict *before* resolving, called from `Engine::{with_vocab, set_vocab,
    > adopt_vocab}`; regression-guarded by
    > `tests/oracle/alias.rs::alias_ids_are_stable_after_future_insert`, which fails on the pre-fix code).
    > Independent of multi-word: equivalences are resolved **once** at
    > install / `set_vocab` time, and a form not yet interned resolves to a deterministic **synthetic** id
    > (`dict::get_or_synthetic`, read-only `extract_readonly` / `compile_features_readonly`). A *later*
    > live `PUT /_doc` interns that same form as a **dense** id via the mutating `extract`
    > (`Arc::make_mut(&mut self.dict)` in `segment/ingest.rs`), but the `EquivMap` (keyed by `FeatureId`)
    > is never re-resolved — so the installed alias **silently goes inactive** for queries inserted after
    > the table was loaded on a fresh index (a false negative — the sacred case). **Affects single-token
    > aliases too**, so it must be fixed in Phase 1. Fix direction: at activation, normalize +
    > intern/reserve every active alias form into the dict, *then* resolve the groups, so an active alias
    > form can never later flip to a different id.

  - **Phase 2 — `feat(match): token-graph multi-word aliases (positive/negative title feature views)`. ✅
    BUILT + oracle-proven ([ADR-061](DECISIONS.md)), single-node.** The matcher-level PR; activates the
    multi-word candidates Phase 1 stored. Multi-word aliases (`ny ≡ new york`, ES `synonym_graph` parity)
    are a **token-graph** problem, not a loader feature. The first attempt hit a *fundamental*
    flat-feature-set conflict: a title emitted **one** feature set used for **both** required and forbidden
    checks, but the overlapping *superset* of phrase entities needed for positive **retrieval** is
    **unsafe for negation** (`foo -"new york"` would wrongly reject `foo new york city`). **The fix:
    two title-side feature views** (a `TitleView` threaded through `verify` / `match_into`): the positive
    overlapping superset `P(T)` drives retrieval + required + any-of; the canonical leftmost-longest
    `N(T)` drives the forbidden checks only — so `foo -"new york"` matches `foo new york city`. **Forbidden
    policy** = canonical leftmost-longest (recall-safe). The normalizer gained an asymmetric alias-phrase
    mode (query-side collapse to the entity so ADR-054 expansion widens it; title-side additive + an
    overlapping automaton for `P(T)`), so the **equivalence machinery is reused unchanged** — a collapsed
    multi-word form resolves to one entity, which is the only thing that blocked `resolve_equivalences`.
    **Overlapping/nested aliases** (`new york` ⊂ `new york city`) are first-class. A declared/manual
    multi-word alias auto-activates (learned ones stay candidates). The broad lane routes through the
    two-view inline path while aliases are active (columnar two-view is a perf follow-on); cluster
    multi-word aliases need cross-process normalizer shipping (deferred). The differential **oracle
    includes forbidden-feature queries over multi-word-alias titles** (`multiword_alias_forbidden_uses_canonical_view`),
    overlapping/nested retrieval, bidirectional match, and exact engine≡brute — zero-FN; default
    byte-identical.
  - **Deferred refinement — component-conjunction alternative on alias activation.** Activating
    `ny ≡ new york` makes a `new york mets` query read the phrase as the entity, so it stops matching
    the *scattered-components* reading (`new amazing york mets`) — the same semantic shift a declared
    collapse phrase has always made (documented in ADR-061 §semantics-of-activation). ES
    `synonym_graph` keeps the original token path as an alternative; RR can too **without any plan-shape
    change** via CNF distributivity — rewrite the expanded requirement to per-component any-of groups:
    `(entity ∨ ny ∨ new) ∧ (entity ∨ ny ∨ york)` ≡ `(entity ∨ ny) ∨ (new ∧ york)`. Strictly widening
    (recall-only), bounded for typical 2–3-form/2–3-token groups (cap the CNF product and fall back).

### Polish / niche

- **SIMD intersection** for medium/large (mostly broad-lane) roaring postings — a micro-optimization
  best folded into the broad-lane work above.

### Evaluated & declined

- **Query-family / shared-prefix DAG** (subtree pruning). Implicit anchor-sharing already captures the
  near-duplicate-clustering benefit, the selective path isn't the bottleneck, and the
  mmap-serialization + compaction-rebuild cost wasn't justified. See [`DECISIONS.md`](DECISIONS.md)
  ADR-019.

---

## Nice-to-have / operational polish backlog

Low-priority polish, ergonomics, and micro-optimizations — none are production blockers (moved here
from the audit's former P3 list). Roughly grouped:

**API / ops ergonomics**
- **No CORS headers** — browser-based tools can't hit the API. Add `tower-http::CorsLayer`.
- ~~**No `--version` flag** in the CLI.~~ **✅ Shipped for the server bin** (rode along with ADR-062 —
  a `clap` `version` attribute); the other bins remain without one (low value).
- **No Dockerfile or k8s manifests.**
- ~~**No segment detail endpoint** (`/_cat/segments`).~~ **✅ Shipped (ADR-023).** `GET /_cat/segments`
  returns per-segment detail — kind (memory/mmap/memtable), entries/alive/deleted, holes ratio, vocab
  epoch + stale flag, and a resident-vs-overhead byte split — as a text table or `?format=json`, read
  lock-free from the snapshot. Two follow-ups it deliberately deferred are tracked as their own items
  below (per-segment filter FP rate; `_cat` verbose/column-selection flags).
- **No thread-pool introspection** (`/_cat/thread_pool` equivalent).
- **No per-segment filter FP rate in `/_cat/segments`** (deferred from ADR-023). The anchor filter doesn't
  retain its inserted key count, and the mmap arm doesn't expose the filter's block count through the
  `BaseSegment` wrapper — so an honest, *symmetric* false-positive-rate column (real for both memory and
  mmap segments) needs a small change first: have `SegmentFilter` retain `n` at build time and expose
  block count on `MmapSegment`. Then add a `filter_fp_pct` column to the endpoint.
- **`_cat` endpoints lack ES `?v` / `?h` / `?help` flags** (noted in ADR-023). `/_cat/*` returns a fixed
  text table (always with a header) or `?format=json`; ES also supports a verbose toggle, column
  selection, and a help listing. Low-value polish, listed for completeness.
- **`took_ms` uses raw f64** — yields values like `0.003284000000000001`. Use integer ms or round to 2 dp.
- **No pre-warming** for mmap'd segments on cold start.
- **No measured restart/reopen time** at ≥1M queries — the mmap-attach + WAL-tail design implies
  sub-second-to-seconds, but no captured number exists ([ADR-064](DECISIONS.md) item 7).
- **No documented/tested backup-restore procedure** — and a *live* hot-copy is not safe as-is: a
  concurrent flush/compaction can commit a new manifest and delete superseded segments mid-copy, so the
  copied manifest can reference files the copy missed. The procedure needs write-quiescing, a filesystem
  snapshot, or file pinning ([ADR-064](DECISIONS.md) item 7; cluster version = [ADR-065](DECISIONS.md)
  criterion 11).
- **Tags are write-only over REST** — no endpoint returns a stored query's tags (`GET /_doc` returns only
  `_source.query`, hits carry no tags field); a small read-back addition for metadata audits
  ([ADR-064](DECISIONS.md) item 7).

**Memory / hot-path micro-optimizations**
- **`alive: Vec<bool>`** uses 8× the memory of a bitvec (1 byte vs 1 bit per entry).
- **`seg_lens` Vec allocated on the match hot path** — could be a fixed-size array.
- **WAL `append_insert` allocates a Vec per write** — production WALs use pre-allocated write buffers.
- **Byte-at-a-time CRC-32** for manifest writes — table-based would be ~10× faster.

**Test-infrastructure follow-ons (deferred from the ADR-063 audit)**
- **Extend the parse-union oracle's fuzz alphabet** (`src/normalize/parse_union_oracle.rs`) with the
  `#`/`/`/`pop` markers, 4-digit years, and fused graders **inside phrase patterns**. Today those are
  excluded as parse-invariant, which is true for top-level tokens — but a phrase that *consumes a
  marker* could change a neighbouring number's classification across parses, a state interaction the
  current alphabet cannot reach. Requires teaching the reference's `emit_parse` the marker/year rules.
- **A cross-seam integration harness** (recovery×vocab, adopt-on-fresh vs adopt-on-recovered,
  cluster `set_vocab` guard matrix, durable-build-guard ordering): each historical seam escape got a
  point regression test with its fix; a parameterized harness would cover the *combinations*
  systematically. ~22% of historical review-caught escapes were cross-seam.
- **Targeted `cargo-mutants` runs** on `normalize`/`compile`/`exact` as an occasional (not per-PR)
  catching-power audit — declined as a gate in ADR-063 for wall-clock cost; still worth a manual run
  after major matcher changes.
- **Messy variants of the cluster oracles** — `tests/cluster_oracle` and the durability oracle still
  run clean corpora only; threading `messify_dataset` through them would extend ADR-063's coverage to
  placement/routing under adversarial bytes (expected cheap: the harnesses already take a `Dataset`).

**Robustness / build hygiene**
- **Durable-ingest segment-write failures surface only as `ingest_rollback`, not `segment_write`.** ADR-021
  routes the *flush* path's segment write through a precise `DurabilityOp::SegmentWrite`, but the durable
  build/bulk path (`build_durable_base`) returns the `io::Error` up to the infallible wrapper, which emits
  `IngestRollback` with the OS error in the `error` field — so the operator sees the cause but not the
  precise op label (unlike a manifest failure, which emits both `manifest_write` + `ingest_rollback`).
  Optional refinement: emit `SegmentWrite`/`SegmentMmap` from inside `build_durable_base` for symmetric
  labeling. Low priority — the underlying error is already visible.
- ~~**Dict format not versioned** — adding a new `FeatureKind` variant would silently corrupt deserialization.~~
  **✅ Shipped (ADR-057).** The feature-dict **and** its tag-dict twin — the last two unversioned binary
  formats — now carry a `magic + version` header (`RDCT`/`RTGD`), so a layout change or a newer-build blob
  **fails loud** instead of misparsing; the `FeatureKind` byte decodes through the strict, canonical
  `dict::kind_from_tag` (an unknown tag is a hard error, never a silent `Generic`); and the body parse is
  fully fallible (a truncated/corrupt blob errors instead of panicking — closing a latent "no panics in
  library code" violation). Legacy header-less blobs still read, and the content-based `fingerprint` is
  untouched ⇒ the gRPC dict/tag-dict adoption handshake is byte-identical. ([`DECISIONS.md`](DECISIONS.md)
  ADR-057.)
- **Deferred from the external-review hardening pass (ADR-052):**
  - ~~**Optional bearer-token / API-key auth for mutating endpoints.**~~ **✅ Shipped (ADR-062).**
    Opt-in `--auth-token`/`RR_AUTH_TOKEN` bearer gate enforced by one middleware
    (`bin/server/auth.rs`): **default-deny** — every non-GET/HEAD request needs the token except the
    POST-read percolate endpoints, so a future mutating endpoint fails closed; `--auth-protect-reads`
    extends to reads (all but `/_health`). Constant-time compare, RFC 6750 401 challenge + ES-style
    `security_exception` envelope, `auth_failures_total{reason}`, fail-loud config, and a
    non-loopback-without-auth startup warning. Unset ⇒ byte-identical. TLS (and gRPC-transport auth)
    remain the Tier-3 items.
  - **Cooperative cancellation on the match path.** `timeout_ms` is a response deadline only — a
    timed-out `/_search`/`/_mpercolate` returns 408 but its `spawn_blocking`/Rayon work runs to
    completion. A coarse per-segment deadline check could shed abandoned CPU, at the cost of a branch
    on the (deliberately branch-predictable) hot path; weigh against simply bounding concurrency.
  - ~~**`from`/offset + per-slot hit truncation on the percolate endpoints.**~~ **✅ Shipped (ADR-059,
    bundled with ranking above).** `/_mpercolate` now takes `from`, and `size`/`from` bound every hits
    array uniformly — including multi-doc `/_search`'s per-slot `slots[*].hits` (`total` still reports
    the untruncated count).
- ~~**`GET /_vocab` acquires the write mutex.**~~ **✅ Fixed.** `EngineSnapshot` now carries the vocab as
  an `Arc<Vocab>` (the `Engine` holds `Option<Arc<Vocab>>`, `Arc::clone`d into each snapshot — O(1) per
  publish), and `get_vocab` reads `state.snapshot.load().vocab()` instead of locking the engine. Vocab
  reads are now lock-free like every other read endpoint, closing the last ADR-016 violation. (No new
  ADR — this completes ADR-016's stated design.)
- ~~**Server/observability deps are not feature-gated.**~~ **✅ Fixed (ADR-028).** The nine
  HTTP/observability crates (`axum`/`tokio`/`clap`/`parking_lot`/`tower`/`uuid`/`tracing`/
  `tracing-subscriber`/`prometheus`) are now `optional` behind a default-on `server` feature, and the
  server bin carries `required-features = ["server"]`. `cargo build --no-default-features` yields the
  lean embeddable core (daachorse/memmap2/rayon/roaring/arc-swap/serde/serde_json + transitives),
  enforced by the new `clippy (lean core)` lane in `check.sh`. `serde`/`serde_json` stay core (Vocab
  JSON, `EngineConfig`, `ExplainDetail`, JSONL loader are all library code).
- ~~**Durability/persistence failures log to stderr, not the observability stack.**~~ **✅ Shipped
  (ADR-021).** All 14 durability/persistence failure sites in
  `src/segment/{lifecycle,ingest,persistence}.rs` (WAL init/append/checkpoint/reset, manifest write,
  segment write/mmap fallback, source-store write/re-map/load, corrupt-segment-skip and torn-WAL-tail
  on recovery) now emit `EngineEvent::DurabilityFailure { op: DurabilityOp, detail, error }` instead of
  `eprintln!`. The server's observer logs each through `tracing` (`error!` for data-at-risk ops, `warn!`
  for display-only/benign ones — `DurabilityOp::is_data_at_risk`) and increments
  `durability_failures_total{op}` for alerting. Construction/recovery failures predate the observer, so
  they are buffered and replayed when `set_observer` is called.

