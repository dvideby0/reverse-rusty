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
  that consolidates hashed terms / learned synonyms on compaction (the "improve" phase), and cross-process
  normalizer shipping. *(Absorbed the former Tier-3 "normalizer/vocab shipping" residue.)*
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

- **Compaction-that-improves.** The merge mechanic is done; add the "improve" phase — recompute stats
  and re-anchor queries whose anchor drifted hot, repacking covers during a merge that's already
  happening. ([`design/ingestion-and-updates.md`](design/ingestion-and-updates.md) §7.)
- **Wire the NPMI learner as the runtime vocab source.** The `learn.rs` corpus learner and the `Vocab`
  runtime plumbing both exist but aren't connected; wiring them lets the feature model self-derive from
  the corpus. ([`research/corpus-feature-learning.md`](research/corpus-feature-learning.md).)
- **Alias / equivalence learning** (e.g. `UD` ≡ `Upper Deck`) with the precision-first safety rail
  (expansion-not-collapse, feedback-validated, reversible) — the one feature-learning sub-problem that
  can affect correctness, so it stays confidence-gated.

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
  deployment** (ADR-027, 029, 031–045). **Tier 0 (the v1 acceptance gate) is complete**; this
  distributed buildout resumes next.
  **Per-ADR detail is
  in [Implemented](#implemented-working-tested) above**; the build path + cross-shard correctness argument are
  in [`design/clustering-and-scaling.md`](design/clustering-and-scaling.md) §10 (hashing-variant survey:
  [`research/clustering-prior-art.md`](research/clustering-prior-art.md)). *(Reliability hardening —
  auto-unfence-on-abort, the translog-lease TTL, and wiring the autoscaler's handoff to `execute_handoff` —
  **landed in ADR-048**; see Implemented above.)* **Still design-only** — the production multi-node residue:
  **auto-split** + `recommended_shard_count` (the autoscaler's split recommendation needs a real split
  mechanism + the clean node→endpoint move it implies); **replicate-broad-to-all** (in-process uses the
  shard-0 lane only); **TLS/auth** on the gRPC + control transports; and an
  end-to-end durable-multi-node rolling-restart harness. *(**Dynamic vocabulary / normalizer shipping moved
  up to Tier 0** — it is a v1 correctness item now, not Tier-3 residue; the cross-process phasing of it may
  remain here per the [research spike](research/dynamic-vocabulary.md).)*
- **Aspects-first ingestion.** Use eBay structured item-specifics as features instead of relying only
  on title parsing — higher feature quality, but a larger domain integration.

### Tier 4 — ES/OS percolator parity (verified against a documented reference workload)

These items close the gaps between Reverse Rusty and how production percolator deployments are actually
*operated* — now **verified against a documented reference workload**
([`research/percolator-workload.md`](research/percolator-workload.md)), not just an initial guess. That
write-up also records what already **aligns** (entity identity ↔ `logical_id`, the
include/exclude/OR-group DSL, create/update/delete + bulk) and what RR **subsumes** (the two-stage
recall→verify pattern — RR's integer-exact verifier makes output false-positive-free, so there is no
app-side re-test); the capability-by-capability mapping is
[`research/prior-art.md`](research/prior-art.md) §2. The **dominant read pattern** — *"percolate, then
narrow to one category"* — makes the **metadata + filtering pair the high-value work**; scoring and batch
pagination are smaller, lower-priority items. *(Validating RR against this workload's **real corpus** — a
false-negative / throughput audit — remains the open step in **Current limitations** below.)*

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
  `tests/persistence.rs` (tagged `.seg`/WAL reopen). **Remaining:** ranking + `/_mpercolate` `from`
  pagination (decision point 4, below); and **threading tags through the (experimental) cluster path** —
  the cluster's add path doesn't accept tags today (untagged-but-consistent, no silent loss). That
  follow-on touches `cluster/clog.rs` (`ClusterMutation::Add` + a versioned `tags` field),
  `cluster/shard.rs` (`Shard::{insert_extracted, ingest_extracted, percolate}` + every impl — `LocalShard`,
  `ReplicatedShard`, `HandoffShard`, and the `distributed` `RemoteShard`/`ShardServer` + a gRPC proto field),
  `cluster/coordinator/{ingest,lifecycle,matching}.rs` (resolve tags → `TagId` via `get_or_synthetic`, the
  coordinator `TagDict` + `ClusterManifest.tag_dict_data` wiring, and a `TagPredicate` through `percolate`),
  and extends `tests/cluster_oracle.rs` + `tests/cluster_durability_oracle.rs` (filter sweep + tags survive
  rebuild-from-log). Full design:
  [`design/matching.md`](design/matching.md) §5 and
  [`design/ingestion-and-updates.md`](design/ingestion-and-updates.md) §11.
- **Match scoring / ranking + `/_mpercolate` pagination — lower priority.** An optional layer *over* the
  boolean-correct result set: sort or boost by a priority tag, top-K, and `from`/offset on `/_mpercolate`
  (`size`-only today, unlike `/_search`). Lower priority because in the reference workload only a public
  search surface ranks — the core matching jobs take the tag-filtered set unranked. It never touches the
  candidate index or verifier ([ADR-049](DECISIONS.md); [`design/matching.md`](design/matching.md) §5.4).
- **Byte-cleaning: punctuation-equivalence rules.** `clean_into` currently maps all
  non-alphanumeric, non-marker characters to a space. Production title corpora treat
  mid-word hyphens (`-`), apostrophes (`'`, `'`), slashes (`/`), and periods differently
  — e.g. `O'Brien`, `O-Brien`, and `OBrien` should all normalize to the same token. Add a
  configurable punctuation-folding table to the byte-cleaning pass so callers can declare
  which characters collapse vs. become word boundaries.
  ([`normalization.md`](design/normalization.md) §2.)
- **`NormalizerBuilder`: bulk synonym / alias registration API.** The builder already
  supports phrases and single-token synonyms, but real deployments need to register
  hundreds of equivalences (abbreviation → canonical, variant spellings, term expansions
  like `auto` ≡ `{autograph, autographed, signature, signed}`). Add a batch registration
  method and/or a file-based vocabulary loader so large synonym tables are easy to maintain
  outside of code.

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
- **No `--version` flag** in the CLI.
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

**Memory / hot-path micro-optimizations**
- **`alive: Vec<bool>`** uses 8× the memory of a bitvec (1 byte vs 1 bit per entry).
- **`seg_lens` Vec allocated on the match hot path** — could be a fixed-size array.
- **WAL `append_insert` allocates a Vec per write** — production WALs use pre-allocated write buffers.
- **Byte-at-a-time CRC-32** for manifest writes — table-based would be ~10× faster.

**Robustness / build hygiene**
- **Durable-ingest segment-write failures surface only as `ingest_rollback`, not `segment_write`.** ADR-021
  routes the *flush* path's segment write through a precise `DurabilityOp::SegmentWrite`, but the durable
  build/bulk path (`build_durable_base`) returns the `io::Error` up to the infallible wrapper, which emits
  `IngestRollback` with the OS error in the `error` field — so the operator sees the cause but not the
  precise op label (unlike a manifest failure, which emits both `manifest_write` + `ingest_rollback`).
  Optional refinement: emit `SegmentWrite`/`SegmentMmap` from inside `build_durable_base` for symmetric
  labeling. Low priority — the underlying error is already visible.
- **Dict format not versioned** — adding a new `FeatureKind` variant would silently corrupt deserialization.
- **Deferred from the external-review hardening pass (ADR-052):**
  - **Optional bearer-token / API-key auth for mutating endpoints.** The HTTP server now defaults to
    a loopback bind (`--host 127.0.0.1`), but has no built-in auth — exposing it requires a trusted
    network or an authenticating reverse proxy. An opt-in `RR_AUTH_TOKEN`-style gate on
    `_doc`/`_bulk`/`_flush`/`_compact`/`_vocab`/`_settings` would let it serve a wider network safely.
  - **Cooperative cancellation on the match path.** `timeout_ms` is a response deadline only — a
    timed-out `/_search`/`/_mpercolate` returns 408 but its `spawn_blocking`/Rayon work runs to
    completion. A coarse per-segment deadline check could shed abandoned CPU, at the cost of a branch
    on the (deliberately branch-predictable) hot path; weigh against simply bounding concurrency.
  - **`from`/offset + per-slot hit truncation on the percolate endpoints.** `/_mpercolate` is
    `size`-only (no `from`), and `/_search`'s per-slot `slots[*].hits` are complete-per-slot
    (unpaginated). Fine today; revisit if per-slot response size becomes a concern.
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

