# Reverse Rusty documentation

The index for everything under `docs/`. It also defines **where each fact lives** (the SSOT registry)
and **the rules for editing docs** (the conventions). If you only read one thing, read the right
gateway for your task — you should never have to scan every file.

- **Building on the code (AI agent or contributor)?** Start at [`../AGENTS.md`](../AGENTS.md) — the
  canonical safety rails + task→doc router. [`../CLAUDE.md`](../CLAUDE.md) is a small compatibility
  shim for tools that discover that filename.
- **Evaluating or using the project (human)?** Start at [`../README.md`](../README.md) — overview,
  quickstart, and links into the reference.

## How these docs are organized

Four levels, each giving *just enough* to decide whether to go deeper:

- **Level 0 — `../AGENTS.md`:** the canonical agent entry point. It inlines the correctness contract
  + invariants (safety) and routes to one doc per task. `../CLAUDE.md` points tools there and repeats
  only the load-bearing contract; neither is a reference manual.
- **Level 1 — gateways:** this hub, plus [`CHANGELOG.md`](CHANGELOG.md), [`roadmap.md`](roadmap.md),
  [`DECISIONS.md`](DECISIONS.md), [`design/README.md`](design/README.md),
  [`performance/README.md`](performance/README.md), [`research/README.md`](research/README.md), and the
  top [`../README.md`](../README.md). Each answers its domain's top question and links deeper.
- **Level 2 — catalogs and deep dives:** the topic files below, the ADR area catalogs in
  [`decisions/areas/`](decisions/areas/), and the per-group endpoint files in
  [`reference/api/`](reference/api/). Read only when a task needs the detail.
- **Level 3 — individual records:** the one-file-per-decision ADR records in
  [`decisions/`](decisions/). The decision tree deliberately adds this final hop so no gateway or
  area catalog grows back into a repository-wide wall of summaries.

## Map — what to read when

### History, roadmap & decisions
- [`CHANGELOG.md`](CHANGELOG.md) — the reverse-chronological record of material changes that
  shipped. Read when asking "what changed, and when?".
- [`roadmap.md`](roadmap.md) — the **prioritized roadmap and proposal hub**: unfinished work only,
  with each idea's problem, direction, constraints, and completion test. Read when asking "what's
  next?" or evaluating a proposed change.
- [`DECISIONS.md`](DECISIONS.md) — the ADR **area hub**: choose a compact catalog under
  [`decisions/areas/`](decisions/areas/), then open the one canonical record under
  [`decisions/`](decisions/). Read when asking "why was it done this way?" or "why was X *not*
  built?" (declined → ADR-019).
- [`testing.md`](testing.md) — **how we test**: the suites, pressure/soak tests, benchmarks, the git
  hooks, and the CI pipeline. Read when running or changing tests, benchmarks, or the gate.

### Design — how it works
- [`design/README.md`](design/README.md) — mental model (the two-phase diagram) + the correctness
  contract + how the design answers the spec. **Start here to understand the system.**
- [`design/normalization.md`](design/normalization.md) — DSL internals, the shared normalizer, the
  feature dictionary, and marketplace title-shape hardening.
- [`design/matching.md`](design/matching.md) — signature-cover optimizer, candidate index, integer
  exact matcher, broad-query cost classes, explain.
- [`design/ingestion-and-updates.md`](design/ingestion-and-updates.md) — LSM write path, segments,
  tombstones, compaction, vocabulary rebuilds, and the current format/semantic fences.
- [`design/clustering-and-scaling.md`](design/clustering-and-scaling.md) — sharding and horizontal
  scale (Cluster v1 = the in-process multi-shard core + durable reopen + dynamic vocabulary, built —
  ADR-027/046, `src/cluster/`; the distributed multi-node layers are built but **experimental** —
  proven in-process, over localhost gRPC, and across single-host container-network deployments;
  independent multi-machine production evidence remains open).

### Reference — how to use it
- [`reference/api.md`](reference/api.md) — the REST API index (server flags + endpoint groups + a
  method/path matrix); per-group endpoint detail lives in [`reference/api/`](reference/api/).
- [`reference/dsl.md`](reference/dsl.md) — the query DSL, normalization, and vocabulary.

### Operations
- [`operations/deployment-modes.md`](operations/deployment-modes.md) — the **supported-deployment
  contract** (ADR-098): the four-mode matrix with exact bring-up commands, the guaranteed
  REST surface, the auth posture, and the consolidated **v1 non-goals** table. Supported-deployment
  truth lives here — the other operations pages link to it rather than restating it.
- [`operations/build-and-smoke.md`](operations/build-and-smoke.md) — the **fresh-clone checklist**:
  build + gate + local smoke + image + Compose/harness smoke + Helm validation, with the exact
  command and what each leg proves (the acceptance recipe for the contract above).
- [`operations/cluster-deployment.md`](operations/cluster-deployment.md) — deploy + run a multi-node
  cluster from the container image: topology, bootstrap order, certs/tokens, scale, recover, monitor,
  the vocab-redeploy procedure ([`deploy/`](../deploy/) packaging + ADR-081).
- [`operations/kubernetes-deployment.md`](operations/kubernetes-deployment.md) — the Helm chart
  ([`deploy/helm/`](../deploy/helm/)): values, secrets, probes, and the k8s deploy procedure (ADR-084).
- [`operations/backup-restore.md`](operations/backup-restore.md) — back up + restore a deployment
  (single-node or cluster); the safety guarantee + the FS-snapshot zero-stall path (ADR-079) + the
  **restore rehearsal** drill.
- [`operations/disaster-recovery.md`](operations/disaster-recovery.md) — the **DR runbook**: the
  RPO/RTO model by mode, the scenario→procedure map, and the flows only it owns —
  shard-volume loss at RF=1, control-quorum majority loss, whole-cluster restore.
- [`operations/rolling-upgrade.md`](operations/rolling-upgrade.md) — the **version-upgrade
  procedure**: preflight, the compatibility-fence contract, the
  control→shards→coordinator order with health gates, the Compose + Helm legs, rollback.
- [`operations/sizing.md`](operations/sizing.md) — the **resource-sizing guide**: the
  memory-driven shard-count method, headroom, cache residency, per-component sizing — pointing at
  [`performance/results.md`](performance/results.md) for the numbers.
- [`operations/alerting.md`](operations/alerting.md) — **what to alert on and why**,
  one section per rule in the shipped, promtool-validated
  [`deploy/prometheus-alerts.yml`](../deploy/prometheus-alerts.yml).
- [`operations/threat-model.md`](operations/threat-model.md) — the **threat model**: trust boundaries,
  assets, adversary model, controls mapped to code, the v1 non-goals + operator checklist, and the
  container-scan baseline (ADR-089).

### Performance
- [`performance/README.md`](performance/README.md) — headline numbers + how to reproduce.
- [`performance/results.md`](performance/results.md) — **the canonical, detailed measurements**,
  bottleneck analysis, and the 100M-query extrapolation.
- [`performance/benchmark-results.txt`](performance/benchmark-results.txt) — the runbook + the
  machine-independent **invariants** (the regression gate) + the dated capture log.

### Research — where the ideas came from
- [`research/README.md`](research/README.md) — index of the prior-art studies.
- [`research/prior-art.md`](research/prior-art.md) — Lucene Monitor, ES/OS percolator, Tantivy,
  roaring, Aho-Corasick, set-containment joins.
- [`research/broad-scaling-prior-art.md`](research/broad-scaling-prior-art.md) — hot/broad
  predicates, duplicate queries, shared evaluation, self-tuning classification: the k-index→Vespa
  lineage, BE/PS/A-Trees, SIFT/Le Subscribe/NiagaraCQ/YFilter, Siena covering, LEO/CE-feedback —
  five evidence-ranked levers + FN-safety arguments (→ ADR-104's measured broad growth).
- [`research/percolator-workload.md`](research/percolator-workload.md) — the abstract **reference
  workload** a production percolator serves (per-query tags, filter-by-tag, two-stage recall+verify,
  ranking-as-presentation) and how Reverse Rusty maps to it (→ ADR-049, ADR-055, ADR-064).
- [`research/clustering-prior-art.md`](research/clustering-prior-art.md) — consistent-hashing variants,
  content-based routing, and the ES distributed-percolator contrast (clustering design; → ADR-027).
- [`research/dynamic-vocabulary.md`](research/dynamic-vocabulary.md) — absorbing new vocabulary after the
  dict is frozen (the Cluster v1 dynamic-vocab work, **built**: ES global ordinals, Vespa, RocksDB dict,
  feature hashing; → ADR-046).
- [`research/corpus-feature-learning.md`](research/corpus-feature-learning.md) — learning the feature
  extractor from the query corpus (NPMI).
- [`research/real-data-findings.md`](research/real-data-findings.md) — marketplace title shapes and
  the generic ingestion boundary.
- [`research/multiword-synonyms.md`](research/multiword-synonyms.md) — design learnings from an
  abandoned multi-word-alias attempt (the token-graph vs flat-feature-set / forbidden-feature wall).

---

## Single source of truth (SSOT) registry

Each fact has exactly **one** canonical home. Everywhere else carries a one-line summary plus a link —
never a second copy. This is what keeps the docs from drifting.

| Fact | Canonical home | Everywhere else |
|---|---|---|
| Two-phase architecture diagram | [`design/README.md`](design/README.md) §1 | `../README.md` keeps a product-facing version; `../AGENTS.md` keeps a skeleton. Both link here. |
| Correctness contract (lossless cover) | [`design/README.md`](design/README.md) §2 | `../AGENTS.md` and the small `../CLAUDE.md` compatibility shim inline the one-sentence form for safety; others link. |
| Critical invariants | [`../AGENTS.md`](../AGENTS.md) | Design docs cite the relevant invariant + link; `../CLAUDE.md` directs tools to the canonical file. |
| Dependency versions | `engine/Cargo.toml` | `../README.md` lists crate *purposes* (no versions); docs never pin versions. |
| Performance numbers | [`performance/results.md`](performance/results.md) (exact) + [`performance/benchmark-results.txt`](performance/benchmark-results.txt) (invariants) | everywhere else summarizes without copying a dated capture. |
| Module map (responsibility → paths → canonical doc) | [`../AGENTS.md`](../AGENTS.md) | `design/README.md` §4 keeps a coarser design-topic↔module view + link. |
| Current behavior | the matching [`design/`](design/README.md), [`reference/`](reference/api.md), or [`operations/`](operations/deployment-modes.md) page | agent entry points keep only the safety-critical skeleton and route to the owner. |
| Shipped change history | [`CHANGELOG.md`](CHANGELOG.md) | ADRs carry rationale and proof; the changelog carries the dated outcome only. |
| Roadmap / proposals | [`roadmap.md`](roadmap.md) (unfinished items with full descriptions) | When work ships, delete the item and add its outcome to the changelog. |
| Completed-work narrative (what shipped, how, scope, proof) | the one ADR file in [`decisions/`](decisions/) | `CHANGELOG.md` carries a short dated outcome; the roadmap contains no finished copy. |
| Architecture decisions / "why" | [`DECISIONS.md`](DECISIONS.md) hub → area catalog in [`decisions/areas/`](decisions/areas/) → one ADR in [`decisions/`](decisions/) | referenced by `ADR-NNN` (pointers, never copies). |
| Test count | `cargo test` | docs describe the suites qualitatively; no hand-maintained integer. |
| Testing / benchmark / CI workflow | [`testing.md`](testing.md) | agent entry points keep the commands; CI rationale in [`DECISIONS.md`](DECISIONS.md) ADR-024; benchmark numbers in `performance/`. |
| REST API / query DSL | [`reference/api.md`](reference/api.md) index + [`reference/api/`](reference/api/) subfiles · [`reference/dsl.md`](reference/dsl.md) | `../README.md` links here instead of inlining. |

---

## Documentation conventions

Read before adding or moving docs. These rules are the only thing keeping a flat, duplicative wall of
text from growing back (the repo is maintained largely by an LLM agent, and there is no automated doc
link-checker in CI — the discipline has to live here).

- **Progressive disclosure.** `AGENTS.md` (rails + router; reached through the `CLAUDE.md`
  compatibility shim when needed) → gateways → deep
  dives. A fact should
  normally be reachable in one hop from its domain gateway. ADRs intentionally use
  `DECISIONS.md` → area catalog → record so the catalogs remain bounded; opening a known ADR number
  directly still reaches the canonical record immediately.
- **Bounded pages, coherent splits.** Treat roughly 600 lines as a soft review signal for both code
  and documentation, not a mechanical limit. When a hub grows, add a catalog/topic layer and move
  complete responsibilities beneath it; keep one canonical owner and preserve direct navigation
  back to the hub. A cohesive record (especially one ADR) may exceed the signal rather than being
  cut into arbitrary fragments.
- **Single source of truth.** Each fact has one owner (registry above). Elsewhere: a one-line summary
  + a section link, or nothing. Before adding a paragraph, ask "does this already live somewhere?" —
  if yes, link it.
- **Where new information goes:**
  - New architecture decision → a new `decisions/adr-NNN-slug.md` file (next number) + one row in the
    matching [`decisions/areas/`](decisions/areas/) catalog. Add an area to
    [`DECISIONS.md`](DECISIONS.md) only when no existing catalog is coherent. **Never renumber or
    delete** — superseded ones are marked, not removed.
  - Component/algorithm design → the matching `design/<topic>.md` (extend, don't fork).
  - "What does it do now?" → the matching design, reference, or operations page. "What shipped?"
    → [`CHANGELOG.md`](CHANGELOG.md). "What's next?" or "what is proposed?" →
    [`roadmap.md`](roadmap.md). When an item ships, add a dated changelog outcome and **delete** the
    roadmap item rather than striking it through; the ADR remains the permanent rationale.
  - Benchmark numbers → append a dated entry to [`performance/benchmark-results.txt`](performance/benchmark-results.txt)
    first, then narrate in [`performance/results.md`](performance/results.md).
  - Dependency version → `engine/Cargo.toml` only. Docs describe a crate's purpose, never its version.
  - A new top-level responsibility or moved module family → update the responsibility map in
    [`../AGENTS.md`](../AGENTS.md). `CLAUDE.md` contains no module copy to synchronize.
  - Testing / benchmark / CI workflow → [`testing.md`](testing.md) (the gate itself is `engine/check.sh`,
    which CI runs; decision rationale → [`DECISIONS.md`](DECISIONS.md) ADR-024).
  - User-facing API/DSL change → the matching [`reference/api/`](reference/api/) subfile (+ the
    [`reference/api.md`](reference/api.md) index/matrix) / [`reference/dsl.md`](reference/dsl.md).
  - Prior art / research → [`research/`](research/).
- **Numbers convention.** Summarize trends outside the performance section; keep measured
  throughputs, resident/durable sizes, and p99 latencies in `performance/results.md` and
  `benchmark-results.txt`.
- **No dangling links.** If you rename or remove a doc/section, `rg 'old-name' -g '*.md'`
  and repoint or delete every reference in the same change.
- **The sanctioned duplications — keep them, keep them in sync.** (1) The one-sentence correctness
  contract appears in `design/README.md`, `AGENTS.md`, and the small `CLAUDE.md` compatibility shim;
  the full critical-invariant list appears only in `AGENTS.md`. (2) The two-phase diagram exists in
  product form (`../README.md`) and engineering form
  (`design/README.md` §1) for different audiences. Do not add another copy; change each twin
  together.
