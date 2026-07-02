# Reverse Rusty documentation

The index for everything under `docs/`. It also defines **where each fact lives** (the SSOT registry)
and **the rules for editing docs** (the conventions). If you only read one thing, read the right
gateway for your task — you should never have to scan every file.

- **Building on the code (AI agent or contributor)?** Start at [`../CLAUDE.md`](../CLAUDE.md) — safety
  rails + a task→doc router.
- **Evaluating or using the project (human)?** Start at [`../README.md`](../README.md) — overview,
  quickstart, and links into the reference.

## How these docs are organized

Three levels, each giving *just enough* to decide whether to go deeper:

- **Level 0 — `../CLAUDE.md`:** the agent entry point. Inlines the correctness contract + invariants
  (safety) and routes to one doc per task. Not a reference manual.
- **Level 1 — gateways:** this hub, plus [`STATUS.md`](STATUS.md), [`roadmap.md`](roadmap.md),
  [`DECISIONS.md`](DECISIONS.md), [`design/README.md`](design/README.md),
  [`performance/README.md`](performance/README.md), [`research/README.md`](research/README.md), and the
  top [`../README.md`](../README.md). Each answers its domain's top question and links deeper.
- **Level 2 — deep dives:** the topic files below — including the per-ADR records in
  [`decisions/`](decisions/) and the per-group endpoint files in [`reference/api/`](reference/api/).
  Read only when a task needs the detail.

## Map — what to read when

### Status & decisions
- [`STATUS.md`](STATUS.md) — **what's built vs design-only**: a one-line-per-capability inventory
  with ADR pointers, the measured numbers in brief, and a one-line tier glance. Read when asking
  "is X implemented?".
- [`roadmap.md`](roadmap.md) — the **prioritized roadmap**: **open work only** (a shipped item is
  deleted — the ADR is the record), plus the operational-polish backlog and evaluated-and-declined.
  Read when asking "what's next?".
- [`DECISIONS.md`](DECISIONS.md) — the **index** of ADRs (architecture decision records); each ADR's
  full record is one file under [`decisions/`](decisions/). Read when asking "why was it done this
  way?" or "why was X *not* built?" (declined → ADR-019).
- [`testing.md`](testing.md) — **how we test**: the suites, pressure/soak tests, benchmarks, the git
  hooks, and the CI pipeline. Read when running or changing tests, benchmarks, or the gate.

### Design — how it works
- [`design/README.md`](design/README.md) — mental model (the two-phase diagram) + the correctness
  contract + how the design answers the spec. **Start here to understand the system.**
- [`design/normalization.md`](design/normalization.md) — DSL internals, the shared normalizer, the
  feature dictionary, eBay-data hardening.
- [`design/matching.md`](design/matching.md) — signature-cover optimizer, candidate index, integer
  exact matcher, broad-query cost classes, explain.
- [`design/ingestion-and-updates.md`](design/ingestion-and-updates.md) — LSM write path, segments,
  tombstones, compaction, feature-model versioning.
- [`design/clustering-and-scaling.md`](design/clustering-and-scaling.md) — sharding and horizontal
  scale (Cluster v1 = the in-process multi-shard core + durable reopen + dynamic vocabulary, built —
  ADR-027/046, `src/cluster/`; the distributed multi-node layers are built but **experimental** —
  oracle-proven in-process / on localhost).

### Reference — how to use it
- [`reference/api.md`](reference/api.md) — the REST API index (server flags + endpoint groups + a
  method/path matrix); per-group endpoint detail lives in [`reference/api/`](reference/api/).
- [`reference/dsl.md`](reference/dsl.md) — the query DSL, normalization, and vocabulary.

### Operations
- [`operations/deployment-modes.md`](operations/deployment-modes.md) — the **supported-deployment
  contract** (Tier 5 M0, ADR-098): the four-mode matrix with exact bring-up commands, the guaranteed
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
  **restore rehearsal** drill (Tier 5 M3).
- [`operations/disaster-recovery.md`](operations/disaster-recovery.md) — the **DR runbook** (Tier 5
  M3): the RPO/RTO model by mode, the scenario→procedure map, and the flows only it owns —
  shard-volume loss at RF=1, control-quorum majority loss, whole-cluster restore.
- [`operations/rolling-upgrade.md`](operations/rolling-upgrade.md) — the **version-upgrade
  procedure** (Tier 5 M3): preflight, the compatibility-fence contract, the
  control→shards→coordinator order with health gates, the Compose + Helm legs, rollback.
- [`operations/sizing.md`](operations/sizing.md) — the **resource-sizing guide** (Tier 5 M3): the
  memory-driven shard-count method, headroom, cache residency, per-component sizing — pointing at
  [`performance/results.md`](performance/results.md) for the numbers.
- [`operations/alerting.md`](operations/alerting.md) — **what to alert on and why** (Tier 5 M3),
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
- [`research/percolator-workload.md`](research/percolator-workload.md) — the abstract **reference
  workload** a production percolator serves (per-query tags, filter-by-tag, two-stage recall+verify,
  ranking-as-presentation) and how Reverse Rusty maps to it (→ ADR-049, STATUS Tier 4).
- [`research/clustering-prior-art.md`](research/clustering-prior-art.md) — consistent-hashing variants,
  content-based routing, and the ES distributed-percolator contrast (clustering design; → ADR-027).
- [`research/dynamic-vocabulary.md`](research/dynamic-vocabulary.md) — absorbing new vocabulary after the
  dict is frozen (the Cluster v1 dynamic-vocab work, **built**: ES global ordinals, Vespa, RocksDB dict,
  feature hashing; → ADR-046).
- [`research/corpus-feature-learning.md`](research/corpus-feature-learning.md) — learning the feature
  extractor from the query corpus (NPMI).
- [`research/real-data-findings.md`](research/real-data-findings.md) — testing the normalizer against
  real eBay titles.
- [`research/multiword-synonyms.md`](research/multiword-synonyms.md) — design learnings from an
  abandoned multi-word-alias attempt (the token-graph vs flat-feature-set / forbidden-feature wall).

---

## Single source of truth (SSOT) registry

Each fact has exactly **one** canonical home. Everywhere else carries a one-line summary plus a link —
never a second copy. This is what keeps the docs from drifting.

| Fact | Canonical home | Everywhere else |
|---|---|---|
| Two-phase architecture diagram | [`design/README.md`](design/README.md) §1 | `../README.md` keeps a product-facing version; `../CLAUDE.md` a skeleton. Both link here. |
| Correctness contract (lossless cover) | [`design/README.md`](design/README.md) §2 | `../CLAUDE.md` inlines the one-sentence form (safety); others link. |
| Critical invariants | [`../CLAUDE.md`](../CLAUDE.md) | design docs cite the one invariant local to each + link. |
| Dependency versions | `engine/Cargo.toml` | `../README.md` lists crate *purposes* (no versions); docs never pin versions. |
| Performance numbers | [`performance/results.md`](performance/results.md) (exact) + [`performance/benchmark-results.txt`](performance/benchmark-results.txt) (invariants) | everywhere else rounds (`~710k`) and links. |
| Module map (file → purpose → ADR) | [`../CLAUDE.md`](../CLAUDE.md) | `design/README.md` §4 keeps a coarser design-topic↔module view + link. |
| Implemented vs design-only | [`STATUS.md`](STATUS.md) (one line per capability) | `../CLAUDE.md` keeps a 3–4 line skeleton + link. |
| Roadmap / next steps | [`roadmap.md`](roadmap.md) (open items only) | [`STATUS.md`](STATUS.md) keeps a one-line tier glance; "tracked in Tier N" refs resolve via either. |
| Completed-work narrative (what shipped, how, scope, proof) | the one ADR file in [`decisions/`](decisions/) | `STATUS.md` carries one line + the ADR number; `roadmap.md` deletes the item on ship. Never a second prose copy. |
| Architecture decisions / "why" | [`DECISIONS.md`](DECISIONS.md) index → one file per ADR in [`decisions/`](decisions/) | referenced by `ADR-NNN` (pointers, never copies). |
| Test count | `cargo test` | docs describe the suites qualitatively; no hand-maintained integer. |
| Testing / benchmark / CI workflow | [`testing.md`](testing.md) | `../CLAUDE.md` Build/test/run keeps the commands; CI rationale in [`DECISIONS.md`](DECISIONS.md) ADR-024; benchmark numbers in `performance/`. |
| REST API / query DSL | [`reference/api.md`](reference/api.md) index + [`reference/api/`](reference/api/) subfiles · [`reference/dsl.md`](reference/dsl.md) | `../README.md` links here instead of inlining. |

---

## Documentation conventions

Read before adding or moving docs. These rules are the only thing keeping a flat, duplicative wall of
text from growing back (the repo is maintained largely by an LLM agent, and there is no automated doc
link-checker in CI — the discipline has to live here).

- **Progressive disclosure.** `CLAUDE.md` (rails + router) → gateways → deep dives. Any fact should be
  reachable in ≤1 hop from a gateway. Don't make a reader open three files to answer one question.
- **Single source of truth.** Each fact has one owner (registry above). Elsewhere: a one-line summary
  + a section link, or nothing. Before adding a paragraph, ask "does this already live somewhere?" —
  if yes, link it.
- **Where new information goes:**
  - New architecture decision → a new `decisions/adr-NNN-slug.md` file (next number) + an index row in
    [`DECISIONS.md`](DECISIONS.md); **never renumber or delete** — superseded ones are marked, not removed.
  - Component/algorithm design → the matching `design/<topic>.md` (extend, don't fork).
  - "Is it built?" → [`STATUS.md`](STATUS.md) — **one line per capability**, the ADR carries the
    narrative. "What's next?" → [`roadmap.md`](roadmap.md) — **open items only**. When an item
    ships: add/extend the one STATUS line, **delete** the roadmap item (don't strike it through),
    and let the ADR be the permanent record. Design docs link here, they don't re-list.
  - Benchmark numbers → append a dated entry to [`performance/benchmark-results.txt`](performance/benchmark-results.txt)
    first, then narrate in [`performance/results.md`](performance/results.md).
  - Dependency version → `engine/Cargo.toml` only. Docs describe a crate's purpose, never its version.
  - A new `src/` file → update the module map in [`../CLAUDE.md`](../CLAUDE.md).
  - Testing / benchmark / CI workflow → [`testing.md`](testing.md) (the gate itself is `engine/check.sh`,
    which CI runs; decision rationale → [`DECISIONS.md`](DECISIONS.md) ADR-024).
  - User-facing API/DSL change → the matching [`reference/api/`](reference/api/) subfile (+ the
    [`reference/api.md`](reference/api.md) index/matrix) / [`reference/dsl.md`](reference/dsl.md).
  - Prior art / research → [`research/`](research/).
- **Numbers convention.** Round in prose (`~710k titles/sec`); keep exact figures (six-significant-figure
  throughputs, p99 latencies) only in `performance/results.md` and `benchmark-results.txt`.
- **No dangling links.** If you rename or remove a doc/section, `grep -rn 'old-name' --include='*.md'`
  and repoint or delete every reference in the same change.
- **The two sanctioned duplications — keep them, keep them in sync.** (1) The correctness contract +
  critical invariants are inlined in `CLAUDE.md` *and* canonical in `design/README.md` §2 — an agent
  must see them without a hop. (2) The two-phase diagram exists in product form (`../README.md`) and
  engineering form (`design/README.md` §1) for different audiences. Don't add a third copy of either,
  and if you change one, change its twin.
