# ADR-098: Deployable Feature Complete — deployment matrix, local smoke gate, versioned release pipeline (Tier 5 M0–M2)

> [Back to the decisions index](../DECISIONS.md)

- **Status:** **Done (2026-07-02).** M0 + M1 shipped first (PR #96):
  [`operations/deployment-modes.md`](../operations/deployment-modes.md) +
  [`deploy/local-smoke.sh`](../../deploy/local-smoke.sh) wired into the required CI gate job.
  **M2 shipped in the follow-up PR** (the staged single-ADR pattern ADR-093 used):
  [`release.yml`](../../.github/workflows/release.yml) (tag-triggered, smoke-the-candidate-first,
  GHCR `{vX.Y.Z, sha-<short>}`), the `check-versions.sh` + `check-topology-parity.sh` guards
  wired per-PR, the production-compose smoke on the harness job's image per-PR, and the
  `k8s-smoke.sh` u64-id fix + its first end-to-end PASS.

- **Context:** The engine + distributed layers are built and oracle-proven, but everything
  deployment-shaped was folded into Distributed-v1 criterion 12 (the ≥20M scale proof, ADR-065) —
  there was no **"deployable feature complete" contract** distinct from "production-proven at
  scale" (the Tier 5 gap named by the 2026-06-24 deployability review). Concretely: the four
  deployment modes and the v1 non-goals were documented but scattered (two runbooks + STATUS +
  ADR-076/078/079); the two **local** modes — the ones a `cargo build` user actually runs — had
  **no smoke at all** (the Compose/Helm modes had scripts, two of them unwired); and there was
  **no versioned release**: no git tags, no image publishing, `:latest`-or-build-it-yourself only.
  The `k8s-smoke.sh` script had also never passed end-to-end (same latent `Path<u64>` id bug the
  Phase 0 run found in `cluster-smoke.sh`) — evidence that an unwired smoke rots.

- **Decision:**
  1. **One canonical supported-deployment contract** —
     [`operations/deployment-modes.md`](../operations/deployment-modes.md): the four-mode matrix
     (single-node · in-process cluster · remote Compose · remote Helm) with the exact bring-up
     command per mode, the guaranteed REST surface (`_doc`/`_bulk`/`_search`/`_mpercolate`/
     `_health`/`_stats`/`_metrics`/`_backup` + restart-reopen), the auth posture (ADR-062/071),
     and the **v1 non-goals consolidated into one named-constraints table** (RF>1-in-Helm, online
     resize, remote custom vocab, cross-shard backup barrier, scale proof, mTLS/per-RPC authz,
     power-loss default — each with its deciding ADR). The runbooks' scattered copies now point
     there; supported-deployment truth lives in exactly one place.
  2. **The M1 acceptance gate**: `deploy/local-smoke.sh` runs BOTH local modes end-to-end (start →
     assert an unauthenticated write is 401 → `_doc` + `_bulk` ingest → `_search`/`_mpercolate`
     incl. a MUST_NOT suppression and an any-of match → `_stats`/`_metrics` → `_backup` →
     **SIGTERM-restart-reopen and re-assert** → **open the backup copy and re-assert**), no
     containers, `curl`+`jq` only. It runs **inside the existing required `gate + benchmarks` CI
     job** (after `check.sh`, before the informational benchmarks) — a hard gate on every PR with
     zero branch-protection changes, reusing the hot release build. A separate job was rejected:
     not required-by-protection, cold cache. `check.sh` stays the *engine*-gate SSOT; this is a
     *deployment* gate over the built artifact, the harness-job precedent.
  3. **(M2, follow-up PR) A tag-triggered release pipeline** (`.github/workflows/release.yml`):
     on `v*` tags — version preflight (tag == `engine/Cargo.toml` == Chart `appVersion`, fail loud
     before compiling) → build the distributed bins natively → wrap `Dockerfile.prebuilt` →
     **smoke the exact candidate image** (Compose `cluster-smoke.sh` + kind `k8s-smoke.sh` +
     topology parity) → only then publish. A `workflow_dispatch` run is the full dry-run rehearsal
     (build + smoke, never push).
  4. **(M2) The image is the ONLY published artifact**: `ghcr.io/<owner>/reverse-rusty` tagged
     `vX.Y.Z` + `sha-<short>`. **No `:latest`** (an unpinned pull fails loud instead of silently
     floating — and it ends the ":latest-only" state the review flagged). **No crates.io publish**
     (`publish = false` stays) and **no GitHub-Release binary artifacts** — the repo may go
     private later (user decision, 2026-07-02); GHCR packages pushed from Actions inherit the
     repo's visibility, so that flip keeps the image private too.
  5. **(M2) Drift guards in per-PR CI**: `deploy/check-versions.sh` (crate == chart appVersion;
     the tag form adds the release preflight) and `deploy/check-topology-parity.sh` (the rendered
     Helm chart and `compose.cluster.yml` agree on shard/control counts, the coordinator's
     routing flags, and ports — a grep-level tripwire, not a semantic differ). The per-PR remote
     coverage: `cluster-smoke.sh` rides the existing harness job's prebuilt image (~30–60 s);
     `k8s-smoke.sh` stays **release-gate-only** (kind's flake budget is not worth per-PR; its
     marginal coverage over lint + kubeconform + parity + the compose smoke is small — revisit if
     a chart-behavior regression ever slips to tag time).

- **Findings (from building the M1 smoke).**
  - **Bulk-vs-live cost classification diverges on a micro-corpus — by design, now documented.**
    The smoke's first run failed: a bulk-ingested any-of query (`(a,b) c`) did not match by
    default, while the same query via `PUT /_doc` did. Mechanism: a bulk batch **finalizes the
    64-bit common mask from its own corpus** (`dict.finalize_mask()`, top-64 by frequency with no
    floor), so on a handful-of-queries corpus EVERY term is "hot" and the query classifies
    **class C** (broad lane, quarantined behind `include_broad`); the live path on a fresh dict
    (and the in-process cluster, whose frozen-empty dict makes every term synthetic and never
    masked) classifies the same query selective. Zero-FN is intact (the broad lane serves it with
    `include_broad: true`); the trap is *visibility on tiny corpora*. Disposition: documented in
    `deployment-modes.md` §2 + in the smoke itself; the smoke's recurring match probes pass
    `include_broad: true` (a pure superset — classification-independent across modes and
    restart/restore legs) with default visibility pinned once where it is deterministic. The
    ingest-time "this query landed in the broad lane" warning stays the recorded backlog item.

- **Consequences.**
  - "Deployable feature complete" is now a named, CI-enforced contract per mode — separate from
    the scale proof, which stays open as Tier 3 criterion 12 (explicitly NOT a blocker here).
  - The M1 smoke costs ~15 s + one incremental `cargo build` inside the gate job; the required
    check's name is unchanged, so branch protection needed no edit.
  - Docs consolidation only moves truth, it does not restate it: the constraints table links the
    deciding ADRs; the runbooks link the table.
  - (M2) Releases become rehearsable without a tag (`workflow_dispatch`) and nothing publishes
    unless the exact candidate image passed both remote smokes + parity.

- **Alternatives considered.**
  - **A separate CI job for the local smoke** — rejected: it would not be required by branch
    protection (the required check is the `gate + benchmarks` job name) and would pay a cold
    build for zero benefit.
  - **Running `k8s-smoke.sh` per-PR** — rejected for kind's flake budget vs. small marginal
    coverage; recorded as revisitable.
  - **Publishing `:latest`** — rejected: fail-loud pinning beats a silently floating tag.
  - **GitHub Release binary tarballs / crates.io** — rejected per the may-go-private decision;
    the container image is the deployable unit.
  - **Per-mode expected values for the classification-sensitive probe** (assert the single-node
    class-C quarantine exactly) — rejected: it would couple the deployable-surface smoke to
    classification internals nobody promised (e.g. a future frequency floor in `finalize_mask`
    would break it); the smoke pins default visibility once where it is deterministic instead.

- **Deferred follow-ons.** Multi-arch (arm64) images; a distroless/`curl`-free base (ADR-089
  carry-over); image/chart signing (cosign) + SBOM; publishing the Helm chart itself (OCI);
  the real-cluster adversarial deploy proof (Phase 0 item 4 — needs a real cluster + corpus).
