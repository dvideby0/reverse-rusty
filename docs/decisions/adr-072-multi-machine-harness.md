# ADR-072: The multi-machine test harness — containers, kill-and-recover, handoff under load (Distributed-v1 criterion 3)

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted

- **Context.** [ADR-065](adr-065-distributed-v1-graduation.md) criterion 3, the last of the three
  that unblock everything else. Every distributed layer is oracle-proven **in-process / on
  localhost** — which structurally cannot catch the failure class real deployments hit: separate
  network namespaces, real process lifecycle (SIGKILL, restart, reconnect), connection
  re-establishment after a peer dies, container filesystems, and startup ordering. The localhost
  oracles assume none of those go wrong; the harness exists to exercise exactly them. ADR-065 also
  demands the harness be **CI-runnable**, so every later criterion lands with harness coverage.

- **Decision.** A compose-based harness under `deploy/`, runnable locally and in CI, exercising the
  **fully secured** stack (ADR-071 TLS + mesh token — the harness never tests a config v1 would not
  ship) through the **public surfaces only** (the ADR-070 coordinator REST + the node bins — no
  test-only hooks):

  1. **`deploy/Dockerfile`** — multi-stage: a `rust:1.95` builder compiling
     `server`/`shardserver`/`controlserver` with `--features distributed`, and a `debian:slim`
     runtime carrying just the three binaries. One image, role chosen by command — the same image
     criterion 10's packaging ships.
  2. **`deploy/compose.harness.yml`** — the cluster under test on an isolated bridge network: K=3
     durable `shardserver` nodes (each its own volume, TLS identity + mesh token), a
     **coordinator** (`server --cluster --shard-endpoint …` over the secured mesh, REST published
     to the host), and a 3-node `controlserver` quorum (durable, secured both directions) — every
     link crosses a real container network boundary.
  3. **`deploy/harness.sh`** — the driver, asserting through REST what the localhost oracles assert
     through the library:
     - **Baseline:** generate an ephemeral CA/identity (never committed), bring the stack up, bulk
       a corpus through `/_bulk`, snapshot percolate results for a probe set.
     - **Kill-and-recover:** `SIGKILL` a shard node → a percolate that routes to it answers
       **502 fail-loud** (never a silently truncated union — the zero-FN posture observable from
       outside); restart the node → its durable shard self-restores (ADR-039) and the coordinator's
       channel reconnects → percolates ≡ baseline, including a live write accepted before the kill.
     - **Rolling restart:** restart every shard node in sequence → ≡ baseline.
     - **Coordinator restart:** kill + restart the (stateless) coordinator → it re-mints the
       identical dict from the same corpus file, the ADR-034 fingerprint handshake holds against
       the populated shards, the `--load-file` skip fires → ≡ baseline.
     - **Handoff under load:** a write loop runs against the coordinator while
       `POST /_cluster/handoff` (new — see below) moves a shard position from its owner to a fresh
       pending node across the container boundary; the loop tolerates only the documented brief
       fence-window rejections, and the moved cluster answers ≡ a freshly computed baseline
       (zero-FN across a live cross-node move, ADR-044's oracle re-proven over real infrastructure).
     - **Control-plane lifecycle:** rolling-restart the controlserver quorum; all three resume
       listening from their durable Raft state (the full kill-the-leader re-election assertion
       stays with the localhost oracle, which drives it through the library — the harness proves
       the *process lifecycle*, the oracle the *protocol*).
  4. **CI:** a separate `multi-machine harness` job in `ci.yml` (PRs + main, like the gate): builds
     the bins natively on the runner, wraps them in a prebuilt-binary image variant
     (`Dockerfile.prebuilt`) to skip the in-container compile, and runs `harness.sh`. The harness
     is also runnable locally with Docker (`deploy/harness.sh`), where the image builds from
     source.

- **`POST /_cluster/handoff`** (the one server addition): `{position, source, target}` →
  `ClusterEngine::execute_handoff` on the blocking pool, answering the new generation — the
  operator surface the live data-moving handoff (ADR-044/048) was missing (it was library-only,
  unreachable from a deployment). Distributed-gated; a non-`distributed` server answers the
  standard 501-with-reason. Errors surface with the engine's message (a non-converging source
  aborts fail-closed + auto-unfences, ADR-048 — the harness asserts the cluster still serves).

- **Why this is safe.** The harness adds no matching/placement code — one REST endpoint over an
  existing oracle-proven mechanism, deployment files, and a shell driver. Its assertions are
  end-to-end black-box invariants (fail-loud on a dead shard; restart-stability; ≡-baseline after
  every lifecycle event), so it can only *find* bugs, not mask them.

- **Scope / explicitly deferred.** Multi-host (non-compose) topologies — the network boundary is
  real but single-machine; cross-host is a deployment of the same images (criterion 10's runbook).
  Fault injection beyond kill/restart (network partitions, latency, disk-full) — valuable, post-v1.
  The control-plane → coordinator wiring (the REST coordinator still runs the in-memory control
  plane; attaching it to a controlserver quorum is part of the deployment-model maturation noted in
  ADR-048's scope).

- **See also:** ADR-065 (the program; criterion 3), ADR-070 (the REST surface the harness drives),
  ADR-071 (the secured mesh it runs on), ADR-044/048 (the handoff the new endpoint exposes),
  ADR-039 (the durable self-restart kill-and-recover proves), criterion 10 (packaging — shares the
  Dockerfile), [`testing.md`](../testing.md) (how to run it).
