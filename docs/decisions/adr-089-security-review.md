# ADR-089: Security review — threat model + container image scan (Phase 0 item 5)

> [Back to the decisions index](../DECISIONS.md)

- **Status:** **Done (2026-06-25).** A new threat-model doc
  ([`operations/threat-model.md`](../operations/threat-model.md)), a container-scan wrapper
  (`deploy/scan-image.sh`) with a dated baseline + triage, and a documented disposition of the
  `_backup` destination-path finding. No engine-code change.

- **Context:** This is **Phase 0, item 5** of the reality/adversarial audit. The security *controls*
  already exist and are individually well-documented — opt-in bearer auth (ADR-062), mesh TLS + a
  shared cluster token (ADR-071), transport hardening (ADR-085), backup safety (ADR-079), and a
  `cargo audit` + `cargo deny` dependency gate (ADR-028) — but there was **no single threat model** tying
  them to trust boundaries and adversaries, **no container image scan**, and no written disposition of
  the one concrete sharp edge a code read surfaced (the `_backup` handler converts a client-supplied
  destination string straight to a `PathBuf` with no normalization). The Phase 0 directive is to prove
  what is real and name the residual risks; for security that means a written model, an actual scan, and
  an honest list of v1 non-goals.

- **Decision:**
  1. **A threat-model document** (`operations/threat-model.md`) enumerating the four trust boundaries
     (REST client↔server, coordinator↔mesh, process↔host, build↔supply-chain), the assets (availability
     = zero-false-negatives-or-loud-failure, corpus integrity, in-transit confidentiality, host
     integrity), the controls per boundary mapped to `file.rs`, the adversary model, and — explicitly —
     the **v1 non-goals** (no mTLS, no per-RPC authorization tiers, single shared tokens, the
     experimental/localhost-proven distributed layers, power-loss beyond the WAL fsync policy). It
     consolidates the previously-scattered operator security checklist.
  2. **Container image scanning** via **Trivy**, wrapped by `deploy/scan-image.sh` (prefers a local
     `trivy`, else the `aquasec/trivy` image over the Docker socket; `--full` for all severities,
     `--strict` to fail on HIGH/CRITICAL). The **baseline** scan is recorded + triaged in the threat
     model. Trivy was chosen as the de-facto standard, zero-install (runs as a container), and able to
     scan a local image without a registry push.
  3. **The `_backup` path finding is dispositioned as operator responsibility**, not a forced code
     change: the endpoint is already auth-gated (default-deny on non-GET), the destination is
     **operator-chosen by design** (a hard jail would break the legitimate use case), and the blast
     radius is bounded by running the process **non-root** (the shipped image already runs as uid 10001
     with only `/data` writable). A config-driven allowlist/jail root is recorded as an **optional
     deferred hardening**.

- **Findings.**
  - **Container scan baseline (2026-06-25):** `reverse-rusty:latest` on `debian:trixie-slim` (13.5) →
    **234 findings (2 CRITICAL, 14 HIGH, 58 MEDIUM, 97 LOW, 63 UNKNOWN)**, **all in OS packages of the
    Debian base, zero in the three Rust binaries.** Both CRITICAL (and most HIGH) are in `perl-base`
    (perl-Archive-Tar path traversal, perl-IO-Compress RCE) plus `curl`/`util-linux`/`bzip2`/`glibc`,
    predominantly Debian `fix_deferred`/`affected` (no upstream fix yet). **None are reachable by the
    service** — the matcher never runs perl, extracts tar, does SMB, or mounts loop devices; `curl` is
    present only for the container healthcheck. Disposition: base-image hygiene, tracked on a rebuild
    cadence; the structural reduction (a distroless / `curl`-free base) is a recorded deploy-image
    follow-on. The application's own dependency surface is `cargo audit`/`deny`-clean.
  - **`_backup` destination path:** no normalization on a client-supplied, auth-gated, operator-named
    path — dispositioned above (document + non-root + deferred optional jail).
  - **No new code-level vulnerability found.** The auth fail-loud rules (incl. the non-UTF-8 fail-open
    fix), the constant-time token comparisons, the default-deny interceptors, and the fail-closed mesh
    transport all hold up under review.

- **Consequences.**
  - The project now has a written threat model, a repeatable container scan with a triaged baseline,
    and an explicit, honest list of v1 security non-goals — closing the Phase 0 item-5 gap.
  - Docs-and-tooling only: no engine-code change; lean/server/distributed builds are byte-identical.
  - The scan is **advisory by default** (it is not wired as a blocking `check.sh` lane): the base-image
    CVEs are real but currently unfixable upstream and unreachable by the service, so gating the build
    on them would be noise. `cargo audit`/`deny` remain the *blocking* dependency gate; the image scan
    is a cadence check an operator runs on each image refresh.

- **Alternatives considered.**
  - **A hard path jail on `_backup`** (rejected as a forced change): would break the operator-chosen
    destination that is the feature's point; the auth gate + non-root account already bound it. Left as
    an optional config-driven allowlist.
  - **Wiring the image scan into `check.sh` as a blocking lane** (rejected): the base image always
    carries unfixable-now CVEs unreachable by the service, so a blocking gate would be permanently red
    and trained-to-ignore. Advisory + a `--strict` mode for a deliberately-pinned clean base instead.
  - **A distroless / static base image now** (deferred): would drop most of the base surface, but
    trades away the `curl` the Compose healthcheck uses and is a larger deploy-image change — recorded
    as a follow-on, not in this review.

- **Deferred follow-ons.** A more minimal (distroless / `curl`-free) base image; an optional
  `_backup` destination allowlist/jail; mTLS + per-RPC authorization tiers for the mesh (the ADR-071
  post-v1 items); building the binaries `cargo auditable` so the image scan also covers the Rust
  dependency graph from the binary.
