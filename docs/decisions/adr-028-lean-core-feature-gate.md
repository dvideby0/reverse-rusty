# ADR-028: Feature-gate the server/observability stack behind a default-on `server` feature (lean core)

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted


- **Context:** The library crate unconditionally compiled the full HTTP/observability stack
  (`axum`, `tokio`, `clap`, `parking_lot`, `tower`, `uuid`, `tracing`, `tracing-subscriber`,
  `prometheus`) even for pure-engine embeddings and the engine-only CLI bins — STATUS flagged this as a
  build-hygiene gap (compile time, binary size, supply-chain surface). It also became *timely*: the next
  increment (gRPC `ShardServer`, ADR-029) adds `tonic`/`prost` — a heavy, network-only dependency that
  needs a clean home behind a feature, not bolted onto the always-on surface. A usage audit confirmed
  all nine crates are imported **only** in `src/bin/server.rs`; none leak into the library.
- **Decision:** Mark the nine crates `optional = true` and gather them under a **`server` feature**, with
  **`default = ["server"]`** so every documented command (`cargo build --release`,
  `cargo run --release --bin server`, `cargo test --release`) behaves exactly as before. The server bin
  carries `required-features = ["server"]`, so under `--no-default-features` Cargo skips it and its
  `use axum::…` never compiles — meaning **zero `#[cfg]` attributes are needed in code**; the gating is
  entirely at the Cargo-manifest level. `serde`/`serde_json` stay **core** (Vocab JSON persistence,
  `EngineConfig` Serialize, `ExplainDetail`, and the JSONL loader are all library code). A new
  `check.sh` lane — `cargo clippy --no-default-features --release -- -D warnings` — enforces that no
  server-only crate ever creeps back into library code (it would fail the lean lint).
- **Why default-on, not lean-by-default:** preserving the documented commands and the green gate is the
  win; the dependency-hygiene guarantee comes from the enforcement lane, not from which feature set is
  the default. The lean core is one flag away (`--no-default-features`) and is continuously verified.
- **Alternatives considered:**
  - *Lean-by-default (`default = []`)* — rejected; forces `--features server` onto every server
    build/run/test and churns every build command in CLAUDE.md + docs, for no benefit the enforcement
    lane doesn't already provide.
  - *Gate the engine-only bins too (bench/demo/clusterdemo/learn/segbench/snapbench/norm behind a `cli`
    feature)* — unnecessary; they use only core deps, so they add nothing to the dependency tree.
  - *Make `serde`/`serde_json` optional as well* — rejected; they are genuine library dependencies.
- **Consequence:** `cargo build --no-default-features` yields the lean embeddable core (daachorse,
  memmap2, rayon, roaring, arc-swap, serde, serde_json + transitives); the full server remains the
  default build. No runtime or behavior change. This is the clean seam beside which ADR-029's
  `distributed` (gRPC) feature slots — tonic/prost land off-by-default without touching the core surface.
- **See also:** ADR-007 (the original three-production-deps philosophy this extends), ADR-029 (gRPC
  `ShardServer` — the `distributed` feature that reuses this seam), [`STATUS.md`](../STATUS.md),
  [`engine/Cargo.toml`](../../engine/Cargo.toml) (authoritative pins + feature defs),
  `engine/check.sh` (the lean-core lane).

