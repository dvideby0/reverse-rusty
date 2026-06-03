# ADR-031: Externalized single-node coordinator mutation log behind `trait ClusterLog` (clustering step 3a)

> [Back to the decisions index](../DECISIONS.md)


- **Status:** Accepted.
- **Context:** ADR-027/029/030 built the in-process multi-shard core and its gRPC transport, but the
  coordinator had *no durability of its own*: live `add_query`/`remove_query` mutations existed only in
  shard memtables, so a coordinator restart lost every post-build write and there was no single ordered
  source of truth the whole cluster could be rebuilt from. `clustering-and-scaling.md` §10 step 3 is
  "externalize the mutation log (start with a single-node WAL, then Raft) and make segments loadable from a
  shared path." This ADR records the **first sub-step (3a) only**: a durable, ordered, append-only log of
  cluster mutations so the entire cluster is rebuildable from the log alone. The shared-path/object-store
  half of step 3 and all of Raft (step 4) stay design-only.
- **Decision:**
  - **A `trait ClusterLog` seam, not a concrete type** — mirroring the proven `trait Shard` local↔remote
    idiom. Two impls ship now: `FileClusterLog` (durable, CRC-framed) and `NullClusterLog` (in-memory: the
    no-`data_dir` path *and* a fast test backend). Both are exercised today, so the trait earns its keep on
    present need, not speculation, and they yield a differential test — `NullClusterLog ≡ FileClusterLog`
    proves coordinator behavior is log-impl-independent. The Raft-backed log drops in behind the same seam
    later (`append`→quorum-commit, `replay`→committed prefix, `checkpoint`→snapshot-install, epoch→term).
  - **Logical-id + raw DSL granularity.** Each `Add` logs `(logical_id, version, dsl)`, each `Remove` logs
    `logical_id`. Raw DSL — never compiled form — is the source of truth (the ADR-029 DSL-on-wire
    invariant), so replay recompiles against the manifest's frozen dict and re-derives placement through the
    existing `anchor_plan` path. Dict (fingerprint-checked) + ring (deterministic) ⇒ recovery reproduces the
    original placement exactly, so no shard boundary drops a match across a restart — the lossless-cover
    argument extended over a crash.
  - **A single `apply(mutation)` funnel.** Live writes and replay flow through one private apply path (the
    Raft state-machine `apply` in disguise), so replay reproduces live application by construction — they
    cannot drift. Write paths are **log-first / fail-closed**: `add_query`/`remove_query` append to the log
    *before* touching any shard; on append failure they emit `DurabilityFailure{WalAppend}` and return the
    error with shards untouched (the engine's WAL-first contract, ADR-017, lifted to the coordinator).
  - **Coordinator-level snapshot, not per-shard-Engine durability.** The base snapshot is the coordinator's
    distinct live set `logical → (version, dsl)` (reusing the `sources.dat` v2 shape + a version column),
    with the frozen dict stored *once* in a `ClusterManifest`. This is the "log is the database" shape (§4.1):
    `ClusterEngine::open` reads the manifest → fingerprint-checks the dict → re-derives the ring →
    bulk-rebuilds shards from the snapshot → replays the log tail through `apply`. (Per-shard segment
    durability — "segments loadable from a shared path" — is the *other* half of step 3, deferred to 3b.)
  - **New `clog.rs` framing, not the existing `Wal`.** `Wal`'s `OP_TOMBSTONE` is per-shard
    `(seg_idx, local_id)` and `Wal::parse_entries` treats unknown ops as a torn tail, so reusing the `Wal`
    *type* for logical-id ops is subtly broken. `clog.rs` copies the proven framing/CRC/torn-tail/fsync
    pattern (ADR-013) into a separate file with logical-level ops — a cluster log and an engine WAL can never
    be confused. `checkpoint()` writes a fresh base snapshot + new manifest (the atomic commit point) *before*
    truncating the log, so a crash mid-checkpoint just replays an already-applied (idempotent) tail.
- **Consequence:** An in-process cluster created with a `data_dir` survives a crash: `ClusterEngine::open`
  reconstructs byte-identical placement (zero false negatives) from manifest + base snapshot + replayed log,
  proven by `tests/cluster_durability_oracle.rs` (rebuild ≡ pre-crash ≡ brute across K∈{1,3,8} × broad
  on/off, plus checkpoint-compaction, torn-tail recovery, append-fails-closed, the two-backend differential,
  fsync parity, and fail-loud guards on a missing/corrupt manifest). Dependency-free (lean core, **not**
  behind `distributed`); the `NullClusterLog` path is byte-identical to pre-ADR-031, so `tests/cluster_oracle.rs`
  is unchanged. `LogPos` and `epoch` are **plumbed but not enforced** — both are needed now (replay cursor;
  checkpoint generation) and merely *shaped* like their Raft counterparts. **Deliberately deferred** (dead
  surface without Raft): per-entry epoch fencing, quorum / read-your-writes append modes, per-shard logs,
  object-store snapshots, and cross-process coordinator durability (`connect_remote` uses an in-memory log
  this increment). *(Amended by **ADR-033**: the "object-store" framing is dropped — the cluster is
  **shared-nothing** (local segments + per-node/coordinator WAL); object storage, if ever added, is only an
  optional pluggable backup target, never the serving path.)*
- **See also:** ADR-027 (the in-process core this extends), ADR-029 (the DSL-on-wire invariant replay relies
  on), ADR-030 (the dict-fingerprint check reused on `open`), ADR-013 (the engine WAL whose framing this
  copies), ADR-017 (the durable all-or-nothing ingest contract), ADR-021 (the `DurabilityFailure` event
  reused), `clustering-and-scaling.md` §10 step 3, `src/cluster/clog.rs`, `src/cluster/coordinator.rs`,
  `src/storage.rs` (cluster manifest + snapshot), `tests/cluster_durability_oracle.rs`.

