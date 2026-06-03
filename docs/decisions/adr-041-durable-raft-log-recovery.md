# ADR-041: Durable Raft log + control-plane restart recovery (clustering step 5e)

> [Back to the decisions index](../DECISIONS.md)


- **Status:** Accepted.
- **Context:** ADR-038 shipped the openraft control-plane backend with an **in-memory** `RaftLogStorage`
  + `RaftStateMachine` — enough to prove consensus convergence (the 3-node oracle), but a manager node
  lost its entire Raft state on restart, so it could not actually rejoin a quorum after a crash. ADR-038's
  own scope note flagged "a durable Raft log (CRC-framed, reusing `storage::crc32`)" as the deferred
  follow-on. This is that follow-on: the byte-level durable substrate that makes a `controlserver` node
  survive a restart.
- **Decision (`src/cluster/control_store.rs` + the durable mode in `control_raft.rs`):**
  - **What openraft actually requires durable (0.9.24 storage FAQ), and only that.** The vote (election
    safety — two leaders in one term if lost), the log entries (so committed-but-un-snapshotted entries can
    replay), the **committed** log id (`save_committed`, so a restart re-applies `(snapshot.last, committed]`),
    and the state-machine **snapshot** (so the log can be `purge`d and the SM rebuilt). The state machine
    itself is **NOT** persisted per-apply — openraft rebuilds it on restart from the latest snapshot + the
    durable log replayed up to `committed`. So `apply` stays a pure in-memory `control::apply` (unchanged
    from ADR-038 ⇒ live ≡ replay preserved), and the durable cost is one fsync per low-rate control op, never
    per query.
  - **Two on-disk shapes, reusing proven patterns** (`control_store.rs`): a **CRC-framed append-only record
    log** (`append_record`/`read_records`/`rewrite_records`) for the Raft entries — the same forward-scan /
    torn-tail recovery shape as `clog`/`wal.rs` (a crash mid-append drops the last partial frame, never
    corrupts an acknowledged prefix), `truncate`/`purge` are an atomic rewrite + reopen; and **atomic
    single-value files** (`write_value`/`read_value`, tmp + fsync + rename + parent-fsync) for the
    vote / committed / last-purged / snapshot. Serialization is `serde_json` (the SAME codec the gRPC
    `RaftNetwork` already uses — every persisted type, incl. `Entry<TypeConfig>` and `SnapshotMeta`, is
    already serde for the wire); CRC via the core `storage::crc32`.
  - **One backend, two modes, selected by a dir.** `LogStore`/`StateMachine` gained `in_memory()` (the
    ADR-038 path — the in-process oracle, **byte-identical**) and `open(dir, fsync)` (durable). `build_node`
    takes `Option<&Path>`; `in_process_cluster` passes `None` (oracle stays in-RAM); `start_grpc_node` (and
    the `controlserver --data-dir` flag) pass a dir → durable, fsync on. The seam (`trait ControlPlane`) and
    every coordinator call site are unchanged.
  - **Restart is idempotent.** A durable node, rebuilt over the same dir, loads vote+log+committed+snapshot,
    re-elects from its persisted vote, and openraft replays the committed tail into the SM. A new
    `RaftControlPlane::shutdown()` cleanly joins the core so the files are released before a restart from the
    same dir; `initialize` returns `NotAllowed` on an already-formed cluster (ignored), so the same builder
    serves first-boot and restart.
- **Honest scope.** A genuine multi-process *rolling* restart (kill one of three live gRPC managers, restart
  it, watch it rejoin) is exercised in spirit by ADR-038's `grpc_three_node_survives_leader_failure` plus
  this step's single-node durable restart; an end-to-end durable-3-node-rolling-restart harness is a deferred
  test (the durability mechanism is proven, the multi-process orchestration is heavier). No log-size/age
  compaction *policy* beyond openraft's own snapshot+purge cadence. TLS/auth on the control transport remains
  deferred (plaintext localhost), as does an allocator acting on the shard→node map (ADR-042).
- **Consequence:** A `controlserver --data-dir` is now a real durable cluster-manager: it survives a crash,
  resumes its committed cluster-state document, and rejoins the quorum. The default in-memory path (oracle +
  any embedding that passes no dir) is byte-identical to ADR-038, so every prior control-plane oracle is
  unchanged and green. Proven by `tests/cluster_control_raft_oracle.rs::durable_node_recovers_committed_document_after_restart`
  (commit a document → `shutdown` → rebuild from the same dir → the committed membership/assignments/model
  survive AND a fresh write still commits) + `control_store.rs` unit tests (log round-trip, torn-tail drop,
  prefix/suffix rewrite, value round-trip). Full `check.sh` green. All `distributed`-gated — the lean core
  never compiles openraft or this store. No new dependency (reuses `serde_json` + `storage::crc32`).
- **See also:** ADR-038 (the openraft backend whose in-memory store this makes durable), ADR-037 (the seam,
  unchanged), ADR-031/039 (the CRC-framed-log + torn-tail pattern this mirrors), ADR-013 (the engine WAL whose
  framing lineage this shares), ADR-033 (shared-nothing — local durable state, no object store),
  `src/cluster/{control_store,control_raft}.rs`, `src/bin/controlserver.rs` (`--data-dir`).

