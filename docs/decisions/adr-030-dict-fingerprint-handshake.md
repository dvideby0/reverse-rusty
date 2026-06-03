# ADR-030: Dict-fingerprint handshake + fallible cluster construction (ADR-029 sharp-edge closure)

> [Back to the decisions index](../DECISIONS.md)


- **Status:** Accepted.
- **Context:** ADR-029 shipped the gRPC transport with five documented "known sharp edges." Two were
  correctness gaps with cheap, already-flagged mitigations: (1) unchecked cross-process dict identity could
  drop matches *silently* — the one false-negative path the fallible seam cannot catch — and (2) the
  11-field `MatchStats` wire map in `cluster/proto.rs` was untested, so a field transposition would go
  undetected. Two smaller issues sat alongside them: cluster *construction* still used `assert!`/`panic!`
  (against the no-panic-in-library rule, ADR-005), and `ClusterEngine::ingest` silently re-indexed
  (duplicated) entries if called on an already-populated cluster. This ADR records closing all four, plus a
  test-only flake fix.
- **Decision:**
  - **Dict-fingerprint handshake.** `Dict::fingerprint()` is a stable `fnv1a64` over the
    *correctness-relevant* content only — the `name→id` mapping (names in id order), each feature's kind and
    common-mask bit, and the `finalized` flag. `freq` is excluded: its sole match-relevant effect (which
    features get a mask bit) is already captured by `mask_bit`, so hashing it would flag false mismatches.
    A new `DictFingerprint` RPC lets `RemoteShard::connect` fetch the server's fingerprint and compare it to
    the coordinator's; a mismatch returns the new `ShardError::DictMismatch` instead of connecting. This
    turns the silent-FN path into a loud connect-time failure. It does **not** ship the dict (servers must
    still be built over the same feature space) — full dict-shipping stays deferred (ADR-029 out-of-scope;
    **shipped in ADR-034**).
  - **Fully-fallible construction.** `HashRing::new`, `ClusterEngine::from_parts`, `build`, and
    `connect_remote` now return `Result<_, ShardError>`, replacing the four construction `assert!`s with the
    new `ShardError::Config`. Chosen over a boundary-only conversion (which would leave `build` infallible):
    the no-panic rule applies to all library construction, and the caller ripple is tests/bins only.
  - **`ingest` re-entry guard.** `ClusterEngine::ingest` errors with `ShardError::Config` on a non-empty
    cluster rather than silently duplicating; its documented contract was always "a freshly assembled
    (empty) cluster" (use `add_query` for incremental adds).
  - **`MatchStats` wire test.** A `proto.rs` unit test asserts the map by field name in *both* directions
    with 11 distinct values (catching a symmetric transposition a pure round-trip would miss); the gRPC
    oracle additionally asserts the gRPC cluster's merged stats equal an in-process cluster's, per title.
  - **gRPC-test port-race fix.** The oracle binds each shard's ephemeral port exactly once via tonic
    `TcpIncoming` + a new `ShardServer::serve_with_incoming`, removing the bind→drop→rebind window that
    could flake CI.
- **Consequence:** ADR-029 sharp edges (1) and (2) are closed; a negative oracle test
  (`grpc_connect_rejects_divergent_dict`) proves the handshake fires on divergence. Cross-process gRPC is
  now correctness-*safe* (a divergent dict fails loud) though still not a full deployment — TLS/auth (edge
  3) and dict-shipping remain open. No hot-path or lean-core change: the fingerprint is connect-time only,
  and every edit lives in the cluster module / `distributed` lane.
- **See also:** ADR-029 (the edges this closes), ADR-005 (typed errors / no panics in library code),
  ADR-027 (the in-process core), `src/dict.rs` (`fingerprint`),
  `src/cluster/{shard,ring,coordinator,remote,server,proto}.rs`, `engine/grpc/proto/shard.proto`,
  `tests/cluster_grpc_oracle.rs`, `tests/cluster_oracle.rs`.


