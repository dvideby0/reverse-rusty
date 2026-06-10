# ADR-070: Cluster REST surface — the coordinator-mode server (Distributed-v1 criterion 1)

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted

- **Context.** [ADR-065](adr-065-distributed-v1-graduation.md) criterion 1 — the first of the three
  items that unblock testing everything else. The HTTP server fronts a **single-node `Engine` only**;
  the cluster is a library API (`ClusterEngine`) plus raw gRPC bins (`shardserver`, `controlserver`).
  Operating a cluster end-to-end — register a query, percolate a title, narrow by tags, change the
  vocabulary, checkpoint, watch health — requires embedding Rust. That is the single biggest usability
  gap to "every advertised feature can be exercised in a real multi-machine deployment": the
  multi-machine harness (criterion 3) and every later criterion want an HTTP surface to drive.

- **Decision.** A **coordinator mode** inside the existing `server` binary (`--cluster`), not a new
  binary: one REST dialect, two backends, with the auth / request-id / metrics middleware shared. The
  mode serves the existing API shapes over a `ClusterEngine`:

  1. **Backends.** In-process mode (default build): `--cluster --shards K [--replication-factor N]`
     builds (or, when `cluster_manifest.bin` exists, **reopens**) an in-process cluster — Cluster v1
     made operable. Remote mode (`--features distributed` build): repeatable
     `--shard-endpoint primary[,replica…]` flags front real `shardserver` nodes via
     `connect_remote`/`connect_replicated` (dict + tag-dict shipped at connect, ADR-034/055). A
     binary built without the feature fails loud on `--shard-endpoint`.
  2. **Concurrency model.** `Arc<RwLock<ClusterEngine>>` + a writer-serialization `Mutex<()>`.
     Percolates and writes take the **read** lock (`ClusterEngine` reads are `&self` lock-free;
     writes are `&self` and internally ordered by the cluster log's mutex) — writes additionally
     hold the serial mutex so bulk batches don't interleave, mirroring the single-node
     `Mutex<Engine>` model. Only the vocabulary paths (`set_vocab` / `learn_and_apply` / alias
     import+learn — `&mut self` blue/green rebuilds) take the **write** lock. Reads are never
     blocked by writes, only (briefly) by a vocab rebuild.
  3. **Cluster-atomic upsert (`PUT /_doc/{id}`).** A new single-frame
     **`ClusterMutation::Upsert { logical, version, dsl, tags }`** (clog **v3**, op 2; same payload
     layout as `Add`) gives the cluster ES `index` semantics — the ADR-067 move at the coordinator.
     Two frames (`Remove` + `Add`) would lose the *old* version if the process dies between the
     appends (replay stops after the `Remove`); one frame replays all-or-nothing. `apply_upsert`
     (live ≡ replay, the one funnel): parse + place **first** — a class-D / parse rejection returns
     without deleting (a failed replace never deletes, ADR-067 parity) — then tombstone the id on
     every shard and insert the new version on its placement shards. Partial failures ride the
     ADR-047 machinery (the queued mutation is the `Upsert`; re-driving it on a failed shard is
     delete + insert, idempotent). `POST /_bulk` maps each index action onto the same upsert.
  4. **Endpoint mapping (same shapes, honest deltas).** `/_search` + `/_mpercolate` resolve the same
     native + ES percolate envelopes (shared `resolve_percolate`) onto
     `percolate_filtered_with_stats`, and gain a **per-request `include_broad`** (the cluster routes
     the broad lane at the coordinator, so the per-shard toggle is free; single-node `/_search`
     parity is ADR-064 item 6, untangled from this). `GET /_doc/{id}` works on local clusters via a
     new `Shard::source_of` probe (remote: a clear 501, not a silent 404). New cluster-ops
     endpoints: `POST /_checkpoint` (the durability commit point), `GET /_cat/shards`,
     `GET /_cluster/state`, `POST /_cluster/nodes` / `DELETE /_cluster/nodes/{id}`,
     `POST /_cluster/rebalance`, `POST /_cluster/resync` (ADR-047). Vocab/alias admin maps onto the
     cluster's own `set_vocab` path — its built-in refusals (non-local / tagged / multi-word,
     ADR-046/055/061) surface as 400s with the engine's message, never weakened. Single-node-only
     surfaces answer **501 with the reason and the alternative** (`/_compact` → per-shard policy +
     `/_checkpoint`; `PUT /_settings` → static in cluster mode; `explain`/`profile-rank` →
     ADR-065 criterion 5) rather than silently degrading.
  5. **Library additions (lean core, additive).** `ClusterEngine::{upsert_query[_with_tags],
     get_source, learn_vocab, import_alias_synonyms, learn_aliases_and_apply,
     percolate_filtered_with_stats}` + introspection getters (`include_broad`, `replication_factor`,
     `is_durable`, `per_shard_config`, `tags_present`); `Shard::source_of` (default: a loud
     unsupported error, so a remote shard can never silently report "not found");
     `ClusterEngine::freeze_feature_space` (pass A of `build`, extracted) so remote-mode startup
     mints the same frozen dict + tag space `build` would.

- **Durability model by mode (recorded, not new).** In-process `--cluster --data-dir`: the ADR-031/032
  story — log-first writes, manifest commit at `/_checkpoint` (also run at graceful shutdown), reopen
  attaches segments + replays the log tail. Remote mode: the coordinator is **stateless** (in-memory
  log); durability lives on the shard nodes (per-shard translog + checkpoint sidecar, ADR-039) — a
  coordinator restart reconnects to the same endpoints and the corpus is still there (the dict is
  re-minted deterministically from the same `--load-file`, and live adds never mutate a frozen dict, so
  the fingerprint handshake holds; a populated server under a *divergent* dict still refuses loud).
  `--load-file` into a non-empty cluster is skipped with a warning (`ingest` requires an empty
  cluster), so a restart never double-loads.

- **Why this is safe (the correctness contract).** The mode adds **no new matching path**: every read
  is `ClusterEngine::percolate_*` — the routing + merge the cluster oracles prove ≡ single-node ≡
  brute (ADR-027/055) — and every write funnels through the same log-first `apply` the durability
  oracle proves replay-identical (ADR-031). The new `Upsert` frame is delete + insert through those
  same funnels; its placement is re-derived from the frozen dict on live apply and replay alike, so
  live ≡ replayed placement byte-identical. Tags never gate; the filter compiles once at the
  coordinator (ADR-055). The signature optimizer, candidate index, and verifier are untouched.

- **Scope / explicitly deferred.** Cluster ranking (`rank` block → criterion 5); explain over the
  cluster; per-request settings (`PUT /_settings`); TLS/auth on the *gRPC* hop (criterion 2 — the
  REST hop reuses ADR-062 bearer auth unchanged); replicate-broad-to-all (criterion 8 — class-D
  stays rejected at cluster placement, ADR-068's recorded boundary); a durable *remote* coordinator
  log (the shard nodes are the durable truth in remote mode).

- **Testing.** clog: `Upsert` round-trip + torn-tail + mixed-stream replay. Coordinator units:
  created/updated outcomes, re-PUT leaves exactly one live copy, an upsert that *moves* a query's
  anchor lands it on the new shard and removes the old, class-D / parse upserts never delete the
  prior version. Durability oracle: upsert survives checkpoint + reopen (replay ≡ live), including
  the moved-anchor case. Handler tests (the co-located server-test pattern) over an in-process
  cluster: doc CRUD + bulk + search/mpercolate (filtered + `include_broad`) + stats/health/shards +
  vocab put/learn + alias import + checkpoint + the 501 contracts. `check.sh` green (incl. the lean
  lane — the server stays behind the `server` feature; `distributed` remains off-default).

- **See also:** ADR-065 (the program; criterion 1), ADR-067 (single-node atomic upsert — the
  semantics this ports), ADR-031/032 (the durable funnel the upsert frame rides), ADR-047 (partial
  apply + resync), ADR-055 (tags through the cluster), ADR-062 (the REST auth gate reused verbatim),
  [`reference/api.md`](../reference/api.md) (the cluster-mode endpoint reference this adds).
