# ADR-022: ES-style runtime settings API (`GET/PUT /_settings`)

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted


- **Context:** Every engine tuning knob (`EngineConfig`) was fixed at process start from CLI flags.
  Changing compaction/flush cadence or query-complexity limits meant a restart — and there was no way to
  *introspect* the live config at all. Operators expect a settings surface like Elasticsearch's
  `GET/PUT /_cluster/settings` / `GET /<index>/_settings`: read the effective config, and update the
  *dynamic* subset at runtime, with the *static* (node/index-creation) settings rejected. The user asked
  for "flexible configuration with a familiar (ES-style) interface."
- **Decision:** Add `GET /_settings` and `PUT /_settings`, borrowing ES *concepts* (dynamic-vs-static,
  `include_defaults`, `acknowledged`) while keeping our existing `ApiError` envelope rather than copying
  ES's verbose error body.
  - **`GET /_settings`** returns the live config as JSON (the `EngineConfig` field names *are* the
    setting keys, so GET output round-trips into PUT input). `?include_defaults=true` also returns
    `EngineConfig::default()`. It reads the **lock-free snapshot**, not the engine mutex — so the config
    now rides in `EngineSnapshot` as `Arc<EngineConfig>` (the `Engine` holds `Arc<EngineConfig>`,
    `Arc::clone`d into each snapshot — O(1) per publish, copy-on-write via `set_config`). This is the
    same pattern as the vocab snapshot fix and keeps *all* read endpoints off the write lock (ADR-016).
  - **`PUT /_settings`** takes a **flat JSON patch** (`{"max_segments": 16}`). A pure
    `apply_settings_patch(cfg, patch)` enforces, per key: dynamic (applied), static (rejected: "setting
    [X] is not dynamically updateable"), unknown (rejected: "unknown setting [X]"), wrong JSON type
    (rejected), then runs `EngineConfig::validate()` for range checks. **All-or-nothing**: every key is
    checked and *any* problem rejects the whole request with all reasons, so a bad key never half-applies
    (matches ES). On success it `set_config`s the validated clone and republishes the snapshot.
  - **Dynamic** (re-read on the next maintenance/compile decision): `max_segments`,
    `holes_ratio_threshold`, `memtable_flush_threshold`, `auto_compact_on_flush`,
    `auto_compact_on_ingest`, `max_query_length`, `max_query_clauses`, `max_anyof_group_size`,
    `compaction_fixed_cost`. **Static** (bound at construction — the data dirs, WAL fsync policy, and
    source-store mode are already established; changing them at runtime is unsafe or meaningless):
    `data_dir`, `wal_sync_on_write`, `retain_source`.
  - **Transient semantics:** updates are **in-memory only** — the startup CLI flags remain the durable
    source, so a restart reverts them. The PUT response says `"persistent": false` so clients aren't
    surprised. (ES historically had transient settings too.) Persisting overrides to a
    `data_dir/settings.json` is deferred — see below.
- **Consequence:** Operators can tune the live engine and read its effective config without a restart,
  through a familiar interface, with precise per-key errors. The pure patch function is unit-tested
  directly (dynamic apply, static/unknown/type/range rejection, all-or-nothing) without the HTTP layer;
  an integration test covers snapshot-carries-config + copy-on-write immutability; the change was also
  verified end-to-end (GET, `include_defaults`, valid PUT round-trip, and the three rejection paths). No
  match semantics change, so the oracle is unchanged. `EngineConfig` gains `Serialize` (its fields are
  serde-friendly); the library does **not** gain `Deserialize` — PUT uses the flat-patch path so the
  dynamic/static policy lives in the server, not the type.
- **Deferred:** (1) **persistent settings** — write dynamic overrides to `data_dir/settings.json` and
  re-apply on `open`, so a tuned node survives restart (the ES "persistent" tier); (2) **server-level
  settings** — `slow_query_threshold_ms` and `include_broad` live in `AppState`, not `EngineConfig`;
  exposing them via `/_settings` needs an atomic/lock around those fields (they're currently set once at
  startup); (3) a config **file** loader (elasticsearch.yml-style) layered under the CLI flags.
- **See also:** ADR-016 (lock-free snapshot reads — this puts the config there too), the vocab snapshot
  fix (same `Arc`-in-snapshot pattern), `config.rs` (`EngineConfig` + `validate`), `STATUS.md` (the
  feature-gating and ops-ergonomics backlog this sits alongside), ADR-025 (the follow-up that actually
  wired the three query-complexity limits into the parser — they were classified *dynamic* here before
  they were enforced anywhere).

