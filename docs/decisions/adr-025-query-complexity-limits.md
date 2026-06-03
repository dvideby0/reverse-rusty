# ADR-025: Wire query-complexity limits into the parser (the config knobs were cosmetic)

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted


- **Context:** `EngineConfig` exposed three query-complexity limits — `max_query_length`,
  `max_query_clauses`, `max_anyof_group_size` — surfaced as CLI flags and as *dynamic* settings in
  ADR-022, and `config.rs` documented them as "rejected at parse time." But the parser
  (`dsl::parse`) only ever enforced its own compiled-in constants (`MAX_QUERY_LENGTH = 10_240`,
  `MAX_CLAUSES = 256`, `MAX_ANY_OF_SIZE = 64`); `parse()` took only `&str` and no ingest path ever read
  the `EngineConfig` fields. So the flags and `PUT /_settings` for these limits were **cosmetic** —
  setting them had no effect. There was also a latent default drift: the config field defaulted to
  `10_000` while the parser actually enforced `10_240`, so the documented and effective defaults
  disagreed. A repo review surfaced the wiring gap.
- **Decision:** Thread the configured limits into the parser, off the match hot path (parsing is
  compile-time, so this respects the no-work-on-the-hot-path invariant).
  - Add `dsl::ParseLimits { max_query_length, max_clauses, max_any_of_size }`, whose `Default` is the
    compiled-in constants, and `dsl::parse_with_limits(input, &limits)`. `dsl::parse` becomes the
    thin default-limits wrapper (used by the explain / read-only path and callers without a config).
  - `EngineConfig::parse_limits()` derives a `ParseLimits` from the three fields. The three **front-door**
    ingest paths — `try_build_from_queries`, `try_insert_live`, `try_bulk_ingest_detailed` — call
    `parse_with_limits` with the live config's limits. The config defaults now *reference* the `dsl`
    constants (single source of truth), so default behavior is unchanged and the `10_000`/`10_240` drift
    is gone. CLI `default_value_t` and the `api.md` example were aligned to the constants too.
  - **WAL replay keeps the compiled-in ceiling** (the non-obvious bit): `replay_insert` deliberately
    calls `dsl::parse` (default limits), *not* the configured limits. A WAL entry was already accepted at
    its front-door write; re-applying a since-tightened limit on recovery could silently drop an
    already-acknowledged write and diverge recovered state from the durable log. Durability beats policy
    on the replay path; the compiled-in ceiling still bounds replay resource use.
- **Consequence:** The `--max-*` flags and `PUT /_settings` now actually govern parsing on every ingest
  path — a tightened limit takes effect on the next ingest and is usable as a real abuse/resource guard —
  making ADR-022's *dynamic* classification and the `config.rs` / `api.md` docs true rather than
  aspirational. No match semantics change, so the oracle is unchanged. Regression-tested by a `dsl` unit
  test (`parse_with_limits_enforces_custom_bounds`, both tighter and looser than the defaults) and an
  integration test (`configured_query_limits_are_enforced_at_ingest_and_are_dynamic`) that also exercises
  the dynamic `set_config` path. The compiled-in constants are retained as the defaults and as the
  replay ceiling.
- **See also:** ADR-022 (the settings API that listed these as dynamic before they were enforced),
  ADR-013 (WAL — why replay must not re-litigate limit policy), ADR-002 (no work on the match hot path —
  why threading limits through compile-time parsing is fine), `dsl.rs` (`ParseLimits` /
  `parse_with_limits`), `config.rs` (`parse_limits`).

