# Running the server

> [Server & shared behavior](../server.md) · [REST API hub](../../api.md)

```bash
cd engine
cargo run --release --bin server
```

Options:

| Flag | Default | Description |
|---|---|---|
| `--host` | 127.0.0.1 | IP address to bind. Loopback by default; set `0.0.0.0` to listen on all interfaces (see [HTTP security](security.md)) |
| `--port` | 9200 | Port to listen on |
| `--auth-token` | *(none — auth off)* | Bearer token required on mutating/admin endpoints (ADR-062). Prefer the `RR_AUTH_TOKEN` env var in production — flag values appear in process listings (see [HTTP security](security.md)) |
| `--auth-protect-reads` | false | Extend bearer-token auth to read endpoints too (everything except `GET`/`HEAD /_health`). Requires an auth token |
| `--data-dir` | *(in-memory)* | Persistence directory for segments and WAL |
| `--load-file` | — | Pre-load queries from a CSV or JSONL file at startup |
| `--vocab-file` | — | Load vocabulary from a JSON file at startup |
| `--ranking-profiles-file` | — | Load strict, fingerprintable CPU ranking profiles from JSON; `RR_RANKING_PROFILES_FILE` is the environment alternative and `static_v1` remains built in ([ranking reference](../../ranking.md)) |
| `--threads` | *(physical cores)* | Number of rayon worker threads |
| `--max-concurrent-searches` | 0 *(unbounded)* | Max `/_search`+`/_mpercolate` requests occupying the match pool at once; excess queue within their own timeout (`timeout` or `timeout_ms`, ADR-099) |
| `--max-ranked-enrichment-bytes` | 16777216 (16 MiB) | Maximum winner source bytes fetched by one local or cluster `/v2/_search` or `/v2/_mpercolate`; overflow fails the whole response with `413 rank_enrichment_limit` (ADR-110/112) |
| `--pit-default-keep-alive-secs` | 60 | Keep-alive for a `POST /v2/_pit` point-in-time when the request names none; renewed on every use (ADR-113) |
| `--pit-max-keep-alive-secs` | 600 | Ceiling on a requested PIT keep-alive; over-ask is a 400 (ADR-113) |
| `--max-open-pits` | 64 | Concurrently open PITs; a breach is `429 pit_limit_exceeded`, never an eviction (ADR-113) |
| `--exhaustive-threads` | 2 | Dedicated Rayon workers for exhaustive jobs; isolated from interactive search (ADR-114) |
| `--max-concurrent-exhaustive-jobs` | 2 | Non-queuing exhaustive admission permits; must not exceed `--exhaustive-threads`; excess starts return 503 |
| `--exhaustive-chunk-size` | 512 | Maximum members per provisional stream chunk (hard ceiling 16,384) |
| `--exhaustive-channel-depth` | 8 | Bounded frames buffered between an exhaustive worker and its stream consumer |
| `--exhaustive-job-timeout-secs` | 300 | Maximum exhaustive admission-to-terminal lifetime (including worker scheduling); a request may ask for less |
| `--max-retained-exhaustive-jobs` | 1024 | In-memory job records; oldest terminal records are pruned, while an all-active full registry rejects with 429 |
| `--include-broad` | false | Include opt-in broad-lane class C and accepted class D queries. Class H is always visible |
| `--drain-timeout` | 30 | Graceful shutdown timeout in seconds |
| `--log-format` | pretty | `pretty` for human-readable, `json` for structured |
| `--slow-query-threshold-ms` | 1000 | Log searches exceeding this at `warn` level (0 disables) |
| `--max-segments` | 8 | Max base segments before compaction triggers |
| `--memtable-flush-threshold` | 100000 | Memtable entries before auto-flush |
| `--max-query-length` | 10240 | Maximum query string length in bytes (10 KiB) |
| `--max-query-clauses` | 256 | Maximum clauses per query |
| `--max-anyof-group-size` | 64 | Maximum members in an any-of group |
| `--max-tags` | 65535 | Maximum metadata tags on one query; larger inputs are rejected rather than truncated |
| `--retain-source` | true | Keep query source text resident; set `false` to store it on disk and fetch `_source`/explain lazily (large memory saving at scale — ADR-020) |
| `--accept-class-d` | false | Store negation-only queries as broad-lane always-candidates instead of rejecting them (ADR-068) — needed at startup for a `--load-file` corpus containing such queries; also dynamic via `/_settings` |
| `--wal-sync-on-write` | false | Fsync the WAL on every mutation before acknowledging it (SQLite FULL). When false, appends reach the OS page cache and fsync at the next flush checkpoint — survives a process crash but not power loss until checkpoint (RocksDB sync=false / SQLite NORMAL) |
| `--broad-batch-size` | 256 | Title sub-batch size for the columnar broad lane on `POST /_mpercolate` (ADR-026) — larger amortizes broad-posting scans over more titles. Dynamic via `/_settings` |
| `--hot-anchor-threshold` | 0 (off) | The hot-anchor threshold θ (class H, ADR-105; recommended 1024): a query whose deciding anchor has no top-64 mask bit but frequency ≥ θ is stored in the always-probed, columnar-evaluated hot tier instead of fattening the realtime lane. Dynamic via `/_settings`; in remote cluster mode run every `shardserver` with the same value (divergence is cost-only, never correctness) |
| `--broad-columnar` | true | Use the columnar broad evaluator (once per batch); set `false` to fall back to the inline per-title broad probe — the kill-switch (identical results, no amortization). Dynamic via `/_settings` |
| `--broad-materialize` | true | Use the pure-anchor materialization fast path (emit pure-anchor broad queries straight from the anchor bitmap, skipping verification). Dynamic via `/_settings` |
| `--max-percolate-batch` | 10000 | Maximum documents accepted in one `/_mpercolate` or multi-document `/_search` request; larger requests are rejected with 400. Dynamic via `/_settings` |

Example with persistence, vocabulary, and pre-loaded queries:

```bash
cargo run --release --bin server -- \
  --port 9200 \
  --data-dir ./data \
  --vocab-file vocab.json \
  --ranking-profiles-file ../deploy/ranking-profiles.example.json \
  --load-file queries.csv \
  --threads 8 \
  --log-format json
```

The server handles SIGINT/SIGTERM gracefully — it drains in-flight requests, flushes the memtable,
and syncs the WAL before exiting.

### Ranking profile file

`--ranking-profiles-file` or `RR_RANKING_PROFILES_FILE` loads an immutable registry before the
server binds. The complete JSON schema, feature meanings, bounds, score formula, settings behavior,
and local/distributed loading contract live in the
[`ranking-profile reference`](../../ranking.md). The checked-in
[`ranking-profiles.example.json`](../../../../deploy/ranking-profiles.example.json) is executable format
documentation, not a trained model.

EngineConfig fields marked dynamic in the options table are tunable through
[`PUT /_settings`](../settings/update-settings.md). Ranking profiles are a separate
startup-only serving registry and are deliberately absent from that API.
