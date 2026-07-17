//! Command-line flags for the HTTP server (clap). The bulk of these mirror the
//! tunable [`reverse_rusty::config::EngineConfig`] knobs; `main` maps them onto a
//! config and the runtime-tunable subset is also reachable via `PUT /_settings`.

use std::path::PathBuf;

use clap::Parser;

#[allow(clippy::struct_excessive_bools)]
#[derive(Parser, Debug)]
#[command(
    name = "reverse-rusty-server",
    about = "Reverse Rusty HTTP server",
    version
)]
pub(crate) struct Cli {
    /// IP address to bind. Defaults to `127.0.0.1` (loopback), so the server is NOT
    /// reachable beyond the local host unless you opt in. The REST API exposes
    /// mutating/admin endpoints (`_doc`, `_bulk`, `_flush`, `_compact`, `_vocab`,
    /// `_settings`); to listen on `0.0.0.0` safely, gate them with a bearer token
    /// (`--auth-token`/`RR_AUTH_TOKEN`, ADR-062) or front the server with an
    /// authenticating reverse proxy (see docs/reference/api.md). Matches the
    /// loopback default the gRPC bins use.
    #[arg(long, default_value = "127.0.0.1")]
    pub(crate) host: std::net::IpAddr,

    /// Bearer token required on mutating/admin endpoints (ADR-062). When set,
    /// `_doc` writes, `_bulk`, `_flush`, `_compact`, `_vocab` writes, and
    /// `_settings` writes demand `Authorization: Bearer <token>`; reads stay
    /// open. Prefer the `RR_AUTH_TOKEN` environment variable in production —
    /// a flag value is visible in process listings. Unset ⇒ no auth (the
    /// historical behavior).
    #[arg(long)]
    pub(crate) auth_token: Option<String>,

    /// Extend bearer-token auth to read endpoints too — everything except the
    /// `GET /_health` liveness probe (so probes keep working without
    /// credentials). Requires an auth token.
    #[arg(long, default_value_t = false)]
    pub(crate) auth_protect_reads: bool,

    /// Port to listen on.
    #[arg(long, default_value_t = 9200)]
    pub(crate) port: u16,

    /// Persistence directory (segments, WAL). Omit for in-memory only.
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,

    /// Pre-load queries from a CSV or JSONL file at startup.
    #[arg(long)]
    pub(crate) load_file: Option<PathBuf>,

    /// Load vocabulary from a JSON file at startup.
    #[arg(long)]
    pub(crate) vocab_file: Option<PathBuf>,

    /// Include broad-lane queries in match results.
    #[arg(long, default_value_t = false)]
    pub(crate) include_broad: bool,

    /// Number of rayon worker threads (defaults to physical cores).
    #[arg(long)]
    pub(crate) threads: Option<usize>,

    /// Bound concurrent search work (ADR-099): at most this many `/_search` /
    /// `/_mpercolate` requests occupy the match pool at once; excess requests
    /// queue on a semaphore (bounded by their own `timeout_ms`, so a queued
    /// request that never gets a permit times out with the usual 408 — never
    /// unbounded pile-up on the pool). 0 = unbounded (the default, byte-identical
    /// to before this flag). Size it around the rayon worker count.
    #[arg(long, default_value_t = 0)]
    pub(crate) max_concurrent_searches: usize,

    /// Maximum source bytes fetched while enriching the final winners of one
    /// local or cluster `POST /v2/_search`. The response fails with 413 before
    /// returning partial enrichment when this bound is exceeded.
    #[arg(long, default_value_t = crate::state::DEFAULT_MAX_RANKED_ENRICHMENT_BYTES)]
    pub(crate) max_ranked_enrichment_bytes: usize,

    /// Graceful shutdown drain timeout in seconds.
    #[arg(long, default_value_t = 30)]
    pub(crate) drain_timeout: u64,

    /// Log format: "json" for structured JSON, "pretty" for human-readable.
    #[arg(long, default_value = "pretty")]
    pub(crate) log_format: String,

    /// Slow-query threshold in milliseconds. Searches exceeding this are logged
    /// at warn level with diagnostic context. 0 disables.
    #[arg(long, default_value_t = 1000)]
    pub(crate) slow_query_threshold_ms: u64,

    /// Max base segments before compaction triggers.
    #[arg(long, default_value_t = 8)]
    pub(crate) max_segments: usize,

    /// Memtable entry count that triggers an automatic flush.
    #[arg(long, default_value_t = 100_000)]
    pub(crate) memtable_flush_threshold: usize,

    /// Maximum query string length in bytes.
    #[arg(long, default_value_t = reverse_rusty::dsl::MAX_QUERY_LENGTH)]
    pub(crate) max_query_length: usize,

    /// Maximum number of clauses per query.
    #[arg(long, default_value_t = reverse_rusty::dsl::MAX_CLAUSES)]
    pub(crate) max_query_clauses: usize,

    /// Maximum members in an any-of group.
    #[arg(long, default_value_t = reverse_rusty::dsl::MAX_ANY_OF_SIZE)]
    pub(crate) max_anyof_group_size: usize,

    /// Maximum number of per-query metadata tags (ADR-049). Capped at u16::MAX
    /// (the structural ceiling of the SoA tag column); a larger query is rejected.
    #[arg(long, default_value_t = u16::MAX as usize)]
    pub(crate) max_tags: usize,

    /// Fsync the write-ahead log on every mutation before acknowledging it.
    /// When false (default), WAL appends reach the OS page cache and are
    /// fsync'd at the next flush checkpoint — an acknowledged write survives a
    /// process crash but not power loss until checkpoint (RocksDB sync=false /
    /// SQLite NORMAL). When true, every write is durable against power loss at
    /// a large per-write latency cost (SQLite FULL).
    #[arg(long, default_value_t = false)]
    pub(crate) wal_sync_on_write: bool,

    /// Accept negation-only (cost class D) queries as broad-lane
    /// always-candidates instead of rejecting them (ADR-068). Needed at startup
    /// for a `--load-file` corpus containing such queries; also runtime-tunable
    /// via `PUT /_settings {"accept_class_d": true}`.
    #[arg(long, default_value_t = false)]
    pub(crate) accept_class_d: bool,

    /// Keep every query's source text resident in RAM (default true — instant
    /// `_source`/explain, historical behavior). Set false to store source text on
    /// disk (`sources.dat`, mmap'd) and fetch it lazily — a large resident-memory
    /// saving at scale (the source store is the single largest resident structure
    /// at ~100M queries), at the cost of a cold binary-search + page fault per
    /// `_source`/explain lookup (never the match hot path). See ADR-020.
    #[arg(long, default_value_t = true)]
    pub(crate) retain_source: bool,

    /// Title sub-batch size for the columnar broad lane on `POST /_mpercolate`
    /// (ADR-026). Larger amortizes broad-posting scans over more titles. Dynamic
    /// via `PUT /_settings`.
    #[arg(long, default_value_t = 256)]
    pub(crate) broad_batch_size: usize,

    /// Use the columnar broad evaluator (once per batch). Set false to fall back
    /// to the inline per-title broad probe — the kill-switch (identical results,
    /// no amortization). Dynamic via `PUT /_settings`.
    #[arg(long, default_value_t = true)]
    pub(crate) broad_columnar: bool,

    /// Use the pure-anchor materialization fast path (emit pure-anchor broad
    /// queries straight from the anchor bitmap, skipping verification). Dynamic
    /// via `PUT /_settings`.
    #[arg(long, default_value_t = true)]
    pub(crate) broad_materialize: bool,

    /// Maximum documents accepted in one `POST /_mpercolate` batch; larger
    /// requests are rejected with 400. Dynamic via `PUT /_settings`.
    #[arg(long, default_value_t = 10_000)]
    pub(crate) max_percolate_batch: usize,

    /// The hot-anchor threshold θ (class H, ADR-105). 0 (the default) disables
    /// the hot tier — classification is byte-identical to the pre-ADR-105
    /// engine. When set (recommended 1024), a query whose deciding anchor has no
    /// top-64 mask bit but a frequency ≥ θ is stored in the always-probed,
    /// columnar-evaluated hot tier instead of fattening the realtime lane.
    /// Dynamic via `PUT /_settings` (affects new writes immediately; sealed
    /// entries migrate at the next re-anchoring compaction). In remote cluster
    /// mode run every `shardserver` with the same value (divergence is
    /// cost-only, never correctness — see ADR-105).
    #[arg(long, default_value_t = 0)]
    pub(crate) hot_anchor_threshold: u32,

    // ---- coordinator (cluster) mode, ADR-070 ----
    /// Run as a CLUSTER coordinator: the same REST API served over a multi-shard
    /// `ClusterEngine` instead of a single-node `Engine` (ADR-070). In-process by
    /// default (`--shards` K in this process, durable with `--data-dir`); with
    /// repeatable `--shard-endpoint` flags the shards are remote `shardserver`
    /// nodes (requires a `--features distributed` build).
    #[arg(long, default_value_t = false)]
    pub(crate) cluster: bool,

    /// Number of shards for an in-process cluster (`--cluster`). Ignored when
    /// `--shard-endpoint`s are given — the endpoint count defines K.
    #[arg(long, default_value_t = 8)]
    pub(crate) shards: usize,

    /// Copies per shard position in cluster mode (1 = primary only). For an
    /// in-process cluster replicas are in-process copies; for a remote cluster
    /// list replicas inside each `--shard-endpoint` group instead.
    #[arg(long, default_value_t = 1)]
    pub(crate) replication_factor: usize,

    /// Remote shard endpoint group for coordinator mode — repeatable, one flag per
    /// shard position, each `primary[,replica,...]` (e.g.
    /// `--shard-endpoint http://10.0.0.1:50051,http://10.0.0.2:50051`). The
    /// coordinator ships its frozen dict + tag space to every endpoint at connect
    /// (ADR-034/055). Requires `--cluster` and a `--features distributed` build.
    #[arg(long)]
    pub(crate) shard_endpoint: Vec<String>,

    /// PEM CA bundle to verify remote shard servers against (mesh TLS, ADR-071).
    /// With it, `--shard-endpoint` URLs should use `https://`. Requires a
    /// `--features distributed` build.
    #[arg(long)]
    pub(crate) grpc_tls_ca: Option<PathBuf>,

    /// TLS verification/SNI domain override for the mesh links (needed when
    /// `--shard-endpoint` URLs are raw IPs but the server certificate names a DNS
    /// SAN). Only meaningful with `--grpc-tls-ca`.
    #[arg(long)]
    pub(crate) grpc_tls_domain: Option<String>,

    /// Mesh cluster token (ADR-071) attached to every gRPC RPC the coordinator sends
    /// its shard servers. Prefer the `RR_CLUSTER_TOKEN` env var in production. This
    /// is distinct from `--auth-token` (the HTTP bearer gate, ADR-062): client-facing
    /// REST and the node mesh are different audiences with different rotation
    /// stories.
    #[arg(long)]
    pub(crate) cluster_token: Option<String>,

    /// Durable control-plane quorum endpoint(s) for coordinator mode (ADR-083) — repeatable, the
    /// `controlserver` `ControlService` URLs (e.g. `--control-endpoint https://control0:50061`).
    /// When set, the coordinator attaches its cluster-state control plane to the quorum (so
    /// membership / assignment / resize decisions are durable + HA across coordinator restarts)
    /// instead of the default in-memory backend. It is a THIN CLIENT — the coordinator does not
    /// join consensus, staying stateless. List ALL quorum members for failover (ADR-086): the
    /// client tries them in order and follows a follower's `ForwardToLeader` redirect. Requires
    /// `--shard-endpoint` (remote mode) and a `--features distributed` build; rides the same mesh
    /// security (`--grpc-tls-ca`/`--grpc-tls-domain`/`--cluster-token`) as the shard links.
    #[arg(long)]
    pub(crate) control_endpoint: Vec<String>,

    /// Route by the committed shard→node assignments instead of the static `--shard-endpoint`
    /// order (ADR-086). The coordinator seeds the quorum from `--shard-endpoint` (position-
    /// preserving) and then resolves its shard topology from the durable document, so the quorum —
    /// not per-coordinator flags — is the topology source of truth (a coordinator can boot with
    /// only `--control-endpoint`). Requires `--control-endpoint`. Fails loud if the committed map
    /// is not position-preserving (a non-data-moving `rebalance`) — see ADR-086.
    #[arg(long, default_value_t = false)]
    pub(crate) route_by_assignments: bool,

    /// Run the unattended re-point reconciler every N seconds (ADR-092): periodically reconcile the
    /// committed shard→node map to the desired HRW placement by MOVING data (the data-moving path, not
    /// the map-only `rebalance`), so a membership change converges routing automatically with no
    /// operator action. Idempotent — a converged map moves nothing — and every move reserves its
    /// nodes in the engine's busy-endpoint move ledger (ADR-095), shared with `/_cluster/reassign`
    /// and the autoscaler, so a pass never overlaps a CONFLICTING manual move. Requires
    /// `--route-by-assignments` (and therefore `--control-endpoint`) and a `--features distributed`
    /// build. Unset (default) ⇒ no reconciler runs (byte-identical).
    #[arg(long)]
    pub(crate) reconcile_interval_secs: Option<u64>,

    /// Wave parallelism for each reconcile pass's moves (ADR-095): up to N conflict-free moves
    /// (disjoint node footprints) run concurrently per pass. Default 1 = the sequential pass,
    /// byte-identical to pre-ADR-095. Each parallel move costs one OS thread + its own gRPC
    /// connections for the duration of an O(corpus) copy — size to what the mesh and the nodes'
    /// disks can absorb. Only meaningful with `--reconcile-interval-secs`.
    #[arg(long, default_value_t = 1)]
    pub(crate) reconcile_max_parallel: usize,

    /// Run an orphan-slot GC sweep after each reconcile pass that leaves the map fully converged
    /// (ADR-096): reclaim the fenced, unrouted slots data-moving reassignment strands on their old
    /// nodes (slot map + `shard_<id>/` disk). Unset (default) ⇒ no sweep ever runs
    /// (byte-identical); a one-shot sweep is also available as `POST /_cluster/gc`. Only
    /// meaningful with `--reconcile-interval-secs`.
    #[arg(long, default_value_t = false)]
    pub(crate) reconcile_gc_orphans: bool,

    /// Coordinator gRPC client connect timeout in seconds (ADR-085) — bounds the TCP+TLS
    /// dial so an unreachable shard fails fast. Default: 5s.
    #[arg(long)]
    pub(crate) grpc_connect_timeout_secs: Option<u64>,

    /// Per-call deadline in seconds for unary READ RPCs (percolate / counts) the coordinator
    /// sends shard servers (ADR-085) — a hung shard fails loud instead of hanging the fan-out,
    /// and idempotent reads retry on a transient error. Default: 10s.
    #[arg(long)]
    pub(crate) grpc_read_timeout_secs: Option<u64>,

    /// Per-call deadline in seconds for unary WRITE RPCs (ingest / insert / delete / flush)
    /// the coordinator sends shard servers (ADR-085); writes never retry. Default: 30s.
    #[arg(long)]
    pub(crate) grpc_write_timeout_secs: Option<u64>,

    /// HTTP/2 keepalive PING interval in seconds for the coordinator's gRPC links (ADR-085) —
    /// detects a dead/half-open peer so a stalled connection is broken. Default: 10s.
    #[arg(long)]
    pub(crate) grpc_keepalive_secs: Option<u64>,

    /// Bounded retry attempts for IDEMPOTENT read RPCs on a transient (UNAVAILABLE) error
    /// (ADR-085); writes never retry. Default: 2.
    #[arg(long)]
    pub(crate) grpc_read_retries: Option<u32>,
}
