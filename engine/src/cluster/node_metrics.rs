//! Per-node Prometheus metrics for the deployable `shardserver` / `controlserver` (ADR-091).
//!
//! Only the coordinator exposed `/_metrics` before this (single-node / `--cluster` server, via the
//! `prometheus` crate behind the `server` feature). The deploy bins are `distributed`-gated and the
//! `prometheus` crate is NOT in that build, so rather than entangle the features this module
//! hand-rolls a tiny, std-only Prometheus **text-exposition** renderer plus a minimal blocking
//! HTTP/1.1 listener on a dedicated thread — the same lean, std-only spirit as
//! [`transport_metrics`](super::transport_metrics). No new dependency; no tokio worker consumed; the
//! listener is decoupled from the gRPC runtime.
//!
//! The endpoint is served on a SEPARATE, plaintext `--metrics-addr` port — never the TLS + token
//! mesh data port — mirroring the ADR-084 `--health-addr` posture: a Prometheus scrape (plaintext,
//! pod-local, non-sensitive observability) reaches it directly. Unset ⇒ no listener, byte-identical.
//!
//! The metric set is **gauges read fresh at scrape time** (no cumulative-counter registry, no
//! `EngineEvent` observer wiring on the deploy bins) plus, on shard nodes, the per-shard RPC
//! **latency histogram** family (ADR-100 — recorded at the gRPC handler boundary, see
//! [`latency`]). Shard nodes emit the SAME `reverse_rusty_*` engine-gauge names a single-node
//! server already emits — so existing dashboards work per-pod — plus a few shard extras; control
//! nodes emit `reverse_rusty_control_*` Raft gauges.

mod latency;

pub(crate) use latency::{LatencySnapshot, ShardRpc, SlotLatency, LATENCY_LE, SHARD_RPC_LABELS};

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::events::{EngineMetrics, SegmentInfo};

/// The Prometheus text-exposition content type (format version 0.0.4).
const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";
/// The per-shard RPC service-latency histogram family (ADR-100).
const RPC_HIST: &str = "reverse_rusty_shard_rpc_duration_seconds";
/// How often the idle accept loop wakes to re-check the stop flag (non-blocking accept poll).
const ACCEPT_POLL: Duration = Duration::from_millis(20);
/// Per-connection read timeout: a scrape is a tiny request line + headers; bound a slow/garbage
/// client so it can never wedge the single-threaded accept loop.
const READ_TIMEOUT: Duration = Duration::from_secs(2);
/// Hard cap on the bytes read for the request line. The port is plaintext + network-facing, so an
/// unbounded read would let a client trickle a newline-less line to grow memory without bound (the
/// per-read timeout only fires when NO bytes arrive). 8 KiB is far above any real GET line (a scrape
/// is < 200 bytes); an over-long / newline-less line just fails `is_metrics_get` and gets a 404.
const MAX_REQUEST_BYTES: u64 = 8 * 1024;

// ---- text-exposition writer --------------------------------------------------------------------

/// A minimal Prometheus text-exposition builder: emits `# HELP` / `# TYPE` headers and
/// `name{labels} value` series into an accumulating `String`. Every metric here is a gauge.
struct Exposition {
    out: String,
}

impl Exposition {
    fn new() -> Self {
        Self {
            out: String::with_capacity(1024),
        }
    }

    /// Emit the `# HELP` + `# TYPE … gauge` header pair for a metric family (call once per family).
    fn header(&mut self, name: &str, help: &str) {
        self.header_typed(name, help, "gauge");
    }

    /// Emit the `# HELP` + `# TYPE … <mtype>` header pair for a metric family of any type
    /// (`gauge` / `histogram`); call once per family.
    fn header_typed(&mut self, name: &str, help: &str, mtype: &str) {
        self.out.push_str("# HELP ");
        self.out.push_str(name);
        self.out.push(' ');
        self.out.push_str(help);
        self.out.push('\n');
        self.out.push_str("# TYPE ");
        self.out.push_str(name);
        self.out.push(' ');
        self.out.push_str(mtype);
        self.out.push('\n');
    }

    /// A single unlabeled gauge (header + one sample).
    fn gauge(&mut self, name: &str, help: &str, value: impl std::fmt::Display) {
        self.header(name, help);
        self.out.push_str(name);
        self.out.push(' ');
        self.out.push_str(&value.to_string());
        self.out.push('\n');
    }

    /// One labeled sample line (the caller emits [`header`](Self::header) once for the family).
    fn sample(&mut self, name: &str, labels: &[(&str, &str)], value: impl std::fmt::Display) {
        self.out.push_str(name);
        if !labels.is_empty() {
            self.out.push('{');
            for (i, (k, v)) in labels.iter().enumerate() {
                if i > 0 {
                    self.out.push(',');
                }
                self.out.push_str(k);
                self.out.push_str("=\"");
                push_escaped(&mut self.out, v);
                self.out.push('"');
            }
            self.out.push('}');
        }
        self.out.push(' ');
        self.out.push_str(&value.to_string());
        self.out.push('\n');
    }

    fn into_string(self) -> String {
        self.out
    }
}

/// Escape a Prometheus label value (`\`, `"`, newline) per the exposition format. The labels this
/// module emits are all static ASCII, so this is defensive — but cheap and correct.
fn push_escaped(out: &mut String, value: &str) {
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
}

// ---- shard rendering ---------------------------------------------------------------------------

/// One hosted shard's live numbers for the per-node exposition (ADR-093). A co-located node passes
/// one of these per LOADED slot to [`render_shards`]; `class` is `[A, B, C, D]` stored-query counts.
pub(crate) struct ShardSample {
    pub shard_id: u32,
    pub metrics: EngineMetrics,
    pub segments: Vec<SegmentInfo>,
    pub class: [u64; 4],
    /// Per-RPC service-latency histograms for this slot, indexed like [`SHARD_RPC_LABELS`]
    /// (ADR-100). All-zero on a slot that has served no RPCs — the family still renders, so the
    /// series exist from the first scrape.
    pub rpc_latency: [LatencySnapshot; SHARD_RPC_LABELS.len()],
}

/// Render a shard node's `/_metrics` body over ALL the node's loaded slots (ADR-093 multi-shard). A
/// co-located node hosts many shards, so each `reverse_rusty_*` family emits its `# HELP`/`# TYPE`
/// header ONCE, then one `{shard="<id>"}`-labeled series per hosted slot — valid *grouped* exposition
/// (duplicating the header per slot would be malformed). A 1:1 node emits exactly one labeled series
/// per family; dashboards keying on the bare metric name still match (the `shard` label is additive).
/// No loaded slot ⇒ [`render_shard_pending`].
///
/// Cluster shards are segments-only (no WAL), so `wal_*` read 0 — that is correct, not a bug; the
/// live LSM-pressure signal on a shard is `tombstoned_entries` (compaction backlog) + `stale_segments`.
pub(crate) fn render_shards(samples: &[ShardSample]) -> String {
    if samples.is_empty() {
        return render_shard_pending();
    }
    let mut e = Exposition::new();
    // `sample()` takes `&str`; pre-stringify each slot's shard-id label once (sorted by the caller).
    let sids: Vec<String> = samples.iter().map(|s| s.shard_id.to_string()).collect();

    e.header(
        "reverse_rusty_total_queries",
        "Stored queries on this shard (incl. tombstoned), like single-node total_queries.",
    );
    for (s, sid) in samples.iter().zip(&sids) {
        e.sample(
            "reverse_rusty_total_queries",
            &[("shard", sid)],
            s.metrics.total_queries,
        );
    }

    e.header(
        "reverse_rusty_base_segments",
        "Sealed immutable base segments on this shard.",
    );
    for (s, sid) in samples.iter().zip(&sids) {
        e.sample(
            "reverse_rusty_base_segments",
            &[("shard", sid)],
            s.metrics.base_segments,
        );
    }

    e.header(
        "reverse_rusty_memtable_entries",
        "Entries in the mutable memtable on this shard.",
    );
    for (s, sid) in samples.iter().zip(&sids) {
        e.sample(
            "reverse_rusty_memtable_entries",
            &[("shard", sid)],
            s.metrics.memtable_entries,
        );
    }

    e.header(
        "reverse_rusty_dict_features",
        "Distinct features in the shared frozen dictionary.",
    );
    for (s, sid) in samples.iter().zip(&sids) {
        e.sample(
            "reverse_rusty_dict_features",
            &[("shard", sid)],
            s.metrics.dict_features,
        );
    }

    e.header(
        "reverse_rusty_memory_bytes",
        "Resident heap memory by component on this shard.",
    );
    for (s, sid) in samples.iter().zip(&sids) {
        for (component, bytes) in [
            ("exact", s.metrics.exact_bytes),
            ("index", s.metrics.index_bytes),
            ("filter", s.metrics.filter_bytes),
            ("dict", s.metrics.dict_bytes),
            ("query_store", s.metrics.query_store_bytes),
            ("logical_index", s.metrics.logical_index_bytes),
            ("alive", s.metrics.alive_bytes),
        ] {
            e.sample(
                "reverse_rusty_memory_bytes",
                &[("shard", sid), ("component", component)],
                bytes,
            );
        }
    }

    e.header(
        "reverse_rusty_wal_size_bytes",
        "WAL size in bytes (0 on a segments-only cluster shard).",
    );
    for (s, sid) in samples.iter().zip(&sids) {
        e.sample(
            "reverse_rusty_wal_size_bytes",
            &[("shard", sid)],
            s.metrics.wal_size_bytes,
        );
    }

    e.header(
        "reverse_rusty_wal_pending_entries",
        "Un-checkpointed WAL entries (0 on a segments-only cluster shard).",
    );
    for (s, sid) in samples.iter().zip(&sids) {
        e.sample(
            "reverse_rusty_wal_pending_entries",
            &[("shard", sid)],
            s.metrics.wal_pending_entries,
        );
    }

    e.header(
        "reverse_rusty_stale_segments",
        "Segments compiled against an older vocab epoch (vocab drift).",
    );
    for (s, sid) in samples.iter().zip(&sids) {
        e.sample(
            "reverse_rusty_stale_segments",
            &[("shard", sid)],
            s.metrics.stale_segments,
        );
    }

    e.header(
        "reverse_rusty_tombstoned_entries",
        "Tombstoned (deleted-but-not-compacted) entries — compaction backlog.",
    );
    for (s, sid) in samples.iter().zip(&sids) {
        let tombstoned: usize = s.segments.iter().map(|x| x.deleted).sum();
        e.sample(
            "reverse_rusty_tombstoned_entries",
            &[("shard", sid)],
            tombstoned,
        );
    }

    e.header(
        "reverse_rusty_class_queries",
        "Stored queries by cost class (c is the broad lane).",
    );
    for (s, sid) in samples.iter().zip(&sids) {
        for (label, count) in [
            ("a", s.class[0]),
            ("b", s.class[1]),
            ("c", s.class[2]),
            ("d", s.class[3]),
        ] {
            e.sample(
                "reverse_rusty_class_queries",
                &[("shard", sid), ("class", label)],
                count,
            );
        }
    }

    // Every slot handed to this renderer holds a serving `ServerState` (the caller filters to loaded
    // slots), so each is ready=1; a node with NO loaded slot took the `render_shard_pending` path.
    e.header(
        "reverse_rusty_shard_ready",
        "1 if this shard has adopted a dict and is serving, else 0.",
    );
    for sid in &sids {
        e.sample("reverse_rusty_shard_ready", &[("shard", sid)], 1u8);
    }

    // Per-shard RPC service-latency histograms (ADR-100): native HISTOGRAM exposition —
    // cumulative `le` buckets + `_sum`/`_count` per {shard, method} — header once across all
    // slots × methods (grouped exposition, the ADR-093 lesson). Quantiles are Prometheus's job:
    // histogram_quantile(0.95, sum by (le, shard)
    //   (rate(reverse_rusty_shard_rpc_duration_seconds_bucket{method="percolate"}[5m]))).
    e.header_typed(
        RPC_HIST,
        "Shard-side service time of successful RPCs at the gRPC handler boundary.",
        "histogram",
    );
    let bucket = format!("{RPC_HIST}_bucket");
    let sum = format!("{RPC_HIST}_sum");
    let count = format!("{RPC_HIST}_count");
    for (s, sid) in samples.iter().zip(&sids) {
        for (snap, method) in s.rpc_latency.iter().zip(SHARD_RPC_LABELS) {
            let mut cumulative = 0u64;
            for (n, &(_, le)) in snap.buckets.iter().zip(LATENCY_LE.iter()) {
                cumulative += n;
                e.sample(
                    &bucket,
                    &[("shard", sid), ("method", method), ("le", le)],
                    cumulative,
                );
            }
            // `total()` clamps to >= the last finite cumulative bucket (torn-read safety) and
            // covers >30s overflow observations; `_count` must equal `le="+Inf"`.
            let total = snap.total();
            e.sample(
                &bucket,
                &[("shard", sid), ("method", method), ("le", "+Inf")],
                total,
            );
            e.sample(
                &sum,
                &[("shard", sid), ("method", method)],
                snap.sum_nanos as f64 / 1e9,
            );
            e.sample(&count, &[("shard", sid), ("method", method)], total);
        }
    }

    e.into_string()
}

/// The `/_metrics` body for a shard that has not yet adopted a dict (`--pending`): a valid scrape
/// reporting only `reverse_rusty_shard_ready 0`, so Prometheus sees a live-but-not-ready target
/// rather than a connection error.
pub(crate) fn render_shard_pending() -> String {
    let mut e = Exposition::new();
    e.gauge(
        "reverse_rusty_shard_ready",
        "1 if this shard has adopted a dict and is serving, else 0.",
        0u8,
    );
    e.into_string()
}

// ---- control rendering -------------------------------------------------------------------------

/// A snapshot of the Raft fields a control node exposes — primitives only, so [`render_control`] is
/// unit-testable without constructing a `Raft`. Filled from openraft's `RaftMetrics` by
/// [`control_view`].
pub(crate) struct ControlMetricsView {
    pub term: u64,
    /// Lowercased server state: `leader` / `follower` / `candidate` / `learner` / `shutdown`.
    pub state: &'static str,
    pub is_leader: bool,
    pub leader_known: bool,
    pub last_log_index: Option<u64>,
    pub last_applied: Option<u64>,
    pub voters: usize,
    pub snapshot_last_index: Option<u64>,
}

/// Render a control node's `/_metrics` body from a [`ControlMetricsView`].
pub(crate) fn render_control(v: &ControlMetricsView) -> String {
    let mut e = Exposition::new();
    e.gauge(
        "reverse_rusty_control_term",
        "Current Raft term as seen by this control node.",
        v.term,
    );
    e.gauge(
        "reverse_rusty_control_is_leader",
        "1 if this control node is the current Raft leader, else 0.",
        u8::from(v.is_leader),
    );
    e.gauge(
        "reverse_rusty_control_leader_known",
        "1 if this control node currently sees an elected leader, else 0.",
        u8::from(v.leader_known),
    );

    e.header(
        "reverse_rusty_control_state",
        "This node's Raft server state (the active state carries value 1).",
    );
    e.sample("reverse_rusty_control_state", &[("state", v.state)], 1u8);

    e.gauge(
        "reverse_rusty_control_last_log_index",
        "Last appended Raft log index (0 if none).",
        v.last_log_index.unwrap_or(0),
    );
    e.gauge(
        "reverse_rusty_control_last_applied",
        "Last applied Raft log index (0 if none).",
        v.last_applied.unwrap_or(0),
    );
    e.gauge(
        "reverse_rusty_control_voters",
        "Voting members in the committed cluster membership.",
        v.voters,
    );
    e.gauge(
        "reverse_rusty_control_snapshot_last_index",
        "Last log index included in the latest Raft snapshot (0 if none).",
        v.snapshot_last_index.unwrap_or(0),
    );
    e.into_string()
}

/// Project openraft's `RaftMetrics` onto the primitive [`ControlMetricsView`] this module renders.
pub(crate) fn control_view(
    m: &openraft::RaftMetrics<u64, openraft::BasicNode>,
) -> ControlMetricsView {
    use openraft::ServerState;
    let state = match m.state {
        ServerState::Leader => "leader",
        ServerState::Follower => "follower",
        ServerState::Candidate => "candidate",
        ServerState::Learner => "learner",
        ServerState::Shutdown => "shutdown",
    };
    ControlMetricsView {
        term: m.current_term,
        state,
        is_leader: matches!(m.state, ServerState::Leader),
        leader_known: m.current_leader.is_some(),
        last_log_index: m.last_log_index,
        last_applied: m.last_applied.map(|l| l.index),
        voters: m.membership_config.membership().voter_ids().count(),
        snapshot_last_index: m.snapshot.map(|l| l.index),
    }
}

// ---- minimal HTTP/1.1 listener -----------------------------------------------------------------

/// A running per-node metrics listener. Dropping the handle DETACHES the server (it keeps serving
/// for the process lifetime — the production path); [`shutdown`](Self::shutdown) stops it and joins
/// the thread (used by tests for clean teardown).
pub struct MetricsHandle {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

impl MetricsHandle {
    /// The actually-bound address (resolves a `:0` request to the real port).
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Stop the listener and wait for its thread to exit. The accept loop wakes within one
    /// [`ACCEPT_POLL`] to observe the flag.
    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(join) = self.join.take() {
            drop(join.join());
        }
    }
}

/// Bind `addr` and serve a plaintext HTTP/1.1 `/_metrics` (and `/metrics` alias) endpoint that, on
/// each GET, returns `render()` as Prometheus text. Any other request gets `404`. The listener runs
/// on a dedicated thread; binding is synchronous so the returned [`MetricsHandle::addr`] reflects the
/// real bound port. Fails loud (`io::Error`) if the port cannot be bound — an explicit
/// `--metrics-addr` misconfiguration should not start silently.
pub fn serve_metrics(
    addr: SocketAddr,
    render: impl Fn() -> String + Send + 'static,
) -> io::Result<MetricsHandle> {
    let listener = TcpListener::bind(addr)?;
    let bound = listener.local_addr()?;
    listener.set_nonblocking(true)?;
    let stop = Arc::new(AtomicBool::new(false));
    let stop_thread = Arc::clone(&stop);
    let join = thread::Builder::new()
        .name("rr-metrics".into())
        .spawn(move || accept_loop(&listener, &stop_thread, &render))?;
    Ok(MetricsHandle {
        addr: bound,
        stop,
        join: Some(join),
    })
}

/// The accept loop: poll for a connection, serve it, re-check the stop flag. Best-effort — a broken
/// client connection is dropped, never logged (library code writes no stderr; ADR-021).
fn accept_loop(listener: &TcpListener, stop: &AtomicBool, render: &impl Fn() -> String) {
    while !stop.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, _peer)) => drop(handle_conn(stream, render)),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => thread::sleep(ACCEPT_POLL),
            // A transient accept error (e.g. a connection reset before accept) must not kill the
            // server; pause briefly and keep serving.
            Err(_) => thread::sleep(ACCEPT_POLL),
        }
    }
}

/// Serve one connection: read the request line, answer `GET /_metrics` (or `/metrics`) with the
/// rendered body, everything else with `404`. `?`-propagates I/O errors to the caller, which drops
/// them (best-effort).
fn handle_conn(stream: TcpStream, render: &impl Fn() -> String) -> io::Result<()> {
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(READ_TIMEOUT))?;
    let mut reader = BufReader::new(stream);
    let mut request_line = String::new();
    // Bounded read (see `MAX_REQUEST_BYTES`): `Take` caps the bytes `read_line` will accumulate, so a
    // trickled newline-less line can neither grow memory without bound nor loop past the cap.
    (&mut reader)
        .take(MAX_REQUEST_BYTES)
        .read_line(&mut request_line)?;
    let is_metrics_get = is_metrics_get(&request_line);
    let mut stream = reader.into_inner();
    if is_metrics_get {
        let body = render();
        write_response(&mut stream, "200 OK", CONTENT_TYPE, &body)
    } else {
        write_response(
            &mut stream,
            "404 Not Found",
            "text/plain; charset=utf-8",
            "not found\n",
        )
    }
}

/// Whether an HTTP request line is `GET /_metrics` (or the `/metrics` alias). Tolerates an absent
/// HTTP version and trailing query string.
fn is_metrics_get(line: &str) -> bool {
    let mut parts = line.split_whitespace();
    if parts.next() != Some("GET") {
        return false;
    }
    let path = parts.next().unwrap_or("");
    let path = path.split('?').next().unwrap_or(path);
    path == "/_metrics" || path == "/metrics"
}

/// Write a complete HTTP/1.1 response with `Connection: close`.
fn write_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &str,
) -> io::Result<()> {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes())
}

#[cfg(test)]
mod tests;
