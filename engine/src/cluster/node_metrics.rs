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
//! The metric set is all **gauges read fresh at scrape time** (no cumulative-counter registry, no
//! `EngineEvent` observer wiring on the deploy bins). Shard nodes emit the SAME `reverse_rusty_*`
//! engine-gauge names a single-node server already emits — so existing dashboards work per-pod —
//! plus a few shard extras; control nodes emit `reverse_rusty_control_*` Raft gauges.

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::events::{EngineMetrics, SegmentInfo};

/// The Prometheus text-exposition content type (format version 0.0.4).
const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";
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
        self.out.push_str("# HELP ");
        self.out.push_str(name);
        self.out.push(' ');
        self.out.push_str(help);
        self.out.push('\n');
        self.out.push_str("# TYPE ");
        self.out.push_str(name);
        self.out.push_str(" gauge\n");
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

/// Render a shard node's `/_metrics` body from a consistent engine snapshot. `class` is
/// `[A, B, C, D]` stored-query counts; `ready` is whether the shard has adopted a dict.
///
/// Cluster shards are segments-only (no WAL), so `wal_*` read 0 — that is correct, not a bug; the
/// live LSM-pressure signal on a shard is `tombstoned_entries` (compaction backlog) + `stale_segments`.
pub(crate) fn render_shard(
    m: &EngineMetrics,
    segments: &[SegmentInfo],
    class: [u64; 4],
    ready: bool,
) -> String {
    let mut e = Exposition::new();
    e.gauge(
        "reverse_rusty_total_queries",
        "Stored queries on this shard (incl. tombstoned), like single-node total_queries.",
        m.total_queries,
    );
    e.gauge(
        "reverse_rusty_base_segments",
        "Sealed immutable base segments on this shard.",
        m.base_segments,
    );
    e.gauge(
        "reverse_rusty_memtable_entries",
        "Entries in the mutable memtable on this shard.",
        m.memtable_entries,
    );
    e.gauge(
        "reverse_rusty_dict_features",
        "Distinct features in the shared frozen dictionary.",
        m.dict_features,
    );

    e.header(
        "reverse_rusty_memory_bytes",
        "Resident heap memory by component on this shard.",
    );
    for (component, bytes) in [
        ("exact", m.exact_bytes),
        ("index", m.index_bytes),
        ("filter", m.filter_bytes),
        ("dict", m.dict_bytes),
        ("query_store", m.query_store_bytes),
        ("logical_index", m.logical_index_bytes),
        ("alive", m.alive_bytes),
    ] {
        e.sample(
            "reverse_rusty_memory_bytes",
            &[("component", component)],
            bytes,
        );
    }

    e.gauge(
        "reverse_rusty_wal_size_bytes",
        "WAL size in bytes (0 on a segments-only cluster shard).",
        m.wal_size_bytes,
    );
    e.gauge(
        "reverse_rusty_wal_pending_entries",
        "Un-checkpointed WAL entries (0 on a segments-only cluster shard).",
        m.wal_pending_entries,
    );
    e.gauge(
        "reverse_rusty_stale_segments",
        "Segments compiled against an older vocab epoch (vocab drift).",
        m.stale_segments,
    );

    let tombstoned: usize = segments.iter().map(|s| s.deleted).sum();
    e.gauge(
        "reverse_rusty_tombstoned_entries",
        "Tombstoned (deleted-but-not-compacted) entries — compaction backlog.",
        tombstoned,
    );

    e.header(
        "reverse_rusty_class_queries",
        "Stored queries by cost class (c is the broad lane).",
    );
    for (label, count) in [
        ("a", class[0]),
        ("b", class[1]),
        ("c", class[2]),
        ("d", class[3]),
    ] {
        e.sample("reverse_rusty_class_queries", &[("class", label)], count);
    }

    e.gauge(
        "reverse_rusty_shard_ready",
        "1 if this shard has adopted a dict and is serving, else 0.",
        u8::from(ready),
    );
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
mod tests {
    use super::{
        is_metrics_get, render_control, render_shard, render_shard_pending, serve_metrics,
        ControlMetricsView,
    };
    use crate::events::{EngineMetrics, SegmentInfo, SegmentKind};
    use std::io::{Read, Write};
    use std::net::TcpStream;

    fn sample_metrics() -> EngineMetrics {
        EngineMetrics {
            total_queries: 7,
            base_segments: 2,
            memtable_entries: 3,
            segment_sizes: vec![4, 1],
            segment_holes: vec![0.0, 0.5],
            rejected_parse: 0,
            rejected_class_d: 0,
            dict_features: 99,
            exact_bytes: 11,
            index_bytes: 22,
            filter_bytes: 33,
            stale_segments: 1,
            dict_bytes: 44,
            query_store_bytes: 55,
            logical_index_bytes: 66,
            alive_bytes: 77,
            wal_size_bytes: 0,
            wal_pending_entries: 0,
        }
    }

    fn seg(deleted: usize) -> SegmentInfo {
        SegmentInfo {
            ordinal: 0,
            kind: SegmentKind::Mmap,
            entries: 10,
            alive: 10 - deleted,
            deleted,
            holes_ratio: 0.0,
            vocab_epoch: 0,
            stale: false,
            resident_bytes: 0,
            overhead_bytes: 0,
        }
    }

    #[test]
    fn render_shard_emits_named_gauges() {
        let out = render_shard(&sample_metrics(), &[seg(3), seg(2)], [1, 2, 3, 4], true);
        assert!(out.contains("# TYPE reverse_rusty_total_queries gauge"));
        assert!(out.contains("\nreverse_rusty_total_queries 7\n"));
        assert!(out.contains("reverse_rusty_dict_features 99"));
        assert!(out.contains("reverse_rusty_memory_bytes{component=\"exact\"} 11"));
        assert!(out.contains("reverse_rusty_memory_bytes{component=\"filter\"} 33"));
        // c is the broad lane; 3rd class slot.
        assert!(out.contains("reverse_rusty_class_queries{class=\"c\"} 3"));
        // tombstoned = sum of segment `deleted` (3 + 2).
        assert!(out.contains("reverse_rusty_tombstoned_entries 5"));
        assert!(out.contains("reverse_rusty_shard_ready 1"));
    }

    #[test]
    fn pending_shard_reports_not_ready() {
        let out = render_shard_pending();
        assert!(out.contains("reverse_rusty_shard_ready 0"));
        assert!(!out.contains("reverse_rusty_total_queries"));
    }

    #[test]
    fn render_control_emits_state_and_indices() {
        let view = ControlMetricsView {
            term: 5,
            state: "leader",
            is_leader: true,
            leader_known: true,
            last_log_index: Some(42),
            last_applied: Some(40),
            voters: 3,
            snapshot_last_index: Some(30),
        };
        let out = render_control(&view);
        assert!(out.contains("reverse_rusty_control_term 5"));
        assert!(out.contains("reverse_rusty_control_is_leader 1"));
        assert!(out.contains("reverse_rusty_control_state{state=\"leader\"} 1"));
        assert!(out.contains("reverse_rusty_control_last_applied 40"));
        assert!(out.contains("reverse_rusty_control_voters 3"));
    }

    #[test]
    fn request_line_parsing() {
        assert!(is_metrics_get("GET /_metrics HTTP/1.1\r\n"));
        assert!(is_metrics_get("GET /metrics HTTP/1.1\r\n"));
        assert!(is_metrics_get("GET /_metrics?foo=bar HTTP/1.1\r\n"));
        assert!(!is_metrics_get("GET /healthz HTTP/1.1\r\n"));
        assert!(!is_metrics_get("POST /_metrics HTTP/1.1\r\n"));
    }

    fn http_get(addr: std::net::SocketAddr, path: &str) -> String {
        let mut stream = TcpStream::connect(addr).unwrap();
        stream
            .write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        response
    }

    #[test]
    fn http_roundtrip_serves_metrics_and_404s() {
        let handle = serve_metrics("127.0.0.1:0".parse().unwrap(), || {
            "reverse_rusty_test 1\n".to_string()
        })
        .unwrap();
        let addr = handle.addr();

        let ok = http_get(addr, "/_metrics");
        assert!(ok.starts_with("HTTP/1.1 200 OK"), "got: {ok}");
        assert!(ok.contains("text/plain; version=0.0.4"));
        assert!(ok.contains("reverse_rusty_test 1"));

        let alias = http_get(addr, "/metrics");
        assert!(alias.contains("reverse_rusty_test 1"));

        let missing = http_get(addr, "/nope");
        assert!(
            missing.starts_with("HTTP/1.1 404 Not Found"),
            "got: {missing}"
        );

        handle.shutdown();
    }

    #[test]
    fn oversized_request_line_does_not_wedge_the_listener() {
        // A newline-less line far larger than the cap must NOT grow memory unbounded or block the
        // single metrics thread forever — the DoS-hardening regression (codex). The bounded read
        // returns and the connection is closed; we tolerate any outcome on this bad connection (the
        // server closes with bytes still unread, which surfaces as a connection reset). The real
        // assertion is that a NORMAL scrape still succeeds afterward — one bad client didn't wedge it.
        let handle = serve_metrics("127.0.0.1:0".parse().unwrap(), || {
            "reverse_rusty_test 1\n".to_string()
        })
        .unwrap();
        let addr = handle.addr();
        {
            let mut bad = TcpStream::connect(addr).unwrap();
            let huge = vec![b'A'; super::MAX_REQUEST_BYTES as usize + 2000];
            let _ = bad.write_all(&huge);
            let mut sink = Vec::new();
            let _ = bad.read_to_end(&mut sink);
        }
        let ok = http_get(addr, "/_metrics");
        assert!(
            ok.starts_with("HTTP/1.1 200 OK"),
            "listener wedged; got: {ok}"
        );
        assert!(ok.contains("reverse_rusty_test 1"));
        handle.shutdown();
    }
}
