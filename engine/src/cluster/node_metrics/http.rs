use super::{
    thread, Arc, AtomicBool, BufRead, BufReader, JoinHandle, Ordering, Read, SocketAddr,
    TcpListener, TcpStream, Write, ACCEPT_POLL, CONTENT_TYPE, MAX_REQUEST_BYTES, READ_TIMEOUT,
};
use std::io;

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
pub(super) fn is_metrics_get(line: &str) -> bool {
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
