//! A minimal in-process S3-compatible server, for protocol-level probes.
//!
//! Every other piece of evidence in this repository runs against an in-memory
//! backend, which has no HTTP layer at all: no ETag headers, no conditional
//! request headers, no 404/412 error documents, no `ListObjectsV2` XML. This
//! server exists so a probe can drive `object_store`'s **real S3 client** —
//! SigV4 signing, header parsing, conditional `PUT`, list pagination — against
//! a socket, with no credentials and no network dependency on a bucket.
//!
//! It is probe tooling, never a product path: no shipped crate depends on
//! `qm-probe`, and the fake bucket below has no authentication, no multipart
//! upload, and no durability.
//!
//! What it models, and why each piece is load-bearing:
//!
//! - **ETag identity**: every accepted write gets a fresh ETag, echoed on
//!   `PUT`, `GET`, `HEAD` and in listings. The CAS contract needs an identity
//!   that moves on every write, not a content hash.
//! - **Conditional writes**: `If-None-Match: *` refuses an existing key, and
//!   `If-Match: <etag>` refuses a stale one, both with `412` and S3 error XML.
//!   `If-Match` against a missing key answers `404`, which is what S3 does and
//!   what the client translates into a precondition failure.
//! - **Atomic check-and-set**: the precondition check and the write happen
//!   under one lock, and every connection is served on its own thread, so two
//!   genuinely concurrent conditional writes are decided by the lock rather
//!   than by the accept backlog.
//! - **R2's PUT-only version id**: with [`StubOptions::put_version_id`] the
//!   `x-amz-version-id` header appears on `PUT` responses only — the shape
//!   `docs/design.md` §5.1 claims the CAS layer is immune to. GET/HEAD never
//!   repeat it.
//! - **Conflict retries**: with [`StubOptions::with_conditional_put_conflicts`]
//!   the next N conditional writes that *satisfy* their preconditions answer
//!   `409 Conflict` instead of writing — the answer real S3 gives when
//!   concurrent `If-Match` writes are in flight, and the one write mode
//!   `object_store` retries on that status. A count of one proves the retry
//!   covers it; a count past the client's retry budget proves a bucket that
//!   never recovers still fails the probe rather than passing quietly.
//!
//! Still not modelled: request signatures (the `Authorization` header is
//! recorded and ignored), multipart uploads, real XML variants, durability,
//! latency, quotas and region behaviour — and conditional **reads**, which
//! are refused rather than answered (see [`State::get`]). See
//! `docs/design.md` §5.1.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// How long one connection may take to deliver a request line and body.
///
/// A client that stalls mid-request would otherwise park a handler thread for
/// good; the timeout turns that into a dropped connection, which the caller
/// sees as an error rather than a hang.
const READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Something the stub does wrong on purpose, so a probe that trusts the
/// backend can be shown failing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    /// Accept every `If-Match`, however stale. This is the "backend silently
    /// ignores preconditions" bug that loses writes under concurrency.
    IgnoreIfMatch,
    /// Answer `GET`/`HEAD` with an ETag that no accepted write ever returned.
    StaleReadEtag,
    /// Answer the *first* page of a listing as if it were the whole listing:
    /// `IsTruncated` says false and no continuation token is offered. A client
    /// that trusts the answer sees a smaller bucket, so a reader that
    /// materialises an index from it gets a partial directory.
    TruncateListing,
    /// Answer `GET` with only the first byte of the object. The
    /// `Content-Length` still describes what was actually sent, so the
    /// transfer succeeds and the caller has to notice the bytes are wrong on
    /// its own — the failure a replica that hands back a damaged object
    /// produces.
    TruncateReadBody,
}

/// Configuration for the stub bucket.
#[derive(Clone, Debug)]
pub struct StubOptions {
    /// Bucket name the stub answers for; anything else is `NoSuchBucket`.
    pub bucket: String,
    /// Return `x-amz-version-id` from `PUT` responses only (R2's shape).
    pub put_version_id: bool,
    /// Faults to inject; empty means a well-behaved backend.
    pub faults: Vec<Fault>,
    /// Objects per `ListObjectsV2` page, to force client pagination.
    pub page_size: usize,
    /// How many conditional writes that satisfy their preconditions to answer
    /// with `409 Conflict` before behaving.
    ///
    /// Unlike [`Self::faults`] this is not a broken backend: a real bucket
    /// answers `409` when conflicting `If-Match` writes overlap, and the
    /// client is expected to retry. One is therefore a *transient* condition
    /// the probe must survive; more than the client's retry budget is one it
    /// must not survive silently.
    pub conditional_put_conflicts: usize,
}

impl Default for StubOptions {
    fn default() -> Self {
        Self {
            bucket: "stub-bucket".to_string(),
            put_version_id: false,
            faults: Vec::new(),
            page_size: 1000,
            conditional_put_conflicts: 0,
        }
    }
}

impl StubOptions {
    /// The bucket name this stub answers for.
    #[must_use]
    pub fn with_bucket(mut self, bucket: impl Into<String>) -> Self {
        self.bucket = bucket.into();
        self
    }

    /// Report a PUT-only `x-amz-version-id` (the R2 shape).
    #[must_use]
    pub fn with_put_version_id(mut self, put_version_id: bool) -> Self {
        self.put_version_id = put_version_id;
        self
    }

    /// Add a fault to inject.
    #[must_use]
    pub fn with_fault(mut self, fault: Fault) -> Self {
        self.faults.push(fault);
        self
    }

    /// Serve at most `page_size` objects per list response.
    #[must_use]
    pub fn with_page_size(mut self, page_size: usize) -> Self {
        self.page_size = page_size.max(1);
        self
    }

    /// Answer the next `count` conditional writes that satisfy their
    /// preconditions with `409 Conflict`, then behave normally.
    #[must_use]
    pub fn with_conditional_put_conflicts(mut self, count: usize) -> Self {
        self.conditional_put_conflicts = count;
        self
    }
}

/// One request the stub actually received, after the response was written.
///
/// The headers are the ones the CAS contract depends on, so a test can assert
/// on what crossed the wire rather than on what the client intended to send.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordedRequest {
    /// HTTP method.
    pub method: String,
    /// Request target as sent, including the query string.
    pub target: String,
    /// `If-Match`, if the request carried one.
    pub if_match: Option<String>,
    /// `If-None-Match`, if the request carried one.
    pub if_none_match: Option<String>,
    /// `Authorization`, if the request carried one (recorded, never checked).
    pub authorization: Option<String>,
    /// Path the request was routed to, with the bucket removed.
    pub key: String,
    /// Status the stub answered with.
    pub status: u16,
}

struct StoredObject {
    bytes: Vec<u8>,
    etag: String,
    last_modified: u64,
    version_id: String,
}

struct State {
    options: StubOptions,
    objects: Mutex<BTreeMap<String, StoredObject>>,
    requests: Mutex<Vec<RecordedRequest>>,
    etag_seq: AtomicU64,
    version_seq: AtomicU64,
    /// Conditional writes left to answer with an injected `409`.
    conditional_put_conflicts: AtomicUsize,
}

/// A running stub server.
///
/// Dropping the handle without [`S3Stub::shutdown`] leaves the listener thread
/// parked in `accept`; tests call `shutdown` so the thread joins.
pub struct S3Stub {
    addr: SocketAddr,
    state: Arc<State>,
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl S3Stub {
    /// Start a well-behaved stub on an ephemeral loopback port.
    ///
    /// # Errors
    /// Fails when the listener cannot bind.
    pub fn start() -> std::io::Result<Self> {
        Self::start_with(StubOptions::default())
    }

    /// Start a stub with the given options on an ephemeral loopback port.
    ///
    /// # Errors
    /// Fails when the listener cannot bind.
    pub fn start_with(options: StubOptions) -> std::io::Result<Self> {
        Self::start_on(0, options)
    }

    /// Start a stub on a chosen loopback port (`0` picks an ephemeral one).
    ///
    /// The standalone `s3-stub` binary uses this to publish a fixed port for
    /// the probe *binaries*, which can only reach a stub over a socket.
    ///
    /// # Errors
    /// Fails when the listener cannot bind.
    pub fn start_on(port: u16, options: StubOptions) -> std::io::Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", port))?;
        let addr = listener.local_addr()?;
        let state = Arc::new(State {
            conditional_put_conflicts: AtomicUsize::new(options.conditional_put_conflicts),
            options,
            objects: Mutex::new(BTreeMap::new()),
            requests: Mutex::new(Vec::new()),
            etag_seq: AtomicU64::new(0),
            version_seq: AtomicU64::new(0),
        });
        let shutdown = Arc::new(AtomicBool::new(false));
        let serve_state = Arc::clone(&state);
        let serve_shutdown = Arc::clone(&shutdown);
        let handle = thread::spawn(move || serve(listener, &serve_state, &serve_shutdown));
        Ok(Self {
            addr,
            state,
            shutdown,
            handle: Some(handle),
        })
    }

    /// The address the stub is listening on.
    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The port the stub is listening on.
    #[must_use]
    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    /// The endpoint to hand to an S3 client, e.g. `http://127.0.0.1:53421`.
    #[must_use]
    pub fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// The bucket name the stub answers for.
    #[must_use]
    pub fn bucket(&self) -> &str {
        &self.state.options.bucket
    }

    /// Every request served so far, in completion order.
    #[must_use]
    pub fn requests(&self) -> Vec<RecordedRequest> {
        self.state
            .requests
            .lock()
            .expect("stub request log lock")
            .clone()
    }

    /// Snapshot of the stored bytes, keyed by object key.
    #[must_use]
    pub fn objects(&self) -> BTreeMap<String, Vec<u8>> {
        self.state
            .objects
            .lock()
            .expect("stub object lock")
            .iter()
            .map(|(key, object)| (key.clone(), object.bytes.clone()))
            .collect()
    }

    /// Stop accepting connections and join the listener thread.
    pub fn shutdown(mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        // Wake the parked `accept` so the loop can observe the flag and exit.
        let _ = TcpStream::connect(self.addr);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn serve(listener: TcpListener, state: &Arc<State>, shutdown: &Arc<AtomicBool>) {
    for stream in listener.incoming() {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }
        let Ok(stream) = stream else { continue };
        let state = Arc::clone(state);
        // Thread per connection on purpose: the atomicity of the stub's
        // check-and-set is what makes the concurrent-write probe meaningful,
        // and a single-threaded accept loop would serialise the race away.
        thread::spawn(move || {
            let _ = serve_connection(stream, &state);
        });
    }
}

fn serve_connection(mut stream: TcpStream, state: &State) -> std::io::Result<()> {
    // Not `let _ =`: a platform that refuses a read deadline would leave a
    // stalled client able to park this handler forever, and nothing else in
    // the suite would notice.
    stream.set_read_timeout(Some(READ_TIMEOUT))?;
    let reader = BufReader::new(stream.try_clone()?);
    let Some(request) = read_request(reader, &mut stream)? else {
        return Ok(());
    };
    let response = state.handle(&request);
    let key = route(&request.target).key;
    state
        .requests
        .lock()
        .expect("stub request log lock")
        .push(RecordedRequest {
            method: request.method.clone(),
            target: request.target.clone(),
            if_match: header(&request.headers, "if-match").map(str::to_string),
            if_none_match: header(&request.headers, "if-none-match").map(str::to_string),
            authorization: header(&request.headers, "authorization").map(str::to_string),
            key,
            status: response.status,
        });
    write_response(&mut stream, &request.method, &response)?;
    stream.flush()?;
    let _ = stream.shutdown(Shutdown::Both);
    Ok(())
}

struct Request {
    method: String,
    target: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_str())
}

fn read_request(
    mut reader: BufReader<TcpStream>,
    stream: &mut TcpStream,
) -> std::io::Result<Option<Request>> {
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            return Ok(None);
        }
        if line.trim().is_empty() {
            continue;
        }
        break;
    }
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let target = parts.next().unwrap_or_default().to_string();

    let mut headers = Vec::new();
    loop {
        let mut raw = String::new();
        if reader.read_line(&mut raw)? == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "the client closed the connection mid-headers",
            ));
        }
        if raw.trim().is_empty() {
            break;
        }
        if let Some((name, value)) = raw.split_once(':') {
            headers.push((name.trim().to_ascii_lowercase(), value.trim().to_string()));
        }
    }

    // reqwest does not send `Expect` by default, but a client that does would
    // otherwise wait for this interim response before sending its body.
    if header(&headers, "expect").is_some_and(|value| value.eq_ignore_ascii_case("100-continue")) {
        stream.write_all(b"HTTP/1.1 100 Continue\r\n\r\n")?;
        stream.flush()?;
    }

    let body = match header(&headers, "transfer-encoding") {
        Some(encoding) if encoding.to_ascii_lowercase().contains("chunked") => {
            read_chunked_body(&mut reader)?
        }
        _ => match header(&headers, "content-length").and_then(|v| v.parse::<usize>().ok()) {
            Some(length) if length > 0 => {
                let mut buffer = vec![0u8; length];
                reader.read_exact(&mut buffer)?;
                buffer
            }
            _ => Vec::new(),
        },
    };

    Ok(Some(Request {
        method,
        target,
        headers,
        body,
    }))
}

fn read_chunked_body(reader: &mut impl BufRead) -> std::io::Result<Vec<u8>> {
    let mut body = Vec::new();
    loop {
        let mut size_line = String::new();
        if reader.read_line(&mut size_line)? == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "the chunked body ended before its zero-length chunk",
            ));
        }
        let digits = size_line.trim().split(';').next().unwrap_or_default();
        let size = usize::from_str_radix(digits, 16).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("not a chunk size: {digits:?}"),
            )
        })?;
        if size == 0 {
            // Consume any trailer headers up to the terminating blank line.
            loop {
                let mut trailer = String::new();
                if reader.read_line(&mut trailer)? == 0 || trailer.trim().is_empty() {
                    return Ok(body);
                }
            }
        }
        let mut chunk = vec![0u8; size];
        reader.read_exact(&mut chunk)?;
        body.extend_from_slice(&chunk);
        let mut terminator = [0u8; 2];
        reader.read_exact(&mut terminator)?;
    }
}

struct Route {
    bucket: String,
    key: String,
    query: String,
}

fn route(target: &str) -> Route {
    let (path, query) = match target.split_once('?') {
        Some((path, query)) => (path, query),
        None => (target, ""),
    };
    let decoded = percent_decode(path);
    let rest = decoded.trim_matches('/');
    let (bucket, key) = match rest.split_once('/') {
        Some((bucket, key)) => (bucket.to_string(), key.to_string()),
        None => (rest.to_string(), String::new()),
    };
    Route {
        bucket,
        key,
        query: query.to_string(),
    }
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            let hex = &value[index + 1..index + 3];
            if let Ok(byte) = u8::from_str_radix(hex, 16) {
                out.push(byte);
                index += 3;
                continue;
            }
        }
        out.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn parse_query(query: &str) -> BTreeMap<String, String> {
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| match pair.split_once('=') {
            Some((key, value)) => (percent_decode(key), percent_decode(value)),
            None => (percent_decode(pair), String::new()),
        })
        .collect()
}

struct Response {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Response {
    fn empty(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    fn xml(status: u16, body: String) -> Self {
        Self {
            status,
            headers: vec![("content-type".to_string(), "application/xml".to_string())],
            body: body.into_bytes(),
        }
    }

    /// An S3 error document. Real buckets never answer a failure with an empty
    /// body, and a client that parses the code should not have to cope with
    /// one here either.
    fn error(status: u16, code: &str, message: &str) -> Self {
        Self::xml(
            status,
            format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{code}</Code>\
                 <Message>{}</Message></Error>",
                escape_xml(message)
            ),
        )
    }
}

impl State {
    fn faulted(&self, fault: Fault) -> bool {
        self.options.faults.contains(&fault)
    }

    fn handle(&self, request: &Request) -> Response {
        let route = route(&request.target);
        if route.bucket != self.options.bucket {
            return Response::error(
                404,
                "NoSuchBucket",
                &format!("the stub only serves {:?}", self.options.bucket),
            );
        }
        match request.method.as_str() {
            "PUT" if !route.key.is_empty() => self.put(&route.key, request),
            "GET" if route.key.is_empty() => self.list(&route.query),
            "GET" | "HEAD" => self.get(&route.key, request),
            "DELETE" if !route.key.is_empty() => self.delete(&route.key),
            other => Response::error(
                501,
                "NotImplemented",
                &format!("the stub does not implement {other} on this target"),
            ),
        }
    }

    /// Consume one injected conflict, if any are left to spend.
    fn take_conditional_put_conflict(&self) -> bool {
        let mut remaining = self.conditional_put_conflicts.load(Ordering::SeqCst);
        while remaining > 0 {
            match self.conditional_put_conflicts.compare_exchange(
                remaining,
                remaining - 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return true,
                Err(actual) => remaining = actual,
            }
        }
        false
    }

    /// Check the preconditions and store the body as one atomic step.
    fn put(&self, key: &str, request: &Request) -> Response {
        let if_match = header(&request.headers, "if-match");
        let if_none_match = header(&request.headers, "if-none-match");

        let mut objects = self.objects.lock().expect("stub object lock");

        if if_none_match.is_some_and(|value| value.trim() == "*") && objects.contains_key(key) {
            return Response::error(412, "PreconditionFailed", "the object already exists");
        }

        if let Some(expected) = if_match {
            // The fault is injected here rather than in the comparison so the
            // rest of the response stays byte-identical to a correct backend.
            if !self.faulted(Fault::IgnoreIfMatch) {
                // `If-Match: *` is HTTP's "any current representation" form:
                // it asserts existence, not identity. Comparing it as an ETag
                // would refuse every object that does exist.
                let any_representation = expected.trim() == "*";
                match objects.get(key) {
                    None => return Response::error(404, "NoSuchKey", "the object does not exist"),
                    Some(existing)
                        if !any_representation
                            && normalize_etag(expected) != normalize_etag(&existing.etag) =>
                    {
                        return Response::error(
                            412,
                            "PreconditionFailed",
                            "the object moved: the If-Match ETag is not the stored one",
                        );
                    }
                    Some(_) => {}
                }
            }
        }

        // Injected conflict, and deliberately *after* the preconditions: this
        // models a write that would have been accepted losing to a concurrent
        // one, which is what S3's 409 means for `If-Match`. Nothing is stored,
        // so the client's retry of the same request is the one that lands.
        if if_match.is_some() && self.take_conditional_put_conflict() {
            return Response::error(
                409,
                "Conflict",
                "a conflicting conditional write is in flight (injected)",
            );
        }

        let seq = self.etag_seq.fetch_add(1, Ordering::SeqCst) + 1;
        let etag = format!("\"stub-{seq}-{}\"", request.body.len());
        let stored = StoredObject {
            bytes: request.body.clone(),
            etag: etag.clone(),
            last_modified: now_seconds(),
            // A version id belongs to the object, not to the response: the
            // R2 shape below is that only *this* response ever repeats it.
            version_id: format!(
                "stub-version-{}",
                self.version_seq.fetch_add(1, Ordering::SeqCst) + 1
            ),
        };
        let version_id = stored.version_id.clone();
        objects.insert(key.to_string(), stored);
        drop(objects);

        let mut headers = vec![("etag".to_string(), etag)];
        if self.options.put_version_id {
            headers.push(("x-amz-version-id".to_string(), version_id));
        }
        Response {
            status: 200,
            headers,
            body: Vec::new(),
        }
    }

    /// The body stays in the response even for `HEAD`: it is what the
    /// `Content-Length` header has to advertise, and the writer drops the body
    /// bytes for a HEAD request.
    ///
    /// Conditional **reads** are refused instead of answered. A `GET` that
    /// ignores `If-None-Match` answers `200` with the body, and that is not a
    /// missing answer but a *wrong* one: a probe built on it would look green
    /// for the wrong reason. `501` keeps the surface honest — the same call
    /// the stub makes for delimiters in [`Self::list`] — and nothing in this
    /// repository performs a conditional read today.
    fn get(&self, key: &str, request: &Request) -> Response {
        if let Some(name) = ["if-none-match", "if-match"]
            .into_iter()
            .find(|name| header(&request.headers, name).is_some())
        {
            return Response::error(
                501,
                "NotImplemented",
                &format!("the stub does not implement conditional reads ({name} on GET/HEAD)"),
            );
        }
        let objects = self.objects.lock().expect("stub object lock");
        let Some(object) = objects.get(key) else {
            return Response::error(404, "NoSuchKey", &format!("no object at {key:?}"));
        };
        let etag = if self.faulted(Fault::StaleReadEtag) {
            format!("\"stale-{}\"", object.etag.trim_matches('"'))
        } else {
            object.etag.clone()
        };
        // The fault shortens the body, not the header: a caller that only
        // checks that the transfer completed still sees a success, so the
        // damage has to be caught by whatever parses the object.
        let body = if self.faulted(Fault::TruncateReadBody) {
            object.bytes[..object.bytes.len().min(1)].to_vec()
        } else {
            object.bytes.clone()
        };
        Response {
            status: 200,
            headers: vec![
                ("etag".to_string(), etag),
                ("last-modified".to_string(), rfc2822(object.last_modified)),
                (
                    "content-type".to_string(),
                    "application/octet-stream".to_string(),
                ),
            ],
            body,
        }
    }

    /// `DELETE` is idempotent in S3: removing a key that is already gone is a
    /// success, not a `404`.
    fn delete(&self, key: &str) -> Response {
        self.objects.lock().expect("stub object lock").remove(key);
        Response::empty(204)
    }

    fn list(&self, query: &str) -> Response {
        let params = parse_query(query);
        if params.get("list-type").map(String::as_str) != Some("2") {
            return Response::error(
                501,
                "NotImplemented",
                "the stub only implements ListObjectsV2 (list-type=2)",
            );
        }
        if params
            .get("delimiter")
            .is_some_and(|value| !value.is_empty())
        {
            return Response::error(
                501,
                "NotImplemented",
                "the stub lists without a delimiter; nothing in this repository asks for one",
            );
        }
        let prefix = params.get("prefix").cloned().unwrap_or_default();
        let start_after = params
            .get("continuation-token")
            .or_else(|| params.get("start-after"))
            .cloned();
        let requested = params
            .get("max-keys")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(1000);
        let page_size = requested.min(self.options.page_size).max(1);

        let objects = self.objects.lock().expect("stub object lock");
        let mut keys: Vec<(&String, &StoredObject)> = objects
            .iter()
            .filter(|(key, _)| key.starts_with(&prefix))
            .collect();
        if let Some(after) = &start_after {
            keys.retain(|(key, _)| key.as_str() > after.as_str());
        }
        // The fault claims the first page answers the whole listing, which is
        // the shape a reader cannot tell apart from a small bucket.
        let truncated = keys.len() > page_size && !self.faulted(Fault::TruncateListing);
        let page = &keys[..page_size.min(keys.len())];

        let mut body = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        body.push_str(&format!(
            "<ListBucketResult xmlns=\"{XMLNS}\"><Name>{}</Name><Prefix>{}</Prefix>\
             <KeyCount>{}</KeyCount><MaxKeys>{requested}</MaxKeys>\
             <IsTruncated>{truncated}</IsTruncated>",
            escape_xml(&self.options.bucket),
            escape_xml(&prefix),
            page.len(),
        ));
        if truncated && let Some((key, _)) = page.last() {
            body.push_str(&format!(
                "<NextContinuationToken>{}</NextContinuationToken>",
                escape_xml(key)
            ));
        }
        for (key, object) in page {
            body.push_str(&format!(
                "<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>&quot;{}&quot;</ETag>\
                 <Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
                escape_xml(key),
                iso8601(object.last_modified),
                escape_xml(object.etag.trim_matches('"')),
                object.bytes.len(),
            ));
        }
        body.push_str("</ListBucketResult>");
        Response::xml(200, body)
    }
}

const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

fn normalize_etag(value: &str) -> String {
    let value = value.trim();
    let value = value.strip_prefix("W/").unwrap_or(value);
    value.trim_matches('"').to_string()
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn write_response(
    stream: &mut TcpStream,
    method: &str,
    response: &Response,
) -> std::io::Result<()> {
    let mut head = format!(
        "HTTP/1.1 {} {}\r\n",
        response.status,
        reason(response.status)
    );
    for (name, value) in &response.headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str(&format!("content-length: {}\r\n", response.body.len()));
    head.push_str("connection: close\r\n\r\n");
    stream.write_all(head.as_bytes())?;
    if method != "HEAD" {
        stream.write_all(&response.body)?;
    }
    Ok(())
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        409 => "Conflict",
        412 => "Precondition Failed",
        501 => "Not Implemented",
        _ => "Internal Server Error",
    }
}

/// `"Tue, 16 Sep 2026 03:04:05 GMT"`, which is what `Last-Modified` has to be
/// for the client's RFC2822 parser.
fn rfc2822(seconds: u64) -> String {
    const WEEKDAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let days = (seconds / 86_400) as i64;
    let (year, month, day) = civil_from_days(days);
    let weekday = WEEKDAYS[((days % 7 + 4 + 7) % 7) as usize];
    let second_of_day = seconds % 86_400;
    format!(
        "{weekday}, {day:02} {} {year:04} {:02}:{:02}:{:02} GMT",
        MONTHS[(month - 1) as usize],
        second_of_day / 3600,
        (second_of_day % 3600) / 60,
        second_of_day % 60,
    )
}

/// `"2026-09-16T03:04:05.000Z"`, the shape `ListObjectsV2` uses.
fn iso8601(seconds: u64) -> String {
    let days = (seconds / 86_400) as i64;
    let (year, month, day) = civil_from_days(days);
    let second_of_day = seconds % 86_400;
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.000Z",
        second_of_day / 3600,
        (second_of_day % 3600) / 60,
        second_of_day % 60,
    )
}

/// Days since 1970-01-01 to a civil date (Howard Hinnant's algorithm), so the
/// stub can emit HTTP dates without pulling in a calendar dependency.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = (z - era * 146_097) as u64;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era as i64 + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * month_prime + 2) / 5 + 1) as u32;
    let month = if month_prime < 10 {
        month_prime + 3
    } else {
        month_prime - 9
    } as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_dates_match_a_calendar() {
        // Three points pin the leap-year rules and the weekday: the epoch, a
        // century leap day, and the timestamp the S3 probe issue was filed.
        assert_eq!(rfc2822(0), "Thu, 01 Jan 1970 00:00:00 GMT");
        assert_eq!(rfc2822(951_782_400), "Tue, 29 Feb 2000 00:00:00 GMT");
        assert_eq!(rfc2822(1_789_493_741), "Tue, 15 Sep 2026 17:35:41 GMT");

        assert_eq!(iso8601(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(iso8601(951_782_400), "2000-02-29T00:00:00.000Z");
        assert_eq!(iso8601(1_789_493_741), "2026-09-15T17:35:41.000Z");
        assert_eq!(iso8601(86_399), "1970-01-01T23:59:59.000Z");
    }

    #[test]
    fn request_targets_are_split_and_decoded() {
        let parsed = route("/bucket/a%20b/c%2Bd?prefix=x%2Fy&list-type=2");
        assert_eq!(parsed.bucket, "bucket");
        assert_eq!(parsed.key, "a b/c+d");
        let params = parse_query(&parsed.query);
        assert_eq!(params.get("prefix").map(String::as_str), Some("x/y"));
        assert_eq!(params.get("list-type").map(String::as_str), Some("2"));

        let bucket_only = route("/bucket?list-type=2");
        assert_eq!(bucket_only.bucket, "bucket");
        assert!(bucket_only.key.is_empty());
    }

    #[test]
    fn etag_comparison_ignores_quoting_and_weakness() {
        assert_eq!(normalize_etag("\"abc\""), "abc");
        assert_eq!(normalize_etag("W/\"abc\""), "abc");
        assert_eq!(normalize_etag("  abc "), "abc");
        assert_ne!(normalize_etag("\"abc\""), normalize_etag("\"abd\""));
    }

    #[test]
    fn xml_escaping_covers_the_characters_a_key_can_carry() {
        assert_eq!(escape_xml("a&b<c>\"d\""), "a&amp;b&lt;c&gt;&quot;d&quot;");
    }
}
