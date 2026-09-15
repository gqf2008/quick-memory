//! The search probe, driven over a real S3 protocol socket.
//!
//! Every other piece of "another machine can search the published corpus"
//! evidence in this repository runs against `object_store`'s `InMemory`
//! backend, which has no HTTP layer at all. `search-probe build` and
//! `search-probe query` are what that promise actually rests on, so this test
//! runs them as **separate processes** against
//! [`qm_probe::s3_stub::S3Stub`]: one process publishes a split, a second
//! process — its own handle, its own freshly created cache directory, nothing
//! shared but the bucket — has to find every page the first one wrote.
//!
//! Two of the tests are fault controls. A stub that answers the first page of
//! a listing as if it were the whole listing, and a stub that answers `GET`
//! with a truncated body, both have to break the reader: without them, "the
//! reader found the whole corpus" would also pass against a reader that
//! quietly searched a partial index.
//!
//! What this does not prove is the part that needs a real account:
//! signatures, latency, quotas, region behaviour and R2's own XML variants
//! (see `docs/design.md` §5.1 and §11).

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::process::{Command, Output};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use qm_probe::s3_stub::{Fault, RecordedRequest, S3Stub, StubOptions};

const ACCESS_KEY: &str = "stub-access";
const SECRET_KEY: &str = "stub-secret";

/// The split every one of these tests publishes and reads.
const SPLIT: &str = "split-a";

/// A token every page in the corpus carries, so a query for it must return the
/// whole corpus: a reader that missed part of the split answers short, and a
/// reader that searched someone else's bytes answers differently.
const SHARED: &str = "zephyrquill";

/// The pages the corpus is made of. The last one matters most: a listing that
/// stops after its first page is exactly a reader that never hears about it.
const PATHS: [&str; 3] = ["notes/alpha.md", "notes/beta.md", "notes/gamma.md"];

/// The far end of the vector chain: process A writes under this scope and a
/// fresh process B resolves the same scope from the same S3 stub.
const VECTOR_WORKSPACE: &str = "probe-vector-ws";
const VECTOR_PROJECT: &str = "probe-vector-proj";
const VECTOR_MODEL_A: &str = "vector-model-a";
const VECTOR_MODEL_B: &str = "vector-model-b";
const VECTOR_DIM: usize = 4;
const VECTOR_KEY: &str = "vector-stub-key";
const VECTOR_TARGET_PATH: &str = "notes/agreement.md";
const VECTOR_TARGET_TITLE: &str = "Distributed agreement";
const VECTOR_TARGET_BODY: &str = "A protocol for choosing one coordinator and ordering updates.";
const VECTOR_DISTRACTOR_PATH: &str = "notes/bread.md";
const VECTOR_DISTRACTOR_TITLE: &str = "Sourdough schedule";
const VECTOR_DISTRACTOR_BODY: &str = "Feed the starter and preheat the oven.";
const VECTOR_QUERY: &str = "How do machines reach shared decisions?";

/// One JSONL corpus, `PATHS.len()` pages, every one carrying [`SHARED`].
fn corpus_jsonl() -> String {
    let mut out = String::new();
    for (index, path) in PATHS.iter().enumerate() {
        let doc = serde_json::json!({
            "workspace_id": "probe-ws",
            "project_id": "probe-proj",
            "path": path,
            "page_id": format!("{:0>64}", index + 1),
            "title": format!("note {}", index + 1),
            "body": format!("{SHARED} page {path} body {}", index + 1),
            "updated_at_ms": 1000 * (index as i64 + 1),
        });
        out.push_str(&doc.to_string());
        out.push('\n');
    }
    out
}

/// Write the corpus where a probe process can read it.
fn write_corpus(dir: &Path) -> std::path::PathBuf {
    let docs = dir.join("machine-a.jsonl");
    std::fs::write(&docs, corpus_jsonl()).expect("writing the corpus");
    docs
}

/// Source pages for `vector-publish`: no page id or embedding is supplied by
/// the caller; the authority commit and the configured provider produce both.
fn vector_corpus_jsonl() -> String {
    let mut out = String::new();
    for (path, title, body) in [
        (VECTOR_TARGET_PATH, VECTOR_TARGET_TITLE, VECTOR_TARGET_BODY),
        (
            VECTOR_DISTRACTOR_PATH,
            VECTOR_DISTRACTOR_TITLE,
            VECTOR_DISTRACTOR_BODY,
        ),
    ] {
        let page = serde_json::json!({
            "path": path,
            "title": title,
            "body": body,
        });
        out.push_str(&page.to_string());
        out.push('\n');
    }
    out
}

fn write_vector_corpus(dir: &Path) -> std::path::PathBuf {
    let docs = dir.join("vector-machine-a.jsonl");
    std::fs::write(&docs, vector_corpus_jsonl()).expect("writing the vector corpus");
    docs
}

/// Run the `search-probe` binary against `stub`, hermetically.
///
/// The probe processes are the thing under test here: each call is a fresh
/// OS process with an empty environment, so nothing it knows can come from
/// this one.
fn probe_with_env(stub: &S3Stub, args: &[&str], extra_env: &[(&str, String)]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_search-probe"));
    command
        .args(args)
        .env_clear()
        .env("QM_S3_ENDPOINT", stub.endpoint())
        .env("QM_S3_BUCKET", stub.bucket())
        .env("QM_S3_ACCESS_KEY_ID", ACCESS_KEY)
        .env("QM_S3_SECRET_ACCESS_KEY", SECRET_KEY)
        .env("QM_S3_FORCE_PATH_STYLE", "true")
        // The probe logs tracing to stdout and prints its answer there too;
        // silence the logs rather than pattern-match around them.
        .env("RUST_LOG", "off");
    for (name, value) in extra_env {
        command.env(name, value);
    }
    command.output().expect("running the search-probe binary")
}

fn probe(stub: &S3Stub, args: &[&str]) -> Output {
    probe_with_env(stub, args, &[])
}

/// `search-probe --split-prefix split-a build <docs>` in its own process.
fn build(stub: &S3Stub, docs: &Path) -> Output {
    probe(
        stub,
        &[
            "--split-prefix",
            SPLIT,
            "build",
            docs.to_str().expect("the corpus path is valid UTF-8"),
        ],
    )
}

/// `search-probe --split-prefix split-a query <text>` in its own process.
fn query(stub: &S3Stub, text: &str) -> Output {
    probe(stub, &["--split-prefix", SPLIT, "query", text])
}

/// Publish the corpus, insisting that the publish itself worked.
fn publish(stub: &S3Stub, docs: &Path) -> Vec<String> {
    let output = build(stub, docs);
    assert!(
        output.status.success(),
        "the build must succeed before the reader can be judged:\n{}",
        combined(&output)
    );
    // Every object the reader will have to fetch, as the bucket itself sees it.
    stub.objects().into_keys().collect()
}

/// The page paths in the probe's JSON answer, or a failure quoting its output.
fn hits(output: &Output) -> Vec<String> {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let answer: serde_json::Value = serde_json::from_str(&stdout).unwrap_or_else(|error| {
        panic!(
            "the query must answer with one JSON object: {error}\n{}",
            combined(output)
        )
    });
    answer["hits"]
        .as_array()
        .unwrap_or_else(|| panic!("the answer must carry hits:\n{}", combined(output)))
        .iter()
        .map(|hit| {
            hit["path"]
                .as_str()
                .expect("every hit names its page")
                .to_string()
        })
        .collect()
}

/// The wire requests the stub served in `window`, filtered to one method.
///
/// A listing is itself a `GET` (of the bucket, not of an object), so callers
/// that mean "fetched an object" want [`object_gets`] instead.
fn with_method<'a>(window: &'a [RecordedRequest], method: &str) -> Vec<&'a RecordedRequest> {
    window
        .iter()
        .filter(|request| request.method == method)
        .collect()
}

/// The object reads in `window` — every `GET` that is not a listing.
fn object_gets(window: &[RecordedRequest]) -> Vec<&RecordedRequest> {
    window
        .iter()
        .filter(|request| request.method == "GET" && !request.target.contains("list-type=2"))
        .collect()
}

/// The listing requests in `window`.
fn listings(window: &[RecordedRequest]) -> Vec<&RecordedRequest> {
    window
        .iter()
        .filter(|request| request.target.contains("list-type=2"))
        .collect()
}

fn combined(output: &Output) -> String {
    format!(
        "exit={:?}\n--- stdout ---\n{}\n--- stderr ---\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EmbeddingRequest {
    model: String,
    inputs: Vec<String>,
}

fn target_embedding_text() -> String {
    format!("{VECTOR_TARGET_TITLE}\n\n{VECTOR_TARGET_BODY}")
}

fn distractor_embedding_text() -> String {
    format!("{VECTOR_DISTRACTOR_TITLE}\n\n{VECTOR_DISTRACTOR_BODY}")
}

/// A hand-written "model": the target and the query point the same way, the
/// distractor is orthogonal, and every other input is refused rather than
/// silently given a fallback vector.
fn vector_for(input: &str) -> Option<Vec<f32>> {
    if input == target_embedding_text() || input == VECTOR_QUERY {
        Some(vec![1.0, 0.0, 0.0, 0.0])
    } else if input == distractor_embedding_text() {
        Some(vec![0.0, 1.0, 0.0, 0.0])
    } else {
        None
    }
}

/// A real HTTP OpenAI-compatible embeddings stub in the test process.
///
/// The child probes are separate OS processes; this listener is the only way
/// they can obtain a query or page vector, so a successful run proves the
/// provider was reached over HTTP rather than substituted in-process.
struct EmbeddingStub {
    address: SocketAddr,
    seen: Arc<Mutex<Vec<EmbeddingRequest>>>,
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl EmbeddingStub {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("binding the embedding stub");
        let address = listener.local_addr().expect("the embedding stub's address");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let shutdown = Arc::new(AtomicBool::new(false));
        let serve_seen = Arc::clone(&seen);
        let serve_shutdown = Arc::clone(&shutdown);
        let handle = std::thread::spawn(move || {
            loop {
                if serve_shutdown.load(Ordering::SeqCst) {
                    break;
                }
                let (stream, _) = listener.accept().expect("accepting an embedding request");
                if serve_shutdown.load(Ordering::SeqCst) {
                    break;
                }
                if let Err(error) = handle_embedding_connection(stream, &serve_seen) {
                    eprintln!("embedding stub request failed: {error}");
                }
            }
        });
        Self {
            address,
            seen,
            shutdown,
            handle: Some(handle),
        }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.address)
    }

    fn seen(&self) -> Vec<EmbeddingRequest> {
        self.seen.lock().expect("embedding request log").clone()
    }

    fn shutdown(mut self) -> Vec<EmbeddingRequest> {
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.address);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        self.seen()
    }
}

fn handle_embedding_connection(
    mut stream: TcpStream,
    seen: &Mutex<Vec<EmbeddingRequest>>,
) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;
    if !request_line.starts_with("POST /embeddings ") {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unexpected request line: {request_line:?}"),
        ));
    }

    let mut content_length = None;
    loop {
        let mut header = String::new();
        reader.read_line(&mut header)?;
        if header == "\r\n" || header.is_empty() {
            break;
        }
        if let Some(value) = header
            .split_once(':')
            .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .map(|(_, value)| value.trim())
        {
            content_length = Some(value.parse::<usize>().map_err(|error| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("bad content-length {value:?}: {error}"),
                )
            })?);
        }
    }
    let content_length = content_length.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "embedding request had no content-length",
        )
    })?;
    let mut body = vec![0_u8; content_length];
    reader.read_exact(&mut body)?;
    let request: serde_json::Value = serde_json::from_slice(&body).map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("embedding request was not JSON: {error}"),
        )
    })?;
    let model = request["model"]
        .as_str()
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "embedding request had no model",
            )
        })?
        .to_string();
    let inputs: Vec<String> = match request.get("input") {
        Some(serde_json::Value::Array(values)) => values
            .iter()
            .map(|value| {
                value.as_str().map(str::to_string).ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "embedding input was not a string",
                    )
                })
            })
            .collect::<std::io::Result<_>>()?,
        Some(serde_json::Value::String(value)) => vec![value.clone()],
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "embedding request had no input",
            ));
        }
    };
    let mut data = Vec::with_capacity(inputs.len());
    for input in &inputs {
        let vector = vector_for(input).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("stub received an unknown input: {input:?}"),
            )
        })?;
        data.push(serde_json::json!({ "embedding": vector }));
    }
    seen.lock()
        .expect("embedding request log")
        .push(EmbeddingRequest { model, inputs });

    let response_body = serde_json::json!({ "data": data }).to_string();
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        response_body.len(),
        response_body
    );
    stream.write_all(response.as_bytes())?;
    stream.flush()?;
    Ok(())
}

/// The positive leg, with the bucket paginating one object per page so the
/// reader has to follow continuation tokens to see the whole split.
#[test]
fn a_second_process_finds_every_page_the_first_one_published() {
    // One object per page: if the reader did not follow the listing it would
    // materialise a single file, which is the control below made real.
    let stub =
        S3Stub::start_with(StubOptions::default().with_page_size(1)).expect("starting the stub");
    let workdir = tempfile::TempDir::new().expect("creating a work directory");
    let docs = write_corpus(workdir.path());

    // Machine A: publish the split.
    let published = publish(&stub, &docs);
    assert!(
        published.len() > 1,
        "a split is more than one object, otherwise the listing carries no information: {published:?}"
    );
    let after_build = stub.requests();
    let uploads = with_method(&after_build, "PUT");
    assert_eq!(
        uploads.len(),
        published.len(),
        "the publish phase must be exactly the objects it uploaded: {after_build:#?}"
    );
    for key in &published {
        assert!(
            uploads.iter().any(|request| &request.key == key),
            "{key} must have been uploaded over the socket: {after_build:#?}"
        );
    }
    assert!(
        with_method(&after_build, "GET").is_empty(),
        "publishing must not read anything back: {after_build:#?}"
    );

    // Machine B: a second process, its own handle, its own empty cache.
    let answer = query(&stub, SHARED);
    assert!(answer.status.success(), "{}", combined(&answer));
    let mut found = hits(&answer);
    found.sort();
    assert_eq!(
        found,
        PATHS.to_vec(),
        "the second process must find every page the first one published:\n{}",
        combined(&answer)
    );

    // The reader's own report has to agree with what the bucket holds.
    let stdout = String::from_utf8_lossy(&answer.stdout);
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout).expect("the answer parses as JSON");
    assert_eq!(
        parsed["materialized_files"],
        serde_json::json!(published.len()),
        "the reader must materialise every object in the bucket:\n{}",
        combined(&answer)
    );

    // ...and the wire agrees too: the second process fetched every key.
    let served = stub.requests();
    let reader_phase = &served[after_build.len()..];
    let fetches = object_gets(reader_phase);
    for key in &published {
        assert!(
            fetches.iter().any(|request| &request.key == key),
            "{key} must have been fetched by the second process: {reader_phase:#?}"
        );
    }
    let lists = listings(reader_phase);
    assert!(
        lists.len() > 1,
        "the reader must have followed the stub's one-object pages, not stopped at the first: {reader_phase:#?}"
    );
    assert!(
        fetches.iter().all(|request| request.status == 200),
        "every fetch must have succeeded: {reader_phase:#?}"
    );

    stub.shutdown();
}

/// Control A: a bucket that answers the first page as if the listing were
/// over. The reader must not report a corpus it never saw.
#[test]
fn a_listing_that_stops_at_its_first_page_cannot_be_searched_as_if_it_were_complete() {
    let workdir = tempfile::TempDir::new().expect("creating a work directory");
    let docs = write_corpus(workdir.path());

    // The same two processes against a healthy bucket: the corpus is found.
    let healthy =
        S3Stub::start_with(StubOptions::default().with_page_size(1)).expect("starting the stub");
    publish(&healthy, &docs);
    let good = query(&healthy, SHARED);
    let mut good_hits = hits(&good);
    good_hits.sort();
    assert_eq!(
        good_hits,
        PATHS.to_vec(),
        "the healthy leg must find the whole corpus, or the control proves nothing:\n{}",
        combined(&good)
    );
    healthy.shutdown();

    // Now the bucket claims its first page is the whole answer.
    let broken = S3Stub::start_with(
        StubOptions::default()
            .with_page_size(1)
            .with_fault(Fault::TruncateListing),
    )
    .expect("starting the stub");
    let published = publish(&broken, &docs);
    let after_build = broken.requests().len();

    let answer = query(&broken, SHARED);
    assert!(
        !answer.status.success(),
        "a listing that stopped at its first page must not answer with a full corpus:\n{}",
        combined(&answer)
    );
    let stderr = String::from_utf8_lossy(&answer.stderr);
    assert!(
        stderr.contains("opening index"),
        "the reader must fail while opening what it materialised:\n{}",
        combined(&answer)
    );
    assert!(
        !String::from_utf8_lossy(&answer.stdout).contains("\"path\""),
        "a failed query must not also print a hit list:\n{}",
        combined(&answer)
    );

    // Tie the failure to the injected fault: the reader was told the bucket
    // holds one object, so it fetched one object, and the index it built from
    // that is missing the rest.
    let served = broken.requests();
    let reader_phase = &served[after_build..];
    let lists = listings(reader_phase);
    let fetches = object_gets(reader_phase);
    assert_eq!(
        lists.len(),
        1,
        "the faulted stub answers the whole listing in one page: {reader_phase:#?}"
    );
    assert_eq!(
        fetches.len(),
        1,
        "and the reader can only fetch what it was told about: {reader_phase:#?}"
    );
    assert!(
        published.len() > 1,
        "the bucket really does hold more than the reader was told: {published:?}"
    );

    broken.shutdown();
}

/// Control B: a bucket that answers `GET` with a truncated body. The transfer
/// succeeds, so only the index parser can notice — and it must.
#[test]
fn a_reader_that_receives_truncated_objects_refuses_the_index() {
    let workdir = tempfile::TempDir::new().expect("creating a work directory");
    let docs = write_corpus(workdir.path());

    let healthy = S3Stub::start_with(StubOptions::default()).expect("starting the stub");
    publish(&healthy, &docs);
    let good = query(&healthy, SHARED);
    assert_eq!(
        hits(&good).len(),
        PATHS.len(),
        "the healthy leg must find the whole corpus, or the control proves nothing:\n{}",
        combined(&good)
    );
    healthy.shutdown();

    let broken = S3Stub::start_with(StubOptions::default().with_fault(Fault::TruncateReadBody))
        .expect("starting the stub");
    let published = publish(&broken, &docs);
    let after_build = broken.requests().len();

    let answer = query(&broken, SHARED);
    assert!(
        !answer.status.success(),
        "an index built from truncated objects must not answer:\n{}",
        combined(&answer)
    );
    assert!(
        String::from_utf8_lossy(&answer.stderr).contains("opening index"),
        "the reader must refuse the index it materialised:\n{}",
        combined(&answer)
    );

    // The load-bearing part: nothing failed in transit. Every object came back
    // with a 200 and a body that matched its own content-length, so the only
    // thing that could have caught this is parsing the bytes.
    let served = broken.requests();
    let reader_phase = &served[after_build..];
    let fetches = object_gets(reader_phase);
    // Per key, like the positive leg: a count alone is satisfied by a reader
    // that fetched one object twice and never asked for another.
    for key in &published {
        assert!(
            fetches.iter().any(|request| &request.key == key),
            "the fault shortens bodies, it does not hide objects: {key} was never fetched: {reader_phase:#?}"
        );
    }
    assert!(
        fetches.iter().all(|request| request.status == 200),
        "every truncated transfer still reports success: {reader_phase:#?}"
    );

    broken.shutdown();
}

/// Process A commits and embeds a vector-bearing project through a real HTTP
/// provider; process B, with its own handle and an empty local cache, queries
/// the same S3 stub. The query has no lexical overlap with either page, so the
/// only stream that can recall the target is `vector`.
#[test]
fn a_second_process_recalls_a_synonym_page_only_through_the_vector_stream_over_real_http() {
    let stub =
        S3Stub::start_with(StubOptions::default().with_page_size(1)).expect("starting the S3 stub");
    let embeddings = EmbeddingStub::start();
    let workdir = tempfile::TempDir::new().expect("creating a work directory");
    let docs = write_vector_corpus(workdir.path());
    let docs_path = docs.to_str().expect("the corpus path is UTF-8");

    let embedding_env = vec![
        ("QM_EMBEDDING_BASE_URL", embeddings.base_url()),
        ("QM_EMBEDDING_API_KEY", VECTOR_KEY.to_string()),
        ("QM_EMBEDDING_MODEL", VECTOR_MODEL_A.to_string()),
        ("QM_EMBEDDING_DIM", VECTOR_DIM.to_string()),
    ];

    // Machine A: authoritative commits, a real embedding call, and a real
    // S3 PUT sequence. The process exits before machine B starts.
    let publish = probe_with_env(
        &stub,
        &[
            "vector-publish",
            docs_path,
            "--workspace",
            VECTOR_WORKSPACE,
            "--project",
            VECTOR_PROJECT,
            "--writer",
            "probe-vector-machine-a",
            "--now-ms",
            "100",
        ],
        &embedding_env,
    );
    assert!(publish.status.success(), "{}", combined(&publish));

    let objects = stub.objects();
    assert!(
        objects
            .keys()
            .any(|key| key.ends_with("embedding-identity.json")),
        "the published split must carry its embedding identity: {objects:#?}"
    );
    assert!(
        objects.keys().any(|key| key.ends_with("manifest.json")),
        "the pages must be authoritative, not only indexed: {objects:#?}"
    );
    let after_publish = stub.requests();

    // Machine B: its own process, its own S3 client and cache. It knows only
    // the bucket coordinates and the query text.
    let answer = probe_with_env(
        &stub,
        &[
            "vector-query",
            "--workspace",
            VECTOR_WORKSPACE,
            "--project",
            VECTOR_PROJECT,
            VECTOR_QUERY,
        ],
        &embedding_env,
    );
    assert!(answer.status.success(), "{}", combined(&answer));
    let parsed: serde_json::Value =
        serde_json::from_slice(&answer.stdout).expect("the vector answer must be JSON");
    let hits = parsed["hits"]
        .as_array()
        .unwrap_or_else(|| panic!("the answer must carry hits: {}", combined(&answer)));
    assert_eq!(
        hits.len(),
        1,
        "only the synonym page should be recalled: {parsed:#?}"
    );
    assert_eq!(hits[0]["path"], serde_json::json!(VECTOR_TARGET_PATH));
    assert_eq!(
        hits[0]["streams"],
        serde_json::json!(["vector"]),
        "the hit must be recalled only by the vector stream: {parsed:#?}"
    );
    assert_eq!(
        parsed["streams_active"],
        serde_json::json!(["vector"]),
        "no keyword stream may be active for this query: {parsed:#?}"
    );
    assert_eq!(parsed["stream_candidates"]["vector"], serde_json::json!(1));
    assert_eq!(
        parsed["stream_candidates"]["body"],
        serde_json::json!(0),
        "the body stream must have returned no candidate: {parsed:#?}"
    );

    // The S3 stub saw the second process read the catalog/authority/split over
    // a real HTTP connection; the process did not share an in-memory backend.
    let served = stub.requests();
    let reader_phase = &served[after_publish.len()..];
    let reads = object_gets(reader_phase);
    assert!(
        !reads.is_empty(),
        "the second process must have fetched objects over HTTP: {reader_phase:#?}"
    );

    // Control 1: no provider is a configuration failure, not a silent
    // keyword-only fallback. It fails before making an embedding request.
    let no_provider = probe(
        &stub,
        &[
            "vector-query",
            "--workspace",
            VECTOR_WORKSPACE,
            "--project",
            VECTOR_PROJECT,
            VECTOR_QUERY,
        ],
    );
    assert!(
        !no_provider.status.success(),
        "a vector query without a provider must fail closed:\n{}",
        combined(&no_provider)
    );
    assert!(
        String::from_utf8_lossy(&no_provider.stderr).contains("QM_EMBEDDING_BASE_URL"),
        "the missing-provider error must name the missing setting:\n{}",
        combined(&no_provider)
    );

    // Control 2: an equal-width model change is refused before the query is
    // embedded, so unrelated coordinates can never be ranked as if they were
    // comparable. The stub would happily answer model B; the identity guard is
    // what has to stop it.
    let mismatch_env = vec![
        ("QM_EMBEDDING_BASE_URL", embeddings.base_url()),
        ("QM_EMBEDDING_API_KEY", VECTOR_KEY.to_string()),
        ("QM_EMBEDDING_MODEL", VECTOR_MODEL_B.to_string()),
        ("QM_EMBEDDING_DIM", VECTOR_DIM.to_string()),
    ];
    let mismatch = probe_with_env(
        &stub,
        &[
            "vector-query",
            "--workspace",
            VECTOR_WORKSPACE,
            "--project",
            VECTOR_PROJECT,
            VECTOR_QUERY,
        ],
        &mismatch_env,
    );
    assert!(
        !mismatch.status.success(),
        "a model mismatch must fail closed:\n{}",
        combined(&mismatch)
    );
    let mismatch_stderr = String::from_utf8_lossy(&mismatch.stderr);
    assert!(
        mismatch_stderr.contains(VECTOR_MODEL_A) && mismatch_stderr.contains(VECTOR_MODEL_B),
        "the mismatch error must name both models:\n{}",
        combined(&mismatch)
    );

    let seen = embeddings.shutdown();
    assert_eq!(
        seen.len(),
        2,
        "only the publish and the positive query may reach the provider: {seen:#?}"
    );
    assert_eq!(seen[0].model, VECTOR_MODEL_A);
    assert_eq!(
        seen[0].inputs,
        vec![target_embedding_text(), distractor_embedding_text()]
    );
    assert_eq!(seen[1].model, VECTOR_MODEL_A);
    assert_eq!(seen[1].inputs, vec![VECTOR_QUERY.to_string()]);

    stub.shutdown();
}

/// The multi-machine scenario (`search-probe project`) over the same socket:
/// three writers, one catalog, one reader. The writers are concurrent tasks
/// inside one process here — what this adds over the in-memory scenario test
/// is that every bucket access is real HTTP.
#[test]
fn the_multi_machine_scenario_runs_over_real_http() {
    let stub =
        S3Stub::start_with(StubOptions::default().with_page_size(2)).expect("starting the stub");

    let output = probe(&stub, &["project"]);
    assert!(output.status.success(), "{}", combined(&output));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("splits=3 hits=1"),
        "the scenario must publish three splits and answer from the current version only:\n{}",
        combined(&output)
    );
    assert!(
        stdout.contains("all checks passed"),
        "the scenario verifies itself; a failure list must not pass:\n{}",
        combined(&output)
    );
    assert!(
        stdout.contains("cleaned up"),
        "the scenario removes its own scope when it is not asked to keep it:\n{}",
        combined(&output)
    );

    // Real conditional writes and a real paginated listing crossed the wire,
    // which is the part `InMemory` cannot show.
    let served = stub.requests();
    let conditional = served
        .iter()
        .filter(|request| request.method == "PUT" && request.if_match.is_some())
        .count();
    let creates = served
        .iter()
        .filter(|request| request.method == "PUT" && request.if_none_match.as_deref() == Some("*"))
        .count();
    assert!(
        conditional > 0 && creates > 0,
        "the scenario must commit through conditional writes: {served:#?}"
    );
    assert!(
        listings(&served).len() > 1,
        "the scenario must have paged through the bucket: {served:#?}"
    );

    stub.shutdown();
}
