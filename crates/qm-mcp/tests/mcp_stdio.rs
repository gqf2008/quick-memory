//! Protocol-level check: speak MCP to the real binary over stdio.
//!
//! This is the only test that exercises the wire format rather than a Rust
//! call, so it catches transport and handshake mistakes that unit tests cannot.
//! It runs against `--synthetic-bucket`, which is explicitly not a backend:
//! the real one is verified by the probes, with credentials.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use qm_core::{KeyLayout, ProjectId, SessionId, WorkspaceId};
use qm_probe::s3_stub::S3Stub;

/// Clients start this binary without ever running `--help`, so `--version` has
/// to work — and it has to work before any bucket configuration is consulted,
/// which is why this runs with a cleared environment.
#[test]
fn the_mcp_binary_reports_its_version() {
    let output = Command::new(env!("CARGO_BIN_EXE_qm-mcp"))
        .arg("--version")
        .env_clear()
        .output()
        .expect("running qm-mcp --version");
    assert!(
        output.status.success(),
        "`qm-mcp --version` must succeed without any configuration: {:?}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(env!("CARGO_PKG_VERSION")),
        "expected version {} in {stdout:?}",
        env!("CARGO_PKG_VERSION")
    );
}

struct Client {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl Client {
    fn start() -> Self {
        Self::start_with(&[])
    }

    /// Same server, with extra environment. Used to configure an embedding
    /// provider; last write wins, so an override may replace the defaults.
    fn start_with(envs: &[(&str, &str)]) -> Self {
        Self::start_inner(envs, true)
    }

    /// Start against an S3-compatible socket instead of the synthetic bucket.
    fn start_s3(envs: &[(&str, &str)]) -> Self {
        Self::start_inner(envs, false)
    }

    fn start_inner(envs: &[(&str, &str)], synthetic: bool) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_qm-mcp"));
        if synthetic {
            command.arg("--synthetic-bucket");
        }
        command
            .env("QM_WORKSPACE", "acme")
            .env("QM_PROJECT", "ai-memory")
            .env("QM_WRITER", "test-machine")
            .env(
                "QM_CACHE_DIR",
                std::env::temp_dir().join(format!("qm-mcp-test-{}", std::process::id())),
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        for (key, value) in envs {
            command.env(key, value);
        }
        let mut child = command.spawn().expect("spawning qm-mcp");
        let stdin = child.stdin.take().expect("stdin");
        let stdout = BufReader::new(child.stdout.take().expect("stdout"));
        Self {
            child,
            stdin,
            stdout,
        }
    }

    fn send(&mut self, message: serde_json::Value) {
        let line = serde_json::to_string(&message).expect("serialize");
        writeln!(self.stdin, "{line}").expect("write");
        self.stdin.flush().expect("flush");
    }

    fn recv(&mut self) -> serde_json::Value {
        let mut line = String::new();
        self.stdout.read_line(&mut line).expect("read");
        assert!(!line.trim().is_empty(), "server closed the stream");
        serde_json::from_str(&line).unwrap_or_else(|error| panic!("bad JSON {error}: {line}"))
    }

    /// `initialize` + `initialized`, as a client must do before any call.
    fn handshake(&mut self) {
        self.send(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "qm-test", "version": "0.0.1"}
            }
        }));
        let response = self.recv();
        assert_eq!(response["id"], 1, "{response}");
        assert_eq!(
            response["result"]["serverInfo"]["name"], "quick-memory",
            "{response}"
        );
        self.send(serde_json::json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        }));
    }

    fn call_tool(
        &mut self,
        id: i64,
        name: &str,
        arguments: serde_json::Value,
    ) -> serde_json::Value {
        let response = self.call_tool_raw(id, name, arguments);
        assert_eq!(response["id"], id, "{response}");
        assert!(response["error"].is_null(), "{response}");
        response
    }

    /// Same call, but tolerating a JSON-RPC error so failure paths are testable.
    fn call_tool_raw(
        &mut self,
        id: i64,
        name: &str,
        arguments: serde_json::Value,
    ) -> serde_json::Value {
        self.send(serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments}
        }));
        self.recv()
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn tool_text(response: &serde_json::Value) -> String {
    response["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("no text content: {response}"))
        .to_string()
}

fn publish_watermarks(cache_dir: &Path) -> Vec<PathBuf> {
    let mut paths = std::fs::read_dir(cache_dir)
        .expect("reading cache dir")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("publish-") && name.ends_with(".json"))
        })
        .collect::<Vec<_>>();
    paths.sort();
    paths
}

/// A publish watermark describes one bucket, not just one machine and cache.
///
/// The first server publishes into bucket A and leaves its watermark in the
/// shared cache. The second server then compacts a non-empty index into bucket
/// B (without touching the publish watermark) and publishes. If the MCP
/// context does not carry the bucket identity, it reads A's watermark while
/// pointing at B and incorrectly reports that B is up to date.
#[test]
fn mcp_publish_watermarks_are_scoped_to_the_bucket() {
    let root = std::env::temp_dir().join(format!("qm-mcp-bucket-identity-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let cache = root.join("cache");
    std::fs::create_dir_all(&cache).expect("creating cache dir");
    let cache_arg = cache.to_str().expect("cache dir is UTF-8");
    let endpoint = "https://synthetic.invalid";
    let writer = "switching-machine";

    {
        let mut bucket_a = Client::start_with(&[
            ("QM_S3_ENDPOINT", endpoint),
            ("QM_S3_BUCKET", "bucket-a"),
            ("QM_CACHE_DIR", cache_arg),
            ("QM_WRITER", writer),
        ]);
        bucket_a.handshake();
        bucket_a.call_tool(
            1,
            "memory_write_page",
            serde_json::json!({"path": "notes/from-a.md", "body": "only in bucket a"}),
        );
        let published = bucket_a.call_tool(2, "memory_publish", serde_json::json!({}));
        let published_text = tool_text(&published);
        assert!(
            !published_text.contains("nothing to publish"),
            "bucket A's first publish must build its index: {published_text}"
        );
    }

    // Premise: bucket A has left a watermark in the shared cache. Bucket B's
    // state will be seeded without calling publish, so this file can only
    // describe A when it is read after the switch.
    let watermarks_from_a = publish_watermarks(&cache);
    assert_eq!(
        watermarks_from_a.len(),
        1,
        "one published bucket must leave one watermark"
    );

    let mut bucket_b = Client::start_with(&[
        ("QM_S3_ENDPOINT", endpoint),
        ("QM_S3_BUCKET", "bucket-b"),
        ("QM_CACHE_DIR", cache_arg),
        ("QM_WRITER", writer),
    ]);
    bucket_b.handshake();
    bucket_b.call_tool(
        3,
        "memory_write_page",
        serde_json::json!({"path": "notes/from-b.md", "body": "written in bucket b"}),
    );
    let compacted = bucket_b.call_tool(4, "memory_compact", serde_json::json!({}));
    let compacted_json: serde_json::Value = serde_json::from_str(&tool_text(&compacted))
        .unwrap_or_else(|error| panic!("compact must return JSON: {error}"));
    assert_eq!(compacted_json["splits_after"], 1, "{compacted_json}");

    let status = bucket_b.call_tool(5, "memory_status", serde_json::json!({}));
    let status_json: serde_json::Value = serde_json::from_str(&tool_text(&status))
        .unwrap_or_else(|error| panic!("status must return JSON: {error}"));
    assert_eq!(status_json["splits"], 1, "{status_json}");

    // Premise: compacting B did not consume or replace A's watermark.
    let watermarks_before_publish = publish_watermarks(&cache);
    assert_eq!(
        watermarks_before_publish, watermarks_from_a,
        "compaction must not write a publish watermark"
    );

    let published = bucket_b.call_tool(6, "memory_publish", serde_json::json!({}));
    let published_text = tool_text(&published);
    assert!(
        !published_text.contains("nothing to publish"),
        "switching buckets must publish into B, not read A's watermark: {published_text}"
    );
    let published_json: serde_json::Value = serde_json::from_str(&published_text)
        .unwrap_or_else(|error| panic!("publish must return JSON: {error}: {published_text}"));
    assert_eq!(published_json["pages"], 1, "{published_json}");

    drop(bucket_b);
    std::fs::remove_dir_all(&root).expect("cleaning synthetic test dir");
}

#[test]
fn mcp_handshake_lists_tools_and_runs_a_capture_search_round_trip() {
    let mut client = Client::start();
    client.handshake();

    client.send(serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/list",
        "params": {}
    }));
    let listed = client.recv();
    let names: Vec<String> = listed["result"]["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .filter_map(|tool| tool["name"].as_str().map(ToString::to_string))
        .collect();
    for expected in [
        "memory_capture",
        "memory_consolidate",
        "memory_search",
        "memory_write_page",
        "memory_read_page",
        "memory_delete_page",
        "memory_publish",
        "memory_compact",
        "memory_maintain",
        "memory_sessions",
        "memory_status",
        "memory_history",
        "memory_restore",
        "memory_log",
        "memory_recent",
        "memory_digest",
        "memory_compact_session",
        "memory_verify",
        "memory_propose",
        "memory_proposals",
        "memory_approve",
        "memory_reject",
        "memory_handoff_open",
        "memory_handoff_list",
        "memory_handoff_claim",
        "memory_handoff_done",
    ] {
        assert!(
            names.contains(&expected.to_string()),
            "missing {expected}: {names:?}"
        );
    }
    assert_eq!(names.len(), 26, "all advertised tools: {names:?}");
    // Every tool must be described well enough for an agent to choose it.
    for tool in listed["result"]["tools"].as_array().unwrap() {
        assert!(
            tool["description"].as_str().is_some_and(|d| !d.is_empty()),
            "tool without description: {tool}"
        );
        assert!(
            tool["inputSchema"].is_object(),
            "tool without schema: {tool}"
        );
    }

    // Capture → consolidate → publish → search, over the wire.
    let captured = client.call_tool(
        3,
        "memory_capture",
        serde_json::json!({
            "session": "sess-mcp",
            "text": "switched the index to tantivy splits"
        }),
    );
    let text = tool_text(&captured);
    assert!(
        text.contains("captured") || text.contains("already"),
        "{text}"
    );

    let _ = client.call_tool(
        4,
        "memory_consolidate",
        serde_json::json!({"session": "sess-mcp"}),
    );
    let _ = client.call_tool(5, "memory_publish", serde_json::json!({}));

    let searched = client.call_tool(
        6,
        "memory_search",
        serde_json::json!({"query": "tantivy", "limit": 5}),
    );
    let text = tool_text(&searched);
    assert!(text.contains("sessions/sess-mcp.md"), "{text}");

    // `memory_recent` answers "what was the last session working on?" without a
    // query, so it must see the page this round trip just published.
    let recent = client.call_tool(7, "memory_recent", serde_json::json!({"limit": 5}));
    let recent_text = tool_text(&recent);
    assert!(
        recent_text.contains("sessions/sess-mcp.md"),
        "{recent_text}"
    );
    // `created_at_ms` is a field `recent` has and the commit log does not, so a
    // tool mis-wired to `Command::Log` cannot satisfy this.
    assert!(
        recent_text.contains("created_at_ms"),
        "recent output should carry its own fields: {recent_text}"
    );

    // `limit` is documented as a per-section cap, so it must bound *every*
    // section. A zero cap is the sharpest probe: under a cap that only reached
    // `pages`, the session head and handoff sections would still come back
    // full, and this call would not be empty.
    let bounded = client.call_tool(29, "memory_digest", serde_json::json!({"limit": 0}));
    let bounded_json: serde_json::Value = serde_json::from_str(&tool_text(&bounded)).unwrap();
    assert!(
        bounded_json["pages"].as_array().is_some_and(Vec::is_empty),
        "limit 0 must cap the pages section: {bounded_json}"
    );
    assert!(
        bounded_json["sessions"]
            .as_array()
            .is_some_and(Vec::is_empty),
        "limit 0 must cap the sessions section, not only pages: {bounded_json}"
    );
    assert!(
        bounded_json["handoffs"]
            .as_array()
            .is_some_and(Vec::is_empty),
        "limit 0 must cap the handoffs section, not only pages: {bounded_json}"
    );

    // `memory_digest` is the "what did I miss?" open. It must carry all three
    // sections, and it must see the activity this round trip just produced:
    // a page commit and a session head.
    let digest = client.call_tool(30, "memory_digest", serde_json::json!({"limit": 5}));
    let digest_text = tool_text(&digest);
    let digest_json: serde_json::Value = serde_json::from_str(&digest_text).unwrap();
    // A tool mis-wired to `memory_log` would have no `sessions` key at all.
    assert!(digest_json["sessions"].is_array(), "{digest_text}");
    assert!(digest_json["handoffs"].is_array(), "{digest_text}");
    assert!(
        digest_text.contains("sessions/sess-mcp.md"),
        "the digest must see the page this session just committed: {digest_text}"
    );
    assert!(
        digest_text.contains("sess-mcp"),
        "the digest must see the session head: {digest_text}"
    );

    // Handoffs: open, list, claim, and refuse a second claim — over the wire.
    let opened = client.call_tool(
        8,
        "memory_handoff_open",
        serde_json::json!({
            "title": "finish the rebuild",
            "body": "compaction is pending"
        }),
    );
    // MCP tools answer in JSON, so an agent can act on the result directly.
    let opened_text = tool_text(&opened);
    let opened_json: serde_json::Value = serde_json::from_str(&opened_text)
        .unwrap_or_else(|error| panic!("open must be JSON: {error}: {opened_text}"));
    assert_eq!(opened_json["title"], "finish the rebuild", "{opened_json}");
    assert_eq!(opened_json["claimed_by"], serde_json::Value::Null);
    let opened_id = opened_json["id"].as_str().expect("handoff id").to_string();

    let listed = client.call_tool(
        9,
        "memory_handoff_list",
        serde_json::json!({"state": "all"}),
    );
    let listed_text = tool_text(&listed);
    let handoffs: serde_json::Value = serde_json::from_str(&listed_text)
        .unwrap_or_else(|error| panic!("list must be JSON: {error}: {listed_text}"));
    let id = handoffs[0]["id"].as_str().expect("handoff id").to_string();
    assert_eq!(
        id, opened_id,
        "the listed handoff must be the one just opened"
    );

    client.call_tool(10, "memory_handoff_claim", serde_json::json!({"id": id}));
    let second_claim =
        client.call_tool_raw(11, "memory_handoff_claim", serde_json::json!({"id": id}));
    assert!(
        !second_claim["error"].is_null(),
        "a second claim must fail: {second_claim}"
    );
    client.call_tool(12, "memory_handoff_done", serde_json::json!({"id": id}));

    // Global search reaches every project in the workspace.
    let global = client.call_tool(
        20,
        "memory_search",
        serde_json::json!({"query": "tantivy", "global": true}),
    );
    let global_text = tool_text(&global);
    assert!(
        global_text.contains("\"project_id\""),
        "global hits must carry provenance: {global_text}"
    );

    // Session retention: a dry run first, then applied.
    let plan = client.call_tool(
        18,
        "memory_compact_session",
        serde_json::json!({"session": "sess-mcp", "keep_last": 1, "keep_ms": 0}),
    );
    let plan_text = tool_text(&plan);
    let plan_json: serde_json::Value = serde_json::from_str(&plan_text)
        .unwrap_or_else(|error| panic!("dry run must be JSON: {error}: {plan_text}"));
    assert_eq!(plan_json["applied"], false, "{plan_json}");
    let applied = client.call_tool(
        19,
        "memory_compact_session",
        serde_json::json!({
            "session": "sess-mcp",
            "keep_last": 1,
            "keep_ms": 0,
            "apply": true
        }),
    );
    let applied_text = tool_text(&applied);
    assert!(
        applied_text.contains("segments") || applied_text.contains("applied"),
        "{applied_text}"
    );

    // Integrity check over the wire.
    let verified = client.call_tool(21, "memory_verify", serde_json::json!({}));
    let verified_text = tool_text(&verified);
    let verified_json: serde_json::Value = serde_json::from_str(&verified_text)
        .unwrap_or_else(|error| panic!("verify must be JSON: {error}: {verified_text}"));
    assert_eq!(
        verified_json["problems"].as_array().map(Vec::len),
        Some(0),
        "{verified_json}"
    );

    // A staged proposal changes nothing until it is approved.
    let proposed = client.call_tool(
        22,
        "memory_propose",
        serde_json::json!({
            "path": "notes/staged.md",
            "title": "Staged",
            "body": "curated body",
            "rationale": "clearer"
        }),
    );
    let proposed_text = tool_text(&proposed);
    let proposal: serde_json::Value = serde_json::from_str(&proposed_text)
        .unwrap_or_else(|error| panic!("propose must be JSON: {error}: {proposed_text}"));
    let proposal_id = proposal["id"].as_str().expect("proposal id").to_string();
    assert_eq!(proposal["state"], "pending", "{proposal}");

    let missing = client.call_tool_raw(
        23,
        "memory_read_page",
        serde_json::json!({"path": "notes/staged.md"}),
    );
    assert!(
        !missing["error"].is_null(),
        "a pending proposal must not create the page: {missing}"
    );

    let approved = client.call_tool(24, "memory_approve", serde_json::json!({"id": proposal_id}));
    let approved_text = tool_text(&approved);
    assert!(approved_text.contains("approved"), "{approved_text}");
    let created = client.call_tool(
        25,
        "memory_read_page",
        serde_json::json!({"path": "notes/staged.md"}),
    );
    assert!(
        tool_text(&created).contains("curated body"),
        "{}",
        tool_text(&created)
    );

    // History and restore over the wire: the older body comes back as a new
    // version rather than overwriting anything.
    let written = client.call_tool(
        13,
        "memory_write_page",
        serde_json::json!({"path": "notes/history.md", "body": "first body"}),
    );
    let _ = written;
    let _ = client.call_tool(
        14,
        "memory_write_page",
        serde_json::json!({"path": "notes/history.md", "body": "second body"}),
    );
    let history = client.call_tool(
        15,
        "memory_history",
        serde_json::json!({"path": "notes/history.md"}),
    );
    let history_text = tool_text(&history);
    let versions: serde_json::Value = serde_json::from_str(&history_text)
        .unwrap_or_else(|error| panic!("history must be JSON: {error}: {history_text}"));
    assert_eq!(versions.as_array().map(Vec::len), Some(2), "{versions}");
    let oldest = versions[0]["page_id"]
        .as_str()
        .expect("version id")
        .to_string();

    let restored = client.call_tool(
        16,
        "memory_restore",
        serde_json::json!({"path": "notes/history.md", "version": oldest}),
    );
    assert!(
        tool_text(&restored).contains("restored_from"),
        "{}",
        tool_text(&restored)
    );
    let current = client.call_tool(
        17,
        "memory_read_page",
        serde_json::json!({"path": "notes/history.md"}),
    );
    assert!(
        tool_text(&current).contains("first body"),
        "{}",
        tool_text(&current)
    );

    // A tool that fails must report the failure rather than invent success.
    let bad = client.call_tool_raw(
        7,
        "memory_read_page",
        serde_json::json!({"path": "../escape"}),
    );
    assert!(
        !bad["error"].is_null() || bad["result"]["isError"] == true,
        "an invalid path must not look like a success: {bad}"
    );
    assert!(
        bad["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("portable")),
        "the error must explain the rejection: {bad}"
    );
}

/// `memory_maintain` is the one-shot MCP counterpart of `qm maintain`.
#[test]
fn mcp_maintain_compiles_publishes_and_is_idempotent() {
    let cache_dir = std::env::temp_dir().join(format!("qm-mcp-maintain-{}", std::process::id()));
    std::fs::create_dir_all(&cache_dir).expect("create maintain cache dir");
    let mut client =
        Client::start_with(&[("QM_CACHE_DIR", cache_dir.to_str().expect("cache dir"))]);
    client.handshake();

    let captured = client.call_tool(
        1,
        "memory_capture",
        serde_json::json!({
            "session": "sess-mcp-maintain",
            "text": "maintain should compile and publish this observation"
        }),
    );
    assert!(
        !tool_text(&captured).is_empty(),
        "capture must return a result"
    );

    let first = client.call_tool(
        2,
        "memory_maintain",
        serde_json::json!({"compiler": "rules", "drain_limit": 10}),
    );
    let first_text = tool_text(&first);
    let first_json: serde_json::Value = serde_json::from_str(&first_text)
        .unwrap_or_else(|error| panic!("maintain must return JSON: {error}: {first_text}"));
    assert_eq!(first_json["consolidated"], 1, "{first_json}");
    assert_eq!(first_json["published"], true, "{first_json}");
    assert_eq!(first_json["failed"], 0, "{first_json}");
    assert_eq!(
        first_json["publish_error"],
        serde_json::Value::Null,
        "{first_json}"
    );

    let second = client.call_tool(3, "memory_maintain", serde_json::json!({}));
    let second_text = tool_text(&second);
    let second_json: serde_json::Value = serde_json::from_str(&second_text)
        .unwrap_or_else(|error| panic!("second maintain must return JSON: {error}: {second_text}"));
    assert_eq!(second_json["consolidated"], 0, "{second_json}");
    assert_eq!(second_json["already_up_to_date"], 1, "{second_json}");
    assert_eq!(second_json["published"], false, "{second_json}");
    assert_eq!(
        second_json["manifest_seq"], first_json["manifest_seq"],
        "idempotent maintain must not advance the manifest: first={first_json}, second={second_json}"
    );
    assert_eq!(
        second_json["splits"], first_json["splits"],
        "idempotent maintain must not add splits: first={first_json}, second={second_json}"
    );

    let searched = client.call_tool(
        4,
        "memory_search",
        serde_json::json!({
            "query": "maintain should compile",
            "limit": 5
        }),
    );
    assert!(
        tool_text(&searched).contains("sessions/sess-mcp-maintain.md"),
        "maintain must make the page searchable: {}",
        tool_text(&searched)
    );
}

/// A partial maintain failure must cross the real JSON-RPC `tools/call`
/// boundary as a tool-level error, with the complete report still available.
#[tokio::test]
async fn mcp_maintain_partial_failure_is_a_tool_error_with_the_full_report() {
    let stub = S3Stub::start().expect("start S3 stub");
    let bucket = AmazonS3Builder::new()
        .with_bucket_name(stub.bucket())
        .with_region("auto")
        .with_endpoint(stub.endpoint())
        .with_allow_http(true)
        .with_access_key_id("stub-access")
        .with_secret_access_key("stub-secret")
        .with_virtual_hosted_style_request(false)
        .build()
        .expect("build S3 client");

    let workspace = WorkspaceId::new("partial-ws").expect("workspace");
    let project = ProjectId::new("partial-project").expect("project");
    let session = SessionId::new("sess-broken").expect("session");
    let key = KeyLayout::new("v1").session_head(&workspace, &project, &session);
    bucket
        .put(
            &object_store::path::Path::from(key),
            b"not-json".to_vec().into(),
        )
        .await
        .expect("seed corrupt session head");

    let cache_dir = std::env::temp_dir().join(format!("qm-mcp-partial-{}", std::process::id()));
    std::fs::create_dir_all(&cache_dir).expect("create partial-failure cache dir");
    let endpoint = stub.endpoint();
    let mut client = Client::start_s3(&[
        ("QM_S3_ENDPOINT", endpoint.as_str()),
        ("QM_S3_BUCKET", stub.bucket()),
        ("QM_S3_ACCESS_KEY_ID", "stub-access"),
        ("QM_S3_SECRET_ACCESS_KEY", "stub-secret"),
        ("QM_CACHE_DIR", cache_dir.to_str().expect("cache dir")),
        ("QM_WORKSPACE", "partial-ws"),
        ("QM_PROJECT", "partial-project"),
        ("QM_WRITER", "partial-machine"),
    ]);
    client.handshake();

    let response = client.call_tool_raw(
        1,
        "memory_maintain",
        serde_json::json!({"compiler": "rules"}),
    );
    assert!(
        response["error"].is_null(),
        "partial failure must stay a tool result: {response}"
    );
    assert_eq!(
        response["result"]["isError"], true,
        "partial failure must set isError: {response}"
    );
    let report_text = response["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("tool error must carry report text: {response}"))
        .to_string();
    let report: serde_json::Value = serde_json::from_str(&report_text)
        .unwrap_or_else(|error| panic!("tool error report must be JSON: {error}: {report_text}"));
    assert_eq!(report["failed"], 1, "{report}");
    assert_eq!(report["failures"][0]["session"], "sess-broken", "{report}");
    assert!(
        report
            .as_object()
            .is_some_and(|report| report.contains_key("publish_error")),
        "{report}"
    );

    stub.shutdown();
}

/// A local embedding endpoint that answers every batch with the same vector.
///
/// The point of the `no_vector` test below is to see the vector stream appear
/// and disappear, which needs a provider that actually answers. A stub keeps
/// that off the network; one vector for everything is enough because the test
/// asks whether the stream ran, not what it ranked.
fn embedding_stub() -> String {
    use std::io::{Read, Write};
    use std::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind stub");
    let address = listener.local_addr().expect("stub address");
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut request = vec![0u8; 65536];
            let read = stream.read(&mut request).unwrap_or(0);
            let request = String::from_utf8_lossy(&request[..read]).to_string();
            // One row per input: the client counts them and would reject a
            // mismatch, so the stub derives the count from the request.
            let rows = request
                .split_once("\"input\"")
                .and_then(|(_, rest)| rest.split_once('['))
                .map_or(1, |(_, rest)| {
                    let end = rest.find(']').unwrap_or(rest.len());
                    (rest[..end].matches('"').count() / 2).max(1)
                });
            let data: Vec<serde_json::Value> = (0..rows)
                .map(|_| serde_json::json!({"embedding": [1.0, 0.0, 0.0, 0.0]}))
                .collect();
            let body = serde_json::json!({ "data": data }).to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes());
        }
    });
    format!("http://{address}")
}

/// `no_vector` must actually switch the stream off over the wire.
///
/// With no provider configured this is invisible: a dropped `no_vector` field
/// would produce byte-identical output, so a test asserting only "the call
/// works" cannot fail. Here a provider is configured (a local stub), which
/// makes the difference observable: the stream shows up in `streams_active`
/// until `no_vector` removes it.
#[test]
fn mcp_search_no_vector_switches_the_vector_stream_off() {
    let base_url = embedding_stub();
    let cache_dir = std::env::temp_dir().join(format!("qm-mcp-no-vector-{}", std::process::id()));
    let mut client = Client::start_with(&[
        ("QM_EMBEDDING_BASE_URL", base_url.as_str()),
        ("QM_EMBEDDING_API_KEY", "test-key"),
        ("QM_EMBEDDING_MODEL", "test-model"),
        ("QM_EMBEDDING_DIM", "4"),
        ("QM_CACHE_DIR", cache_dir.to_str().expect("cache dir")),
    ]);
    client.handshake();

    let _ = client.call_tool(
        1,
        "memory_capture",
        serde_json::json!({"session": "sess-vec", "text": "switched the index to tantivy splits"}),
    );
    let _ = client.call_tool(
        2,
        "memory_consolidate",
        serde_json::json!({"session": "sess-vec"}),
    );
    // Publication embeds through the stub, so the split carries vectors.
    let published = client.call_tool(3, "memory_publish", serde_json::json!({}));
    assert!(
        !tool_text(&published).contains("error"),
        "publishing with a provider must embed: {}",
        tool_text(&published)
    );

    let on = client.call_tool(
        4,
        "memory_search",
        serde_json::json!({"query": "tantivy", "limit": 5}),
    );
    let on_json: serde_json::Value = serde_json::from_str(&tool_text(&on))
        .unwrap_or_else(|error| panic!("search must be JSON: {error}: {}", tool_text(&on)));
    let streams = |value: &serde_json::Value| -> Vec<String> {
        value["streams_active"]
            .as_array()
            .expect("streams_active")
            .iter()
            .filter_map(|stream| stream.as_str().map(ToString::to_string))
            .collect()
    };
    assert!(
        streams(&on_json).iter().any(|stream| stream == "vector"),
        "with a provider configured the vector stream must run: {on_json}"
    );

    let off = client.call_tool(
        5,
        "memory_search",
        serde_json::json!({"query": "tantivy", "limit": 5, "no_vector": true}),
    );
    let off_json: serde_json::Value = serde_json::from_str(&tool_text(&off))
        .unwrap_or_else(|error| panic!("search must be JSON: {error}: {}", tool_text(&off)));
    assert!(
        off_json["hits"]
            .as_array()
            .is_some_and(|hits| hits.iter().any(|hit| hit["path"] == "sessions/sess-vec.md")),
        "no_vector must not cost the keyword hits: {off_json}"
    );
    assert!(
        !streams(&off_json).iter().any(|stream| stream == "vector"),
        "no_vector must keep the vector stream out: {off_json}"
    );
}

/// `QM_MANIFEST_FORMAT` reaches the store through the MCP binary.
///
/// The switch is resolved once, by the process that starts the server, and has
/// to survive every tool call after that: a server that fell back to the
/// default would write a whole manifest into a scope the operator declared
/// sharded, and the store would refuse it — correctly, but only once a write is
/// attempted. `memory_status` reports the form the scope is *stored* in, so it
/// is what shows which shape the writes actually took.
#[test]
fn the_manifest_form_switch_reaches_the_mcp_store() {
    let mut server = Client::start_with(&[("QM_MANIFEST_FORMAT", "2")]);
    server.handshake();
    let written = server.call_tool(
        1,
        "memory_write_page",
        serde_json::json!({"path": "notes/sharded.md", "body": "written while sharded"}),
    );
    let written_text = tool_text(&written);
    assert!(
        !written_text.contains("refusing to commit"),
        "the server has to write the form it was told to: {written_text}"
    );

    let status = server.call_tool(2, "memory_status", serde_json::json!({}));
    let status: serde_json::Value = serde_json::from_str(&tool_text(&status))
        .unwrap_or_else(|error| panic!("status must return JSON: {error}"));
    assert_eq!(status["manifest_format"], 2, "{status}");
    assert_eq!(status["manifest_shards"], 1, "{status}");
    assert_eq!(status["pages"], 1, "{status}");

    // The premise: the same call on a server that was not told anything keeps
    // writing the single object, so the assertion above is about the switch.
    let mut plain = Client::start();
    plain.handshake();
    plain.call_tool(
        1,
        "memory_write_page",
        serde_json::json!({"path": "notes/plain.md", "body": "written by default"}),
    );
    let status = plain.call_tool(2, "memory_status", serde_json::json!({}));
    let status: serde_json::Value = serde_json::from_str(&tool_text(&status)).unwrap();
    assert_eq!(status["manifest_format"], 1, "{status}");
    assert_eq!(status["manifest_shards"], 0, "{status}");
}
